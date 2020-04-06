import asyncio
import enum
import logging
import random
import math

import aiohttp
from aiohttp import web

logger = logging.getLogger(__name__)

ELECTION_TIMEOUT_HIGH = 600
ELECTION_TIMEOUT_LOW = ELECTION_TIMEOUT_HIGH / 2

HEARTBEAT_TIMEOUT = 200


def invoke(cbl):
    future = asyncio.ensure_future(cbl)

    def done_callback(future):
        try:
            future.result()
        except:
            logger.exception("Unhandled exception in async task")

    future.add_done_callback(done_callback)


class NodeState(enum.IntEnum):

    FOLLOWER = 1
    CANDIDATE = 2
    LEADER = 3


class Node:

    def __init__(self):
        self.state = NodeState.FOLLOWER
        self.identifier = "self"

        # persistent state
        self.current_term = 0
        self.voted_for = None
        self.log = []

        # volatile state
        self.commit_index = 0
        self.last_applied = 0

        # leader state
        self.remotes = []

        self._heartbeat = None

    @property
    def last_index(self):
        return len(self.log)

    @property
    def last_term(self):
        if not self.log:
            return 0
        return self.log[-1][0]

    @property
    def cluster_size(self):
        return len(self.remotes) + 1

    @property
    def quorum(self):
        return math.floor(self.cluster_size / 2) + 1

    def add_member(self, identifier):
        self.remotes.append(RemoteNode(identifier))

    def cancel_election_timeout(self):
        logger.debug("Cancelling election timeout")
        if self._heartbeat:
            self._heartbeat.cancel()
            self._heartbeat = None

    def reset_election_timeout(self):
        self.cancel_election_timeout()

        logger.debug("Setting election timeout")
        timeout = random.randrange(ELECTION_TIMEOUT_LOW, ELECTION_TIMEOUT_HIGH) / 1000
        loop = asyncio.get_event_loop()
        self._heartbeat = loop.call_later(timeout, self.become_candidate)

    def become_follower(self):
        logger.debug("Became follower")
        self.state = NodeState.FOLLOWER
        self.reset_election_timeout()

    def become_candidate(self):
        if self.state == NodeState.LEADER:
            logger.debug("Can't become candidate when already leader")
            return

        logger.debug("Became candidate")
        self.state = NodeState.CANDIDATE

        invoke(self.do_gather_votes())

    def become_leader(self):
        logger.debug("Became leader")
        self.state = NodeState.LEADER
        self.cancel_election_timeout()

        invoke(self.do_heartbeats())

    async def do_gather_votes(self):
        """Try and gather votes from all peers for current term."""
        while self.state == NodeState.CANDIDATE:
            self.current_term += 1
            self.voted_for = self.identifier

            payload = {
                "term": self.current_term,
                "candidate_id": self.identifier,
                "last_index": self.last_index,
                "last_term": self.last_term,
            }

            requests = [
                node.send_request_vote(payload) for node in self.remotes
            ]

            random_timeout = random.randrange(300, 500) / 100
 
            gathered = asyncio.gather(*requests, return_exceptions=True)

            try:
                responses = await asyncio.wait_for(gathered, random_timeout)
            except asyncio.TimeoutError:
                logger.debug("Election timed out. Starting again.")
                continue

            votes = 1
            for response in responses:
                if isinstance(response, Exception):
                    logger.exception(response)
                    continue
                if response["vote_granted"] == True:
                    votes += 1

            logger.debug("In term %s, got %d votes, needed %d", self.current_term, votes, self.quorum)
            if votes >= self.quorum:
                self.become_leader()
                return

            # This is useful in testing, not so much in the real world?
            await asyncio.sleep(random_timeout)

    async def do_heartbeats(self):
        while self.state == NodeState.LEADER:
            logger.debug("Sending heartbeat")
            payload = {}

            for node in self.remotes:
                payload = {
                    "term": self.current_term,
                    "leader_id": self.identifier,
                    "prev_index": self.last_index,
                    "prev_term": self.last_term,
                    "entries": [],
                    "leader_commit": self.commit_index,
                }
                invoke(node.send_append_entries(payload))

            await asyncio.sleep(0.1)

    def maybe_become_follower(self, term):
        if node.state == NodeState.LEADER:
            if term > node.current_term:
                logger.debug("Follower has higher term (%d vs %d)", term, node.current_term)
                node.become_follower()

    async def recv_append_entries(self, request):
        term = request["term"]

        node.maybe_become_follower(request["term"])

        node.reset_election_timeout()

        if term < self.current_term:
            logger.debug("Message received for old term %d, current term is %d", term, self.current_term)
            return False

        prev_index = request["prev_index"]
        prev_term = request["prev_term"]

        if prev_index < self.last_index:
            logger.debug("Leader assumed we had log entry %d but we do not", prev_index)
            return False

        if prev_index and self.log[prev_index][0] != prev_term:
            logger.debug("Log not valid - mismatched terms %d and %d at index %d", prev_term, self.log[prev_index][0], prev_index)
            return False

        # FIXME: If an existing entry conflicts with a new one (same index but different terms) delete the existing entry and all that follow it
        # Does that just mean trim before the previous return false???

        self.log.extend(request["entries"])

        if request["leader_commit"] > self.commit_index:
            commit_index = min(request["leader_commit"], len(self.log) - 1)
            logger.debug("Commit index advanced from %d to %d", self.commit_index, commit_index)
            self.commit_index = commit_index

        return True

    async def recv_request_vote(self, request):
        term = request["term"]
        logger.debug("Received a vote request for term %d", term)

        node.maybe_become_follower(request["term"])

        if term < self.current_term:
            logger.debug("Vote request rejected as term already over")
            return False

        if term == self.current_term and self.voted_for:
            logger.debug("Vote request rejected as already voted for self")
            return False

        last_term = request["last_term"]
        if last_term < self.last_term:
            logger.debug("Vote request rejected as last term older than current term")
            return False

        last_index = request["last_index"]
        if last_index < self.last_index:
            logger.debug("Vote request rejected as last index older than own log")
            return False

        self.voted_for = request["candidate_id"]
        return True


class RemoteNode:

    def __init__(self, identifier):
        self.identifier = identifier
        self.next_index = 0
        self.match_index = 0
        self.session = aiohttp.ClientSession()

    async def send_append_entries(self, payload):
        resp = await self.session.post(f'http://{self.identifier}/append-entries', json=payload)
        if resp.status != 200:
            return {"term": 0, "success": False}
        return resp.json()

    async def send_request_vote(self, payload):
        resp = await self.session.post(f'http://{self.identifier}/request-vote', json=payload)
        if resp.status != 200:
            return {"term": 0, "vote_granted": False}
        return await resp.json()


routes = web.RouteTableDef()

@routes.post('/append-entries')
async def append_entries(request):
    payload = await request.json()

    return web.json_response({
        "term": node.current_term,
        "success": await node.recv_append_entries(payload),
    })


@routes.post('/request-vote')
async def request_vote(request):
    payload = await request.json()

    return web.json_response({
        "term": node.current_term,
        "vote_granted": await node.recv_request_vote(payload),
    })

logging.basicConfig(level=logging.DEBUG)

import argparse

parser = argparse.ArgumentParser()
parser.add_argument("port")
args = parser.parse_args()

node = Node()
node.identifier = f"127.0.0.1:{args.port}"
for remote in (8080, 8081, 8082):
    if int(args.port) != remote:
        node.add_member(f"127.0.0.1:{remote}")
node.become_follower()

app = web.Application()
app.add_routes(routes)
web.run_app(app, port=args.port)
