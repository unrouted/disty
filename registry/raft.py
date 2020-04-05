import asyncio
import enum
import logging
import random

import aiohttp
from aiohttp import web

logger = logging.getLogger(__name__)

ELECTION_TIMEOUT_HIGH = 600
ELECTION_TIMEOUT_LOW = ELECTION_TIMEOUT_HIGH / 2

HEARTBEAT_TIMEOUT = 200


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

        self.become_candidate_flag = asyncio.Event()

        self._heartbeat = None

    def add_member(self, identifier):
        self.remotes.append(RemoteNode(identifier))

    async def send_append_entries(self, node):
        payload = {
            "term": self.current_term,
            "leader_id": self.identifier,

            # FIXME
            "prev_log_index": 0,
            "prev_log_term": 0,

            "leader_commit": self.commit_index,

            # FIXME
            "entries": [],
        }

        result = await node.send_append_entries(payload)

        if result["success"] == False:
            node.next_index -= 1
            return

        # The next entry we want to to try and send
        node.next_index += 1

        # The index we know to be replicated
        node.match_index += 1

    def reset_heartbeat(self):
        if self._heartbeat:
            self._heartbeat.cancel()
            self._heartbeat = None
        
        timeout = random.randrange(ELECTION_TIMEOUT_LOW, ELECTION_TIMEOUT_HIGH) / 1000
        loop = asyncio.get_event_loop()
        self._heartbeat = loop.call_later(timeout, self.become_candidate)

    async def become_follower(self):
        self.state = NodeState.FOLLOWER
        await self.become_candidate_flag.wait()
        await self.become_candidate()

    async def become_candidate(self):
        self.state = NodeState.CANDIDATE
        self.current_term += 1
        self.voted_for = self.identifier

        payload = {
            "term": self.current_term,
            "candidate_id": self.identifier,
            "last_log_index": 0,
            "last_log_term": 0,
        }

        requests = [
            node.send_request_vote(payload) for node in self.remotes
        ]

        votes = 0
        async for response in asyncio.gather(*requests, return_exceptions=True):
            try:
                result = response.get()
            except:
                continue
            if result["voteGranted"] == True:
                votes += 1

        if votes > self.chorum:
            await self.become_leader()

    async def become_leader(self):
        self.state = NodeState.LEADER

    def maybe_become_follower(self, term):
        if node.state == NodeState.LEADER:
            if term > node.current_term:
                node.become_follower()

    async def recv_append_entries(self, request):
        term = request["term"]

        node.maybe_become_follower(request["term"])

        node.reset_heartbeat()

        if term < self.current_term:
            logger.debug("Message received for old term %d, current term is %d", term, self.current_term)
            return False

        prev_index = request["prev_index"]
        prev_term = request["prev_term"]

        if prev_index not in self.log:
            logger.debug("Leader assumed we had log entry %d but we do not", prev_index)
            return False

        if self.log[prev_index][0] != prev_term:
            logger.debug("Log not valid - mismatched terms %d and %d at index %d", prev_term, self.log[prev_index][0], prev_index)
            return False

        # FIXME: If an existing entry conflicts with a new one (same index but different terms) delete the existing entry and all that follow it
        # Does that just mean trim before the previous return false???

        self.log.extend(request["entries"])

        if request["leaderCommit"] > self.commit_index:
            commit_index = min(request["leaderCommit"], len(self.log) - 1)
            logger.debug("Commit index advanced from %d to %d", self.commit_index, commit_index)
            self.commit_index = commit_index

        return True

    async def recv_request_vote(self, request):
        term = request["term"]

        node.maybe_become_follower(request["term"])

        if term < self.current_term:
            return False

        if self.voted_for:
            return False

        last_index = request["last_index"]
        last_term = request["last_term"]
    
        if last_term < self.current_term:
            return False

        if last_index < (len(self.log) - 1):
            return False

        self.voted_for = request["candidate_id"]

        return True


class RemoteNode:

    def __init__(self, identifier):
        self.identifier = identifier
        self.next_index = 0
        self.match_index = 0

    async def send_append_entries(self, payload):
        async with aiohttp.ClientSession() as client:
            resp = await client.post(f'http://{self.identifier}/append-entries', json=payload)
            if resp.status != 200:
                return {"term": 0, "success": False}
            return resp.json()

    async def send_request_vote(self, payload):
        async with aiohttp.ClientSession() as client:
            resp = await client.post(f'http://{self.identifier}/request-vote', json=payload)
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


import argparse

parser = argparse.ArgumentParser()
parser.add_argument("port")
args = parser.parse_args()

node = Node()
node.identifier = f"127.0.0.1:{args.port}"
for remote in (8080, 8081, 8082):
    node.add_member(f"127.0.0.1:{remote}")
node.become_follower()

app = web.Application()
app.add_routes(routes)
web.run_app(app, port=args.port)
