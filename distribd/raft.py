import asyncio
import enum
import logging
import math
import random

import aiohttp
from aiohttp import web

from .utils.web import run_server

logger = logging.getLogger(__name__)

ELECTION_TIMEOUT_HIGH = 600
ELECTION_TIMEOUT_LOW = ELECTION_TIMEOUT_HIGH / 2

HEARTBEAT_TIMEOUT = 100

SCALE_FACTOR = 1000
SCALE_FACTOR = 100


def invoke(cbl):
    future = asyncio.ensure_future(cbl)

    def done_callback(future):
        try:
            future.result()
        except Exception:
            logger.exception("Unhandled exception in async task")

    future.add_done_callback(done_callback)


class NotALeader(Exception):
    pass


class NodeState(enum.IntEnum):

    FOLLOWER = 1
    CANDIDATE = 2
    LEADER = 3


class Node:
    def __init__(self, identifier, log):
        self.state = NodeState.FOLLOWER
        self.identifier = identifier

        # persistent state
        self.log = log
        self.current_term = log.last_term
        self.voted_for = None

        # volatile state
        self.commit_index = 0
        self.last_applied = 0

        # leader state
        self.remotes = []

        self._heartbeat = None

    async def add_entry(self, entry):
        if self.state != NodeState.LEADER:
            raise NotALeader("Only leader can append to log")
        await self.log.commit(self.current_term, entry)
        return self.log.last_term, self.log.last_index

    @property
    def cluster_size(self):
        return len(self.remotes) + 1

    @property
    def quorum(self):
        return math.floor(self.cluster_size / 2) + 1

    def add_member(self, identifier):
        node = RemoteNode(identifier)
        node.next_index = self.log.last_index + 1
        self.remotes.append(node)

    def cancel_election_timeout(self):
        logger.debug("Cancelling election timeout")
        if self._heartbeat:
            self._heartbeat.cancel()
            self._heartbeat = None

    def reset_election_timeout(self):
        self.cancel_election_timeout()

        logger.debug("Setting election timeout")
        timeout = (
            random.randrange(ELECTION_TIMEOUT_LOW, ELECTION_TIMEOUT_HIGH) / SCALE_FACTOR
        )
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

        # It's considered "unsafe" to commit a transaction from a previous term
        # But safe to indirectly commit it when commiting a no-op in the current term
        invoke(self.add_entry({}))

    async def advance_commit_index(self):
        logger.debug("Maybe advance commit index")

        commit_index = 0
        i = max(self.commit_index, 1)
        while i <= self.log.last_index:
            if self.log[i][0] != self.current_term:
                i += 1
                continue

            # Start counting at 1 because we count as a vote
            match_count = 1
            for peer in self.remotes:
                logger.debug("Peer match index is %d", peer.match_index)
                if peer.match_index >= i:
                    match_count += 1

            logger.debug("%d peers have index %d", match_count, i)

            if match_count >= self.quorum:
                logger.debug("quorum for index %d", i)
                commit_index = i

            i += 1

        if commit_index <= self.commit_index:
            logger.debug("Commit index unchanged")
            return

        logger.debug(
            "Achieved a quorum - advancing commit index from %d to %d",
            self.commit_index,
            commit_index,
        )
        self.commit_index = commit_index

        await self.log.apply(self.commit_index)

    async def do_gather_votes(self):
        """Try and gather votes from all peers for current term."""
        while self.state == NodeState.CANDIDATE:
            self.current_term += 1
            self.voted_for = self.identifier

            payload = {
                "term": self.current_term,
                "candidate_id": self.identifier,
                "last_index": self.log.last_index,
                "last_term": self.log.last_term,
            }

            requests = [node.send_request_vote(payload) for node in self.remotes]

            random_timeout = (
                random.randrange(ELECTION_TIMEOUT_LOW, ELECTION_TIMEOUT_HIGH)
                / SCALE_FACTOR
            )

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
                if response["vote_granted"] is True:
                    votes += 1

            logger.debug(
                "In term %s, got %d votes, needed %d",
                self.current_term,
                votes,
                self.quorum,
            )
            if votes >= self.quorum and self.state == NodeState.CANDIDATE:
                self.become_leader()
                return

            # This is useful in testing, not so much in the real world?
            await asyncio.sleep(random_timeout)

    async def do_heartbeat(self, node):
        print(node.match_index, node.next_index)

        prev_index = node.next_index - 1
        prev_term = self.log[prev_index][0] if prev_index else 0
        entries = self.log[node.next_index :]

        logger.debug("WILL SEND %s", entries)
        logger.debug(
            "prev_index %d prev_term %d next_index %d entries %s",
            prev_index,
            prev_term,
            node.next_index,
            entries,
        )
        payload = {
            "term": self.current_term,
            "leader_id": self.identifier,
            "prev_index": prev_index,
            "prev_term": prev_term,
            "entries": entries,
            "leader_commit": self.commit_index,
        }

        result = await node.send_append_entries(payload)

        if not result["success"]:
            if node.next_index > 0:
                node.next_index -= 1
            return

        node.next_index += len(entries)
        node.match_index = node.next_index - 1

        await self.advance_commit_index()

    async def do_heartbeats(self):
        while self.state == NodeState.LEADER:
            logger.debug("Sending heartbeat")
            logger.debug(
                "Current log at term %d, index %d",
                self.log.last_term,
                self.log.last_index,
            )

            for node in self.remotes:
                invoke(self.do_heartbeat(node))

            await asyncio.sleep(HEARTBEAT_TIMEOUT / SCALE_FACTOR)

    def maybe_become_follower(self, term):
        if self.state != NodeState.FOLLOWER:
            if term > self.current_term:
                logger.debug(
                    "Follower has higher term (%d vs %d)", term, self.current_term
                )
                self.become_follower()

    async def recv_append_entries(self, request):
        logger.debug(request)

        term = request["term"]

        self.maybe_become_follower(request["term"])

        self.reset_election_timeout()

        if term < self.current_term:
            logger.debug(
                "Message received for old term %d, current term is %d",
                term,
                self.current_term,
            )
            return False

        prev_index = request["prev_index"]
        prev_term = request["prev_term"]

        if prev_index > self.log.last_index:
            logger.debug("Leader assumed we had log entry %d but we do not", prev_index)
            return False

        print(prev_index, self.log.last_index)
        if prev_index and self.log[prev_index][0] != prev_term:
            logger.debug(
                "Log not valid - mismatched terms %d and %d at index %d",
                prev_term,
                self.log[prev_index][0],
                prev_index,
            )
            return False

        # FIXME: If an existing entry conflicts with a new one (same index but different terms) delete the existing entry and all that follow it
        # Does that just mean trim before the previous return false???

        for entry in request["entries"]:
            await self.log.commit(entry[0], entry[1])

        if request["leader_commit"] > self.commit_index:
            commit_index = min(request["leader_commit"], self.log.last_index)
            logger.debug(
                "Commit index advanced from %d to %d", self.commit_index, commit_index
            )
            self.commit_index = commit_index
            await self.log.apply(self.commit_index)

        logger.debug(
            "Current log at term %d index %d", self.log.last_term, self.log.last_index
        )

        return True

    async def recv_request_vote(self, request):
        term = request["term"]
        logger.debug("Received a vote request for term %d", term)

        self.maybe_become_follower(request["term"])

        if term < self.current_term:
            logger.debug("Vote request rejected as term already over")
            return False

        if term == self.current_term and self.voted_for:
            logger.debug("Vote request rejected as already voted for self")
            return False

        last_term = request["last_term"]
        if last_term < self.log.last_term:
            logger.debug("Vote request rejected as last term older than current term")
            return False

        last_index = request["last_index"]
        if last_index < self.log.last_index:
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

    async def send_add_entry(self, payload):
        resp = await self.session.post(
            f"http://{self.identifier}/add-entry", json=payload
        )
        if resp.status != 200:
            raise NotALeader("Unable to write to this node")
        payload = await resp.json()
        return resp["last_term"], resp["last_index"]

    async def send_append_entries(self, payload):
        resp = await self.session.post(
            f"http://{self.identifier}/append-entries", json=payload
        )
        if resp.status != 200:
            return {"term": 0, "success": False}
        return await resp.json()

    async def send_request_vote(self, payload):
        resp = await self.session.post(
            f"http://{self.identifier}/request-vote", json=payload
        )
        if resp.status != 200:
            return {"term": 0, "vote_granted": False}
        return await resp.json()


routes = web.RouteTableDef()


@routes.post("/append-entries")
async def append_entries(request):
    node = request.app["node"]

    payload = await request.json()

    return web.json_response(
        {"term": node.current_term, "success": await node.recv_append_entries(payload)}
    )


@routes.post("/request-vote")
async def request_vote(request):
    node = request.app["node"]

    payload = await request.json()

    return web.json_response(
        {
            "term": node.current_term,
            "vote_granted": await node.recv_request_vote(payload),
        }
    )


@routes.post("/add-entry")
async def add_entry(request):
    node = request.app["node"]

    payload = await request.json()

    if node.state != NodeState.LEADER:
        return web.json_response(
            status=400, reason="Not a leader", json={"reason": "NOT_A_LEADER"},
        )

    last_term, last_index = await node.add_entry(payload)

    return web.json_response({"last_term": last_term, "last_index": last_index})


async def run_raft(log, port):
    node = Node(f"127.0.0.1:{port}", log)

    for remote in (8080, 8081, 8082):
        if int(port) != remote:
            node.add_member(f"127.0.0.1:{remote}")

    node.become_follower()

    return await run_server("127.0.0.1", port, routes, node=node)
