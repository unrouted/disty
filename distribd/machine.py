"""
A raft state macine.

This is the raft algorithm without any disk or network i/o.
"""

import enum
import logging
import math
import random
import time

from . import exceptions

logger = logging.getLogger(__name__)


class Message(str, enum.Flag):

    Tick = "Tick"
    Vote = "Vote"
    VoteReply = "VoteReply"
    PreVote = "PreVote"
    PreVoteReply = "PreVoteReply"
    AppendEntries = "AppendEntries"
    AppendEntriesReply = "AppendEntriesReply"
    AddEntries = "AddEntries"


REPLIES = {
    Message.Vote: Message.VoteReply,
    Message.PreVote: Message.PreVoteReply,
    Message.AppendEntries: Message.AppendEntriesReply,
}


class NodeState(enum.IntEnum):

    FOLLOWER = 1
    PRE_CANDIDATE = 2
    CANDIDATE = 3
    LEADER = 4


ELECTION_TICK_LOW = 1000
ELECTION_TICK_HIGH = 2000
HEARTBEAT_TICK = ELECTION_TICK_LOW / 10


class Msg:
    def __init__(self, source, destination, message_type, term=0, **kwargs):
        self.source = source
        self.destination = destination
        self.type = message_type
        self.term = term
        self.kwargs = kwargs

    def __getattr__(self, key):
        return self.kwargs[key]

    def reply(self, term, **kwargs):
        return Msg(
            source=self.destination,
            destination=self.source,
            message_type=REPLIES[self.type],
            term=term,
            **kwargs,
        )

    def to_dict(self):
        msg = {
            "source": self.source,
            "destination": self.destination,
            "type": str(self.type),
            "term": self.term,
        }
        msg.update(self.kwargs)
        return msg

    @classmethod
    def from_dict(cls, message: dict):
        source = message.pop("source")
        destination = message.pop("destination")
        message_type = getattr(Message, message.pop("type").split(".")[1])
        term = message.pop("term")

        return cls(
            source=source,
            destination=destination,
            message_type=message_type,
            term=term,
            **message,
        )


class Peer:
    def __init__(self, identifier):
        self.identifier = identifier
        self.next_index = 0
        self.match_index = 0


class Log:
    def __init__(self):
        self._log = []

        self.snapshot = None
        self.snapshot_index = 0
        self.snapshot_term = 0

    def load(self, log):
        self._log = []
        self._log.extend(log)

    @property
    def last_index(self):
        return self.snapshot_index + len(self._log)

    @property
    def last_term(self):
        if not self._log:
            return self.snapshot_term
        return self._log[-1][0]

    def truncate(self, index):
        self.truncate_index = index
        self._log = self._log[: self.truncate_index]
        return True

    def append(self, entry):
        self._log.append(entry)

    def __getitem__(self, key):
        if isinstance(key, slice):
            new_slice = slice(
                key.start - 1 if key.start else None,
                key.stop - 1 if key.stop else None,
                key.step,
            )
            return self._log[new_slice]

        return self._log[key - 1]


class Machine:
    def __init__(self, identifier: str):
        self.identifier = identifier

        self.log = Log()

        self.peers = {}

        self.term = 0
        self.voted_for = None

        self.outbox = []

        self.state = NodeState.FOLLOWER
        self.tick = 0
        self.obedient = True
        self.leader = None
        self.vote_count = 0

        # volatile state
        self.commit_index = 0
        self.truncate_index = 0

    def start(self):
        self._become_follower(self.term)

    def add_peer(self, identifier):
        peer = Peer(identifier)
        self.peers[identifier] = peer

    def send(self, peer, message_type, term, **kwargs):
        m = Msg(
            source=self.identifier,
            destination=peer.identifier,
            message_type=message_type,
            term=term,
            **kwargs,
        )
        self.outbox.append(m)

    def reply(self, message, term, **kwargs):
        m = message.reply(term, **kwargs)
        self.outbox.append(m)

    @property
    def cluster_size(self):
        return len(self.peers) + 1

    @property
    def quorum(self):
        return math.floor(self.cluster_size / 2) + 1

    @property
    def tick_expired(self):
        return self.tick < self.current_tick()

    @property
    def leader_active(self):
        if self.state == NodeState.LEADER:
            return True

        return self.obedient is True

    def append(self, entry):
        if self.state != NodeState.LEADER:
            raise exceptions.NotALeader()
        self.log.append((self.term, entry))

    def _reset(self, term):
        if term != self.term:
            self.term = term
            self.voted_for = None

        self.leader = None

    def current_tick(self):
        return time.monotonic_ns()

    def _reset_election_tick(self):
        random_tick = random.randrange(ELECTION_TICK_LOW, ELECTION_TICK_HIGH) * 1000000
        self.tick = self.current_tick() + random_tick

    def _reset_heartbeat_tick(self):
        self.tick = self.current_tick() + (HEARTBEAT_TICK * 1000000)

    def _become_follower(self, term, leader=None):
        logger.debug("Became follower %s %s", self.identifier, leader)
        self.state = NodeState.FOLLOWER
        self._reset(term)
        self.leader = leader
        self._reset_election_tick()

    def _become_pre_candidate(self):
        logger.debug("Became pre-candidate %s", self.identifier)
        self.state = NodeState.PRE_CANDIDATE
        self.obedient = False
        self.vote_count = 1
        self._reset_election_tick()

        for peer in self.peers.values():
            # FIXME: What about the rest of the payload?
            self.send(peer, Message.PreVote, self.term + 1, index=self.log.last_index)

    def _become_candidate(self):
        logger.debug("Became candidate %s", self.identifier)
        self.state = NodeState.CANDIDATE
        self._reset(self.term + 1)
        self.vote_count = 1
        self.voted_for = self.identifier

        self._reset_election_tick()

        for peer in self.peers.values():
            # FIXME: What about the rest of the payload?
            self.send(peer, Message.Vote, self.term, index=self.log.last_index)

    def _become_leader(self):
        logger.debug("Became leader %s", self.identifier)
        self.state = NodeState.LEADER
        self._reset(self.term)
        self._reset_heartbeat_tick()

        for peer in self.peers.values():
            peer.next_index = self.log.last_index + 1
            peer.match_index = 0

        self.broadcast_entries()

        self.append({})

    def peer_current(self, index, term):
        if self.log.last_index > index:
            return False
        if self.log.last_term > term:
            return False
        return True

    def can_vote(self, message):
        # We have already voted, but for someone else
        if self.voted_for and self.voted_for == message.source:
            return True

        # We have not voted, but we think there is a leader
        if not self.voted_for and not self.obedient:
            return True

        # FIXME: Is last_term appropriate here???
        if message.type == Message.PreVote and message.term > self.term:
            return True

        return False

    def maybe_commit(self):
        commit_index = 0
        i = max(self.commit_index, 1)
        while i <= self.log.last_index:
            if self.log[i][0] != self.term:
                i += 1
                continue

            # Start counting at 1 because we count as a vote
            match_count = 1
            for peer in self.peers.values():
                if peer.match_index >= i:
                    match_count += 1

            if match_count >= self.quorum:
                commit_index = i

            i += 1

        if commit_index <= self.commit_index:
            return False

        self.commit_index = min(self.log.last_index, commit_index)

        return True

    def find_first_inconsistency(self, ours, theirs):
        for i, (our_entry, their_entry) in enumerate(zip(ours, theirs)):
            if our_entry[0] != their_entry[0]:
                return i
        return min(len(ours), len(theirs))

    def is_append_entries_valid(self, message: Msg):
        if message.prev_index > self.log.last_index:
            logger.debug(
                "Leader assumed we had log entry %d but we do not", message.prev_index,
            )
            return False

        if message.prev_index and self.log[message.prev_index][0] != message.prev_term:
            logger.warning(
                "Log not valid - mismatched terms %d and %d at index %d",
                message.prev_term,
                self.log[message.prev_index][0],
                message.prev_index,
            )
            return False

        return True

    def step(self, message):
        self.log.truncate_index = None

        if message.type != Message.Tick and message.type != Message.AppendEntriesReply:
            if message.type != Message.AppendEntries or len(message.entries) > 0:
                logger.debug(message.__dict__)

        self.step_term(message)

        if self.step_voting(message):
            return

        if message.type == Message.AppendEntries:
            if not self.is_append_entries_valid(message):
                self.reply(message, self.term, reject=True)
                return

            self._reset_election_tick()
            self.obedient = True

            self.leader = message.source

            # If leader sends us a batch of entries we already have we can avoid truncating
            # if they are actually consistent
            inconsistency_offset = self.find_first_inconsistency(
                self.log[message.prev_index + 1 :], message.entries
            )
            prev_index = message.prev_index + inconsistency_offset
            entries = message.entries[inconsistency_offset:]

            if self.log.last_index > prev_index:
                logger.error("Need to truncate log to recover quorum")
                if not self.log.truncate(prev_index):
                    return False

            for entry in entries:
                self.log.append((entry[0], entry[1]))

            if message.leader_commit > self.commit_index:
                commit_index = min(message.leader_commit, self.log.last_index)
                self.commit_index = commit_index

            self.reply(message, self.term, reject=False, log_index=self.log.last_index)

        if self.state == NodeState.FOLLOWER:
            self.step_follower(message)

        elif self.state == NodeState.PRE_CANDIDATE:
            self.step_pre_candidate(message)

        elif self.state == NodeState.CANDIDATE:
            self.step_candidate(message)

        elif self.state == NodeState.LEADER:
            self.step_leader(message)

    def step_term(self, message):
        if message.term == 0:
            # local message
            return

        if message.term > self.term:
            # If we have a leader we should ignore PreVote and Vote
            if message.type in (Message.PreVote, Message.Vote):
                if self.leader is not None and self.leader_active:
                    logger.debug("PreVote: sticky leader")
                    return

            # Never change term in response to prevote
            if message.type == Message.PreVote:
                logger.debug("PreVote: Not bumping term")
                return

            if message.type == Message.PreVoteReply and not message.reject:
                # We send pre-votes with a future term, when we become a
                # candidate we will actually apply it
                logger.debug("PreVote: not rejected; not bumping")
                return

            self._become_follower(message.term, message.source)

        elif message.term < self.term:
            if message.type == Message.PreVote:
                self.reply(message, term=self.term, reject=True)

            logger.debug("Message from old term")
            return

    def step_voting(self, message):
        if message.type != Message.Vote and message.type != Message.PreVote:
            return

        can_vote = self.can_vote(message)
        peer_current = self.peer_current(message.index, message.term)

        if not can_vote or not peer_current:
            logger.debug("Vote for %s rejected", message.source)
            self.reply(message, self.term, reject=True)
            return

        self.reply(message, self.term, reject=False)

        if message.type == Message.Vote:
            self._reset_election_tick()
            self.voted_for = message.source

    def step_follower(self, message):
        if message.type == Message.Tick:
            if self.tick_expired:
                self._become_pre_candidate()

    def step_pre_candidate(self, message):
        if message.type == Message.PreVoteReply:
            if not message.reject:
                self.vote_count += 1

            if self.vote_count >= self.quorum:
                self._become_candidate()
                return

        elif message.type == Message.Tick:
            if self.tick_expired:
                self._become_follower(self.term)

    def step_candidate(self, message):
        if message.type == Message.VoteReply:
            if not message.reject:
                self.vote_count += 1

            if self.vote_count >= self.quorum:
                self._become_leader()
                return

        elif message.type == Message.Tick:
            if self.tick_expired:
                self._become_follower(self.term)

    def step_leader(self, message):
        if message.type == Message.AddEntries:
            logger.critical("PROCESSING Message.AddEntries")
            for entry in message.entries:
                self.append(entry)
            self.broadcast_entries()

        elif message.type == Message.AppendEntriesReply:
            peer = self.peers[message.source]

            if message.reject:
                if peer.next_index > 1:
                    peer.next_index -= 1
                return

            peer.match_index = min(message.log_index, self.log.last_index)
            peer.next_index = peer.match_index + 1

            self.maybe_commit()

        elif message.type == Message.Tick:
            if self.tick_expired:
                for peer in self.peers.values():
                    self.send_heartbeat(peer)
                self._reset_heartbeat_tick()

    def broadcast_entries(self):
        for peer in self.peers.values():
            self.send_heartbeat(peer)
        self._reset_heartbeat_tick()

    def send_heartbeat(self, peer):
        # The biggest prev_index can be is last_index, so cap its size to that.
        # Though the question is, how does it end up bigger than last_index in the first place.
        prev_index = min(peer.next_index - 1, self.log.last_index)
        prev_term = self.log[prev_index][0] if prev_index >= 1 else 0
        entries = self.log[peer.next_index :]

        payload = {
            "prev_index": prev_index,
            "prev_term": prev_term,
            "entries": entries,
            "leader_commit": self.commit_index,
        }

        self.send(peer, Message.AppendEntries, self.term, **payload)
