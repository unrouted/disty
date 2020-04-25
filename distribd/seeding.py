import asyncio
import logging

from .machine import Machine, Message, Msg

logger = logging.getLogger(__name__)


class Seeder:
    def __init__(self, config, session, spread):
        self.config = config
        self.spread = spread
        self.identifier = config["node"]["identifier"].get(str)

        self.current_state = {"generation": session}
        self.peers = {}

        self.is_gossiping = False

        self._task = None

    async def close(self):
        task = self._task
        if task:
            self.stop_gossiping()
            try:
                await task
            except asyncio.CancelledError:
                pass

    @property
    def current_gossip(self):
        gossip = {self.identifier: self.current_state}
        gossip.update(self.peers)
        return gossip

    async def _gossip(self):
        while True:
            resp = await self.spread(self.current_gossip)
            # We use a boot-count aware merge function in
            # case we have got tastier gossip in the meantime
            self.update_from_gossip(resp)

            await asyncio.sleep(0.1)

    def start_gossiping(self):
        self._task = asyncio.ensure_future(self._gossip())
        self.is_gossiping = True

        def finalizer(fut):
            self.is_gossiping = False
            try:
                fut.result()
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception("Unhandled error whilst gossiping")

        self._task.add_done_callback(finalizer)

    def stop_gossiping(self):
        self._task.cancel()

    def update_from_gossip(self, gossip):
        """
        Merges gossip from a peer with our own.

        Returns a set() of nodes where we have better gossip.
        """
        stale = set()

        for node, rumour in gossip.items():
            if node == self.identifier:
                if rumour["generation"] > self.current_state["generation"]:
                    # They have better gossip about us that we do about then??
                    # That can't be true so ratchet our generation
                    # NOTE: Of course, if 2 nodes with same identifier...
                    logger.warning(
                        "Our generation is older than another peer with same identifier"
                    )
                continue

            if node not in self.peers:
                self.peers[node] = rumour
                continue

            if rumour["generation"] > self.peers[node]["generation"]:
                self.peers[node] = rumour
                continue

            stale.add(node)

        return stale

    def exchange_gossip(self, gossip):
        stale = self.update_from_gossip(gossip)

        for node, rumour in self.peers.items():
            if node not in gossip:
                stale.add(node)

        response = {}
        if self.current_state_valid():
            response[self.identifier] = self.current_state
        for node in stale:
            response[node] = self.peers[node]

        return response

    def all_peers_known(self):
        for node in self.config["peers"].get(list):
            if node not in self.peers:
                return False
        return True

    def current_state_valid(self):
        if "raft" not in self.current_state:
            return False
        if "registry" not in self.current_state:
            return False
        return True

    def should_gossip(self, machine):
        if not self.current_state_valid():
            return False

        if not self.all_peers_known():
            return True

        if not machine.leader_active:
            return True

        return False

    def process_state_change(self, discovery_info):
        if "raft" in discovery_info:
            raft = self.current_state.setdefault("raft", {})
            raft.update(discovery_info["raft"])

        if "registry" in discovery_info:
            registry = self.current_state.setdefault("registry", {})
            registry.update(discovery_info["registry"])

    def step(self, machine: Machine, msg: Msg):
        if msg.type == Message.StateChanged:
            self.process_state_change(msg.discovery_info)

        if self.should_gossip(machine) and not self.is_gossiping:
            logger.debug("Starting gossiping")
            self.start_gossiping()

        elif not self.should_gossip(machine) and self.is_gossiping:
            logger.debug("Stopping gossiping")
            self.stop_gossiping()

        logger.debug("%s %s %s", self.identifier, self.current_state, self.peers)

    def __contains__(self, key):
        return key in self.peers

    def __getitem__(self, key):
        return self.peers[key]
