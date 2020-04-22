import asyncio
import logging

logger = logging.getLogger(__name__)


class Seeder:
    def __init__(self, config, spread):
        self.config = config
        self.spread = spread
        self.identifier = config["node"]["identifier"].get(str)

        self.nodes = {
            self.identifier: {
                "raft": {
                    "address": config["raft"]["address"].get(str),
                    "port": config["raft"]["port"].get(int),
                },
                "registry": {
                    "address": config["registry"]["address"].get(str),
                    "port": config["registry"]["port"].get(int),
                },
                "generation": 0,
            }
        }

        self.is_gossiping = False

        self._task = None

    @property
    def generation(self):
        return self.nodes[self.identifier]["generation"]

    async def _gossip(self):
        while True:
            resp = await self.spread(self.nodes)
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
            if node not in self.nodes:
                self.nodes[node] = rumour
                continue

            if rumour["generation"] > self.nodes[node]["generation"]:
                if node == self.identifier:
                    # They have better gossip about us that we do about then??
                    # That can't be true so ratchet our generation
                    # NOTE: Of course, if 2 nodes with same identifier...
                    self.nodes[node]["generation"] = rumour["generation"] + 1
                    stale.add(self.identifier)
                else:
                    # Not us, just update the gossip
                    self.nodes[node] = rumour
                continue

            stale.add(node)

        return stale

    def exchange_gossip(self, gossip):
        stale = self.update_from_gossip(gossip)

        for node, rumour in self.nodes.items():
            if node not in gossip:
                stale.add(node)

        response = {}
        for node in stale:
            response[node] = self.nodes[node]

        return response

    def all_peers_known(self):
        for node in self.config["peers"].get(list):
            if node not in self.nodes:
                return False
        return True

    def should_gossip(self, machine):
        if not self.all_peers_known():
            return True

        if not machine.leader_active:
            return True

        return False

    def step(self, machine):
        if self.should_gossip(machine) and not self.is_gossiping:
            logger.debug("Starting gossiping")
            self.start_gossiping()

        elif not self.should_gossip(machine) and self.is_gossiping:
            logger.debug("Stopping gossiping")
            self.stop_gossiping()

    def __contains__(self, key):
        return key in self.nodes

    def __getitem__(self, key):
        return self.nodes[key]
