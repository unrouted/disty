import asyncio
import logging

from .machine import Machine

logger = logging.getLogger(__name__)


class Reducers:
    def __init__(self, machine: Machine):
        # Entries that are safely committed
        self.applied_index = 0

        # Functions to call with changes that are safe to apply to replicated data structures.
        self._callbacks = []

        # List of (commit_index, event)
        self._waiters = []

        self.machine = machine

    def add_reducer(self, callback):
        self._callbacks.append(callback)

    async def step(self, machine: Machine):
        """Indexes up to `commit_index` can now be applied to the state machine."""
        if self.applied_index >= machine.commit_index:
            return

        logger.critical("Safe to apply log up to index %d", machine.commit_index)

        entries = self.machine.log[self.applied_index : machine.commit_index]

        for callback in self._callbacks:
            callback(entries)

        waiters = []
        for waiter_index, ev in self._waiters:
            if waiter_index <= machine.commit_index:
                ev.set()
                continue
            waiters.append((waiter_index, ev))
        self._waiters = waiters

        logger.debug("Applied index %d", machine.commit_index)
        self.applied_index = machine.commit_index

    async def wait_for_commit(self, term, index):
        ev = asyncio.Event()
        self._waiters.append((index, ev))
        await ev.wait()
        return self[index][0] == term
