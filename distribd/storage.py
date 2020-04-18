import asyncio
import json
import logging
import os

from aiofile import AIOFile, Writer

from .machine import Machine

logger = logging.getLogger(__name__)


class Storage:
    def __init__(self, path):
        self._path = path
        self._term_path = path.parent / "term"

        self.snapshot = None
        self.snapshot_index = 0
        self.snapshot_term = 0

        self.current_term = 0

        # Entries that are safely committed
        self.applied_index = 0

        # You cannot make changes to the log without holding the commit lock.
        # This ensures the txn log on disk and in memory is actually in the same order.
        self._commit_lock = asyncio.Lock()

        # aiofile requests that these are created within an async context
        self._fp: AIOFile = None
        self._writer: Writer = None

        self._log = []

        # Functions to call with changes that are safe to apply to replicated data structures.
        self._callbacks = []

        # List of (commit_index, event)
        self._waiters = []

    async def step(self, machine: Machine):
        aws = []

        if machine.term > self.current_term:
            aws.append(self.set_term(machine.term))

        if not aws:
            return

        await asyncio.gather(aws)

    async def set_term(self, term):
        async with self._commit_lock:
            if term <= self.current_term:
                return

            async with AIOFile(self._term_path, "w") as fp:
                w = Writer(fp)
                await w(json.dumps(term))
                await fp.fsync()

            self.current_term = term

    def add_reducer(self, callback):
        self._callbacks.append(callback)

    @property
    def last_term(self):
        if self._log:
            return self._log[-1][0]
        return self.snapshot_term

    @property
    def last_index(self):
        return self.snapshot_index + len(self._log)

    def read_term(self):
        if not os.path.exists(self._term_path):
            return
        with open(self._term_path, "r") as fp:
            self.current_term = json.load(fp)
            logger.debug(f"Restored persisted term: %s", self.current_term)

    async def read_log(self):
        if not os.path.exists(self._path):
            return

        self._log = []

        with open(self._path, "r") as fp:
            for i, line in enumerate(fp):
                try:
                    payload = json.loads(line)
                except Exception:
                    logger.exception("Corrupt journal at line %d", i + 1)
                    # FIXME: Take a copy of journal at truncate at this point?
                    return

                self._log.append(tuple(payload))

        logger.info("Restored to term: %d index: %d", self.last_term, self.last_index)

        if self.last_term > self.current_term:
            logger.warning("Journal is ahead of persisted term - fixing")
            await self.set_term(self.last_term)

    async def open(self):
        self.read_term()
        await self.read_log()

        if not self._path.parent.exists():
            os.makedirs(self._path.parent)

        self._fp = AIOFile(self._path, "a+")
        await self._fp.open()

        self._writer = Writer(self._fp)

    async def close(self):
        if self._fp:
            await self._fp.close()
            self._fp = None

    async def rollback(self, last_index):
        """Drop all records after index and commit to disk."""
        if last_index < self.snapshot_index:
            logger.warning(
                "Cannot rollback as rollback position is inside most recent snapshot"
            )
            return False

        async with self._commit_lock:
            await self.close()

            while self.last_index > last_index:
                del self._log[-1]

            async with AIOFile(self._path, "w") as fp:
                writer = Writer(fp)
                for row in self._log:
                    await writer(json.dumps(row) + "\n")
                await fp.fsync()

            await self.open()

        return True

    async def snapshot(self):
        raise NotImplementedError(self.snapshot)

    async def commit(self, term, entry):
        record = [term, entry]

        async with self._commit_lock:
            await self._writer(json.dumps(record) + "\n")
            await self._fp.fsync()

            self._log.append(record)

        logger.debug("Committed term %d index %d", self.last_term, self.last_index)

    async def apply(self, index):
        """Indexes up to `index` can now be applied to the state machine."""
        logger.critical("Safe to apply log up to index %d", index)

        entries = self._log[self.applied_index : index]

        for callback in self._callbacks:
            callback(entries)

        waiters = []
        for waiter_index, ev in self._waiters:
            if waiter_index <= index:
                ev.set()
                continue
            waiters.append((waiter_index, ev))
        self._waiters = waiters

        logger.debug("Applied index %d", index)
        self.applied_index = index

    def __getitem__(self, key):
        if isinstance(key, slice):
            new_slice = slice(
                key.start - 1 if key.start else None,
                key.stop - 1 if key.stop else None,
                key.step,
            )
            return self._log[new_slice]

        return self._log[key - 1]

    async def wait_for_commit(self, term, index):
        ev = asyncio.Event()
        self._waiters.append((index, ev))
        await ev.wait()
        return self[index][0] == term
