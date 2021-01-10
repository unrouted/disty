import asyncio
import json
import logging
import os
import pathlib

from aiofile import async_open

from .machine import Machine

logger = logging.getLogger(__name__)


class Storage:
    def __init__(self, path: pathlib.Path):
        self._path = path
        self._term_path = path.parent / "term"
        self._session_path = path.parent / "session"

        self.session = 0

        self.snapshot = None
        self.snapshot_index = 0
        self.snapshot_term = 0

        self.current_term = 0

        # You cannot make changes to the log without holding the commit lock.
        # This ensures the txn log on disk and in memory is actually in the same order.
        self._commit_lock = asyncio.Lock()

        # aiofile requests that these are created within an async context
        self._fp = None

        self.log = []

    async def step(self, machine: Machine):
        aws = []

        if machine.term > self.current_term:
            aws.append(self.write_term(machine.term))

        if machine.log.snapshot_index != self.snapshot_index:
            aws.append(self.write_snapshot(machine.term))

        if machine.log.truncate_index or machine.log.last_index != self.last_index:
            aws.append(self.write_journal(machine))

        if not aws:
            return

        await asyncio.gather(*aws)

    async def write_term(self, term):
        async with self._commit_lock:
            if term <= self.current_term:
                return

            async with async_open(self._term_path, "w") as fp:
                await fp.write(json.dumps(term))
                await fp.file.fsync()

            self.current_term = term

    async def write_snapshot(self, snapshot_index, snapshot_term, snapshot):
        # FIXME: Write to disk
        self.snapshot = snapshot
        self.snapshot_index = snapshot_index
        self.snapshot_term

    async def write_journal(self, machine: Machine):
        if machine.log.truncate_index is not None:
            await self.rollback(machine.log.truncate_index)

        while self.last_index < machine.log.last_index:
            logger.critical(
                "write_journal %s %s %s",
                machine.identifier,
                machine.log.last_index,
                self.last_index,
            )
            term, entry = machine.log[self.last_index + 1]
            await self.commit(term, entry)

    @property
    def last_term(self):
        if self.log:
            return self.log[-1][0]
        return self.snapshot_term

    @property
    def last_index(self):
        return self.snapshot_index + len(self.log)

    def read_term(self):
        if not os.path.exists(self._term_path):
            return
        with open(self._term_path, "r") as fp:
            self.current_term = json.load(fp)
            logger.debug(f"Restored persisted term: %s", self.current_term)

    async def read_log(self):
        if not os.path.exists(self._path):
            return

        self.log = []

        with open(self._path, "r") as fp:
            for i, line in enumerate(fp):
                try:
                    payload = json.loads(line)
                except Exception:
                    logger.exception("Corrupt journal at line %d", i + 1)
                    # FIXME: Take a copy of journal at truncate at this point?
                    return

                self.log.append(tuple(payload))

        logger.info("Restored to term: %d index: %d", self.last_term, self.last_index)

        if self.last_term > self.current_term:
            logger.warning("Journal is ahead of persisted term - fixing")
            await self.write_term(self.last_term)

    async def read_session(self):
        if not os.path.exists(self._session_path):
            return
        async with async_open(self._session_path, "r") as afp:
            self.session = json.loads(await afp.read())

    async def write_session(self):
        async with async_open(self._session_path, "w") as afp:
            await afp.write(json.dumps(self.session))
            await afp.file.fsync()

    async def open(self):
        self.read_term()
        await self.read_log()

        if not self._path.parent.exists():
            os.makedirs(self._path.parent)

        await self.read_session()
        self.session += 1
        await self.write_session()

        self._path.touch()
        self._fp = async_open(self._path, "a+")
        await self._fp.file.open()

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
                del self.log[-1]

            async with async_open(self._path, "w") as fp:
                for row in self.log:
                    await fp.write(json.dumps(row) + "\n")
                await fp.file.fsync()

            await self.open()

        return True

    async def snapshot(self):
        raise NotImplementedError(self.snapshot)

    async def commit(self, term, entry):
        record = [term, entry]

        async with self._commit_lock:
            await self._fp.write(json.dumps(record) + "\n")
            await self._fp.file.fsync()

            self.log.append(record)

        logger.debug("Committed term %d index %d", self.last_term, self.last_index)

    def __getitem__(self, key):
        if isinstance(key, slice):
            new_slice = slice(
                key.start - 1 if key.start else None,
                key.stop - 1 if key.stop else None,
                key.step,
            )
            return self.log[new_slice]

        return self.log[key - 1]
