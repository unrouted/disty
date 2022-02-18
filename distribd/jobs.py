import asyncio
import logging

from aiojobs import Scheduler
from aiojobs._job import Job

logger = logging.getLogger(__name__)


class WorkerPool(Scheduler):
    def __init__(self, limit=10):
        super().__init__(
            close_timeout=0.1,
            limit=limit,
            pending_limit=10000,
            exception_handler=None,
        )

    def spawn(self, coro):
        if self._closed:
            return
        job = Job(coro, self, self._loop)
        should_start = self._limit is None or self.active_count < self._limit
        self._jobs.add(job)
        if should_start:
            job._start()
        else:
            # wait for free slot in queue
            self._pending.put_nowait(job)
