import asyncio
import logging

logger = logging.getLogger(__name__)


class WorkerPool:
    def __init__(self):
        self._jobs = set()

    @property
    def active(self):
        return len(self._jobs)

    def spawn(self, coro):
        task = asyncio.ensure_future(coro)

        def done_callback(future):
            self._jobs.discard(task)

            try:
                future.result()
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception("Unhandled exception in async task")

        task.add_done_callback(done_callback)

        self._jobs.add(task)
        return task

    async def close(self):
        [task.cancel() for task in self._jobs]

        for future in asyncio.as_completed(self._jobs):
            try:
                await future
            except asyncio.CancelledError:
                pass
            except Exception:
                logging.exception("Unhandled error whilst closing worker pool")
