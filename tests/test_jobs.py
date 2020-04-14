import asyncio

from distribd.jobs import WorkerPool


async def test_active():
    pool = WorkerPool()
    assert pool.active == 0


async def test_invocation():
    ev = asyncio.Event()

    async def setter():
        ev.set()

    pool = WorkerPool()
    pool.spawn(setter())
    await ev.wait()
    await pool.close()


async def test_exception():
    ev = asyncio.Event()

    async def thrower():
        try:
            raise RuntimeError("Test exception")
        finally:
            ev.set()

    pool = WorkerPool()
    pool.spawn(thrower())
    await ev.wait()
    await pool.close()
