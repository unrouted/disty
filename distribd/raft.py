import asyncio
import logging

import aiohttp
from aiohttp import web
from aiohttp.abc import AbstractAccessLogger

from . import config
from .machine import Machine, Message, Msg
from .reducers import Reducers
from .storage import Storage
from .utils.web import run_server

logger = logging.getLogger(__name__)


class RaftAccessLog(AbstractAccessLogger):
    def log(self, request, response, time):
        pass


class Raft:
    def __init__(self, machine: Machine, storage: Storage, reducers: Reducers):
        self.machine = machine
        self.storage = storage
        self.reducers = reducers
        self.queue = asyncio.Queue()

    async def append(self, entry):
        pass

    async def close(self):
        pass

    async def _process_queue(self):
        while True:
            msg = await self.queue.get()

            try:
                self.machine.step(Msg.from_dict(msg))

                await self.storage.step(self.machine)

                if self.machine.outbox:
                    aws = []
                    for message in self.machine.outbox:
                        aws.append(self.send(message))

                    for future in asyncio.as_completed(aws):
                        try:
                            await future

                        except asyncio.CancelledError:
                            raise

                        except Exception:
                            logger.exception("Unhandled error while sending message")

                    self.machine.outbox = []

                await self.reducers.step(self.machine)

            except Exception:
                logger.exception("Unhandled error processing incoming message")

            self.queue.task_done()

    async def _ticker(self):
        while True:
            await self.queue.put(
                {
                    "type": str(Message.Tick),
                    "source": self.machine.identifier,
                    "destination": self.machine.identifier,
                    "term": 0,
                }
            )
            await asyncio.sleep(0.1)

    async def run_forever(self, port: int):
        ticker_fn = self._ticker()
        queue_worker = self._process_queue()
        listener = self._run_listener(port)

        try:
            return await asyncio.gather(ticker_fn, queue_worker, listener)
        finally:
            await self.close()

    async def send(self, message: Msg):
        raise NotImplementedError(self.send)

    async def _append_remote(self, message: Msg):
        raise NotImplementedError(self.send)


class HttpRaft(Raft):
    def __init__(self, machine: Machine, storage: Storage, reducers: Reducers):
        super().__init__(machine, storage, reducers)
        self.session = aiohttp.ClientSession()

    async def _append_remote(self, entry):
        if not self.machne.leader:
            raise RuntimeError("No leader")

        url = config.config[self.machine.leader]["raft_url"]

        async with self.session.post(f"{url}/append", json=entry) as resp:
            if resp.status != 200:
                raise RuntimeError("Remote append failed")
            payload = await resp.json()
            return payload["index"], payload["term"]

    async def send(self, message: Msg):
        url = config.config[message.destination]["raft_url"]
        body = message.to_dict()

        try:
            async with self.session.post(f"{url}/rpc", json=body) as resp:
                if resp.status != 200:
                    logger.debug("Message rejected")
        except aiohttp.ClientError:
            # Message wasn't delivered - client broken or netsplit
            pass

    async def _receive_message(self, request):
        await self.queue.put(await request.json())
        return web.json_response({})

    async def _receive_append(self, request):
        entry = await request.json()

        await self.queue.put(entry)
        return web.json_response({})

    async def _receive_status(self, request):
        payload = {
            "status": self.machine.state,
            "log_last_index": self.machine.log.last_index,
            "log_last_term": self.machine.log.last_term,
            "applied_index": self.reducers.applied_index,
            "committed_index": self.machine.commit_index,
            "consensus": self.machine.leader_active,
        }

        return web.json_response(payload)

    async def _run_listener(self, port):
        routes = web.RouteTableDef()
        routes.post("/rpc")(self._receive_message)
        routes.post("/append")(self._receive_append)
        routes.get("/status")(self._receive_status)

        return await run_server(
            "127.0.0.1", port, routes, access_log_class=RaftAccessLog,
        )

    async def close(self):
        await super().close()
        await self.session.close()
