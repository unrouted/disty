import asyncio
import logging

import aiohttp
from aiohttp import web
from aiohttp.abc import AbstractAccessLogger

from . import config
from .machine import Machine, Message, Msg
from .utils.web import run_server

logger = logging.getLogger(__name__)


class RaftAccessLog(AbstractAccessLogger):
    def log(self, request, response, time):
        pass


routes = web.RouteTableDef()


@routes.post("/rpc")
async def append_entries(request):
    queue: asyncio.Queue = request.app["queue"]

    await queue.put(await request.json())
    return web.json_response({})


@routes.get("/status")
async def status(request):
    machine: Machine = request.app["machine"]

    payload = {
        "status": machine.state,
        "log_last_index": machine.log.last_index,
        "log_last_term": machine.log.last_term,
        # "applied_index": machine.log.applied_index,
        "committed_index": machine.commit_index,
        "consensus": machine.leader_active,
    }

    return web.json_response(payload)


async def send_message(session: aiohttp.ClientSession, message: Message):
    url = config.config[message.destination]["raft_url"]
    body = message.to_dict()

    async with session.post(f"{url}/rpc", json=body) as resp:
        if resp.status != 200:
            logger.debug("Message rejected")


async def process_queue(machine: Machine, storage, queue: asyncio.Queue):
    async with aiohttp.ClientSession() as session:
        while True:
            msg = await queue.get()

            try:
                machine.step(Msg.from_dict(msg))

                await storage.step(machine)

                if machine.outbox:
                    aws = []
                    for message in machine.outbox:
                        aws.append(send_message(session, message))

                    for future in asyncio.as_completed(aws):
                        try:
                            await future

                        except asyncio.CancelledError:
                            raise

                        except aiohttp.ClientError:
                            # Message wasn't delivered - client broken or netsplit
                            pass

                        except Exception:
                            logger.exception("Unhandled error while sending message")

                    machine.outbox = []
            except Exception:
                logger.exception("Unhandled error processing incoming message")

            queue.task_done()


async def ticker(machine, queue):
    while True:
        await queue.put(
            {
                "type": str(Message.Tick),
                "source": machine.identifier,
                "destination": machine.identifier,
                "term": 0,
            }
        )
        await asyncio.sleep(0.1)


async def run_raft_forever(machine: Machine, storage, port: int):
    queue = asyncio.Queue()

    ticker_fn = ticker(machine, queue)

    queue_worker = process_queue(machine, storage, queue)

    server = run_server(
        "127.0.0.1",
        port,
        routes,
        access_log_class=RaftAccessLog,
        queue=queue,
        machine=machine,
    )

    return await asyncio.gather(ticker_fn, server, queue_worker)
