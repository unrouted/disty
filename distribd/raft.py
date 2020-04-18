import asyncio
import logging

import aiohttp
from aiohttp import web
from aiohttp.abc import AbstractAccessLogger

from .config import config
from .machine import Machine, Message
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
    url = config[message.destination]["raft_url"]
    body = message.to_dict()

    async with session.post(f"{url}/rpc", json=body) as resp:
        if resp.status != 200:
            logger.debug("Message rejected")


async def process_queue(machine: Machine, queue: asyncio.Queue):
    async with aiohttp.ClientSession() as session:
        while True:
            msg = await queue.get()

            machine.step(Message(**msg))

            # await storage.step(machine)

            if machine.outbox:
                aws = []
                for message in machine.outbox:
                    aws.append(send_message(session, message))

                for future in aws.as_completed(aws):
                    try:
                        await future()
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        logger.exception("Unhandled error while sending message")

                machine.outbox = []

            queue.task_done()


async def run_raft_forever(machine: Machine, port: int):
    queue = asyncio.Queue()

    queue_worker = asyncio.ensure_future(process_queue(machine, queue))
    server = asyncio.ensure_future(
        run_server("127.0.0.1", port, routes, queue=queue, machine=machine)
    )

    return await asyncio.gather(server, queue_worker)
