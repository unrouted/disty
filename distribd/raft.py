import asyncio
import logging
import random

import aiohttp
from aiohttp import web
from aiohttp.abc import AbstractAccessLogger
from yarl import URL

from . import exceptions
from .machine import Machine, Message, Msg, NodeState
from .reducers import Reducers
from .seeding import Seeder
from .storage import Storage
from .utils.tick import Tick
from .utils.web import run_server

logger = logging.getLogger(__name__)


class RaftAccessLog(AbstractAccessLogger):
    def log(self, request, response, time):
        pass


class Raft:
    def __init__(self, config, machine: Machine, storage: Storage, reducers: Reducers):
        self.config = config
        self.machine = machine
        self.storage = storage
        self.reducers = reducers
        self.queue = asyncio.Queue()
        self.peers = None

        self._closed = False

        self._ticker = Tick(self._tick)

    async def append(self, entries):
        if self.machine.state == NodeState.LEADER:
            index, term = await self._append_local(entries)
        else:
            index, term = await self._append_remote(entries)

        return await self.reducers.wait_for_commit(term, index)

    async def _append_local(self, entries):
        logger.critical("_append_local: %s", self.machine.identifier)
        f = asyncio.Future()
        await self.queue.put(
            (
                {
                    "type": str(Message.AddEntries),
                    "source": self.machine.identifier,
                    "destination": self.machine.identifier,
                    "term": 0,
                    "entries": entries,
                },
                f,
            )
        )
        await f
        return self.machine.log.last_index, self.machine.log.last_term

    async def close(self):
        self._closed = True

    def _tick(self):
        self.queue.put_nowait(
            (
                {
                    "type": str(Message.Tick),
                    "source": self.machine.identifier,
                    "destination": self.machine.identifier,
                    "term": 0,
                },
                None,
            )
        )

    def state_changed(self, **discovery_info):
        self.queue.put_nowait(
            (
                {
                    "type": str(Message.StateChanged),
                    "source": self.machine.identifier,
                    "destination": self.machine.identifier,
                    "term": 0,
                    "discovery_info": discovery_info,
                },
                None,
            )
        )

    async def step(self, raw_msg):
        msg = Msg.from_dict(raw_msg)

        self.machine.step(msg)

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

        await self.reducers.step(self.machine)

        self.peers.step(self.machine, msg)

    async def _process_queue(self):
        task_complete = None

        self._ticker.reschedule(self.machine.next_tick)

        while not self._closed:
            try:
                payload = await self.queue.get()
                msg, task_complete = payload

                await self.step(msg)
                self._ticker.reschedule(self.machine.next_tick)

            except asyncio.CancelledError as e:
                if task_complete:
                    task_complete.set_exception(e)
                return

            except Exception as e:
                logger.exception("Unhandled error processing incoming message")
                if task_complete:
                    task_complete.set_exception(e)

            else:
                if task_complete:
                    task_complete.set_result(self.machine)

            self.queue.task_done()

    async def run_forever(self):
        queue_worker = self._process_queue()
        listener = self._run_listener()

        try:
            return await asyncio.gather(queue_worker, listener)
        finally:
            await self.close()

    async def send(self, message: Msg):
        raise NotImplementedError(self.send)

    async def _append_remote(self, entries):
        raise NotImplementedError(self.send)


class HttpRaft(Raft):
    def __init__(self, config, machine: Machine, storage: Storage, reducers: Reducers):
        super().__init__(config, machine, storage, reducers)
        self.session = aiohttp.ClientSession()
        self.peers = Seeder(config, self.storage.session, self.spread)

    def url_for_peer(self, peer):
        address = self.peers[peer]["raft"]["address"]
        port = self.peers[peer]["raft"]["port"]
        return URL.build(host=address, port=port)

    async def _append_remote(self, entries):
        logger.critical("_append_remote: %s", self.machine.identifier)

        if not self.machine.leader:
            logger.critical(
                "_append_remote: no leader: %s %s %s",
                self.machine.identifier,
                self.machine.leader,
                self.machine.leader_active,
            )
            raise exceptions.LeaderUnavailable()

        url = self.url_for_peer(self.machine.leader)
        async with self.session.post(url / "append", json=entries) as resp:
            if resp.status != 200:
                logger.critical(resp.status, await resp.text())
                raise RuntimeError("Remote append failed")
            payload = await resp.json()
            return payload["index"], payload["term"]

    async def send(self, message: Msg):
        if message.destination not in self.peers:
            # We don't know where this peer is, drop the message
            return

        body = message.to_dict()

        try:
            url = self.url_for_peer(message.destination)
            async with self.session.post(url / "rpc", json=body) as resp:
                if resp.status != 200:
                    logger.debug("Message rejected")
        except aiohttp.ClientError:
            # Message wasn't delivered - client broken or netsplit
            pass

    async def spread(self, gossip):
        async with aiohttp.ClientSession() as session:
            url = URL(random.choice(self.config["seeding"]["urls"].get(list)))
            try:
                async with session.post(url / "gossip", json=gossip) as resp:
                    if resp.status == 200:
                        return await resp.json()
            except aiohttp.ClientError:
                pass
        return {}

    async def _receive_message(self, request):
        payload = await request.json()
        await self.queue.put((payload, None))
        return web.json_response({})

    async def _receive_append(self, request):
        entries = await request.json()
        if self.machine.state != NodeState.LEADER:
            raise exceptions.LeaderUnavailable()
        index, term = await self._append_local(entries)
        return web.json_response({"index": index, "term": term})

    async def _receive_gossip(self, request):
        gossip = await request.json()
        reply = self.peers.exchange_gossip(gossip)
        return web.json_response(reply)

    async def _receive_status(self, request):
        stable = self.machine.log.last_index == self.reducers.applied_index
        stable = stable and self.machine.leader_active
        stable = stable and (
            self.machine.state == NodeState.LEADER or self.machine.leader is not None
        )

        payload = {
            "state": self.machine.state,
            "log_last_index": self.machine.log.last_index,
            "log_last_term": self.machine.log.last_term,
            "applied_index": self.reducers.applied_index,
            "committed_index": self.machine.commit_index,
            # No unapplied log entries
            "stable": stable,
            # DEPRECATED
            "consensus": self.machine.leader_active,
        }

        return web.json_response(payload)

    async def _run_listener(self):
        routes = web.RouteTableDef()
        routes.post("/rpc")(self._receive_message)
        routes.post("/append")(self._receive_append)
        routes.post("/gossip")(self._receive_gossip)
        routes.get("/status")(self._receive_status)

        return await run_server(
            self, "raft", self.config["raft"], routes, access_log_class=RaftAccessLog,
        )

    async def close(self):
        await super().close()
        await self.peers.close()
        await self.session.close()
