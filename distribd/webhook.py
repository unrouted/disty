import asyncio
import itertools
import logging
import os
import re

import aiohttp
import ujson

from .jobs import WorkerPool

logger = logging.getLogger(__name__)


class WebhookManager:
    def __init__(self):
        self.pool = WorkerPool()
        self.session = aiohttp.ClientSession(json_serialize=ujson.dumps)

        self.webhooks = []
        self.load_targets()

    def load_targets(self):
        self.webhooks.clear()

        for i in itertools.count():
            url = os.environ.get(f"WEBHOOK_{i}_URL", None)
            if url is None:
                break

            regex = re.compile(os.environ.get(f"WEBHOOK_{i}_REGEX", ".*"))

            self.webhooks.append({"url": url, "matcher": regex})

    async def _send_webhook(self, url, event):
        try:
            async with self.session.post(url, json={"events": [event]}) as resp:
                if resp.status == 200:
                    return

        except asyncio.CancelledError:
            pass

        except Exception:
            logger.exception("Unhandled error whilst processing webhook: %s", url)

        logger.info("Scheduling retry for webhook %s", url)
        loop = asyncio.get_event_loop()
        loop.call_later(30, lambda: self.pool.spawn(self._send_webhook, url, event))

    def send(self, event):
        for webhook in self.webhooks:
            value = ":".join((event["target"]["repository"], event["target"]["tag"]))
            if not webhook["matcher"].matches(value):
                continue
            self.pool.spawn(self._send_webhook(webhook["url"], event))

    async def close(self):
        await self.pool.close()
        await self.session.close()
