import asyncio
import logging
import re

import aiohttp
import ujson

from .jobs import WorkerPool

logger = logging.getLogger(__name__)


class WebhookManager:
    def __init__(self, config):
        self.config = config

        self.pool = WorkerPool()
        self.session = aiohttp.ClientSession(json_serialize=ujson.dumps)

        self.webhooks = []
        self.load_targets()

    def load_targets(self):
        self.webhooks.clear()

        if "webhooks" not in self.config:
            return

        for webhook in self.config["webhooks"].get(list):
            url = webhook["url"]
            regex = re.compile(webhook.get("matcher", ".*"))
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
        loop.call_later(30, lambda: self.pool.spawn(self._send_webhook(url, event)))

    def send(self, event):
        for webhook in self.webhooks:
            value = ":".join((event["target"]["repository"], event["target"]["tag"]))
            if webhook["matcher"].search(value) is None:
                continue
            self.pool.spawn(self._send_webhook(webhook["url"], event))

    async def close(self):
        await self.pool.close()
        await self.session.close()
