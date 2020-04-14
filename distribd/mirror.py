import hashlib
import logging
import os
import random
import uuid

import aiohttp

from . import config
from .actions import RegistryActions
from .jobs import WorkerPool
from .state import Reducer
from .utils.registry import get_blob_path, get_manifest_path

logger = logging.getLogger(__name__)


class Mirrorer(Reducer):
    def __init__(self, image_directory, identifier, send_action):
        self.image_directory = image_directory
        self.identifier = identifier
        self.send_action = send_action

        self.blob_locations = {}
        self.blob_repos = {}
        self.manifest_locations = {}
        self.manifest_repos = {}

        self.pool = WorkerPool()

    async def close(self):
        await self.pool.close()

    async def _do_transfer(self, hash, urls, destination):
        if destination.exists():
            logger.debug("%s already exists, not requesting", destination)
            return

        url = random.choice(urls)
        logger.critical("Starting download from %s to %s", url, destination)

        if not destination.parent.exists():
            os.makedirs(destination.parent)

        temporary_path = self.image_directory / "uploads" / str(uuid.uuid4())
        if not temporary_path.parent.exists():
            os.makedirs(temporary_path.parent)

        digest = hashlib.sha256()

        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                if resp.status != 200:
                    logger.error("Failed to retrieve: %s, status %s", url, resp.status)
                    return False
                # FIXME: Use aiofile
                with open(temporary_path, "wb") as fp:
                    chunk = await resp.content.read(1024 * 1024)
                    while chunk:
                        fp.write(chunk)
                        digest.update(chunk)
                        chunk = await resp.content.read(1024 * 1024)

        if digest.hexdigest() != hash:
            os.unlink(destination)
            return False

        os.rename(temporary_path, destination)

        return True

    def should_download_blob(self, hash):
        if hash not in self.blob_repos:
            return False

        if hash not in self.blob_locations:
            return False

        locations = self.blob_locations[hash]

        if len(locations) == 0:
            return False

        if self.identifier in locations:
            return False

        return True

    def urls_for_blob(self, hash):
        repo = next(iter(self.blob_repos[hash]))
        locations = [
            config.config[l]["registry_url"] for l in self.blob_locations[hash]
        ]

        return [f"{location}/v2/{repo}/blobs/sha256:{hash}" for location in locations]

    async def do_download_blob(self, hash):
        if not self.should_download_blob(hash):
            return

        destination = get_blob_path(self.image_directory, hash)
        if not await self._do_transfer(hash, self.urls_for_blob(hash), destination):
            return

        await self.send_action(
            [
                {
                    "type": RegistryActions.BLOB_STORED,
                    "hash": hash,
                    "location": self.identifier,
                }
            ]
        )

    def should_download_manifest(self, hash):
        if hash not in self.manifest_repos:
            return False

        if hash not in self.manifest_locations:
            return False

        locations = self.manifest_locations[hash]

        if len(locations) == 0:
            return False

        if self.identifier in locations:
            return False

        return True

    def urls_for_manifest(self, hash):
        repo = next(iter(self.manifest_repos[hash]))
        locations = [
            config.config[l]["registry_url"] for l in self.manifest_locations[hash]
        ]
        return [
            f"{location}/v2/{repo}/manifests/sha256:{hash}" for location in locations
        ]

    async def do_download_manifest(self, hash):
        if not self.should_download_manifest(hash):
            return

        destination = get_manifest_path(self.image_directory, hash)
        if not await self._do_transfer(hash, self.urls_for_manifest(hash), destination):
            return

        await self.send_action(
            [
                {
                    "type": RegistryActions.MANIFEST_STORED,
                    "hash": hash,
                    "location": self.identifier,
                }
            ]
        )

    def dispatch(self, entry):
        if entry["type"] == RegistryActions.BLOB_STORED:
            blob = self.blob_locations.setdefault(entry["hash"], set())
            blob.add(entry["location"])

            if self.should_download_blob(entry["hash"]):
                self.pool.spawn(self.do_download_blob(entry["hash"]))

        elif entry["type"] == RegistryActions.BLOB_MOUNTED:
            blob = self.blob_repos.setdefault(entry["hash"], set())
            blob.add(entry["repository"])

            if self.should_download_blob(entry["hash"]):
                self.pool.spawn(self.do_download_blob(entry["hash"]))

        elif entry["type"] == RegistryActions.MANIFEST_STORED:
            manifest = self.manifest_locations.setdefault(entry["hash"], set())
            manifest.add(entry["location"])

            if self.should_download_manifest(entry["hash"]):
                self.pool.spawn(self.do_download_manifest(entry["hash"]))

        elif entry["type"] == RegistryActions.MANIFEST_MOUNTED:
            manifest = self.manifest_repos.setdefault(entry["hash"], set())
            manifest.add(entry["repository"])

            if self.should_download_manifest(entry["hash"]):
                self.pool.spawn(self.do_download_manifest(entry["hash"]))
