import asyncio
import logging
import pathlib
import time

from .actions import RegistryActions
from .jobs import WorkerPool
from .state import ATTR_LOCATIONS, ATTR_TYPE, TYPE_BLOB, TYPE_MANIFEST
from .utils.registry import get_blob_path, get_manifest_path

logger = logging.getLogger(__name__)

GARBAGE_COLLECTION_TRIGGER = [
    RegistryActions.BLOB_UNSTORED,
    RegistryActions.MANIFEST_UNSTORED,
    # Tag might have been deleted by deleting a manifest hash
    RegistryActions.MANIFEST_UNMOUNTED,
    # Tag overwritten with another tag
    RegistryActions.HASH_TAGGED,
]

# An object must be 12 hours old before considered for garbage collection
MINIMUM_GARBAGE_AGE = 60 * 60 * 12


class GarbageCollector:
    def __init__(self, image_directory, identifier, state, send_action):
        self.image_directory = image_directory
        self.identifier = identifier
        self.state = state
        self.send_action = send_action

        self.pool = WorkerPool()
        self._futures = {}

        self._lock = asyncio.Lock()

    async def close(self):
        await self.pool.close()

    def should_garbage_collect(self, entries):
        for term, entry in entries:
            if "type" not in entry:
                continue

            if entry["type"] in GARBAGE_COLLECTION_TRIGGER:
                return True

        return False

    def cleanup_object(self, image_directory: pathlib.Path, path: pathlib.Path):
        if path.exists():
            mtime = time.time() - path.stat().st_mtime
            if mtime < MINIMUM_GARBAGE_AGE:
                logger.debug(
                    "Skipped orphaned object: %s (younger than 12 hours)", path
                )
                return False

            try:
                logger.debug("Unlinking orphaned object: %s", path)
                path.unlink()
            except pathlib.FileNotFoundError:
                pass

        # Clean empty directories caused by cleanup. Given
        #
        # a = pathlib.Path("/etc/foo")
        # b = pathlib.Path("/etc")
        #
        # a.is_relative_to(b) == True
        # a.parent.is_relative_to(b) == True
        # a.parent.parent.is_relative_to(b) == False
        #
        # Then traverse up until the parent directory is not relative to image_directory
        # As that means the current path *is* the image directory.

        while path.parent.is_relative_to(image_directory):
            try:
                next(path.iterdir())

            except StopIteration:
                logger.debug("Unlinking empty directory: %s", path)
                path.rmdir()

                path = path.parent

            finally:
                break

        return True

    async def garbage_collect(self, state):
        async with self._lock:
            actions = []

            for garbage_hash in state.get_orphaned_objects():
                object = state[garbage_hash]

                if self.identifier not in object[ATTR_LOCATIONS]:
                    continue

                if object[ATTR_TYPE] == TYPE_BLOB:
                    path = get_blob_path(self.image_directory, garbage_hash)
                    if not self.cleanup_object(self.image_directory, path):
                        continue

                    actions.append(
                        {
                            "type": RegistryActions.BLOB_UNSTORED,
                            "hash": garbage_hash,
                            "location": self.identifier,
                        }
                    )

                elif object[ATTR_TYPE] == TYPE_MANIFEST:
                    path = get_manifest_path(self.image_directory, garbage_hash)
                    if not self.cleanup_object(self.image_directory, path):
                        continue

                    actions.append(
                        {
                            "type": RegistryActions.MANIFEST_UNSTORED,
                            "hash": garbage_hash,
                            "location": self.identifier,
                        }
                    )

            if actions:
                await self.send_action(actions)

    def dispatch_entries(self, state, entries):
        if self.should_garbage_collect(entries):
            logger.critical("MEMEMEMEMEMEMEME")
            self.pool.spawn(self.garbage_collect(state))
        pass
