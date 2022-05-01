import asyncio
import datetime
import logging
import pathlib

from .actions import RegistryActions
from .state import (
    ATTR_CREATED,
    ATTR_LOCATIONS,
    ATTR_REPOSITORIES,
    ATTR_TYPE,
    TYPE_BLOB,
    TYPE_MANIFEST,
)
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


async def do_garbage_collect_phase1(machine, state, send_action):
    if not machine.is_leader:
        logger.info("Garbage collection: Phase 1: Not leader")
        return

    logger.info(
        "Garbage collection: Phase 1: Sweeping for mounted objects with no dependents"
    )

    actions = []

    for garbage_hash in state.get_orphaned_objects():
        object = state[garbage_hash]

        if object[ATTR_TYPE] == TYPE_BLOB:
            action = RegistryActions.BLOB_UNMOUNTED
        elif object[ATTR_TYPE] == TYPE_MANIFEST:
            action = RegistryActions.MANIFEST_UNMOUNTED
        else:
            continue

        now = datetime.datetime.now(datetime.timezone.utc)
        if (now - object[ATTR_CREATED]).total_seconds() < MINIMUM_GARBAGE_AGE:
            logger.info(
                "Garbage collection: Phase 1: %s is orphaned but less than 12 hours old",
                garbage_hash,
            )
            continue

        for repository in object[ATTR_REPOSITORIES]:
            actions.append(
                {
                    "type": action,
                    "timestamp": datetime.datetime.now(
                        datetime.timezone.utc
                    ).isoformat(),
                    "hash": garbage_hash,
                    "repository": repository,
                }
            )

    if actions:
        logger.info("Garbage collection: Phase 1: Reaped %d mounts", len(actions))
        await send_action(actions)


def cleanup_object(image_directory: pathlib.Path, path: pathlib.Path):
    if path.exists():
        try:
            logger.info("Unlinking orphaned object: %s", path)
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
            logger.info("Unlinking empty directory: %s", path)
            path.rmdir()

            path = path.parent

        finally:
            break

    return True


async def do_garbage_collect_phase2(machine, state, send_action, image_directory):
    logger.info(
        "Garbage collection: Phase 2: Sweeping for unmounted objects that can be unstored"
    )

    actions = []

    for garbage_hash in state.get_orphaned_objects():
        object = state[garbage_hash]

        if machine.identifier not in object[ATTR_LOCATIONS]:
            continue

        if len(object[ATTR_REPOSITORIES]) > 0:
            logger.debug(
                "Garbage collection: Phase 2: %s is orphaned but still part of %d repositories",
                garbage_hash,
                len(object[ATTR_REPOSITORIES]),
            )
            continue

        if object[ATTR_TYPE] == TYPE_BLOB:
            path = get_blob_path(image_directory, garbage_hash)
            if not cleanup_object(image_directory, path):
                continue
            actions.append(
                {
                    "type": RegistryActions.BLOB_UNSTORED,
                    "timestamp": datetime.datetime.now(
                        datetime.timezone.utc
                    ).isoformat(),
                    "hash": garbage_hash,
                    "location": machine.identifier,
                }
            )

        elif object[ATTR_TYPE] == TYPE_MANIFEST:
            path = get_manifest_path(image_directory, garbage_hash)
            if not cleanup_object(image_directory, path):
                continue

            actions.append(
                {
                    "type": RegistryActions.MANIFEST_UNSTORED,
                    "timestamp": datetime.datetime.now(
                        datetime.timezone.utc
                    ).isoformat(),
                    "hash": garbage_hash,
                    "location": machine.identifier,
                }
            )

    if actions:
        logger.info("Garbage collection: Phase 2: Reaped %d stores", len(actions))
        await send_action(actions)


async def do_garbage_collect(machine, state, send_action, image_directory):
    while True:
        try:
            await do_garbage_collect_phase1(machine, state, send_action)
            await do_garbage_collect_phase2(
                machine, state, send_action, image_directory
            )

        except asyncio.CancelledError:
            return

        except Exception:
            logger.exception("Unhandled error whilst collecting garbage")

        await asyncio.sleep(60)
