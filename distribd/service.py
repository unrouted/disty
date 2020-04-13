import argparse
import asyncio
import logging
import pathlib
import sys

import coloredlogs
import verboselogs

from . import config
from .log import Log
from .mirror import Mirrorer
from .raft import Node
from .registry import run_registry
from .state import RegistryState

logger = logging.getLogger(__name__)


async def main(argv=None):
    verboselogs.install()
    coloredlogs.install(
        level="DEBUG", fmt="%(asctime)s %(name)s %(levelname)s %(message)s"
    )

    parser = argparse.ArgumentParser()
    parser.add_argument("nodename")
    args = parser.parse_args(argv or sys.argv[1:])

    identifier = f"{args.nodename}"
    logger.debug("Starting node %s", identifier)

    cfg = config.config[identifier]
    raft_port = cfg["raft_port"]
    registry_port = cfg["registry_port"]
    images_directory = pathlib.Path(cfg["images_directory"])

    log = Log(images_directory / "journal")
    await log.open()

    node = Node(identifier, log)

    for other_identifier, detail in config.config.items():
        if identifier != other_identifier:
            node.add_member(other_identifier)

    registry_state = RegistryState()
    log.add_reducer(registry_state.dispatch_entries)

    mirrorer = Mirrorer(images_directory, node.identifier, node.send_action)
    log.add_reducer(mirrorer.dispatch_entries)

    try:
        await asyncio.gather(
            node.run_forever(raft_port),
            run_registry(
                node.identifier,
                registry_state,
                node.send_action,
                images_directory,
                registry_port,
            ),
        )

    except asyncio.CancelledError:
        pass

    finally:
        await asyncio.gather(node.close(), log.close(), mirrorer.close())
