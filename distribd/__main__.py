import argparse
import asyncio
import logging
import pathlib

import coloredlogs
import verboselogs

from .config import config
from .log import Log
from .mirror import Mirrorer
from .raft import Node
from .registry import run_registry
from .state import RegistryState

logger = logging.getLogger(__name__)


async def main():
    verboselogs.install()
    coloredlogs.install(
        level="DEBUG", fmt="%(asctime)s %(name)s %(levelname)s %(message)s"
    )

    parser = argparse.ArgumentParser()
    parser.add_argument("port")
    args = parser.parse_args()

    identifier = f"distrib-{args.port}"
    logger.debug("Starting node %s", identifier)

    raft_port = config[identifier]["raft_port"]
    registry_port = config[identifier]["registry_port"]
    images_directory = pathlib.Path(config[identifier]["images_directory"])

    log = Log(images_directory / "journal")
    await log.open()

    node = Node(identifier, log)

    for other_identifier, detail in config.items():
        if identifier != other_identifier:
            node.add_member(other_identifier)

    registry_state = RegistryState()
    log.add_reducer(registry_state.dispatch_entries)

    mirrorer = Mirrorer(images_directory, node.identifier, node.send_action)
    log.add_reducer(mirrorer.dispatch_entries)

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


if __name__ == "__main__":
    asyncio.run(main())
