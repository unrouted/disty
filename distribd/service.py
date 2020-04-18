import argparse
import asyncio
import logging
import pathlib
import sys

import coloredlogs
import verboselogs

from . import config
from .machine import Machine
from .mirror import Mirrorer
from .prometheus import run_prometheus
from .raft import run_raft_forever
from .registry import run_registry
from .state import RegistryState
from .storage import Storage

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
    prometheus_port = cfg["prometheus_port"]
    images_directory = pathlib.Path(cfg["images_directory"])

    storage = Storage(images_directory / "journal")
    await storage.open()

    machine = Machine(identifier)

    for other_identifier, detail in config.config.items():
        if identifier != other_identifier:
            machine.add_peer(other_identifier)

    machine.start()

    registry_state = RegistryState()
    storage.add_reducer(registry_state.dispatch_entries)

    mirrorer = Mirrorer(images_directory, machine.identifier, lambda x: None)
    storage.add_reducer(mirrorer.dispatch_entries)

    try:
        await asyncio.gather(
            run_raft_forever(machine, raft_port),
            run_registry(
                machine.identifier,
                registry_state,
                lambda x: None,
                images_directory,
                registry_port,
            ),
            run_prometheus(
                machine.identifier,
                registry_state,
                images_directory,
                machine,
                prometheus_port,
            ),
        )

    except asyncio.CancelledError:
        pass

    finally:
        await asyncio.gather(storage.close(), mirrorer.close())
