import argparse
import asyncio
import logging
import pathlib
import sys

import coloredlogs
import verboselogs
from confuse import Configuration

from . import config
from .machine import Machine
from .mirror import Mirrorer
from .prometheus import run_prometheus
from .raft import HttpRaft
from .reducers import Reducers
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
    parser.add_argument("--name", dest="node.identifier")
    args = parser.parse_args(argv or sys.argv[1:])

    app_config = Configuration('distribd', __name__)
    app_config.set_args(args)

    identifier = app_config["node.identifier"].get(str)

    logger.debug("Starting node %s", identifier)

    cfg = config.config[identifier]
    raft_port = cfg["raft_port"]
    registry_port = cfg["registry_port"]
    prometheus_port = cfg["prometheus_port"]
    images_directory = pathlib.Path(cfg["images_directory"])

    storage = Storage(images_directory / "journal")
    await storage.open()

    machine = Machine(identifier)
    machine.term = storage.current_term
    machine.log.load(storage.log)

    for other_identifier, detail in config.config.items():
        if identifier != other_identifier:
            machine.add_peer(other_identifier)

    machine.start()

    reducers = Reducers(machine)

    raft = HttpRaft(machine, storage, reducers)

    registry_state = RegistryState()
    reducers.add_reducer(registry_state.dispatch_entries)

    mirrorer = Mirrorer(images_directory, machine.identifier, raft.append)
    reducers.add_reducer(mirrorer.dispatch_entries)

    try:
        await asyncio.gather(
            raft.run_forever(raft_port),
            run_registry(
                config,
                machine.identifier,
                registry_state,
                raft.append,
                images_directory,
                registry_port,
            ),
            run_prometheus(
                machine.identifier,
                registry_state,
                images_directory,
                raft,
                prometheus_port,
            ),
        )

    except asyncio.CancelledError:
        pass

    finally:
        await asyncio.gather(storage.close(), mirrorer.close())
