import argparse
import asyncio
import logging
import sys

import coloredlogs
import confuse
import verboselogs

from .machine import Machine
from .mirror import Mirrorer
from .prometheus import run_prometheus
from .raft import HttpRaft
from .reducers import Reducers
from .registry import run_registry
from .state import RegistryState
from .storage import Storage

logger = logging.getLogger(__name__)


async def main(argv=None, config=None):
    verboselogs.install()
    coloredlogs.install(
        level="DEBUG", fmt="%(asctime)s %(name)s %(levelname)s %(message)s"
    )

    parser = argparse.ArgumentParser()
    parser.add_argument("--name", dest="node.identifier")
    parser.add_argument("--raft-address", dest="raft.address")
    parser.add_argument("--registry-address", dest="registry.address")
    parser.add_argument("--prometheus-address", dest="prometheus.address")
    args = parser.parse_args(argv if argv is not None else sys.argv[1:])

    if not config:
        config = confuse.Configuration("distribd", __name__)

    config.set_args(args, dots=True)

    logger.debug("Configuration directory: %s", config.config_dir())

    identifier = config["node"]["identifier"].get(str)

    logger.debug("Starting node %s", identifier)

    images_directory = config["storage"].as_path()

    storage = Storage(images_directory / "journal")
    await storage.open()

    machine = Machine(identifier)
    if storage.current_term > machine.term:
        machine.term = storage.current_term
    machine.log.load(storage.log)

    for other_identifier in config["peers"].get(list):
        if identifier != other_identifier:
            machine.add_peer(other_identifier)

    machine.start()

    reducers = Reducers(machine)

    raft = HttpRaft(config, machine, storage, reducers)

    registry_state = RegistryState()
    reducers.add_reducer(registry_state.dispatch_entries)

    mirrorer = Mirrorer(
        config, raft.peers, images_directory, machine.identifier, raft.append
    )
    reducers.add_reducer(mirrorer.dispatch_entries)

    services = [
        raft.run_forever(),
        run_prometheus(
            raft, config, machine.identifier, registry_state, images_directory,
        ),
    ]

    for listener in config["registry"]:
        services.append(
            run_registry(
                raft,
                listener,
                config["registry"][listener],
                machine.identifier,
                registry_state,
                images_directory,
                mirrorer,
            )
        )

    try:
        await asyncio.gather(*services)

    except asyncio.CancelledError:
        pass

    finally:
        await asyncio.gather(raft.close(), storage.close(), mirrorer.close())
