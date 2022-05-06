import argparse
import asyncio
import logging
import sys

import coloredlogs
import confuse
import verboselogs

from .machine import Machine
from .raft import HttpRaft
from .reducers import Reducers
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
    args = parser.parse_args(argv if argv is not None else sys.argv[1:])

    if not config:
        config = confuse.Configuration("distribd", __name__)

    config.set_args(args, dots=True)

    logger.debug("Configuration directory: %s", config.config_dir())
    logger.debug(config.dump())

    identifier = config["node"]["identifier"].get(str)

    logger.debug("Starting node %s", identifier)

    images_directory = config["storage"].as_path()

    storage = Storage(images_directory / "journal")
    await storage.open()

    machine = Machine(identifier)
    if storage.current_term > machine.term:
        machine.term = storage.current_term
    machine.log.load(storage.log)

    for peer in config["peers"].get(list):
        other_identifier = peer["name"]
        if identifier != other_identifier:
            machine.add_peer(other_identifier)

    machine.start()

    registry_state = RegistryState()
    reducers = Reducers(machine, registry_state)

    raft = HttpRaft(config, machine, storage, reducers)

    services = [
        raft.run_forever(),
    ]

    from distribd.distribd import start_registry_service

    try:
        webhooks = config["webhooks"].get(list)
    except confuse.exceptions.NotFoundError:
        webhooks = []

    token_server = config["token_server"]
    if token_server["enabled"].get(bool):
        token_config = {
            "enabled": True,
            "realm": token_server["realm"].get(str),
            "service": token_server["service"].get(str),
            "issuer": token_server["issuer"].get(str),
        }
        public_key_path = token_server["public_key"].as_path()
        with open(public_key_path, "r") as fp:
            token_config["public_key"] = fp.read()

    else:
        token_config = {"enabled": False}

    mint_server = config["mirroring"]
    if mint_server["realm"].exists():
        mint_config = {
            "enabled": True,
            "realm": mint_server["realm"].get(str),
            "service": mint_server["service"].get(str),
            "username": mint_server["username"].get(str),
            "password": mint_server["password"].get(str),
        }

    else:
        mint_config = {"enabled": False}

    if not start_registry_service(
        registry_state,
        raft.append,
        str(images_directory),
        webhooks,
        token_config,
        mint_config,
        machine,
        machine.identifier,
        reducers,
        asyncio.get_running_loop(),
    ):
        return

    try:
        await asyncio.gather(*services)

    except asyncio.CancelledError:
        pass

    finally:
        await asyncio.gather(raft.close(), storage.close(), mirrorer.close())
