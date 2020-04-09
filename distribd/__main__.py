import argparse
import asyncio
import pathlib

import coloredlogs
import verboselogs

from .log import Log
from .mirror import Mirrorer
from .raft import Node
from .registry import run_registry
from .state import RegistryState


async def main():
    verboselogs.install()
    coloredlogs.install(
        level="DEBUG", fmt="%(asctime)s %(name)s %(levelname)s %(message)s"
    )

    parser = argparse.ArgumentParser()
    parser.add_argument("port")
    args = parser.parse_args()

    raft_port = int(args.port)
    registry_port = raft_port + 1000
    images_directory = pathlib.Path("images") / str(raft_port)

    log = Log(f"127.0.0.1-{raft_port}.log")
    await log.open()

    node = Node(f"127.0.0.1:{raft_port}", log)

    for remote in (8080, 8081, 8082):
        if raft_port != remote:
            node.add_member(f"127.0.0.1:{remote}")

    registry_state = RegistryState()
    log.add_reducer(registry_state.dispatch_entries)

    mirrorer = Mirrorer(node.identifier, node.send_action)
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
