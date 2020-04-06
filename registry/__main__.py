import argparse
import asyncio
import logging

from .raft import run_raft
from .registry import run_registry


async def main():
    logging.basicConfig(level=logging.DEBUG)

    parser = argparse.ArgumentParser()
    parser.add_argument("port")
    args = parser.parse_args()

    raft_port = int(args.port)
    registry_port = int(args.port) + 1000

    await asyncio.gather(
        run_raft(raft_port),
        run_registry(registry_port),
    )


if __name__ == "__main__":
    asyncio.run(main())
