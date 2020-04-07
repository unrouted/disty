import argparse
import asyncio
import logging

from .log import Log
from .raft import run_raft
from .registry import run_registry


async def main():
    logging.basicConfig(level=logging.DEBUG)

    parser = argparse.ArgumentParser()
    parser.add_argument("port")
    args = parser.parse_args()

    raft_port = int(args.port)
    registry_port = int(args.port) + 1000

    log = Log(f"127.0.0.1-{raft_port}.log")
    await log.open()

    await asyncio.gather(
        run_raft(log, raft_port), run_registry(registry_port),
    )


if __name__ == "__main__":
    asyncio.run(main())
