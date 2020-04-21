import logging
import os
import socket

import aiohttp
import confuse
import pytest


@pytest.fixture
async def client_session(loop):
    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=0.1)
    ) as session:
        yield session


@pytest.fixture(autouse=True)
def configure_logging(caplog):
    caplog.set_level(logging.DEBUG)


def unused_port():
    """Return a port that is unused on the current host."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def cluster_config(tmp_path):
    cluster = {}

    seeds = []

    for node in ("node1", "node2", "node3"):
        config = cluster[node] = confuse.Configuration("distribd", read=False)
        config.read(False)

        dir = tmp_path / node
        if not dir.exists():
            os.makedirs(dir)

        config["node"]["identifier"].set(node)
        config["raft"]["address"].set("127.0.0.1")
        config["raft"]["port"].set(unused_port())
        config["registry"]["address"].set("127.0.0.1")
        config["registry"]["port"].set(unused_port())
        config["prometheus"]["address"].set("127.0.0.1")
        config["prometheus"]["port"].set(unused_port())
        config["storage"].set(str(dir))

        config["token_server"]["enabled"].set(False)

        config["peers"].set(["node1", "node2", "node3"])

        raft_port = config["raft"]["port"].get()
        seeds.append(f"http://127.0.0.1:{raft_port}")

    for node in ("node1", "node2", "node3"):
        cluster[node]["seeding"]["urls"].set(seeds)

    return cluster
