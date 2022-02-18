import logging
import os
import pathlib

import aiohttp
import confuse
import pytest


@pytest.fixture
def fixtures_path():
    return pathlib.Path(__file__).parent / "fixtures"


@pytest.fixture
async def client_session(event_loop):
    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=0.1)
    ) as session:
        yield session


@pytest.fixture(autouse=True)
def configure_logging(caplog):
    caplog.set_level(logging.DEBUG)


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
        config["raft"]["port"].set(0)
        config["registry"]["default"]["address"].set("127.0.0.1")
        config["registry"]["default"]["port"].set(0)
        config["registry"]["default"]["token_server"]["enabled"].set(False)

        config["prometheus"]["address"].set("127.0.0.1")
        config["prometheus"]["port"].set(0)
        config["storage"].set(str(dir))

        config["peers"].set(["node1", "node2", "node3"])

        raft_port = config["raft"]["port"].get()
        seeds.append(f"http://127.0.0.1:{raft_port}")

    for node in ("node1", "node2", "node3"):
        cluster[node]["seeding"]["urls"].set(seeds)

    return cluster
