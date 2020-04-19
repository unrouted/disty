import asyncio
import json
import logging
import os
import socket

from distribd import config
from distribd.service import main
import pytest

logger = logging.getLogger(__name__)

examples = [
    # Example 1 - transaction log empty other than having previously achieved consensus
    (
        [(1, {"type": "consensus"})],
        [(1, {"type": "consensus"})],
        [(1, {"type": "consensus"})],
        [
            [(1, {"type": "consensus"}), (2, {})],
            # First vote fails
            [(1, {"type": "consensus"}), (3, {})],
            # Consisentency but not immediately
            [(1, {"type": "consensus"}), (4, {})],
            # Consisentency but not immediately
            [(1, {"type": "consensus"}), (5, {})],
        ],
    ),
    # Example 2 - 1 node has more entries than other nodes
    (
        [(1, {"type": "consensus"}), (2, {}), (2, {"type": "something-happened"})],
        [(1, {"type": "consensus"})],
        [(1, {"type": "consensus"})],
        [
            # Node 1 is leader, most history is preserved
            [
                (1, {"type": "consensus"}),
                (2, {}),
                (2, {"type": "something-happened"}),
                (3, {}),
            ],
            # Node 2 is leader with node 3
            [(1, {"type": "consensus"}), (2, {})],
            # First vote fails and consensus reached between node 2 and 3
            [(1, {"type": "consensus"}), (3, {})],
            # Consisentency between node 2 and 3, but not immediately
            [(1, {"type": "consensus"}), (4, {})],
            # Consisentency between node 2 and 3, but not immediately
            [(1, {"type": "consensus"}), (5, {})],
            # Node 1 leader, consensu achieved eventually
            [
                (1, {"type": "consensus"}),
                (2, {}),
                (2, {"type": "something-happened"}),
                (4, {}),
            ],
            # Node 1 leader, consenus achieved eventually
            [
                (1, {"type": "consensus"}),
                (2, {}),
                (2, {"type": "something-happened"}),
                (5, {}),
            ],
        ],
    ),
]


@pytest.fixture
async def fake_cluster(tmp_path, monkeypatch, loop):
    test_config = {}

    for node in ("node1", "node2", "node3"):
        dir = tmp_path / node
        if not dir.exists():
            os.makedirs(dir)

        raft_port = unused_port()
        registry_port = unused_port()
        prometheus_port = unused_port()

        test_config[node] = {
            "raft_port": raft_port,
            "raft_url": f"http://127.0.0.1:{raft_port}",
            "registry_port": registry_port,
            "registry_url": f"http://127.0.0.1:{registry_port}",
            "prometheus_port": prometheus_port,
            "images_directory": dir,
        }

    monkeypatch.setattr(config, "config", test_config)

    servers = []

    async def start(node, journal):
        with open(tmp_path / "node1" / "journal", "w") as fp:
            for row in journal:
                fp.write(json.dumps(row) + "\n")

        servers.append(asyncio.ensure_future(main([node])))

    yield start

    for server in servers:
        server.cancel()

    try:
        await asyncio.gather(*servers)
    except asyncio.CancelledError:
        pass


@pytest.mark.parametrize("node1,node2,node3,agreements", examples)
async def test_recovery(tmp_path, fake_cluster, node1, node2, node3, agreements):
    await fake_cluster("node1", node1)
    await fake_cluster("node2", node2)
    await fake_cluster("node3", node2)
    await asyncio.sleep(0)

    for i in range(100):
        result1 = []
        with open(tmp_path / "node1" / "journal", "r") as fp:
            for row in fp:
                result1.append(tuple(json.loads(row)))

        result2 = []
        with open(tmp_path / "node2" / "journal", "r") as fp:
            for row in fp:
                result2.append(tuple(json.loads(row)))

        result3 = []
        with open(tmp_path / "node3" / "journal", "r") as fp:
            for row in fp:
                result3.append(tuple(json.loads(row)))

        all_matching = result1 == result2 == result3
        match_one_agreement = any(result1 == agreement for agreement in agreements)

        if all_matching and match_one_agreement:
            break

        await asyncio.sleep(0.1)
    else:
        logger.critical("node 1: %s", result1)
        logger.critical("node 2: %s", result2)
        logger.critical("node 3: %s", result3)

        raise RuntimeError("Did not converge on a valid agreement")


def unused_port():
    """Return a port that is unused on the current host."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.mark.parametrize("node1,node2,node3,agreements", examples)
@pytest.mark.parametrize("third_node", ["node1", "node2", "node3"])
async def test_third_node_recovery(
    tmp_path, fake_cluster, node1, node2, node3, agreements, third_node
):
    nodes = {
        "node1": node1,
        "node2": node2,
        "node3": node3,
    }

    for name, journal in nodes.items():
        if name != third_node:
            await fake_cluster(name, journal)

    # FIXME: Need to find a wait to wait for stability
    await asyncio.sleep(2)

    await fake_cluster(third_node, nodes[third_node])
    await asyncio.sleep(0)

    for i in range(100):
        result1 = []
        with open(tmp_path / "node1" / "journal", "r") as fp:
            for row in fp:
                result1.append(tuple(json.loads(row)))

        result2 = []
        with open(tmp_path / "node2" / "journal", "r") as fp:
            for row in fp:
                result2.append(tuple(json.loads(row)))

        result3 = []
        with open(tmp_path / "node3" / "journal", "r") as fp:
            for row in fp:
                result3.append(tuple(json.loads(row)))

        all_matching = result1 == result2 == result3
        match_one_agreement = any(result1 == agreement for agreement in agreements)

        if all_matching and match_one_agreement:
            break

        await asyncio.sleep(0.1)
    else:
        logger.critical("node 1: %s", result1)
        logger.critical("node 2: %s", result2)
        logger.critical("node 3: %s", result3)

        raise RuntimeError("Did not converge on a valid agreement")
