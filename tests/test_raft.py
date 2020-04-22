import asyncio
import json
import logging

from distribd.service import main
import pytest
from aiofile import AIOFile, LineReader

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
async def fake_cluster(cluster_config, loop):
    servers = []

    async def start(node, journal):
        with open(cluster_config[node]["storage"].as_path() / "journal", "w") as fp:
            for row in journal:
                fp.write(json.dumps(row) + "\n")

        servers.append(asyncio.ensure_future(main([], cluster_config[node])))

    yield start

    for server in servers:
        server.cancel()

    try:
        await asyncio.gather(*servers)
    except asyncio.CancelledError:
        pass


async def wait_converged(tmp_path, agreements):
    for i in range(500):
        result1 = []

        async with AIOFile(tmp_path / "node1" / "journal", "r") as afp:
            async for row in LineReader(afp):
                result1.append(tuple(json.loads(row)))

        result2 = []
        async with AIOFile(tmp_path / "node2" / "journal", "r") as afp:
            async for row in LineReader(afp):
                result2.append(tuple(json.loads(row)))

        result3 = []
        async with AIOFile(tmp_path / "node3" / "journal", "r") as afp:
            async for row in LineReader(afp):
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


@pytest.mark.parametrize("node1,node2,node3,agreements", examples)
async def test_recovery(tmp_path, fake_cluster, node1, node2, node3, agreements):
    await fake_cluster("node1", node1)
    await fake_cluster("node2", node2)
    await fake_cluster("node3", node2)
    await asyncio.sleep(0)

    await wait_converged(tmp_path, agreements)


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

    await wait_converged(tmp_path, agreements)
