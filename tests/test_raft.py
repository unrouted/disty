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
            # #31
            [(1, {"type": "consensus"}), (2, {}), (3, {})],
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
            # #32
            [(1, {"type": "consensus"}), (2, {}), (3, {})],
            # BUG #33
            # Node 2 or 3 becomes candidate. Can't get vote from node 1 because previous term.
            # Gets 2 votes, becomes leader, commits (2, 2). node 1 log truncated to 1 entry.
            # Suspect node 1 becomes leader after truncating but before commiting term 2 index 2? . All logs reverted to (1,1).
            # Somehow node 1 *didn't* commit a new entry after this.
            [(1, {"type": "consensus"})],
        ],
    ),
]


@pytest.mark.parametrize("node1,node2,node3,agreements", examples)
async def test_recovery(tmp_path, monkeypatch, node1, node2, node3, agreements):
    test_config = {}

    for node in ("node1", "node2", "node3"):
        dir = tmp_path / node
        if not dir.exists():
            os.makedirs(dir)

        raft_port = unused_port()
        registry_port = unused_port()

        test_config[node] = {
            "raft_port": raft_port,
            "raft_url": f"http://127.0.0.1:{raft_port}",
            "registry_port": registry_port,
            "registry_url": f"http://127.0.0.1:{registry_port}",
            "images_directory": dir,
        }

    monkeypatch.setattr(config, "config", test_config)

    with open(tmp_path / "node1" / "journal", "w") as fp:
        for row in node1:
            fp.write(json.dumps(row) + "\n")

    with open(tmp_path / "node2" / "journal", "w") as fp:
        for row in node2:
            fp.write(json.dumps(row) + "\n")

    with open(tmp_path / "node3" / "journal", "w") as fp:
        for row in node3:
            fp.write(json.dumps(row) + "\n")

    servers = asyncio.ensure_future(
        asyncio.gather(main(["node1"]), main(["node2"]), main(["node3"]),)
    )
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

    # Cancel servers. Ignore CancelledError.
    servers.cancel()
    try:
        await servers
    except asyncio.CancelledError:
        pass


def unused_port():
    """Return a port that is unused on the current host."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.mark.parametrize("node1,node2,node3,agreements", examples)
@pytest.mark.parametrize("third_node", ["node1", "node2", "node3"])
async def test_third_node_recovery(
    tmp_path, monkeypatch, node1, node2, node3, agreements, third_node
):
    test_config = {}

    for node in ("node1", "node2", "node3"):
        dir = tmp_path / node
        if not dir.exists():
            os.makedirs(dir)

        raft_port = unused_port()
        registry_port = unused_port()

        test_config[node] = {
            "raft_port": raft_port,
            "raft_url": f"http://127.0.0.1:{raft_port}",
            "registry_port": registry_port,
            "registry_url": f"http://127.0.0.1:{registry_port}",
            "images_directory": dir,
        }

    monkeypatch.setattr(config, "config", test_config)

    with open(tmp_path / "node1" / "journal", "w") as fp:
        for row in node1:
            fp.write(json.dumps(row) + "\n")

    with open(tmp_path / "node2" / "journal", "w") as fp:
        for row in node2:
            fp.write(json.dumps(row) + "\n")

    with open(tmp_path / "node3" / "journal", "w") as fp:
        for row in node3:
            fp.write(json.dumps(row) + "\n")

    ports = [p for p in ["node1", "node2", "node3"] if p != third_node]
    servers = [asyncio.ensure_future(main([str(port)])) for port in ports]

    # FIXME: Need to find a wait to wait for stability
    await asyncio.sleep(2)

    servers.append(asyncio.ensure_future(main([str(third_node)])))
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

    # Cancel servers. Ignore CancelledError.
    servers = asyncio.gather(*servers)
    servers.cancel()
    try:
        await servers
    except asyncio.CancelledError:
        pass
