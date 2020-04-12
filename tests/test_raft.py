import asyncio
import copy
import json
import logging
import os

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
        [[(1, {"type": "consensus"}), (2, {})]],
    ),
    # Example 2 - 1 node has more entries than other nodes
    (
        [(1, {"type": "consensus"}), (2, {}), (2, {"type": "something-happened"})],
        [(1, {"type": "consensus"})],
        [(1, {"type": "consensus"})],
        [
            [
                (1, {"type": "consensus"}),
                (2, {}),
                (2, {"type": "something-happened"}),
                (3, {}),
            ],
            [(1, {"type": "consensus"}), (2, {})],
        ],
    ),
]


@pytest.mark.parametrize("node1,node2,node3,agreements", examples)
async def test_recovery(tmp_path, monkeypatch, node1, node2, node3, agreements):
    test_config = copy.deepcopy(config.config)
    for port in ("8080", "8081", "8082"):
        dir = test_config[f"distrib-{port}"]["images_directory"] = tmp_path / port
        if not dir.exists():
            os.makedirs(dir)
    monkeypatch.setattr(config, "config", test_config)

    with open(tmp_path / "8080" / "journal", "w") as fp:
        for row in node1:
            fp.write(json.dumps(row) + "\n")

    with open(tmp_path / "8081" / "journal", "w") as fp:
        for row in node2:
            fp.write(json.dumps(row) + "\n")

    with open(tmp_path / "8082" / "journal", "w") as fp:
        for row in node3:
            fp.write(json.dumps(row) + "\n")

    servers = asyncio.ensure_future(
        asyncio.gather(main(["8080"]), main(["8081"]), main(["8082"]),)
    )
    await asyncio.sleep(0)

    for i in range(100):
        result1 = []
        with open(tmp_path / "8080" / "journal", "r") as fp:
            for row in fp:
                result1.append(tuple(json.loads(row)))

        result2 = []
        with open(tmp_path / "8081" / "journal", "r") as fp:
            for row in fp:
                result2.append(tuple(json.loads(row)))

        result3 = []
        with open(tmp_path / "8082" / "journal", "r") as fp:
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


@pytest.mark.parametrize("node1,node2,node3,agreements", examples)
@pytest.mark.parametrize("third_node", [8080, 8081, 8082])
async def test_third_node_recovery(
    tmp_path, monkeypatch, node1, node2, node3, agreements, third_node
):
    test_config = copy.deepcopy(config.config)
    for port in ("8080", "8081", "8082"):
        dir = test_config[f"distrib-{port}"]["images_directory"] = tmp_path / port
        if not dir.exists():
            os.makedirs(dir)
    monkeypatch.setattr(config, "config", test_config)

    with open(tmp_path / "8080" / "journal", "w") as fp:
        for row in node1:
            fp.write(json.dumps(row) + "\n")

    with open(tmp_path / "8081" / "journal", "w") as fp:
        for row in node2:
            fp.write(json.dumps(row) + "\n")

    with open(tmp_path / "8082" / "journal", "w") as fp:
        for row in node3:
            fp.write(json.dumps(row) + "\n")

    ports = [p for p in [8080, 8081, 8082] if p != third_node]
    servers = [asyncio.ensure_future(main([str(port)])) for port in ports]

    # FIXME: Need to find a wait to wait for stability
    await asyncio.sleep(2)

    servers.append(asyncio.ensure_future(main([str(third_node)])))
    await asyncio.sleep(0)

    for i in range(100):
        result1 = []
        with open(tmp_path / "8080" / "journal", "r") as fp:
            for row in fp:
                result1.append(tuple(json.loads(row)))

        result2 = []
        with open(tmp_path / "8081" / "journal", "r") as fp:
            for row in fp:
                result2.append(tuple(json.loads(row)))

        result3 = []
        with open(tmp_path / "8082" / "journal", "r") as fp:
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
