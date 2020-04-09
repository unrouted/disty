import asyncio
import copy
from unittest import mock

import aiohttp

from distribd.service import main
from distribd import config


async def test_cluster_starts(tmp_path, monkeypatch):
    test_config = copy.deepcopy(config.config)
    for port in ("8080", "8081", "8082"):
        test_config[f"distrib-{port}"]["images_directory"] = tmp_path / port
    monkeypatch.setattr(config, "config", test_config)

    servers = asyncio.ensure_future(asyncio.gather(
        main(["8080"]),
        main(["8081"]),
        main(["8082"]),
    ))
    await asyncio.sleep(0)

    async with aiohttp.ClientSession() as session:
        async with session.get("http://localhost:9080/v2/") as resp:
            assert resp.status == 200

        async with session.get("http://localhost:9081/v2/") as resp:
            assert resp.status == 200

        async with session.get("http://localhost:9082/v2/") as resp:
            assert resp.status == 200

    # Cancel servers. Ignore CancelledError.
    servers.cancel()
    try:
        await servers
    except asyncio.CancelledError:
        pass
