import asyncio
import copy
import logging

from distribd import config
from distribd.service import main

logger = logging.getLogger(__name__)


async def test_prometheus(loop, tmp_path, monkeypatch, client_session):
    test_config = copy.deepcopy(config.config)
    for port in ("8080", "8081", "8082"):
        test_config[f"{port}"]["images_directory"] = tmp_path / port
    monkeypatch.setattr(config, "config", test_config)

    servers = asyncio.ensure_future(
        asyncio.gather(main(["8080"]), main(["8081"]), main(["8082"]),)
    )
    await asyncio.sleep(0)

    for i in range(100):
        async with client_session.get("http://localhost:8080/status") as resp:
            assert resp.status == 200
            payload = await resp.json()
            if payload["consensus"]:
                break
        await asyncio.sleep(1)
    else:
        raise RuntimeError("No consensus")

    for port in (7080, 7081, 7082):
        async with client_session.get(f"http://localhost:{port}/healthz") as resp:
            assert resp.status == 200
            assert await resp.json() == {"ok": True}

        async with client_session.get(f"http://localhost:{port}/metrics") as resp:
            assert resp.status == 200

    # Cancel servers. Ignore CancelledError.
    servers.cancel()
    try:
        await servers
    except asyncio.CancelledError:
        pass
