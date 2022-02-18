import asyncio
import logging

from distribd.service import main

logger = logging.getLogger(__name__)


async def test_prometheus(event_loop, cluster_config, client_session):
    servers = asyncio.ensure_future(
        asyncio.gather(
            main([], cluster_config["node1"]),
            main([], cluster_config["node2"]),
            main([], cluster_config["node3"]),
        )
    )
    # FIXME Better non-time based hook...
    await asyncio.sleep(0.1)

    for i in range(100):
        address = cluster_config["node1"]["raft"]["address"].get()
        port = cluster_config["node1"]["raft"]["port"].get()

        async with client_session.get(f"http://{address}:{port}/status") as resp:
            assert resp.status == 200
            payload = await resp.json()
            if payload["consensus"]:
                break
        await asyncio.sleep(1)
    else:
        raise RuntimeError("No consensus")

    for node in ("node1", "node2", "node3"):
        address = cluster_config[node]["prometheus"]["address"].get()
        port = cluster_config[node]["prometheus"]["port"].get()

        async with client_session.get(f"http://{address}:{port}/healthz") as resp:
            assert resp.status == 200
            assert await resp.json() == {"ok": True}

        async with client_session.get(f"http://{address}:{port}/metrics") as resp:
            assert resp.status == 200

    # Cancel servers. Ignore CancelledError.
    servers.cancel()
    try:
        await servers
    except asyncio.CancelledError:
        pass
