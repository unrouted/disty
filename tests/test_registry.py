import asyncio
import logging
import socket

import aiohttp
from distribd import config
from distribd.machine import NodeState
from distribd.service import main
import pytest

logger = logging.getLogger(__name__)


def unused_port():
    """Return a port that is unused on the current host."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


async def check_consensus(config, session):
    consensus = set()
    states = set()

    for node in config.values():
        port = node["raft_port"]

        try:
            async with session.get(f"http://localhost:{port}/status") as resp:
                assert resp.status == 200
                payload = await resp.json()

        except asyncio.TimeoutError:
            return False

        if not payload["stable"]:
            return False

        consensus.add(payload["applied_index"])
        states.add(payload["state"])

    return len(consensus) == 1 and int(NodeState.LEADER) in states


@pytest.fixture
async def fake_cluster(loop, tmp_path, monkeypatch, client_session):
    test_config = {}

    for node in ("node1", "node2", "node3"):
        dir = tmp_path / node

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

    servers = asyncio.ensure_future(
        asyncio.gather(
            main(["--name", "node1"]),
            main(["--name", "node2"]),
            main(["--name", "node3"]),
        )
    )

    await asyncio.sleep(1)

    for i in range(100):
        if await check_consensus(test_config, client_session):
            break
        await asyncio.sleep(0.1)
    else:
        raise RuntimeError("No consensus")

    yield test_config

    # Cancel servers. Ignore CancelledError.
    servers.cancel()
    try:
        await servers
    except asyncio.CancelledError:
        pass


async def test_v2_redir(fake_cluster, client_session):
    for node in ("node1", "node2", "node3"):
        port = fake_cluster[node]["registry_port"]

        async with client_session.get(
            f"http://localhost:{port}/v2", allow_redirects=False
        ) as resp:
            assert resp.status == 302
            assert resp.headers["Location"] == "/v2/"


async def test_list_tags_404(fake_cluster, client_session):
    for node in ("node1", "node2", "node3"):
        port = fake_cluster[node]["registry_port"]

        async with client_session.get(
            f"http://localhost:{port}/v2/alpine/tags/list"
        ) as resp:
            assert resp.status == 404
            assert await resp.json() == {
                "errors": [
                    {
                        "code": "NAME_UNKNOWN",
                        "detail": {"repository": "alpine"},
                        "message": "repository name not known to registry",
                    }
                ]
            }


async def get_blob(port, hash):
    for i in range(100):
        async with aiohttp.ClientSession() as session:
            async with session.head(
                f"http://localhost:{port}/v2/alpine/blobs/sha256:{hash}"
            ) as resp:
                if resp.status == 404:
                    # Eventual consistency...
                    await asyncio.sleep(0.1)
                    continue
                assert resp.headers["Docker-Content-Digest"] == f"sha256:{hash}"

            async with session.get(
                f"http://localhost:{port}/v2/alpine/blobs/sha256:{hash}"
            ) as resp:
                if resp.status == 404:
                    # Eventual consistency...
                    await asyncio.sleep(0.1)
                    continue
                assert resp.headers["Docker-Content-Digest"] == f"sha256:{hash}"
                return resp.headers["Content-Length"], await resp.read()

    raise RuntimeError("Didn't achieve consistency in time")


async def get_manifest(port, hash):
    for i in range(100):
        async with aiohttp.ClientSession() as session:
            url = f"http://localhost:{port}/v2/alpine/manifests/sha256:{hash}"

            async with session.head(url) as resp:
                if resp.status == 404:
                    # Eventual consistency...
                    await asyncio.sleep(0.1)
                    continue
                assert resp.status == 200

            async with session.get(url) as resp:
                assert resp.status == 200
                assert resp.headers["Docker-Content-Digest"] == f"sha256:{hash}"
                return resp.headers["Content-Length"], await resp.json()

    raise RuntimeError("Didn't achieve consistency in time")


async def get_manifest_byt_tag(port, tag, repository="alpine"):
    for i in range(100):
        async with aiohttp.ClientSession() as session:
            url = f"http://localhost:{port}/v2/{repository}/manifests/{tag}"

            async with session.head(url) as resp:
                if resp.status == 404:
                    # Eventual consistency...
                    await asyncio.sleep(0.1)
                    continue
                assert resp.status == 200

            async with session.get(url) as resp:
                assert resp.status == 200
                digest = resp.headers["Docker-Content-Digest"].split(":", 1)[1]
                return digest, await resp.json()

    raise RuntimeError("Didn't achieve consistency in time")


async def assert_blob(fake_cluster, hash, repository="alpine"):
    for node in ("node1", "node2", "node3"):
        port = fake_cluster[node]["registry_port"]
        content_length, body = await get_blob(port, hash)
        assert content_length == "4"
        assert body == b"9080"


async def assert_manifest(fake_cluster, hash, expected_body):
    for node in ("node1", "node2", "node3"):
        port = fake_cluster[node]["registry_port"]
        logger.critical("Getting manifest for port %s", port)
        content_length, body = await get_manifest(port, hash)
        assert body == expected_body


async def test_put_blob_fail_invalid_hash(fake_cluster):
    port = fake_cluster["node1"]["registry_port"]

    async with aiohttp.ClientSession() as session:
        async with session.post(
            f"http://localhost:{port}/v2/alpine/blobs/uploads/"
        ) as resp:
            assert resp.status == 202
            assert resp.headers["Location"].startswith("/v2/alpine/blobs/uploads/")
            location = resp.headers["Location"]

        async with session.patch(
            f"http://localhost:{port}{location}", data=b"9080"
        ) as resp:
            assert resp.status == 202

        async with session.put(
            f"http://localhost:{port}{location}?digest=sha256:invalid_hash_here"
        ) as resp:
            assert resp.status == 400


async def test_put_blob(fake_cluster):
    port = fake_cluster["node1"]["registry_port"]
    digest = "bd2079738bf102a1b4e223346f69650f1dcbe685994da65bf92d5207eb44e1cc"

    async with aiohttp.ClientSession() as session:
        async with session.post(
            f"http://localhost:{port}/v2/alpine/blobs/uploads/"
        ) as resp:
            assert resp.status == 202
            assert resp.headers["Location"].startswith("/v2/alpine/blobs/uploads/")
            location = resp.headers["Location"]

        async with session.patch(
            f"http://localhost:{port}{location}", data=b"9080"
        ) as resp:
            assert resp.status == 202

        async with session.put(
            f"http://localhost:{port}{location}?digest=sha256:{digest}"
        ) as resp:
            assert resp.status == 201
            assert resp.headers["Location"] == f"/v2/alpine/blobs/sha256:{digest}"
            assert resp.headers["Docker-Content-Digest"] == f"sha256:{digest}"

        await assert_blob(fake_cluster, digest)


async def test_put_blob_without_patches(fake_cluster):
    port = fake_cluster["node1"]["registry_port"]
    digest = "bd2079738bf102a1b4e223346f69650f1dcbe685994da65bf92d5207eb44e1cc"

    async with aiohttp.ClientSession() as session:
        async with session.post(
            f"http://localhost:{port}/v2/alpine/blobs/uploads/"
        ) as resp:
            assert resp.status == 202
            assert resp.headers["Location"].startswith("/v2/alpine/blobs/uploads/")
            location = resp.headers["Location"]

        async with session.put(
            f"http://localhost:{port}{location}?digest=sha256:{digest}", data=b"9080"
        ) as resp:
            assert resp.status == 201
            assert resp.headers["Location"] == f"/v2/alpine/blobs/sha256:{digest}"
            assert resp.headers["Docker-Content-Digest"] == f"sha256:{digest}"

        await assert_blob(fake_cluster, digest)


async def test_put_blob_with_cross_mount(fake_cluster):
    port = fake_cluster["node1"]["registry_port"]
    digest = "bd2079738bf102a1b4e223346f69650f1dcbe685994da65bf92d5207eb44e1cc"

    async with aiohttp.ClientSession() as session:
        # First upload an ordinary blob
        async with session.post(
            f"http://localhost:{port}/v2/alpine/blobs/uploads/"
        ) as resp:
            assert resp.status == 202
            assert resp.headers["Location"].startswith("/v2/alpine/blobs/uploads/")
            location = resp.headers["Location"]

        async with session.patch(
            f"http://localhost:{port}{location}", data=b"9080"
        ) as resp:
            assert resp.status == 202

        async with session.delete(f"http://localhost:{port}{location}") as resp:
            assert resp.status == 204

        async with session.delete(f"http://localhost:{port}{location}") as resp:
            assert resp.status == 404

        async with session.put(
            f"http://localhost:{port}{location}?digest=sha256:{digest}", data=b"9080"
        ) as resp:
            assert resp.status == 400


async def test_put_blob_and_cancel(fake_cluster):
    port = fake_cluster["node1"]["registry_port"]
    digest = "bd2079738bf102a1b4e223346f69650f1dcbe685994da65bf92d5207eb44e1cc"

    async with aiohttp.ClientSession() as session:
        # First upload an ordinary blob
        async with session.post(
            f"http://localhost:{port}/v2/alpine/blobs/uploads/"
        ) as resp:
            assert resp.status == 202
            assert resp.headers["Location"].startswith("/v2/alpine/blobs/uploads/")
            location = resp.headers["Location"]

        async with session.put(
            f"http://localhost:{port}{location}?digest=sha256:{digest}", data=b"9080"
        ) as resp:
            assert resp.status == 201
            assert resp.headers["Location"] == f"/v2/alpine/blobs/sha256:{digest}"
            assert resp.headers["Docker-Content-Digest"] == f"sha256:{digest}"

        # Then cross-mount it from alpine repository to enipla registry
        url2 = f"http://localhost:{port}/v2/enipla/blobs/uploads/?mount=sha256:{digest}&from=alpine"
        async with session.post(url2) as resp:
            assert resp.status == 201
            assert resp.headers["Location"] == f"/v2/enipla/blobs/sha256:{digest}"

        await assert_blob(fake_cluster, digest, repository="enipla")


async def test_put_blob_and_get_status(fake_cluster):
    port = fake_cluster["node1"]["registry_port"]

    async with aiohttp.ClientSession() as session:
        # First upload an ordinary blob
        async with session.post(
            f"http://localhost:{port}/v2/alpine/blobs/uploads/"
        ) as resp:
            assert resp.status == 202
            assert resp.headers["Location"].startswith("/v2/alpine/blobs/uploads/")
            location = resp.headers["Location"]

        async with session.patch(
            f"http://localhost:{port}{location}", data=b"9080"
        ) as resp:
            assert resp.status == 202

        async with session.get(f"http://localhost:{port}{location}") as resp:
            assert resp.status == 204
            assert resp.headers["Range"] == "0-4"


async def test_put_blob_and_delete(fake_cluster):
    port = fake_cluster["node1"]["registry_port"]
    digest = "bd2079738bf102a1b4e223346f69650f1dcbe685994da65bf92d5207eb44e1cc"

    async with aiohttp.ClientSession() as session:
        async with session.post(
            f"http://localhost:{port}/v2/alpine/blobs/uploads/"
        ) as resp:
            assert resp.status == 202
            location = resp.headers["Location"]

        async with session.put(
            f"http://localhost:{port}{location}?digest=sha256:{digest}", data=b"9080"
        ) as resp:
            assert resp.status == 201
            location = f"http://localhost:{port}" + resp.headers["Location"]

        async with session.head(location) as resp:
            assert resp.status == 200

        async with session.delete(location) as resp:
            assert resp.status == 202

        async with session.head(location) as resp:
            assert resp.status == 404

        async with session.delete(location) as resp:
            assert resp.status == 404


async def test_list_tags(fake_cluster):
    port = fake_cluster["node1"]["registry_port"]

    manifest = {
        "manifests": [],
        "mediaType": "application/vnd.docker.distribution.manifest.list.v2+json",
        "schemaVersion": 2,
    }

    url = f"http://localhost:{port}/v2/alpine/manifests/3.11"

    async with aiohttp.ClientSession() as session:
        async with session.put(url, json=manifest) as resp:
            assert resp.status == 201
            hash = resp.headers["Docker-Content-Digest"].split(":", 1)[1]

        await assert_manifest(fake_cluster, hash, manifest)

        for node in fake_cluster.values():
            port = node["registry_port"]
            async with session.get(
                f"http://localhost:{port}/v2/alpine/tags/list"
            ) as resp:
                body = await resp.json()
                assert body == {"name": "alpine", "tags": ["3.11"]}


async def test_list_tags_pagination(fake_cluster):
    port = fake_cluster["node1"]["registry_port"]

    manifest = {
        "manifests": [],
        "mediaType": "application/vnd.docker.distribution.manifest.list.v2+json",
        "schemaVersion": 2,
    }

    async with aiohttp.ClientSession() as session:
        for tag in ("3.10", "3.11", "3.12"):
            url = f"http://localhost:{port}/v2/alpine/manifests/{tag}"

            async with session.put(url, json=manifest) as resp:
                assert resp.status == 201
                hash = resp.headers["Docker-Content-Digest"].split(":", 1)[1]

        await assert_manifest(fake_cluster, hash, manifest)

        for node in fake_cluster.values():
            port = node["registry_port"]

            async with session.get(
                f"http://localhost:{port}/v2/alpine/tags/list?n=1"
            ) as resp:
                body = await resp.json()
                assert body == {"name": "alpine", "tags": ["3.10"]}
                assert (
                    resp.headers["Link"]
                    == '/v2/alpine/tags/list?n=1&last=3.10; rel="next"'
                )


async def test_delete_manifest(fake_cluster):
    port = fake_cluster["node1"]["registry_port"]

    manifest = {
        "manifests": [],
        "mediaType": "application/vnd.docker.distribution.manifest.list.v2+json",
        "schemaVersion": 2,
    }

    url = f"http://localhost:{port}/v2/alpine/manifests/3.11"

    async with aiohttp.ClientSession() as session:
        async with session.put(url, json=manifest) as resp:
            assert resp.status == 201
            hash = resp.headers["Docker-Content-Digest"].split(":", 1)[1]

        await assert_manifest(fake_cluster, hash, manifest)

        manifest_url = f"http://localhost:{port}/v2/alpine/manifests/sha256:{hash}"

        async with session.head(manifest_url) as resp:
            assert resp.status == 200

        async with session.delete(manifest_url) as resp:
            assert resp.status == 202

        async with session.head(manifest_url) as resp:
            assert resp.status == 404

        async with session.delete(manifest_url) as resp:
            assert resp.status == 404


async def test_full_manifest_round_trip(fake_cluster):
    port = fake_cluster["node1"]["registry_port"]

    manifest = {
        "manifests": [],
        "mediaType": "application/vnd.docker.distribution.manifest.list.v2+json",
        "schemaVersion": 2,
    }

    url = f"http://localhost:{port}/v2/alpine/manifests/3.11"

    logger.critical("Starting put")
    async with aiohttp.ClientSession() as session:
        async with session.put(url, json=manifest) as resp:
            assert resp.status == 201
            hash = resp.headers["Docker-Content-Digest"].split(":", 1)[1]
    logger.critical("Finished put")

    await assert_manifest(fake_cluster, hash, manifest)

    digest, body = await get_manifest_byt_tag(port, "3.11")
    assert digest == hash
    assert body == manifest
