import asyncio
import hashlib
import json
import logging

import aiohttp
from distribd.machine import NodeState
from distribd.service import main
import pytest

logger = logging.getLogger(__name__)


async def check_consensus(cluster_config, session):
    consensus = set()
    states = set()

    for node, config in cluster_config.items():
        port = config["raft"]["port"].get(int)

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
async def fake_cluster(loop, cluster_config, tmp_path, monkeypatch, client_session):
    servers = asyncio.ensure_future(
        asyncio.gather(
            main([], cluster_config["node1"]),
            main([], cluster_config["node2"]),
            main([], cluster_config["node3"]),
        )
    )

    await asyncio.sleep(0.1)

    seeds = []
    for node in ("node1", "node2", "node3"):
        raft_port = cluster_config[node]["raft"]["port"].get()
        if raft_port != 0:
            seeds.append(f"http://127.0.0.1:{raft_port}")

    for node in ("node1", "node2", "node3"):
        cluster_config[node]["seeding"]["urls"].set(seeds)

    for i in range(100):
        if await check_consensus(cluster_config, client_session):
            break

        await asyncio.sleep(0.1)
    else:
        raise RuntimeError("No consensus")

    yield cluster_config

    # Cancel servers. Ignore CancelledError.
    servers.cancel()
    try:
        await servers
    except asyncio.CancelledError:
        pass


async def test_v2_redir(fake_cluster, client_session):
    for node in ("node1", "node2", "node3"):
        port = fake_cluster[node]["registry"]["port"].get(int)

        async with client_session.get(
            f"http://localhost:{port}/v2", allow_redirects=False
        ) as resp:
            assert resp.status == 302
            assert resp.headers["Location"] == "/v2/"


async def test_list_tags_404(fake_cluster, client_session):
    for node in ("node1", "node2", "node3"):
        port = fake_cluster[node]["registry"]["port"].get(int)

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
        port = fake_cluster[node]["registry"]["port"].get(int)
        content_length, body = await get_blob(port, hash)
        assert content_length == "4"
        assert body == b"9080"


async def assert_manifest(fake_cluster, hash, expected_body):
    for node in ("node1", "node2", "node3"):
        port = fake_cluster[node]["registry"]["port"].get(int)
        logger.critical("Getting manifest for port %s", port)
        content_length, body = await get_manifest(port, hash)
        assert body == expected_body


async def test_put_blob_fail_invalid_hash(fake_cluster):
    port = fake_cluster["node1"]["registry"]["port"].get(int)

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
    port = fake_cluster["node1"]["registry"]["port"].get(int)
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
    port = fake_cluster["node1"]["registry"]["port"].get(int)
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
    port = fake_cluster["node1"]["registry"]["port"].get(int)
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
    port = fake_cluster["node1"]["registry"]["port"].get(int)
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
    port = fake_cluster["node1"]["registry"]["port"].get(int)

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
    port = fake_cluster["node1"]["registry"]["port"].get(int)
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
    port = fake_cluster["node1"]["registry"]["port"].get(int)

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
            port = node["registry"]["port"].get(int)
            async with session.get(
                f"http://localhost:{port}/v2/alpine/tags/list"
            ) as resp:
                body = await resp.json()
                assert body == {"name": "alpine", "tags": ["3.11"]}


async def test_list_tags_pagination(fake_cluster):
    port = fake_cluster["node1"]["registry"]["port"].get(int)

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
            port = node["registry"]["port"].get(int)

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
    port = fake_cluster["node1"]["registry"]["port"].get(int)

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
    port = fake_cluster["node1"]["registry"]["port"].get(int)

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


async def create_test_blob_from_json(fake_cluster, obj):
    port = fake_cluster["node1"]["registry"]["port"].get(int)

    serialized = json.dumps(obj).encode("utf-8")
    digest = "sha256:" + hashlib.sha256(serialized).hexdigest()

    async with aiohttp.ClientSession() as session:
        async with session.post(
            f"http://localhost:{port}/v2/alpine/blobs/uploads/"
        ) as resp:
            assert resp.status == 202
            location = resp.headers["Location"]

        async with session.put(
            f"http://localhost:{port}{location}?digest={digest}", data=serialized
        ) as resp:
            assert resp.status == 201
            location = f"http://localhost:{port}" + resp.headers["Location"]

    return digest


async def test_validation_of_inner_manifest_works(fake_cluster):
    port = fake_cluster["node1"]["registry"]["port"].get(int)

    inner_manifest = await create_test_blob_from_json(
        fake_cluster,
        {
            "manifests": [],
            "mediaType": "application/vnd.docker.distribution.manifest.list.v2+json",
            "schemaVersion": 2,
        },
    )

    manifest = {
        "manifests": [
            {
                "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
                "digest": inner_manifest,
                "size": 0,
                "platform": {"os": "linux", "architecture": "amd64"},
            }
        ],
        "mediaType": "application/vnd.docker.distribution.manifest.list.v2+json",
        "schemaVersion": 2,
    }

    headers = {
        "Content-Type": "application/vnd.docker.distribution.manifest.list.v2+json"
    }

    url = f"http://localhost:{port}/v2/alpine/manifests/3.11"

    async with aiohttp.ClientSession() as session:
        async with session.put(url, json=manifest, headers=headers) as resp:
            assert resp.status == 400
            assert await resp.json() == {
                "errors": [
                    {
                        "code": "MANIFEST_INVALID",
                        "message": "manifest invalid",
                        "detail": {
                            "reason": "sha256:533622ac90715ccb3fe2659fb9b9d7fc9ae2e261945b02c03a950c2e2027f2e5 invalid"
                        },
                    }
                ]
            }


async def test_inner_resource_must_exist(fake_cluster):
    port = fake_cluster["node1"]["registry"]["port"].get(int)

    manifest = {
        "manifests": [
            {
                "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
                "digest": "sha256:533622ac90715ccb3fe2659fb9b9d7fc9ae2e261945b02c03a950c2e2027f2e5",
                "size": 0,
                "platform": {"os": "linux", "architecture": "amd64"},
            }
        ],
        "mediaType": "application/vnd.docker.distribution.manifest.list.v2+json",
        "schemaVersion": 2,
    }

    headers = {
        "Content-Type": "application/vnd.docker.distribution.manifest.list.v2+json"
    }

    url = f"http://localhost:{port}/v2/alpine/manifests/3.11"

    async with aiohttp.ClientSession() as session:
        async with session.put(url, json=manifest, headers=headers) as resp:
            assert resp.status == 400
            assert await resp.json() == {
                "errors": [
                    {
                        "code": "MANIFEST_INVALID",
                        "message": "manifest invalid",
                        "detail": {
                            "reason": "sha256:533622ac90715ccb3fe2659fb9b9d7fc9ae2e261945b02c03a950c2e2027f2e5 missing"
                        },
                    }
                ]
            }
