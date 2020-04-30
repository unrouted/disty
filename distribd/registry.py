import hashlib
import json
import logging
import os
import uuid

from aiofile import AIOFile, Writer
from aiohttp import web
from yarl import URL

from . import exceptions
from .actions import RegistryActions
from .analyzer import recursive_analyze
from .raft import Raft
from .utils.registry import get_blob_path, get_manifest_path
from .utils.tokenchecker import TokenChecker
from .utils.web import run_server

logger = logging.getLogger(__name__)

routes = web.RouteTableDef()


@routes.get("/v2")
async def handle_bare_v2(request):
    raise web.HTTPFound("/v2/")


@routes.get("/v2/")
async def handle_v2_root(request):
    request.app["token_checker"].authenticate(request)
    return web.Response(text="")


@routes.get("/v2/{repository:[^{}]+}/tags/list")
async def list_images_in_repository(request):
    registry_state = request.app["registry_state"]
    repository = request.match_info["repository"]

    request.app["token_checker"].authenticate(request, repository, ["pull"])

    try:
        tags = registry_state.get_tags(repository)
    except KeyError:
        raise exceptions.NameUnknown(repository=repository)

    tags.sort()

    include_link = False

    last = request.query.get("last", None)
    if last:
        start = tags.index(last)
        tags = tags[start:]

    n = request.query.get("n", None)
    if n is not None:
        n = int(n)
        if n < len(tags):
            include_link = True

        tags = tags[:n]

    headers = {}

    if include_link:
        url = URL(f"/v2/{repository}/tags/list")
        if n is not None:
            url = url.update_query({"n": str(n)})
        url = url.update_query({"last": tags[-1]})
        headers["Link"] = f'{url}; rel="next"'

    return web.json_response({"name": repository, "tags": tags}, headers=headers)


async def _manifest_head_by_hash(images_directory, repository: str, hash: str):
    manifest_path = get_manifest_path(images_directory, hash)
    if not manifest_path.is_file():
        raise exceptions.ManifestUnknown(hash=hash)

    size = os.path.getsize(manifest_path)

    return web.Response(
        status=200,
        headers={"Content-Length": str(size), "Docker-Content-Digest": f"{hash}"},
    )


@routes.head("/v2/{repository:[^{}]+}/manifests/sha256:{hash}")
async def head_manifest_by_hash(request):
    images_directory = request.app["images_directory"]
    repository = request.match_info["repository"]
    hash = "sha256:" + request.match_info["hash"]

    request.app["token_checker"].authenticate(request, repository, ["pull"])

    registry_state = request.app["registry_state"]
    if not registry_state.is_manifest_available(repository, hash):
        raise exceptions.ManifestUnknown(hash=hash)

    return await _manifest_head_by_hash(images_directory, repository, hash)


@routes.head("/v2/{repository:[^{}]+}/manifests/{tag}")
async def head_manifest_by_tag(request):
    registry_state = request.app["registry_state"]
    images_directory = request.app["images_directory"]

    repository = request.match_info["repository"]
    tag = request.match_info["tag"]

    request.app["token_checker"].authenticate(request, repository, ["pull"])

    try:
        hash = registry_state.get_tag(repository, tag)
    except KeyError:
        raise exceptions.ManifestUnknown(tag=tag)

    registry_state = request.app["registry_state"]
    if not registry_state.is_manifest_available(repository, hash):
        raise exceptions.ManifestUnknown(hash=hash)

    return await _manifest_head_by_hash(images_directory, repository, hash)


async def _manifest_by_hash(images_directory, repository: str, hash: str):
    manifest_path = get_manifest_path(images_directory, hash)
    if not manifest_path.is_file():
        raise exceptions.ManifestUnknown(hash=hash)

    async with AIOFile(manifest_path, "r") as fp:
        manifest = json.loads(await fp.read())

    size = os.path.getsize(manifest_path)

    return web.FileResponse(
        headers={
            "Docker-Content-Digest": f"{hash}",
            "Content-Type": manifest["mediaType"],
            "Content-Length": str(size),
        },
        path=manifest_path,
    )


@routes.get("/v2/{repository:[^{}]+}/manifests/sha256:{hash}")
async def get_manifest_by_hash(request):
    images_directory = request.app["images_directory"]
    repository = request.match_info["repository"]
    hash = "sha256:" + request.match_info["hash"]

    request.app["token_checker"].authenticate(request, repository, ["pull"])

    registry_state = request.app["registry_state"]
    if not registry_state.is_manifest_available(repository, hash):
        raise exceptions.ManifestUnknown(hash=hash)

    return await _manifest_by_hash(images_directory, repository, hash)


@routes.get("/v2/{repository:[^{}]+}/manifests/{tag}")
async def get_manifest_by_tag(request):
    registry_state = request.app["registry_state"]
    images_directory = request.app["images_directory"]

    repository = request.match_info["repository"]
    tag = request.match_info["tag"]

    request.app["token_checker"].authenticate(request, repository, ["pull"])

    try:
        hash = registry_state.get_tag(repository, tag)
    except KeyError:
        raise exceptions.ManifestUnknown(tag=tag)

    registry_state = request.app["registry_state"]
    if not registry_state.is_manifest_available(repository, hash):
        raise exceptions.ManifestUnknown(hash=hash)

    return await _manifest_by_hash(images_directory, repository, hash)


@routes.delete("/v2/{repository:[^{}]+}/manifests/sha256:{hash}")
async def delete_manifest_by_hash(request):
    registry_state = request.app["registry_state"]
    repository = request.match_info["repository"]
    hash = "sha256:" + request.match_info["hash"]

    request.app["token_checker"].authenticate(request, repository, ["push"])

    if not registry_state.is_manifest_available(repository, hash):
        raise exceptions.ManifestUnknown(hash=hash)

    send_action = request.app["send_action"]

    success = await send_action(
        [
            {
                "type": RegistryActions.MANIFEST_UNMOUNTED,
                "hash": hash,
                "repository": repository,
                "user": request["user"],
            },
        ]
    )

    if not success:
        raise exceptions.LeaderUnavailable()

    return web.Response(status=202, headers={"Content-Length": "0"})


@routes.delete("/v2/{repository:[^{}]+}/manifests/{tag}")
async def delete_manifest_by_ref(request):
    repository = request.match_info["repository"]
    request.app["token_checker"].authenticate(request, repository, ["push"])

    raise exceptions.Unsupported()


@routes.head("/v2/{repository:[^{}]+}/blobs/sha256:{hash}")
async def head_blob(request):
    images_directory = request.app["images_directory"]
    registry_state = request.app["registry_state"]

    repository = request.match_info["repository"]
    hash = "sha256:" + request.match_info["hash"]

    request.app["token_checker"].authenticate(request, repository, ["pull"])

    if not registry_state.is_blob_available(repository, hash):
        raise exceptions.BlobUnknown(hash=hash)

    hash_path = get_blob_path(images_directory, hash)
    if not hash_path.is_file():
        raise exceptions.BlobUnknown(hash=hash)

    size = hash_path.stat().st_size

    return web.Response(
        status=200,
        headers={
            "Content-Length": f"{size}",
            "Docker-Content-Digest": hash,
            "Content-Type": "application/octet-stream",
        },
    )


@routes.get("/v2/{repository:[^{}]+}/blobs/sha256:{hash}")
async def get_blob_by_hash(request):
    images_directory = request.app["images_directory"]
    registry_state = request.app["registry_state"]

    repository = request.match_info["repository"]
    hash = "sha256:" + request.match_info["hash"]

    request.app["token_checker"].authenticate(request, repository, ["pull"])

    if not registry_state.is_blob_available(repository, hash):
        raise exceptions.BlobUnknown(hash=hash)

    hash_path = get_blob_path(images_directory, hash)
    if not hash_path.is_file():
        raise exceptions.BlobUnknown(hash=hash)

    size = os.path.getsize(hash_path)

    return web.FileResponse(
        headers={
            "Docker-Content-Digest": hash,
            "Content-Type": "application/octet-stream",
            "Content-Length": str(size),
        },
        path=hash_path,
    )


@routes.delete("/v2/{repository:[^{}]+}/blobs/sha256:{hash}")
async def delete_blob_by_hash(request):
    registry_state = request.app["registry_state"]

    repository = request.match_info["repository"]
    hash = "sha256:" + request.match_info["hash"]

    request.app["token_checker"].authenticate(request, repository, ["push"])

    if not registry_state.is_blob_available(repository, hash):
        raise exceptions.BlobUnknown(hash=hash)

    send_action = request.app["send_action"]

    success = await send_action(
        [
            {
                "type": RegistryActions.BLOB_UNMOUNTED,
                "hash": hash,
                "repository": repository,
                "user": request["user"],
            },
        ]
    )

    if not success:
        raise exceptions.LeaderUnavailable()

    return web.Response(status=202, headers={"Content-Length": "0"})


@routes.post("/v2/{repository:[^{}]+}/blobs/uploads/")
async def start_upload(request):
    images_directory = request.app["images_directory"]

    repository = request.match_info["repository"]
    mount_digest = request.query.get("mount", "")
    mount_repository = request.query.get("from", "")

    request.app["token_checker"].authenticate(request, repository, ["push"])

    if mount_digest and mount_repository:
        request.app["token_checker"].authenticate(request, mount_repository, ["pull"])

        if mount_repository == repository:
            raise exceptions.BlobUploadInvalid(
                mount=mount_digest, repository=mount_repository
            )

        registry_state = request.app["registry_state"]
        if registry_state.is_blob_available(mount_repository, mount_digest):
            send_action = request.app["send_action"]

            success = await send_action(
                [
                    {
                        "type": RegistryActions.BLOB_MOUNTED,
                        "hash": mount_digest,
                        "repository": repository,
                        "user": request["user"],
                    },
                ]
            )

            if not success:
                logger.warning(
                    "Can cross-mount %s from %s to %s but failed to commit to journal",
                    mount_digest,
                    mount_repository,
                    repository,
                )
                raise exceptions.BlobUploadInvalid(
                    mount=mount_digest, repository=mount_repository
                )

            return web.Response(
                status=201,
                headers={
                    "Location": f"/v2/{repository}/blobs/{mount_digest}",
                    "Content-Length": "0",
                    "Docker-Content-Digest": mount_digest,
                },
            )

    session_id = str(uuid.uuid4())

    session = request.app["sessions"][session_id] = {
        "hasher": hashlib.sha256(),
        "size": 0,
    }

    expected_digest = request.query.get("digest", None)
    if expected_digest:
        uploads = images_directory / "uploads"
        if not uploads.exists():
            os.makedirs(uploads)

        upload_path = uploads / session_id

        async with AIOFile(upload_path, "ab") as fp:
            writer = Writer(fp)
            chunk = await request.content.read(1024 * 1024)
            while chunk:
                await writer(chunk)
                session["hasher"].update(chunk)
                session["size"] += len(chunk)
                chunk = await request.content.read(1024 * 1024)
            await fp.fsync()

        hash = session["hasher"].hexdigest()
        digest = f"sha256:{hash}"

        if expected_digest != digest:
            raise exceptions.BlobUploadInvalid()

        blob_path = get_blob_path(images_directory, digest)
        blob_dir = blob_path.parent
        if not blob_dir.exists():
            os.makedirs(blob_dir)

        os.rename(upload_path, blob_path)

        send_action = request.app["send_action"]
        identifier = request.app["identifier"]

        success = await send_action(
            [
                {
                    "type": RegistryActions.BLOB_MOUNTED,
                    "hash": digest,
                    "repository": repository,
                    "user": request["user"],
                },
                {
                    "type": RegistryActions.BLOB_STAT,
                    "hash": digest,
                    "size": session["size"],
                },
                {
                    "type": RegistryActions.BLOB_STORED,
                    "hash": digest,
                    "location": identifier,
                    "user": request["user"],
                },
            ]
        )

        if not success:
            raise exceptions.BlobUploadInvalid()

        return web.Response(
            status=201,
            headers={
                "Location": f"/v2/{repository}/blobs/{digest}",
                "Docker-Content-Digest": digest,
            },
        )

    return web.json_response(
        {},
        status=202,
        headers={
            "Location": f"/v2/{repository}/blobs/uploads/{session_id}",
            "Range": "0-0",
            "Blob-Upload-Session-ID": session_id,
        },
    )


@routes.patch("/v2/{repository:[^{}]+}/blobs/uploads/{session_id}")
async def upload_chunk_by_patch(request):
    images_directory = request.app["images_directory"]
    repository = request.match_info["repository"]
    session_id = request.match_info["session_id"]

    request.app["token_checker"].authenticate(request, repository, ["push"])

    session = request.app["sessions"].get(session_id, None)
    if not session:
        raise exceptions.BlobUploadInvalid(session=session_id)

    uploads = images_directory / "uploads"
    if not uploads.exists():
        os.makedirs(uploads)

    upload_path = uploads / session_id

    content_range = request.headers.get("Content-Range", "")
    if content_range:
        size = 0
        if os.path.exists(upload_path):
            size = os.path.getsize(upload_path)

        content_range = request.headers["Content-Range"]
        left, right = content_range.split("-")

        if int(left) != size:
            raise web.HTTPRequestRangeNotSatisfiable(
                headers={
                    "Location": f"/v2/{repository}/blobs/uploads/{session_id}",
                    "Range": f"0-{size}",
                    "Content-Length": "0",
                    "Blob-Upload-Session-ID": session_id,
                }
            )

    async with AIOFile(upload_path, "ab") as fp:
        writer = Writer(fp)
        chunk = await request.content.read(1024 * 1024)
        while chunk:
            await writer(chunk)
            session["hasher"].update(chunk)
            session["size"] += len(chunk)
            chunk = await request.content.read(1024 * 1024)
        await fp.fsync()

    info = os.stat(upload_path)

    return web.json_response(
        {},
        status=202,
        headers={
            "Location": f"/v2/{repository}/blobs/uploads/{session_id}",
            "Blob-Upload-Session-ID": session_id,
            "Range": f"0-{info.st_size}",
        },
    )


@routes.put("/v2/{repository:[^{}]+}/blobs/uploads/{session_id}")
async def upload_finish(request):
    images_directory = request.app["images_directory"]
    repository = request.match_info["repository"]
    session_id = request.match_info["session_id"]
    expected_digest = request.query.get("digest", "")

    request.app["token_checker"].authenticate(request, repository, ["push"])

    session = request.app["sessions"].get(session_id, None)
    if not session:
        raise exceptions.BlobUploadInvalid(session=session_id)

    uploads = images_directory / "uploads"
    if not uploads.exists():
        os.makedirs(uploads)

    upload_path = uploads / session_id

    async with AIOFile(upload_path, "ab") as fp:
        writer = Writer(fp)
        chunk = await request.content.read(1024 * 1024)
        while chunk:
            await writer(chunk)
            session["hasher"].update(chunk)
            session["size"] += len(chunk)
            chunk = await request.content.read(1024 * 1024)
        await fp.fsync()

    hash = session["hasher"].hexdigest()
    digest = f"sha256:{hash}"

    if expected_digest != digest:
        raise exceptions.BlobUploadInvalid()

    blob_path = get_blob_path(images_directory, digest)
    blob_dir = blob_path.parent
    if not blob_dir.exists():
        os.makedirs(blob_dir)

    os.rename(upload_path, blob_path)

    send_action = request.app["send_action"]
    identifier = request.app["identifier"]

    success = await send_action(
        [
            {
                "type": RegistryActions.BLOB_MOUNTED,
                "hash": digest,
                "repository": repository,
                "user": request["user"],
            },
            {
                "type": RegistryActions.BLOB_STAT,
                "hash": digest,
                "size": session["size"],
            },
            {
                "type": RegistryActions.BLOB_STORED,
                "hash": digest,
                "location": identifier,
                "user": request["user"],
            },
        ]
    )

    if not success:
        raise exceptions.BlobUploadInvalid()

    return web.json_response(
        {},
        status=201,
        headers={
            "Location": f"/v2/{repository}/blobs/{digest}",
            "Docker-Content-Digest": digest,
        },
    )


@routes.get("/v2/{repository:[^{}]+}/blobs/uploads/{session_id}")
async def upload_status(request):
    images_directory = request.app["images_directory"]
    session_id = request.match_info["session_id"]
    repository = request.match_info["repository"]

    request.app["token_checker"].authenticate(request, repository, ["push"])

    upload_path = images_directory / "uploads" / session_id
    if not upload_path.exists():
        raise exceptions.BlobUploadUnknown()

    size = os.path.getsize(upload_path)

    return web.Response(
        status=204,
        headers={
            "Content-Length": "0",
            "Location": f"/v2/{repository}/blobs/uploads/{session_id}",
            "Range": f"0-{size}",
        },
    )


@routes.delete("/v2/{repository:[^{}]+}/blobs/uploads/{session_id}")
async def cancel_upload(request):
    images_directory = request.app["images_directory"]
    repository = request.match_info["repository"]
    session_id = request.match_info["session_id"]

    request.app["token_checker"].authenticate(request, repository, ["push"])

    upload_path = images_directory / "uploads" / session_id
    if not upload_path.exists():
        raise exceptions.BlobUploadUnknown()

    try:
        upload_path.unlink()
    except Exception:
        pass

    return web.Response(status=204, headers={"Content-Length": "0"})


@routes.put("/v2/{repository:[^{}]+}/manifests/{tag}")
async def put_manifest(request):
    mirrorer = request.app["mirrorer"]
    images_directory = request.app["images_directory"]
    repository = request.match_info["repository"]
    tag = request.match_info["tag"]

    request.app["token_checker"].authenticate(request, repository, ["push"])

    content_type = request.headers.get("Content-Type", "")
    if not content_type:
        raise exceptions.ManifestInvalid(reason="no_content_type")

    manifest = await request.read()
    content_size = len(manifest)

    # This makes sure the manifest is somewhat valid
    try:
        direct_deps, dependencies = await recursive_analyze(
            mirrorer, repository, content_type, manifest
        )
    except Exception:
        logger.exception("Error while validating manifest")
        raise

    hash = hashlib.sha256(manifest).hexdigest()
    prefixed_hash = f"sha256:{hash}"

    manifest_path = get_manifest_path(images_directory, prefixed_hash)
    manifests_dir = manifest_path.parent

    if not os.path.exists(manifests_dir):
        os.makedirs(manifests_dir)

    async with AIOFile(manifest_path, "wb") as fp:
        writer = Writer(fp)
        await writer(manifest)
        await fp.fsync()

    send_action = request.app["send_action"]
    identifier = request.app["identifier"]

    success = await send_action(
        dependencies
        + [
            {
                "type": RegistryActions.MANIFEST_MOUNTED,
                "hash": prefixed_hash,
                "repository": repository,
                "user": request["user"],
            },
            {
                "type": RegistryActions.MANIFEST_STORED,
                "hash": prefixed_hash,
                "location": identifier,
                "user": request["user"],
            },
            {
                "type": RegistryActions.MANIFEST_STAT,
                "hash": prefixed_hash,
                "size": content_size,
            },
            {
                "type": RegistryActions.MANIFEST_INFO,
                "hash": prefixed_hash,
                "dependencies": direct_deps,
                "content_type": content_type,
            },
            {
                "type": RegistryActions.HASH_TAGGED,
                "repository": repository,
                "tag": tag,
                "hash": prefixed_hash,
                "user": request["user"],
            },
        ]
    )

    if not success:
        raise exceptions.ManifestInvalid()

    return web.Response(
        status=201,
        headers={
            "Location": f"/v2/{repository}/manifests/{prefixed_hash}",
            "Docker-Content-Digest": prefixed_hash,
        },
    )


async def run_registry(
    raft: Raft, name, config, identifier, registry_state, images_directory, mirrorer
):
    token_checker = TokenChecker(config)

    return await run_server(
        raft,
        f"registry.{name}",
        config,
        routes,
        identifier=identifier,
        registry_state=registry_state,
        send_action=raft.append,
        images_directory=images_directory,
        sessions={},
        token_checker=token_checker,
        mirrorer=mirrorer,
    )
