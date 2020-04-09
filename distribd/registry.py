import hashlib
import json
import logging
import os
import uuid

from aiofile import AIOFile, Writer
from aiohttp import web

from . import exceptions
from .actions import RegistryActions
from .utils.web import run_server

logger = logging.getLogger(__name__)

routes = web.RouteTableDef()


@routes.get("/v2")
async def handle_bare_v2(request):
    raise web.HTTPFound("/v2/")


@routes.get("/v2/")
async def handle_v2_root(request):
    return web.Response(text="")


@routes.get("/v2/{repository:[^{}]+}/tags/list")
async def list_images_in_repository(request):
    registry_state = request.app["registry_state"]
    repository = request.match_info["repository"]
    tags = registry_state.get_tags(repository)

    if not tags:
        raise exceptions.NameUnknown(repository=repository)

    return web.json_response({"name": repository, "tags": tags})


async def _manifest_by_hash(images_directory, repository: str, hash: str):
    manifest_path = images_directory / "manifests" / hash
    if not manifest_path.is_file():
        raise exceptions.ManifestUnknown(hash=hash)

    with open(manifest_path, "r") as fp:
        manifest = json.load(fp)

    return web.FileResponse(
        headers={
            "Docker-Content-Digest": f"sha256:{hash}",
            "Content-Type": manifest["mediaType"],
        },
        path=manifest_path,
    )


@routes.get("/v2/{repository:[^{}]+}/manifests/{tag}")
async def get_manifest_by_tag(request):
    registry_state = request.app["registry_state"]
    images_directory = request.app["images_directory"]

    repository = request.match_info["repository"]
    tag = request.match_info["tag"]

    try:
        hash = registry_state.get_tag(repository, tag)
    except KeyError:
        raise exceptions.ManifestUnknown(hash=hash)

    return await _manifest_by_hash(images_directory, repository, hash)


@routes.get("/v2/{repository:[^{}]+}/manifests/sha256:{hash}")
async def get_manifest_by_hash(request):
    images_directory = request.app["images_directory"]
    repository = request.match_info["repository"]
    hash = request.match_info["hash"]

    return await _manifest_by_hash(images_directory, repository, hash)


@routes.get("/v2/{repository:[^{}]+}/blobs/sha256:{hash}")
async def get_blob_by_hash(request):
    images_directory = request.app["images_directory"]
    registry_state = request.app["registry_state"]

    repository = request.match_info["repository"]
    hash = request.match_info["hash"]

    if not registry_state.is_blob_available(repository, hash):
        raise exceptions.BlobUnknown(hash=hash)

    hash_path = images_directory / "blobs" / hash
    if not hash_path.is_file():
        raise exceptions.BlobUnknown(hash=hash)

    return web.FileResponse(
        headers={
            "Docker-Content-Digest": f"sha256:{hash}",
            "Content-Type": "application/octet-stream",
        },
        path=hash_path,
    )


@routes.post("/v2/{repository:[^{}]+}/blobs/uploads/")
async def start_upload(request):
    repository = request.match_info["repository"]

    session_id = str(uuid.uuid4())

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

    uploads = images_directory / "uploads"
    if not uploads.exists():
        os.makedirs(uploads)

    upload_path = uploads / session_id

    async with AIOFile(upload_path, "ab") as fp:
        writer = Writer(fp)
        body = await request.read()
        await writer(body)
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

    uploads = images_directory / "uploads"
    if not uploads.exists():
        os.makedirs(uploads)

    upload_path = uploads / session_id

    # FIXME: This PUT might have some payload associated with it

    # FIXME: Do on each upload incrementally
    with open(upload_path, "rb") as fp:
        hash = hashlib.sha256(fp.read()).hexdigest()
        digest = f"sha256:{hash}"

    # FIXME: Read digest out of URL and make sure it matches

    blob_dir = images_directory / "blobs"
    if not blob_dir.exists():
        os.makedirs(blob_dir)

    blob_path = blob_dir / hash

    os.rename(upload_path, blob_path)

    send_action = request.app["send_action"]
    identifier = request.app["identifier"]

    await send_action(
        {"type": RegistryActions.BLOB_STORED, "hash": hash, "location": identifier}
    )

    await send_action(
        {"type": RegistryActions.BLOB_MOUNTED, "hash": hash, "repository": repository}
    )

    return web.json_response(
        {},
        status=201,
        headers={
            "Location": f"/v2/{repository}/blobs/{digest}",
            "Docker-Content-Digest": digest,
        },
    )


@routes.head("/v2/{repository:[^{}]+}/blobs/sha256:{hash}")
async def head_blob(request):
    images_directory = request.app["images_directory"]
    registry_state = request.app["registry_state"]

    repository = request.match_info["repository"]
    hash = request.match_info["hash"]

    if not registry_state.is_blob_available(repository, hash):
        raise exceptions.BlobUnknown(hash=hash)

    hash_path = images_directory / "blobs" / hash
    if not hash_path.is_file():
        raise exceptions.BlobUnknown(hash=hash)

    size = hash_path.stat().st_size

    return web.json_response(
        {},
        status=200,
        headers={
            "Content-Length": f"{size}",
            "Docker-Content-Digest": f"sha256:{hash}",
            "Content-Type": "application/octet-stream",
        },
    )


@routes.put("/v2/{repository:[^{}]+}/manifests/{tag}")
async def put_manifest(request):
    images_directory = request.app["images_directory"]
    repository = request.match_info["repository"]
    tag = request.match_info["tag"]

    manifest = await request.read()
    hash = hashlib.sha256(manifest).hexdigest()
    prefixed_hash = f"sha256:{hash}"

    manifests_dir = images_directory / "manifests"
    if not os.path.exists(manifests_dir):
        os.makedirs(manifests_dir)

    manifest_path = manifests_dir / hash

    async with AIOFile(manifest_path, "wb") as fp:
        writer = Writer(fp)
        await writer(manifest)
        await fp.fsync()

    send_action = request.app["send_action"]
    identifier = request.app["identifier"]

    await send_action(
        {"type": RegistryActions.MANIFEST_STORED, "hash": hash, "location": identifier}
    )

    # These 2 could be merged?

    await send_action(
        {
            "type": RegistryActions.MANIFEST_MOUNTED,
            "hash": hash,
            "repository": repository,
        }
    )

    await send_action(
        {
            "type": RegistryActions.HASH_TAGGED,
            "repository": repository,
            "tag": tag,
            "hash": hash,
        }
    )

    return web.json_response(
        {},
        status=200,
        headers={"Content-Length": "0", "Docker-Content-Digest": prefixed_hash},
    )


async def run_registry(identifier, registry_state, send_action, images_directory, port):
    return await run_server(
        "0.0.0.0",
        port,
        routes,
        identifier=identifier,
        registry_state=registry_state,
        send_action=send_action,
        images_directory=images_directory,
    )
