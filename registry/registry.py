import pathlib
import hashlib
import logging
from aiohttp import web

from .utils.web import run_server

images_directory = pathlib.Path("images")

manifests_by_hash = {}
manifests_by_path = {}
tags = {}
blobs = {}


def scan_images():
    for path in images_directory.glob("**/manifest.json"):
        with open(path, "rb") as fp:
            hash = hashlib.sha256(fp.read()).hexdigest()

        manifests_by_path[str(path)] = hash
        manifests_by_hash[hash] = path

        repository = str(path.parent.parent.relative_to(images_directory))
        tag = path.parent.name
        tags.setdefault(repository, []).append(tag)


scan_images()

routes = web.RouteTableDef()

@routes.get('/v2')
async def handle_bare_v2(request):
    raise web.HTTPFound('/v2/')


@routes.get('/v2/')
async def handle_v2_root(request):
    return web.Response(text="")


@routes.get('/v2/{repository:[^{}]+}/tags/list')
async def list_images_in_repository(request):
    repository = request.match_info["repository"]

    if repository not in tags:
        raise web.HTTPNotFound(
            headers={"Content-Type": "application/json"},
            text='{"errors": [{"message": "manifest tag did not match URI", "code": "TAG_INVALID", "detail": ""}]}',
        )

    return web.json_response({
        "name": repository,
        "tags": tags[repository],
    })


@routes.get('/v2/{repository:[^{}]+}/manifests/{tag}')
async def get_manifest_by_tag(request):
    print("!!!!")
    # Return images/repository/tag/manifest.json
    repository = request.match_info["repository"]
    tag = request.match_info["tag"]

    manifest_path = images_directory / repository / tag / "manifest.json"
    if not manifest_path.is_file():
        raise web.HTTPNotFound(
            headers={"Content-Type": "application/json"},
            text='{"errors": [{"message": "manifest tag did not match URI", "code": "TAG_INVALID", "detail": ""}]}',
        )

    hash = manifests_by_path[str(manifest_path)]

    return web.FileResponse(
        headers={
            "Docker-Content-Digest": f"sha256:{hash}"
        },
        path=manifest_path,
    )


@routes.get('/v2/{repository:[^{}]+}/manifests/sha256:{hash}')
async def get_manifest_by_hash(request):
    repository = request.match_info["repository"]
    hash = request.match_info["hash"]

    if hash not in manifests_by_hash:
        raise web.HTTPNotFound(
            headers={"Content-Type": "application/json"},
            text='{"errors": [{"message": "manifest tag did not match URI", "code": "TAG_INVALID", "detail": ""}]}',
        )

    manifest_path = manifests_by_hash[hash]

    return web.FileResponse(
        headers={
            "Docker-Content-Digest": f"sha256:{hash}"
        },
        path=manifest_path,
    )



@routes.get('/v2/{repository:[^{}]+}/blobs/sha255:{hash}')
async def get_blob_by_hash(request):
    repository = request.match_info["repository"]
    hash = request.match_info["hash"]

    repository_dir = images_directory / repository
    if not repository_dir.is_dir():
        raise web.HTTPNotFound(
            headers={"Content-Type": "application/json"},
            text='{"errors": [{"message": "manifest tag did not match URI", "code": "TAG_INVALID", "detail": ""}]}',
        )

    hash_path = repository_dir / hash
    if not hash_path.is_file():
        raise web.HTTPNotFound(
            headers={"Content-Type": "application/json"},
            text='{"errors": [{"message": "manifest tag did not match URI", "code": "TAG_INVALID", "detail": ""}]}',
        )

    return web.FileResponse(
        headers={},
        path=hash_path,
    )


@routes.post('/v2/{repository:[^{}]+}/blobs/uploads/')
async def start_upload(request):
    repository = request.match_info["repository"]

    session_id = "session-id"

    return web.json_response(
        {},
        status=202,
        headers={
            "Location": f"/v2/{repository}/blobs/uploads/{session_id}",
            "Range": "0-0",
            "Blob-Upload-Session-ID": session_id,
        }
    )


@routes.patch('/v2/{repository:[^{}]+}/blobs/uploads/{session_id}')
async def upload_chunk_by_patch(request):
    repository = request.match_info["repository"]
    session_id = request.match_info["session_id"]

    body = await request.read()
    current_length = len(body)

    return web.json_response(
        {},
        status=202,
        headers={
            "Location": f"/v2/{repository}/blobs/uploads/{session_id}",
            "Blob-Upload-Session-ID": session_id,
            "Range": f"0-{current_length}",
        }
    )


@routes.put('/v2/{repository:[^{}]+}/blobs/uploads/{session_id}')
async def upload_finish(request):
    repository = request.match_info["repository"]
    session_id = request.match_info["session_id"]

    digest = "sha256:6c3c624b58dbbcd3c0dd82b4c53f04194d1247c6eebdaab7c610cf7d66709b3b"

    return web.json_response(
        {},
        status=201,
        headers={
            "Location": f"/v2/{repository}/blobs/{digest}",
            "Docker-Content-Digest": digest,
        }
    )


@routes.head('/v2/{repository:[^{}]+}/blobs/{hash}')
async def head_blob(request):
    repository = request.match_info["repository"]
    hash = request.match_info["hash"]

    return web.json_response(
        {},
        status=200,
        headers={
            "Content-Length": "0",
            "Docker-Content-Digest": hash,
        }
    )


@routes.put('/v2/{repository:[^{}]+}/manifests/{tag}')
async def head_blob(request):
    repository = request.match_info["repository"]
    tag = request.match_info["tag"]

    manifest = await request.read()
    hash = "sha256:" + hashlib.sha256(manifest).hexdigest()

    return web.json_response(
        {},
        status=200,
        headers={
            "Content-Length": "0",
            "Docker-Content-Digest": hash,
        }
    )


async def run_registry(port):
    return await run_server(
        "0.0.0.0",
        port,
        routes,
    )
