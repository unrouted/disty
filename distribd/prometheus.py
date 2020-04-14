from aiohttp import web
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from .utils.web import run_server

routes = web.RouteTableDef()


@routes.get("/healthz")
async def ok(request):
    return web.json_response({"ok": True})


@routes.get("/metrics")
async def metrics(request):
    return web.Response(
        body=generate_latest(), headers={"Content-Type": CONTENT_TYPE_LATEST}
    )


async def run_prometheus(identifier, registry_state, images_directory, port):
    return await run_server(
        "0.0.0.0",
        port,
        routes,
        identifier=identifier,
        registry_state=registry_state,
        images_directory=images_directory,
    )
