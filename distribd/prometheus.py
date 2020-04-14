from aiohttp import web

from .utils.web import run_server

routes = web.RouteTableDef()


@routes.get("/ok")
async def ok(request):
    return web.json_response({"ok": True})


@routes.get("/metrics")
async def metrics(request):
    return web.json_response({"ok": True})


async def run_prometheus(identifier, registry_state, images_directory, port):
    return await run_server(
        "0.0.0.0",
        port,
        routes,
        identifier=identifier,
        registry_state=registry_state,
        images_directory=images_directory,
    )
