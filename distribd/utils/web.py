import asyncio

import aiohttp.web


async def run_server(bind_config, routes, access_log_class=None, **context):
    app = aiohttp.web.Application(client_max_size=1024 ** 3)

    for key, value in context.items():
        app[key] = value
    app.add_routes(routes)

    kwargs = {}
    if access_log_class:
        kwargs["access_log_class"] = access_log_class

    runner = aiohttp.web.AppRunner(app, handle_signals=False, **kwargs)
    await runner.setup()

    host = bind_config["address"].get(str)
    port = bind_config["port"].get(int)

    site = aiohttp.web.TCPSite(runner, host, port, shutdown_timeout=1.0)
    await site.start()

    actual_port = site._server.sockets[0].getsockname()[1]
    bind_config["port"].set(actual_port)

    try:
        # Sleep forever. No activity is needed.
        await asyncio.Event().wait()
    finally:
        # On any reason of exit, stop reporting the health.
        await asyncio.shield(runner.shutdown())
        await asyncio.shield(runner.cleanup())
