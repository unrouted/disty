import asyncio

import aiohttp.web


async def run_server(host, port, routes, access_log_class=None, **context):
    app = aiohttp.web.Application(client_max_size=1024 ** 3)

    for key, value in context.items():
        app[key] = value
    app.add_routes(routes)

    kwargs = {}
    if access_log_class:
        kwargs["access_log_class"] = access_log_class

    runner = aiohttp.web.AppRunner(app, handle_signals=False, **kwargs)
    await runner.setup()

    site = aiohttp.web.TCPSite(runner, host, port, shutdown_timeout=1.0)
    await site.start()

    try:
        # Sleep forever. No activity is needed.
        await asyncio.Event().wait()
    finally:
        # On any reason of exit, stop reporting the health.
        await asyncio.shield(runner.cleanup())
