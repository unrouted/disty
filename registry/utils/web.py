import asyncio

import aiohttp.web


async def run_server(host, port, routes, **context):
    app = aiohttp.web.Application()
    for key, value in context.items():
        app[key] = value
    app.add_routes(routes)

    runner = aiohttp.web.AppRunner(app, handle_signals=False)
    await runner.setup()

    site = aiohttp.web.TCPSite(runner, host, port, shutdown_timeout=1.0)
    await site.start()

    try:
        # Sleep forever. No activity is needed.
        await asyncio.Event().wait()
    finally:
        # On any reason of exit, stop reporting the health.
        await asyncio.shield(runner.cleanup())
