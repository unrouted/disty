import asyncio

import aiohttp.web

from .tls import create_server_context


async def run_server(
    raft, server_name, bind_config, routes, access_log_class=None, **context
):
    app = aiohttp.web.Application(client_max_size=1024 ** 3)

    for key, value in context.items():
        app[key] = value
    app["raft"] = raft
    app.add_routes(routes)

    kwargs = {}
    if access_log_class:
        kwargs["access_log_class"] = access_log_class

    runner = aiohttp.web.AppRunner(app, handle_signals=False, **kwargs)
    await runner.setup()

    host = bind_config["address"].get(str)
    port = bind_config["port"].get(int)

    site = aiohttp.web.TCPSite(
        runner,
        host,
        port,
        shutdown_timeout=1.0,
        ssl_context=create_server_context(bind_config["tls"]),
    )
    await site.start()

    sockname = site._server.sockets[0].getsockname()
    bind_config["address"].set(sockname[0])
    bind_config["port"].set(sockname[1])

    raft.state_changed(**{server_name: {"address": sockname[0], "port": sockname[1]}})

    try:
        # Sleep forever. No activity is needed.
        await asyncio.Event().wait()
    finally:
        # On any reason of exit, stop reporting the health.
        await asyncio.shield(runner.shutdown())
        await asyncio.shield(runner.cleanup())
