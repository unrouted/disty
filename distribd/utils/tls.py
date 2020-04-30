import ssl

import confuse


def _configure_context_common(context: ssl.SSLContext, config: confuse.ConfigView):
    if config["ciphers"].exists():
        context.set_ciphers(config["ciphers"].as_str())


def create_server_context(config: confuse.ConfigView):
    if not config.exists():
        return None

    context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)

    _configure_context_common(context, config)

    return context


def create_client_context(config: confuse.ConfigView):
    if not config.exists():
        return None

    context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)

    _configure_context_common(context, config)

    return context
