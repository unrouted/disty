import ssl

import confuse
from distribd.utils.tls import create_client_context, create_server_context


def test_create_client_context_unencrypted():
    config = confuse.Configuration("test", read=False)

    context = create_client_context(config["registry"]["tls"])
    assert context is None


def test_create_client_context_encrypted():
    config = confuse.Configuration("test", read=False)
    config["registry"]["tls"]["key"] = "registry.key"
    config["registry"]["tls"]["ca"] = "ca.pem"
    config["registry"]["tls"]["cert"] = "cert.pem"

    context = create_client_context(config["registry"]["tls"])

    assert isinstance(context, ssl.SSLContext)


def test_create_server_context_unencrypted():
    config = confuse.Configuration("test", read=False)

    context = create_server_context(config["registry"]["tls"])
    assert context is None


def test_create_server_context_encrypted():
    config = confuse.Configuration("test", read=False)
    config["registry"]["tls"]["key"] = "registry.key"
    config["registry"]["tls"]["ca"] = "ca.pem"
    config["registry"]["tls"]["cert"] = "cert.pem"

    context = create_server_context(config["registry"]["tls"])

    assert isinstance(context, ssl.SSLContext)
