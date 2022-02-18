from confuse import Configuration

from distribd.webhook import WebhookManager


async def test_configuration():
    c = Configuration("distribd", __name__)
    c["webhooks"].set([{"url": "http://foo.bar.svc:1234", "matcher": "foo:.*"}])

    wh = WebhookManager(c)

    assert wh.webhooks[0]["url"] == "http://foo.bar.svc:1234"
    assert wh.webhooks[0]["matcher"].search("bar:badger") is None
    assert wh.webhooks[0]["matcher"].search("foo:badger") is not None
