import json

from distribd.exceptions import BlobUnknown, TagInvalid


def test_blob_unknown():
    exception = BlobUnknown()

    assert exception.status_code == 404
    assert exception.reason == "Not Found"
    assert json.loads(exception.body) == {
        "errors": [{"code": "BLOB_UNKNOWN", "message": "blob unknown to registry"}]
    }
    assert exception.headers["Content-Type"] == "application/json; charset=utf-8"


def test_tag_invalid():
    exception = TagInvalid()

    assert exception.status_code == 400
    assert exception.reason == "Bad Request"
    assert json.loads(exception.body) == {
        "errors": [{"code": "TAG_INVALID", "message": "manifest tag did not match URI"}]
    }
    assert exception.headers["Content-Type"] == "application/json; charset=utf-8"
