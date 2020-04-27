from distribd.utils.registry import get_blob_path, get_manifest_path


def test_get_blob_path(tmp_path):
    path = get_blob_path(tmp_path, "sha256:abcdefghijklmnopqrstuvwxyz").relative_to(
        tmp_path
    )
    assert str(path) == "blobs/ab/cd/ef/ghijklmnopqrstuvwxyz"


def test_get_manifest_path(tmp_path):
    path = get_manifest_path(tmp_path, "sha256:abcdefghijklmnopqrstuvwxyz").relative_to(
        tmp_path
    )
    assert str(path) == "manifests/ab/cd/ef/ghijklmnopqrstuvwxyz"
