from distribd.actions import RegistryActions
from distribd.state import RegistryState
import pytest


def test_blob_not_available_initially():
    registry_state = RegistryState()
    assert not registry_state.is_blob_available("alpine", "abcdefgh")


def test_blob_available():
    registry_state = RegistryState()
    registry_state.dispatch_entries(
        [
            [
                1,
                {
                    "type": RegistryActions.BLOB_MOUNTED,
                    "repository": "alpine",
                    "hash": "abcdefgh",
                },
            ],
        ]
    )

    assert registry_state.is_blob_available("alpine", "abcdefgh")


def test_blob_not_available_after_delete():
    registry_state = RegistryState()
    registry_state.dispatch_entries(
        [
            [
                1,
                {
                    "type": RegistryActions.BLOB_MOUNTED,
                    "repository": "alpine",
                    "hash": "abcdefgh",
                },
            ],
            [
                1,
                {
                    "type": RegistryActions.BLOB_UNMOUNTED,
                    "repository": "alpine",
                    "hash": "abcdefgh",
                },
            ],
        ]
    )

    assert not registry_state.is_blob_available("alpine", "abcdefgh")


def test_blob_dependencies():
    registry_state = RegistryState()
    registry_state.dispatch_entries(
        [
            [
                1,
                {
                    "type": RegistryActions.BLOB_MOUNTED,
                    "repository": "alpine",
                    "hash": "abcdefgh",
                },
            ],
            [
                1,
                {
                    "type": RegistryActions.BLOB_INFO,
                    "hash": "abcdefgh",
                    "dependencies": ["sha256:abcdefg"],
                    "content_type": "application/json",
                },
            ],
        ]
    )

    assert set(registry_state.graph.successors("abcdefgh")) == {"sha256:abcdefg"}


def test_blob_available_after_delete_and_restore():
    registry_state = RegistryState()
    registry_state.dispatch_entries(
        [
            [
                1,
                {
                    "type": RegistryActions.BLOB_MOUNTED,
                    "repository": "alpine",
                    "hash": "abcdefgh",
                },
            ],
            [
                1,
                {
                    "type": RegistryActions.BLOB_UNMOUNTED,
                    "repository": "alpine",
                    "hash": "abcdefgh",
                },
            ],
            [
                1,
                {
                    "type": RegistryActions.BLOB_MOUNTED,
                    "repository": "alpine",
                    "hash": "abcdefgh",
                },
            ],
        ]
    )

    assert registry_state.is_blob_available("alpine", "abcdefgh")


def test_manifest_not_available_initially():
    registry_state = RegistryState()
    assert not registry_state.is_manifest_available("alpine", "abcdefgh")


def test_manifest_available():
    registry_state = RegistryState()
    registry_state.dispatch_entries(
        [
            [
                1,
                {
                    "type": RegistryActions.MANIFEST_MOUNTED,
                    "repository": "alpine",
                    "hash": "abcdefgh",
                },
            ],
            [
                1,
                {
                    "type": RegistryActions.MANIFEST_INFO,
                    "hash": "abcdefgh",
                    "content_type": "application/json",
                    "dependencies": [],
                },
            ],
        ]
    )

    assert registry_state.is_manifest_available("alpine", "abcdefgh")


def test_manifest_not_available_after_delete():
    registry_state = RegistryState()
    registry_state.dispatch_entries(
        [
            [
                1,
                {
                    "type": RegistryActions.MANIFEST_MOUNTED,
                    "repository": "alpine",
                    "hash": "abcdefgh",
                },
            ],
            [
                1,
                {
                    "type": RegistryActions.MANIFEST_INFO,
                    "hash": "abcdefgh",
                    "content_type": "application/json",
                    "dependencies": [],
                },
            ],
            [
                1,
                {
                    "type": RegistryActions.MANIFEST_UNMOUNTED,
                    "repository": "alpine",
                    "hash": "abcdefgh",
                },
            ],
        ]
    )

    assert not registry_state.is_manifest_available("alpine", "abcdefgh")


def test_manifest_available_after_delete_and_restore():
    registry_state = RegistryState()
    registry_state.dispatch_entries(
        [
            [
                1,
                {
                    "type": RegistryActions.MANIFEST_MOUNTED,
                    "repository": "alpine",
                    "hash": "abcdefgh",
                },
            ],
            [
                1,
                {
                    "type": RegistryActions.MANIFEST_INFO,
                    "hash": "abcdefgh",
                    "content_type": "application/json",
                    "dependencies": [],
                },
            ],
            [
                1,
                {
                    "type": RegistryActions.MANIFEST_UNMOUNTED,
                    "repository": "alpine",
                    "hash": "abcdefgh",
                },
            ],
            [
                1,
                {
                    "type": RegistryActions.MANIFEST_MOUNTED,
                    "repository": "alpine",
                    "hash": "abcdefgh",
                },
            ],
            [
                1,
                {
                    "type": RegistryActions.MANIFEST_INFO,
                    "hash": "abcdefgh",
                    "content_type": "application/json",
                    "dependencies": [],
                },
            ],
        ]
    )

    assert registry_state.is_manifest_available("alpine", "abcdefgh")


def test_get_orphaned_objects():
    """
    A blob layer is published, then a manifest points at that is published. That manifest is tagged.
    Later a new manifest and tag are pushed. The blob layer is not orphaned, and the old tag is just
    overwritten. In this exam[le, this leaves layer2-a as something that can be garbage collected.
    """
    registry_state = RegistryState()
    registry_state.dispatch_entries(
        [
            [
                1,
                {
                    "type": RegistryActions.BLOB_MOUNTED,
                    "repository": "alpine",
                    "hash": "base",
                },
            ],
            [
                1,
                {
                    "type": RegistryActions.MANIFEST_MOUNTED,
                    "repository": "alpine",
                    "hash": "layer2-a",
                },
            ],
            [
                1,
                {
                    "type": RegistryActions.MANIFEST_INFO,
                    "hash": "layer2-a",
                    "content_type": "application/json",
                    "dependencies": ["base"],
                },
            ],
            [
                1,
                {
                    "type": RegistryActions.HASH_TAGGED,
                    "repository": "alpine",
                    "tag": "3.11",
                    "hash": "layer2-a",
                },
            ],
            [
                1,
                {
                    "type": RegistryActions.MANIFEST_MOUNTED,
                    "repository": "alpine",
                    "hash": "layer2-b",
                },
            ],
            [
                1,
                {
                    "type": RegistryActions.MANIFEST_INFO,
                    "hash": "layer2-b",
                    "content_type": "application/json",
                    "dependencies": ["base"],
                },
            ],
            [
                1,
                {
                    "type": RegistryActions.HASH_TAGGED,
                    "repository": "alpine",
                    "tag": "3.11",
                    "hash": "layer2-b",
                },
            ],
        ]
    )

    assert registry_state.get_orphaned_objects() == {"layer2-a"}


def test_get_tags_tag_not_available():
    registry_state = RegistryState()
    with pytest.raises(KeyError):
        registry_state.get_tags("alpine")


def test_get_tags_tag_available():
    registry_state = RegistryState()
    registry_state.dispatch_entries(
        [
            [
                1,
                {
                    "type": RegistryActions.MANIFEST_MOUNTED,
                    "repository": "alpine",
                    "hash": "abcdefgh",
                },
            ],
            [
                1,
                {
                    "type": RegistryActions.HASH_TAGGED,
                    "repository": "alpine",
                    "tag": "3.11",
                    "hash": "abcdefgh",
                },
            ],
        ]
    )
    assert registry_state.get_tags("alpine") == ["3.11"]


def test_tag_not_available():
    registry_state = RegistryState()
    with pytest.raises(KeyError):
        registry_state.get_tag("alpine", "3.11")


def test_tag_available():
    registry_state = RegistryState()
    registry_state.dispatch_entries(
        [
            [
                1,
                {
                    "type": RegistryActions.HASH_TAGGED,
                    "repository": "alpine",
                    "tag": "3.11",
                    "hash": "abcdefgh",
                },
            ],
        ]
    )
    assert registry_state.get_tag("alpine", "3.11") == "abcdefgh"
