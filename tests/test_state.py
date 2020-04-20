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
        ]
    )

    assert registry_state.is_manifest_available("alpine", "abcdefgh")


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
