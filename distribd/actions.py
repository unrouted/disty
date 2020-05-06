import enum


class RegistryActions(str, enum.Enum):

    # A given sha256 blob was committed to disk and should be replicated
    BLOB_STORED = "blob-stored"

    # A given sha256 blob hash was deleted and is safe to delete from disk
    BLOB_UNSTORED = "blob-unstored"

    # Associate a blob hash with a repository
    BLOB_MOUNTED = "blob-mounted"

    # Associate a blob hash with a repository
    BLOB_UNMOUNTED = "blob-unmounted"

    # Associate a blob with metadata about it (like its depgraph)
    BLOB_INFO = "blob-info"

    # How big is our blob store?
    BLOB_STAT = "blob-stat"

    # A given sha256 hash was stored on a node
    MANIFEST_STORED = "manifest-stored"

    # A given sha256 hash was deleted from the cluster and is safe to garbage collect
    MANIFEST_UNSTORED = "manifest-unstored"

    # Associate a manifest hash with a repository.
    MANIFEST_MOUNTED = "manifest-mounted"

    # Associate a manifest hash with a repository.
    MANIFEST_UNMOUNTED = "manifest-unmounted"

    # Associate a manifest with metadata about it (like its depgraph)
    MANIFEST_INFO = "manifest-info"

    # How big is our manifest store
    MANIFEST_STAT = "manifest-stat"

    # A given sha256 manifest hash was tagged with a repository and a tag
    HASH_TAGGED = "hash-tagged"
