use serde::{Deserialize, Serialize};

use super::digest::Digest;
use super::RepositoryName;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub enum RegistryAction {
    // A given sha256 blob was committed to disk and should be replicated
    //BLOB_STORED = "blob-stored"
    BlobStored {
        digest: Digest,
        location: String,
    },

    // A given sha256 blob hash was deleted and is safe to delete from disk
    //BLOB_UNSTORED = "blob-unstored"
    BlobUnstored {
        digest: Digest,
        location: String,
    },

    // Associate a blob hash with a repository
    //BLOB_MOUNTED = "blob-mounted"
    BlobMounted {
        digest: Digest,
        repository: RepositoryName,
    },

    // Associate a blob hash with a repository
    //BLOB_UNMOUNTED = "blob-unmounted"
    BlobUnmounted {
        digest: Digest,
        repository: RepositoryName,
    },

    // Associate a blob with metadata about it (like its depgraph)
    //BLOB_INFO = "blob-info"
    BlobInfo {
        digest: Digest,
        dependencies: Vec<Digest>,
        content_type: String,
    },

    // How big is our blob store?
    //BLOB_STAT = "blob-stat"
    BlobStat {
        digest: Digest,
        size: usize,
    },

    // A given sha256 hash was stored on a node
    //MANIFEST_STORED = "manifest-stored"
    ManifestStored {
        digest: Digest,
        location: String,
    },

    // A given sha256 hash was deleted from the cluster and is safe to garbage collect
    //MANIFEST_UNSTORED = "manifest-unstored"
    ManifestUnstored {
        digest: Digest,
        location: String,
    },

    // Associate a manifest hash with a repository.
    //MANIFEST_MOUNTED = "manifest-mounted"
    ManifestMounted {
        digest: Digest,
        repository: RepositoryName,
    },

    // Associate a manifest hash with a repository.
    //MANIFEST_UNMOUNTED = "manifest-unmounted"
    ManifestUnmounted {
        digest: Digest,
        repository: RepositoryName,
    },

    // Associate a manifest with metadata about it (like its depgraph)
    //MANIFEST_INFO = "manifest-info"
    ManifestInfo {
        digest: Digest,
        dependencies: Vec<Digest>,
        content_type: String,
    },

    // How big is our manifest store
    //MANIFEST_STAT = "manifest-stat"
    ManifestStat {
        digest: Digest,
        size: usize,
    },

    // A given sha256 manifest hash was tagged with a repository and a tag
    //HASH_TAGGED = "hash-tagged"
    HashTagged {
        digest: Digest,
        repository: RepositoryName,
        tag: String,
    },

    // A blob upload was initiated
    BlobUploadStarted {
        session_id: String,
        repository: RepositoryName,
    },

    BlobUploadPart {
        session_id: String,
        start: usize,
        finish: usize,
    },

    BlobUploadLocation {
        session_id: String,
        location: String,
    },

    BlobUploadDelete {
        session_id: String,
    },

    BlobUploadFinish {
        session_id: String,
    },
}
