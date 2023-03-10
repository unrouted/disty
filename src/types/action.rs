use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::digest::Digest;
use super::RepositoryName;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub enum RegistryAction {
    Empty,

    // A given sha256 blob was committed to disk and should be replicated
    //BLOB_STORED = "blob-stored"
    BlobStored {
        timestamp: DateTime<Utc>,
        digest: Digest,
        location: String,
        user: String,
    },

    // A given sha256 blob hash was deleted and is safe to delete from disk
    //BLOB_UNSTORED = "blob-unstored"
    BlobUnstored {
        timestamp: DateTime<Utc>,
        digest: Digest,
        location: String,
        user: String,
    },

    // Associate a blob hash with a repository
    //BLOB_MOUNTED = "blob-mounted"
    BlobMounted {
        timestamp: DateTime<Utc>,
        digest: Digest,
        repository: RepositoryName,
        user: String,
    },

    // Associate a blob hash with a repository
    //BLOB_UNMOUNTED = "blob-unmounted"
    BlobUnmounted {
        timestamp: DateTime<Utc>,
        digest: Digest,
        repository: RepositoryName,
        user: String,
    },

    // Associate a blob with metadata about it (like its depgraph)
    //BLOB_INFO = "blob-info"
    BlobInfo {
        timestamp: DateTime<Utc>,
        digest: Digest,
        dependencies: Vec<Digest>,
        content_type: String,
    },

    // How big is our blob store?
    //BLOB_STAT = "blob-stat"
    BlobStat {
        timestamp: DateTime<Utc>,
        digest: Digest,
        size: u64,
    },

    // A given sha256 hash was stored on a node
    //MANIFEST_STORED = "manifest-stored"
    ManifestStored {
        timestamp: DateTime<Utc>,
        digest: Digest,
        location: String,
        user: String,
    },

    // A given sha256 hash was deleted from the cluster and is safe to garbage collect
    //MANIFEST_UNSTORED = "manifest-unstored"
    ManifestUnstored {
        timestamp: DateTime<Utc>,
        digest: Digest,
        location: String,
        user: String,
    },

    // Associate a manifest hash with a repository.
    //MANIFEST_MOUNTED = "manifest-mounted"
    ManifestMounted {
        timestamp: DateTime<Utc>,
        digest: Digest,
        repository: RepositoryName,
        user: String,
    },

    // Associate a manifest hash with a repository.
    //MANIFEST_UNMOUNTED = "manifest-unmounted"
    ManifestUnmounted {
        timestamp: DateTime<Utc>,
        digest: Digest,
        repository: RepositoryName,
        user: String,
    },

    // Associate a manifest with metadata about it (like its depgraph)
    //MANIFEST_INFO = "manifest-info"
    ManifestInfo {
        timestamp: DateTime<Utc>,
        digest: Digest,
        dependencies: Vec<Digest>,
        content_type: String,
    },

    // How big is our manifest store
    //MANIFEST_STAT = "manifest-stat"
    ManifestStat {
        timestamp: DateTime<Utc>,
        digest: Digest,
        size: u64,
    },

    // A given sha256 manifest hash was tagged with a repository and a tag
    //HASH_TAGGED = "hash-tagged"
    HashTagged {
        timestamp: DateTime<Utc>,
        digest: Digest,
        repository: RepositoryName,
        tag: String,
        user: String,
    },
}
