use chrono::prelude::*;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use serde::{Deserialize, Serialize};

use super::digest::Digest;
use super::RepositoryName;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub enum RegistryAction {
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

impl IntoPy<PyObject> for RegistryAction {
    fn into_py(self, py: Python) -> PyObject {
        match self {
            RegistryAction::BlobStored {
                timestamp,
                digest,
                location,
                user,
            } => {
                let dict = PyDict::new(py);
                dict.set_item("type", "blob-stored").unwrap();
                dict.set_item("timestamp", timestamp.to_rfc3339()).unwrap();
                dict.set_item("hash", digest).unwrap();
                dict.set_item("location", location).unwrap();
                dict.set_item("user", user).unwrap();
                dict.into()
            }
            // A given sha256 blob hash was deleted and is safe to delete from disk
            //BLOB_UNSTORED = "blob-unstored"
            RegistryAction::BlobUnstored {
                timestamp,
                digest,
                location,
                user,
            } => {
                let dict = PyDict::new(py);
                dict.set_item("type", "blob-unstored").unwrap();
                dict.set_item("timestamp", timestamp.to_rfc3339()).unwrap();
                dict.set_item("hash", digest).unwrap();
                dict.set_item("location", location).unwrap();
                dict.set_item("user", user).unwrap();
                dict.into()
            }

            // Associate a blob hash with a repository
            //BLOB_MOUNTED = "blob-mounted"
            RegistryAction::BlobMounted {
                timestamp,
                digest,
                repository,
                user,
            } => {
                let dict = PyDict::new(py);
                dict.set_item("type", "blob-mounted").unwrap();
                dict.set_item("timestamp", timestamp.to_rfc3339()).unwrap();
                dict.set_item("hash", digest).unwrap();
                dict.set_item("repository", repository.to_string()).unwrap();
                dict.set_item("user", user).unwrap();
                dict.into()
            }
            // Deassociate a blob hash with a repository
            //BLOB_UNMOUNTED = "blob-unmounted"
            RegistryAction::BlobUnmounted {
                timestamp,
                digest,
                repository,
                user,
            } => {
                let dict = PyDict::new(py);
                dict.set_item("type", "blob-unmounted").unwrap();
                dict.set_item("timestamp", timestamp.to_rfc3339()).unwrap();
                dict.set_item("hash", digest).unwrap();
                dict.set_item("repository", repository.to_string()).unwrap();
                dict.set_item("user", user).unwrap();
                dict.into()
            }
            // Associate a blob with metadata about it (like its depgraph)
            //BLOB_INFO = "blob-info"
            RegistryAction::BlobInfo {
                timestamp,
                digest,
                dependencies,
                content_type,
            } => {
                let dict = PyDict::new(py);
                dict.set_item("type", "blob-info").unwrap();
                dict.set_item("timestamp", timestamp.to_rfc3339()).unwrap();
                dict.set_item("hash", digest).unwrap();
                dict.set_item("dependencies", dependencies).unwrap();
                dict.set_item("content_type", content_type).unwrap();
                dict.into()
            }
            // How big is our blob store?
            //BLOB_STAT = "blob-stat"
            RegistryAction::BlobStat {
                timestamp,
                digest,
                size,
            } => {
                let dict = PyDict::new(py);
                dict.set_item("type", "blob-stat").unwrap();
                dict.set_item("timestamp", timestamp.to_rfc3339()).unwrap();
                dict.set_item("hash", digest).unwrap();
                dict.set_item("size", size).unwrap();
                dict.into()
            }

            RegistryAction::ManifestStored {
                timestamp,
                digest,
                location,
                user,
            } => {
                let dict = PyDict::new(py);
                dict.set_item("type", "manifest-stored").unwrap();
                dict.set_item("timestamp", timestamp.to_rfc3339()).unwrap();
                dict.set_item("hash", digest).unwrap();
                dict.set_item("location", location).unwrap();
                dict.set_item("user", user).unwrap();
                dict.into()
            }
            // A given sha256 manifest hash was deleted and is safe to delete from disk
            //MANIFEST_UNSTORED = "manifest-unstored"
            RegistryAction::ManifestUnstored {
                timestamp,
                digest,
                location,
                user,
            } => {
                let dict = PyDict::new(py);
                dict.set_item("type", "manifest-unstored").unwrap();
                dict.set_item("timestamp", timestamp.to_rfc3339()).unwrap();
                dict.set_item("hash", digest).unwrap();
                dict.set_item("location", location).unwrap();
                dict.set_item("user", user).unwrap();
                dict.into()
            }

            // Associate a manifest hash with a repository
            //MANIFEST_MOUNTED = "manifest-mounted"
            RegistryAction::ManifestMounted {
                timestamp,
                digest,
                repository,
                user,
            } => {
                let dict = PyDict::new(py);
                dict.set_item("type", "manifest-mounted").unwrap();
                dict.set_item("timestamp", timestamp.to_rfc3339()).unwrap();
                dict.set_item("hash", digest).unwrap();
                dict.set_item("repository", repository.to_string()).unwrap();
                dict.set_item("user", user).unwrap();
                dict.into()
            }
            // Deassociate a manifest hash with a repository
            //MANIFEST_UNMOUNTED = "manifest-unmounted"
            RegistryAction::ManifestUnmounted {
                timestamp,
                digest,
                repository,
                user,
            } => {
                let dict = PyDict::new(py);
                dict.set_item("type", "manifest-unmounted").unwrap();
                dict.set_item("timestamp", timestamp.to_rfc3339()).unwrap();
                dict.set_item("hash", digest).unwrap();
                dict.set_item("repository", repository.to_string()).unwrap();
                dict.set_item("user", user).unwrap();
                dict.into()
            }
            // Associate a manifest with metadata about it (like its depgraph)
            //MANIFEST_INFO = "manifest-info"
            RegistryAction::ManifestInfo {
                timestamp,
                digest,
                dependencies,
                content_type,
            } => {
                let dict = PyDict::new(py);
                dict.set_item("type", "manifest-info").unwrap();
                dict.set_item("timestamp", timestamp.to_rfc3339()).unwrap();
                dict.set_item("hash", digest).unwrap();
                dict.set_item("dependencies", dependencies).unwrap();
                dict.set_item("content_type", content_type).unwrap();
                dict.into()
            }
            // How big is our manifest store?
            //MANIFEST_STAT = "manifest-stat"
            RegistryAction::ManifestStat {
                timestamp,
                digest,
                size,
            } => {
                let dict = PyDict::new(py);
                dict.set_item("type", "manifest-stat").unwrap();
                dict.set_item("timestamp", timestamp.to_rfc3339()).unwrap();
                dict.set_item("hash", digest).unwrap();
                dict.set_item("size", size).unwrap();
                dict.into()
            }

            // A given sha256 manifest hash was tagged with a repository and a tag
            //HASH_TAGGED = "hash-tagged"
            RegistryAction::HashTagged {
                timestamp,
                digest,
                repository,
                tag,
                user,
            } => {
                let dict = PyDict::new(py);
                dict.set_item("type", "hash-tagged").unwrap();
                dict.set_item("timestamp", timestamp.to_rfc3339()).unwrap();
                dict.set_item("repository", repository.to_string()).unwrap();
                dict.set_item("hash", digest).unwrap();
                dict.set_item("tag", tag).unwrap();
                dict.set_item("user", user).unwrap();
                dict.into()
            }
        }
    }
}
