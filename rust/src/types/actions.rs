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
        digest: Digest,
        location: String,
        user: String,
    },

    // A given sha256 blob hash was deleted and is safe to delete from disk
    //BLOB_UNSTORED = "blob-unstored"
    BlobUnstored {
        digest: Digest,
        location: String,
        user: String,
    },

    // Associate a blob hash with a repository
    //BLOB_MOUNTED = "blob-mounted"
    BlobMounted {
        digest: Digest,
        repository: RepositoryName,
        user: String,
    },

    // Associate a blob hash with a repository
    //BLOB_UNMOUNTED = "blob-unmounted"
    BlobUnmounted {
        digest: Digest,
        repository: RepositoryName,
        user: String,
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
        size: u64,
    },

    // A given sha256 hash was stored on a node
    //MANIFEST_STORED = "manifest-stored"
    ManifestStored {
        digest: Digest,
        location: String,
        user: String,
    },

    // A given sha256 hash was deleted from the cluster and is safe to garbage collect
    //MANIFEST_UNSTORED = "manifest-unstored"
    ManifestUnstored {
        digest: Digest,
        location: String,
        user: String,
    },

    // Associate a manifest hash with a repository.
    //MANIFEST_MOUNTED = "manifest-mounted"
    ManifestMounted {
        digest: Digest,
        repository: RepositoryName,
        user: String,
    },

    // Associate a manifest hash with a repository.
    //MANIFEST_UNMOUNTED = "manifest-unmounted"
    ManifestUnmounted {
        digest: Digest,
        repository: RepositoryName,
        user: String,
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
        size: u64,
    },

    // A given sha256 manifest hash was tagged with a repository and a tag
    //HASH_TAGGED = "hash-tagged"
    HashTagged {
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
                digest,
                location,
                user,
            } => {
                let dict = PyDict::new(py);
                dict.set_item("type", "blob-stored").unwrap();
                dict.set_item("hash", digest).unwrap();
                dict.set_item("location", location).unwrap();
                dict.set_item("user", user).unwrap();
                dict.into()
            }
            // A given sha256 blob hash was deleted and is safe to delete from disk
            //BLOB_UNSTORED = "blob-unstored"
            RegistryAction::BlobUnstored {
                digest,
                location,
                user,
            } => {
                let dict = PyDict::new(py);
                dict.set_item("type", "blob-unstored").unwrap();
                dict.set_item("hash", digest).unwrap();
                dict.set_item("location", location).unwrap();
                dict.set_item("user", user).unwrap();
                dict.into()
            }

            // Associate a blob hash with a repository
            //BLOB_MOUNTED = "blob-mounted"
            RegistryAction::BlobMounted {
                digest,
                repository,
                user,
            } => {
                let dict = PyDict::new(py);
                dict.set_item("type", "blob-mounted").unwrap();
                dict.set_item("hash", digest).unwrap();
                dict.set_item("repository", repository.to_string()).unwrap();
                dict.set_item("user", user).unwrap();
                dict.into()
            }
            // Deassociate a blob hash with a repository
            //BLOB_UNMOUNTED = "blob-unmounted"
            RegistryAction::BlobUnmounted {
                digest,
                repository,
                user,
            } => {
                let dict = PyDict::new(py);
                dict.set_item("type", "blob-ummounted").unwrap();
                dict.set_item("hash", digest).unwrap();
                dict.set_item("repository", repository.to_string()).unwrap();
                dict.set_item("user", user).unwrap();
                dict.into()
            }
            // Associate a blob with metadata about it (like its depgraph)
            //BLOB_INFO = "blob-info"
            RegistryAction::BlobInfo {
                digest,
                dependencies,
                content_type,
            } => {
                let dict = PyDict::new(py);
                dict.set_item("type", "blob-info").unwrap();
                dict.set_item("hash", digest).unwrap();
                dict.set_item("dependencies", dependencies).unwrap();
                dict.set_item("content_type", content_type).unwrap();
                dict.into()
            }
            // How big is our blob store?
            //BLOB_STAT = "blob-stat"
            RegistryAction::BlobStat { digest, size } => {
                let dict = PyDict::new(py);
                dict.set_item("type", "blob-stat").unwrap();
                dict.set_item("hash", digest).unwrap();
                dict.set_item("size", size).unwrap();
                dict.into()
            }

            /*
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
                size: u64,
            },

            // A given sha256 manifest hash was tagged with a repository and a tag
            //HASH_TAGGED = "hash-tagged"
            HashTagged {
                digest: Digest,
                repository: RepositoryName,
                tag: String,
            },*/
            _ => PyDict::new(py).into(),
        }
    }
}
