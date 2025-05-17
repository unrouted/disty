use serde::{Deserialize, Serialize};

use crate::digest::Digest;

#[derive(Serialize, Deserialize)]
pub(crate) enum Notification {
    ManifestAdded {
        node: u64,
        digest: Digest,
        repository: String,
    },
    BlobAdded {
        node: u64,
        digest: Digest,
        repository: String,
    },
}
