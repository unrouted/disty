use serde::{Deserialize, Serialize};

use crate::digest::Digest;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum Notification {
    Tick,
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
