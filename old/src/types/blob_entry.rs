use super::{Blob, Digest};

pub struct BlobEntry {
    pub digest: Digest,
    pub blob: Blob,
}
