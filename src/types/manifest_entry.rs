use super::{Digest, Manifest};

pub struct ManifestEntry {
    pub digest: Digest,
    pub manifest: Manifest,
}
