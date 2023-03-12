use std::sync::Arc;

use openraft::Config;

use crate::types::Digest;
use crate::RegistryNodeId;
use crate::RegistryRaft;
use crate::RegistryStore;

// Representation of an application state. This struct can be shared around to share
// instances of raft, store and more.
pub struct RegistryApp {
    pub id: RegistryNodeId,
    pub addr: String,
    pub raft: RegistryRaft,
    pub store: Arc<RegistryStore>,
    pub config: Arc<Config>,
}

impl RegistryApp {
    pub fn get_blob_path(&self, digest: &Digest) -> std::path::PathBuf {
        // FIXME: Hookup to settings
        let images_directory = "tmp".to_string();

        let mut path = std::path::Path::new(&images_directory).to_path_buf();
        let digest_string = &digest.hash;

        path.push("blobs");
        path.push(&digest_string[0..2]);
        path.push(&digest_string[2..4]);
        path.push(&digest_string[4..6]);

        std::fs::create_dir_all(path.clone()).unwrap();

        path.push(&digest_string[6..]);

        path
    }

    pub fn get_manifest_path(&self, digest: &Digest) -> std::path::PathBuf {
        // FIXME: Hookup to settings
        let images_directory = "tmp".to_string();

        let mut path = std::path::Path::new(&images_directory).to_path_buf();
        let digest_string = &digest.hash;

        path.push("manifests");
        path.push(&digest_string[0..2]);
        path.push(&digest_string[2..4]);
        path.push(&digest_string[4..6]);

        std::fs::create_dir_all(path.clone()).unwrap();

        path.push(&digest_string[6..]);

        path
    }
}
