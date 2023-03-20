use std::sync::Arc;

use tokio::sync::mpsc::Sender;
use uuid::Uuid;

use crate::client::RegistryClient;
use crate::config::Configuration;
use crate::extractor::Extractor;
use crate::store::RegistryRequest;
use crate::types::Blob;
use crate::types::Digest;
use crate::types::Manifest;
use crate::types::RegistryAction;
use crate::types::RepositoryName;
use crate::webhook::Event;
use crate::RegistryNodeId;
use crate::RegistryRaft;
use crate::RegistryStore;

// Representation of an application state. This struct can be shared around to share
// instances of raft, store and more.
pub struct RegistryApp {
    pub id: RegistryNodeId,
    pub raft: RegistryRaft,
    pub store: Arc<RegistryStore>,
    pub config: Configuration,
    pub extractor: Arc<Extractor>,
    pub webhooks: Arc<Sender<Event>>,
}

impl RegistryApp {
    pub async fn submit(&self, actions: Vec<RegistryAction>) -> bool {
        let req = RegistryRequest::Transaction { actions };

        if let Ok(_resp) = self.raft.client_write(req.clone()).await {
            return true;
        }

        // FIXME: This is super dumb
        for _i in 1..4 {
            for (idx, peer) in self.config.peers.iter().enumerate() {
                let client = RegistryClient::new(
                    (idx + 1) as u64,
                    format!("{}:{}", peer.raft.address, peer.raft.port),
                );
                let resp = client.write(&req).await;
                if resp.is_ok() {
                    return true;
                }
            }
        }

        false
    }

    pub async fn get_blob(&self, digest: &Digest) -> Option<Blob> {
        let sm = self.store.state_machine.read().await;
        sm.get_blob(digest).unwrap()
    }
    pub async fn wait_for_blob(&self, digest: &Digest) {
        return;
        tracing::debug!(
            "distribd::mirror wait_for_blob id {}",
            self.config.identifier
        );
        let sm = self.store.state_machine.write().await;
        sm.wait_for_blob(digest).await;
    }
    pub async fn get_manifest(&self, digest: &Digest) -> Option<Manifest> {
        let sm = self.store.state_machine.read().await;
        sm.get_manifest(digest).unwrap()
    }
    pub async fn wait_for_manifest(&self, digest: &Digest) {
        return;
        let sm = self.store.state_machine.write().await;
        sm.wait_for_manifest(digest).await;
    }
    pub async fn get_tag(&self, repository: &RepositoryName, tag: &str) -> Option<Digest> {
        let sm = self.store.state_machine.read().await;
        sm.get_tag(repository, tag).unwrap()
    }

    pub async fn get_tags(&self, repository: &RepositoryName) -> Option<Vec<String>> {
        let sm = self.store.state_machine.read().await;
        sm.get_tags(repository).unwrap()
    }

    pub fn get_blob_path(&self, digest: &Digest) -> std::path::PathBuf {
        let mut path = std::path::Path::new(&self.config.storage).to_path_buf();
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
        let mut path = std::path::Path::new(&self.config.storage).to_path_buf();
        let digest_string = &digest.hash;

        path.push("manifests");
        path.push(&digest_string[0..2]);
        path.push(&digest_string[2..4]);
        path.push(&digest_string[4..6]);

        std::fs::create_dir_all(path.clone()).unwrap();

        path.push(&digest_string[6..]);

        path
    }

    pub fn get_upload_path(&self, upload_id: &str) -> std::path::PathBuf {
        let mut path = std::path::Path::new(&self.config.storage).to_path_buf();
        path.push("uploads");
        path.push(format!("blob-{upload_id}"));

        path
    }

    pub fn get_temp_path(&self) -> std::path::PathBuf {
        let upload_id = Uuid::new_v4().as_hyphenated().to_string();

        let mut path = std::path::Path::new(&self.config.storage).to_path_buf();
        path.push("uploads");
        path.push(format!("manifest-{upload_id}"));

        path
    }

    pub fn get_temp_mirror_path(&self) -> std::path::PathBuf {
        let upload_id = Uuid::new_v4().as_hyphenated().to_string();

        let mut path = std::path::Path::new(&self.config.storage).to_path_buf();
        path.push("uploads");
        path.push(format!("mirror-{upload_id}"));

        path
    }
}
