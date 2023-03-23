use std::sync::Arc;
use std::sync::Mutex;

use prometheus_client::registry::Registry;
use tokio::sync::mpsc::Sender;
use tracing::debug;
use tracing::log::warn;

use crate::client::RegistryClient;
use crate::config::Configuration;
use crate::extractor::Extractor;
use crate::store::RegistryRequest;
use crate::types::Blob;
use crate::types::Digest;
use crate::types::Manifest;
use crate::types::RegistryAction;
use crate::types::RepositoryName;
use crate::utils;
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
    pub registry: Mutex<Registry>,
}

impl RegistryApp {
    pub async fn submit(&self, actions: Vec<RegistryAction>) -> bool {
        let req = RegistryRequest::Transaction { actions };

        match self.raft.client_write(req.clone()).await {
            Ok(_) => {
                return true;
            }
            Err(e) => {
                tracing::error!("local submit error: {:?}", e);
            }
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
                tracing::error!("submit error: {:?}", resp);
            }
        }

        false
    }

    pub fn get_blob(&self, digest: &Digest) -> Option<Blob> {
        self.store.get_blob(digest).unwrap()
    }
    pub async fn wait_for_blob(&self, digest: &Digest) {
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();

        {
            let mut sm = self.store.state_machine.write().unwrap();

            let values = sm
                .blob_waiters
                .entry(digest.clone())
                .or_insert_with(std::vec::Vec::new);

            values.push(tx);

            drop(sm);
        }

        debug!("State: Wait for blob: Waiting for {digest} to download");

        match rx.await {
            Ok(_) => {
                debug!("State: Wait for blob: {digest}: Download complete");
            }
            Err(err) => {
                warn!("State: Failure whilst waiting for blob to be downloaded: {digest}: {err}");
            }
        }
    }
    pub fn get_manifest(&self, digest: &Digest) -> Option<Manifest> {
        self.store.get_manifest(digest).unwrap()
    }
    pub async fn wait_for_manifest(&self, digest: &Digest) {
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();

        {
            let mut sm = self.store.state_machine.write().unwrap();

            let values = sm
                .manifest_waiters
                .entry(digest.clone())
                .or_insert_with(std::vec::Vec::new);

            values.push(tx);

            drop(sm);
        }

        debug!("State: Wait for manifest: Waiting for {digest} to download");

        match rx.await {
            Ok(_) => {
                debug!("State: Wait for manifest: {digest}: Download complete");
            }
            Err(err) => {
                warn!(
                    "State: Failure whilst waiting for manifest to be downloaded: {digest}: {err}"
                );
            }
        }
    }
    pub fn get_tag(&self, repository: &RepositoryName, tag: &str) -> Option<Digest> {
        self.store.get_tag(repository, tag).unwrap()
    }

    pub fn get_tags(&self, repository: &RepositoryName) -> Option<Vec<String>> {
        self.store.get_tags(repository).unwrap()
    }

    pub fn get_blob_path(&self, digest: &Digest) -> std::path::PathBuf {
        utils::get_blob_path(&self.config.storage, digest)
    }

    pub fn get_manifest_path(&self, digest: &Digest) -> std::path::PathBuf {
        utils::get_manifest_path(&self.config.storage, digest)
    }

    pub fn get_upload_path(&self, upload_id: &str) -> std::path::PathBuf {
        utils::get_upload_path(&self.config.storage, upload_id)
    }

    pub fn get_temp_path(&self) -> std::path::PathBuf {
        utils::get_temp_path(&self.config.storage)
    }

    pub fn get_temp_mirror_path(&self) -> std::path::PathBuf {
        utils::get_temp_mirror_path(&self.config.storage)
    }
}
