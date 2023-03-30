use std::sync::Arc;
use std::sync::Mutex;

use openraft::error::ForwardToLeader;
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
    pub async fn submit_write(&self, actions: Vec<RegistryAction>) -> bool {
        let req = RegistryRequest::Transaction { actions };

        match self.raft.client_write(req.clone()).await {
            Ok(_) => {
                return true;
            }
            Err(e) => {
                if let Some(ForwardToLeader {
                    leader_id: Some(leader_id),
                    leader_node: Some(leader_node),
                }) = e.forward_to_leader()
                {
                    let client = RegistryClient::new(*leader_id, leader_node.addr.clone(), None);
                    let resp = client.write(&req).await;
                    if resp.is_ok() {
                        return true;
                    }
                    tracing::error!("Unhandled error processing submission: {:?}", e);
                } else {
                    tracing::error!("Unhandled error processing submission: {:?}", e);
                    return false;
                }
            }
        }

        false
    }

    pub async fn consistent_write(&self, actions: Vec<RegistryAction>) -> bool {
        let req = RegistryRequest::Transaction { actions };

        let resp = match self.raft.client_write(req.clone()).await {
            Ok(resp) => resp,
            Err(e) => {
                if let Some(ForwardToLeader {
                    leader_id: Some(leader_id),
                    leader_node: Some(leader_node),
                }) = e.forward_to_leader()
                {
                    let client = RegistryClient::new(*leader_id, leader_node.addr.clone(), None);
                    if let Ok(resp) = client.write(&req).await {
                        resp
                    } else {
                        tracing::error!("Unhandled error processing submission: {:?}", e);
                        return false;
                    }
                } else {
                    tracing::error!("Unhandled error processing submission: {:?}", e);
                    return false;
                }
            }
        };

        let target_index = resp.data.value;

        let mut receiver = self.raft.metrics();
        loop {
            let value = receiver.borrow().clone();
            if let Some(applied) = value.last_applied {
                if applied.index >= target_index {
                    return true;
                }
            }
            receiver.changed().await.unwrap();
        }
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
