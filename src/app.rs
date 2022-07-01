use log::error;
use std::sync::Arc;

use openraft::raft::ClientWriteRequest;
use openraft::EntryPayload;

use crate::config::Configuration;
use crate::store::RegistryRequest;
use crate::types::Blob;
use crate::types::BlobEntry;
use crate::types::Digest;
use crate::types::Manifest;
use crate::types::ManifestEntry;
use crate::types::RegistryAction;
use crate::types::RepositoryName;
use crate::NodeId;
use crate::RegistryRaft;
use crate::RegsistryStore;

// Representation of an application state. This struct can be shared around to share
// instances of raft, store and more.
pub struct RegistryApp {
    pub id: NodeId,
    pub addr: String,
    pub raft: RegistryRaft,
    pub store: Arc<RegsistryStore>,
    pub settings: Configuration,
}

impl RegistryApp {
    pub async fn submit(&self, actions: Vec<RegistryAction>) -> bool {
        let transaction = RegistryRequest::RepositoryTransaction { actions };
        let request = ClientWriteRequest::new(EntryPayload::Normal(transaction));
        if let Err(err) = self.raft.client_write(request).await {
            error!("Error whilst writing to raft: {err}");
            return false;
        }

        true
    }

    pub async fn is_blob_available(&self, repository: &RepositoryName, hash: &Digest) -> bool {
        let state = self.store.state_machine.read().await;
        state.is_blob_available(repository, hash)
    }

    pub async fn get_blob_directly(&self, hash: &Digest) -> Option<Blob> {
        let state = self.store.state_machine.read().await;
        state.get_blob_directly(hash)
    }

    pub async fn get_blob(&self, repository: &RepositoryName, hash: &Digest) -> Option<Blob> {
        let state = self.store.state_machine.read().await;
        state.get_blob(repository, hash)
    }

    pub async fn get_manifest_directly(&self, hash: &Digest) -> Option<Manifest> {
        let state = self.store.state_machine.read().await;
        state.get_manifest_directly(hash)
    }
    pub async fn get_manifest(
        &self,
        repository: &RepositoryName,
        hash: &Digest,
    ) -> Option<Manifest> {
        let state = self.store.state_machine.read().await;
        state.get_manifest(repository, hash)
    }
    pub async fn get_tag(&self, repository: &RepositoryName, tag: &str) -> Option<Digest> {
        let state = self.store.state_machine.read().await;
        state.get_tag(repository, tag)
    }

    pub async fn get_tags(&self, repository: &RepositoryName) -> Option<Vec<String>> {
        let state = self.store.state_machine.read().await;
        state.get_tags(repository)
    }

    pub async fn is_manifest_available(&self, repository: &RepositoryName, hash: &Digest) -> bool {
        let state = self.store.state_machine.read().await;
        state.is_manifest_available(repository, hash)
    }

    pub async fn get_orphaned_blobs(&self) -> Vec<BlobEntry> {
        let state = self.store.state_machine.read().await;
        state.get_orphaned_blobs()
    }

    pub async fn get_orphaned_manifests(&self) -> Vec<ManifestEntry> {
        let state = self.store.state_machine.read().await;
        state.get_orphaned_manifests()
    }
}
