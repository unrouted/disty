use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use log::{debug, error, info, warn};
use raft::eraftpb::Message;
use raft::prelude::*;
use raft::RawNode;
use tokio::sync::broadcast::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::config::Configuration;
use crate::extractor::Extractor;
use crate::store::RegistryStorage;
use crate::types::Blob;
use crate::types::BlobEntry;
use crate::types::Digest;
use crate::types::Manifest;
use crate::types::ManifestEntry;
use crate::types::RegistryAction;
use crate::types::RepositoryName;

#[derive(Clone, Debug)]
pub enum Lifecycle {
    Shutdown,
}

// Representation of an application state. This struct can be shared around to share
// instances of raft, store and more.
pub struct RegistryApp {
    pub group: RwLock<RawNode<RegistryStorage>>,
    pub inbox: Sender<Msg>,
    pub seq: AtomicU64,
    pub outboxes: HashMap<u64, Sender<Vec<u8>>>,
    pub settings: Configuration,
    pub extractor: Extractor,
    services: RwLock<FuturesUnordered<JoinHandle<anyhow::Result<()>>>>,
    lifecycle: tokio::sync::broadcast::Sender<Lifecycle>,
    manifest_waiters: Mutex<HashMap<Digest, Vec<tokio::sync::oneshot::Sender<()>>>>,
    blob_waiters: Mutex<HashMap<Digest, Vec<tokio::sync::oneshot::Sender<()>>>>,
}

pub enum Msg {
    Propose {
        actions: Vec<RegistryAction>,
        id: u64,
        cb: tokio::sync::oneshot::Sender<u64>,
    },
    Raft(Message),
}

impl RegistryApp {
    pub fn new(
        group: RwLock<RawNode<RegistryStorage>>,
        inbox: Sender<Msg>,
        outboxes: HashMap<u64, Sender<Vec<u8>>>,
        settings: Configuration,
    ) -> Self {
        let (tx, _rx) = tokio::sync::broadcast::channel(16);

        let extractor = crate::extractor::Extractor::new(settings.clone());

        RegistryApp {
            group,
            inbox,
            seq: AtomicU64::new(1),
            outboxes,
            settings,
            extractor,
            services: RwLock::new(FuturesUnordered::new()),
            lifecycle: tx,
            blob_waiters: Mutex::new(HashMap::new()),
            manifest_waiters: Mutex::new(HashMap::new()),
        }
    }

    pub fn subscribe_lifecycle(&self) -> Receiver<Lifecycle> {
        self.lifecycle.subscribe()
    }

    pub async fn spawn<T>(&self, task: T)
    where
        T: Future<Output = anyhow::Result<()>>,
        T: Send + 'static,
    {
        self.services.write().await.push(tokio::spawn(task));
    }

    pub async fn wait_for_shutdown(&self) -> Result<()> {
        let mut services = self.services.write().await;

        tokio::select!(
            _ = tokio::signal::ctrl_c() => {
                warn!("Got ctrl+c. Starting graceful shutdown");
            }
            Some(res) = services.next() => {
                match res? {
                    Ok(()) => {
                        warn!("Service task unexpectedly exited! Will exit now to prevent corruption.");
                    }
                    Err(err) => {
                        error!("{err:?}");
                    }
                }
            }
        );

        self.lifecycle
            .send(Lifecycle::Shutdown)
            .context("Failed to notify services of shutdown")?;

        info!("Waiting for graceful shutdown");
        while let Some(res) = services.next().await {
            match res {
                Ok(inner) => {
                    if let Err(err) = inner {
                        error!("{err:?}");
                    }
                }
                Err(err) => {
                    error!("{err:?}");
                }
            }
        }

        Ok(())
    }

    pub async fn is_leader(&self) -> bool {
        let group = self.group.read().await;
        group.raft.leader_id == group.raft.id
    }

    async fn wait_for_leader(&self) -> u64 {
        loop {
            let group = self.group.read().await;
            let leader_id = group.raft.leader_id;
            drop(group);

            if leader_id > 0 {
                return leader_id;
            }

            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
    }

    pub async fn submit_local(&self, actions: Vec<RegistryAction>) -> Result<u64> {
        let (tx, rx) = tokio::sync::oneshot::channel();

        let proposal = Msg::Propose {
            actions,
            id: self.seq.fetch_add(1, Ordering::Relaxed),
            cb: tx,
        };

        self.inbox
            .send(proposal)
            .await
            .map_err(|e| anyhow::anyhow!(e.to_string()))
            .context("Failed to enqueue local proposal")?;

        rx.await.context("Failed to wait for local proposal")
    }

    pub async fn submit_remote(&self, actions: Vec<RegistryAction>, idx: u64) -> Result<u64> {
        let peer = self.settings.peers.get((idx as usize) - 1).unwrap();
        let address = &peer.raft.address;
        let port = peer.raft.port;
        let url = format!("http://{address}:{port}/submit");

        let client = reqwest::Client::builder()
            .user_agent("distribd/raft")
            .build()
            .unwrap();

        let resp = client.post(&url).json(&actions).send().await?;

        Ok(resp.json().await?)
    }

    pub async fn submit(&self, actions: Vec<RegistryAction>) -> bool {
        let leader_id = self.wait_for_leader().await;

        if leader_id == self.group.read().await.raft.id {
            self.submit_local(actions).await.unwrap();
        } else if (self.submit_remote(actions, leader_id).await).is_ok() {
            // while self.group.read().await.raft.store().applied_index < target {
            //    tokio::time::sleep(Duration::from_millis(100)).await;
            // }
        }

        true
    }

    async fn blob_available(&self, digest: &Digest) {
        let mut waiters = self.blob_waiters.lock().await;

        if let Some(blobs) = waiters.remove(digest) {
            debug!(
                "State: Wait for blob: {digest} now available. {} waiters to process",
                blobs.len()
            );
            for sender in blobs {
                if sender.send(()).is_err() {
                    warn!("Some blob waiters may have failed: {digest}");
                }
            }
        } else {
            debug!("State: Wait for blob: {digest} now available - no active waiters");
        }
    }

    pub async fn wait_for_blob(&self, digest: &Digest) {
        let mut waiters = self.blob_waiters.lock().await;

        if let Some(blob) = self.get_blob_directly(digest).await {
            if blob.locations.contains(&self.settings.identifier) {
                // Blob already exists at this endpoint, no need to wait
                debug!("State: Wait for blob: {digest} already available");
                return;
            }
        }

        let (tx, rx) = tokio::sync::oneshot::channel::<()>();

        let values = waiters
            .entry(digest.clone())
            .or_insert_with(std::vec::Vec::new);
        values.push(tx);

        drop(waiters);

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

    async fn manifest_available(&self, digest: &Digest) {
        let mut waiters = self.manifest_waiters.lock().await;

        if let Some(manifests) = waiters.remove(digest) {
            for sender in manifests {
                if sender.send(()).is_err() {
                    warn!("Some manifest waiters may have failed: {digest}");
                }
            }
        }
    }

    pub async fn wait_for_manifest(&self, digest: &Digest) {
        let mut waiters = self.manifest_waiters.lock().await;

        if let Some(manifest) = self.get_manifest_directly(digest).await {
            if manifest.locations.contains(&self.settings.identifier) {
                // manifest already exists at this endpoint, no need to wait
                return;
            }
        }

        let (tx, rx) = tokio::sync::oneshot::channel::<()>();

        let values = waiters
            .entry(digest.clone())
            .or_insert_with(std::vec::Vec::new);
        values.push(tx);

        drop(waiters);

        match rx.await {
            Ok(_) => (),
            Err(err) => {
                warn!("Failure whilst waiting for manifest to be downloaded: {digest}: {err}");
            }
        }
    }

    pub async fn is_blob_available(&self, repository: &RepositoryName, hash: &Digest) -> bool {
        let group = self.group.read().await;
        let state = &group.store().store;
        state.is_blob_available(repository, hash)
    }

    pub async fn get_blob_directly(&self, hash: &Digest) -> Option<Blob> {
        let group = self.group.read().await;
        let state = &group.store().store;
        state.get_blob_directly(hash)
    }

    pub async fn get_blob(&self, repository: &RepositoryName, hash: &Digest) -> Option<Blob> {
        let group = self.group.read().await;
        let state = &group.store().store;
        state.get_blob(repository, hash)
    }

    pub async fn get_manifest_directly(&self, hash: &Digest) -> Option<Manifest> {
        let group = self.group.read().await;
        let state = &group.store().store;
        state.get_manifest_directly(hash)
    }
    pub async fn get_manifest(
        &self,
        repository: &RepositoryName,
        hash: &Digest,
    ) -> Option<Manifest> {
        let group = self.group.read().await;
        let state = &group.store().store;
        state.get_manifest(repository, hash)
    }
    pub async fn get_tag(&self, repository: &RepositoryName, tag: &str) -> Option<Digest> {
        let group = self.group.read().await;
        let state = &group.raft.raft_log.store.store;
        state.get_tag(repository, tag)
    }

    pub async fn get_tags(&self, repository: &RepositoryName) -> Option<Vec<String>> {
        let group = self.group.read().await;
        let state = &group.store().store;
        state.get_tags(repository)
    }

    pub async fn is_manifest_available(&self, repository: &RepositoryName, hash: &Digest) -> bool {
        let group = self.group.read().await;
        let state = &group.store().store;
        state.is_manifest_available(repository, hash)
    }

    pub async fn get_orphaned_blobs(&self) -> Vec<BlobEntry> {
        let group = self.group.read().await;
        let state = &group.store().store;
        state.get_orphaned_blobs()
    }

    pub async fn get_orphaned_manifests(&self) -> Vec<ManifestEntry> {
        let group = self.group.read().await;
        let state = &group.store().store;
        state.get_orphaned_manifests()
    }
}

async fn handle_messages(app: &RegistryApp, messages: Vec<Message>) -> anyhow::Result<()> {
    for msg in messages {
        let serialized = protobuf::Message::write_to_bytes(&msg).unwrap();
        let dest = msg.to;
        if let Some(outbox) = app.outboxes.get(&dest) {
            outbox
                .send(serialized)
                .await
                .context("Failure to enqueue message to {dest}")?;
        }
    }

    Ok(())
}

async fn handle_commits(
    app: Arc<RegistryApp>,
    store: &mut RegistryStorage,
    cbs: &mut HashMap<u64, tokio::sync::oneshot::Sender<u64>>,
    actions_tx: &tokio::sync::mpsc::Sender<Vec<RegistryAction>>,
    entries: Vec<Entry>,
) -> anyhow::Result<()> {
    if entries.is_empty() {
        return Ok(());
    }

    for entry in entries {
        if entry.index <= store.applied_index {
            continue;
        }

        info!("Applying entry {entry:?}");

        if entry.get_entry_type() == EntryType::EntryNormal && !entry.data.is_empty() {
            let actions: Vec<RegistryAction> = serde_json::from_slice(entry.get_data()).unwrap();
            store.dispatch_actions(&actions);

            if let Some(cb) = cbs.remove(&bincode::deserialize(&entry.context).unwrap()) {
                cb.send(entry.index).unwrap();
            }

            for action in &actions {
                match action {
                    RegistryAction::ManifestStored {
                        timestamp: _,
                        digest,
                        location,
                        user: _,
                    } => {
                        if &app.settings.identifier == location {
                            app.manifest_available(digest).await;
                        }
                    }
                    RegistryAction::BlobStored {
                        timestamp: _,
                        digest,
                        location,
                        user: _,
                    } => {
                        if &app.settings.identifier == location {
                            app.blob_available(digest).await;
                        }
                    }
                    _ => {}
                }
            }

            actions_tx
                .send(actions)
                .await
                .context("Failed to notify mirrorer about new application state")?;
        }

        store.applied_index = entry.index;
    }

    Ok(())
}

async fn on_ready(
    app: &Arc<RegistryApp>,
    cbs: &mut HashMap<u64, tokio::sync::oneshot::Sender<u64>>,
    actions_tx: &tokio::sync::mpsc::Sender<Vec<RegistryAction>>,
) -> anyhow::Result<()> {
    let mut group = app.group.write().await;

    if !group.has_ready() {
        return Ok(());
    }

    // Get the `Ready` with `RawNode::ready` interface.
    let mut ready = group.ready();

    if !ready.messages().is_empty() {
        // Send out the messages come from the node.comm
        handle_messages(app, ready.take_messages())
            .await
            .context("Failed to send ready messages")?;
    }

    if !ready.snapshot().is_empty() {
        // This is a snapshot, we need to apply the snapshot at first.
        let store = &mut group.raft.raft_log.store;
        store
            .apply_snapshot(ready.snapshot().clone())
            .await
            .unwrap();
    }

    {
        let store = &mut group.raft.raft_log.store;
        handle_commits(
            app.clone(),
            store,
            cbs,
            actions_tx,
            ready.take_committed_entries(),
        )
        .await?;
    }

    if !ready.entries().is_empty() {
        let store = &mut group.raft.raft_log.store;
        store
            .append(ready.entries())
            .await
            .context("Failed to persist log entries")?;
    }

    if let Some(hs) = ready.hs() {
        let store = &mut group.raft.raft_log.store;
        store
            .set_hardstate(hs.clone())
            .context("Failed to update hardstate")?;
    }

    if !ready.persisted_messages().is_empty() {
        handle_messages(app, ready.take_persisted_messages())
            .await
            .context("Failed to send persisted messages")?;
    }

    let mut light_rd = group.advance(ready);
    if let Some(commit) = light_rd.commit_index() {
        let store = &mut group.raft.raft_log.store;
        store.set_commit(commit);
    }
    handle_messages(app, light_rd.take_messages())
        .await
        .context("Filed to send light ready messages")?;

    {
        let store = &mut group.raft.raft_log.store;
        handle_commits(
            app.clone(),
            store,
            cbs,
            actions_tx,
            light_rd.take_committed_entries(),
        )
        .await?;
    }

    group.advance_apply();

    Ok(())
}

pub async fn do_raft_ticks(
    app: Arc<RegistryApp>,
    mut mailbox: tokio::sync::mpsc::Receiver<Msg>,
    actions_tx: tokio::sync::mpsc::Sender<Vec<RegistryAction>>,
) -> anyhow::Result<()> {
    let mut t = Instant::now();
    let mut timeout = Duration::from_millis(100);
    let mut cbs = HashMap::new();

    let mut lifecycle = app.subscribe_lifecycle();

    loop {
        tokio::select! {
            post = mailbox.recv() => {
                match post {
                    Some(crate::app::Msg::Propose { actions, id, cb }) => {
                        cbs.insert(id, cb);
                        let mut r = app.group.write().await;
                        r.propose(
                            bincode::serialize(&id).unwrap(),
                            serde_json::to_vec(&actions).unwrap(),
                        )
                        .unwrap();
                    }
                    Some(crate::app::Msg::Raft(m)) => {
                        let mut r = app.group.write().await;
                        r.step(m).unwrap();
                    }
                    None => {
                        break;
                    }
                }
            }
            _ = tokio::time::sleep(timeout) => {},
            Ok(_ev) = lifecycle.recv() => {
                info!("Raft loop: Graceful shutdown");
                break;
            }
        }

        let d = t.elapsed();
        t = Instant::now();
        if d >= timeout {
            timeout = Duration::from_millis(100);
            let mut r = app.group.write().await;
            r.tick();
        } else {
            timeout -= d;
        }

        on_ready(&app, &mut cbs, &actions_tx)
            .await
            .context("Failed to apply ready state")?;
    }

    Ok(())
}
