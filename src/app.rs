use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use raft::eraftpb::Message;
use raft::prelude::*;
use raft::RawNode;
use rocket::Shutdown;
use tokio::join;
use tokio::sync::broadcast::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::config::Configuration;

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
    services: RwLock<Vec<JoinHandle<()>>>,
    lifecycle: tokio::sync::broadcast::Sender<Lifecycle>,
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
        let (tx, rx) = tokio::sync::broadcast::channel(16);

        RegistryApp {
            group,
            inbox,
            seq: AtomicU64::new(1),
            outboxes,
            settings,
            services: RwLock::new(vec![]),
            lifecycle: tx,
        }
    }

    pub fn subscribe_lifecycle(&self) -> Receiver<Lifecycle> {
        self.lifecycle.subscribe()
    }

    pub async fn spawn<T>(&self, task: T)
    where
        T: Future<Output = ()>,
        T: Send + 'static,
    {
        let handle: JoinHandle<()> = tokio::spawn(task);
        self.services.write().await.push(handle);
    }

    pub async fn wait_for_shutdown(&self) -> Result<()> {
        tokio::signal::ctrl_c().await.unwrap();

        println!("Starting shutdown...");
        self.lifecycle
            .send(Lifecycle::Shutdown)
            .context("Failed to notify services of shutdown")?;

        println!("Awaiting on pending services");
        for handle in self.services.write().await.drain(..) {
            handle.await;
        }
        // self.shutdown.notify()

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

    pub async fn submit_local(&self, actions: Vec<RegistryAction>) -> Option<u64> {
        let (tx, rx) = tokio::sync::oneshot::channel();

        let proposal = Msg::Propose {
            actions,
            id: self.seq.fetch_add(1, Ordering::Relaxed),
            cb: tx,
        };

        self.inbox.send(proposal).await;

        match rx.await {
            Err(err) => {
                println!("RECV FAILURE: {err:?}");
                None
            }
            Ok(value) => {
                println!("SUBMIT SUCCESS");
                Some(value)
            }
        }
    }

    pub async fn submit_remote(&self, actions: Vec<RegistryAction>, idx: u64) -> Option<u64> {
        let peer = self.settings.peers.get((idx as usize) - 1).unwrap();
        let address = &peer.raft.address;
        let port = peer.raft.port;
        let url = format!("http://{address}:{port}/submit");

        let client = reqwest::Client::builder()
            .user_agent("distribd/raft")
            .build()
            .unwrap();

        let resp = client.post(&url).json(&actions).send().await;

        Some(resp.unwrap().json().await.unwrap())
    }

    pub async fn submit(&self, actions: Vec<RegistryAction>) -> bool {
        let leader_id = self.wait_for_leader().await;

        if leader_id == self.group.read().await.raft.id {
            self.submit_local(actions).await;
        } else {
            self.submit_remote(actions, leader_id).await;

            // FIXME: Wait until this commit is available locally
        }

        true
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

async fn handle_messages(app: &RegistryApp, messages: Vec<Message>) {
    for msg in messages {
        let serialized = protobuf::Message::write_to_bytes(&msg).unwrap();
        let dest = msg.to;
        if let Some(outbox) = app.outboxes.get(&dest) {
            outbox.send(serialized).await;
        }
    }
}

async fn handle_commits(
    _app: Arc<RegistryApp>,
    store: &mut RegistryStorage,
    cbs: &mut HashMap<u64, tokio::sync::oneshot::Sender<u64>>,
    actions_tx: &tokio::sync::mpsc::Sender<Vec<RegistryAction>>,
    entries: Vec<Entry>,
) {
    if entries.len() == 0 {
        return;
    }

    for entry in entries {
        println!("Applying: {}", entry.index);

        if entry.get_entry_type() == EntryType::EntryNormal && !entry.data.is_empty() {
            let actions: Vec<RegistryAction> = serde_json::from_slice(entry.get_data()).unwrap();
            store.dispatch_actions(&actions);

            if let Some(cb) = cbs.remove(&bincode::deserialize(&entry.context).unwrap()) {
                cb.send(entry.index);
            }

            actions_tx.send(actions).await;
        }

        store.applied_index = entry.index;
        println!("Applied: {}", entry.index);
        // TODO: handle EntryConfChange
    }

    println!("Done handling commits");
}

async fn on_ready(
    app: &Arc<RegistryApp>,
    cbs: &mut HashMap<u64, tokio::sync::oneshot::Sender<u64>>,
    actions_tx: &tokio::sync::mpsc::Sender<Vec<RegistryAction>>,
) {
    let mut group = app.group.write().await;

    if !group.has_ready() {
        return;
    }

    // Get the `Ready` with `RawNode::ready` interface.
    let mut ready = group.ready();

    if !ready.messages().is_empty() {
        // Send out the messages come from the node.comm
        handle_messages(app, ready.take_messages()).await;
    }

    if !ready.snapshot().is_empty() {
        // This is a snapshot, we need to apply the snapshot at first.
        let store = &mut group.raft.raft_log.store;
        store.apply_snapshot(ready.snapshot().clone()).unwrap();
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
        .await;
    }

    if !ready.entries().is_empty() {
        let store = &mut group.raft.raft_log.store;
        store.append(ready.entries()).unwrap();
    }

    if let Some(hs) = ready.hs() {
        let store = &mut group.raft.raft_log.store;
        store.set_hardstate(hs.clone());
    }

    if !ready.persisted_messages().is_empty() {
        handle_messages(app, ready.take_persisted_messages()).await;
    }

    let mut light_rd = group.advance(ready);
    if let Some(commit) = light_rd.commit_index() {
        let store = &mut group.raft.raft_log.store;
        store.set_commit(commit);
    }
    handle_messages(app, light_rd.take_messages()).await;

    {
        let store = &mut group.raft.raft_log.store;
        handle_commits(
            app.clone(),
            store,
            cbs,
            actions_tx,
            light_rd.take_committed_entries(),
        )
        .await;
    }

    group.advance_apply();
}

pub async fn do_raft_ticks(
    app: Arc<RegistryApp>,
    mut mailbox: tokio::sync::mpsc::Receiver<Msg>,
    actions_tx: tokio::sync::mpsc::Sender<Vec<RegistryAction>>,
) {
    let mut t = Instant::now();
    let mut timeout = Duration::from_millis(100);
    let mut cbs = HashMap::new();

    loop {
        match tokio::time::timeout(timeout, mailbox.recv()).await {
            Ok(Some(crate::app::Msg::Propose { actions, id, cb })) => {
                cbs.insert(id, cb);
                let mut r = app.group.write().await;
                r.propose(
                    bincode::serialize(&id).unwrap(),
                    serde_json::to_vec(&actions).unwrap(),
                )
                .unwrap();
            }
            Ok(Some(crate::app::Msg::Raft(m))) => {
                let mut r = app.group.write().await;
                r.step(m).unwrap();
            }
            Ok(None) => {
                return;
            }
            Err(_) => {}
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

        on_ready(&app, &mut cbs, &actions_tx).await;
    }
}
