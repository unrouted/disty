use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::{Bound, RangeBounds};
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;

use chrono::{DateTime, Utc};
use log::warn;
use openraft::async_trait::async_trait;
use openraft::storage::LogState;
use openraft::storage::Snapshot;
use openraft::AnyError;
use openraft::EffectiveMembership;
use openraft::Entry;
use openraft::EntryPayload;
use openraft::ErrorSubject;
use openraft::ErrorVerb;
use openraft::LogId;
use openraft::RaftLogReader;
use openraft::RaftSnapshotBuilder;
use openraft::RaftStorage;
use openraft::SnapshotMeta;
use openraft::StateMachineChanges;
use openraft::StorageError;
use openraft::StorageIOError;
use openraft::Vote;
use serde::Deserialize;
use serde::Serialize;
use sled::{Db, IVec};
use tokio::sync::RwLock;

use crate::NodeId;
use crate::RegistryTypeConfig;
pub mod config;
pub mod registry_store;

use crate::store::config::Config;
use crate::types::{
    Blob, BlobEntry, Digest, Manifest, ManifestEntry, RegistryAction, RepositoryName,
};

#[derive(Debug)]
pub struct RegistrySnapshot {
    pub meta: SnapshotMeta<NodeId>,

    /// The data of the state machine at the time of this snapshot.
    pub data: Vec<u8>,
}

/**
 * Here you will set the types of request that will interact with the raft nodes.
 * For Registry the `Set` will be used to write data (key and value) to the raft database.
 * The `AddNode` will append a new node to the current existing shared list of nodes.
 * You will want to add any request that can write data in all nodes here.
 */
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum RegistryRequest {
    Set { key: String, value: String },
    RepositoryTransaction { actions: Vec<RegistryAction> },
}

/**
 * Here you will defined what type of answer you expect from reading the data of a node.
 * In this Registry it will return a optional value from a given key in
 * the `RegistryRequest.Set`.
 *
 * TODO: SHould we explain how to create multiple `AppDataResponse`?
 *
 */
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RegistryResponse {
    pub value: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct StateMachineContent {
    pub last_applied_log: Option<LogId<NodeId>>,

    // TODO: it should not be Option.
    pub last_membership: EffectiveMembership<NodeId>,

    /// Application data.
    pub data: BTreeMap<String, String>,

    pub sequance: u64,
}

/**
 * Here defines a state machine of the raft, this state represents a copy of the data
 * between each node. Note that we are using `serde` to serialize the `data`, which has
 * a implementation to be serialized. Note that for this test we set both the key and
 * value as String, but you could set any type of value that has the serialization impl.
 */
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct RegistryStateMachine {
    pub last_applied_log: Option<LogId<NodeId>>,
    pub last_membership: EffectiveMembership<NodeId>,

    blobs: HashMap<Digest, Blob>,
    manifests: HashMap<Digest, Manifest>,
    tags: HashMap<RepositoryName, HashMap<String, Digest>>,

    // Resources which are known to the graph but not stored on the local machine
    missing_manifests: HashSet<Digest>,
    missing_blobs: HashSet<Digest>,

    // Leftovers
    pub data: BTreeMap<String, String>,
}

impl RegistryStateMachine {
    fn get_mut_blob(&mut self, digest: &Digest, timestamp: DateTime<Utc>) -> Option<&mut Blob> {
        if let Some(mut blob) = self.blobs.get_mut(digest) {
            blob.updated = timestamp;
            return Some(blob);
        }

        None
    }

    fn get_or_insert_blob(&mut self, digest: Digest, timestamp: DateTime<Utc>) -> &mut Blob {
        let mut blob = self.blobs.entry(digest).or_insert_with(|| Blob {
            created: timestamp,
            updated: timestamp,
            content_type: None,
            size: None,
            dependencies: Some(vec![]),
            locations: HashSet::new(),
            repositories: HashSet::new(),
        });

        blob.updated = timestamp;

        blob
    }

    fn get_mut_manifest(
        &mut self,
        digest: &Digest,
        timestamp: DateTime<Utc>,
    ) -> Option<&mut Manifest> {
        if let Some(mut manifest) = self.manifests.get_mut(digest) {
            manifest.updated = timestamp;
            return Some(manifest);
        }

        None
    }

    fn get_or_insert_manifest(
        &mut self,
        digest: Digest,
        timestamp: DateTime<Utc>,
    ) -> &mut Manifest {
        let mut manifest = self.manifests.entry(digest).or_insert_with(|| Manifest {
            created: timestamp,
            updated: timestamp,
            content_type: None,
            size: None,
            dependencies: Some(vec![]),
            locations: HashSet::new(),
            repositories: HashSet::new(),
        });

        manifest.updated = timestamp;

        manifest
    }

    pub fn is_blob_available(&self, repository: &RepositoryName, hash: &Digest) -> bool {
        match self.blobs.get(hash) {
            None => false,
            Some(blob) => blob.repositories.contains(repository),
        }
    }

    pub fn get_blob_directly(&self, hash: &Digest) -> Option<Blob> {
        self.blobs.get(hash).cloned()
    }

    pub fn get_blob(&self, repository: &RepositoryName, hash: &Digest) -> Option<Blob> {
        match self.blobs.get(hash) {
            None => None,
            Some(blob) => {
                if blob.repositories.contains(repository) {
                    return Some(blob.clone());
                }
                None
            }
        }
    }

    pub fn get_manifest_directly(&self, hash: &Digest) -> Option<Manifest> {
        self.manifests.get(hash).cloned()
    }
    pub fn get_manifest(&self, repository: &RepositoryName, hash: &Digest) -> Option<Manifest> {
        match self.manifests.get(hash) {
            None => None,
            Some(manifest) => {
                if manifest.repositories.contains(repository) {
                    return Some(manifest.clone());
                }
                None
            }
        }
    }
    pub fn get_tag(&self, repository: &RepositoryName, tag: &str) -> Option<Digest> {
        match self.tags.get(repository) {
            Some(repository) => repository.get(tag).cloned(),
            None => None,
        }
    }

    pub fn get_tags(&self, repository: &RepositoryName) -> Option<Vec<String>> {
        self.tags
            .get(repository)
            .map(|repository| repository.keys().cloned().collect())
    }

    pub fn is_manifest_available(&self, repository: &RepositoryName, hash: &Digest) -> bool {
        match self.manifests.get(hash) {
            None => false,
            Some(manifest) => manifest.repositories.contains(repository),
        }
    }

    pub fn get_orphaned_blobs(&self) -> Vec<BlobEntry> {
        let blobs: HashSet<Digest> = self.blobs.keys().cloned().collect();

        let mut visited: HashSet<Digest> = HashSet::new();
        let mut visiting: HashSet<Digest> = HashSet::new();

        for manifest in self.manifests.values() {
            if let Some(dependencies) = &manifest.dependencies {
                visiting.extend(dependencies.iter().cloned());
            }
        }

        while let Some(digest) = visiting.iter().next().cloned() {
            match self.blobs.get(&digest) {
                Some(blob) => match &blob.dependencies {
                    Some(dependencies) => {
                        visiting.extend(
                            dependencies
                                .iter()
                                .cloned()
                                .filter(|digest| !visited.contains(digest)),
                        );
                    }
                    None => {}
                },
                _ => {
                    warn!("Dangling dependency found: {digest} missing");
                }
            }

            visiting.remove(&digest);
            visited.insert(digest);
        }

        blobs
            .difference(&visited)
            .cloned()
            .map(|digest| BlobEntry {
                blob: self.blobs.get(&digest).unwrap().clone(),
                digest,
            })
            .collect::<Vec<BlobEntry>>()
    }

    pub fn get_orphaned_manifests(&self) -> Vec<ManifestEntry> {
        let manifests: HashSet<Digest> = self.manifests.keys().cloned().collect();
        let mut tags: HashSet<Digest> = HashSet::new();

        for repo_tags in self.tags.values() {
            tags.extend(repo_tags.values().cloned());
        }

        manifests
            .difference(&tags)
            .cloned()
            .map(|digest| ManifestEntry {
                manifest: self.manifests.get(&digest).unwrap().clone(),
                digest,
            })
            .collect::<Vec<ManifestEntry>>()
    }

    pub fn dispatch(&mut self, actions: &Vec<RegistryAction>) {
        for action in actions {
            match action {
                RegistryAction::Empty {} => {}
                RegistryAction::BlobStored {
                    timestamp,
                    user: _,
                    digest,
                    location,
                } => {
                    let blob = self.get_or_insert_blob(digest.clone(), *timestamp);
                    blob.locations.insert(location.clone());

                    //if location == self.machine_identifier {
                    //    self.blob_available(&digest).await;
                    //}
                }
                RegistryAction::BlobUnstored {
                    timestamp,
                    user: _,
                    digest,
                    location,
                } => {
                    if let Some(blob) = self.get_mut_blob(digest, *timestamp) {
                        blob.locations.remove(location);

                        if blob.locations.is_empty() {
                            self.blobs.remove(digest);
                        }
                    }
                }
                RegistryAction::BlobMounted {
                    timestamp,
                    user: _,
                    digest,
                    repository,
                } => {
                    let blob = self.get_or_insert_blob(digest.clone(), *timestamp);
                    blob.repositories.insert(repository.clone());
                }
                RegistryAction::BlobUnmounted {
                    timestamp,
                    user: _,
                    digest,
                    repository,
                } => {
                    if let Some(blob) = self.get_mut_blob(digest, *timestamp) {
                        blob.repositories.remove(repository);
                    }
                }
                RegistryAction::BlobInfo {
                    timestamp,
                    digest,
                    dependencies,
                    content_type,
                } => {
                    if let Some(mut blob) = self.get_mut_blob(digest, *timestamp) {
                        blob.dependencies = Some(dependencies.clone());
                        blob.content_type = Some(content_type.clone());
                    }
                }
                RegistryAction::BlobStat {
                    timestamp,
                    digest,
                    size,
                } => {
                    if let Some(mut blob) = self.get_mut_blob(digest, *timestamp) {
                        blob.size = Some(*size);
                    }
                }
                RegistryAction::ManifestStored {
                    timestamp,
                    user: _,
                    digest,
                    location,
                } => {
                    let manifest = self.get_or_insert_manifest(digest.clone(), *timestamp);
                    manifest.locations.insert(location.clone());

                    //if location == self.machine_identifier {
                    //    self.manifest_available(&digest).await;
                    // }
                }
                RegistryAction::ManifestUnstored {
                    timestamp,
                    user: _,
                    digest,
                    location,
                } => {
                    if let Some(manifest) = self.get_mut_manifest(digest, *timestamp) {
                        manifest.locations.remove(location);

                        if manifest.locations.is_empty() {
                            self.manifests.remove(digest);
                        }
                    }
                }
                RegistryAction::ManifestMounted {
                    timestamp,
                    user: _,
                    digest,
                    repository,
                } => {
                    let manifest = self.get_or_insert_manifest(digest.clone(), *timestamp);
                    manifest.repositories.insert(repository.clone());
                }
                RegistryAction::ManifestUnmounted {
                    timestamp,
                    user: _,
                    digest,
                    repository,
                } => {
                    if let Some(manifest) = self.get_mut_manifest(digest, *timestamp) {
                        manifest.repositories.remove(repository);

                        if let Some(tags) = self.tags.get_mut(repository) {
                            tags.retain(|_, value| value != digest);
                        }
                    }
                }
                RegistryAction::ManifestInfo {
                    timestamp,
                    digest,
                    dependencies,
                    content_type,
                } => {
                    if let Some(mut manifest) = self.get_mut_manifest(digest, *timestamp) {
                        manifest.dependencies = Some(dependencies.clone());
                        manifest.content_type = Some(content_type.clone());
                    }
                }
                RegistryAction::ManifestStat {
                    timestamp,
                    digest,
                    size,
                } => {
                    if let Some(mut manifest) = self.get_mut_manifest(digest, *timestamp) {
                        manifest.size = Some(*size);
                    }
                }
                RegistryAction::HashTagged {
                    timestamp: _,
                    user: _,
                    digest,
                    repository,
                    tag,
                } => {
                    let repository = self
                        .tags
                        .entry(repository.clone())
                        .or_insert_with(HashMap::new);
                    repository.insert(tag.clone(), digest.clone());
                }
            }
        }
    }

    pub fn to_content(&self) -> StateMachineContent {
        StateMachineContent {
            last_applied_log: self.last_applied_log,
            last_membership: self.last_membership.clone(),
            data: self.data.clone(),
            sequance: 0,
        }
    }

    pub fn from_content(&mut self, content: &StateMachineContent) {
        // ** Build from content **
        self.last_applied_log = content.last_applied_log;
        self.last_membership = content.last_membership.clone();
        self.data = content.data.clone();
    }
}

#[derive(Debug)]
pub struct RegsistryStore {
    last_purged_log_id: RwLock<Option<LogId<NodeId>>>,

    /// The Raft log.
    pub log: sled::Tree, //RwLock<BTreeMap<u64, Entry<StorageRaftTypeConfig>>>,

    /// The Raft state machine.
    pub state_machine: RwLock<RegistryStateMachine>,

    /// The current granted vote.
    vote: sled::Tree,

    snapshot_idx: Arc<Mutex<u64>>,

    current_snapshot: RwLock<Option<RegistrySnapshot>>,

    config: Config,

    pub node_id: NodeId,
}

fn get_sled_db(config: Config, node_id: NodeId) -> Db {
    let db_path = format!(
        "{}/{}-{}.binlog",
        config.journal_path, config.instance_prefix, node_id
    );
    let db = sled::open(db_path.clone()).unwrap();
    tracing::debug!("get_sled_db: created log at: {:?}", db_path);
    db
}

impl RegsistryStore {
    pub async fn open_test() -> Arc<RegsistryStore> {
        let path = Path::new("/tmp/journal/match-1.binlog");
        if path.exists() {
            std::fs::remove_dir_all(path).unwrap();
        }
        Arc::new(RegsistryStore::open_create(1))
    }

    pub fn open_create(node_id: NodeId) -> RegsistryStore {
        tracing::info!("open_create, node_id: {}", node_id);

        let config = Config::default();

        let db = get_sled_db(config.clone(), node_id);

        let log = db
            .open_tree(format!("journal_entities_{}", node_id))
            .unwrap();

        let vote = db.open_tree(format!("votes_{}", node_id)).unwrap();

        let current_snapshot = RwLock::new(None);

        RegsistryStore {
            last_purged_log_id: Default::default(),
            config,
            node_id,
            log,
            state_machine: Default::default(),
            vote,
            snapshot_idx: Arc::new(Mutex::new(0)),
            current_snapshot,
        }
    }
}

//Store trait for restore things from snapshot and log
#[async_trait]
pub trait Restore {
    async fn restore(&mut self);
}

#[async_trait]
impl Restore for Arc<RegsistryStore> {
    #[tracing::instrument(level = "trace", skip(self))]
    async fn restore(&mut self) {
        tracing::debug!("restore");
        let log = &self.log;

        let first = log
            .iter()
            .rev()
            .next()
            .map(|res| res.unwrap())
            .map(|(_, val)| {
                serde_json::from_slice::<Entry<RegistryTypeConfig>>(&*val)
                    .unwrap()
                    .log_id
            });

        match first {
            Some(x) => {
                tracing::debug!("restore: first log id = {:?}", x);
                let mut ld = self.last_purged_log_id.write().await;
                *ld = Some(x);
            }
            None => {}
        }

        let snapshot = self.get_current_snapshot().await.unwrap();

        match snapshot {
            Some(ss) => {
                self.install_snapshot(&ss.meta, ss.snapshot).await.unwrap();
            }
            None => {}
        }
    }
}

#[async_trait]
impl RaftLogReader<RegistryTypeConfig> for Arc<RegsistryStore> {
    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_log_state(
        &mut self,
    ) -> Result<LogState<RegistryTypeConfig>, StorageError<NodeId>> {
        let log = &self.log;
        let last = log
            .iter()
            .rev()
            .next()
            .map(|res| res.unwrap())
            .map(|(_, val)| {
                serde_json::from_slice::<Entry<RegistryTypeConfig>>(&*val)
                    .unwrap()
                    .log_id
            });

        let last_purged = *self.last_purged_log_id.read().await;

        let last = match last {
            None => last_purged,
            Some(x) => Some(x),
        };
        tracing::debug!(
            "get_log_state: last_purged = {:?}, last = {:?}",
            last_purged,
            last
        );
        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id: last,
        })
    }

    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send + Sync>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<RegistryTypeConfig>>, StorageError<NodeId>> {
        let log = &self.log;
        let response = log
            .range(transform_range_bound(range))
            .map(|res| res.unwrap())
            .map(|(_, val)| serde_json::from_slice::<Entry<RegistryTypeConfig>>(&*val).unwrap())
            .collect();

        Ok(response)
    }
}

fn transform_range_bound<RB: RangeBounds<u64> + Clone + Debug + Send + Sync>(
    range: RB,
) -> (Bound<IVec>, Bound<IVec>) {
    (
        serialize_bound(&range.start_bound()),
        serialize_bound(&range.end_bound()),
    )
}

fn serialize_bound(v: &Bound<&u64>) -> Bound<IVec> {
    match v {
        Bound::Included(v) => Bound::Included(IVec::from(&v.to_be_bytes())),
        Bound::Excluded(v) => Bound::Excluded(IVec::from(&v.to_be_bytes())),
        Bound::Unbounded => Bound::Unbounded,
    }
}

#[async_trait]
impl RaftSnapshotBuilder<RegistryTypeConfig, Cursor<Vec<u8>>> for Arc<RegsistryStore> {
    #[tracing::instrument(level = "trace", skip(self))]
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<RegistryTypeConfig, Cursor<Vec<u8>>>, StorageError<NodeId>> {
        let (data, last_applied_log);

        {
            // Serialize the data of the state machine.
            let state_machine = self.state_machine.read().await;
            data = serde_json::to_vec(&*state_machine).map_err(|e| {
                StorageIOError::new(
                    ErrorSubject::StateMachine,
                    ErrorVerb::Read,
                    AnyError::new(&e),
                )
            })?;

            last_applied_log = state_machine.last_applied_log;
        }

        let last_applied_log = match last_applied_log {
            None => {
                panic!("can not compact empty state machine");
            }
            Some(x) => x,
        };

        let snapshot_idx = {
            let mut l = self.snapshot_idx.lock().unwrap();
            *l += 1;
            *l
        };

        let snapshot_id = format!(
            "{}-{}-{}",
            last_applied_log.leader_id, last_applied_log.index, snapshot_idx
        );

        let meta = SnapshotMeta {
            last_log_id: last_applied_log,
            snapshot_id,
        };

        let snapshot = RegistrySnapshot {
            meta: meta.clone(),
            data: data.clone(),
        };

        {
            let mut current_snapshot = self.current_snapshot.write().await;
            *current_snapshot = Some(snapshot);
        }

        self.write_snapshot().await.unwrap();

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

#[async_trait]
impl RaftStorage<RegistryTypeConfig> for Arc<RegsistryStore> {
    type SnapshotData = Cursor<Vec<u8>>;
    type LogReader = Self;
    type SnapshotBuilder = Self;

    #[tracing::instrument(level = "trace", skip(self))]
    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.vote
            .insert(b"vote", IVec::from(serde_json::to_vec(vote).unwrap()))
            .unwrap();
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        let value = self.vote.get(b"vote").unwrap();
        match value {
            None => Ok(None),
            Some(val) => Ok(Some(serde_json::from_slice::<Vote<NodeId>>(&*val).unwrap())),
        }
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn append_to_log(
        &mut self,
        entries: &[&Entry<RegistryTypeConfig>],
    ) -> Result<(), StorageError<NodeId>> {
        let log = &self.log;
        for entry in entries {
            log.insert(
                entry.log_id.index.to_be_bytes(),
                IVec::from(serde_json::to_vec(&*entry).unwrap()),
            )
            .unwrap();
        }
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<NodeId>,
    ) -> Result<(), StorageError<NodeId>> {
        tracing::debug!("delete_log: [{:?}, +oo)", log_id);

        let log = &self.log;
        let keys = log
            .range(transform_range_bound(log_id.index..))
            .map(|res| res.unwrap())
            .map(|(k, _v)| k); //TODO Why originally used collect instead of the iter.
        for key in keys {
            log.remove(&key).unwrap();
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn purge_logs_upto(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        tracing::debug!("delete_log: [{:?}, +oo)", log_id);

        {
            let mut ld = self.last_purged_log_id.write().await;
            assert!(*ld <= Some(log_id));
            *ld = Some(log_id);
        }

        {
            let log = &self.log;

            let keys = log
                .range(transform_range_bound(..=log_id.index))
                .map(|res| res.unwrap())
                .map(|(k, _)| k);
            for key in keys {
                log.remove(&key).unwrap();
            }
        }

        Ok(())
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, EffectiveMembership<NodeId>), StorageError<NodeId>> {
        let state_machine = self.state_machine.read().await;
        Ok((
            state_machine.last_applied_log,
            state_machine.last_membership.clone(),
        ))
    }

    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn apply_to_state_machine(
        &mut self,
        entries: &[&Entry<RegistryTypeConfig>],
    ) -> Result<Vec<RegistryResponse>, StorageError<NodeId>> {
        let mut res = Vec::with_capacity(entries.len());

        let mut sm = self.state_machine.write().await;

        for entry in entries {
            tracing::debug!(%entry.log_id, "replicate to sm");

            sm.last_applied_log = Some(entry.log_id);

            match entry.payload {
                EntryPayload::Blank => res.push(RegistryResponse { value: None }),
                EntryPayload::Normal(ref req) => match req {
                    RegistryRequest::RepositoryTransaction { actions } => {
                        sm.dispatch(actions);
                    }
                    RegistryRequest::Set { key: _, value: _ } => {}
                },
                EntryPayload::Membership(ref mem) => {
                    sm.last_membership = EffectiveMembership::new(Some(entry.log_id), mem.clone());
                    res.push(RegistryResponse { value: None })
                }
            };
        }
        Ok(res)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Self::SnapshotData>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    #[tracing::instrument(level = "trace", skip(self, snapshot))]
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId>,
        snapshot: Box<Self::SnapshotData>,
    ) -> Result<StateMachineChanges<RegistryTypeConfig>, StorageError<NodeId>> {
        tracing::info!(
            { snapshot_size = snapshot.get_ref().len() },
            "decoding snapshot for installation"
        );

        let new_snapshot = RegistrySnapshot {
            meta: meta.clone(),
            data: snapshot.into_inner(),
        };

        // Update the state machine.
        {
            let updated_state_machine: RegistryStateMachine =
                serde_json::from_slice(&new_snapshot.data).map_err(|e| {
                    StorageIOError::new(
                        ErrorSubject::Snapshot(new_snapshot.meta.clone()),
                        ErrorVerb::Read,
                        AnyError::new(&e),
                    )
                })?;
            let mut state_machine = self.state_machine.write().await;
            *state_machine = updated_state_machine;
        }

        // Update current snapshot.
        let mut current_snapshot = self.current_snapshot.write().await;
        *current_snapshot = Some(new_snapshot);
        Ok(StateMachineChanges {
            last_applied: meta.last_log_id,
            is_snapshot: true,
        })
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<RegistryTypeConfig, Self::SnapshotData>>, StorageError<NodeId>>
    {
        tracing::debug!("get_current_snapshot: start");

        match &*self.current_snapshot.read().await {
            Some(snapshot) => {
                let data = snapshot.data.clone();
                Ok(Some(Snapshot {
                    meta: snapshot.meta.clone(),
                    snapshot: Box::new(Cursor::new(data)),
                }))
            }
            None => {
                let data = self.read_snapshot_file().await;
                //tracing::debug!("get_current_snapshot: data = {:?}",data);

                let data = match data {
                    Ok(c) => c,
                    Err(_e) => return Ok(None),
                };

                let content: RegistryStateMachine = serde_json::from_slice(&data).unwrap();

                let last_applied_log = content.last_applied_log.unwrap();
                tracing::debug!(
                    "get_current_snapshot: last_applied_log = {:?}",
                    last_applied_log
                );

                let snapshot_idx = {
                    let mut l = self.snapshot_idx.lock().unwrap();
                    *l += 1;
                    *l
                };

                let snapshot_id = format!(
                    "{}-{}-{}",
                    last_applied_log.leader_id, last_applied_log.index, snapshot_idx
                );

                let meta = SnapshotMeta {
                    last_log_id: last_applied_log,
                    snapshot_id,
                };

                tracing::debug!("get_current_snapshot: meta {:?}", meta);

                Ok(Some(Snapshot {
                    meta,
                    snapshot: Box::new(Cursor::new(data)),
                }))
            }
        }
    }
}

#[cfg(test)]
mod test {
    use openraft::testing::Suite;

    use super::RegsistryStore;

    #[test]
    pub fn test_store() {
        Suite::test_all(RegsistryStore::open_test).unwrap();
    }
}
