#[cfg(test)]
mod test;

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::error::Error;
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::Arc;
use std::sync::RwLock;

use bincode::options;
use bincode::Options;
use byteorder::BigEndian;
use byteorder::ByteOrder;
use byteorder::ReadBytesExt;
use openraft::async_trait::async_trait;
use openraft::storage::LogState;
use openraft::storage::Snapshot;
use openraft::AnyError;
use openraft::BasicNode;
use openraft::Entry;
use openraft::EntryPayload;
use openraft::ErrorSubject;
use openraft::ErrorVerb;
use openraft::LogId;
use openraft::RaftLogReader;
use openraft::RaftSnapshotBuilder;
use openraft::RaftStorage;
use openraft::SnapshotMeta;
use openraft::StorageError;
use openraft::StorageIOError;
use openraft::StoredMembership;
use openraft::Vote;
use prometheus_client::registry::Registry;
use serde::Deserialize;
use serde::Serialize;
use sled::transaction::TransactionalTree;
use sled::Transactional;
use sled::Tree;
use tokio::sync::watch::channel;
use tokio::sync::watch::Sender;

use crate::types::Blob;
use crate::types::Digest;
use crate::types::Manifest;
use crate::types::RegistryAction;
use crate::types::RepositoryName;
use crate::types::TagKey;
use crate::RegistryTypeConfig;

use self::metrics::StorageMetrics;

pub mod metrics;

pub type RegistryNodeId = u64;

/**
 * Here you will set the types of request that will interact with the raft nodes.
 * For Registry the `Set` will be used to write data (key and value) to the raft database.
 * The `AddNode` will append a new node to the current existing shared list of nodes.
 * You will want to add any request that can write data in all nodes here.
 */
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum RegistryRequest {
    Transaction { actions: Vec<RegistryAction> },
}

/**
 * Here you will defined what type of answer you expect from reading the data of a node.
 * In this Registry it will return a optional value from a given key in
 * the `RegistryRequest.Set`.
 *
 * TODO: Should we explain how to create multiple `AppDataResponse`?
 *
 */
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RegistryResponse {
    pub value: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RegistrySnapshot {
    pub meta: SnapshotMeta<RegistryNodeId, BasicNode>,

    /// The data of the state machine at the time of this snapshot.
    pub data: Vec<u8>,
}

/**
 * Here defines a state machine of the raft, this state represents a copy of the data
 * between each node. Note that we are using `serde` to serialize the `data`, which has
 * a implementation to be serialized. Note that for this test we set both the key and
 * value as String, but you could set any type of value that has the serialization impl.
 */
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct SerializableRegistryStateMachine {
    pub last_applied_log: Option<LogId<RegistryNodeId>>,

    pub last_membership: StoredMembership<RegistryNodeId, BasicNode>,

    /// Application data.
    pub manifests: BTreeMap<Digest, Manifest>,
    pub blobs: BTreeMap<Digest, Blob>,
    pub tags: BTreeMap<RepositoryName, BTreeMap<String, Digest>>,
}

#[derive(Debug)]
pub struct RegistryStateMachine {
    /// Application data.
    pub db: Arc<sled::Db>,
    pub metrics: StorageMetrics,
    pub pending_manifests: Arc<Sender<HashSet<Digest>>>,
    pub pending_blobs: Arc<Sender<HashSet<Digest>>>,
    pub manifest_waiters: HashMap<Digest, Vec<tokio::sync::oneshot::Sender<()>>>,
    pub blob_waiters: HashMap<Digest, Vec<tokio::sync::oneshot::Sender<()>>>,
}

impl From<&RegistryStateMachine> for SerializableRegistryStateMachine {
    fn from(state: &RegistryStateMachine) -> Self {
        let mut blob_tree = BTreeMap::new();
        for entry_res in blobs(&state.db).iter() {
            let entry = entry_res.expect("read db failed");

            let key = options()
                .with_big_endian()
                .deserialize::<Digest>(&entry.0)
                .expect("invalid data");
            let value = options()
                .with_big_endian()
                .deserialize::<Blob>(&entry.1)
                .expect("invalid data");
            blob_tree.insert(key, value);
        }

        let mut manifest_tree = BTreeMap::new();
        for entry_res in manifests(&state.db).iter() {
            let entry = entry_res.expect("read db failed");

            let key = options()
                .with_big_endian()
                .deserialize::<Digest>(&entry.0)
                .expect("invalid data");
            let value = options()
                .with_big_endian()
                .deserialize::<Manifest>(&entry.1)
                .expect("invalid data");
            manifest_tree.insert(key, value);
        }

        let mut tag_tree = BTreeMap::new();
        for entry_res in tags(&state.db).iter() {
            let entry = entry_res.expect("read db failed");

            let key = options()
                .with_big_endian()
                .deserialize::<TagKey>(&entry.0)
                .expect("invalid data");
            let value = options()
                .with_big_endian()
                .deserialize::<Digest>(&entry.1)
                .expect("invalid data");

            let repo = tag_tree.entry(key.repository).or_insert_with(BTreeMap::new);
            repo.insert(key.tag, value);
        }

        Self {
            last_applied_log: state.get_last_applied_log().expect("last_applied_log"),
            last_membership: state.get_last_membership().expect("last_membership"),
            manifests: manifest_tree,
            blobs: blob_tree,
            tags: tag_tree,
        }
    }
}

fn sm_r_err<E: Error + 'static>(e: E) -> StorageError<RegistryNodeId> {
    StorageIOError::new(
        ErrorSubject::StateMachine,
        ErrorVerb::Read,
        AnyError::new(&e),
    )
    .into()
}
fn sm_w_err<E: Error + 'static>(e: E) -> StorageError<RegistryNodeId> {
    StorageIOError::new(
        ErrorSubject::StateMachine,
        ErrorVerb::Write,
        AnyError::new(&e),
    )
    .into()
}
fn s_r_err<E: Error + 'static>(e: E) -> StorageError<RegistryNodeId> {
    StorageIOError::new(ErrorSubject::Store, ErrorVerb::Read, AnyError::new(&e)).into()
}
fn s_w_err<E: Error + 'static>(e: E) -> StorageError<RegistryNodeId> {
    StorageIOError::new(ErrorSubject::Store, ErrorVerb::Write, AnyError::new(&e)).into()
}
fn v_r_err<E: Error + 'static>(e: E) -> StorageError<RegistryNodeId> {
    StorageIOError::new(ErrorSubject::Vote, ErrorVerb::Read, AnyError::new(&e)).into()
}
fn v_w_err<E: Error + 'static>(e: E) -> StorageError<RegistryNodeId> {
    StorageIOError::new(ErrorSubject::Vote, ErrorVerb::Write, AnyError::new(&e)).into()
}
fn l_r_err<E: Error + 'static>(e: E) -> StorageError<RegistryNodeId> {
    StorageIOError::new(ErrorSubject::Logs, ErrorVerb::Read, AnyError::new(&e)).into()
}
fn l_w_err<E: Error + 'static>(e: E) -> StorageError<RegistryNodeId> {
    StorageIOError::new(ErrorSubject::Logs, ErrorVerb::Write, AnyError::new(&e)).into()
}
fn m_r_err<E: Error + 'static>(e: E) -> StorageError<RegistryNodeId> {
    StorageIOError::new(ErrorSubject::Store, ErrorVerb::Read, AnyError::new(&e)).into()
}
fn m_w_err<E: Error + 'static>(e: E) -> StorageError<RegistryNodeId> {
    StorageIOError::new(ErrorSubject::Store, ErrorVerb::Write, AnyError::new(&e)).into()
}
fn t_err<E: Error + 'static>(e: E) -> StorageError<RegistryNodeId> {
    StorageIOError::new(ErrorSubject::Store, ErrorVerb::Write, AnyError::new(&e)).into()
}

fn ct_err<E: Error + 'static>(e: E) -> sled::transaction::ConflictableTransactionError<AnyError> {
    sled::transaction::ConflictableTransactionError::Abort(AnyError::new(&e))
}

impl RegistryStateMachine {
    pub fn get_last_membership(
        &self,
    ) -> StorageResult<StoredMembership<RegistryNodeId, BasicNode>> {
        let state_machine = state_machine(&self.db);
        state_machine
            .get(b"last_membership")
            .map_err(m_r_err)
            .and_then(|value| {
                value
                    .map(|v| serde_json::from_slice(&v).map_err(sm_r_err))
                    .unwrap_or_else(|| Ok(StoredMembership::default()))
            })
    }
    async fn set_last_membership(
        &self,
        membership: StoredMembership<RegistryNodeId, BasicNode>,
    ) -> StorageResult<()> {
        let value = serde_json::to_vec(&membership).map_err(sm_w_err)?;
        let state_machine = state_machine(&self.db);
        state_machine
            .insert(b"last_membership", value)
            .map_err(m_w_err)?;

        let flushed = flush_async(&state_machine).await.map_err(m_w_err)?;
        self.metrics.flushed_bytes.inc_by(flushed as u64);
        Ok(())
    }
    fn set_last_membership_tx(
        &self,
        tx_state_machine: &sled::transaction::TransactionalTree,
        membership: StoredMembership<RegistryNodeId, BasicNode>,
    ) -> Result<(), sled::transaction::ConflictableTransactionError<AnyError>> {
        let value = serde_json::to_vec(&membership).map_err(ct_err)?;
        tx_state_machine
            .insert(b"last_membership", value)
            .map_err(ct_err)?;
        Ok(())
    }
    fn get_last_applied_log(&self) -> StorageResult<Option<LogId<RegistryNodeId>>> {
        let state_machine = state_machine(&self.db);
        state_machine
            .get(b"last_applied_log")
            .map_err(l_r_err)
            .and_then(|value| {
                value
                    .map(|v| serde_json::from_slice(&v).map_err(sm_r_err))
                    .transpose()
            })
    }
    async fn set_last_applied_log(&self, log_id: LogId<RegistryNodeId>) -> StorageResult<()> {
        let value = serde_json::to_vec(&log_id).map_err(sm_w_err)?;
        let state_machine = state_machine(&self.db);
        state_machine
            .insert(b"last_applied_log", value)
            .map_err(l_r_err)?;

        let flushed = flush_async(&state_machine).await.map_err(l_r_err)?;
        self.metrics.flushed_bytes.inc_by(flushed as u64);
        Ok(())
    }
    fn set_last_applied_log_tx(
        &self,
        tx_state_machine: &sled::transaction::TransactionalTree,
        log_id: LogId<RegistryNodeId>,
    ) -> Result<(), sled::transaction::ConflictableTransactionError<AnyError>> {
        let value = serde_json::to_vec(&log_id).map_err(ct_err)?;
        tx_state_machine
            .insert(b"last_applied_log", value)
            .map_err(ct_err)?;
        Ok(())
    }
    async fn from_serializable(
        id: RegistryNodeId,
        sm: SerializableRegistryStateMachine,
        db: Arc<sled::Db>,
        metrics: StorageMetrics,
    ) -> StorageResult<Self> {
        let mut pblob = HashSet::new();
        let blob_tree = blobs(&db);
        let mut batch = sled::Batch::default();
        for (key, value) in sm.blobs {
            batch.insert(
                options().with_big_endian().serialize(&key).unwrap(),
                options().with_big_endian().serialize(&value).unwrap(),
            );
            if !value.locations.contains(&id) && !value.locations.is_empty() {
                pblob.insert(key);
            }
        }
        blob_tree.apply_batch(batch).map_err(sm_w_err)?;
        let flushed = flush_async(&blob_tree).await.map_err(s_w_err)?;
        metrics.flushed_bytes.inc_by(flushed as u64);

        let mut pmanifest = HashSet::new();
        let manifest_tree = manifests(&db);
        let mut batch = sled::Batch::default();
        for (key, value) in sm.manifests {
            batch.insert(
                options().with_big_endian().serialize(&key).unwrap(),
                options().with_big_endian().serialize(&value).unwrap(),
            );
            if !value.locations.contains(&id) && !value.locations.is_empty() {
                pmanifest.insert(key);
            }
        }
        manifest_tree.apply_batch(batch).map_err(sm_w_err)?;
        let flushed = flush_async(&manifest_tree).await.map_err(s_w_err)?;
        metrics.flushed_bytes.inc_by(flushed as u64);

        let tag_tree = tags(&db);
        let mut batch = sled::Batch::default();
        for (key, value) in sm.tags {
            for (tag, digest) in value {
                let real_key = TagKey {
                    repository: key.clone(),
                    tag,
                };
                batch.insert(
                    options().with_big_endian().serialize(&real_key).unwrap(),
                    options().with_big_endian().serialize(&digest).unwrap(),
                )
            }
        }
        tag_tree.apply_batch(batch).map_err(sm_w_err)?;
        let flushed = flush_async(&tag_tree).await.map_err(s_w_err)?;
        metrics.flushed_bytes.inc_by(flushed as u64);

        let (pending_blobs, _) = channel(pblob);
        let (pending_manifests, _) = channel(pmanifest);

        let r = Self {
            db,
            metrics,
            pending_blobs: Arc::new(pending_blobs),
            pending_manifests: Arc::new(pending_manifests),
            manifest_waiters: HashMap::new(),
            blob_waiters: HashMap::new(),
        };
        if let Some(log_id) = sm.last_applied_log {
            r.set_last_applied_log(log_id).await?;
        }
        r.set_last_membership(sm.last_membership).await?;

        Ok(r)
    }

    fn new(
        db: Arc<sled::Db>,
        metrics: StorageMetrics,
        blobs: HashSet<Digest>,
        manifests: HashSet<Digest>,
    ) -> RegistryStateMachine {
        let (pending_blobs, _) = channel(blobs);
        let (pending_manifests, _) = channel(manifests);
        Self {
            db,
            metrics,
            pending_blobs: Arc::new(pending_blobs),
            pending_manifests: Arc::new(pending_manifests),
            manifest_waiters: HashMap::new(),
            blob_waiters: HashMap::new(),
        }
    }
    fn tx_get_blob(
        &self,
        blobs: &TransactionalTree,
        digest: &Digest,
    ) -> StorageResult<Option<Blob>> {
        let key = options().with_big_endian().serialize(digest).unwrap();
        blobs
            .get(key)
            .map(|value| {
                value.map(|value| {
                    options()
                        .with_big_endian()
                        .deserialize(&value)
                        .expect("invalid data")
                })
            })
            .map_err(|e| {
                StorageIOError::new(ErrorSubject::Store, ErrorVerb::Read, AnyError::new(&e)).into()
            })
    }

    fn tx_put_blob(
        &self,
        blobs: &TransactionalTree,
        digest: &Digest,
        blob: &Blob,
    ) -> StorageResult<()> {
        let key = options().with_big_endian().serialize(digest).unwrap();
        blobs
            .insert(
                key,
                options()
                    .with_big_endian()
                    .serialize(blob)
                    .expect("invalid data"),
            )
            .map(|_value| ())
            .map_err(|e| {
                StorageIOError::new(ErrorSubject::Store, ErrorVerb::Write, AnyError::new(&e)).into()
            })
    }

    fn tx_del_blob(&self, blobs: &TransactionalTree, digest: &Digest) -> StorageResult<()> {
        let key = options().with_big_endian().serialize(digest).unwrap();
        blobs.remove(key).map(|_value| ()).map_err(|e| {
            StorageIOError::new(ErrorSubject::Store, ErrorVerb::Write, AnyError::new(&e)).into()
        })
    }

    fn tx_get_manifest(
        &self,
        manifests: &TransactionalTree,
        digest: &Digest,
    ) -> StorageResult<Option<Manifest>> {
        let key = options().with_big_endian().serialize(digest).unwrap();
        manifests
            .get(key)
            .map(|value| {
                value.map(|value| {
                    options()
                        .with_big_endian()
                        .deserialize(&value)
                        .expect("invalid data")
                })
            })
            .map_err(|e| {
                StorageIOError::new(ErrorSubject::Store, ErrorVerb::Read, AnyError::new(&e)).into()
            })
    }

    fn tx_put_manifest(
        &self,
        manifests: &TransactionalTree,
        digest: &Digest,
        blob: &Manifest,
    ) -> StorageResult<()> {
        let key = options().with_big_endian().serialize(digest).unwrap();
        manifests
            .insert(
                key,
                options()
                    .with_big_endian()
                    .serialize(blob)
                    .expect("invalid data"),
            )
            .map(|_value| ())
            .map_err(|e| {
                StorageIOError::new(ErrorSubject::Store, ErrorVerb::Write, AnyError::new(&e)).into()
            })
    }

    fn tx_del_manifest(&self, manifests: &TransactionalTree, digest: &Digest) -> StorageResult<()> {
        let key = options().with_big_endian().serialize(digest).unwrap();
        manifests.remove(key).map(|_value| ()).map_err(|e| {
            StorageIOError::new(ErrorSubject::Store, ErrorVerb::Write, AnyError::new(&e)).into()
        })
    }

    fn tx_put_tag(
        &self,
        tags: &TransactionalTree,
        repository: &RepositoryName,
        tag: &str,
        digest: &Digest,
    ) -> StorageResult<()> {
        let opts = options().with_big_endian();
        let key = opts
            .serialize(&TagKey {
                repository: repository.clone(),
                tag: tag.to_owned(),
            })
            .unwrap();
        tags.insert(
            key,
            options()
                .with_big_endian()
                .serialize(digest)
                .expect("invalid data"),
        )
        .map(|_value| ())
        .map_err(|e| {
            StorageIOError::new(ErrorSubject::Store, ErrorVerb::Write, AnyError::new(&e)).into()
        })
    }
}

#[derive(Debug)]
pub struct RegistryStore {
    db: Arc<sled::Db>,
    id: RegistryNodeId,

    /// The Raft state machine.
    pub state_machine: RwLock<RegistryStateMachine>,

    // Metrics
    pub metrics: StorageMetrics,
}

type StorageResult<T> = Result<T, StorageError<RegistryNodeId>>;

/// converts an id to a byte vector for storing in the database.
/// Note that we're using big endian encoding to ensure correct sorting of keys
/// with notes form: https://github.com/spacejam/sled#a-note-on-lexicographic-ordering-and-endianness
fn id_to_bin(id: u64) -> [u8; 8] {
    let mut buf: [u8; 8] = [0; 8];
    BigEndian::write_u64(&mut buf, id);
    buf
}

fn bin_to_id(buf: &[u8]) -> u64 {
    (&buf[0..8]).read_u64::<BigEndian>().unwrap()
}

async fn flush_async(tree: &Tree) -> sled::Result<usize> {
    // Tests were hitting
    // https://github.com/spacejam/sled/issues/1357
    // https://github.com/spacejam/sled/issues/1308

    // We need
    // https://github.com/spacejam/sled/commit/61803984dc1cd8c35a3d537c2a7f5538fa659fac

    let tree = tree.clone();
    tokio::task::spawn_blocking(move || tree.flush())
        .await
        .unwrap()
}

impl RegistryStore {
    async fn flush_async(&self) -> sled::Result<usize> {
        // Tests were hitting
        // https://github.com/spacejam/sled/issues/1357
        // https://github.com/spacejam/sled/issues/1308

        // We need
        // https://github.com/spacejam/sled/commit/61803984dc1cd8c35a3d537c2a7f5538fa659fac

        let db = self.db.clone();
        tokio::task::spawn_blocking(move || db.flush())
            .await
            .unwrap()
    }

    fn get_last_purged_(&self) -> StorageResult<Option<LogId<u64>>> {
        let store_tree = store(&self.db);
        let val = store_tree
            .get(b"last_purged_log_id")
            .map_err(|e| {
                StorageIOError::new(ErrorSubject::Store, ErrorVerb::Read, AnyError::new(&e))
            })?
            .and_then(|v| serde_json::from_slice(&v).ok());

        Ok(val)
    }

    async fn set_last_purged_(&self, log_id: LogId<u64>) -> StorageResult<()> {
        let store_tree = store(&self.db);
        let val = serde_json::to_vec(&log_id).unwrap();
        store_tree
            .insert(b"last_purged_log_id", val.as_slice())
            .map_err(s_w_err)?;

        let flushed = flush_async(&store_tree).await.map_err(s_w_err)?;
        self.metrics.flushed_bytes.inc_by(flushed as u64);
        Ok(())
    }

    fn get_snapshot_index_(&self) -> StorageResult<u64> {
        let store_tree = store(&self.db);
        let val = store_tree
            .get(b"snapshot_index")
            .map_err(s_r_err)?
            .and_then(|v| serde_json::from_slice(&v).ok())
            .unwrap_or(0);

        Ok(val)
    }

    async fn set_snapshot_index_(&self, snapshot_index: u64) -> StorageResult<()> {
        let store_tree = store(&self.db);
        let val = serde_json::to_vec(&snapshot_index).unwrap();
        store_tree
            .insert(b"snapshot_index", val.as_slice())
            .map_err(s_w_err)?;

        let flushed = flush_async(&store_tree).await.map_err(s_w_err)?;
        self.metrics.flushed_bytes.inc_by(flushed as u64);
        Ok(())
    }

    async fn set_vote_(&self, vote: &Vote<RegistryNodeId>) -> StorageResult<()> {
        let store_tree = store(&self.db);
        let val = serde_json::to_vec(vote).unwrap();
        store_tree
            .insert(b"vote", val)
            .map_err(v_w_err)
            .map(|_| ())?;

        let flushed = flush_async(&store_tree).await.map_err(s_w_err)?;
        self.metrics.flushed_bytes.inc_by(flushed as u64);
        Ok(())
    }

    fn get_vote_(&self) -> StorageResult<Option<Vote<RegistryNodeId>>> {
        let store_tree = store(&self.db);
        let val = store_tree
            .get(b"vote")
            .map_err(v_r_err)?
            .and_then(|v| serde_json::from_slice(&v).ok());

        Ok(val)
    }

    fn get_current_snapshot_(&self) -> StorageResult<Option<RegistrySnapshot>> {
        let store_tree = store(&self.db);
        let val = store_tree
            .get(b"snapshot")
            .map_err(s_r_err)?
            .and_then(|v| serde_json::from_slice(&v).ok());

        Ok(val)
    }

    async fn set_current_snapshot_(&self, snap: RegistrySnapshot) -> StorageResult<()> {
        let store_tree = store(&self.db);
        let val = serde_json::to_vec(&snap).unwrap();
        let meta = snap.meta.clone();
        store_tree
            .insert(b"snapshot", val.as_slice())
            .map_err(|e| StorageError::IO {
                source: StorageIOError::new(
                    ErrorSubject::Snapshot(snap.meta.signature()),
                    ErrorVerb::Write,
                    AnyError::new(&e),
                ),
            })?;

        let flushed = flush_async(&store_tree).await.map_err(|e| {
            StorageIOError::new(
                ErrorSubject::Snapshot(meta.signature()),
                ErrorVerb::Write,
                AnyError::new(&e),
            )
        })?;
        self.metrics.flushed_bytes.inc_by(flushed as u64);
        Ok(())
    }
}

#[async_trait]
impl RaftLogReader<RegistryTypeConfig> for Arc<RegistryStore> {
    async fn get_log_state(&mut self) -> StorageResult<LogState<RegistryTypeConfig>> {
        let last_purged_log_id = self.get_last_purged_()?;

        let logs_tree = logs(&self.db);
        let last_res = logs_tree.last();
        if last_res.is_err() {
            return Ok(LogState {
                last_purged_log_id,
                last_log_id: last_purged_log_id,
            });
        }

        let last = last_res.unwrap().and_then(|(_, ent)| {
            Some(
                serde_json::from_slice::<Entry<RegistryTypeConfig>>(&ent)
                    .ok()?
                    .log_id,
            )
        });

        let last_log_id = match last {
            None => last_purged_log_id,
            Some(x) => Some(x),
        };
        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send + Sync>(
        &mut self,
        range: RB,
    ) -> StorageResult<Vec<Entry<RegistryTypeConfig>>> {
        let start_bound = range.start_bound();
        let start = match start_bound {
            std::ops::Bound::Included(x) => id_to_bin(*x),
            std::ops::Bound::Excluded(x) => id_to_bin(*x + 1),
            std::ops::Bound::Unbounded => id_to_bin(0),
        };
        let logs_tree = logs(&self.db);
        let logs = logs_tree
            .range::<&[u8], _>(start.as_slice()..)
            .map(|el_res| {
                let el = el_res.expect("Failed read log entry");
                let id = el.0;
                let val = el.1;
                let entry: StorageResult<Entry<_>> =
                    serde_json::from_slice(&val).map_err(|e| StorageError::IO {
                        source: StorageIOError::new(
                            ErrorSubject::Logs,
                            ErrorVerb::Read,
                            AnyError::new(&e),
                        ),
                    });
                let id = bin_to_id(&id);

                assert_eq!(Ok(id), entry.as_ref().map(|e| e.log_id.index));
                (id, entry)
            })
            .take_while(|(id, _)| range.contains(id))
            .map(|x| x.1)
            .collect();
        logs
    }
}

#[async_trait]
impl RaftSnapshotBuilder<RegistryTypeConfig, Cursor<Vec<u8>>> for Arc<RegistryStore> {
    #[tracing::instrument(level = "trace", skip(self))]
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<RegistryNodeId, BasicNode, Cursor<Vec<u8>>>, StorageError<RegistryNodeId>>
    {
        let data;
        let last_applied_log;
        let last_membership;

        {
            // Serialize the data of the state machine.
            let state_machine =
                SerializableRegistryStateMachine::from(&*self.state_machine.read().unwrap());
            data = serde_json::to_vec(&state_machine).map_err(|e| {
                StorageIOError::new(
                    ErrorSubject::StateMachine,
                    ErrorVerb::Read,
                    AnyError::new(&e),
                )
            })?;

            last_applied_log = state_machine.last_applied_log;
            last_membership = state_machine.last_membership;
        }

        // TODO: we probably want this to be atomic.
        let snapshot_idx: u64 = self.get_snapshot_index_()? + 1;
        self.set_snapshot_index_(snapshot_idx).await?;

        let snapshot_id = if let Some(last) = last_applied_log {
            format!("{}-{}-{}", last.leader_id, last.index, snapshot_idx)
        } else {
            format!("--{}", snapshot_idx)
        };

        let meta = SnapshotMeta {
            last_log_id: last_applied_log,
            last_membership,
            snapshot_id,
        };

        let snapshot = RegistrySnapshot {
            meta: meta.clone(),
            data: data.clone(),
        };

        self.set_current_snapshot_(snapshot).await?;

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

#[async_trait]
impl RaftStorage<RegistryTypeConfig> for Arc<RegistryStore> {
    type SnapshotData = Cursor<Vec<u8>>;
    type LogReader = Self;
    type SnapshotBuilder = Self;

    #[tracing::instrument(level = "trace", skip(self))]
    async fn save_vote(
        &mut self,
        vote: &Vote<RegistryNodeId>,
    ) -> Result<(), StorageError<RegistryNodeId>> {
        self.set_vote_(vote).await
    }

    async fn read_vote(
        &mut self,
    ) -> Result<Option<Vote<RegistryNodeId>>, StorageError<RegistryNodeId>> {
        self.get_vote_()
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn append_to_log(&mut self, entries: &[&Entry<RegistryTypeConfig>]) -> StorageResult<()> {
        let logs_tree = logs(&self.db);
        let mut batch = sled::Batch::default();
        for entry in entries {
            let id = id_to_bin(entry.log_id.index);
            assert_eq!(bin_to_id(&id), entry.log_id.index);
            let value = serde_json::to_vec(entry).map_err(l_w_err)?;
            batch.insert(id.as_slice(), value);
        }
        logs_tree.apply_batch(batch).map_err(l_w_err)?;

        let flushed = flush_async(&logs_tree).await.map_err(l_w_err)?;
        self.metrics.flushed_bytes.inc_by(flushed as u64);
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<RegistryNodeId>,
    ) -> StorageResult<()> {
        tracing::debug!("delete_log: [{:?}, +oo)", log_id);

        let from = id_to_bin(log_id.index);
        let to = id_to_bin(0xff_ff_ff_ff_ff_ff_ff_ff);
        let logs_tree = logs(&self.db);
        let entries = logs_tree.range::<&[u8], _>(from.as_slice()..to.as_slice());
        let mut batch_del = sled::Batch::default();
        for entry_res in entries {
            let entry = entry_res.expect("Read db entry failed");
            batch_del.remove(entry.0);
        }
        logs_tree.apply_batch(batch_del).map_err(l_w_err)?;
        let flushed = flush_async(&logs_tree).await.map_err(l_w_err)?;
        self.metrics.flushed_bytes.inc_by(flushed as u64);
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn purge_logs_upto(
        &mut self,
        log_id: LogId<RegistryNodeId>,
    ) -> Result<(), StorageError<RegistryNodeId>> {
        tracing::debug!("delete_log: [0, {:?}]", log_id);

        self.set_last_purged_(log_id).await?;
        let from = id_to_bin(0);
        let to = id_to_bin(log_id.index);
        let logs_tree = logs(&self.db);
        let entries = logs_tree.range::<&[u8], _>(from.as_slice()..=to.as_slice());
        let mut batch_del = sled::Batch::default();
        for entry_res in entries {
            let entry = entry_res.expect("Read db entry failed");
            batch_del.remove(entry.0);
        }
        logs_tree.apply_batch(batch_del).map_err(l_w_err)?;

        let flushed = flush_async(&logs_tree).await.map_err(l_w_err)?;
        self.metrics.flushed_bytes.inc_by(flushed as u64);
        Ok(())
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<RegistryNodeId>>,
            StoredMembership<RegistryNodeId, BasicNode>,
        ),
        StorageError<RegistryNodeId>,
    > {
        let state_machine = self.state_machine.read().unwrap();
        Ok((
            state_machine.get_last_applied_log()?,
            state_machine.get_last_membership()?,
        ))
    }

    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn apply_to_state_machine(
        &mut self,
        entries: &[&Entry<RegistryTypeConfig>],
    ) -> Result<Vec<RegistryResponse>, StorageError<RegistryNodeId>> {
        let state_machine = state_machine(&self.db);
        let blob_tree = blobs(&self.db);
        let manifest_tree = manifests(&self.db);
        let tag_tree = tags(&self.db);

        let trans_res = (&state_machine, &blob_tree, &manifest_tree, &tag_tree).transaction(
            |(tx_state_machine, tx_blob_tree, tx_manifest_tree, tx_tag_tree)| {
                let sm = self.state_machine.write().unwrap();

                let mut res = Vec::with_capacity(entries.len());

                for entry in entries {
                    tracing::debug!(%entry.log_id, "replicate to sm");

                    sm.set_last_applied_log_tx(tx_state_machine, entry.log_id)?;

                    match entry.payload {
                        EntryPayload::Blank => res.push(RegistryResponse {
                            value: entry.log_id.index,
                        }),
                        EntryPayload::Normal(ref req) => match req {
                            RegistryRequest::Transaction { actions } => {
                                for action in actions {
                                    match action {
                                        RegistryAction::Empty => {}
                                        RegistryAction::BlobStored {
                                            timestamp,
                                            digest,
                                            location,
                                            user: _,
                                        } => {
                                            let mut blob = sm
                                                .tx_get_blob(tx_blob_tree, digest)
                                                .unwrap()
                                                .unwrap();
                                            blob.updated = *timestamp;
                                            blob.locations.insert(*location);
                                            sm.tx_put_blob(tx_blob_tree, digest, &blob).unwrap();
                                        }
                                        RegistryAction::BlobUnstored {
                                            timestamp,
                                            digest,
                                            location,
                                            user: _,
                                        } => {
                                            if let Some(mut blob) =
                                                sm.tx_get_blob(tx_blob_tree, digest).unwrap()
                                            {
                                                blob.updated = *timestamp;
                                                blob.locations.remove(location);
                                                if blob.locations.is_empty() {
                                                    sm.tx_del_blob(tx_blob_tree, digest).unwrap();
                                                } else {
                                                    sm.tx_put_blob(tx_blob_tree, digest, &blob)
                                                        .unwrap();
                                                }
                                            }
                                        }
                                        RegistryAction::BlobMounted {
                                            timestamp,
                                            digest,
                                            repository,
                                            user: _,
                                        } => {
                                            let mut blob =
                                                match sm.tx_get_blob(tx_blob_tree, digest).unwrap()
                                                {
                                                    Some(blob) => blob,
                                                    None => Blob {
                                                        created: *timestamp,
                                                        updated: *timestamp,
                                                        content_type: None,
                                                        size: None,
                                                        dependencies: Some(vec![]),
                                                        locations: HashSet::new(),
                                                        repositories: HashSet::new(),
                                                    },
                                                };
                                            blob.updated = *timestamp;
                                            blob.repositories.insert(repository.clone());
                                            sm.tx_put_blob(tx_blob_tree, digest, &blob).unwrap();
                                        }
                                        RegistryAction::BlobUnmounted {
                                            timestamp,
                                            digest,
                                            repository,
                                            user: _,
                                        } => {
                                            if let Some(mut blob) =
                                                sm.tx_get_blob(tx_blob_tree, digest).unwrap()
                                            {
                                                blob.updated = *timestamp;
                                                blob.repositories.remove(repository);
                                                sm.tx_put_blob(tx_blob_tree, digest, &blob)
                                                    .unwrap();
                                            }
                                        }
                                        RegistryAction::BlobInfo {
                                            timestamp,
                                            digest,
                                            dependencies,
                                            content_type,
                                        } => {
                                            if let Some(mut blob) =
                                                sm.tx_get_blob(tx_blob_tree, digest).unwrap()
                                            {
                                                blob.updated = *timestamp;
                                                blob.dependencies = Some(dependencies.clone());
                                                blob.content_type = Some(content_type.clone());
                                                sm.tx_put_blob(tx_blob_tree, digest, &blob)
                                                    .unwrap();
                                            }
                                        }
                                        RegistryAction::BlobStat {
                                            timestamp,
                                            digest,
                                            size,
                                        } => {
                                            if let Some(mut blob) =
                                                sm.tx_get_blob(tx_blob_tree, digest).unwrap()
                                            {
                                                blob.updated = *timestamp;
                                                blob.size = Some(*size);
                                                sm.tx_put_blob(tx_blob_tree, digest, &blob)
                                                    .unwrap();
                                            }
                                        }
                                        RegistryAction::ManifestStored {
                                            timestamp,
                                            digest,
                                            location,
                                            user: _,
                                        } => {
                                            let mut manifest = sm
                                                .tx_get_manifest(tx_manifest_tree, digest)
                                                .unwrap()
                                                .unwrap();
                                            manifest.updated = *timestamp;
                                            manifest.locations.insert(*location);
                                            sm.tx_put_manifest(tx_manifest_tree, digest, &manifest)
                                                .unwrap();
                                        }
                                        RegistryAction::ManifestUnstored {
                                            timestamp,
                                            digest,
                                            location,
                                            user: _,
                                        } => {
                                            if let Some(mut manifest) = sm
                                                .tx_get_manifest(tx_manifest_tree, digest)
                                                .unwrap()
                                            {
                                                manifest.updated = *timestamp;
                                                manifest.locations.remove(location);
                                                if manifest.locations.is_empty() {
                                                    sm.tx_del_manifest(tx_manifest_tree, digest)
                                                        .unwrap();
                                                } else {
                                                    sm.tx_put_manifest(
                                                        tx_manifest_tree,
                                                        digest,
                                                        &manifest,
                                                    )
                                                    .unwrap();
                                                }
                                            }
                                        }
                                        RegistryAction::ManifestMounted {
                                            timestamp,
                                            digest,
                                            repository,
                                            user: _,
                                        } => {
                                            let mut manifest = match sm
                                                .tx_get_manifest(tx_manifest_tree, digest)
                                                .unwrap()
                                            {
                                                Some(manifest) => manifest,
                                                None => Manifest {
                                                    created: *timestamp,
                                                    updated: *timestamp,
                                                    content_type: None,
                                                    size: None,
                                                    dependencies: Some(vec![]),
                                                    locations: HashSet::new(),
                                                    repositories: HashSet::new(),
                                                },
                                            };

                                            manifest.updated = *timestamp;
                                            manifest.repositories.insert(repository.clone());
                                            sm.tx_put_manifest(tx_manifest_tree, digest, &manifest)
                                                .unwrap();
                                        }
                                        RegistryAction::ManifestUnmounted {
                                            timestamp,
                                            digest,
                                            repository,
                                            user: _,
                                        } => {
                                            if let Some(mut manifest) = sm
                                                .tx_get_manifest(tx_manifest_tree, digest)
                                                .unwrap()
                                            {
                                                manifest.updated = *timestamp;
                                                manifest.repositories.remove(repository);
                                                sm.tx_put_manifest(
                                                    tx_manifest_tree,
                                                    digest,
                                                    &manifest,
                                                )
                                                .unwrap();
                                            }
                                        }
                                        RegistryAction::ManifestInfo {
                                            timestamp,
                                            digest,
                                            dependencies,
                                            content_type,
                                        } => {
                                            if let Some(mut manifest) = sm
                                                .tx_get_manifest(tx_manifest_tree, digest)
                                                .unwrap()
                                            {
                                                manifest.updated = *timestamp;
                                                manifest.dependencies = Some(dependencies.clone());
                                                manifest.content_type = Some(content_type.clone());
                                                sm.tx_put_manifest(
                                                    tx_manifest_tree,
                                                    digest,
                                                    &manifest,
                                                )
                                                .unwrap();
                                            }
                                        }
                                        RegistryAction::ManifestStat {
                                            timestamp,
                                            digest,
                                            size,
                                        } => {
                                            if let Some(mut manifest) = sm
                                                .tx_get_manifest(tx_manifest_tree, digest)
                                                .unwrap()
                                            {
                                                manifest.updated = *timestamp;
                                                manifest.size = Some(*size);
                                                sm.tx_put_manifest(
                                                    tx_manifest_tree,
                                                    digest,
                                                    &manifest,
                                                )
                                                .unwrap();
                                            }
                                        }
                                        RegistryAction::HashTagged {
                                            timestamp: _,
                                            digest,
                                            repository,
                                            tag,
                                            user: _,
                                        } => {
                                            sm.tx_put_tag(tx_tag_tree, repository, tag, digest)
                                                .unwrap();
                                        }
                                    }
                                }
                                res.push(RegistryResponse {
                                    value: entry.log_id.index,
                                });
                            }
                        },
                        EntryPayload::Membership(ref mem) => {
                            let membership = StoredMembership::new(Some(entry.log_id), mem.clone());
                            sm.set_last_membership_tx(tx_state_machine, membership)?;
                            res.push(RegistryResponse {
                                value: entry.log_id.index,
                            })
                        }
                    };
                }
                Ok(res)
            },
        );
        let result_vec = trans_res.map_err(t_err)?;

        let flushed = self.flush_async().await.map_err(|e| {
            StorageIOError::new(ErrorSubject::Logs, ErrorVerb::Write, AnyError::new(&e))
        })?;
        self.metrics.flushed_bytes.inc_by(flushed as u64);

        let mut sm = self.state_machine.write().unwrap();

        let mut pending_blobs = sm.pending_blobs.borrow().clone();
        let mut pending_manifests = sm.pending_manifests.borrow().clone();

        for entry in entries {
            if let EntryPayload::Normal(RegistryRequest::Transaction { ref actions }) =
                entry.payload
            {
                for action in actions {
                    match action {
                        RegistryAction::BlobStored {
                            location, digest, ..
                        } => {
                            if let Some(blob) = self.get_blob(digest).unwrap() {
                                if !blob.locations.contains(&self.id) && !blob.locations.is_empty()
                                {
                                    pending_blobs.insert(digest.clone());
                                } else {
                                    pending_blobs.remove(digest);
                                }
                            }
                            if location == &self.id {
                                if let Some(senders) = sm.blob_waiters.remove(digest) {
                                    for sender in senders {
                                        sender.send(()).unwrap();
                                    }
                                }
                            }
                        }
                        RegistryAction::BlobUnstored { digest, .. } => {
                            if let Some(blob) = self.get_blob(digest).unwrap() {
                                if !blob.locations.contains(&self.id) && !blob.locations.is_empty()
                                {
                                    pending_blobs.insert(digest.clone());
                                } else {
                                    pending_blobs.remove(digest);
                                }
                            }
                        }
                        RegistryAction::ManifestStored {
                            location, digest, ..
                        } => {
                            if let Some(manifest) = self.get_manifest(digest).unwrap() {
                                if !manifest.locations.contains(&self.id)
                                    && !manifest.locations.is_empty()
                                {
                                    pending_manifests.insert(digest.clone());
                                } else {
                                    pending_manifests.remove(digest);
                                }
                            }
                            if location == &self.id {
                                if let Some(senders) = sm.manifest_waiters.remove(digest) {
                                    for sender in senders {
                                        sender.send(()).unwrap();
                                    }
                                }
                            }
                        }
                        RegistryAction::ManifestUnstored { digest, .. } => {
                            if let Some(manifest) = self.get_manifest(digest).unwrap() {
                                if !manifest.locations.contains(&self.id)
                                    && !manifest.locations.is_empty()
                                {
                                    pending_manifests.insert(digest.clone());
                                } else {
                                    pending_manifests.remove(digest);
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
        }

        sm.pending_blobs.send_replace(pending_blobs);
        sm.pending_manifests.send_replace(pending_manifests);

        Ok(result_vec)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Self::SnapshotData>, StorageError<RegistryNodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    #[tracing::instrument(level = "trace", skip(self, snapshot))]
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<RegistryNodeId, BasicNode>,
        snapshot: Box<Self::SnapshotData>,
    ) -> Result<(), StorageError<RegistryNodeId>> {
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
            let updated_state_machine: SerializableRegistryStateMachine =
                serde_json::from_slice(&new_snapshot.data).map_err(|e| {
                    StorageIOError::new(
                        ErrorSubject::Snapshot(new_snapshot.meta.signature()),
                        ErrorVerb::Read,
                        AnyError::new(&e),
                    )
                })?;
            let new_sm = RegistryStateMachine::from_serializable(
                self.id,
                updated_state_machine,
                self.db.clone(),
                self.metrics.clone(),
            )
            .await?;
            let mut state_machine = self.state_machine.write().unwrap();
            *state_machine = new_sm;
        }

        self.set_current_snapshot_(new_snapshot).await?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_current_snapshot(
        &mut self,
    ) -> Result<
        Option<Snapshot<RegistryNodeId, BasicNode, Self::SnapshotData>>,
        StorageError<RegistryNodeId>,
    > {
        match RegistryStore::get_current_snapshot_(self)? {
            Some(snapshot) => {
                let data = snapshot.data.clone();
                Ok(Some(Snapshot {
                    meta: snapshot.meta,
                    snapshot: Box::new(Cursor::new(data)),
                }))
            }
            None => Ok(None),
        }
    }
}
pub fn get_blobs(tree: &Tree) -> StorageResult<BTreeMap<Digest, Blob>> {
    let opts = options().with_big_endian();
    let mut blobs = BTreeMap::new();
    for row in tree.iter() {
        if let Ok((key, value)) = row {
            let key = opts.deserialize::<Digest>(&key).unwrap();
            let value = opts.deserialize::<Blob>(&value).unwrap();
            blobs.insert(key, value);
            continue;
        }
        break;
    }

    Ok(blobs)
}
pub fn get_manifests(tree: &Tree) -> StorageResult<BTreeMap<Digest, Manifest>> {
    let opts = options().with_big_endian();
    let mut manifests = BTreeMap::new();
    for row in tree.iter() {
        if let Ok((key, value)) = row {
            let key = opts.deserialize::<Digest>(&key).unwrap();
            let value = opts.deserialize::<Manifest>(&value).unwrap();
            manifests.insert(key, value);
            continue;
        }
        break;
    }

    Ok(manifests)
}
impl RegistryStore {
    pub async fn new(
        db: Arc<sled::Db>,
        id: RegistryNodeId,
        registry: &mut Registry,
    ) -> Arc<RegistryStore> {
        let _store = store(&db);
        let _state_machine = state_machine(&db);
        let blobs = blobs(&db);
        let manifests = manifests(&db);
        let _tags = tags(&db);
        let _logs = logs(&db);

        let pblobs = get_blobs(&blobs)
            .unwrap()
            .iter()
            .filter(|(_handle, blob)| !blob.locations.contains(&id) && !blob.locations.is_empty())
            .map(|(digest, _)| digest.clone())
            .collect();

        let pmans = get_manifests(&manifests)
            .unwrap()
            .iter()
            .filter(|(_, manifest)| {
                !manifest.locations.contains(&id) && !manifest.locations.is_empty()
            })
            .map(|(digest, _)| digest.clone())
            .collect();

        let metrics = StorageMetrics::new(registry);

        let state_machine = RwLock::new(RegistryStateMachine::new(
            db.clone(),
            metrics.clone(),
            pblobs,
            pmans,
        ));

        Arc::new(RegistryStore {
            db,
            id,
            state_machine,
            metrics,
        })
    }

    pub fn get_blob(&self, key: &Digest) -> StorageResult<Option<Blob>> {
        let key = options().with_big_endian().serialize(key).unwrap();
        let blob_tree = blobs(&self.db);
        blob_tree
            .get(key)
            .map(|value| {
                value.map(|value| {
                    options()
                        .with_big_endian()
                        .deserialize(&value)
                        .expect("invalid data")
                })
            })
            .map_err(|e| {
                StorageIOError::new(ErrorSubject::Store, ErrorVerb::Read, AnyError::new(&e)).into()
            })
    }
    pub fn get_blobs(&self) -> StorageResult<BTreeMap<Digest, Blob>> {
        get_blobs(&blobs(&self.db))
    }
    pub fn get_manifest(&self, key: &Digest) -> StorageResult<Option<Manifest>> {
        let key = options().with_big_endian().serialize(key).unwrap();
        let manifest_tree = manifests(&self.db);
        manifest_tree
            .get(key)
            .map(|value| {
                value.map(|value| {
                    options()
                        .with_big_endian()
                        .deserialize(&value)
                        .expect("invalid data")
                })
            })
            .map_err(|e| {
                StorageIOError::new(ErrorSubject::Store, ErrorVerb::Read, AnyError::new(&e)).into()
            })
    }
    pub fn get_manifests(&self) -> StorageResult<BTreeMap<Digest, Manifest>> {
        get_manifests(&manifests(&self.db))
    }
    pub fn get_tag(&self, repository: &RepositoryName, tag: &str) -> StorageResult<Option<Digest>> {
        let key = options()
            .with_big_endian()
            .serialize(&TagKey {
                repository: repository.clone(),
                tag: tag.to_owned(),
            })
            .unwrap();
        let tag_tree = tags(&self.db);
        tag_tree
            .get(key)
            .map(|value| {
                value.map(|value| {
                    options()
                        .with_big_endian()
                        .deserialize(&value)
                        .expect("invalid data")
                })
            })
            .map_err(|e| {
                StorageIOError::new(ErrorSubject::Store, ErrorVerb::Read, AnyError::new(&e)).into()
            })
    }
    pub fn get_tags(&self, repository: &RepositoryName) -> StorageResult<Option<Vec<String>>> {
        let opts = options().with_big_endian();

        let key = opts
            .serialize(&TagKey {
                repository: repository.clone(),
                tag: "".to_string(),
            })
            .unwrap();

        let tag_tree = tags(&self.db);
        let r = tag_tree.range(key..);

        let mut digests = vec![];

        for row in r {
            if let Ok((key, _value)) = row {
                let key = opts.deserialize::<TagKey>(&key).unwrap();
                if &key.repository != repository {
                    break;
                }
                digests.push(key.tag.clone());
                continue;
            }
            break;
        }

        Ok(Some(digests))
    }

    pub fn get_all_tags(&self) -> StorageResult<BTreeMap<TagKey, Digest>> {
        let opts = options().with_big_endian();

        let tag_tree = tags(&self.db);
        let r = tag_tree.iter();

        let mut results = BTreeMap::new();

        for row in r {
            if let Ok((key, value)) = row {
                let key = opts.deserialize::<TagKey>(&key).unwrap();
                let value = opts.deserialize(&value).unwrap();
                results.insert(key, value);
                continue;
            }
            break;
        }

        Ok(results)
    }

    pub fn get_orphaned_blobs(&self) -> StorageResult<BTreeMap<Digest, Blob>> {
        let mut blobs = self.get_blobs()?;
        let mut visited: HashSet<Digest> = HashSet::new();
        let mut visiting: HashSet<Digest> = HashSet::new();

        for manifest in self.get_manifests()?.values() {
            if let Some(dependencies) = &manifest.dependencies {
                visiting.extend(dependencies.iter().cloned());
            }
        }

        while let Some(digest) = visiting.iter().next().cloned() {
            match blobs.get(&digest) {
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
                    tracing::debug!("Dangling dependency found: {digest} missing");
                }
            }

            visiting.remove(&digest);
            visited.insert(digest);
        }

        blobs.retain(|k, _| !visited.contains(k));

        Ok(blobs)
    }

    pub fn get_orphaned_manifests(&self) -> StorageResult<BTreeMap<Digest, Manifest>> {
        let mut manifests = self.get_manifests()?;
        let tags: HashSet<Digest> = self.get_all_tags()?.values().cloned().collect();
        manifests.retain(|k, _| !tags.contains(k));
        Ok(manifests)
    }
}

fn store(db: &sled::Db) -> sled::Tree {
    db.open_tree("store").expect("store open failed")
}
fn logs(db: &sled::Db) -> sled::Tree {
    db.open_tree("logs").expect("logs open failed")
}
fn blobs(db: &sled::Db) -> sled::Tree {
    db.open_tree("blobs").expect("blobs open failed")
}
fn manifests(db: &sled::Db) -> sled::Tree {
    db.open_tree("manifests").expect("manifests open failed")
}
fn tags(db: &sled::Db) -> sled::Tree {
    db.open_tree("tags").expect("tags open failed")
}
fn state_machine(db: &sled::Db) -> sled::Tree {
    db.open_tree("state_machine")
        .expect("state_machine open failed")
}
