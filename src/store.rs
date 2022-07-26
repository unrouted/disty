use std::path::PathBuf;

use anyhow::{bail, Context};
use bincode::Options;
use log::{error, info, warn};
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Registry;
use raft::util::limit_size;
use raft::{eraftpb::*, RaftState, Storage};
use raft::{Error, Result, StorageError};
use sled::IVec;
use std::io::ErrorKind;
use thiserror::Error;

use crate::config::Configuration;
use crate::state::RegistryState;
use crate::types::RegistryAction;

const KEY_HARD_INDEX: &[u8; 8] = b"hs-index";
const KEY_HARD_TERM: &[u8; 7] = b"hs-term";
const KEY_HARD_VOTE: &[u8; 7] = b"hs-vote";

#[derive(Error, Debug)]
pub enum RegistryStorageError {
    #[error("Gaps in log entries: expected:{expected} actual:{actual}")]
    LogsMissing { expected: u64, actual: u64 },
    #[error("Gaps between snapshot and log entries: snapshot@{snapshot_index}, first_index@{first_index}")]
    LogsTooCompacted {
        snapshot_index: u64,
        first_index: u64,
    },
    #[error("Hard state term lagging log state - may indicate corruption: hs_term@{hs_term}, log_term@{log_term}")]
    HardStateStaleTerm { hs_term: u64, log_term: u64 },
    #[error("Hard state commit points at invalid log index, indicates corruption: last_index@{last_index}, commit@{commit}")]
    HardStateDanglingCommit { commit: u64, last_index: u64 },
}
#[derive(Clone)]
pub struct StorageMetrics {
    hs_index: Gauge,
    hs_term: Gauge,
    applied_index: Gauge,
    first_index: Gauge,
    last_index: Gauge,
    snapshot_index: Gauge,
    snapshot_term: Gauge,
    flushed_bytes: Counter,
}

fn serialize_u64(value: &u64) -> anyhow::Result<IVec> {
    let options = bincode::options().with_fixint_encoding().with_big_endian();
    Ok(options.serialize(value).unwrap().into())
}

fn deserialize_u64(value: &IVec) -> anyhow::Result<u64> {
    let options = bincode::options().with_fixint_encoding().with_big_endian();
    Ok(options.deserialize(value).unwrap())
}

impl StorageMetrics {
    fn new(registry: &mut Registry) -> Self {
        let registry = registry.sub_registry_with_prefix("distribd_storage");

        let hs_index = Gauge::default();
        registry.register(
            "commit_index",
            "The most recently commit",
            Box::new(hs_index.clone()),
        );

        let hs_term = Gauge::default();
        registry.register("term", "The most recent term", Box::new(hs_term.clone()));

        let applied_index = Gauge::default();
        registry.register(
            "applied_index",
            "The latest applied log entry in the journal",
            Box::new(applied_index.clone()),
        );

        let first_index = Gauge::default();
        registry.register(
            "first_index",
            "The first log entry in the journal",
            Box::new(first_index.clone()),
        );

        let last_index = Gauge::default();
        registry.register(
            "last_index",
            "The last log entry in the journal",
            Box::new(last_index.clone()),
        );

        let snapshot_index = Gauge::default();
        registry.register(
            "snapshot_index",
            "The last log entry of the most recent snapshot",
            Box::new(snapshot_index.clone()),
        );

        let snapshot_term = Gauge::default();
        registry.register(
            "snapshot_term",
            "The term of the most recent snapshot",
            Box::new(snapshot_term.clone()),
        );

        let flushed_bytes = Counter::default();
        registry.register(
            "flushed_bytes",
            "Journal data flushed to disk",
            Box::new(flushed_bytes.clone()),
        );

        Self {
            hs_index,
            hs_term,
            applied_index,
            first_index,
            last_index,
            snapshot_index,
            snapshot_term,
            flushed_bytes,
        }
    }
}
#[derive(Clone)]
pub struct RegistryStorage {
    db: sled::Db,
    entries: sled::Tree,
    state: sled::Tree,
    conf_state: ConfState,
    pub snapshot_metadata: SnapshotMetadata,
    pub store: RegistryState,
    pub applied_index: u64,
    storage_path: PathBuf,
    metrics: StorageMetrics,
}

impl RegistryStorage {
    pub async fn new(
        config: &Configuration,
        registry: &mut Registry,
    ) -> anyhow::Result<RegistryStorage> {
        let storage_path = std::path::Path::new(&config.storage);

        std::fs::create_dir_all(storage_path).context("Failed to ensure store location existed")?;

        let path = storage_path.join("raft");
        let db = sled::open(path)?;
        let entries = db.open_tree("entries")?;
        let state = db.open_tree("state")?;

        let voters = config
            .peers
            .iter()
            .enumerate()
            .map(|(idx, _peer)| (idx + 1) as u64)
            .collect();

        let conf_state = ConfState {
            voters,
            ..Default::default()
        };

        let mut applied_index = 0;
        let mut store = RegistryState::default();
        let mut snapshot_metadata = SnapshotMetadata {
            term: 1,
            ..Default::default()
        };

        match tokio::fs::read(&storage_path.join("snapshot.latest")).await {
            Ok(data) => {
                let snapshot = <Snapshot as protobuf::Message>::parse_from_bytes(&data).unwrap();

                Self::validate_snapshot(&snapshot).context("Failed to validate snapshot")?;

                snapshot_metadata = snapshot.get_metadata().clone();
                applied_index = snapshot_metadata.index;
                store = serde_json::from_slice(snapshot.get_data()).unwrap();
            }
            Err(err) => match err.kind() {
                ErrorKind::NotFound => {
                    match tokio::fs::read(&storage_path.join("snapshot.bootstrap")).await {
                        Ok(data) => {
                            store = serde_json::from_slice(&data)
                                .context("Failed to parse bootstrap snapshot")?;
                        }
                        Err(err) => match err.kind() {
                            ErrorKind::NotFound => {}
                            _ => {
                                bail!("Unexpected error: {err:?}");
                            }
                        },
                    };
                }
                _ => {
                    bail!("Unexpected error: {err:?}");
                }
            },
        };

        let metrics = StorageMetrics::new(registry);
        metrics.applied_index.set(applied_index);
        metrics.snapshot_index.set(snapshot_metadata.index);
        metrics.snapshot_term.set(snapshot_metadata.term);

        let store = RegistryStorage {
            db,
            entries,
            state,
            conf_state,
            snapshot_metadata,
            store,
            applied_index,
            storage_path: storage_path.to_path_buf(),
            metrics,
        };

        store.ensure_initialized()?;

        Ok(store)
    }

    #[inline(always)]
    pub fn set_applied_index(&mut self, applied_index: u64) {
        self.applied_index = applied_index;
        self.metrics.applied_index.set(applied_index);
    }

    pub fn ensure_initialized(&self) -> anyhow::Result<()> {
        if self.state.get(KEY_HARD_INDEX).unwrap().is_none() {
            self.set_hardstate(HardState {
                commit: self.snapshot_metadata.index,
                term: self.snapshot_metadata.term,
                ..Default::default()
            })
            .context("Could not initialize hard state")?;
        }

        let expected_first_index = self.snapshot_metadata.index + 1;
        if self.first_index() < expected_first_index {
            warn!("Unexpected log entries: A previous compaction failed and left stale journal records (snapshot@{}, logs start@{})", self.snapshot_metadata.index, self.first_index());

            while self.first_index() < expected_first_index {
                self.entries
                    .pop_min()
                    .context("Failed to compact old log entry")?;
            }
        }

        if expected_first_index < self.first_index() {
            return Err(RegistryStorageError::LogsTooCompacted {
                snapshot_index: self.snapshot_metadata.index,
                first_index: self.first_index(),
            }
            .into());
        }

        // If first_index > self.last_index, then there is a snapshot and NO log entries
        if self.first_index() <= self.last_index() {
            let expected_log_length = self.last_index() - self.first_index() + 1;
            let log_length = self.entries.len() as u64;

            if expected_log_length != log_length {
                return Err(RegistryStorageError::LogsMissing {
                    expected: expected_log_length,
                    actual: log_length,
                }
                .into());
            }
        }

        let hs = self.initial_state().unwrap().hard_state;
        if hs.commit > self.last_index() {
            return Err(RegistryStorageError::HardStateDanglingCommit {
                last_index: self.last_index(),
                commit: hs.commit,
            }
            .into());
        }

        if let Ok(term) = self.term(self.last_index()) {
            if hs.term < term {
                return Err(RegistryStorageError::HardStateStaleTerm {
                    hs_term: hs.term,
                    log_term: term,
                }
                .into());
            }
        }

        Ok(())
    }

    pub fn set_hardstate(&self, hs: HardState) -> anyhow::Result<()> {
        self.state
            .transaction::<_, _, sled::transaction::TransactionError>(|t| {
                t.insert(KEY_HARD_INDEX, serialize_u64(&hs.commit).unwrap())?;
                t.insert(KEY_HARD_TERM, serialize_u64(&hs.term).unwrap())?;
                t.insert(KEY_HARD_VOTE, serialize_u64(&hs.vote).unwrap())?;
                Ok(())
            })
            .context("Transaction failure")?;

        self.metrics.hs_index.set(hs.commit);
        self.metrics.hs_term.set(hs.term);

        Ok(())
    }

    pub fn set_commit(&self, commit: u64) {
        self.state
            .insert(KEY_HARD_INDEX, serialize_u64(&commit).unwrap())
            .unwrap();

        self.metrics.hs_index.set(commit);
    }

    pub fn last_index(&self) -> u64 {
        if let Some((key, _value)) = self.entries.last().unwrap() {
            return deserialize_u64(&key).unwrap();
        }

        self.snapshot_metadata.index
    }

    pub fn log_first_index(&self) -> Option<u64> {
        if let Some((key, _value)) = self.entries.first().unwrap() {
            return Some(deserialize_u64(&key).unwrap());
        }

        None
    }

    pub fn first_index(&self) -> u64 {
        self.log_first_index()
            .unwrap_or(self.snapshot_metadata.index + 1)
    }

    pub fn dispatch_actions(&mut self, actions: &Vec<RegistryAction>) {
        self.store.dispatch_actions(actions);
    }

    fn validate_snapshot(snapshot: &Snapshot) -> anyhow::Result<()> {
        if !snapshot.has_metadata() {
            bail!("Snapshot has no metadata");
        }

        if snapshot.get_metadata().term == 0 {
            bail!("Snapshot has invalid term");
        }

        if !snapshot.get_metadata().has_conf_state() {
            bail!("Snapshot has no conf state");
        }

        Ok(())
    }

    /// Replace the current snapshot on disk with `snapshot`.
    pub async fn persist_snapshot(&self, snapshot: &Snapshot) -> anyhow::Result<()> {
        let incoming_path = self.storage_path.join("snapshot.incoming");
        let snapshot_path = self.storage_path.join("snapshot.latest");

        let bytes = protobuf::Message::write_to_bytes(snapshot).unwrap();

        tokio::fs::write(&incoming_path, bytes)
            .await
            .context(format!("Failure writing snapshot to {incoming_path:?}"))?;

        tokio::fs::rename(&incoming_path, &snapshot_path)
            .await
            .context(format!(
                "Failure renaming {incoming_path:?} to {snapshot_path:?}"
            ))?;

        Ok(())
    }

    pub async fn store_snapshot(&mut self) -> anyhow::Result<()> {
        let snapshot = self
            .snapshot(self.applied_index)
            .context("Failed to generate a local snapshot")?;

        Self::validate_snapshot(&snapshot).context("Failed to validate local snapshot")?;

        self.persist_snapshot(&snapshot)
            .await
            .context("Failure to persist a local snapshot")?;

        self.entries
            .clear()
            .context("Failed to flush log entries after taking snapshot")?;
        self.metrics.first_index.set(self.first_index());

        self.snapshot_metadata = snapshot.get_metadata().clone();
        self.metrics
            .snapshot_index
            .set(self.snapshot_metadata.index);
        self.metrics.snapshot_term.set(self.snapshot_metadata.term);

        Ok(())
    }

    pub async fn apply_snapshot(&mut self, snapshot: Snapshot) -> anyhow::Result<()> {
        let meta = snapshot.get_metadata();
        let index = meta.index;

        if self.first_index() > index {
            bail!("Asked to apply an out of date snapshot");
        }

        Self::validate_snapshot(&snapshot).context("Failed to validate local snapshot")?;

        self.persist_snapshot(&snapshot)
            .await
            .context("Failure to apply snapshot from raft cluster")?;

        warn!("Applying snapshot at index: {index}");

        self.snapshot_metadata = meta.clone();
        self.metrics
            .snapshot_index
            .set(self.snapshot_metadata.index);
        self.metrics.snapshot_term.set(self.snapshot_metadata.term);

        self.entries
            .clear()
            .context("Failed to clear entries journal")?;
        self.metrics.first_index.set(self.first_index());
        self.metrics.last_index.set(self.last_index());

        self.store = serde_json::from_slice(&snapshot.data).unwrap();

        let mut hs = self.initial_state().unwrap().hard_state;
        hs.commit = index;
        hs.term = std::cmp::max(meta.term, hs.term);
        self.set_hardstate(hs)?;

        self.set_applied_index(index);

        Ok(())
    }

    pub async fn append(&self, ents: &[Entry]) -> anyhow::Result<()> {
        if ents.is_empty() {
            return Ok(());
        }

        if self.first_index() > ents[0].index {
            bail!(
                "overwrite compacted raft logs, compacted: {}, append: {}",
                self.first_index() - 1,
                ents[0].index,
            );
        }
        if self.last_index() + 1 < ents[0].index {
            bail!(
                "raft logs should be continuous, last index: {}, new appended: {}",
                self.last_index(),
                ents[0].index,
            );
        }

        while self.last_index() > ents[0].index {
            self.entries
                .pop_max()
                .context("Failed to truncate log entry before appending")?;
        }

        for ent in ents {
            let key = serialize_u64(&ent.index)?;
            let value = protobuf::Message::write_to_bytes(ent).unwrap();
            self.entries
                .insert(key, value)
                .context("Failed to store log entry")?;
        }

        self.metrics.last_index.set(self.last_index());

        let flushed = self
            .db
            .flush_async()
            .await
            .context("Failed to flush appended entries to journal")?;

        self.metrics.flushed_bytes.inc_by(flushed as u64);

        Ok(())
    }
}

#[cfg(test)]
impl RegistryStorage {
    pub fn delete(&self, idx: u64) -> Result<()> {
        self.entries.remove(serialize_u64(&idx).unwrap()).unwrap();

        Ok(())
    }

    pub fn size(&self) -> u64 {
        self.entries.len() as u64
    }
}

impl Storage for RegistryStorage {
    fn initial_state(&self) -> Result<RaftState> {
        let mut raft_state = RaftState {
            conf_state: self.conf_state.clone(),
            ..Default::default()
        };

        if let Some(index) = self.state.get(KEY_HARD_INDEX).unwrap() {
            raft_state.hard_state.commit = deserialize_u64(&index).unwrap();
        }

        if let Some(term) = self.state.get(KEY_HARD_TERM).unwrap() {
            raft_state.hard_state.term = deserialize_u64(&term).unwrap();
        }

        info!("Loaded intial raft state: {raft_state:?}");

        self.metrics.hs_index.set(raft_state.hard_state.commit);
        self.metrics.hs_term.set(raft_state.hard_state.term);

        Ok(raft_state)
    }

    fn entries(&self, low: u64, high: u64, max_size: impl Into<Option<u64>>) -> Result<Vec<Entry>> {
        let max_size = max_size.into();

        if low < self.first_index() {
            return Err(Error::Store(StorageError::Compacted));
        }

        if high > self.last_index() + 1 {
            error!(
                "index out of bound (last: {}, high: {})",
                self.last_index() + 1,
                high
            );
            return Err(Error::Store(StorageError::Unavailable));
        }

        let low = serialize_u64(&low).unwrap();
        let high = serialize_u64(&high).unwrap();
        let mut ents = self
            .entries
            .range(low..high)
            .map(|x| <Entry as protobuf::Message>::parse_from_bytes(&x.unwrap().1).unwrap())
            .collect();

        limit_size(&mut ents, max_size);

        Ok(ents)
    }

    fn term(&self, idx: u64) -> Result<u64> {
        let encoded_idx = serialize_u64(&idx).unwrap();
        if let Some(bytes) = self.entries.get(encoded_idx).unwrap() {
            let entry = <Entry as protobuf::Message>::parse_from_bytes(&bytes).unwrap();
            return Ok(entry.term);
        }

        if idx == self.snapshot_metadata.index {
            return Ok(self.snapshot_metadata.term);
        }

        let offset = self.first_index();
        if idx < offset {
            return Err(Error::Store(StorageError::Compacted));
        }

        Err(Error::Store(StorageError::Unavailable))
    }

    fn first_index(&self) -> Result<u64> {
        Ok(self.first_index())
    }

    fn last_index(&self) -> Result<u64> {
        Ok(self.last_index())
    }

    fn snapshot(&self, request_index: u64) -> Result<Snapshot> {
        info!("Snapshot requested: {}", request_index);

        if request_index > self.applied_index {
            return Err(Error::Store(StorageError::SnapshotTemporarilyUnavailable));
        }

        let mut snapshot = Snapshot::default();

        let meta = snapshot.mut_metadata();
        meta.index = self.applied_index;
        meta.term = self.term(self.applied_index)?;

        meta.set_conf_state(self.conf_state.clone());

        snapshot.set_data(serde_json::to_vec(&self.store).unwrap().into());

        info!(
            "Generated new snapshot for index {} (requested {request_index})",
            self.applied_index
        );

        Ok(snapshot)
    }
}

#[cfg(test)]
mod test {
    use crate::config::{PeerConfig, RaftConfig, RegistryConfig};
    use std::ops::{Deref, DerefMut};
    use tempfile::TempDir;

    use super::*;

    struct TestStorage {
        storage: RegistryStorage,
        _tempdir: TempDir,
    }

    impl TestStorage {
        async fn new() -> Self {
            let tempdir = tempfile::tempdir().unwrap();

            let config = Configuration {
                peers: vec![PeerConfig {
                    name: "registry-1".into(),
                    raft: RaftConfig {
                        address: "127.0.0.1".into(),
                        port: 8080,
                    },
                    registry: RegistryConfig {
                        address: "127.0.0.1".into(),
                        port: 8080,
                    },
                }],
                storage: tempdir.path().to_str().unwrap().to_owned(),
                ..Default::default()
            };

            let mut registry = <prometheus_client::registry::Registry>::default();

            TestStorage {
                storage: RegistryStorage::new(&config, &mut registry).await.unwrap(),
                _tempdir: tempdir,
            }
        }
    }

    impl Deref for TestStorage {
        type Target = RegistryStorage;

        fn deref(&self) -> &Self::Target {
            &self.storage
        }
    }

    impl DerefMut for TestStorage {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.storage
        }
    }

    #[test]
    fn test_key_assumptions() {
        let i: u64 = 1;
        assert_eq!(serialize_u64(&i).unwrap(), [0, 0, 0, 0, 0, 0, 0, 1]);

        let i: u64 = 254;
        assert_eq!(serialize_u64(&i).unwrap(), [0, 0, 0, 0, 0, 0, 0, 254]);

        let i: u64 = 255;
        assert_eq!(serialize_u64(&i).unwrap(), [0, 0, 0, 0, 0, 0, 0, 255]);

        let i: u64 = 256;
        assert_eq!(serialize_u64(&i).unwrap(), [0, 0, 0, 0, 0, 0, 1, 0]);
    }

    #[tokio::test]
    async fn test_initial_state() {
        let store = TestStorage::new().await;
        let initial_state = store.initial_state().unwrap();

        assert_eq!(initial_state.conf_state.voters, vec![1]);

        assert_eq!(initial_state.hard_state.commit, 0);
        assert_eq!(initial_state.hard_state.term, 1);
        assert_eq!(initial_state.hard_state.vote, 0);
    }

    #[tokio::test]
    async fn test_append() {
        let store = TestStorage::new().await;

        let entries = vec![
            Entry {
                entry_type: EntryType::EntryNormal,
                index: 1,
                term: 1,
                ..Default::default()
            },
            Entry {
                entry_type: EntryType::EntryNormal,
                index: 2,
                term: 1,
                ..Default::default()
            },
        ];
        store.append(&entries).await.unwrap();
        assert_eq!(store.last_index(), 2);
    }

    #[tokio::test]
    async fn test_append_lots() {
        let store = TestStorage::new().await;

        assert_eq!(store.first_index(), 1);
        assert_eq!(store.last_index(), 0);

        let mut entries = Vec::with_capacity(1024);
        for i in 1..1025 {
            entries.push(Entry {
                entry_type: EntryType::EntryNormal,
                index: i,
                term: 1,
                ..Default::default()
            });
        }

        store.append(&entries).await.unwrap();

        assert_eq!(store.first_index(), 1);
        assert_eq!(store.last_index(), 1024);
    }

    #[tokio::test]
    async fn take_snapshot() {
        let mut store = TestStorage::new().await;

        let mut entries = Vec::with_capacity(1024);
        for i in 1..1025 {
            entries.push(Entry {
                entry_type: EntryType::EntryNormal,
                index: i,
                term: 1,
                ..Default::default()
            });
        }

        store.append(&entries).await.unwrap();
        assert_eq!(store.size(), 1024);

        store.applied_index = 1024;
        store.store_snapshot().await.unwrap();

        assert_eq!(store.size(), 0);
    }

    #[tokio::test]
    async fn test_term() {
        let store = TestStorage::new().await;

        let entries = vec![
            Entry {
                entry_type: EntryType::EntryNormal,
                index: 1,
                term: 1,
                ..Default::default()
            },
            Entry {
                entry_type: EntryType::EntryNormal,
                index: 2,
                term: 1,
                ..Default::default()
            },
        ];
        store.append(&entries).await.unwrap();

        assert_eq!(store.term(1), Ok(1));
        assert_eq!(store.term(2), Ok(1));
        assert_eq!(store.term(3), Err(Error::Store(StorageError::Unavailable)));
    }

    #[tokio::test]
    async fn test_fsck_snapshot_log_gap() {
        let store = TestStorage::new().await;

        let entries = vec![
            Entry {
                entry_type: EntryType::EntryNormal,
                index: 1,
                term: 1,
                ..Default::default()
            },
            Entry {
                entry_type: EntryType::EntryNormal,
                index: 2,
                term: 1,
                ..Default::default()
            },
        ];
        store.append(&entries).await.unwrap();

        store.delete(1).unwrap();

        match store.ensure_initialized() {
            Ok(_) => {
                panic!("Error condition was not detected");
            }
            Err(err) => {
                let actual_err = err.downcast::<RegistryStorageError>().unwrap();
                println!("{actual_err:?}");
                assert!(matches!(
                    actual_err,
                    RegistryStorageError::LogsTooCompacted {
                        snapshot_index: 0,
                        first_index: 2
                    }
                ));
            }
        }
    }

    #[tokio::test]
    async fn test_fsck_log_gap() {
        let store = TestStorage::new().await;

        let entries = vec![
            Entry {
                entry_type: EntryType::EntryNormal,
                index: 1,
                term: 1,
                ..Default::default()
            },
            Entry {
                entry_type: EntryType::EntryNormal,
                index: 2,
                term: 1,
                ..Default::default()
            },
            Entry {
                entry_type: EntryType::EntryNormal,
                index: 3,
                term: 1,
                ..Default::default()
            },
        ];
        store.append(&entries).await.unwrap();

        store.delete(2).unwrap();

        match store.ensure_initialized() {
            Ok(_) => {
                panic!("Error condition was not detected");
            }
            Err(err) => {
                let actual_err = err.downcast::<RegistryStorageError>().unwrap();
                assert!(matches!(
                    actual_err,
                    RegistryStorageError::LogsMissing {
                        expected: 3,
                        actual: 2
                    }
                ));
            }
        }
    }

    #[tokio::test]
    async fn test_fsck_log_compaction() {
        let mut store = TestStorage::new().await;

        let entries = vec![
            Entry {
                entry_type: EntryType::EntryNormal,
                index: 1,
                term: 1,
                ..Default::default()
            },
            Entry {
                entry_type: EntryType::EntryNormal,
                index: 2,
                term: 1,
                ..Default::default()
            },
        ];
        store.append(&entries).await.unwrap();

        assert_eq!(store.first_index(), 1);

        // Simulate a snapshot where compaction failed - so now there are extra log entries
        // that are *before* the latest snapshot.
        store.snapshot_metadata.index = 1;

        assert!(store.ensure_initialized().is_ok());
        assert_eq!(store.first_index(), 2);
    }

    #[tokio::test]
    async fn hard_state_term_stale() {
        let store = TestStorage::new().await;

        let entries = vec![
            Entry {
                entry_type: EntryType::EntryNormal,
                index: 1,
                term: 2,
                ..Default::default()
            },
            Entry {
                entry_type: EntryType::EntryNormal,
                index: 2,
                term: 3,
                ..Default::default()
            },
            Entry {
                entry_type: EntryType::EntryNormal,
                index: 3,
                term: 4,
                ..Default::default()
            },
        ];
        store.append(&entries).await.unwrap();

        match store.ensure_initialized() {
            Ok(_) => {
                panic!("Error condition was not detected");
            }
            Err(err) => {
                let actual_err = err.downcast::<RegistryStorageError>().unwrap();
                assert!(matches!(
                    actual_err,
                    RegistryStorageError::HardStateStaleTerm {
                        hs_term: 1,
                        log_term: 4
                    }
                ));
            }
        }
    }

    #[tokio::test]
    async fn hard_state_commit_dangling() {
        let store = TestStorage::new().await;

        store
            .set_hardstate(HardState {
                commit: 512,
                term: 1,
                ..Default::default()
            })
            .unwrap();

        match store.ensure_initialized() {
            Ok(_) => {
                panic!("Error condition was not detected");
            }
            Err(err) => {
                let actual_err = err.downcast::<RegistryStorageError>().unwrap();
                assert!(matches!(
                    actual_err,
                    RegistryStorageError::HardStateDanglingCommit {
                        commit: 512,
                        last_index: 0
                    }
                ));
            }
        }
    }
}
