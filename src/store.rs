use std::path::PathBuf;

use anyhow::{bail, Context};
use log::{error, info, warn};
use raft::{eraftpb::*, RaftState, Storage};
use std::io::ErrorKind;

use raft::util::limit_size;
use raft::{Error, Result, StorageError};

use crate::config::Configuration;
use crate::state::RegistryState;
use crate::types::RegistryAction;

const KEY_HARD_INDEX: &[u8; 8] = b"hs-index";
const KEY_HARD_TERM: &[u8; 7] = b"hs-term";
const KEY_HARD_VOTE: &[u8; 7] = b"hs-vote";

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
}

impl RegistryStorage {
    pub async fn new(config: &Configuration) -> anyhow::Result<RegistryStorage> {
        let storage_path = std::path::Path::new(&config.storage);
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
                ErrorKind::NotFound => {}
                _ => {
                    bail!("Unexpected error: {err:?}");
                }
            },
        };

        let store = RegistryStorage {
            db,
            entries,
            state,
            conf_state,
            snapshot_metadata,
            store,
            applied_index,
            storage_path: storage_path.to_path_buf(),
        };

        store.ensure_initialized()?;

        Ok(store)
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

        Ok(())
    }

    pub fn set_hardstate(&self, hs: HardState) -> anyhow::Result<()> {
        self.state
            .transaction::<_, _, sled::transaction::TransactionError>(|t| {
                t.insert(KEY_HARD_INDEX, bincode::serialize(&hs.commit).unwrap())?;
                t.insert(KEY_HARD_TERM, bincode::serialize(&hs.term).unwrap())?;
                t.insert(KEY_HARD_VOTE, bincode::serialize(&hs.vote).unwrap())?;
                Ok(())
            })
            .context("Transaction failure")?;

        Ok(())
    }

    pub fn set_commit(&self, commit: u64) {
        self.state
            .insert(KEY_HARD_INDEX, bincode::serialize(&commit).unwrap())
            .unwrap();
    }

    pub fn last_index(&self) -> u64 {
        if let Some((key, _value)) = self.entries.last().unwrap() {
            return bincode::deserialize(&key).unwrap();
        }

        self.snapshot_metadata.index
    }

    pub fn first_index(&self) -> u64 {
        if let Some((key, _value)) = self.entries.first().unwrap() {
            return bincode::deserialize(&key).unwrap();
        }

        self.snapshot_metadata.index + 1
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

        self.compact(self.applied_index)
            .context("Failure to compact after making snapshot")?;

        self.snapshot_metadata = snapshot.get_metadata().clone();

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
        self.store = serde_json::from_slice(&snapshot.data).unwrap();

        let mut hs = self.initial_state().unwrap().hard_state;
        hs.commit = index;
        hs.term = std::cmp::max(meta.term, hs.term);
        self.set_hardstate(hs)?;

        self.applied_index = index;

        Ok(())
    }

    fn compact(&self, compact_index: u64) -> anyhow::Result<()> {
        if compact_index <= self.first_index() {
            // Don't need to treat this case as an error.
            return Ok(());
        }

        if compact_index > self.last_index() + 1 {
            bail!(
                "compact not received raft logs: {}, last index: {}",
                compact_index,
                self.last_index()
            );
        }

        while self.first_index() < compact_index {
            self.entries
                .pop_min()
                .context("Failed to compact old log entry")?;
        }

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
            let key = bincode::serialize(&ent.index).unwrap();
            let value = protobuf::Message::write_to_bytes(ent).unwrap();
            self.entries
                .insert(key, value)
                .context("Failed to store log entry")?;
        }

        self.db
            .flush_async()
            .await
            .context("Failed to flush appended entries to journal")?;

        Ok(())
    }
}

impl Storage for RegistryStorage {
    fn initial_state(&self) -> Result<RaftState> {
        let mut raft_state = RaftState {
            conf_state: self.conf_state.clone(),
            ..Default::default()
        };

        if let Some(index) = self.state.get(KEY_HARD_INDEX).unwrap() {
            raft_state.hard_state.commit = bincode::deserialize(&index).unwrap();
        }

        if let Some(term) = self.state.get(KEY_HARD_TERM).unwrap() {
            raft_state.hard_state.term = bincode::deserialize(&term).unwrap();
        }

        info!("Loaded intial raft state: {raft_state:?}");

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

        let low = bincode::serialize(&low).unwrap();
        let high = bincode::serialize(&high).unwrap();
        let mut ents = self
            .entries
            .range(low..high)
            .map(|x| <Entry as protobuf::Message>::parse_from_bytes(&x.unwrap().1).unwrap())
            .collect();

        limit_size(&mut ents, max_size);

        Ok(ents)
    }

    fn term(&self, idx: u64) -> Result<u64> {
        let encoded_idx = bincode::serialize(&idx).unwrap();
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

    use super::*;

    async fn get_test_storage() -> RegistryStorage {
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

        RegistryStorage::new(&config).await.unwrap()
    }

    #[tokio::test]
    async fn test_initial_state() {
        let store = get_test_storage().await;
        let initial_state = store.initial_state().unwrap();

        assert_eq!(initial_state.conf_state.voters, vec![1]);

        assert_eq!(initial_state.hard_state.commit, 1);
        assert_eq!(initial_state.hard_state.term, 1);
        assert_eq!(initial_state.hard_state.vote, 0);
    }

    #[tokio::test]
    async fn test_append() {
        let store = get_test_storage().await;

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
    async fn test_compact() {
        let store = get_test_storage().await;

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

        store.compact(2).unwrap();
        assert_eq!(store.first_index(), 2);

        store.compact(2).unwrap();
        assert_eq!(store.first_index(), 2);
    }

    #[tokio::test]
    async fn test_term() {
        let store = get_test_storage().await;

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
}
