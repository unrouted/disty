use std::cmp;

use raft::{eraftpb::*, RaftState, Storage};

use raft::util::limit_size;
use raft::{Error, Result, StorageError};

use crate::config::Configuration;

const KEY_HARD_INDEX: &[u8; 8] = b"hs-index";
const KEY_HARD_TERM: &[u8; 7] = b"hs-term";
const KEY_HARD_VOTE: &[u8; 7] = b"hs-vote";

#[derive(Clone)]
pub struct RegistryStorage {
    db: sled::Db,
    entries: sled::Tree,
    state: sled::Tree,
    conf_state: ConfState,
}

impl RegistryStorage {
    pub fn new(config: &Configuration) -> anyhow::Result<RegistryStorage> {
        let path = std::path::Path::new(&config.storage).join("raft");
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

        Ok(RegistryStorage {
            db,
            entries,
            state,
            conf_state,
        })
    }

    pub fn set_hardstate(&self, hs: HardState) {
        self.state
            .transaction::<_, _, sled::transaction::TransactionError>(|t| {
                t.insert(KEY_HARD_INDEX, bincode::serialize(&hs.commit).unwrap())?;
                t.insert(KEY_HARD_TERM, bincode::serialize(&hs.term).unwrap())?;
                t.insert(KEY_HARD_VOTE, bincode::serialize(&hs.vote).unwrap())?;
                Ok(())
            });
    }

    pub fn set_commit(&self, commit: u64) {
        self.state
            .insert(KEY_HARD_INDEX, bincode::serialize(&commit).unwrap());
    }

    pub fn last_index(&self) -> u64 {
        if let Some((key, _value)) = self.entries.last().unwrap() {
            return bincode::deserialize(&key).unwrap();
        }

        //  self.snapshot_metadata.index,

        0
    }

    pub fn first_index(&self) -> u64 {
        if let Some((key, _value)) = self.entries.first().unwrap() {
            return bincode::deserialize(&key).unwrap();
        }

        // self.snapshot_metadata.index + 1,

        0
    }

    pub fn apply_snapshot(&self, mut snapshot: Snapshot) -> Result<()> {
        /*
        let mut meta = snapshot.take_metadata();
        let index = meta.index;

        if self.first_index() > index {
            return Err(Error::Store(StorageError::SnapshotOutOfDate));
        }

        self.snapshot_metadata = meta.clone();

        self.raft_state.hard_state.term = cmp::max(self.raft_state.hard_state.term, meta.term);
        self.raft_state.hard_state.commit = index;
        self.entries.clear();

        // Update conf states.
        self.raft_state.conf_state = meta.take_conf_state();
        Ok(())
        */

        Ok(())
    }

    pub fn compact(&self, compact_index: u64) -> Result<()> {
        if compact_index <= self.first_index() {
            // Don't need to treat this case as an error.
            return Ok(());
        }

        if compact_index > self.last_index() + 1 {
            panic!(
                "compact not received raft logs: {}, last index: {}",
                compact_index,
                self.last_index()
            );
        }

        while self.first_index() > compact_index {
            self.entries.pop_min();
        }

        Ok(())
    }

    pub fn append(&self, ents: &[Entry]) -> Result<()> {
        if ents.is_empty() {
            return Ok(());
        }
        if self.first_index() > ents[0].index {
            panic!(
                "overwrite compacted raft logs, compacted: {}, append: {}",
                self.first_index() - 1,
                ents[0].index,
            );
        }
        if self.last_index() + 1 < ents[0].index {
            panic!(
                "raft logs should be continuous, last index: {}, new appended: {}",
                self.last_index(),
                ents[0].index,
            );
        }

        while self.last_index() > ents[0].index {
            self.entries.pop_max();
        }

        for ent in ents {
            let key = bincode::serialize(&ent.index).unwrap();
            let value = protobuf::Message::write_to_bytes(ent).unwrap();
            self.entries.insert(key, value);
        }

        Ok(())
    }
}

impl Storage for RegistryStorage {
    fn initial_state(&self) -> Result<RaftState> {
        let mut raft_state = RaftState::default();

        raft_state.conf_state = self.conf_state.clone();

        if let Some(index) = self.state.get(KEY_HARD_INDEX).unwrap() {
            raft_state.hard_state.commit = bincode::deserialize(&index).unwrap();
        }

        if let Some(term) = self.state.get(KEY_HARD_TERM).unwrap() {
            raft_state.hard_state.term = bincode::deserialize(&term).unwrap();
        }

        Ok(raft_state)
    }

    fn entries(&self, low: u64, high: u64, max_size: impl Into<Option<u64>>) -> Result<Vec<Entry>> {
        let max_size = max_size.into();

        if low < self.first_index() {
            return Err(Error::Store(StorageError::Compacted));
        }

        if high > self.last_index() + 1 {
            panic!(
                "index out of bound (last: {}, high: {})",
                self.last_index() + 1,
                high
            );
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

        /*
        if idx == self.snapshot_metadata.index {
            return Ok(self.snapshot_metadata.term);
        }
        */

        let offset = self.first_index();
        if idx < offset {
            return Err(Error::Store(StorageError::Compacted));
        }

        return Err(Error::Store(StorageError::Unavailable));
    }

    fn first_index(&self) -> Result<u64> {
        Ok(self.first_index())
    }

    fn last_index(&self) -> Result<u64> {
        Ok(self.last_index())
    }

    fn snapshot(&self, request_index: u64) -> Result<Snapshot> {
        // FIXME: This check should be for *applied index* not last index
        let applied_index = self.last_index();

        if request_index > applied_index {
            return Err(Error::Store(StorageError::SnapshotTemporarilyUnavailable));
        }

        let mut snapshot = Snapshot::default();

        let meta = snapshot.mut_metadata();
        meta.index = applied_index;
        meta.term = self.term(applied_index)?;

        // meta.set_conf_state(self.raft_state.conf_state.clone());

        // FIXME: Need access to app.state for this
        // snapshot.set_data();

        Ok(snapshot)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_initial_state() {
        let config = Configuration {
            peers: vec![],
            ..Default::default()
        };
        let store = RegistryStorage::new(&config).unwrap();
    }

    #[test]
    fn test_append() {
        let config = Configuration {
            peers: vec![],
            ..Default::default()
        };
        let store = RegistryStorage::new(&config).unwrap();

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
        store.append(&entries);
        assert_eq!(store.last_index(), 2);
    }

    #[test]
    fn test_compact() {
        let config = Configuration {
            peers: vec![],
            ..Default::default()
        };
        let store = RegistryStorage::new(&config).unwrap();

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
        store.append(&entries);
        assert_eq!(store.last_index(), 2);

        store.compact(1);
        assert_eq!(store.last_index(), 1);
    }
}
