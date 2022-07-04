use std::cmp;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use raft::{eraftpb::*, RaftState, Storage};

use raft::util::limit_size;
use raft::{Error, Result, StorageError};

pub struct RegistryStorageCore {
    raft_state: RaftState,
    entries: Vec<Entry>,
    snapshot_metadata: SnapshotMetadata,
}

impl Default for RegistryStorageCore {
    fn default() -> RegistryStorageCore {
        RegistryStorageCore {
            raft_state: Default::default(),
            entries: vec![],
            snapshot_metadata: Default::default(),
        }
    }
}

impl RegistryStorageCore {
    pub fn set_hardstate(&mut self, hs: HardState) {
        self.raft_state.hard_state = hs;
    }

    pub fn hard_state(&self) -> &HardState {
        &self.raft_state.hard_state
    }

    pub fn mut_hard_state(&mut self) -> &mut HardState {
        &mut self.raft_state.hard_state
    }

    pub fn commit_to(&mut self, index: u64) -> Result<()> {
        assert!(
            self.has_entry_at(index),
            "commit_to {} but the entry does not exist",
            index
        );

        let diff = (index - self.entries[0].index) as usize;
        self.raft_state.hard_state.commit = index;
        self.raft_state.hard_state.term = self.entries[diff].term;
        Ok(())
    }

    pub fn set_conf_state(&mut self, cs: ConfState) {
        self.raft_state.conf_state = cs;
    }

    #[inline]
    fn has_entry_at(&self, index: u64) -> bool {
        !self.entries.is_empty() && index >= self.first_index() && index <= self.last_index()
    }

    fn first_index(&self) -> u64 {
        match self.entries.first() {
            Some(e) => e.index,
            None => self.snapshot_metadata.index + 1,
        }
    }

    fn last_index(&self) -> u64 {
        match self.entries.last() {
            Some(e) => e.index,
            None => self.snapshot_metadata.index,
        }
    }

    pub fn apply_snapshot(&mut self, mut snapshot: Snapshot) -> Result<()> {
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
    }

    fn snapshot(&self) -> Snapshot {
        let mut snapshot = Snapshot::default();

        let meta = snapshot.mut_metadata();
        meta.index = self.raft_state.hard_state.commit;
        meta.term = match meta.index.cmp(&self.snapshot_metadata.index) {
            cmp::Ordering::Equal => self.snapshot_metadata.term,
            cmp::Ordering::Greater => {
                let offset = self.entries[0].index;
                self.entries[(meta.index - offset) as usize].term
            }
            cmp::Ordering::Less => {
                panic!(
                    "commit {} < snapshot_metadata.index {}",
                    meta.index, self.snapshot_metadata.index
                );
            }
        };

        meta.set_conf_state(self.raft_state.conf_state.clone());
        snapshot
    }

    pub fn compact(&mut self, compact_index: u64) -> Result<()> {
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

        if let Some(entry) = self.entries.first() {
            let offset = compact_index - entry.index;
            self.entries.drain(..offset as usize);
        }
        Ok(())
    }

    pub fn append(&mut self, ents: &[Entry]) -> Result<()> {
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

        let diff = ents[0].index - self.first_index();
        self.entries.drain(diff as usize..);
        self.entries.extend_from_slice(&ents);
        Ok(())
    }

    pub fn commit_to_and_set_conf_states(&mut self, idx: u64, cs: Option<ConfState>) -> Result<()> {
        self.commit_to(idx)?;
        if let Some(cs) = cs {
            self.raft_state.conf_state = cs;
        }
        Ok(())
    }
}


#[derive(Clone, Default)]
pub struct RegistryStorage {
    core: Arc<RwLock<RegistryStorageCore>>,
}

impl RegistryStorage {
    pub fn new() -> RegistryStorage {
        RegistryStorage {
            ..Default::default()
        }
    }

    pub fn new_with_conf_state<T>(conf_state: T) -> RegistryStorage
    where
        ConfState: From<T>,
    {
        let store = RegistryStorage::new();
        store.initialize_with_conf_state(conf_state);
        store
    }

    pub fn initialize_with_conf_state<T>(&self, conf_state: T)
    where
        ConfState: From<T>,
    {
        assert!(!self.initial_state().unwrap().initialized());
        let mut core = self.wl();
        core.raft_state.conf_state = ConfState::from(conf_state);
    }

    pub fn rl(&self) -> RwLockReadGuard<'_, RegistryStorageCore> {
        self.core.read().unwrap()
    }

    pub fn wl(&self) -> RwLockWriteGuard<'_, RegistryStorageCore> {
        self.core.write().unwrap()
    }
}

impl Storage for RegistryStorage {
    fn initial_state(&self) -> Result<RaftState> {
        Ok(self.rl().raft_state.clone())
    }

    fn entries(&self, low: u64, high: u64, max_size: impl Into<Option<u64>>) -> Result<Vec<Entry>> {
        let max_size = max_size.into();
        let core = self.rl();
        if low < core.first_index() {
            return Err(Error::Store(StorageError::Compacted));
        }

        if high > core.last_index() + 1 {
            panic!(
                "index out of bound (last: {}, high: {})",
                core.last_index() + 1,
                high
            );
        }

        let offset = core.entries[0].index;
        let lo = (low - offset) as usize;
        let hi = (high - offset) as usize;
        let mut ents = core.entries[lo..hi].to_vec();
        limit_size(&mut ents, max_size);
        Ok(ents)
    }

    fn term(&self, idx: u64) -> Result<u64> {
        let core = self.rl();
        if idx == core.snapshot_metadata.index {
            return Ok(core.snapshot_metadata.term);
        }

        let offset = core.first_index();
        if idx < offset {
            return Err(Error::Store(StorageError::Compacted));
        }

        if idx > core.last_index() {
            return Err(Error::Store(StorageError::Unavailable));
        }
        Ok(core.entries[(idx - offset) as usize].term)
    }

    fn first_index(&self) -> Result<u64> {
        Ok(self.rl().first_index())
    }

    fn last_index(&self) -> Result<u64> {
        Ok(self.rl().last_index())
    }

    fn snapshot(&self, request_index: u64) -> Result<Snapshot> {
        let mut core = self.wl();
        let mut snap = core.snapshot();
        if snap.get_metadata().index < request_index {
            snap.mut_metadata().index = request_index;
        }
        Ok(snap)
    }
}
