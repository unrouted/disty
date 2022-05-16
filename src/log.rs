use serde::{Deserialize, Serialize};

use crate::types::RegistryAction;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Deserialize, Serialize)]
pub struct LogEntry {
    pub term: u64,
    pub entry: RegistryAction,
}

#[derive(Default)]
pub struct Log {
    pub entries: Vec<LogEntry>,
    pub snapshot_index: Option<u64>,
    pub snapshot_term: Option<u64>,
    pub truncate_index: Option<u64>,
    pub stored_index: u64,
}

impl<Idx> std::ops::Index<Idx> for Log
where
    Idx: std::slice::SliceIndex<[LogEntry]>,
{
    type Output = Idx::Output;

    fn index(&self, index: Idx) -> &Self::Output {
        &self.entries[index]
    }
}

impl Log {
    pub fn last_index(&self) -> u64 {
        let snapshot_index = self.snapshot_index.unwrap_or(0);

        snapshot_index + self.entries.len() as u64
    }

    pub fn last_term(&self) -> u64 {
        match self.entries.last() {
            Some(entry) => entry.term,
            None => self.snapshot_term.unwrap_or(0),
        }
    }

    pub fn truncate(&mut self, index: u64) {
        self.truncate_index = Some(index);

        while self.last_index() >= index {
            if self.entries.pop().is_none() {
                break;
            }
        }
    }

    pub fn get(&self, index: u64) -> LogEntry {
        let snapshot_index = match self.snapshot_index {
            Some(index) => index,
            _ => 0,
        };

        let adjusted_index = index - snapshot_index - 1;

        if let Some(entry) = self.entries.get(adjusted_index as usize) {
            return entry.clone();
        }

        panic!("Ugh, error");
    }

    pub fn append(&mut self, entry: LogEntry) {
        self.entries.push(entry);
    }
}
