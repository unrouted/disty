use serde::{Deserialize, Serialize};

use crate::types::RegistryAction;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Deserialize, Serialize)]
pub struct LogEntry {
    pub term: usize,
    pub entry: RegistryAction,
}

#[derive(Default)]
pub struct Log {
    pub entries: Vec<LogEntry>,
    pub snapshot_index: Option<usize>,
    pub snapshot_term: Option<usize>,
    pub truncate_index: Option<usize>,
    pub stored_index: Option<usize>,
}

impl Log {
    pub fn last_index(&self) -> Option<usize> {
        if self.entries.is_empty() {
            return self.snapshot_index;
        }

        Some((self.entries.len() - 1) + self.snapshot_index.unwrap_or(0))
    }

    pub fn last_term(&self) -> usize {
        match self.entries.last() {
            Some(entry) => entry.term,
            None => self.snapshot_term.unwrap_or(0),
        }
    }

    pub fn truncate(&mut self, index: usize) {
        self.truncate_index = Some(index);

        while self.last_index() > Some(index) {
            if self.entries.pop().is_none() {
                break;
            }
        }
    }

    pub fn get(&self, index: usize) -> LogEntry {
        let snapshot_index = match self.snapshot_index {
            Some(index) => index,
            _ => 0,
        };

        let adjusted_index = index - snapshot_index;

        if let Some(entry) = self.entries.get(adjusted_index) {
            return entry.clone();
        }

        panic!("Ugh, error");
    }

    pub fn append(&mut self, entry: LogEntry) {
        self.entries.push(entry);
    }
}
