use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::{
    select,
    sync::{broadcast, Mutex},
    time::Instant,
};

use crate::{
    config::Configuration,
    machine::{Envelope, LogEntry, Machine, Message},
};

#[derive(Clone, Debug)]
pub enum RaftEvent {
    Committed {
        start_index: u64,
        entries: Vec<LogEntry>,
    },
}

struct RaftQueueEntry {
    envelope: Envelope,
    callback: Option<tokio::sync::oneshot::Sender<Result<RaftQueueResult, String>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftQueueResult {
    pub index: u64,
    pub term: u64,
}

pub struct Raft {
    config: Configuration,
    machine: Arc<Mutex<Machine>>,
    inbox: tokio::sync::mpsc::Sender<RaftQueueEntry>,
    inbox_rx: Mutex<tokio::sync::mpsc::Receiver<RaftQueueEntry>>,
    pub events: broadcast::Sender<RaftEvent>,
}

impl Raft {
    pub fn new(config: Configuration, machine: Arc<Mutex<Machine>>) -> Self {
        let (tx, _) = broadcast::channel::<RaftEvent>(100);
        let (inbox, inbox_rx) = tokio::sync::mpsc::channel::<RaftQueueEntry>(100);

        Self {
            config,
            machine,
            events: tx,
            inbox,
            inbox_rx: Mutex::new(inbox_rx),
        }
    }

    pub async fn queue_envelope(&self, envelope: Envelope) {
        self.inbox
            .send(RaftQueueEntry {
                envelope,
                callback: None,
            })
            .await;
    }

    pub async fn run_envelope(&self, envelope: Envelope) -> Result<RaftQueueResult, String> {
        let (tx, rx) = tokio::sync::oneshot::channel::<Result<RaftQueueResult, String>>();
        self.inbox
            .send(RaftQueueEntry {
                envelope,
                callback: Some(tx),
            })
            .await;

        match rx.await {
            Ok(res) => res,
            Err(err) => Err(format!("Error waiting for raft state machine: {err:?}")),
        }
    }

    pub async fn run(&self) {
        let mut next_tick = Instant::now();

        loop {
            let RaftQueueEntry { envelope, callback } = select! {
                _ = tokio::time::sleep_until(next_tick) => {
                    RaftQueueEntry{
                    callback: None,
                    envelope: Envelope {
                        source: self.config.identifier.clone(),
                        destination: self.config.identifier.clone(),
                        term: 0,
                        message: Message::Tick {},
                    }}
                },
                Some(entry) = async { self.inbox_rx.lock().await.recv().await } => entry,
            };

            let mut machine = self.machine.lock().await;
            let current_index = machine.commit_index as usize;

            match machine.step(&envelope) {
                Ok(()) => {
                    if let Some(callback) = callback {
                        callback.send(Ok(RaftQueueResult {
                            index: machine.log.last_index(),
                            term: machine.log.last_term(),
                        }));
                    }
                }
                Err(err) => {
                    error!("Raft: State machine rejected message {envelope:?} with: {err}");

                    if let Some(callback) = callback {
                        callback.send(Err(err));
                    }
                }
            }

            let next_index = machine.commit_index as usize;
            if next_index - current_index > 0 {
                if let Err(err) = self.events.send(RaftEvent::Committed {
                    start_index: current_index as u64,
                    entries: machine.log[current_index..next_index].to_vec(),
                }) {
                    warn!("Error while notifying of commit events: {err:?}");
                }
            }

            next_tick = machine.tick;
        }
    }
}
