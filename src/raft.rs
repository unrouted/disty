use std::{collections::HashMap, sync::Arc};

use serde::{Deserialize, Serialize};
use tokio::{
    select,
    sync::{broadcast, Mutex},
    time::Instant,
};

use crate::{
    config::Configuration,
    log::LogEntry,
    machine::{Envelope, Machine, Message},
    storage::Storage,
    types::Broadcast,
};

#[derive(Clone, Debug)]
pub enum RaftEvent {
    Committed {
        start_index: usize,
        entries: Vec<LogEntry>,
    },
}

#[derive(Debug)]
struct RaftQueueEntry {
    envelope: Envelope,
    callback: Option<tokio::sync::oneshot::Sender<Result<RaftQueueResult, String>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftQueueResult {
    pub index: usize,
    pub term: usize,
}

pub struct Raft {
    config: Configuration,
    machine: Arc<Mutex<Machine>>,
    inbox: tokio::sync::mpsc::Sender<RaftQueueEntry>,
    inbox_rx: Mutex<tokio::sync::mpsc::Receiver<RaftQueueEntry>>,
    pub events: broadcast::Sender<RaftEvent>,
    clients: HashMap<String, tokio::sync::mpsc::Sender<Envelope>>,
}

impl Raft {
    pub fn new(
        config: Configuration,
        machine: Arc<Mutex<Machine>>,
        clients: HashMap<String, tokio::sync::mpsc::Sender<Envelope>>,
    ) -> Self {
        let (tx, _) = broadcast::channel::<RaftEvent>(100);
        let (inbox, inbox_rx) = tokio::sync::mpsc::channel::<RaftQueueEntry>(100);

        Self {
            config,
            machine,
            events: tx,
            inbox,
            inbox_rx: Mutex::new(inbox_rx),
            clients,
        }
    }

    pub async fn queue_envelope(&self, envelope: Envelope) {
        let res = self
            .inbox
            .send(RaftQueueEntry {
                envelope,
                callback: None,
            })
            .await;

        if let Err(err) = res {
            warn!("Error whilst queueing envelope: {err:?}");
        }
    }

    pub async fn run_envelope(&self, envelope: Envelope) -> Result<RaftQueueResult, String> {
        let (tx, rx) = tokio::sync::oneshot::channel::<Result<RaftQueueResult, String>>();

        let res = self
            .inbox
            .send(RaftQueueEntry {
                envelope,
                callback: Some(tx),
            })
            .await;

        if let Err(err) = res {
            warn!("Error whilst queueing envelope: {err:?}");
            return Err(format!("Error whilst queueing envelope: {err:?}"));
        }

        println!("Waiting");

        match rx.await {
            Ok(res) => res,
            Err(err) => Err(format!("Error waiting for raft state machine: {err:?}")),
        }
    }

    pub async fn run(&self, mut broadcasts: tokio::sync::broadcast::Receiver<Broadcast>) {
        let mut next_tick = Instant::now();

        let (mut storage, term, mut log) = Storage::new(self.config.clone()).await;

        {
            let mut machine = self.machine.lock().await;
            machine.term = term;
        }

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
                _ = broadcasts.recv() => {
                    debug!("Raft: Stopping in response to SIGINT");
                    return;
                },
                Some(entry) = async { self.inbox_rx.lock().await.recv().await } => entry,
            };

            let mut machine = self.machine.lock().await;
            let current_index = machine.commit_index;

            match machine.step(&mut log, &envelope) {
                Ok(()) => {
                    if let Some(callback) = callback {
                        let res = callback.send(Ok(RaftQueueResult {
                            index: machine.pending_index,
                            term: log.last_term(),
                        }));

                        if let Err(err) = res {
                            warn!("Error whilst running callback: {err:?}");
                        }
                    }
                }
                Err(err) => {
                    error!("Raft: State machine rejected message {envelope:?} with: {err}");

                    if let Some(callback) = callback {
                        let res = callback.send(Err(err));
                        if let Err(err) = res {
                            warn!("Error whilst running callback: {err:?}");
                        }
                    }
                }
            }

            storage.step(&mut log, machine.term).await;
            machine.stored_index = log.last_index();

            for envelope in machine.outbox.iter().cloned() {
                match self.clients.get(&envelope.destination) {
                    Some(client) => {
                        if let Err(err) = client.send(envelope.clone()).await {
                            warn!("Error queueing envelope in outbox: {err:?}")
                        }
                    }
                    None => {}
                }
            }

            if machine.commit_index > current_index {
                if let Some(next_index) = machine.commit_index {
                    let ev = match current_index {
                        None => RaftEvent::Committed {
                            start_index: 0,
                            entries: log.entries.clone(),
                        },
                        Some(current_index) => RaftEvent::Committed {
                            start_index: current_index,
                            entries: log[current_index..next_index].to_vec(),
                        },
                    };
                    if let Err(err) = self.events.send(ev) {
                        warn!("Error while notifying of commit events: {err:?}");
                    }
                }
            }

            next_tick = machine.tick;
        }
    }
}
