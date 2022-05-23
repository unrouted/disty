use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Registry;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};
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
use crate::{machine::PeerState, types::MachineMetricLabels};

#[derive(Clone, Debug)]
pub enum RaftEvent {
    Committed {
        start_index: Option<usize>,
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
    pub leader: tokio::sync::watch::Receiver<Option<String>>,
    leader_tx: tokio::sync::watch::Sender<Option<String>>,
    clients: HashMap<String, tokio::sync::mpsc::Sender<Envelope>>,
    last_saved: Family<MachineMetricLabels, Gauge>,
    last_term_saved: Family<MachineMetricLabels, Gauge>,
    current_term: Family<MachineMetricLabels, Gauge>,
    current_state: Family<MachineMetricLabels, Gauge>,
}

impl Raft {
    pub fn new(
        config: Configuration,
        machine: Arc<Mutex<Machine>>,
        clients: HashMap<String, tokio::sync::mpsc::Sender<Envelope>>,
        registry: &mut Registry,
    ) -> Self {
        let (tx, _) = broadcast::channel::<RaftEvent>(100);
        let (inbox, inbox_rx) = tokio::sync::mpsc::channel::<RaftQueueEntry>(100);

        let last_saved = Family::<MachineMetricLabels, Gauge>::default();
        registry.register(
            "distribd_last_saved",
            "Last index that was stored in the commit log",
            Box::new(last_saved.clone()),
        );

        let last_term_saved = Family::<MachineMetricLabels, Gauge>::default();
        registry.register(
            "distribd_last_saved_term",
            "Last term that was stored in the commit log",
            Box::new(last_term_saved.clone()),
        );

        let current_term = Family::<MachineMetricLabels, Gauge>::default();
        registry.register(
            "distribd_current_term",
            "The current term for a node",
            Box::new(current_term.clone()),
        );

        let current_state = Family::<MachineMetricLabels, Gauge>::default();
        registry.register(
            "distribd_current_state",
            "The current state for a node",
            Box::new(current_state.clone()),
        );

        let (leader_tx, leader) = tokio::sync::watch::channel::<Option<String>>(None);

        Self {
            config,
            machine,
            events: tx,
            inbox,
            inbox_rx: Mutex::new(inbox_rx),
            leader,
            leader_tx,
            clients,
            last_saved,
            last_term_saved,
            current_term,
            current_state,
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
            machine.pending_index = log.last_index();
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
                            index: machine.pending_index.unwrap_or(0),
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
                            start_index: None,
                            entries: log.entries.clone(),
                        },
                        Some(current_index) => RaftEvent::Committed {
                            start_index: Some(current_index),
                            entries: log.entries[current_index..next_index].to_vec(),
                        },
                    };
                    if let Err(err) = self.events.send(ev) {
                        warn!("Error while notifying of commit events: {err:?}");
                    }
                }
            }

            let leader = match machine.state {
                PeerState::Leader => Some(machine.identifier.clone()),
                _ => machine.leader.clone(),
            };

            if *self.leader_tx.borrow() != leader {
                self.leader_tx.send_replace(leader);
            }

            let labels = MachineMetricLabels {
                identifier: self.config.identifier.clone(),
            };
            self.last_saved
                .get_or_create(&labels)
                .set(machine.stored_index.unwrap_or(0) as u64);
            self.last_term_saved
                .get_or_create(&labels)
                .set(machine.applied_index as u64);
            self.current_term
                .get_or_create(&labels)
                .set(machine.applied_index as u64);
            self.current_state
                .get_or_create(&labels)
                .set(machine.applied_index as u64);

            next_tick = machine.tick;
        }
    }
}
