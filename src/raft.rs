use std::sync::Arc;

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
    Committed { entries: Vec<LogEntry> },
}

pub struct Raft {
    config: Configuration,
    machine: Arc<Mutex<Machine>>,
    pub events: broadcast::Sender<RaftEvent>,
}

impl Raft {
    pub fn new(config: Configuration, machine: Arc<Mutex<Machine>>) -> Self {
        let (tx, _) = broadcast::channel::<RaftEvent>(100);

        Self {
            config,
            machine,
            events: tx,
        }
    }

    pub async fn run(&mut self) {
        let mut next_tick = Instant::now();

        loop {
            let envelope = select! {
                _ = tokio::time::sleep_until(next_tick) => {
                    Envelope {
                        source: self.config.identifier.clone(),
                        destination: self.config.identifier.clone(),
                        term: 0,
                        message: Message::Tick {},
                    }
                },
                // Some(request) = rx.recv() => {requests.insert(request);}
            };

            let mut machine = self.machine.lock().await;
            let current_index = machine.commit_index as usize;

            match machine.step(&envelope) {
                Ok(()) => {}
                Err(err) => {
                    error!("Raft: State machine rejected message {envelope:?} with: {err}");
                }
            }

            let next_index = machine.commit_index as usize;

            if let Err(err) = self.events.send(RaftEvent::Committed {
                entries: machine.log[current_index..next_index].to_vec(),
            }) {
                warn!("Error while notifying of commit events: {err:?}");
            }

            next_tick = machine.tick;
        }
    }
}
