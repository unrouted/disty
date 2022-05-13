use std::sync::Arc;

use tokio::{select, sync::Mutex, time::Instant};

use crate::{
    config::Configuration,
    machine::{Envelope, Machine, Message},
};

pub struct Raft {
    config: Configuration,
    machine: Arc<Mutex<Machine>>,
}

impl Raft {
    pub fn new(config: Configuration, machine: Arc<Mutex<Machine>>) -> Self {
        Self { config, machine }
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

            match machine.step(&envelope) {
                Ok(()) => {}
                Err(err) => {
                    error!("Raft: State machine rejected message {envelope:?} with: {err}");
                }
            }

            next_tick = machine.tick;
        }
    }
}
