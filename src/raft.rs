use std::sync::Arc;

use tokio::sync::Mutex;

use crate::machine::{Envelope, Machine, Message};

pub struct Raft {
    machine: Arc<Mutex<Machine>>,
}

impl Raft {
    pub fn new(machine: Arc<Mutex<Machine>>) -> Self {
        Self { machine }
    }

    pub async fn run(&mut self) {
        loop {
            let mut machine = self.machine.lock().await;

            let msg = Envelope {
                source: machine.identifier.clone(),
                destination: machine.identifier.clone(),
                term: 0,
                message: Message::Tick {},
            };

            match machine.step(&msg) {
                Ok(()) => {}
                Err(err) => {
                    error!("Raft: State machine rejected message {msg:?} with: {err}");
                }
            }
        }
    }
}
