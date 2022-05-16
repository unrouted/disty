use crate::log::Log;

pub struct Storage {}

impl Storage {
    pub async fn load(&self) -> Log {
        Log::default()
    }

    pub async fn step(&self, log: &mut Log) {}
}
