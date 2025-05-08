use std::path::PathBuf;

use hiqlite::Client;

pub struct RegistryState {
    pub client: Client,
}

impl RegistryState {
    pub fn upload_path(&self, upload_id: &str) -> PathBuf {
        PathBuf::from(format!("uploads/{upload_id}"))
    }
}
