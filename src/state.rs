use hiqlite::Client;

pub struct RegistryState {
    pub client: Client,
}

impl RegistryState {
    pub fn upload_path(&self, upload_id: &str) -> String {
        format!("uploads/{upload_id}")
    }
}