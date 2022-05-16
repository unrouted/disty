use crate::{config::Configuration, log::Log};

use jsonl;

pub struct Storage {
    file: tokio::fs::File,
}

impl Storage {
    pub async fn new(config: Configuration) -> (Self, Log) {
        let path = std::path::PathBuf::from(&config.storage).join("journal");
        let mut log = Log::default();

        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(&path)
            .await;

        let file = match file {
            Ok(file) => file,
            Err(err) => {
                panic!("Could not open journal");
            }
        };

        let mut reader = tokio::io::BufReader::new(file.try_clone().await.unwrap());
        loop {
            match jsonl::read(&mut reader).await {
                Ok(line) => log.entries.push(line),
                Err(jsonl::ReadError::Eof) => break,
                Err(err) => {
                    warn!("Couldt load journal: {err:?}");
                }
            }
        }

        log.stored_index = log.last_index();

        (Storage { file: file }, log)
    }

    pub async fn step(&mut self, log: &mut Log) {
        if let Some(_truncate_index) = log.truncate_index {
            if let Err(err) = self.file.set_len(0).await {
                warn!("Error truncating: {err:?}");
                return;
            }

            log.stored_index = 0;
            log.truncate_index = None;
        }

        for row in &log.entries[log.stored_index as usize..] {
            match jsonl::write(&mut self.file, row).await {
                Ok(_) => (),
                Err(err) => {
                    warn!("Error writing log entry: {err:?}");
                    return;
                }
            }
        }

        log.stored_index = log.last_index();
    }
}
