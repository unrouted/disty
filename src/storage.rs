use std::io::SeekFrom;

use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use crate::{config::Configuration, log::Log};

pub struct Storage {
    term: tokio::fs::File,
    file: tokio::fs::File,
}

impl Storage {
    pub async fn new(config: Configuration) -> (Self, usize, Log) {
        let path = std::path::PathBuf::from(&config.storage).join("term");
        let mut log = Log::default();

        let term = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(path)
            .await;

        let mut term = match term {
            Ok(file) => file,
            Err(_err) => {
                panic!("Could not open term");
            }
        };

        let mut buffer = String::new();
        let buffer = match term.read_to_string(&mut buffer).await {
            Ok(term) => {
                if term == 0 {
                    "0".to_string()
                } else {
                    buffer
                }
            }
            Err(_err) => {
                panic!("Could not read term");
            }
        };

        let term_value: usize = match serde_json::from_str(&buffer) {
            Ok(term_value) => term_value,
            Err(err) => {
                panic!("term file is corrupt: {err:?}");
            }
        };

        let path = std::path::PathBuf::from(&config.storage).join("journal");
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(path)
            .await;

        let file = match file {
            Ok(file) => file,
            Err(_err) => {
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

        (Storage { file, term }, term_value, log)
    }

    pub async fn step(&mut self, log: &mut Log, term: usize) {
        if let Err(err) = self.term.seek(SeekFrom::Start(0)).await {
            warn!("Error truncating term: {err:?}");
            return;
        }

        if let Err(err) = self.term.write(term.to_string().as_bytes()).await {
            warn!("Error truncating term: {err:?}");
            return;
        }

        if let Some(_truncate_index) = log.truncate_index {
            if let Err(err) = self.file.set_len(0).await {
                warn!("Error truncating: {err:?}");
                return;
            }

            log.stored_index = None;
            log.truncate_index = None;
        }

        let source = match log.stored_index {
            None => &log.entries,
            Some(stored_index) => &log.entries[stored_index..],
        };

        for row in source {
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
