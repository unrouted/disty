use anyhow::{Context, Result};
use axum::body::BodyDataStream;
use futures_util::StreamExt;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::digest::Digest;

pub(crate) async fn upload_part(
    filename: &std::path::Path,
    mut body: BodyDataStream,
) -> Result<()> {
    let parent = filename
        .parent()
        .context("Could not construct parent path")?;
    tokio::fs::create_dir_all(parent)
        .await
        .context("Failed to ensure path exists")?;

    let mut file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(&filename)
        .await
        .with_context(|| format!("Unable to create and open file: {:?}", &filename))?;

    while let Some(item) = body.next().await {
        let item = item.context("Unable to stream from client")?;
        file.write_all(&item)
            .await
            .context("Unable to write to open file")?;
    }

    file.sync_all().await.context("Failed to fsync")?;

    Ok(())
}

pub(crate) async fn get_hash(filename: &std::path::Path) -> Option<Digest> {
    match File::open(&filename).await {
        Ok(file) => {
            let mut buffer = [0; 1024];
            let mut reader = tokio::io::BufReader::new(file);
            let mut hasher = ring::digest::Context::new(&ring::digest::SHA256);

            loop {
                let len = match reader.read(&mut buffer).await {
                    Ok(0) => break,
                    Ok(size) => size,
                    _ => {
                        return None;
                    }
                };
                hasher.update(&buffer[..len]);
            }

            Some(Digest::from_sha256(&hasher.finish()))
        }
        _ => None,
    }
}

pub(crate) async fn validate_hash(filename: &std::path::Path, expected_hash: &Digest) -> bool {
    match get_hash(filename).await {
        Some(actual_digest) => &actual_digest == expected_hash,
        None => false,
    }
}
