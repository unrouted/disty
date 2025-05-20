use std::sync::Arc;

use anyhow::{Context, Result, bail};
use rand::seq::IndexedRandom;
use reqwest::Client;
use tokio::{io::AsyncWriteExt, task::JoinSet};
use tracing::{debug, error};

use crate::{
    digest::Digest,
    notify::Notification,
    state::{Blob, RegistryState},
};

async fn download_blob(blob: &Blob, state: &RegistryState, client: &Client) -> Result<()> {
    let mut urls = vec![];

    let digest = blob.digest.to_string();

    for node in state.config.nodes.iter() {
        // FIXME: Add extra check that we can never ever download from ourself.
        let node_id = 1 << (node.id - 1);
        if (blob.location & node_id) == 0 {
            continue;
        }

        let url = &node.addr_registry;

        let repo = match blob.repositories.iter().next() {
            Some(repo) => repo,
            None => continue,
        };

        urls.push(format!("{url}/{repo}/blobs/{digest}"));
    }

    let url = urls
        .choose(&mut rand::rng())
        .context("Unable to select url for blob")?;

    let mut resp = client.get(url).send().await?.error_for_status()?;

    let file_name = state.get_temp_path();

    let mut file = tokio::fs::File::create(&file_name).await?;

    let mut hasher = ring::digest::Context::new(&ring::digest::SHA256);

    loop {
        match resp.chunk().await? {
            Some(chunk) => {
                file.write_all(&chunk).await?;
                debug!("Mirroring: Downloaded {} bytes", chunk.len());
                hasher.update(&chunk);
            }
            None => {
                debug!("Mirroring: Finished streaming");
                break;
            }
        };
    }

    file.flush().await?;

    debug!("Mirroring: Output flushed");

    file.sync_all().await?;

    debug!("Mirroring: Output synced");

    drop(file);

    debug!("Mirroring: File handle dropped");

    let download_digest = Digest::from_sha256(&hasher.finish());

    if blob.digest != download_digest {
        debug!("Mirroring: Download of {url} complete but wrong digest: {download_digest}");
        bail!("Output corrupt");
    }

    debug!("Mirroring: Download has correct hash ({download_digest} vs {digest})");

    let storage_path = state.get_blob_path(&blob.digest);
    tokio::fs::rename(file_name, storage_path).await?;

    // FIXME: Update database

    Ok(())
}

pub(crate) fn start_mirror(
    tasks: &mut JoinSet<Result<()>>,
    state: Arc<RegistryState>,
) -> Result<()> {
    if state.config.nodes.len() <= 1 {
        debug!("not starting mirror as not enough nodes");
        return Ok(());
    }

    tasks.spawn(async move {
        let client = reqwest::ClientBuilder::new().build()?;

        loop {
            let _: Notification = state.client.listen_after_start().await?;

            for blob in state.get_missing_blobs().await? {
                if let Err(e) = download_blob(&blob, &state, &client).await {
                    error!("Failed to mirror blob {}", blob.digest);
                };
            }
        }
    });

    Ok(())
}
