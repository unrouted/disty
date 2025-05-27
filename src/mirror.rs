use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result, bail};
use futures::StreamExt;
use rand::seq::IndexedRandom;
use reqwest::Client;
use tokio::{io::AsyncWriteExt, task::JoinSet, time::interval};
use tokio_stream::wrappers::IntervalStream;
use tracing::{debug, error, info};

use crate::{
    config::acl::Action,
    digest::Digest,
    issuer::issue_token,
    notify::Notification,
    state::{Blob, Manifest, RegistryState},
    token::Access,
};

async fn download_blob(blob: &Blob, state: &RegistryState, client: &Client) -> Result<()> {
    let mut urls = vec![];

    let digest = blob.digest.to_string();

    let repo = match blob.repositories.iter().next() {
        Some(repo) => repo,
        None => bail!("Blob not available via any repository"),
    };

    for node in state.config.nodes.iter() {
        // FIXME: Add extra check that we can never ever download from ourself.
        let node_id = 1 << (node.id - 1);
        if (blob.location & node_id) == 0 {
            continue;
        }

        let url = &node.addr_registry;

        urls.push(format!("http://{url}/v2/{repo}/blobs/{digest}"));
    }

    let url = urls
        .choose(&mut rand::rng())
        .context("Unable to select url for blob")?;

    let req = client.get(url);

    let req = match &state.config.token_server {
        Some(token_server) => {
            let token = issue_token(
                &token_server,
                vec![Access {
                    type_: "repository".to_string(),
                    name: repo.to_string(),
                    actions: [Action::Pull].into_iter().collect(),
                }],
            )?
            .token;
            req.header("Authorization", format!("Bearer {token}"))
        }
        None => req,
    };

    let mut resp = req.send().await?.error_for_status()?;

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

    let parent = storage_path
        .parent()
        .context("Couldn't find parent directory")?;
    tokio::fs::create_dir_all(parent).await?;

    tokio::fs::rename(file_name, storage_path).await?;

    state.blob_downloaded(&blob.digest).await?;

    Ok(())
}

async fn download_manifest(
    manifest: &Manifest,
    state: &RegistryState,
    client: &Client,
) -> Result<()> {
    let mut urls = vec![];

    let digest = manifest.digest.to_string();

    let repo = &manifest.repository;

    for node in state.config.nodes.iter() {
        // FIXME: Add extra check that we can never ever download from ourself.
        let node_id = 1 << (node.id - 1);
        if (manifest.location & node_id) == 0 {
            continue;
        }

        let url = &node.addr_registry;

        urls.push(format!("http://{url}/v2/{repo}/manifests/{digest}"));
    }

    let url = urls
        .choose(&mut rand::rng())
        .context("Unable to select url for manifest")?;

    let req = client.get(url);

    let req = match &state.config.token_server {
        Some(token_server) => {
            let token = issue_token(
                &token_server,
                vec![Access {
                    type_: "repository".to_string(),
                    name: repo.to_string(),
                    actions: [Action::Pull].into_iter().collect(),
                }],
            )?
            .token;
            req.header("Authorization", format!("Bearer {token}"))
        }
        None => req,
    };

    let mut resp = req.send().await?.error_for_status()?;

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

    if manifest.digest != download_digest {
        debug!("Mirroring: Download of {url} complete but wrong digest: {download_digest}");
        bail!("Output corrupt");
    }

    debug!("Mirroring: Download has correct hash ({download_digest} vs {digest})");

    let storage_path = state.get_manifest_path(&manifest.digest);

    let parent = storage_path
        .parent()
        .context("Couldn't find parent directory")?;
    tokio::fs::create_dir_all(parent).await?;

    tokio::fs::rename(file_name, storage_path).await?;

    state.manifest_downloaded(&manifest.digest).await?;

    Ok(())
}

async fn ensure_mirrored(state: &RegistryState, client: &Client) -> Result<()> {
    for blob in state.get_missing_blobs().await? {
        if let Err(e) = download_blob(&blob, &state, &client).await {
            error!("Failed to mirror blob {}: {e:?}", blob.digest);
        };
    }

    for manifest in state.get_missing_manifests().await? {
        if let Err(e) = download_manifest(&manifest, &state, &client).await {
            error!("Failed to mirror blob {}: {e:?}", manifest.digest);
        };
    }

    Ok(())
}

fn notifications(state: Arc<RegistryState>) -> impl tokio_stream::Stream<Item = Notification> {
    futures::stream::unfold(state, |state| async {
        match state.client.listen().await {
            Ok(n) => Some((n, state)),
            Err(e) => {
                eprintln!("Listen error: {}", e);
                // Decide whether to continue or stop stream
                Some((Notification::Tick, state))
            }
        }
    })
}

pub(crate) fn start_mirror(
    tasks: &mut JoinSet<Result<()>>,
    state: Arc<RegistryState>,
) -> Result<()> {
    if state.config.nodes.len() <= 1 {
        debug!("not starting mirror as not enough nodes");
        return Ok(());
    }

    let startup_stream = futures::stream::once(async { Notification::Tick }).boxed();

    let periodic_stream = IntervalStream::new(interval(Duration::from_secs(30)))
        .map(|_| Notification::Tick)
        .boxed();

    let notification_stream = notifications(state.clone()).boxed();

    let mut event_stream =
        futures::stream::select_all(vec![startup_stream, periodic_stream, notification_stream]);

    let client = reqwest::ClientBuilder::new().build()?;

    tasks.spawn(async move {
        while let Some(trigger) = event_stream.next().await {
            info!("Mirroring: Reconciliation trigger: {:?}", trigger);

            ensure_mirrored(&state, &client).await?;
        }

        Ok(())
    });

    Ok(())
}
