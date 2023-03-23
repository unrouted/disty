use crate::app::RegistryApp;
use crate::types::{Digest, RegistryAction};
use actix_web::web::Data;
use chrono::Utc;
use rand::seq::SliceRandom;
use std::path::PathBuf;
use std::time::Duration;
use std::{collections::HashSet, sync::Arc};
use tokio::io::AsyncWriteExt;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tracing::debug;

#[derive(Hash, PartialEq, std::cmp::Eq, Debug)]
pub enum MirrorRequest {
    Blob { digest: Digest },
    Manifest { digest: Digest },
}

pub enum MirrorResult {
    Retry {
        request: MirrorRequest,
    },
    Success {
        request: MirrorRequest,
        action: RegistryAction,
    },
    None,
}

impl MirrorRequest {
    pub fn success(self, location: String) -> MirrorResult {
        let action = match self {
            MirrorRequest::Blob { ref digest } => RegistryAction::BlobStored {
                timestamp: Utc::now(),
                digest: digest.clone(),
                location,
                user: String::from("$internal"),
            },
            MirrorRequest::Manifest { ref digest } => RegistryAction::ManifestStored {
                timestamp: Utc::now(),
                digest: digest.clone(),
                location,
                user: String::from("$internal"),
            },
        };

        MirrorResult::Success {
            request: self,
            action,
        }
    }

    pub fn storage_path(&self, app: &Arc<RegistryApp>) -> PathBuf {
        match self {
            MirrorRequest::Blob { digest } => app.get_blob_path(digest),
            MirrorRequest::Manifest { digest } => app.get_manifest_path(digest),
        }
    }
}

async fn do_transfer(
    app: Data<RegistryApp>,
    client: reqwest::Client,
    request: MirrorRequest,
) -> MirrorResult {
    let (digest, locations, object_type) = match request {
        MirrorRequest::Blob { ref digest } => match app.get_blob(digest) {
            Some(blob) => {
                if blob.locations.contains(&app.config.identifier) {
                    debug!("Mirroring: {digest:?}: Already downloaded by this node; nothing to do. {:?} {:?}", blob.locations, app.config.identifier);
                    return MirrorResult::None;
                }

                (digest, blob.locations, "blobs")
            }
            None => {
                debug!("Mirroring: {digest:?}: missing from graph; nothing to mirror");
                return MirrorResult::None;
            }
        },
        MirrorRequest::Manifest { ref digest } => match app.get_manifest(digest) {
            Some(manifest) => {
                if manifest.locations.contains(&app.config.identifier) {
                    debug!("Mirroring: {digest:?}: Already downloaded by this node; nothing to do");
                    return MirrorResult::None;
                }

                (digest, manifest.locations, "manifests")
            }
            None => {
                debug!("Mirroring: {digest:?}: missing from graph; nothing to mirror");
                return MirrorResult::None;
            }
        },
    };

    let mut urls = vec![];
    for peer in &app.config.peers {
        if !locations.contains(&peer.name) {
            continue;
        }

        let address = &peer.raft.address;
        let port = &peer.raft.port;

        let url = format!("http://{address}:{port}/{object_type}/{digest}");
        urls.push(url);
    }

    let url = match urls.choose(&mut rand::thread_rng()) {
        Some(url) => url,
        None => {
            debug!("Mirroring: {digest:?}: Failed to pick a node to mirror from");
            return MirrorResult::None;
        }
    };

    debug!("Mirroring: Will download: {url}");

    let mut resp = match client.get(url).send().await {
        Ok(resp) => resp,
        Err(err) => {
            debug!("Mirroring: Unable to fetch {url}: {err}");
            return MirrorResult::Retry { request };
        }
    };

    let status_code = resp.status();

    if status_code != reqwest::StatusCode::OK {
        debug!("Mirroring: Unable to fetch {url}: {status_code}");
        return MirrorResult::Retry { request };
    }

    let file_name = app.get_temp_mirror_path();

    let mut file = match tokio::fs::File::create(&file_name).await {
        Ok(file) => {
            debug!("Mirroring: {file_name:?}: Created new file for writing");
            file
        }
        Err(err) => {
            debug!("Mirroring: Failed creating output file for {url}: {err}");
            return MirrorResult::Retry { request };
        }
    };

    let mut hasher = ring::digest::Context::new(&ring::digest::SHA256);

    loop {
        match resp.chunk().await {
            Ok(Some(chunk)) => {
                if let Err(err) = file.write_all(&chunk).await {
                    debug!("Mirroring: Failed write output chunk for {url}: {err}");
                    return MirrorResult::Retry { request };
                }

                debug!("Mirroring: Downloaded {} bytes", chunk.len());
                hasher.update(&chunk);
            }
            Ok(None) => {
                debug!("Mirroring: Finished streaming");
                break;
            }
            Err(err) => {
                debug!("Mirroring: Failed reading chunk for {url}: {err}");
                return MirrorResult::Retry { request };
            }
        };
    }

    if let Err(err) = file.flush().await {
        debug!("Mirroring: Failed to flush output file for {url}: {err}");
        return MirrorResult::Retry { request };
    }

    debug!("Mirroring: Output flushed");

    if let Err(err) = file.sync_all().await {
        debug!("Mirroring: Failed to sync_all output file for {url}: {err}");
        return MirrorResult::Retry { request };
    }

    debug!("Mirroring: Output synced");

    drop(file);

    debug!("Mirroring: File handle dropped");

    let download_digest = Digest::from_sha256(&hasher.finish());

    if digest != &download_digest {
        debug!("Mirroring: Download of {url} complete but wrong digest: {download_digest}");
        return MirrorResult::Retry { request };
    }

    debug!("Mirroring: Download has correct hash ({download_digest} vs {digest})");

    if !crate::registry::utils::validate_hash(&file_name, digest).await {
        debug!("Mirroring: Downloaded file for {url} is corrupt");
        return MirrorResult::Retry { request };
    };

    let storage_path = request.storage_path(&app);
    if let Err(err) = tokio::fs::rename(file_name, storage_path).await {
        debug!("Mirroring: Failed to store file for {url}: {err}");
        return MirrorResult::Retry { request };
    }

    debug!("Mirroring: Mirrored {digest}");

    request.success(app.config.identifier.clone())
}

enum MirrorState {
    Changed,
    Timeout,
    Close,
}

#[inline]
async fn wait_for_change(mut subscriber: watch::Receiver<HashSet<Digest>>) -> MirrorState {
    if let Err(result) = subscriber.changed().await {
        tracing::debug!("RecvError: Closing mirror: {:?}", result);
        return MirrorState::Close;
    }

    MirrorState::Changed
}

#[inline]
async fn wait_for_time() -> MirrorState {
    tokio::time::sleep(Duration::from_secs(10)).await;

    MirrorState::Timeout
}

pub(crate) async fn do_miroring(app: Data<RegistryApp>) -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .user_agent("distribd/mirror")
        .build()
        .unwrap();

    let app1 = app.clone();
    let client1 = client.clone();

    tokio::spawn(async move {
        let mut subscriber = app1
            .store
            .state_machine
            .read()
            .unwrap()
            .pending_blobs
            .subscribe();
        let mut pending_blobs = subscriber.borrow_and_update().clone();
        let mut waiters = JoinSet::new();

        waiters.spawn(wait_for_change(subscriber.clone()));
        if !pending_blobs.is_empty() {
            waiters.spawn(wait_for_time());
        }

        while let Some(res) = waiters.join_next().await {
            match res {
                Ok(MirrorState::Timeout | MirrorState::Changed) => {
                    pending_blobs = subscriber.borrow_and_update().clone();
                    for digest in pending_blobs.iter() {
                        let result = do_transfer(
                            app1.clone(),
                            client1.clone(),
                            MirrorRequest::Blob {
                                digest: digest.clone(),
                            },
                        )
                        .await;

                        if let MirrorResult::Success { action, request: _ } = result {
                            if app1.submit(vec![action]).await {
                                debug!("Mirroring: Download logged to raft");
                            }
                        }
                    }
                }
                Ok(MirrorState::Close) => {
                    break;
                }
                Err(err) => {
                    tracing::error!("JoinError during mirror: {:?}", err);
                    break;
                }
            }

            match res {
                Ok(MirrorState::Changed) => {
                    waiters.spawn(wait_for_change(subscriber.clone()));
                }
                Ok(MirrorState::Timeout) => {
                    if !pending_blobs.is_empty() {
                        waiters.spawn(wait_for_time());
                    }
                }
                _ => {}
            }
        }
    });

    let app1 = app;
    let client1 = client;

    tokio::spawn(async move {
        let mut subscriber = app1
            .store
            .state_machine
            .read()
            .unwrap()
            .pending_manifests
            .subscribe();
        let mut pending_manifests = subscriber.borrow_and_update().clone();
        let mut waiters = JoinSet::new();

        waiters.spawn(wait_for_change(subscriber.clone()));
        if !pending_manifests.is_empty() {
            waiters.spawn(wait_for_time());
        }

        while let Some(res) = waiters.join_next().await {
            match res {
                Ok(MirrorState::Timeout | MirrorState::Changed) => {
                    pending_manifests = subscriber.borrow_and_update().clone();
                    for digest in pending_manifests.iter() {
                        let result = do_transfer(
                            app1.clone(),
                            client1.clone(),
                            MirrorRequest::Manifest {
                                digest: digest.clone(),
                            },
                        )
                        .await;

                        if let MirrorResult::Success { action, request: _ } = result {
                            if app1.submit(vec![action]).await {
                                debug!("Mirroring: Download logged to raft");
                            }
                        }
                    }
                }
                Ok(MirrorState::Close) => {
                    break;
                }
                Err(err) => {
                    tracing::error!("JoinError during mirror: {:?}", err);
                    break;
                }
            }

            match res {
                Ok(MirrorState::Changed) => {
                    waiters.spawn(wait_for_change(subscriber.clone()));
                }
                Ok(MirrorState::Timeout) => {
                    if !pending_manifests.is_empty() {
                        waiters.spawn(wait_for_time());
                    }
                }
                _ => {}
            }
        }
    });

    Ok(())
}
