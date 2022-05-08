use crate::config::Configuration;
use crate::mint::Mint;
use crate::{
    machine::Machine,
    types::{Digest, RegistryAction, RegistryState},
};
use chrono::Utc;
use log::{debug, warn};
use pyo3::{
    prelude::*,
    types::{self, PyDict, PyTuple},
};
use rand::seq::SliceRandom;
use std::path::PathBuf;
use std::{collections::HashSet, sync::Arc};
use tokio::{
    io::AsyncWriteExt,
    sync::mpsc::{channel, Receiver, Sender},
};
use tokio::{runtime::Runtime, select};

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

    pub fn storage_path(&self, images_directory: &str) -> PathBuf {
        match self {
            MirrorRequest::Blob { digest } => crate::utils::get_blob_path(images_directory, digest),
            MirrorRequest::Manifest { digest } => {
                crate::utils::get_manifest_path(images_directory, digest)
            }
        }
    }
}

async fn do_transfer(
    config: Configuration,
    state: Arc<RegistryState>,
    machine: Arc<Machine>,
    mint: Mint,
    client: reqwest::Client,
    request: MirrorRequest,
) -> MirrorResult {
    let (digest, repository, locations, object_type) = match request {
        MirrorRequest::Blob { ref digest } => match state.get_blob_directly(digest) {
            Some(blob) => {
                let repository = match blob.repositories.iter().next() {
                    Some(repository) => repository.clone(),
                    None => {
                        debug!("Mirroring: {digest:?}: Digest pending deletion; nothing to do");
                        return MirrorResult::None;
                    }
                };
                if blob.locations.contains(&machine.identifier) {
                    debug!("Mirroring: {digest:?}: Already downloaded by this node; nothing to do");
                    return MirrorResult::None;
                }

                (digest, repository, blob.locations, "blobs")
            }
            None => {
                debug!("Mirroring: {digest:?}: missing from graph; nothing to mirror");
                return MirrorResult::None;
            }
        },
        MirrorRequest::Manifest { ref digest } => match state.get_manifest_directly(digest) {
            Some(manifest) => {
                let repository = match manifest.repositories.iter().next() {
                    Some(repository) => repository.clone(),
                    None => {
                        debug!("Mirroring: {digest:?}: Digest pending deletion; nothing to do");
                        return MirrorResult::None;
                    }
                };
                if manifest.locations.contains(&machine.identifier) {
                    debug!("Mirroring: {digest:?}: Already downloaded by this node; nothing to do");
                    return MirrorResult::None;
                }

                (digest, repository, manifest.locations, "manifests")
            }
            None => {
                debug!("Mirroring: {digest:?}: missing from graph; nothing to mirror");
                return MirrorResult::None;
            }
        },
    };

    let mut urls = vec![];
    for peer in &config.peers {
        if !locations.contains(&peer.name) {
            continue;
        }

        let address = &peer.registry.address;
        let port = &peer.registry.port;

        let url = format!("http://{address}:{port}/v2/{repository}/{object_type}/{digest:?}");
        urls.push(url);
    }

    let url = match urls.choose(&mut rand::thread_rng()) {
        Some(url) => url,
        None => {
            debug!("Mirroring: {digest:?}: Failed to pick a node to mirror from");
            return MirrorResult::None;
        }
    };

    let builder = match mint.enrich_request(client.get(url), repository).await {
        Ok(builder) => builder,
        Err(err) => {
            warn!("Mirroring: Unable to fetch {url} as minting failed: {err}");
            return MirrorResult::Retry { request };
        }
    };

    let mut resp = match builder.send().await {
        Ok(resp) => resp,
        Err(err) => {
            warn!("Mirroring: Unable to fetch {url}: {err}");
            return MirrorResult::Retry { request };
        }
    };

    let status_code = resp.status();

    if status_code != reqwest::StatusCode::OK {
        warn!("Mirroring: Unable to fetch {url}: {status_code}");
        return MirrorResult::Retry { request };
    }

    let file_name = crate::utils::get_temp_mirror_path(&config.storage);

    let mut file = match tokio::fs::File::create(&file_name).await {
        Ok(file) => file,
        Err(err) => {
            warn!("Mirroring: Failed creating output file for {url}: {err}");
            return MirrorResult::Retry { request };
        }
    };

    let mut hasher = ring::digest::Context::new(&ring::digest::SHA256);

    loop {
        match resp.chunk().await {
            Ok(Some(chunk)) => {
                if let Err(err) = file.write(&chunk).await {
                    warn!("Mirroring: Failed write output chunk for {url}: {err}");
                    return MirrorResult::Retry { request };
                }

                hasher.update(&chunk);
            }
            Ok(None) => break,
            Err(err) => {
                warn!("Mirroring: Failed reading chunk for {url}: {err}");
                return MirrorResult::Retry { request };
            }
        };
    }

    if let Err(err) = file.flush().await {
        warn!("Mirroring: Failed to flush output file for {url}: {err}");
        return MirrorResult::Retry { request };
    }

    let download_digest = Digest::from_sha256(&hasher.finish());

    if digest != &download_digest {
        warn!("Mirroring: Download of {url} complete but wrong digest: {download_digest}");
        return MirrorResult::Retry { request };
    }

    let storage_path = request.storage_path(&config.storage);
    if let Err(err) = tokio::fs::rename(file_name, storage_path).await {
        warn!("Mirroring: Failed to store file for {url}: {err}");
        return MirrorResult::Retry { request };
    }

    request.success(String::from("temp"))
}

async fn do_mirroring(
    machine: Arc<Machine>,
    state: Arc<RegistryState>,
    mint: Mint,
    configuration: Configuration,
    mut rx: Receiver<MirrorRequest>,
) {
    let client = reqwest::Client::builder()
        .user_agent("distribd/mirror")
        .build()
        .unwrap();

    let mut requests = HashSet::<MirrorRequest>::new();

    loop {
        select! {
            _ = tokio::time::sleep(core::time::Duration::from_secs(10)) => {},
            Some(request) = rx.recv() => {requests.insert(request);}
        };

        // FIXME: Ideally we'd have some worker pool here are download a bunch
        // of objects in parallel.

        let tasks: Vec<MirrorRequest> = requests.drain().collect();
        for task in tasks {
            let client = client.clone();
            let result = do_transfer(
                configuration.clone(),
                state.clone(),
                machine.clone(),
                mint.clone(),
                client,
                task,
            )
            .await;

            match result {
                MirrorResult::Retry { request } => {
                    requests.insert(request);
                }
                MirrorResult::Success { action, request } => {
                    if !state.send_actions(vec![action]).await {
                        requests.insert(request);
                    };
                }
                MirrorResult::None => {}
            }
        }
    }
}

pub(crate) fn start_mirroring(
    runtime: &Runtime,
    config: Configuration,
    machine: Arc<Machine>,
    state: Arc<RegistryState>,
) -> Sender<MirrorRequest> {
    let (tx, rx) = channel::<MirrorRequest>(500);

    let mint = Mint::new(config.mirroring.clone());

    runtime.spawn(do_mirroring(machine, state, mint, config, rx));

    tx
}

fn dispatch_entries(entries: Vec<RegistryAction>, tx: Sender<MirrorRequest>) {
    for entry in &entries {
        match entry {
            RegistryAction::BlobStored {
                timestamp: _,
                digest,
                location: _,
                user: _,
            } => {
                tx.blocking_send(MirrorRequest::Blob {
                    digest: digest.clone(),
                })
                .unwrap();
            }
            RegistryAction::ManifestStored {
                timestamp: _,
                digest,
                location: _,
                user: _,
            } => {
                tx.blocking_send(MirrorRequest::Manifest {
                    digest: digest.clone(),
                })
                .unwrap();
            }
            _ => {}
        }
    }
}

pub(crate) fn add_side_effect(reducers: PyObject, tx: Sender<MirrorRequest>) {
    Python::with_gil(|py| {
        let dispatch_entries = move |args: &PyTuple, _kwargs: Option<&PyDict>| -> PyResult<_> {
            let entries: Vec<RegistryAction> = args.get_item(1)?.extract()?;
            info!("side_effect: {:?}", entries);
            dispatch_entries(entries, tx.clone());
            Ok(true)
        };
        let dispatch_entries = types::PyCFunction::new_closure(dispatch_entries, py).unwrap();

        let result = reducers.call_method1(py, "add_side_effects", (dispatch_entries,));

        if result.is_err() {
            panic!("Boot failure: Could not setup mirroring side effects")
        }
    })
}
