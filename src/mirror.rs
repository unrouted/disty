use crate::{
    machine::Machine,
    types::{Digest, RegistryAction, RegistryState, RepositoryName},
};
use chrono::Utc;
use log::{debug, warn};
use pyo3::{
    prelude::*,
    types::{self, PyDict, PyTuple},
};
use std::str::FromStr;
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
    Retry { request: MirrorRequest },
    Success { action: RegistryAction },
    None,
}

impl MirrorRequest {
    pub fn success(&self, location: String) -> MirrorResult {
        let action = match self {
            MirrorRequest::Blob { digest } => RegistryAction::BlobStored {
                timestamp: Utc::now(),
                digest: digest.clone(),
                location,
                user: String::from("$internal"),
            },
            MirrorRequest::Manifest { digest } => RegistryAction::ManifestStored {
                timestamp: Utc::now(),
                digest: digest.clone(),
                location,
                user: String::from("$internal"),
            },
        };

        MirrorResult::Success { action }
    }
}

async fn do_transfer(
    images_directory: &str,
    state: Arc<RegistryState>,
    machine: Arc<Machine>,
    client: reqwest::Client,
    request: MirrorRequest,
) -> MirrorResult {
    let digest = match request {
        MirrorRequest::Blob { ref digest } => {
            match state.get_blob(&RepositoryName::from_str("*").unwrap(), digest) {
                Some(blob) => {
                    if blob.repositories.is_empty() {
                        debug!("Mirroring: {digest:?}: Digest pending deletion; nothing to do");
                        return MirrorResult::None;
                    }
                    if blob.locations.contains(&machine.identifier) {
                        debug!(
                            "Mirroring: {digest:?}: Already downloaded by this node; nothing to do"
                        );
                        return MirrorResult::None;
                    }

                    digest
                }
                None => {
                    debug!("Mirroring: {digest:?}: missing from graph; nothing to mirror");
                    return MirrorResult::None;
                }
            }
        }
        MirrorRequest::Manifest { ref digest } => {
            match state.get_manifest(&RepositoryName::from_str("*").unwrap(), digest) {
                Some(manifest) => {
                    if manifest.repositories.is_empty() {
                        debug!("Mirroring: {digest:?}: Digest pending deletion; nothing to do");
                        return MirrorResult::None;
                    }
                    if manifest.locations.contains(&machine.identifier) {
                        debug!(
                            "Mirroring: {digest:?}: Already downloaded by this node; nothing to do"
                        );
                        return MirrorResult::None;
                    }

                    digest
                }
                None => {
                    debug!("Mirroring: {digest:?}: missing from graph; nothing to mirror");
                    return MirrorResult::None;
                }
            }
        }
    };

    let url = format!("http://localhost/v2/*/manifests/{digest:?}");

    let mut resp = match client.get(&url).send().await {
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

    let file_name = crate::utils::get_temp_mirror_path(images_directory);

    let mut file = match tokio::fs::File::create(file_name).await {
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

    request.success(String::from("temp"))
}

async fn do_mirroring(
    machine: Arc<Machine>,
    state: Arc<RegistryState>,
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
            let result = do_transfer("", state.clone(), machine.clone(), client, task).await;

            match result {
                MirrorResult::Retry { request } => {
                    requests.insert(request);
                }
                MirrorResult::Success { action } => {
                    info!("{:?}", action);
                }
                MirrorResult::None => {}
            }
        }
    }
}

pub(crate) fn start_mirroring(
    runtime: &Runtime,
    machine: Arc<Machine>,
    state: Arc<RegistryState>,
) -> Sender<MirrorRequest> {
    let (tx, rx) = channel::<MirrorRequest>(500);

    runtime.spawn(do_mirroring(machine, state, rx));

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
            info!("{:?}", entries);
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

/*
class Mirrorer:
    def __init__(self, config, peers, image_directory, identifier, state, send_action):
        self.peers = {}
        for peer in config["peers"].get(list):
            self.peers[peer["name"]] = {
                "address": peer["registry"]["address"],
                "port": peer["registry"]["port"],
            }
        self.image_directory = image_directory
        self.identifier = identifier
        self.state = state
        self.send_action = send_action

        self.token_getter = None
        if config["mirroring"]["realm"].exists():
            self.token_getter = TokenGetter(
                self.session,
                config["mirroring"]["realm"].get(str),
                config["mirroring"]["service"].get(str),
                config["mirroring"]["username"].get(str),
                config["mirroring"]["password"].get(str),
            )

    async def _do_transfer(self, hash, repo, urls, destination):
        if destination.exists():
            logger.debug("%s already exists, not requesting", destination)
            return True

        if not urls:
            logger.debug("No urls for hash %s yet", hash)
            return False

        url = random.choice(urls)
        logger.critical("Starting download from %s to %s", url, destination)

        if not destination.parent.exists():
            os.makedirs(destination.parent)

        # If auth is turned on we need to supply a JWT token
        if self.token_getter:
            token = await self.token_getter.get_token(repo, ["pull"])
            headers["Authorization"] = f"Bearer {token}"

        os.rename(temporary_path, destination)

        return True

    def urls_for_blob(self, hash):
        node = self.state.graph.nodes[hash]

        repo = next(iter(node[ATTR_REPOSITORIES]))

        urls = []

        for location in node[ATTR_LOCATIONS]:
            if location not in self.peers:
                continue

            address = self.peers[location]["address"]
            port = self.peers[location]["port"]
            url = f"http://{address}:{port}"
            urls.append(f"{url}/v2/{repo}/blobs/{hash}")

        return repo, urls

    async def do_download_blob(self, hash, retry_count=0):
        if not self.download_needed(hash):
            return

        try:
            destination = get_blob_path(self.image_directory, hash)
            repo, urls = self.urls_for_blob(hash)
            if await self._do_transfer(hash, repo, urls, destination):
                return

        except asyncio.CancelledError:
            pass

        except Exception:
            logger.exception("Unhandled error whilst processing blob download %r", hash)

    def urls_for_manifest(self, hash):
        node = self.state.graph.nodes[hash]

        repo = next(iter(node[ATTR_REPOSITORIES]))

        urls = []

        for location in node[ATTR_LOCATIONS]:
            if location not in self.peers:
                continue

            address = self.peers[location]["address"]
            port = self.peers[location]["port"]
            url = f"http://{address}:{port}"
            urls.append(f"{url}/v2/{repo}/manifests/{hash}")

        return repo, urls

    async def do_download_manifest(self, hash, retry_count=0):
        if not self.download_needed(hash):
            return

        try:
            destination = get_manifest_path(self.image_directory, hash)
            repo, urls = self.urls_for_manifest(hash)
            if await self._do_transfer(hash, repo, urls, destination):
                return

        except Exception:
            logger.exception("Unhandled error whilst processing blob download %r", hash)

*/
