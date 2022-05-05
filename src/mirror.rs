use crate::{
    machine::Machine,
    types::{Digest, RegistryAction, RegistryState, RepositoryName},
};
use log::debug;
use pyo3::{
    prelude::*,
    types::{self, PyDict, PyTuple},
};
use reqwest;
use std::str::FromStr;
use std::{collections::HashSet, sync::Arc};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::{runtime::Runtime, select};

async fn do_mirroring(machine: Arc<Machine>, state: Arc<RegistryState>, mut rx: Receiver<Digest>) {
    let client = reqwest::Client::builder()
        .user_agent("distribd/mirror")
        .build()
        .unwrap();

    let mut digests = HashSet::<Digest>::new();

    loop {
        select! {
            _ = tokio::time::sleep(core::time::Duration::from_secs(10)) => {},
            Some(digest) = rx.recv() => {digests.insert(digest); ()}
        };

        digests.retain(|digest| {
            match state.get_blob(&RepositoryName::from_str("MEH").unwrap(), &digest) {
                Some(blob) => {
                    if blob.locations.contains(&machine.identifier) {
                        debug!(
                            "Mirroring: {digest}: Already downloaded by this node; nothing to do"
                        );
                        return false;
                    }
                }
                None => {
                    debug!("Mirroring: {digest}: missing from graph; nothing to mirror");
                    return false;
                }
            }

            true
        });
    }
}

pub(crate) fn start_mirroring(
    runtime: &Runtime,
    machine: Arc<Machine>,
    state: Arc<RegistryState>,
) -> Sender<Digest> {
    let (tx, rx) = channel::<Digest>(500);

    runtime.spawn(do_mirroring(machine, state, rx));

    tx
}

fn dispatch_entries(entries: Vec<RegistryAction>, tx: Sender<Digest>) {
    for entry in &entries {
        match entry {
            RegistryAction::BlobStored {
                timestamp,
                digest,
                location,
                user,
            } => {
                tx.blocking_send(digest.clone()).unwrap();
            }
            RegistryAction::ManifestStored {
                timestamp,
                digest,
                location,
                user,
            } => {
                tx.blocking_send(digest.clone()).unwrap();
            }
            _ => {}
        }
    }
}

pub(crate) fn add_side_effect(reducers: PyObject, tx: Sender<Digest>) {
    Python::with_gil(|py| {
        let dispatch_entries = move |args: &PyTuple, _kwargs: Option<&PyDict>| -> PyResult<_> {
            let entries: Vec<RegistryAction> = args.get_item(1)?.extract()?;
            info!("{:?}", entries);
            dispatch_entries(entries, tx.clone());
            Ok(true)
        };
        let dispatch_entries = types::PyCFunction::new_closure(dispatch_entries, py).unwrap();

        let result = reducers.call_method1(py, "add_side_effects", (dispatch_entries,));

        match result {
            Err(_) => panic!("Boot failure: Could not setup mirroring side effects"),
            _ => {}
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

        self._futures = {}

    async def wait_for_blob(self, digest):
        if self.identifier in self.state.graph.nodes[digest][ATTR_LOCATIONS]:
            return get_blob_path(self.image_directory, digest)

        fut = asyncio.Future()
        self._futures.setdefault(digest, []).append(fut)
        logger.warning("Waiting for %s", digest)
        return await fut

    async def close(self):
        await self.pool.close()
        await self.session.close()

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

        temporary_path = self.image_directory / "uploads" / str(uuid.uuid4())
        if not temporary_path.parent.exists():
            os.makedirs(temporary_path.parent)

        digest = hashlib.sha256()

        headers = {}

        # If auth is turned on we need to supply a JWT token
        if self.token_getter:
            token = await self.token_getter.get_token(repo, ["pull"])
            headers["Authorization"] = f"Bearer {token}"

        async with self.session.get(url, headers=headers) as resp:
            if resp.status != 200:
                logger.error("Failed to retrieve: %s, status %s", url, resp.status)
                return False
            async with AIOFile(temporary_path, "wb") as fp:
                writer = Writer(fp)
                chunk = await resp.content.read(1024 * 1024)
                while chunk:
                    await writer(chunk)
                    digest.update(chunk)
                    chunk = await resp.content.read(1024 * 1024)
                await fp.fsync()

        mirrored_hash = "sha256:" + digest.hexdigest()

        if mirrored_hash != hash:
            os.unlink(temporary_path)
            return False

        os.rename(temporary_path, destination)

        for fut in self._futures.get(hash, []):
            fut.set_result(destination)

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
                await self.send_action(
                    [
                        {
                            "type": RegistryActions.BLOB_STORED,
                            "timestamp": datetime.datetime.now(
                                datetime.timezone.utc
                            ).isoformat(),
                            "hash": hash,
                            "location": self.identifier,
                            "user": "$internal",
                        }
                    ]
                )
                return

        except asyncio.CancelledError:
            pass

        except Exception:
            logger.exception("Unhandled error whilst processing blob download %r", hash)

        logger.info("Scheduling retry for blob download %s", hash)
        loop = asyncio.get_event_loop()
        loop.call_later(
            retry_count,
            lambda: self.pool.spawn(
                self.do_download_blob(hash, retry_count=retry_count + 1)
            ),
        )

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
                await self.send_action(
                    [
                        {
                            "type": RegistryActions.MANIFEST_STORED,
                            "timestamp": datetime.datetime.now(
                                datetime.timezone.utc
                            ).isoformat(),
                            "hash": hash,
                            "location": self.identifier,
                            "user": "$internal",
                        }
                    ]
                )
                return

        except Exception:
            logger.exception("Unhandled error whilst processing blob download %r", hash)

        logger.info("Scheduling retry for manifest download %s", hash)
        loop = asyncio.get_event_loop()
        loop.call_later(
            retry_count,
            lambda: self.pool.spawn(
                self.do_download_manifest(hash, retry_count=retry_count + 1)
            ),
        )

    def download_needed(self, hash):
        if hash not in self.state.graph.nodes:
            # It was deleted or never existed in the first place
            return False

        node = self.state.graph.nodes[hash]

        if len(node[ATTR_REPOSITORIES]) == 0:
            # It's pending deletion
            return False

        if len(node[ATTR_LOCATIONS]) == 0:
            # It's not available for download anywhere
            return False

        if self.identifier in node[ATTR_LOCATIONS]:
            # Already downloaded it
            return False

        return True

    def dispatch_entries(self, state, entries):
        manifests = set()
        blobs = set()

        for term, entry in entries:
            if "type" not in entry:
                continue

            if entry["type"] == RegistryActions.BLOB_STORED:
                hash = entry["hash"]
                if self.download_needed(hash):
                    blobs.add(hash)

            elif entry["type"] == RegistryActions.MANIFEST_STORED:
                hash = entry["hash"]
                if self.download_needed(hash):
                    manifests.add(hash)

        for blob in blobs:
            self.pool.spawn(self.do_download_blob(blob))

        for manifest in manifests:
            self.pool.spawn(self.do_download_manifest(manifest))
*/
