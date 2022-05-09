use crate::reducer::ReducerDispatch;
use crate::types::Blob;
use crate::types::Manifest;
use crate::types::RegistryAction;
use crate::types::{Digest, RepositoryName};
use crate::webhook::Event;
use pyo3::prelude::*;
use pyo3::types;
use pyo3::types::PyDict;
use pyo3::types::PyTuple;
use pyo3_asyncio::{into_future_with_locals, TaskLocals};
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio::sync::Mutex;

use super::{BlobEntry, ManifestEntry};

pub fn into_future_with_loop(
    event_loop: &PyAny,
    awaitable: &PyAny,
) -> PyResult<impl Future<Output = PyResult<PyObject>> + Send> {
    into_future_with_locals(
        &TaskLocals::new(event_loop).copy_context(event_loop.py())?,
        awaitable,
    )
}

pub struct RegistryState {
    pub machine_identifier: String,
    send_action: PyObject,
    state: PyObject,
    webhook_send: tokio::sync::mpsc::Sender<Event>,
    event_loop: PyObject,
    manifest_waiters: Mutex<HashMap<Digest, Vec<tokio::sync::oneshot::Sender<()>>>>,
    blob_waiters: Mutex<HashMap<Digest, Vec<tokio::sync::oneshot::Sender<()>>>>,
}

impl RegistryState {
    pub fn new(
        state: PyObject,
        send_action: PyObject,
        webhook_send: tokio::sync::mpsc::Sender<Event>,
        machine_identifier: String,
        event_loop: PyObject,
    ) -> RegistryState {
        RegistryState {
            state,
            send_action,
            webhook_send,
            machine_identifier,
            event_loop,
            manifest_waiters: Mutex::new(HashMap::new()),
            blob_waiters: Mutex::new(HashMap::new()),
        }
    }

    // FIXME: Call this
    pub fn blob_available(&self, digest: &Digest) {
        let mut waiters = self.blob_waiters.blocking_lock();

        if let Some(blobs) = waiters.remove(digest) {
            for sender in blobs {
                if sender.send(()).is_err() {
                    warn!("Some blob waiters may have failed: {digest}");
                }
            }
        }
    }

    pub async fn wait_for_blob(&self, digest: &Digest) {
        let mut waiters = self.blob_waiters.lock().await;

        if let Some(blob) = self.get_blob_directly(digest) {
            if blob.locations.contains(&self.machine_identifier) {
                // Blob already exists at this endpoint, no need to wait
                return;
            }
        }

        // FIXME: There is a tiny race that we need to fix after registry state is rust native
        // Can the registry state update already be in flight so we won't get a tx but the blob
        // store won't be up to date yet?
        // We can be certain of this when the whole struct is in rust and we can wrap it in a lock.

        let (tx, rx) = oneshot::channel::<()>();

        let values = waiters.entry(digest.clone()).or_insert_with(|| vec![]);
        values.push(tx);

        match rx.await {
            Ok(_) => return,
            Err(err) => {
                warn!("Failure whilst waiting for blob to be downloaded: {digest}: {err}");
                return;
            }
        }
    }

    // FIXME: Call this
    pub fn manifest_available(&self, digest: &Digest) {
        let mut waiters = self.manifest_waiters.blocking_lock();

        if let Some(manifests) = waiters.remove(digest) {
            for sender in manifests {
                if sender.send(()).is_err() {
                    warn!("Some manifest waiters may have failed: {digest}");
                }
            }
        }
    }

    pub async fn wait_for_manifest(&self, digest: &Digest) {
        let mut waiters = self.manifest_waiters.lock().await;

        if let Some(manifest) = self.get_manifest_directly(digest) {
            if manifest.locations.contains(&self.machine_identifier) {
                // manifest already exists at this endpoint, no need to wait
                return;
            }
        }

        // FIXME: There is a tiny race that we need to fix after registry state is rust native
        // Can the registry state update already be in flight so we won't get a tx but the manifest
        // store won't be up to date yet?
        // We can be certain of this when the whole struct is in rust and we can wrap it in a lock.

        let (tx, rx) = oneshot::channel::<()>();

        let values = waiters.entry(digest.clone()).or_insert_with(|| vec![]);
        values.push(tx);

        match rx.await {
            Ok(_) => return,
            Err(err) => {
                warn!("Failure whilst waiting for manifest to be downloaded: {digest}: {err}");
                return;
            }
        }
    }

    pub async fn send_webhook(&self, event: Event) -> bool {
        matches!(self.webhook_send.send(event).await, Ok(_))
    }

    pub async fn send_actions(&self, actions: Vec<RegistryAction>) -> bool {
        let result = Python::with_gil(|py| {
            let event_loop = self.event_loop.as_ref(py);

            into_future_with_loop(
                event_loop,
                self.send_action.call1(py, (actions,))?.as_ref(py),
            )
        });

        match result {
            Ok(result) => match result.await {
                Ok(value) => match Python::with_gil(|py| value.extract(py)) {
                    Ok(value) => value,
                    _ => false,
                },
                _ => false,
            },
            _ => false,
        }
    }

    pub fn is_blob_available(&self, repository: &RepositoryName, hash: &Digest) -> bool {
        Python::with_gil(|py| {
            let retval = self.state.call_method1(
                py,
                "is_blob_available",
                (repository.to_string(), hash.to_string()),
            );
            match retval {
                Ok(inner) => match inner.extract(py) {
                    Ok(extracted) => extracted,
                    _ => false,
                },
                _ => false,
            }
        })
    }

    pub fn get_blob(&self, repository: &RepositoryName, hash: &Digest) -> Option<Blob> {
        Python::with_gil(|py| {
            let retval =
                self.state
                    .call_method1(py, "get_blob", (repository.to_string(), hash.to_string()));
            match retval {
                Ok(inner) => match inner.extract(py) {
                    Ok(extracted) => Some(extracted),
                    _ => None,
                },
                _ => None,
            }
        })
    }

    pub fn get_blob_directly(&self, hash: &Digest) -> Option<Blob> {
        Python::with_gil(|py| {
            let retval = self
                .state
                .call_method1(py, "get_blob_directly", (hash.to_string(),));
            match retval {
                Ok(inner) => match inner.extract(py) {
                    Ok(extracted) => Some(extracted),
                    _ => None,
                },
                _ => None,
            }
        })
    }

    pub fn get_manifest(&self, repository: &RepositoryName, hash: &Digest) -> Option<Manifest> {
        Python::with_gil(|py| {
            let retval = self.state.call_method1(
                py,
                "get_manifest",
                (repository.to_string(), hash.to_string()),
            );
            match retval {
                Ok(inner) => match inner.extract(py) {
                    Ok(extracted) => Some(extracted),
                    _ => None,
                },
                _ => None,
            }
        })
    }

    pub fn get_manifest_directly(&self, hash: &Digest) -> Option<Manifest> {
        Python::with_gil(|py| {
            let retval = self
                .state
                .call_method1(py, "get_manifest_directly", (hash.to_string(),));
            match retval {
                Ok(inner) => match inner.extract(py) {
                    Ok(extracted) => Some(extracted),
                    _ => None,
                },
                _ => None,
            }
        })
    }

    pub fn get_tag(&self, repository: &RepositoryName, tag: &str) -> Option<Digest> {
        Python::with_gil(|py| {
            let retval =
                self.state
                    .call_method1(py, "get_tag", (repository.to_string(), tag.to_string()));
            match retval {
                Ok(inner) => match inner.extract(py) {
                    Ok(extracted) => Some(extracted),
                    _ => None,
                },
                _ => None,
            }
        })
    }

    pub fn get_tags(&self, repository: &RepositoryName) -> Option<Vec<String>> {
        Python::with_gil(|py| {
            let retval = self
                .state
                .call_method1(py, "get_tags", (repository.to_string(),));
            match retval {
                Ok(inner) => match inner.extract(py) {
                    Ok(extracted) => Some(extracted),
                    _ => None,
                },
                _ => None,
            }
        })
    }

    pub fn is_manifest_available(&self, repository: &RepositoryName, hash: &Digest) -> bool {
        Python::with_gil(|py| {
            let retval = self.state.call_method1(
                py,
                "is_manifest_available",
                (repository.to_string(), hash.to_string()),
            );
            match retval {
                Ok(inner) => match inner.extract(py) {
                    Ok(extracted) => extracted,
                    _ => false,
                },
                _ => false,
            }
        })
    }

    pub fn get_orphaned_blobs(&self) -> Vec<BlobEntry> {
        Python::with_gil(|py| {
            let retval = self.state.call_method1(py, "get_orphaned_blobs", ());
            match retval {
                Ok(inner) => match inner.extract(py) {
                    Ok(extracted) => extracted,
                    _ => vec![],
                },
                _ => vec![],
            }
        })
    }

    pub fn get_orphaned_manifests(&self) -> Vec<ManifestEntry> {
        Python::with_gil(|py| {
            let retval = self.state.call_method1(py, "get_orphaned_manifests", ());
            match retval {
                Ok(inner) => match inner.extract(py) {
                    Ok(extracted) => extracted,
                    _ => vec![],
                },
                _ => vec![],
            }
        })
    }

    pub fn dispatch_entries(&self, entries: Vec<crate::reducer::ReducerDispatch>) {
        for entry in &entries {
            match &entry.1 {
                RegistryAction::BlobStored {
                    timestamp: _,
                    digest,
                    location,
                    user: _,
                } => {
                    if location == &self.machine_identifier {
                        self.blob_available(&digest);
                    }
                }
                RegistryAction::ManifestStored {
                    timestamp: _,
                    digest,
                    location,
                    user: _,
                } => {
                    if location == &self.machine_identifier {
                        self.manifest_available(&digest);
                    }
                }
                _ => {}
            }
        }
    }
}

pub(crate) fn add_side_effect(reducers: &PyObject, state: Arc<RegistryState>) {
    Python::with_gil(|py| {
        let dispatch_entries = move |args: &PyTuple, _kwargs: Option<&PyDict>| -> PyResult<_> {
            let entries: Vec<ReducerDispatch> = args.get_item(1)?.extract()?;
            state.dispatch_entries(entries);
            Ok(true)
        };
        let dispatch_entries = types::PyCFunction::new_closure(dispatch_entries, py).unwrap();

        let result = reducers.call_method1(py, "add_side_effects", (dispatch_entries,));

        if result.is_err() {
            panic!("Boot failure: Could not registry state side effects")
        }
    })
}
