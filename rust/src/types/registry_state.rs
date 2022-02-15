use crate::types::RegistryAction;
use crate::types::{Digest, RepositoryName};
use pyo3::prelude::*;
use pyo3_asyncio::tokio::into_future;

pub struct RegistryState {
    pub repository_path: String,
    send_action: PyObject,
    state: PyObject,
}

impl RegistryState {
    pub fn new(state: PyObject, send_action: PyObject, repository_path: String) -> RegistryState {
        RegistryState {
            state,
            send_action,
            repository_path,
        }
    }

    pub fn check_token(&self, _repository: &RepositoryName, _permission: &String) -> bool {
        true
    }

    pub async fn send_actions(&self, actions: Vec<RegistryAction>) -> bool {
        let result =
            Python::with_gil(|py| into_future(self.send_action.call1(py, (actions,))?.as_ref(py)));

        match result {
            Ok(result) => match result.await {
                Ok(value) => match Python::with_gil(|py| value.extract(py)) {
                    Ok(value) => value,
                    _ => false,
                },
                _ => false,
            },
            _ => {
                return false;
            }
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
}
