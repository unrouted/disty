use crate::types::{Digest, RepositoryName};
use pyo3::prelude::*;

pub struct RegistryState {
    pub repository_path: String,
    state: PyObject,
}

impl RegistryState {
    pub fn new(state: PyObject, repository_path: String) -> RegistryState {
        RegistryState {
            state: state,
            repository_path: repository_path,
        }
    }

    pub fn check_token(&self, _repository: &RepositoryName, _permission: &String) -> bool {
        true
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
