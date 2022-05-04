use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use super::{Blob, Digest};

pub struct BlobEntry {
    pub digest: Digest,
    pub blob: Blob,
}

impl FromPyObject<'_> for BlobEntry {
    fn extract(dict: &'_ PyAny) -> PyResult<Self> {
        let digest: Digest = match dict.get_item("digest") {
            Ok(value) => match value.extract() {
                Ok(extracted) => extracted,
                _ => return PyResult::Err(PyValueError::new_err("Extraction of 'digest' failed")),
            },
            _ => return PyResult::Err(PyValueError::new_err("Key 'digest' missing")),
        };

        let blob: Blob = match dict.get_item("blob") {
            Ok(value) => match value.extract() {
                Ok(extracted) => extracted,
                _ => return PyResult::Err(PyValueError::new_err("Extraction of 'blob' failed")),
            },
            _ => return PyResult::Err(PyValueError::new_err("Key 'blob' missing")),
        };

        Ok(BlobEntry { digest, blob })
    }
}
