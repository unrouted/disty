use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use super::{Digest, Manifest};

pub struct ManifestEntry {
    pub digest: Digest,
    pub manifest: Manifest,
}

impl FromPyObject<'_> for ManifestEntry {
    fn extract(dict: &'_ PyAny) -> PyResult<Self> {
        let digest: Digest = match dict.get_item("digest") {
            Ok(value) => match value.extract() {
                Ok(extracted) => extracted,
                _ => return PyResult::Err(PyValueError::new_err("Extraction of 'digest' failed")),
            },
            _ => return PyResult::Err(PyValueError::new_err("Key 'digest' missing")),
        };

        let manifest: Manifest = match dict.get_item("manifest") {
            Ok(value) => match value.extract() {
                Ok(extracted) => extracted,
                _ => {
                    return PyResult::Err(PyValueError::new_err("Extraction of 'manifest' failed"))
                }
            },
            _ => return PyResult::Err(PyValueError::new_err("Key 'manifest' missing")),
        };

        Ok(ManifestEntry { digest, manifest })
    }
}
