use super::{Digest, RepositoryName};

use chrono::{DateTime, Utc};

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

#[derive(Debug, Clone)]
pub struct Manifest {
    pub size: Option<u64>,
    pub content_type: Option<String>,
    pub dependencies: Option<Vec<Digest>>,
    pub repositories: Vec<RepositoryName>,
    pub created: DateTime<Utc>,
    pub updated: DateTime<Utc>,
}

impl FromPyObject<'_> for Manifest {
    fn extract(dict: &'_ PyAny) -> PyResult<Self> {
        // FIXME: This should send nice errors back to python if any of the unwraps fail...
        let size: u64 = dict.get_item("size").unwrap().extract().unwrap();
        let content_type: String = dict.get_item("content_type").unwrap().extract().unwrap();

        let dependencies = Vec::new();

        let repositories: Vec<RepositoryName> = match dict.get_item("repositories") {
            Ok(value) => match value.extract() {
                Ok(extracted) => extracted,
                _ => {
                    return PyResult::Err(PyValueError::new_err(
                        "Extraction of 'repositories' failed",
                    ))
                }
            },
            _ => return PyResult::Err(PyValueError::new_err("Key 'repositories' missing")),
        };

        let created: pyo3_chrono::NaiveDateTime = match dict.get_item("created") {
            Ok(value) => match value.extract() {
                Ok(extracted) => extracted,
                _ => return PyResult::Err(PyValueError::new_err("Extraction of 'created' failed")),
            },
            _ => return PyResult::Err(PyValueError::new_err("Key 'created' missing")),
        };

        let updated: pyo3_chrono::NaiveDateTime = match dict.get_item("updated") {
            Ok(value) => match value.extract() {
                Ok(extracted) => extracted,
                _ => return PyResult::Err(PyValueError::new_err("Extraction of 'updated' failed")),
            },
            _ => return PyResult::Err(PyValueError::new_err("Key 'updated' missing")),
        };

        Ok(Manifest {
            size: Some(size),
            content_type: Some(content_type),
            dependencies: Some(dependencies),
            repositories,
            created: DateTime::from_utc(created.into(), Utc),
            updated: DateTime::from_utc(updated.into(), Utc),
        })
    }
}
