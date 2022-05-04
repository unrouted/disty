use std::collections::HashSet;

use super::{Digest, RepositoryName};

use chrono::{DateTime, Utc};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

#[derive(Debug, Clone)]
pub struct Blob {
    pub size: Option<u64>,
    pub content_type: Option<String>,
    pub dependencies: Option<Vec<Digest>>,
    pub repositories: HashSet<RepositoryName>,
    pub locations: HashSet<String>,
    pub created: DateTime<Utc>,
    pub updated: DateTime<Utc>,
}

impl FromPyObject<'_> for Blob {
    fn extract(dict: &'_ PyAny) -> PyResult<Self> {
        // FIXME: This should send nice errors back to python if any of the unwraps fail...
        let size = match dict.get_item("size") {
            Ok(value) => match value.extract() {
                Ok(extracted) => Some(extracted),
                _ => None,
            },
            _ => None,
        };

        let content_type = match dict.get_item("content_type") {
            Ok(value) => match value.extract() {
                Ok(extracted) => Some(extracted),
                _ => None,
            },
            _ => None,
        };

        let dependencies = match dict.get_item("dependencies") {
            Ok(pydeps) => {
                let mut dependencies: Vec<Digest> = Vec::new();
                match pydeps.iter() {
                    Ok(iterator) => {
                        for dep in iterator {
                            match dep {
                                Ok(dep) => {
                                    dependencies.push(dep.extract().unwrap());
                                }
                                _ => {
                                    return PyResult::Err(PyValueError::new_err(
                                        "Could not convert list item to digest",
                                    ))
                                }
                            }
                        }
                        dependencies
                    }
                    _ => vec![],
                }
            }
            _ => vec![],
        };

        let repositories: HashSet<RepositoryName> = match dict.get_item("repositories") {
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

        let locations: HashSet<String> = match dict.get_item("locations") {
            Ok(value) => match value.extract() {
                Ok(extracted) => extracted,
                _ => {
                    return PyResult::Err(PyValueError::new_err("Extraction of 'locations' failed"))
                }
            },
            _ => return PyResult::Err(PyValueError::new_err("Key 'locations' missing")),
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

        Ok(Blob {
            size,
            content_type,
            dependencies: Some(dependencies),
            repositories,
            locations,
            created: DateTime::from_utc(created.into(), Utc),
            updated: DateTime::from_utc(updated.into(), Utc),
        })
    }
}
