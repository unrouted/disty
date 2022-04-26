use super::Digest;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

#[derive(Debug, Clone)]
pub struct Blob {
    pub size: Option<u64>,
    pub content_type: Option<String>,
    pub dependencies: Option<Vec<Digest>>,
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

        Ok(Blob {
            size,
            content_type,
            dependencies: Some(dependencies),
        })
    }
}
