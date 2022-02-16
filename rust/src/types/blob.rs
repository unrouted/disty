use super::Digest;

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
        let size: u64 = dict.get_item("size").unwrap().extract().unwrap();
        let content_type: String = dict.get_item("content_type").unwrap().extract().unwrap();

        let pydeps = dict.get_item("dependencies").unwrap();
        let mut dependencies = Vec::new();
        for dep in pydeps.iter() {
            dependencies.push(dep.extract().unwrap());
        }

        Ok(Blob {
            size: Some(size),
            content_type: Some(content_type),
            dependencies: Some(dependencies),
        })
    }
}
