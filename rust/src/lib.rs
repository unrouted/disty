#[macro_use]
extern crate rocket;

mod headers;
mod registry;
mod responses;
mod types;
mod utils;
mod views;

use pyo3::prelude::*;

fn create_dir(parent_dir: &String, child_dir: &str) -> bool {
    let path = std::path::PathBuf::from(&parent_dir).join(child_dir);
    if !path.exists() {
        return match std::fs::create_dir_all(path) {
            Ok(_) => true,
            _ => false,
        };
    }
    true
}

#[pyfunction]
fn start_registry_service(registry_state: PyObject, repository_path: String) -> bool {
    if !create_dir(&repository_path, "uploads")
        || !create_dir(&repository_path, "manifests")
        || !create_dir(&repository_path, "blobs")
    {
        return false;
    }

    let runtime = pyo3_asyncio::tokio::get_runtime();
    runtime.spawn(
        rocket::build()
            .manage(crate::types::RegistryState::new(
                registry_state,
                repository_path,
            ))
            .mount("/v2/", crate::registry::routes())
            .launch(),
    );

    true
}

#[pymodule]
fn distribd_rust(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(start_registry_service, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
