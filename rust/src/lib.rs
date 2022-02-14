#[macro_use]
extern crate rocket;

mod headers;
mod registry;
mod responses;
mod types;
mod utils;

use pyo3::prelude::*;

#[pyfunction]
fn start_registry_service(registry_state: PyObject, repository_path: String) -> () {
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
