#[macro_use]
extern crate rocket;

mod config;
mod extractor;
mod garbage;
mod headers;
mod machine;
mod mint;
mod mirror;
mod prometheus;
mod registry;
mod types;
mod utils;
mod views;
mod webhook;

use std::sync::Arc;

use machine::Machine;
use pyo3::prelude::*;
use regex::Captures;
use rocket::{fairing::AdHoc, http::uri::Origin};
use webhook::{start_webhook_worker, WebhookConfig};

fn create_dir(parent_dir: &str, child_dir: &str) -> bool {
    let path = std::path::PathBuf::from(&parent_dir).join(child_dir);
    if !path.exists() {
        return matches!(std::fs::create_dir_all(path), Ok(()));
    }
    true
}

pub fn rewrite_urls(url: &str) -> String {
    // /v2/foo/bar/manifests/tagname -> /v2/foo:bar/manifests/tagname

    // FIXME: Make this a static
    let re = regex::Regex::new(r"(^/v2/)(.+)(/(manifests|blobs|tags).*$)").unwrap();

    let result = re.replace(url, |caps: &Captures| {
        let prefix = &caps[1];
        let encoded = urlencoding::encode(&caps[2]).into_owned();
        let suffix = &caps[3];

        format!("{prefix}{encoded}{suffix}")
    });

    result.to_string()
}

#[pyfunction]
fn start_registry_service(
    registry_state: PyObject,
    send_action: PyObject,
    repository_path: String,
    webhooks: Vec<WebhookConfig>,
    machine: PyObject,
    machine_identifier: String,
    reducers: PyObject,
    event_loop: PyObject,
) -> bool {
    if !create_dir(&repository_path, "uploads")
        || !create_dir(&repository_path, "manifests")
        || !create_dir(&repository_path, "blobs")
    {
        return false;
    }

    let config = crate::config::config();

    let mut registry = <prometheus_client::registry::Registry>::default();

    let webhook_send = start_webhook_worker(webhooks, &mut registry);
    let extractor = crate::extractor::Extractor::new();

    let machine = Arc::new(Machine::new(
        &mut registry,
        machine_identifier.clone(),
        machine,
    ));
    let state = Arc::new(crate::types::RegistryState::new(
        registry_state,
        send_action,
        repository_path.clone(),
        webhook_send,
        machine_identifier,
        event_loop,
    ));

    let runtime = pyo3_asyncio::tokio::get_runtime();

    runtime.spawn(crate::garbage::do_garbage_collect(
        machine.clone(),
        state.clone(),
        repository_path.clone(),
    ));

    let tx = crate::mirror::start_mirroring(
        runtime,
        config.clone(),
        machine,
        state.clone(),
        repository_path,
    );
    crate::mirror::add_side_effect(reducers, tx);

    runtime.spawn(
        rocket::build()
            .attach(AdHoc::on_request("URL Rewriter", |req, _| {
                Box::pin(async move {
                    let origin = req.uri().to_string();
                    req.set_uri(Origin::parse_owned(rewrite_urls(&origin)).unwrap());
                })
            }))
            .manage(state)
            .manage(extractor)
            .manage(config.token_server)
            .attach(crate::prometheus::HttpMetrics::new(&mut registry))
            .mount("/v2/", crate::registry::routes())
            .launch(),
    );

    let prometheus_conf = rocket::Config::figment().merge(("port", 7080));

    runtime.spawn(crate::prometheus::configure(rocket::custom(prometheus_conf), registry).launch());

    true
}

#[pymodule]
fn distribd(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(start_registry_service, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rewriting_ruls_middleware() {
        assert_eq!(rewrite_urls("/"), "/");
        assert_eq!(
            rewrite_urls("/v2/foo/manifests/sha256:abcdefgh"),
            "/v2/foo/manifests/sha256:abcdefgh"
        );
        assert_eq!(
            rewrite_urls("/v2/foo/bar/manifests/sha256:abcdefgh"),
            "/v2/foo%2Fbar/manifests/sha256:abcdefgh"
        );
    }
}
