use std::sync::Arc;

use prometheus_client::registry::Registry;
use regex::Captures;
use rocket::{Route, fairing::AdHoc, http::uri::Origin, Rocket, Build};

use crate::{config::Configuration, types::RegistryState, rpc::RpcClient};

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


pub fn routes() -> Vec<Route> {
    routes![
        // Uploads
        crate::views::blobs::uploads::delete::delete,
        crate::views::blobs::uploads::patch::patch,
        crate::views::blobs::uploads::post::post,
        crate::views::blobs::uploads::put::put,
        crate::views::blobs::uploads::get::get,
        // Blobs
        crate::views::blobs::delete::delete,
        crate::views::blobs::get::get,
        // Manifests
        crate::views::manifests::put::put,
        crate::views::manifests::get::get,
        crate::views::manifests::get::get_by_tag,
        crate::views::manifests::delete::delete,
        crate::views::manifests::delete::delete_by_tag,
        // Tags
        crate::views::tags::get::get,
        // Root
        crate::views::get::get,
    ]
}

fn configure(config: Configuration, registry: &mut Registry, state: Arc<RegistryState>, rpc_client: Arc<RpcClient>) -> Rocket<Build> {
    let extractor = crate::extractor::Extractor::new(config.clone());

    let registry_conf = rocket::Config::figment()
        .merge(("port", &config.registry.port))
        .merge(("address", &config.registry.address));

    rocket::custom(registry_conf)
        .attach(AdHoc::on_request("URL Rewriter", |req, _| {
            Box::pin(async move {
                let origin = req.uri().to_string();
                req.set_uri(Origin::parse_owned(rewrite_urls(&origin)).unwrap());
            })
        }))
        .manage(config.clone())
        .manage(state)
        .manage(extractor)
        .manage(rpc_client)
        .attach(crate::prometheus::HttpMetrics::new(registry))
        .mount("/v2/", crate::registry::routes())
}

pub fn launch(config: Configuration, registry: &mut Registry, state: Arc<RegistryState>, rpc_client: Arc<RpcClient>) {
    tokio::spawn(configure(config, registry, state, rpc_client).launch());
}

#[cfg(test)]
mod test {
    use super::*;
    use rocket::local::blocking::Client;

    fn client() -> Client {
        let server = rocket::build().mount("/", super::routes());
        Client::tracked(server).expect("valid rocket instance")
    }

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

    /*
    #[test]
    fn put_sha_query_param_fail() {
        let client = client();
        let response = client
            .put("/REPOSITORY/blobs/uploads/UPLOADID?digest=sha255:hello")
            .dispatch();
        assert_eq!(response.status(), Status::NotFound);
    }
    */

    #[test]
    fn put() {}
}
