mod blobs;
mod get;
mod manifests;
mod tags;
pub(crate) mod utils;

use std::sync::Arc;

use prometheus_client::registry::Registry;
use regex::Captures;
use rocket::{fairing::AdHoc, http::uri::Origin, routes, Build, Rocket, Route};

use crate::{
    app::RegistryApp,
    middleware::prometheus::{HttpMetrics, Port},
    webhook::start_webhook_worker,
};

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
        blobs::uploads::delete::delete,
        blobs::uploads::patch::patch,
        blobs::uploads::post::post,
        blobs::uploads::put::put,
        blobs::uploads::get::get,
        // Blobs
        blobs::delete::delete,
        blobs::get::get,
        // Manifests
        manifests::put::put,
        manifests::get::get,
        manifests::get::get_by_tag,
        manifests::delete::delete,
        manifests::delete::delete_by_tag,
        // Tags
        tags::get::get,
        // Root
        get::get,
    ]
}

fn configure(app: Arc<RegistryApp>, registry: &mut Registry) -> Rocket<Build> {
    let extractor = crate::extractor::Extractor::new(app.settings.clone());
    let webhook_queue = start_webhook_worker(app.settings.webhooks.clone(), registry);

    let registry_conf = rocket::Config::figment()
        .merge(("port", &app.settings.registry.port))
        .merge(("address", &app.settings.registry.address))
        .merge(("log_level", "off"));

    rocket::custom(registry_conf)
        .attach(AdHoc::on_request("Registry URL Rewriter", |req, _| {
            Box::pin(async move {
                let origin = req.uri().to_string();
                req.set_uri(Origin::parse_owned(rewrite_urls(&origin)).unwrap());
            })
        }))
        .manage(app)
        .manage(extractor)
        .manage(webhook_queue)
        .attach(HttpMetrics::new(registry, Port::Registry))
        .mount("/v2/", routes())
}

pub fn launch(app: Arc<RegistryApp>, registry: &mut Registry) {
    tokio::spawn(configure(app, registry).launch());
}

#[cfg(test)]
mod test {
    use crate::{
        config::{Configuration, PeerConfig, PrometheusConfig, RaftConfig, RegistryConfig},
        start_registry_services,
    };

    use super::*;
    use lazy_static::lazy_static;
    use log::debug;
    use reqwest::{
        header::{HeaderMap, CONTENT_TYPE},
        StatusCode, Url,
    };
    use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
    use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
    use serde_json::{json, Value};
    use simple_pool::{ResourcePool, ResourcePoolGuard};
    use tempfile::TempDir;

    lazy_static! {
        static ref IP_ADDRESSES: ResourcePool<String> = {
            let r = ResourcePool::new();

            for i in 1..100 {
                r.append(format!("127.37.0.{i}"));
            }

            r
        };
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

    struct TestInstance {
        url: Url,
        client: ClientWithMiddleware,
        _tempdir: TempDir,
        _address: ResourcePoolGuard<String>,
    }

    async fn configure() -> TestInstance {
        let tempdir = tempfile::tempdir().unwrap();
        let address = IP_ADDRESSES.get().await;

        let config = Configuration {
            identifier: "registry-1".into(),
            registry: RegistryConfig {
                address: address.clone(),
                port: 8000,
            },
            raft: RaftConfig {
                address: address.clone(),
                port: 8080,
            },
            prometheus: PrometheusConfig {
                address: address.clone(),
                port: 7080,
            },
            storage: tempdir.path().to_str().unwrap().to_owned(),
            peers: vec![PeerConfig {
                name: "registry-1".into(),
                raft: RaftConfig {
                    address: address.clone(),
                    port: 8080,
                },
                registry: RegistryConfig {
                    address: address.clone(),
                    port: 8000,
                },
            }],
            ..Default::default()
        };
        debug!("Launching registry with config: {:?}", config);

        start_registry_services(config).await.unwrap();

        let url = format!("http://{}:8000/v2/", address.clone());
        debug!("Registry url: {url}");

        let url = Url::parse(&url).unwrap();

        let retry_policy = ExponentialBackoff::builder().build_with_max_retries(3);
        let client = ClientBuilder::new(reqwest::Client::new())
            .with(RetryTransientMiddleware::new_with_policy(retry_policy))
            .build();

        TestInstance {
            url,
            client,
            _tempdir: tempdir,
            _address: address,
        }
    }

    #[tokio::test]
    async fn get_root() {
        let TestInstance {
            client,
            url,
            _tempdir,
            _address,
        } = configure().await;

        let resp = client.get(url).send().await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn upload_whole_blob() {
        let TestInstance {
            client,
            url,
            _tempdir,
            _address,
        } = configure().await;

        {
            let url = url.clone().join("foo/bar/blobs/uploads?digest=sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5").unwrap();
            let resp = client.post(url).body("FOOBAR").send().await.unwrap();
            assert_eq!(resp.status(), StatusCode::CREATED);
        }

        {
            let url = url.join("foo/bar/blobs/sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5").unwrap();
            let resp = client.get(url).send().await.unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
            assert_eq!(resp.text().await.unwrap(), "FOOBAR".to_string());
        }
    }

    #[tokio::test]
    async fn upload_cross_mount() {
        let TestInstance {
            client,
            url,
            _tempdir,
            _address,
        } = configure().await;

        {
            let url = url.clone().join("foo/bar/blobs/uploads?digest=sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5").unwrap();
            let resp = client.post(url).body("FOOBAR").send().await.unwrap();
            assert_eq!(resp.status(), StatusCode::CREATED);
        }

        {
            let url = url.join("foo/bar/blobs/sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5").unwrap();
            let resp = client.get(url).send().await.unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
            assert_eq!(resp.text().await.unwrap(), "FOOBAR".to_string());
        }

        {
            let url = url.clone().join("bar/foo/blobs/uploads?from=foo%2Fbar&mount=sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5").unwrap();
            let resp = client.post(url).send().await.unwrap();
            assert_eq!(resp.status(), StatusCode::CREATED);
        }

        {
            let url = url.join("bar/foo/blobs/sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5").unwrap();
            let resp = client.get(url).send().await.unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
            assert_eq!(resp.text().await.unwrap(), "FOOBAR".to_string());
        }
    }

    #[tokio::test]
    async fn upload_blob_multiple() {
        let TestInstance {
            client,
            url,
            _tempdir,
            _address,
        } = configure().await;

        let upload_id = {
            let url = url.clone().join("foo/bar/blobs/uploads").unwrap();
            let resp = client.post(url).send().await.unwrap();
            assert_eq!(resp.status(), StatusCode::ACCEPTED);

            resp.headers().get("Docker-Upload-UUID").unwrap().clone()
        };

        {
            let url = url
                .join("foo/bar/blobs/uploads/")
                .unwrap()
                .join(upload_id.to_str().unwrap())
                .unwrap();

            for chonk in ["FO", "OB", "AR"] {
                let resp = client.patch(url.clone()).body(chonk).send().await.unwrap();
                assert_eq!(resp.status(), StatusCode::ACCEPTED);
            }
        }

        {
            let mut url = url
                .join("foo/bar/blobs/uploads/")
                .unwrap()
                .join(upload_id.to_str().unwrap())
                .unwrap();
            url.query_pairs_mut().append_pair(
                "digest",
                "sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5",
            );

            let resp = client.put(url.clone()).send().await.unwrap();
            assert_eq!(resp.status(), StatusCode::CREATED);
        }

        {
            let url = url.join("foo/bar/blobs/sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5").unwrap();
            let resp = client.get(url).send().await.unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
            assert_eq!(resp.text().await.unwrap(), "FOOBAR".to_string());
        }
    }

    #[tokio::test]
    async fn upload_blob_multiple_finish_with_put() {
        let TestInstance {
            client,
            url,
            _tempdir,
            _address,
        } = configure().await;

        let upload_id = {
            let url = url.clone().join("foo/bar/blobs/uploads").unwrap();
            let resp = client.post(url).send().await.unwrap();
            assert_eq!(resp.status(), StatusCode::ACCEPTED);

            resp.headers().get("Docker-Upload-UUID").unwrap().clone()
        };

        {
            let url = url
                .join("foo/bar/blobs/uploads/")
                .unwrap()
                .join(upload_id.to_str().unwrap())
                .unwrap();

            for chonk in ["FO", "OB"] {
                let resp = client.patch(url.clone()).body(chonk).send().await.unwrap();
                assert_eq!(resp.status(), StatusCode::ACCEPTED);
            }
        }

        {
            let mut url = url
                .join("foo/bar/blobs/uploads/")
                .unwrap()
                .join(upload_id.to_str().unwrap())
                .unwrap();
            url.query_pairs_mut().append_pair(
                "digest",
                "sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5",
            );

            let resp = client.put(url.clone()).body("AR").send().await.unwrap();
            assert_eq!(resp.status(), StatusCode::CREATED);
        }

        {
            let url = url.join("foo/bar/blobs/sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5").unwrap();
            let resp = client.get(url).send().await.unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
            assert_eq!(resp.text().await.unwrap(), "FOOBAR".to_string());
        }
    }

    #[tokio::test]
    async fn delete_blob() {
        let TestInstance {
            client,
            url,
            _tempdir,
            _address,
        } = configure().await;

        {
            let url = url.clone().join("foo/bar/blobs/uploads?digest=sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5").unwrap();
            let resp = client.post(url).body("FOOBAR").send().await.unwrap();
            assert_eq!(resp.status(), StatusCode::CREATED);
        }

        {
            let url = url.join("foo/bar/blobs/sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5").unwrap();
            let resp = client.delete(url).send().await.unwrap();
            assert_eq!(resp.status(), StatusCode::ACCEPTED);
        }

        {
            let url = url.join("foo/bar/blobs/sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5").unwrap();
            let resp = client.get(url).send().await.unwrap();
            assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        }
    }

    #[tokio::test]
    async fn upload_manifest() {
        let TestInstance {
            client,
            url,
            _tempdir,
            _address,
        } = configure().await;

        let payload = json!({
            "schemaVersion": 2,
            "mediaType": "application/vnd.docker.distribution.manifest.list.v2+json",
            "manifests": []
        });

        {
            let url = url.clone().join("foo/bar/manifests/latest").unwrap();

            let mut headers = HeaderMap::new();
            headers.insert(
                CONTENT_TYPE,
                "application/vnd.docker.distribution.manifest.list.v2+json"
                    .parse()
                    .unwrap(),
            );

            let resp = client
                .put(url)
                .json(&payload)
                .headers(headers)
                .send()
                .await
                .unwrap();

            assert_eq!(resp.status(), StatusCode::CREATED);
        }

        {
            let url = url.join("foo/bar/manifests/latest").unwrap();
            let resp = client.get(url).send().await.unwrap();
            assert_eq!(resp.status(), StatusCode::OK);

            let value: Value = resp.json().await.unwrap();
            assert_eq!(value, payload);
        }

        {
            let url = url.join("foo/bar/manifests/sha256:a3f9bc842ffddfb3d3deed4fac54a2e8b4ac0e900d2a88125cd46e2947485ed1").unwrap();
            let resp = client.get(url).send().await.unwrap();
            assert_eq!(resp.status(), StatusCode::OK);

            let value: Value = resp.json().await.unwrap();
            assert_eq!(value, payload);
        }
    }

    #[tokio::test]
    async fn list_tags() {
        let TestInstance {
            client,
            url,
            _tempdir,
            _address,
        } = configure().await;

        let payload = json!({
            "schemaVersion": 2,
            "mediaType": "application/vnd.docker.distribution.manifest.list.v2+json",
            "manifests": []
        });

        {
            let url = url.clone().join("foo/bar/manifests/latest").unwrap();

            let mut headers = HeaderMap::new();
            headers.insert(
                CONTENT_TYPE,
                "application/vnd.docker.distribution.manifest.list.v2+json"
                    .parse()
                    .unwrap(),
            );

            let resp = client
                .put(url)
                .json(&payload)
                .headers(headers)
                .send()
                .await
                .unwrap();

            assert_eq!(resp.status(), StatusCode::CREATED);
        }

        {
            let url = url.join("foo/bar/tags/list").unwrap();
            let resp = client.get(url).send().await.unwrap();
            assert_eq!(resp.status(), StatusCode::OK);

            let value: Value = resp.json().await.unwrap();
            assert_eq!(value, json!({"name": "foo/bar", "tags": ["latest"]}));
        }
    }

    #[tokio::test]
    async fn delete_tag() {
        let TestInstance {
            client,
            url,
            _tempdir,
            _address,
        } = configure().await;

        let payload = json!({
            "schemaVersion": 2,
            "mediaType": "application/vnd.docker.distribution.manifest.list.v2+json",
            "manifests": []
        });

        {
            let url = url.clone().join("foo/bar/manifests/latest").unwrap();

            let mut headers = HeaderMap::new();
            headers.insert(
                CONTENT_TYPE,
                "application/vnd.docker.distribution.manifest.list.v2+json"
                    .parse()
                    .unwrap(),
            );

            let resp = client
                .put(url)
                .json(&payload)
                .headers(headers)
                .send()
                .await
                .unwrap();

            assert_eq!(resp.status(), StatusCode::CREATED);
        }

        // Confirm upload worked
        {
            let url = url.join("foo/bar/manifests/latest").unwrap();
            let resp = client.get(url).send().await.unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
        }

        // Delete manifest
        {
            let url = url.join("foo/bar/manifests/latest").unwrap();
            let resp = client.delete(url).send().await.unwrap();
            assert_eq!(resp.status(), StatusCode::ACCEPTED);
        }

        // Confirm delete worked
        {
            let url = url.join("foo/bar/manifests/latest").unwrap();
            let resp = client.get(url).send().await.unwrap();
            assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        }
    }

    #[tokio::test]
    async fn delete_upload() {
        let TestInstance {
            client,
            url,
            _tempdir,
            _address,
        } = configure().await;

        // Initiate a multi-part upload
        let upload_id = {
            let url = url.clone().join("foo/bar/blobs/uploads").unwrap();
            let resp = client.post(url).send().await.unwrap();
            assert_eq!(resp.status(), StatusCode::ACCEPTED);

            resp.headers().get("Docker-Upload-UUID").unwrap().clone()
        };

        // Upload some data
        {
            let url = url
                .join("foo/bar/blobs/uploads/")
                .unwrap()
                .join(upload_id.to_str().unwrap())
                .unwrap();

            for chonk in ["FO", "OB", "AR"] {
                let resp = client.patch(url.clone()).body(chonk).send().await.unwrap();
                assert_eq!(resp.status(), StatusCode::ACCEPTED);
            }
        }

        // Delete upload
        {
            let url = url
                .join("foo/bar/blobs/uploads/")
                .unwrap()
                .join(upload_id.to_str().unwrap())
                .unwrap();

            let resp = client.delete(url.clone()).send().await.unwrap();
            assert_eq!(resp.status(), StatusCode::NO_CONTENT);
        }

        // Verify upload was cancelled
        {
            let url = url
                .join("foo/bar/blobs/uploads/")
                .unwrap()
                .join(upload_id.to_str().unwrap())
                .unwrap();

            let resp = client.get(url.clone()).send().await.unwrap();
            assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        }
    }
}
