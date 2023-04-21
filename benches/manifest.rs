use std::str::FromStr;
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;

use criterion::black_box;
use criterion::criterion_group;
use criterion::criterion_main;
use criterion::Criterion;
use distribd::client::RegistryClient;
use distribd::config::Configuration;
use distribd::config::PrometheusConfig;
use distribd::config::RaftConfig;
use distribd::config::RegistryConfig;
use distribd::start_raft_node;
use distribd::types::Digest;
use lazy_static::lazy_static;
use maplit::btreeset;
use reqwest::Response;
use reqwest::StatusCode;
use reqwest::{
    header::{HeaderMap, CONTENT_TYPE},
    Url,
};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
use serde_json::json;
use serde_json::Value;
use simple_pool::{ResourcePool, ResourcePoolGuard};
use tempfile::TempDir;
use tokio::runtime::Runtime;
use tokio::sync::Notify;

fn test_config(node_id: u64, addr: String) -> Configuration {
    let mut config = distribd::config::config(None);

    config.identifier = format!("registry-{}", node_id - 1);

    config.storage = format!("tmp{}", node_id);

    config.raft = RaftConfig {
        address: addr.clone(),
        port: (8079 + node_id) as u16,
        tls: None,
    };

    config.registry = RegistryConfig {
        address: addr.clone(),
        port: (9079 + node_id) as u16,
    };

    config.prometheus = PrometheusConfig {
        address: addr,
        port: (7079 + node_id) as u16,
    };

    config
}

struct TestNode {
    address: String,
    backend: RegistryClient,
    url: Url,
    client: ClientWithMiddleware,
    _tempdir: TempDir,
    _thread: Arc<Notify>,
    _handle: Option<JoinHandle<()>>,
}

impl Drop for TestNode {
    fn drop(&mut self) {
        self._thread.notify_one();
        if self._handle.take().unwrap().join().is_ok() {}
    }
}

struct TestCluster {
    peers: Vec<TestNode>,
}

impl TestCluster {
    async fn head_all<F: Fn(Response) -> bool>(&self, url: &str, cb: F) {
        for peer in self.peers.iter() {
            'peer: {
                let full_url = peer.url.clone().join(url).unwrap();
                for _i in 1..10 {
                    let resp = peer.client.head(full_url.clone()).send().await.unwrap();
                    if !cb(resp) {
                        break 'peer;
                    }
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }

                panic!("Shouldn't have got here");
            }
        }
    }
}

impl Drop for TestCluster {
    fn drop(&mut self) {
        // We do this explicitly to make sure the drop of the peers happens before the drop of the resource pool guard
        for peer in self.peers.drain(..) {
            drop(peer);
        }
    }
}

async fn configure() -> anyhow::Result<TestCluster> {
    let address = "127.0.0.1".to_string();

    let mut peers = vec![];
    for id in 1..4 {
        let mut config = test_config(id, address.clone());

        let tempdir = tempfile::tempdir().unwrap();
        config.storage = tempdir.path().to_owned().to_string_lossy().to_string();

        let thread_config = config.clone();

        let sender = Arc::new(Notify::new());
        let receiver = sender.clone();

        let handle = thread::spawn(move || {
            let rt = Runtime::new().unwrap();
            rt.block_on(async move {
                let tasks = start_raft_node(thread_config).await.unwrap();
                receiver.notified().await;
                tasks.notify_one();
                tokio::time::sleep(Duration::from_secs(5)).await;
            });
            rt.shutdown_timeout(Duration::from_secs(5));
        });

        let retry_policy = Some(ExponentialBackoff::builder().build_with_max_retries(3));
        let backend = RegistryClient::new(
            id,
            format!("{}:{}", address.clone(), config.raft.port),
            retry_policy,
        );

        let retry_policy = ExponentialBackoff::builder().build_with_max_retries(3);
        let client = ClientBuilder::new(reqwest::Client::new())
            .with(RetryTransientMiddleware::new_with_policy(retry_policy))
            .build();

        peers.push(TestNode {
            address: format!("{}:{}", address.clone(), config.raft.port),
            url: Url::parse(&format!(
                "http://{}:{}/v2/",
                address.clone(),
                config.registry.port
            ))
            .unwrap(),
            backend,
            client,
            _handle: Some(handle),
            _thread: sender,
            _tempdir: tempdir,
        });
    }

    // Wait for server to start up.
    tokio::time::sleep(Duration::from_millis(1000)).await;

    let leader = &peers.get(0).unwrap().backend;
    leader.init(peers.get(0).unwrap().address.clone()).await?;
    leader
        .add_learner((2, peers.get(1).unwrap().address.clone()))
        .await?;
    leader
        .add_learner((3, peers.get(2).unwrap().address.clone()))
        .await?;
    leader.change_membership(&btreeset! {1,2,3}).await?;

    Ok(TestCluster { peers })
}

pub fn manifest_put_get_delete(c: &mut Criterion) {
    let _handle = tokio::spawn(async {
        let cluster = configure().await.unwrap();
        let client = &cluster.peers.get(0).unwrap().client;
        let url = cluster.peers.get(0).unwrap().url.clone();
    });

    c.bench_function("fib 20", |b| b.iter(|| println!("{}", black_box(20) * 20)));

    /*
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
    cluster
    .head_all("foo/bar/manifests/latest", |resp| {
        if resp.status() == StatusCode::OK {
            return true;
        }
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        false
    })
    .await;

    // Confirm delete worked
    {
        let url = url.join("foo/bar/manifests/latest").unwrap();
        let resp = client.get(url).send().await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }
    */
}

criterion_group!(benches, manifest_put_get_delete);
criterion_main!(benches);
