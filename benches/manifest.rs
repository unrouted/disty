use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;

use criterion::criterion_group;
use criterion::criterion_main;
use criterion::Criterion;
use distribd::client::RegistryClient;
use distribd::config::Configuration;
use distribd::config::PrometheusConfig;
use distribd::config::RaftConfig;
use distribd::config::RegistryConfig;
use distribd::start_raft_node;
use maplit::btreeset;
use rand::seq::SliceRandom;
use reqwest::StatusCode;
use reqwest::{
    header::{HeaderMap, CONTENT_TYPE},
    Url,
};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
use serde_json::json;
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
    runtime: Runtime,
}

impl Drop for TestCluster {
    fn drop(&mut self) {
        // We do this explicitly to make sure the drop of the peers happens before the drop of the resource pool guard
        for peer in self.peers.drain(..) {
            drop(peer);
        }
    }
}

fn configure() -> anyhow::Result<Arc<TestCluster>> {
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

    let cluster = Arc::new(TestCluster {
        peers,
        runtime: Runtime::new().unwrap(),
    });
    let cluster2 = cluster.clone();

    cluster2.runtime.block_on(async move {
        let peers = &cluster.peers;
        let leader = &peers.first().unwrap().backend;

        // Wait for server to start up.
        tokio::time::sleep(Duration::from_millis(1000)).await;

        leader
            .init(peers.first().unwrap().address.clone())
            .await
            .unwrap();

        leader
            .add_learner((2, peers.get(1).unwrap().address.clone()))
            .await
            .unwrap();
        leader
            .add_learner((3, peers.get(2).unwrap().address.clone()))
            .await
            .unwrap();

        leader.change_membership(&btreeset! {1,2,3}).await.unwrap();
    });

    Ok(cluster2)
}

async fn async_manifest_put_get_delete(cluster: Arc<TestCluster>) {
    let client = &cluster.peers.first().unwrap().client;
    let peer = cluster.peers.choose(&mut rand::thread_rng()).unwrap();
    let url = peer.url.clone();

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
}

pub fn manifest_put_get_delete(c: &mut Criterion) {
    let cluster = configure().unwrap();

    c.bench_function("manifest::put", |b| {
        b.to_async(Runtime::new().unwrap())
            .iter(|| async_manifest_put_get_delete(cluster.clone()))
    });
}

async fn async_manifest_get_404(cluster: Arc<TestCluster>) {
    let client = &cluster.peers.first().unwrap().client;
    let peer = cluster.peers.choose(&mut rand::thread_rng()).unwrap();
    let url = peer.url.clone().join("foo/bar/manifests/latest").unwrap();
    let resp = client.get(url).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

pub fn manifest_get_404(c: &mut Criterion) {
    let cluster = configure().unwrap();

    c.bench_function("manifest::get (404)", |b| {
        b.to_async(Runtime::new().unwrap())
            .iter(|| async_manifest_get_404(cluster.clone()))
    });
}

async fn async_manifest_get(cluster: Arc<TestCluster>) {
    let client = &cluster.peers.first().unwrap().client;
    let peer = cluster.peers.choose(&mut rand::thread_rng()).unwrap();
    let url = peer.url.clone().join("foo/bar/manifests/latest").unwrap();
    let resp = client.get(url).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

pub fn manifest_get(c: &mut Criterion) {
    let cluster = configure().unwrap();
    let cluster2 = cluster.clone();

    cluster2.clone().runtime.block_on(async move {
        let client = &cluster.peers.first().unwrap().client;

        let payload = json!({
            "schemaVersion": 2,
            "mediaType": "application/vnd.docker.distribution.manifest.list.v2+json",
            "manifests": []
        });

        {
            let peer = cluster.peers.choose(&mut rand::thread_rng()).unwrap();
            let url = peer.url.clone().join("foo/bar/manifests/latest").unwrap();

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
    });

    c.bench_function("manifest::get (200)", |b| {
        b.to_async(Runtime::new().unwrap())
            .iter(|| async_manifest_get(cluster2.clone()))
    });
}

criterion_group!(
    benches,
    manifest_get,
    manifest_get_404,
    manifest_put_get_delete
);
criterion_main!(benches);
