use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;

use distribd::client::RegistryClient;
use distribd::config::Configuration;
use distribd::config::PeerConfig;
use distribd::config::RaftConfig;
use distribd::config::RegistryConfig;
use distribd::start_raft_node;
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

lazy_static! {
    static ref IP_ADDRESSES: ResourcePool<String> = {
        let r = ResourcePool::new();

        //for i in 1..200 {
        //    r.append(format!("127.37.0.{i}"));
        //}

        r.append("127.0.0.1".to_owned());

        r
    };
}

fn test_config(node_id: u64, addr: String) -> Configuration {
    let mut config = distribd::config::config(None);

    config.identifier = format!("registry{}", node_id);

    config.storage = format!("tmp{}", node_id);

    config.raft = RaftConfig {
        address: addr.clone(),
        port: (8079 + node_id) as u16,
    };

    config.registry = RegistryConfig {
        address: addr.clone(),
        port: (9079 + node_id) as u16,
    };

    config.peers.push(PeerConfig {
        name: "registry1".to_owned(),
        raft: RaftConfig {
            address: addr.clone(),
            port: 8080,
        },
        registry: RegistryConfig {
            address: addr.clone(),
            port: 9080,
        },
    });
    config.peers.push(PeerConfig {
        name: "registry2".to_owned(),
        raft: RaftConfig {
            address: addr.clone(),
            port: 8080,
        },
        registry: RegistryConfig {
            address: addr.clone(),
            port: 9080,
        },
    });
    config.peers.push(PeerConfig {
        name: "registry3".to_owned(),
        raft: RaftConfig {
            address: addr.clone(),
            port: 8080,
        },
        registry: RegistryConfig {
            address: addr,
            port: 9080,
        },
    });

    config
}

/*
/// Setup a cluster of 3 nodes.
/// Write to it and read from it.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_cluster() -> anyhow::Result<()> {
    // --- The client itself does not store addresses for all nodes, but just node id.
    //     Thus we need a supporting component to provide mapping from node id to node address.
    //     This is only used by the client. A raft node in this example stores node addresses in its
    // store.

    std::panic::set_hook(Box::new(|panic| {
        log_panic(panic);
    }));

    tracing_subscriber::fmt()
        .with_target(true)
        .with_thread_ids(true)
        .with_level(true)
        .with_ansi(false)
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let get_addr = |node_id| {
        let addr = match node_id {
            1 => "127.0.0.1:8080".to_string(),
            2 => "127.0.0.1:8081".to_string(),
            3 => "127.0.0.1:8082".to_string(),
            _ => {
                return Err(anyhow::anyhow!("node {} not found", node_id));
            }
        };
        Ok(addr)
    };

    // --- Start 3 raft node in 3 threads.

    let _h1 = thread::spawn(|| {
        let rt = Runtime::new().unwrap();
        let x = rt.block_on(async move { start_raft_node(test_config(1, "127.0.0.1")).await });
        println!("x: {:?}", x);
    });

    let _h2 = thread::spawn(|| {
        let rt = Runtime::new().unwrap();
        let x = rt.block_on(async move { start_raft_node(test_config(2)).await });
        println!("x: {:?}", x);
    });

    let _h3 = thread::spawn(|| {
        let rt = Runtime::new().unwrap();
        let x = rt.block_on(async move { start_raft_node(test_config(3)).await });
        println!("x: {:?}", x);
    });

    // Wait for server to start up.
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // --- Create a client to the first node, as a control handle to the cluster.

    let client = RegistryClient::new(1, get_addr(1)?);

    // --- 1. Initialize the target node as a cluster of only one node.
    //        After init(), the single node cluster will be fully functional.

    println!("=== init single node cluster");
    client.init().await?;

    println!("=== metrics after init");
    let _x = client.metrics().await?;

    // --- 2. Add node 2 and 3 to the cluster as `Learner`, to let them start to receive log replication
    // from the        leader.

    println!("=== add-learner 2");
    let _x = client.add_learner((2, get_addr(2)?)).await?;

    println!("=== add-learner 3");
    let _x = client.add_learner((3, get_addr(3)?)).await?;

    println!("=== metrics after add-learner");
    let x = client.metrics().await?;

    assert_eq!(
        &vec![btreeset![1]],
        x.membership_config.membership().get_joint_config()
    );

    let nodes_in_cluster = x
        .membership_config
        .nodes()
        .map(|(nid, node)| (*nid, node.clone()))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        btreemap! {
            1 => BasicNode::new("127.0.0.1:8080"),
            2 => BasicNode::new("127.0.0.1:8081"),
            3 => BasicNode::new("127.0.0.1:8082"),
        },
        nodes_in_cluster
    );

    // --- 3. Turn the two learners to members. A member node can vote or elect itself as leader.

    println!("=== change-membership to 1,2,3");
    let _x = client.change_membership(&btreeset! {1,2,3}).await?;

    // --- After change-membership, some cluster state will be seen in the metrics.
    //
    // ```text
    // metrics: RaftMetrics {
    //   current_leader: Some(1),
    //   membership_config: EffectiveMembership {
    //        log_id: LogId { leader_id: LeaderId { term: 1, node_id: 1 }, index: 8 },
    //        membership: Membership { learners: {}, configs: [{1, 2, 3}] }
    //   },
    //   leader_metrics: Some(LeaderMetrics { replication: {
    //     2: ReplicationMetrics { matched: Some(LogId { leader_id: LeaderId { term: 1, node_id: 1 }, index: 7 }) },
    //     3: ReplicationMetrics { matched: Some(LogId { leader_id: LeaderId { term: 1, node_id: 1 }, index: 8 }) }} })
    // }
    // ```

    println!("=== metrics after change-member");
    let x = client.metrics().await?;
    assert_eq!(
        &vec![btreeset![1, 2, 3]],
        x.membership_config.membership().get_joint_config()
    );

    // --- Try to write some application data through the leader.

    println!("=== write `foo=bar`");
    let _x = client
        .write(&RegistryRequest::Set {
            key: "foo".to_string(),
            value: "bar".to_string(),
        })
        .await?;

    // --- Wait for a while to let the replication get done.

    tokio::time::sleep(Duration::from_millis(1_000)).await;

    // --- Read it on every node.

    println!("=== read `foo` on node 1");
    let x = client.read(&("foo".to_string())).await?;
    assert_eq!("bar", x);

    println!("=== read `foo` on node 2");
    let client2 = RegistryClient::new(2, get_addr(2)?);
    let x = client2.read(&("foo".to_string())).await?;
    assert_eq!("bar", x);

    println!("=== read `foo` on node 3");
    let client3 = RegistryClient::new(3, get_addr(3)?);
    let x = client3.read(&("foo".to_string())).await?;
    assert_eq!("bar", x);

    // --- A write to non-leader will be automatically forwarded to a known leader

    println!("=== read `foo` on node 2");
    let _x = client2
        .write(&RegistryRequest::Set {
            key: "foo".to_string(),
            value: "wow".to_string(),
        })
        .await?;

    tokio::time::sleep(Duration::from_millis(1_000)).await;

    // --- Read it on every node.

    println!("=== read `foo` on node 1");
    let x = client.read(&("foo".to_string())).await?;
    assert_eq!("wow", x);

    println!("=== read `foo` on node 2");
    let client2 = RegistryClient::new(2, get_addr(2)?);
    let x = client2.read(&("foo".to_string())).await?;
    assert_eq!("wow", x);

    println!("=== read `foo` on node 3");
    let client3 = RegistryClient::new(3, get_addr(3)?);
    let x = client3.read(&("foo".to_string())).await?;
    assert_eq!("wow", x);

    println!("=== consistent_read `foo` on node 1");
    let x = client.consistent_read(&("foo".to_string())).await?;
    assert_eq!("wow", x);

    println!("=== consistent_read `foo` on node 2 MUST return CheckIsLeaderError");
    let x = client2.consistent_read(&("foo".to_string())).await;
    match x {
        Err(e) => {
            let s = e.to_string();
            let expect_err:String = "error occur on remote peer 2: has to forward request to: Some(1), Some(BasicNode { addr: \"127.0.0.1:8080\" })".to_string();

            assert_eq!(s, expect_err);
        }
        Ok(_) => panic!("MUST return CheckIsLeaderError"),
    }

    /*
    // --- Remove node 1,2 from the cluster.

    println!("=== change-membership to 3, ");
    let _x = client.change_membership(&btreeset! {3}).await?;

    tokio::time::sleep(Duration::from_millis(8_000)).await;

    println!("=== metrics after change-membership to {{3}}");
    let x = client.metrics().await?;
    assert_eq!(
        &vec![btreeset![3]],
        x.membership_config.membership().get_joint_config()
    );

    println!("=== write `foo=zoo` to node-3");
    let _x = client3
        .write(&RegistryRequest::Set {
            key: "foo".to_string(),
            value: "zoo".to_string(),
        })
        .await?;

    println!("=== read `foo=zoo` to node-3");
    let got = client3.read(&"foo".to_string()).await?;
    assert_eq!("zoo", got);
    */

    Ok(())
}
*/

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
    _address: ResourcePoolGuard<String>,
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
    /*std::panic::set_hook(Box::new(|panic| {
        log_panic(panic);
    }));*/

    /*
    tracing_subscriber::fmt()
    .with_target(true)
    .with_thread_ids(true)
    .with_level(true)
    .with_ansi(false)
    .with_env_filter(EnvFilter::from_default_env())
    .init();
    */

    let address = IP_ADDRESSES.get().await;

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

        let backend = RegistryClient::new(id, format!("{}:{}", address.clone(), config.raft.port));

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
    leader.init().await?;
    leader
        .add_learner((2, peers.get(1).unwrap().address.clone()))
        .await?;
    leader
        .add_learner((3, peers.get(2).unwrap().address.clone()))
        .await?;
    leader.change_membership(&btreeset! {1,2,3}).await?;

    Ok(TestCluster {
        peers,
        _address: address,
    })
}

#[tokio::test]
async fn get_root() {
    let cluster = configure().await.unwrap();
    let client = &cluster.peers.get(0).unwrap().client;
    let url = cluster.peers.get(0).unwrap().url.clone();

    let resp = client.get(url).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn upload_whole_blob() {
    let cluster = configure().await.unwrap();
    let client = &cluster.peers.get(0).unwrap().client;
    let url = cluster.peers.get(0).unwrap().url.clone();

    {
        let url = url.clone().join("foo/bar/blobs/uploads?digest=sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5").unwrap();
        let resp = client.post(url).body("FOOBAR").send().await.unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
    }

    cluster
        .head_all(
            "bar/foo/blobs/sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5",
            |resp| {
                if resp.status() == StatusCode::NOT_FOUND {
                    return true;
                }
                assert_eq!(resp.status(), StatusCode::OK);
                false
            },
        )
        .await;

    {
        let url = url.join("foo/bar/blobs/sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5").unwrap();
        let resp = client.get(url).send().await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.text().await.unwrap(), "FOOBAR".to_string());
    }
}

#[tokio::test]
async fn upload_cross_mount() {
    let cluster = configure().await.unwrap();
    let client = &cluster.peers.get(0).unwrap().client;
    let url = cluster.peers.get(0).unwrap().url.clone();

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

    cluster
        .head_all(
            "bar/foo/blobs/sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5",
            |resp| {
                if resp.status() == StatusCode::NOT_FOUND {
                    return true;
                }
                assert_eq!(resp.status(), StatusCode::OK);
                false
            },
        )
        .await;

    {
        let url = url.join("bar/foo/blobs/sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5").unwrap();
        let resp = client.get(url).send().await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.text().await.unwrap(), "FOOBAR".to_string());
    }

    {
        let url = cluster.peers.get(1).unwrap().url.clone();
        let url = url.join("bar/foo/blobs/sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5").unwrap();
        let client = &cluster.peers.get(1).unwrap().client;
        let resp = client.get(url).send().await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.text().await.unwrap(), "FOOBAR".to_string());
    }
}

#[tokio::test]
async fn upload_blob_multiple() {
    let cluster = configure().await.unwrap();
    let client = &cluster.peers.get(0).unwrap().client;
    let url = cluster.peers.get(0).unwrap().url.clone();

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

    cluster
        .head_all(
            "bar/foo/blobs/sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5",
            |resp| {
                if resp.status() == StatusCode::NOT_FOUND {
                    return true;
                }
                assert_eq!(resp.status(), StatusCode::OK);
                false
            },
        )
        .await;

    {
        let url = url.join("foo/bar/blobs/sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5").unwrap();
        let resp = client.get(url).send().await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.text().await.unwrap(), "FOOBAR".to_string());
    }
}

#[tokio::test]
async fn upload_blob_multiple_finish_with_put() {
    let cluster = configure().await.unwrap();
    let client = &cluster.peers.get(0).unwrap().client;
    let url = cluster.peers.get(0).unwrap().url.clone();

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

    cluster
        .head_all(
            "bar/foo/blobs/sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5",
            |resp| {
                if resp.status() == StatusCode::NOT_FOUND {
                    return true;
                }
                assert_eq!(resp.status(), StatusCode::OK);
                false
            },
        )
        .await;

    {
        let url = url.join("foo/bar/blobs/sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5").unwrap();
        let resp = client.get(url).send().await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.text().await.unwrap(), "FOOBAR".to_string());
    }
}

#[tokio::test]
async fn delete_blob() {
    let cluster = configure().await.unwrap();
    let client = &cluster.peers.get(0).unwrap().client;
    let url = cluster.peers.get(0).unwrap().url.clone();

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

    cluster
        .head_all(
            "bar/foo/blobs/sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5",
            |resp| {
                if resp.status() == StatusCode::OK {
                    return true;
                }
                assert_eq!(resp.status(), StatusCode::NOT_FOUND);
                false
            },
        )
        .await;

    {
        let url = url.join("foo/bar/blobs/sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5").unwrap();
        let resp = client.get(url).send().await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }
}

#[tokio::test]
async fn upload_manifest() {
    let cluster = configure().await.unwrap();
    let client = &cluster.peers.get(0).unwrap().client;
    let url = cluster.peers.get(0).unwrap().url.clone();

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

    cluster
        .head_all(
            "foo/bar/manifests/sha256:a3f9bc842ffddfb3d3deed4fac54a2e8b4ac0e900d2a88125cd46e2947485ed1",
            |resp| {
                if resp.status() == StatusCode::NOT_FOUND {
                    return true;
                }
                assert_eq!(resp.status(), StatusCode::OK);
                false
            },
        )
        .await;

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
    let cluster = configure().await.unwrap();
    let client = &cluster.peers.get(0).unwrap().client;
    let url = cluster.peers.get(0).unwrap().url.clone();

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
    let cluster = configure().await.unwrap();
    let client = &cluster.peers.get(0).unwrap().client;
    let url = cluster.peers.get(0).unwrap().url.clone();

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
}

#[tokio::test]
async fn delete_upload() {
    let cluster = configure().await.unwrap();
    let client = &cluster.peers.get(0).unwrap().client;
    let url = cluster.peers.get(0).unwrap().url.clone();

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
