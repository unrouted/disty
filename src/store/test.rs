use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;


use chrono::Utc;
use openraft::storage::Adaptor;
use openraft::testing::StoreBuilder;
use openraft::testing::Suite;
use openraft::Entry;
use openraft::EntryPayload;
use openraft::LeaderId;
use openraft::LogId;
use openraft::RaftStorage;
use openraft::StorageError;
use tempfile::TempDir;
use tracing_test::traced_test;

use crate::types::Digest;
use crate::types::RegistryAction;
use crate::types::RepositoryName;
use crate::RegistryNodeId;
use crate::RegistryStore;
use crate::RegistryTypeConfig;

use super::RegistryRequest;

static GLOBAL_TEST_COUNT: AtomicUsize = AtomicUsize::new(0);

struct SledBuilder {}

type LogStore = Adaptor<RegistryTypeConfig, Arc<RegistryStore>>;
type StateMachine = Adaptor<RegistryTypeConfig, Arc<RegistryStore>>;

#[test]
pub fn test_raft_store() -> Result<(), StorageError<RegistryNodeId>> {
    Suite::test_all(SledBuilder {})
}

impl StoreBuilder<RegistryTypeConfig, LogStore, StateMachine, TempDir> for SledBuilder {
    async fn build(
        &self,
    ) -> Result<(TempDir, LogStore, StateMachine), StorageError<RegistryNodeId>> {
        let pid = std::process::id();
        let td = tempfile::TempDir::new().expect("couldn't create temp dir");
        let temp_dir_path = td.path().to_str().expect("Could not convert temp dir");

        let old_count = GLOBAL_TEST_COUNT.fetch_add(1, Ordering::SeqCst);
        let db_dir_str = format!("{}pid{}/num{}/", &temp_dir_path, pid, old_count);

        let db_dir = std::path::Path::new(&db_dir_str);
        if !db_dir.exists() {
            std::fs::create_dir_all(db_dir)
                .unwrap_or_else(|_| panic!("could not create: {:?}", db_dir.to_str()));
        }

        let db: sled::Db =
            sled::open(db_dir).unwrap_or_else(|_| panic!("could not open: {:?}", db_dir.to_str()));

        let mut registry = <prometheus_client::registry::Registry>::default();
        let store = RegistryStore::new(Arc::new(db), 0, &mut registry).await;
        let (log_store, sm) = Adaptor::new(store);

        Ok((td, log_store, sm))
    }
}

struct TestStorage {
    store: Arc<RegistryStore>,
    _tempdir: TempDir,
}

async fn setup_state() -> TestStorage {
    let tempdir = tempfile::tempdir().unwrap();
    let db_dir = tempdir.path().to_owned().to_string_lossy().to_string();

    let db: sled::Db =
        sled::open(&db_dir).unwrap_or_else(|_| panic!("could not open: {:?}", db_dir));

    let mut registry = <prometheus_client::registry::Registry>::default();
    let store = RegistryStore::new(Arc::new(db), 0, &mut registry).await;

    TestStorage {
        store,
        _tempdir: tempdir,
    }
}

impl TestStorage {
    async fn dispatch_actions(&mut self, actions: Vec<RegistryAction>) {
        let log_id = LogId {
            leader_id: LeaderId {
                term: 1,
                node_id: 1,
            },
            index: 1,
        };
        let payload =
            EntryPayload::<RegistryTypeConfig>::Normal(RegistryRequest::Transaction { actions });
        let entry = Entry::<RegistryTypeConfig> { log_id, payload };
        self.store.apply_to_state_machine(&[entry]).await.unwrap();
    }
}

// BLOB TESTS

#[tokio::test]
#[traced_test]
async fn blob_not_available_initially() {
    let state = setup_state().await;

    let digest = "sha256:abcdefg".parse().unwrap();

    assert!(state.store.get_blob(&digest).unwrap().is_none());
}

#[tokio::test]
#[traced_test]
async fn blob_becomes_available() {
    let mut state = setup_state().await;

    let repository = "myrepo".parse().unwrap();
    let digest = "sha256:abcdefg".parse().unwrap();

    state
        .dispatch_actions(vec![RegistryAction::BlobMounted {
            timestamp: Utc::now(),
            user: "test".to_string(),
            repository,
            digest,
        }])
        .await;

    let repository = "myrepo".parse().unwrap();
    let digest = "sha256:abcdefg".parse().unwrap();

    let blob = state.store.get_blob(&digest).unwrap().unwrap();
    assert!(blob.repositories.contains(&repository));
}

#[tokio::test]
#[traced_test]
async fn blob_metadata() {
    let mut state = setup_state().await;

    let repository: RepositoryName = "myrepo".parse().unwrap();
    let digest: Digest = "sha256:abcdefg".parse().unwrap();
    let dependency: Digest = "sha256:zxyjkl".parse().unwrap();

    state
        .dispatch_actions(vec![
            RegistryAction::BlobMounted {
                timestamp: Utc::now(),
                user: "test".to_string(),
                repository,
                digest: digest.clone(),
            },
            RegistryAction::BlobInfo {
                timestamp: Utc::now(),
                digest,
                content_type: "application/json".to_string(),
                dependencies: vec![dependency],
            },
        ])
        .await;

    let digest: Digest = "sha256:abcdefg".parse().unwrap();

    let item = state.store.get_blob(&digest).unwrap().unwrap();
    assert_eq!(item.content_type, Some("application/json".to_string()));
    assert_eq!(item.dependencies.as_ref().unwrap().len(), 1);

    let dependencies = vec!["sha256:zxyjkl".parse().unwrap()];
    assert_eq!(item.dependencies, Some(dependencies));
}

#[tokio::test]
#[traced_test]
async fn blob_size() {
    let mut state = setup_state().await;

    let repository = "myrepo".parse().unwrap();
    let digest = "sha256:abcdefg".parse().unwrap();

    state
        .dispatch_actions(vec![RegistryAction::BlobMounted {
            timestamp: Utc::now(),
            user: "test".to_string(),
            repository,
            digest,
        }])
        .await;

    let digest = "sha256:abcdefg".parse().unwrap();

    state
        .dispatch_actions(vec![RegistryAction::BlobStat {
            timestamp: Utc::now(),
            digest,
            size: 1234,
        }])
        .await;

    let digest: Digest = "sha256:abcdefg".parse().unwrap();
    let item = state.store.get_blob(&digest).unwrap().unwrap();

    assert_eq!(item.size, Some(1234));
}

#[tokio::test]
#[traced_test]
async fn blob_becomes_unavailable() {
    let mut state = setup_state().await;

    let repository = "myrepo".parse().unwrap();
    let digest = "sha256:abcdefg".parse().unwrap();

    state
        .dispatch_actions(vec![RegistryAction::BlobMounted {
            timestamp: Utc::now(),
            user: "test".to_string(),
            repository,
            digest,
        }])
        .await;

    let repository = "myrepo".parse().unwrap();
    let digest = "sha256:abcdefg".parse().unwrap();

    state
        .dispatch_actions(vec![RegistryAction::BlobUnmounted {
            timestamp: Utc::now(),
            user: "test".to_string(),
            repository,
            digest,
        }])
        .await;

    let digest = "sha256:abcdefg".parse().unwrap();

    let blob = state.store.get_blob(&digest).unwrap().unwrap();
    assert_eq!(blob.repositories.len(), 0);
}

#[tokio::test]
#[traced_test]
async fn blob_becomes_available_again() {
    let mut state = setup_state().await;

    // Create node
    let repository = "myrepo".parse().unwrap();
    let digest = "sha256:abcdefg".parse().unwrap();

    state
        .dispatch_actions(vec![RegistryAction::BlobMounted {
            timestamp: Utc::now(),
            user: "test".to_string(),
            repository,
            digest,
        }])
        .await;

    // Make node unavailable
    let repository = "myrepo".parse().unwrap();
    let digest = "sha256:abcdefg".parse().unwrap();

    state
        .dispatch_actions(vec![RegistryAction::BlobUnmounted {
            timestamp: Utc::now(),
            user: "test".to_string(),
            repository,
            digest,
        }])
        .await;

    // Make node available again
    let repository = "myrepo".parse().unwrap();
    let digest = "sha256:abcdefg".parse().unwrap();

    state
        .dispatch_actions(vec![RegistryAction::BlobMounted {
            timestamp: Utc::now(),
            user: "test".to_string(),
            repository,
            digest,
        }])
        .await;

    // Should be visible...
    let repository = "myrepo".parse().unwrap();
    let digest = "sha256:abcdefg".parse().unwrap();

    let blob = state.store.get_blob(&digest).unwrap().unwrap();
    assert!(blob.repositories.contains(&repository));
}

// MANIFEST TESTS

#[tokio::test]
#[traced_test]
async fn manifest_not_available_initially() {
    let state = setup_state().await;

    let digest = "sha256:abcdefg".parse().unwrap();

    assert!(state.store.get_manifest(&digest).unwrap().is_none())
}

#[tokio::test]
#[traced_test]
async fn manifest_becomes_available() {
    let mut state = setup_state().await;

    let repository = "myrepo".parse().unwrap();
    let digest = "sha256:abcdefg".parse().unwrap();

    state
        .dispatch_actions(vec![RegistryAction::ManifestMounted {
            timestamp: Utc::now(),
            user: "test".to_string(),
            repository,
            digest,
        }])
        .await;

    let repository = "myrepo".parse().unwrap();
    let digest = "sha256:abcdefg".parse().unwrap();

    let manifest = state.store.get_manifest(&digest).unwrap().unwrap();
    assert!(manifest.repositories.contains(&repository));
}

#[tokio::test]
#[traced_test]
async fn manifest_metadata() {
    let mut state = setup_state().await;

    let repository = "myrepo".parse().unwrap();
    let digest = "sha256:abcdefg".parse().unwrap();

    state
        .dispatch_actions(vec![RegistryAction::ManifestMounted {
            timestamp: Utc::now(),
            user: "test".to_string(),
            repository,
            digest,
        }])
        .await;

    let digest = "sha256:abcdefg".parse().unwrap();
    let dependency: Digest = "sha256:zxyjkl".parse().unwrap();

    state
        .dispatch_actions(vec![RegistryAction::ManifestInfo {
            timestamp: Utc::now(),
            digest,
            content_type: "application/json".to_string(),
            dependencies: vec![dependency],
        }])
        .await;

    let digest: Digest = "sha256:abcdefg".parse().unwrap();
    let item = state.store.get_manifest(&digest).unwrap().unwrap();

    assert_eq!(item.content_type, Some("application/json".to_string()));
    assert_eq!(item.dependencies.as_ref().unwrap().len(), 1);

    let dependencies = vec!["sha256:zxyjkl".parse().unwrap()];
    assert_eq!(item.dependencies, Some(dependencies));
}

#[tokio::test]
#[traced_test]
async fn manifest_size() {
    let mut state = setup_state().await;

    let repository = "myrepo".parse().unwrap();
    let digest = "sha256:abcdefg".parse().unwrap();

    state
        .dispatch_actions(vec![RegistryAction::ManifestMounted {
            timestamp: Utc::now(),
            user: "test".to_string(),
            repository,
            digest,
        }])
        .await;

    let digest = "sha256:abcdefg".parse().unwrap();

    state
        .dispatch_actions(vec![RegistryAction::ManifestStat {
            timestamp: Utc::now(),
            digest,
            size: 1234,
        }])
        .await;

    let digest: Digest = "sha256:abcdefg".parse().unwrap();
    let item = state.store.get_manifest(&digest).unwrap().unwrap();

    assert_eq!(item.size, Some(1234));
}

#[tokio::test]
#[traced_test]
async fn manifest_becomes_unavailable() {
    let mut state = setup_state().await;

    let repository = "myrepo".parse().unwrap();
    let digest = "sha256:abcdefg".parse().unwrap();

    state
        .dispatch_actions(vec![RegistryAction::ManifestMounted {
            timestamp: Utc::now(),
            user: "test".to_string(),
            repository,
            digest,
        }])
        .await;

    let repository = "myrepo".parse().unwrap();
    let digest = "sha256:abcdefg".parse().unwrap();

    state
        .dispatch_actions(vec![RegistryAction::ManifestUnmounted {
            timestamp: Utc::now(),
            user: "test".to_string(),
            repository,
            digest,
        }])
        .await;

    let digest = "sha256:abcdefg".parse().unwrap();
    let manifest = state.store.get_manifest(&digest).unwrap().unwrap();
    assert_eq!(manifest.repositories.len(), 0);
}

#[tokio::test]
#[traced_test]
async fn manifest_becomes_available_again() {
    let mut state = setup_state().await;

    // Create node
    let repository = "myrepo".parse().unwrap();
    let digest = "sha256:abcdefg".parse().unwrap();

    state
        .dispatch_actions(vec![RegistryAction::ManifestMounted {
            timestamp: Utc::now(),
            user: "test".to_string(),
            repository,
            digest,
        }])
        .await;

    // Make node unavailable
    let repository = "myrepo".parse().unwrap();
    let digest = "sha256:abcdefg".parse().unwrap();

    state
        .dispatch_actions(vec![RegistryAction::ManifestUnmounted {
            timestamp: Utc::now(),
            user: "test".to_string(),
            repository,
            digest,
        }])
        .await;
    // Make node available again
    let repository = "myrepo".parse().unwrap();
    let digest = "sha256:abcdefg".parse().unwrap();

    state
        .dispatch_actions(vec![RegistryAction::ManifestMounted {
            timestamp: Utc::now(),
            user: "test".to_string(),
            repository,
            digest,
        }])
        .await;

    // Should be visible...
    let repository = "myrepo".parse().unwrap();
    let digest = "sha256:abcdefg".parse().unwrap();

    let manifest = state.store.get_manifest(&digest).unwrap().unwrap();
    assert!(manifest.repositories.contains(&repository));
}

#[tokio::test]
#[traced_test]
async fn can_tag_manifest() {
    let mut state = setup_state().await;

    // Create node
    let repository = "myrepo".parse().unwrap();
    let digest = "sha256:abcdefg".parse().unwrap();

    state
        .dispatch_actions(vec![RegistryAction::HashTagged {
            timestamp: Utc::now(),
            user: "test".to_string(),
            repository,
            digest,
            tag: "latest".to_string(),
        }])
        .await;

    let repository = "myrepo2".parse().unwrap();
    assert_eq!(state.store.get_tags(&repository).unwrap(), Some(vec![]));

    let repository = "myrepo".parse().unwrap();
    assert_eq!(
        state.store.get_tags(&repository).unwrap(),
        Some(vec!["latest".to_string()])
    );
}

#[tokio::test]
#[traced_test]
async fn can_collect_orphaned_manifests() {
    let mut state = setup_state().await;

    // Create node
    let repository: RepositoryName = "myrepo".parse().unwrap();
    let digest1: Digest = "sha256:abcdefg".parse().unwrap();
    let digest2: Digest = "sha256:gfedcba".parse().unwrap();

    state
        .dispatch_actions(vec![
            RegistryAction::ManifestMounted {
                timestamp: Utc::now(),
                user: "test".to_string(),
                repository: repository.clone(),
                digest: digest1.clone(),
            },
            RegistryAction::ManifestStored {
                timestamp: Utc::now(),
                user: "test".to_string(),
                location: 0,
                digest: digest1.clone(),
            },
            RegistryAction::HashTagged {
                timestamp: Utc::now(),
                user: "test".to_string(),
                repository: repository.clone(),
                digest: digest1.clone(),
                tag: "latest".to_string(),
            },
            RegistryAction::ManifestMounted {
                timestamp: Utc::now(),
                user: "test".to_string(),
                repository: repository.clone(),
                digest: digest2.clone(),
            },
            RegistryAction::ManifestStored {
                timestamp: Utc::now(),
                user: "test".to_string(),
                location: 0,
                digest: digest2.clone(),
            },
            RegistryAction::HashTagged {
                timestamp: Utc::now(),
                user: "test".to_string(),
                repository,
                digest: digest2,
                tag: "latest".to_string(),
            },
        ])
        .await;

    let collected = state.store.get_orphaned_manifests().unwrap();
    assert_eq!(collected.len(), 1);

    let entry = collected.iter().next().unwrap();
    assert_eq!(entry.0, &digest1);
    assert!(entry.1.locations.contains(&0));
}

#[tokio::test]
#[traced_test]
async fn can_collect_orphaned_blobs() {
    let mut state = setup_state().await;

    // Create node
    let repository: RepositoryName = "myrepo".parse().unwrap();
    let digest1: Digest = "sha256:abcdefg".parse().unwrap();
    let digest2: Digest = "sha256:gfedcba".parse().unwrap();
    let digest3: Digest = "sha256:aaaaaaa".parse().unwrap();
    let digest4: Digest = "sha256:bbbbbbb".parse().unwrap();
    let manifest_digest: Digest = "sha256:ababababababa".parse().unwrap();

    state
        .dispatch_actions(vec![
            // BLOB 1 DAG
            RegistryAction::BlobMounted {
                timestamp: Utc::now(),
                user: "test".to_string(),
                repository: repository.clone(),
                digest: digest1.clone(),
            },
            RegistryAction::BlobStored {
                timestamp: Utc::now(),
                user: "test".to_string(),
                location: 0,
                digest: digest1.clone(),
            },
            RegistryAction::BlobMounted {
                timestamp: Utc::now(),
                user: "test".to_string(),
                repository: repository.clone(),
                digest: digest2.clone(),
            },
            RegistryAction::BlobStored {
                timestamp: Utc::now(),
                user: "test".to_string(),
                location: 0,
                digest: digest2.clone(),
            },
            RegistryAction::BlobInfo {
                timestamp: Utc::now(),
                digest: digest2.clone(),
                content_type: "foo".to_string(),
                dependencies: vec![digest1.clone()],
            },
            // BLOB 2 DAG
            RegistryAction::BlobMounted {
                timestamp: Utc::now(),
                user: "test".to_string(),
                repository: repository.clone(),
                digest: digest3.clone(),
            },
            RegistryAction::BlobStored {
                timestamp: Utc::now(),
                user: "test".to_string(),
                location: 0,
                digest: digest3.clone(),
            },
            RegistryAction::BlobMounted {
                timestamp: Utc::now(),
                user: "test".to_string(),
                repository: repository.clone(),
                digest: digest4.clone(),
            },
            RegistryAction::BlobStored {
                timestamp: Utc::now(),
                user: "test".to_string(),
                location: 0,
                digest: digest4.clone(),
            },
            RegistryAction::BlobInfo {
                timestamp: Utc::now(),
                digest: digest4.clone(),
                content_type: "foo".to_string(),
                dependencies: vec![digest3],
            },
            // MANIFEST DAG
            RegistryAction::ManifestMounted {
                timestamp: Utc::now(),
                user: "test".to_string(),
                repository: repository.clone(),
                digest: manifest_digest.clone(),
            },
            RegistryAction::ManifestStored {
                timestamp: Utc::now(),
                user: "test".to_string(),
                location: 0,
                digest: manifest_digest.clone(),
            },
            RegistryAction::ManifestInfo {
                timestamp: Utc::now(),
                digest: manifest_digest.clone(),
                content_type: "foo".to_string(),
                dependencies: vec![digest4],
            },
        ])
        .await;

    let collected = state.store.get_orphaned_blobs().unwrap();
    assert_eq!(collected.len(), 2);

    for (digest, blob) in collected.iter() {
        if digest == &digest1 {
            assert_eq!(blob.dependencies.as_ref().unwrap().len(), 0);
        } else if digest == &digest2 {
            assert_eq!(blob.dependencies.as_ref().unwrap().len(), 1);
        } else {
            panic!("Unexpected digest was collected")
        }
    }

    // If we delete the manifest all blobs should now be garbage collected

    state
        .dispatch_actions(vec![
            RegistryAction::ManifestUnmounted {
                timestamp: Utc::now(),
                user: "test".to_string(),
                repository,
                digest: manifest_digest.clone(),
            },
            RegistryAction::ManifestUnstored {
                timestamp: Utc::now(),
                user: "test".to_string(),
                location: 0,
                digest: manifest_digest,
            },
        ])
        .await;

    let collected = state.store.get_orphaned_blobs().unwrap();
    assert_eq!(collected.len(), 4);
}
