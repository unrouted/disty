use std::future::Future;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use async_trait::async_trait;
use openraft::testing::StoreBuilder;
use openraft::testing::Suite;
use openraft::StorageError;

use crate::config::Configuration;
use crate::RegistryNodeId;
use crate::RegistryStore;
use crate::RegistryTypeConfig;

static GLOBAL_TEST_COUNT: AtomicUsize = AtomicUsize::new(0);

struct SledBuilder {}

#[test]
pub fn test_raft_store() -> Result<(), StorageError<RegistryNodeId>> {
    Suite::test_all(SledBuilder {})
}

#[async_trait]
impl StoreBuilder<RegistryTypeConfig, Arc<RegistryStore>> for SledBuilder {
    async fn run_test<Fun, Ret, Res>(&self, t: Fun) -> Result<Ret, StorageError<RegistryNodeId>>
    where
        Res: Future<Output = Result<Ret, StorageError<RegistryNodeId>>> + Send,
        Fun: Fn(Arc<RegistryStore>) -> Res + Sync + Send,
    {
        let pid = std::process::id();
        let td = tempfile::TempDir::new().expect("couldn't create temp dir");
        let temp_dir_path = td.path().to_str().expect("Could not convert temp dir");
        let r = {
            let old_count = GLOBAL_TEST_COUNT.fetch_add(1, Ordering::SeqCst);
            let db_dir_str = format!("{}pid{}/num{}/", &temp_dir_path, pid, old_count);

            let db_dir = std::path::Path::new(&db_dir_str);
            if !db_dir.exists() {
                std::fs::create_dir_all(db_dir)
                    .unwrap_or_else(|_| panic!("could not create: {:?}", db_dir.to_str()));
            }

            let db: sled::Db = sled::open(db_dir)
                .unwrap_or_else(|_| panic!("could not open: {:?}", db_dir.to_str()));

            let mut registry = <prometheus_client::registry::Registry>::default();
            let store =
                RegistryStore::new(Arc::new(db), Configuration::default(), &mut registry).await;
            let test_res = t(store).await;

            if db_dir.exists() {
                std::fs::remove_dir_all(db_dir).expect("Could not clean up test directory");
            }
            test_res
        };
        r
    }
}
