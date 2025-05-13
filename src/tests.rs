use std::{borrow::Cow, ops::Deref};

use anyhow::Result;
use axum::Router;
use hiqlite::{Node, NodeConfig};
use once_cell::sync::Lazy;
use prometheus_client::registry::Registry;
use tempfile::{TempDir, tempdir};
use tokio::{sync::Mutex, task::JoinSet};

use crate::{Migrations, webhook::WebhookService};

use super::*;

pub static EXCLUSIVE_TEST_LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

#[must_use = "Fixture must be used and `.teardown().await` must be called to ensure proper cleanup."]
pub(crate) struct StateFixture {
    _guard: Box<dyn std::any::Any + Send>,
    dirs: Vec<TempDir>,
    registries: Vec<Arc<RegistryState>>,
    tasks: JoinSet<Result<()>>,
}

impl StateFixture {
    pub(crate) async fn new() -> Result<Self> {
        let lock = EXCLUSIVE_TEST_LOCK.lock().await;
        unsafe {
            std::env::set_var("ENC_KEY_ACTIVE", "828W10qknpOT");
            std::env::set_var(
                "ENC_KEYS",
                "828W10qknpOT/CIneMTth3mnRZZq0PMtztfWrnU+5xeiS0jrTB8iq6xc=",
            );
        }

        let config = NodeConfig {
            secret_api: "aaaaaaaaaaaaaaaa".into(),
            secret_raft: "bbbbbbbbbbbbbbbb".into(),
            log_statements: true,
            nodes: vec![Node {
                id: 1,
                addr_raft: "127.0.0.1:9999".to_string(),
                addr_api: "127.0.0.1:9998".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        };

        let mut tasks = JoinSet::new();
        let mut registries = vec![];
        let mut dirs = vec![];

        for node in config.nodes.iter() {
            let dir = tempdir()?;
            let data_dir = dir.path();

            let mut registry = Registry::with_prefix("disty");

            let client = hiqlite::start_node(NodeConfig {
                node_id: node.id,
                data_dir: Cow::Owned(data_dir.to_string_lossy().into_owned()),
                ..config.clone()
            })
            .await?;

            dirs.push(dir);
            registries.push(Arc::new(RegistryState {
                node_id: node.id,
                client,
                extractor: Extractor::new(),
                webhooks: WebhookService::start(&mut tasks, vec![], &mut registry),
            }));
        }

        registries[0].client.wait_until_healthy_db().await;
        registries[0].client.migrate::<Migrations>().await?;

        Ok(StateFixture {
            dirs,
            registries,
            _guard: Box::new(lock),
            tasks,
        })
    }

    pub(crate) async fn teardown(mut self) -> Result<()> {
        for registry in self.registries {
            registry.client.shutdown().await?;
        }
        self.tasks.shutdown().await;
        Ok(())
    }
}

impl Deref for StateFixture {
    type Target = RegistryState;

    fn deref(&self) -> &Self::Target {
        &self.registries[0]
    }
}

pub(crate) struct RegistryFixture {
    state: StateFixture,
    pub router: Router<()>,
}

impl RegistryFixture {
    pub async fn new() -> Result<RegistryFixture> {
        let state = StateFixture::new().await?;

        let router = crate::router(state.registries[0].clone());

        Ok(RegistryFixture { state, router })
    }

    pub async fn teardown(self) -> Result<()> {
        Ok(self.state.teardown().await?)
    }
}
