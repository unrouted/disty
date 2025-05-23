use std::{ops::Deref, path::PathBuf};

use anyhow::{Context, Result};
use axum::{Router, body::Body, http::Request, response::Response};
use once_cell::sync::Lazy;
use prometheus_client::registry::Registry;
use tempfile::{TempDir, tempdir};
use tokio::{sync::Mutex, task::JoinSet};
use tower::ServiceExt;

use crate::{
    Cache, Migrations,
    config::{ApiConfig, Configuration, DistyNode, RaftConfig},
    webhook::WebhookService,
};

use super::*;

pub static EXCLUSIVE_TEST_LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

#[must_use = "Fixture must be used and `.teardown().await` must be called to ensure proper cleanup."]
pub(crate) struct StateFixture {
    _guard: Box<dyn std::any::Any + Send>,
    dirs: Vec<TempDir>,
    pub registries: Vec<Arc<RegistryState>>,
    tasks: JoinSet<Result<()>>,
}

impl StateFixture {
    pub(crate) async fn new() -> Result<Self> {
        Self::with_size(1).await
    }

    pub(crate) async fn with_size(cluster_size: usize) -> Result<Self> {
        let lock = EXCLUSIVE_TEST_LOCK.lock().await;
        unsafe {
            std::env::set_var("ENC_KEY_ACTIVE", "828W10qknpOT");
            std::env::set_var(
                "ENC_KEYS",
                "828W10qknpOT/CIneMTth3mnRZZq0PMtztfWrnU+5xeiS0jrTB8iq6xc=",
            );
        }

        let nodes = (0..cluster_size)
            .into_iter()
            .map(|idx| DistyNode {
                id: (idx + 1) as u64,
                addr_api: format!("127.0.0.1:{}", 9999 - 3 * idx),
                addr_raft: format!("127.0.0.1:{}", 9999 - 3 * idx - 1),
                addr_registry: format!("127.0.0.1:{}", 9999 - 3 * idx - 2),
            })
            .collect::<Vec<DistyNode>>();

        let mut tasks = JoinSet::new();
        let mut registries = vec![];
        let mut dirs = vec![];

        for node in nodes.iter() {
            let dir = tempdir()?;
            let data_dir = dir.path();

            let configuration = Configuration {
                node_id: node.id,
                storage: PathBuf::from(data_dir),
                raft: RaftConfig {
                    secret: Some("aaaaaaaaaaaaaaaa".into()),
                    ..Default::default()
                },
                api: ApiConfig {
                    secret: Some("bbbbbbbbbbbbbbbb".into()),
                    ..Default::default()
                },
                nodes: nodes.clone(),
                ..Default::default()
            };

            let mut registry = Registry::with_prefix("disty");

            let client =
                hiqlite::start_node_with_cache::<Cache>(configuration.clone().try_into()?).await?;

            dirs.push(dir);
            registries.push(Arc::new(RegistryState {
                node_id: node.id,
                config: configuration,
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

    pub async fn request(&self, req: Request<Body>) -> Result<Response> {
        self.router
            .clone()
            .oneshot(req)
            .await
            .context("Failed to make test request")
    }

    pub async fn teardown(self) -> Result<()> {
        self.state.teardown().await
    }
}
