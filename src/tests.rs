use std::{ops::Deref, sync::Mutex};

use anyhow::{Context, Result};
use axum::{Router, body::Body, http::Request, response::Response};
use figment::value::magic::RelativePathBuf;
use jwt_simple::prelude::ES256KeyPair;
use once_cell::sync::Lazy;
use prometheus_client::registry::Registry;
use std::{collections::HashSet, sync::Arc};
use tempfile::{TempDir, tempdir};
use tokio::task::JoinSet;
use tower::ServiceExt;

use crate::{
    Cache, Migrations,
    config::{
        ApiConfig, AuthenticationConfig, Configuration, DistyNode, KeyPair, RaftConfig, User,
        acl::AccessRule, lifecycle::DeletionRule,
    },
    issuer::issue_token,
    token::Access,
    webhook::WebhookService,
};

use super::*;

/// A thread-safe pool of TCP ports.
#[derive(Clone)]
pub struct PortPool {
    available: Arc<Mutex<HashSet<u64>>>,
}

impl PortPool {
    /// Create a new pool of ports in the range [start, end].
    pub fn new() -> Self {
        let set: HashSet<u64> = (1..=100).collect();
        Self {
            available: Arc::new(Mutex::new(set)),
        }
    }

    /// Acquire a free port from the pool.
    /// Returns None if none available.
    pub fn acquire(&self) -> Option<LeasedPort> {
        let mut available = self.available.lock().unwrap();
        if let Some(&offset) = available.iter().next() {
            available.remove(&offset);
            Some(LeasedPort {
                offset,
                pool: self.available.clone(),
            })
        } else {
            None
        }
    }
}

/// RAII guard that returns the port back to the pool on drop.
pub struct LeasedPort {
    offset: u64,
    pool: Arc<Mutex<HashSet<u64>>>,
}

impl LeasedPort {
    pub fn raft_port(&self) -> u64 {
        10000 + (30 * self.offset)
    }

    pub fn api_port(&self) -> u64 {
        10000 + (30 * self.offset) + 10
    }

    pub fn registry_port(&self) -> u64 {
        10000 + (30 * self.offset) + 20
    }
}

impl Drop for LeasedPort {
    fn drop(&mut self) {
        let mut available = self.pool.lock().unwrap();
        available.insert(self.offset);
    }
}

/// Global port pool from 10000 to 11000 (customize as you want)
pub static GLOBAL_PORT_POOL: Lazy<PortPool> = Lazy::new(PortPool::new);

/// Acquire a port from the global pool, panic if none available
pub fn acquire_port() -> LeasedPort {
    GLOBAL_PORT_POOL
        .acquire()
        .expect("No ports available in the pool")
}

pub struct FixtureBuilder {
    pub cluster_size: u64,
    pub authentication: bool,
    pub users: Vec<User>,
    pub acls: Vec<AccessRule>,
    pub cleanup: Vec<DeletionRule>,
}

impl FixtureBuilder {
    pub fn new() -> Self {
        FixtureBuilder {
            cluster_size: 1,
            authentication: false,
            users: vec![],
            acls: vec![],
            cleanup: vec![],
        }
    }

    pub fn cluster_size(mut self, size: u64) -> Self {
        self.cluster_size = size;
        self
    }

    pub fn authenticated(mut self, authenticated: bool) -> Self {
        self.authentication = authenticated;
        self
    }

    pub fn user(mut self, user: User) -> Self {
        self.users.push(user);
        self
    }

    pub fn acl(mut self, acl: AccessRule) -> Self {
        self.acls.push(acl);
        self
    }

    pub fn cleanup(mut self, rule: DeletionRule) -> Self {
        self.cleanup.push(rule);
        self
    }

    pub async fn build(self) -> Result<StateFixture> {
        StateFixture::with_builder(self).await
    }
}

#[must_use = "Fixture must be used and `.teardown().await` must be called to ensure proper cleanup."]
pub(crate) struct StateFixture {
    _ports: LeasedPort,
    _dirs: Vec<TempDir>,
    pub registries: Vec<Arc<RegistryState>>,
    tasks: JoinSet<Result<()>>,
}

impl StateFixture {
    pub(crate) async fn new() -> Result<Self> {
        FixtureBuilder::new().cluster_size(1).build().await
    }

    pub async fn with_builder(builder: FixtureBuilder) -> Result<Self> {
        let port_range = acquire_port();
        let raft_port = port_range.raft_port();
        let api_port = port_range.api_port();
        let registry_port = port_range.registry_port();

        let authentication = match builder.authentication {
            true => Some(AuthenticationConfig {
                issuer: "some-issuer".into(),
                audience: "some-audience".into(),
                realm: "fixme".into(),
                key_pair: KeyPair {
                    original: "".into(),
                    key_pair: Arc::new(ES256KeyPair::generate()),
                },
                users: builder.users.clone(),
                acls: builder.acls.clone(),
            }),
            false => None,
        };

        let nodes = (0..builder.cluster_size)
            .map(|idx| DistyNode {
                id: (idx + 1),
                addr_api: format!("127.0.0.1:{}", api_port + idx),
                addr_raft: format!("127.0.0.1:{}", raft_port + idx),
                addr_registry: format!("127.0.0.1:{}", registry_port + idx),
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
                storage: RelativePathBuf::from(data_dir),
                raft: RaftConfig {
                    address: "127.0.0.1".into(),
                    secret: Some("aaaaaaaaaaaaaaaa".into()),
                    ..Default::default()
                },
                api: ApiConfig {
                    address: "127.0.0.1".into(),
                    secret: Some("bbbbbbbbbbbbbbbb".into()),
                    ..Default::default()
                },
                nodes: nodes.clone(),
                authentication: authentication.clone(),
                cleanup: builder.cleanup.clone(),
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
                webhooks: WebhookService::start(&mut tasks, vec![], &mut registry),
                registry,
            }));
        }

        registries[0].client.wait_until_healthy_db().await;
        registries[0].client.migrate::<Migrations>().await?;

        Ok(StateFixture {
            _ports: port_range,
            _dirs: dirs,
            registries,
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
    pub state: StateFixture,
    pub router: Router<()>,
}

impl RegistryFixture {
    pub async fn new() -> Result<RegistryFixture> {
        let state = StateFixture::new().await?;
        Self::with_state(state)
    }

    pub fn with_state(state: StateFixture) -> Result<RegistryFixture> {
        let router = crate::router(state.registries[0].clone());
        Ok(RegistryFixture { state, router })
    }

    pub fn bearer_header(&self, access: Vec<Access>) -> Result<String> {
        let config = self
            .state
            .config
            .authentication
            .as_ref()
            .context("Authentication not configured")?;
        let token = issue_token(config, "test", access)?.token;
        Ok(format!("Bearer {token}"))
    }

    pub async fn request(&self, req: Request<Body>) -> Result<Response> {
        let app = registry::RewriteUriLayer {}.layer(self.router.clone());
        app.oneshot(req)
            .await
            .context("Failed to make test request")
    }

    pub async fn teardown(self) -> Result<()> {
        self.state.teardown().await
    }
}
