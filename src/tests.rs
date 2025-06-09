use std::ops::Deref;

use anyhow::{Context, Result};
use axum::{Router, body::Body, http::Request, response::Response};
use figment::value::magic::RelativePathBuf;
use jwt_simple::prelude::ES256KeyPair;
use once_cell::sync::Lazy;
use prometheus_client::registry::Registry;
use tempfile::{TempDir, tempdir};
use tokio::{sync::Mutex, task::JoinSet};
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

pub static EXCLUSIVE_TEST_LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

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
    _guard: Box<dyn std::any::Any + Send>,
    dirs: Vec<TempDir>,
    pub registries: Vec<Arc<RegistryState>>,
    tasks: JoinSet<Result<()>>,
}

impl StateFixture {
    pub(crate) async fn new() -> Result<Self> {
        FixtureBuilder::new().cluster_size(1).build().await
    }

    pub async fn with_builder(builder: FixtureBuilder) -> Result<Self> {
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

        let lock = EXCLUSIVE_TEST_LOCK.lock().await;
        unsafe {
            std::env::set_var("ENC_KEY_ACTIVE", "828W10qknpOT");
            std::env::set_var(
                "ENC_KEYS",
                "828W10qknpOT/CIneMTth3mnRZZq0PMtztfWrnU+5xeiS0jrTB8iq6xc=",
            );
        }

        let nodes = (0..builder.cluster_size)
            .map(|idx| DistyNode {
                id: (idx + 1),
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
                storage: RelativePathBuf::from(data_dir),
                raft: RaftConfig {
                    secret: Some("aaaaaaaaaaaaaaaa".into()),
                    ..Default::default()
                },
                api: ApiConfig {
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
