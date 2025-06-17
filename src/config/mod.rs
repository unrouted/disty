use std::{borrow::Cow, path::PathBuf, sync::Arc};

use acl::AccessRule;
use anyhow::{Context, Result, bail};
use figment::{
    Figment,
    providers::{Env, Format, Serialized, Yaml},
    value::magic::RelativePathBuf,
};
use figment_file_provider_adapter::FileAdapter;
use hiqlite::{Node, NodeConfig, s3::EncKeys};
use jwt_simple::prelude::ES256KeyPair;
use p256::ecdsa::SigningKey;
use p256::pkcs8::EncodePrivateKey;
use platform_dirs::AppDirs;
use regex::Regex;
use sec1::DecodeEcPrivateKey;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{config::lifecycle::DeletionRule, jwt::JWKSPublicKey};

pub(crate) mod acl;
pub(crate) mod lifecycle;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistyNode {
    pub id: u64,
    pub addr_raft: String,
    pub addr_api: String,
    pub addr_registry: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TlsConfig {
    pub key: String,
    pub chain: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, Default)]
pub struct RaftConfig {
    pub secret: Option<String>,
    pub tls: Option<TlsConfig>,
}

#[derive(Clone, Debug, Deserialize, Serialize, Default)]
pub struct ApiConfig {
    pub secret: Option<String>,
    pub tls: Option<TlsConfig>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PrometheusConfig {
    pub address: String,
    pub port: u16,
}

impl Default for PrometheusConfig {
    fn default() -> Self {
        Self {
            address: "0.0.0.0".to_string(),
            port: 9080,
        }
    }
}

#[derive(Clone)]
pub struct KeyPair {
    pub original: String,
    pub key_pair: Arc<ES256KeyPair>,
}

impl std::fmt::Debug for KeyPair {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyPair")
            .field("original", &self.original)
            .finish()
    }
}

impl Serialize for KeyPair {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.original)
    }
}

fn load_keypair(pem: &str) -> Result<ES256KeyPair> {
    let signing_key = SigningKey::from_sec1_pem(pem)?;
    let der = signing_key.to_pkcs8_der()?;
    ES256KeyPair::from_der(der.as_bytes())
}

impl<'de> Deserialize<'de> for KeyPair {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let pem: String = Deserialize::deserialize(deserializer)?;
        let key_pair = Arc::new(load_keypair(&pem).unwrap());

        Ok(KeyPair {
            original: pem,
            key_pair,
        })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct AuthenticationConfig {
    pub issuer: String,
    pub audience: String,
    pub realm: String,
    pub key_pair: KeyPair,
    pub users: Vec<User>,
    pub acls: Vec<AccessRule>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum User {
    Password {
        username: String,
        password: String,
    },
    Token {
        username: String,
        issuer: JWKSPublicKey,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct WebhookConfig {
    pub url: String,

    #[serde(with = "serde_regex")]
    pub matcher: Regex,
}

#[derive(Clone, Debug, Deserialize, Serialize, Default)]
pub struct ScrubberConfig {
    pub enabled: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SentryConfig {
    pub endpoint: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct BackupEncryptionKey {
    pub id: String,
    #[serde(with = "crate::jwt::base64url")]
    pub key: Vec<u8>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct BackupEncryptionConfig {
    pub keys: Vec<BackupEncryptionKey>,
    pub active: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct BackupConfig {
    encryption: BackupEncryptionConfig,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct Configuration {
    pub node_id: u64,
    pub nodes: Vec<DistyNode>,
    pub raft: RaftConfig,
    pub api: ApiConfig,
    pub prometheus: PrometheusConfig,
    pub authentication: Option<AuthenticationConfig>,
    #[serde(serialize_with = "RelativePathBuf::serialize_original")]
    pub storage: RelativePathBuf,
    pub webhooks: Vec<WebhookConfig>,
    pub scrubber: ScrubberConfig,
    pub sentry: Option<SentryConfig>,
    pub cleanup: Vec<DeletionRule>,
    pub backups: BackupConfig,
}

impl Configuration {
    pub fn figment(config: Option<PathBuf>) -> Figment {
        let app_dirs = AppDirs::new(Some("disty"), true).unwrap();
        let config_dir = app_dirs.config_dir;
        let config_path = config_dir.join("config.yaml");

        let fig = Figment::from(Serialized::defaults(Configuration::default()));

        match config {
            Some(config_path) => fig.merge(FileAdapter::wrap(Yaml::file(config_path))),
            None => match config_path.exists() {
                true => fig.merge(FileAdapter::wrap(Yaml::file(config_path))),
                false => fig,
            },
        }
        .merge(FileAdapter::wrap(Env::prefixed("DISTY_")))
    }

    pub fn config(figment: Figment) -> Result<Configuration> {
        let mut config: Configuration =
            figment.extract().context("Failed to load configuration")?;

        if config.node_id == 0 {
            let binding = hostname::get().expect("Cannot read hostname");
            let hostname = binding.to_str().expect("Invalid hostname format");

            let (_, node_id) = hostname
                .rsplit_once('-')
                .context("Hostname does not does not contain a dash")?;

            let node_id: u64 = node_id
                .parse()
                .context("Hostname does not end with a number")?;

            config.node_id = node_id + 1;
        }

        if config.node_id < 1 {
            bail!("node_id must be at least 1");
        }

        if config.node_id > (config.nodes.len() as u64) {
            bail!("node_id greater than number of configured nodes");
        }

        Ok(config)
    }
}

impl TryFrom<Configuration> for NodeConfig {
    type Error = anyhow::Error;

    fn try_from(value: Configuration) -> std::result::Result<Self, Self::Error> {
        let nodes = value
            .nodes
            .iter()
            .map(|n| Node {
                id: n.id,
                addr_api: n.addr_api.clone(),
                addr_raft: n.addr_raft.clone(),
            })
            .collect();

        Ok(Self {
            node_id: value.node_id,
            nodes,
            data_dir: Cow::Owned(
                value
                    .storage
                    .relative()
                    .join("hiqlite")
                    .to_string_lossy()
                    .into_owned(),
            ),
            secret_raft: value
                .raft
                .secret
                .context("You must provide a raft secret")?,
            secret_api: value.api.secret.context("You must provide an API secret")?,
            enc_keys: EncKeys {
                enc_key_active: value.backups.encryption.active.clone(),
                enc_keys: value
                    .backups
                    .encryption
                    .keys
                    .iter()
                    .map(|key| (key.id.clone(), key.key.clone()))
                    .collect(),
            },
            log_statements: true,
            ..Default::default()
        })
    }
}

impl Default for Configuration {
    fn default() -> Self {
        let random_keys = EncKeys::generate().unwrap();
        let first_key = random_keys.enc_keys.into_iter().next().unwrap();

        Self {
            node_id: 0,
            raft: RaftConfig::default(),
            api: ApiConfig::default(),
            prometheus: PrometheusConfig::default(),
            authentication: None,
            storage: "var".to_string().into(),
            webhooks: vec![],
            scrubber: ScrubberConfig::default(),
            sentry: None,
            nodes: vec![],
            cleanup: vec![],
            backups: BackupConfig {
                encryption: BackupEncryptionConfig {
                    active: random_keys.enc_key_active,
                    keys: vec![BackupEncryptionKey {
                        id: first_key.0,
                        key: first_key.1,
                    }],
                },
            },
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn entrypoint() {
        unsafe {
            std::env::set_var(
                "XDG_CONFIG_HOME",
                std::env::current_dir()
                    .unwrap()
                    .join("fixtures/etc")
                    .as_os_str(),
            );
        }

        let config =
            Configuration::config(Configuration::figment(None).join(("node_id", 1)).join((
                "nodes",
                vec![DistyNode {
                    id: 1,
                    addr_raft: "127.0.0.1:9999".into(),
                    addr_api: "127.0.0.1:9998".into(),
                    addr_registry: "127.0.0.1:9997".into(),
                }],
            )))
            .unwrap();
        assert_eq!(config.node_id, 1);
    }

    #[test]
    fn defaults() {
        let defaults = Configuration::default();
        assert_eq!(defaults.raft.secret, None);
    }

    #[test]
    fn config_issuer() {
        figment::Jail::expect_with(|jail| {
            jail.create_file(
                "token.key",
                include_str!("../../fixtures/etc/disty/token.key"),
            )?;

            jail.create_file(
                "config.yaml",
                r#"
                {
                  "authentication": {
                    "issuer": "Test Issuer",
                    "realm": "testrealm",
                    "audience": "myservice",
                    "key_pair_file": "token.key",
                    "users": [],
                    "acls": []
                  }
                }
                "#,
            )?;

            let path = jail.directory().join("config.yaml");

            let config: Configuration = Configuration::figment(Some(path))
                .extract()
                .expect("Configuration should be parseable");

            let t = config
                .authentication
                .as_ref()
                .expect("Authentication shouldn't be empty");

            assert_eq!(t.issuer, "Test Issuer");
            assert_eq!(t.realm, "testrealm");
            assert_eq!(t.audience, "myservice");
            assert_eq!(
                t.key_pair.key_pair.public_key().to_pem().unwrap(),
                "-----BEGIN PUBLIC KEY-----\nMFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEPEUDSJJ2ThQmq1py0QUp1VHfLxOS\nGjl1uDis2P2rq3YWN96TDWgYbmk4v1Fd3sznlgTnM7cZ22NrrdKvM4TmVg==\n-----END PUBLIC KEY-----\n"
            );

            Ok(())
        });
    }

    #[test]
    fn webhook_config() {
        let data = r#"
        {
            "url": "http://localhost:1234",
            "matcher": "matcher.*"
        }"#;

        let t: WebhookConfig = serde_json::from_str(data).unwrap();

        assert_eq!(t.url, "http://localhost:1234");
        assert!(!t.matcher.is_match("testrealm"));
        assert!(t.matcher.is_match("matcherZ"));
    }
}
