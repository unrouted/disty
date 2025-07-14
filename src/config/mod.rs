use std::{borrow::Cow, path::PathBuf, sync::Arc, time::Duration};

use acl::AccessRule;
use anyhow::{Context, Result, bail};
use figment::{
    Figment,
    providers::{Env, Format, Serialized, Yaml},
    value::magic::RelativePathBuf,
};
use hiqlite::{Node, NodeConfig, ServerTlsConfig, s3::EncKeys};
use jwt_simple::prelude::ES256KeyPair;
use p256::ecdsa::SigningKey;
use p256::pkcs8::EncodePrivateKey;
use platform_dirs::AppDirs;
use regex::Regex;
use sec1::DecodeEcPrivateKey;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{
    config::{files::RecursiveFileProvider, lifecycle::DeletionRule},
    jwt::JWKSPublicKey,
};

pub(crate) mod acl;
pub(crate) mod duration;
pub(crate) mod files;
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

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RaftConfig {
    pub address: String,
    pub secret: Option<String>,
    pub tls: Option<TlsConfig>,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            address: "0.0.0.0".to_string(),
            secret: None,
            tls: None,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ApiConfig {
    pub address: String,
    pub secret: Option<String>,
    pub tls: Option<TlsConfig>,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            address: "0.0.0.0".to_string(),
            secret: None,
            tls: None,
        }
    }
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

const fn default_timeout() -> Duration {
    Duration::from_secs(5)
}

const fn default_flush_interval() -> Duration {
    Duration::from_secs(5)
}

const fn default_retry_base() -> Duration {
    Duration::from_secs(5)
}

const fn default_batch_size() -> usize {
    10
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct WebhookConfig {
    #[serde(with = "serde_regex")]
    pub matcher: Regex,
    pub url: String,
    #[serde(with = "crate::config::duration", default = "default_timeout")]
    pub timeout: Duration,
    #[serde(with = "crate::config::duration", default = "default_flush_interval")]
    pub flush_interval: Duration,
    #[serde(with = "crate::config::duration", default = "default_retry_base")]
    pub retry_base: Duration,
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
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
    pub url: String,
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
    pub fn figment(configs: Vec<PathBuf>) -> Figment {
        let fig = Figment::from(Serialized::defaults(Configuration::default()));

        let app_dirs = AppDirs::new(Some("disty"), true).unwrap();
        let config_dir = app_dirs.config_dir;
        let config_path = config_dir.join("config.yaml");

        let fig = match config_path.exists() {
            true => fig.admerge(RecursiveFileProvider::new(Yaml::file(config_path))),
            false => fig,
        };

        let fig = configs.into_iter().fold(fig, |fig, config_path| {
            fig.admerge(RecursiveFileProvider::new(Yaml::file(config_path)))
        });

        fig.admerge(RecursiveFileProvider::new(Env::prefixed("DISTY_")))
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
            listen_addr_api: Cow::Owned(value.api.address),
            tls_api: match value.api.tls {
                Some(tls) => Some(ServerTlsConfig {
                    key: tls.key.into(),
                    cert: tls.chain.into(),
                    danger_tls_no_verify: false,
                }),
                None => None,
            },
            listen_addr_raft: Cow::Owned(value.raft.address),
            tls_raft: match value.raft.tls {
                Some(tls) => Some(ServerTlsConfig {
                    key: tls.key.into(),
                    cert: tls.chain.into(),
                    danger_tls_no_verify: false,
                }),
                None => None,
            },
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
            url: "http://localhost".into(),
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
            Configuration::config(Configuration::figment(vec![]).join(("node_id", 1)).join((
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

    /// Should be able to stack config files and have acls merged
    #[test]
    fn stacking() {
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
                    "key_pair_file": "token.key",
                    "users": [],
                    "acls": []
                  }
                }
                "#,
            )?;

            jail.create_file(
                "auth1.yaml",
                r#"
                {
                  "authentication": {
                    "acls": [
                        {
                            "subject": {
                                "username": "bob"
                            },
                            "actions": ["push", "pull"]
                        }
                    ]
                  }
                }
                "#,
            )?;

            jail.create_file(
                "auth2.yaml",
                r#"
                {
                  "authentication": {
                    "acls": [
                        {
                            "subject": {
                                "username": "admin"
                            },
                            "actions": ["push", "pull"]
                        }
                    ]
                  }
                }
                "#,
            )?;

            jail.create_file(
                "auth3.yaml",
                r#"
                {
                  "authentication": {
                    "acls": []
                  }
                }
                "#,
            )?;

            let config: Configuration = Configuration::figment(vec![
                jail.directory().join("config.yaml"),
                jail.directory().join("auth1.yaml"),
                jail.directory().join("auth2.yaml"),
                jail.directory().join("auth3.yaml"),
            ])
            .extract()
            .expect("Configuration should be parseable");

            assert_eq!(config.authentication.unwrap().acls.len(), 2);

            Ok(())
        });
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
                    "key_pair_file": "token.key",
                    "users": [],
                    "acls": []
                  }
                }
                "#,
            )?;

            let path = jail.directory().join("config.yaml");

            let config: Configuration = Configuration::figment(vec![path])
                .extract()
                .expect("Configuration should be parseable");

            let t = config
                .authentication
                .as_ref()
                .expect("Authentication shouldn't be empty");

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
