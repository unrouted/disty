use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    net::IpAddr,
    path::PathBuf,
    sync::Arc,
};

use anyhow::{Context, Result, bail};
use figment::{
    Figment,
    providers::{Env, Format, Serialized, Yaml},
};
use hiqlite::{Node, NodeConfig};
use ip_network::IpNetwork;
use jwt_simple::prelude::{ES256KeyPair, ES256PublicKey};
use p256::ecdsa::SigningKey;
use p256::pkcs8::EncodePrivateKey;
use platform_dirs::AppDirs;
use regex::Regex;
use sec1::DecodeEcPrivateKey;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use x509_parser::prelude::Pem;

use crate::jwt::JWKSPublicKey;

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
            port: 7080,
        }
    }
}

#[derive(Clone, Debug)]
pub struct PublicKey {
    pub path: String,
    pub public_key: ES256PublicKey,
}

impl Serialize for PublicKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.path)
    }
}

impl<'de> Deserialize<'de> for PublicKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: String = Deserialize::deserialize(deserializer)?;
        let mut p = PathBuf::from(s.clone());
        if p.is_relative() {
            let app_dirs = AppDirs::new(Some("disty"), true).unwrap();
            let config_dir = app_dirs.config_dir;
            p = config_dir.join(p);
        }
        println!("{:?}", p);
        let pem = std::fs::read_to_string(&p).unwrap();

        let public_key = match ES256PublicKey::from_pem(&pem) {
            Ok(public_key) => public_key,
            Err(_) => {
                let pem = Pem::iter_from_buffer(pem.as_bytes())
                    .next()
                    .unwrap()
                    .unwrap();
                let x509 = pem.parse_x509().expect("X.509: decoding DER failed");
                let raw = &x509.public_key().subject_public_key.data;

                ES256PublicKey::from_bytes(raw).unwrap()
            }
        };

        Ok(PublicKey {
            path: s,
            public_key,
        })
    }
}

#[derive(Clone)]
pub struct KeyPair {
    pub path: String,
    pub key_pair: Arc<ES256KeyPair>,
}

impl std::fmt::Debug for KeyPair {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyPair").field("path", &self.path).finish()
    }
}

impl Serialize for KeyPair {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.path)
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
        let s: String = Deserialize::deserialize(deserializer)?;
        let mut p = PathBuf::from(s.clone());
        if p.is_relative() {
            let app_dirs = AppDirs::new(Some("disty"), true).unwrap();
            let config_dir = app_dirs.config_dir;
            p = config_dir.join(p);
        }
        let pem = std::fs::read_to_string(&p).unwrap();

        let key_pair = Arc::new(load_keypair(&pem).unwrap());

        Ok(KeyPair { path: s, key_pair })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TokenConfig {
    pub issuer: String,
    pub service: String,
    pub realm: String,
    pub public_key: PublicKey,
    pub key_pair: KeyPair,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct JwtRule {
    pub issuer: Option<String>,
    pub claims: Option<HashMap<String, String>>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MatchRule {
    /// Name of the registry
    pub repository: Option<String>,

    /// IP address that token can be requested from
    pub network: Option<IpNetwork>,

    /// Username or token subject
    pub username: Option<String>,

    /// JWT specific matching rules
    pub jwt: Option<JwtRule>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AccessRule {
    pub check: MatchRule,
    pub actions: HashSet<String>,
    pub comment: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Issuer {
    pub issuer: String,
    pub service: String,
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
        issuer: JWKSPublicKey
    }
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

pub fn deserialize_absolute<'de, D>(deserializer: D) -> Result<PathBuf, D::Error>
where
    D: Deserializer<'de>,
{
    let raw_path: String = Deserialize::deserialize(deserializer)?;
    let path = PathBuf::from(raw_path);

    let abs_path = if path.is_absolute() {
        path
    } else {
        std::env::current_dir()
            .map_err(serde::de::Error::custom)?
            .join(path)
    };

    // Try to canonicalize, but fall back if it fails (e.g. file doesn't exist yet)
    match std::fs::canonicalize(&abs_path) {
        Ok(p) => Ok(p),
        Err(_) => Ok(abs_path),
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Configuration {
    pub node_id: u64,
    pub nodes: Vec<DistyNode>,
    pub raft: RaftConfig,
    pub api: ApiConfig,
    pub prometheus: PrometheusConfig,
    pub token_server: Option<TokenConfig>,
    pub issuer: Option<Issuer>,
    #[serde(deserialize_with = "deserialize_absolute")]
    pub storage: PathBuf,
    pub webhooks: Vec<WebhookConfig>,
    pub scrubber: ScrubberConfig,
    pub sentry: Option<SentryConfig>,
}

impl Configuration {
    pub fn figment(config: Option<PathBuf>) -> Figment {
        let app_dirs = AppDirs::new(Some("disty"), true).unwrap();
        let config_dir = app_dirs.config_dir;
        let config_path = config_dir.join("config.yaml");

        let fig = Figment::from(Serialized::defaults(Configuration::default()));

        match config {
            Some(config_path) => fig.merge(Yaml::file(config_path)),
            None => match config_path.exists() {
                true => fig.merge(Yaml::file(config_path)),
                false => fig,
            },
        }
        .merge(Env::prefixed("DISTY_"))
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

        println!("{:?}", config);

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
            data_dir: Cow::Owned(value.storage.join("hiqlite").to_string_lossy().into_owned()),
            secret_raft: value
                .raft
                .secret
                .context("You must provide a raft secret")?,
            secret_api: value.api.secret.context("You must provide an API secret")?,
            log_statements: true,
            ..Default::default()
        })
    }
}

impl Default for Configuration {
    fn default() -> Self {
        Self {
            node_id: 0,
            raft: RaftConfig::default(),
            api: ApiConfig::default(),
            prometheus: PrometheusConfig::default(),
            token_server: None,
            issuer: None,
            storage: "var".to_string().into(),
            webhooks: vec![],
            scrubber: ScrubberConfig::default(),
            sentry: None,
            nodes: vec![],
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
    fn token_config() {
        unsafe {
            std::env::set_var(
                "XDG_CONFIG_HOME",
                std::env::current_dir()
                    .unwrap()
                    .join("fixtures/etc")
                    .as_os_str(),
            );
        }

        let data = r#"
        {
            "issuer": "Test Issuer",
            "realm": "testrealm",
            "service": "myservice",
            "public_key": "token.pub",
            "key_pair": "token.key"
        }"#;

        let t: TokenConfig = serde_json::from_str(data).unwrap();

        assert_eq!(t.issuer, "Test Issuer");
        assert_eq!(t.realm, "testrealm");
        assert_eq!(t.service, "myservice");
        assert_eq!(t.public_key.path, "token.pub");
        assert_eq!(
            t.public_key.public_key.to_pem().unwrap(),
            "-----BEGIN PUBLIC KEY-----\nMFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEPEUDSJJ2ThQmq1py0QUp1VHfLxOS\nGjl1uDis2P2rq3YWN96TDWgYbmk4v1Fd3sznlgTnM7cZ22NrrdKvM4TmVg==\n-----END PUBLIC KEY-----\n"
        );
    }

    #[test]
    fn token_confi_cert() {
        unsafe {
            std::env::set_var(
                "XDG_CONFIG_HOME",
                std::env::current_dir()
                    .unwrap()
                    .join("fixtures/etc")
                    .as_os_str(),
            );
        }

        let data = r#"
        {
            "issuer": "Test Issuer",
            "realm": "testrealm",
            "service": "myservice",
            "public_key": "token.crt",
            "key_pair": "token.key"
        }"#;

        let t: TokenConfig = serde_json::from_str(data).unwrap();

        assert_eq!(t.issuer, "Test Issuer");
        assert_eq!(t.realm, "testrealm");
        assert_eq!(t.service, "myservice");
        assert_eq!(t.public_key.path, "token.crt");
        assert_eq!(
            t.public_key.public_key.to_pem().unwrap(),
            "-----BEGIN PUBLIC KEY-----\nMFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEPEUDSJJ2ThQmq1py0QUp1VHfLxOS\nGjl1uDis2P2rq3YWN96TDWgYbmk4v1Fd3sznlgTnM7cZ22NrrdKvM4TmVg==\n-----END PUBLIC KEY-----\n"
        );
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
