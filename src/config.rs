use std::path::PathBuf;

use jwt_simple::prelude::ES256PublicKey;
use platform_dirs::AppDirs;
use regex::Regex;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RaftConfig {
    pub address: String,
    pub port: u32,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            address: "0.0.0.0".to_string(),
            port: 8080,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RegistryConfig {
    pub address: String,
    pub port: u32,
}

impl Default for RegistryConfig {
    fn default() -> Self {
        Self {
            address: "0.0.0.0".to_string(),
            port: 8000,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PrometheusConfig {
    pub address: String,
    pub port: u32,
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
            let app_dirs = AppDirs::new(Some("distribd"), true).unwrap();
            let config_dir = app_dirs.config_dir;
            p = config_dir.join(p);
        }
        let pem = std::fs::read_to_string(&p).unwrap();
        Ok(PublicKey {
            path: s,
            public_key: ES256PublicKey::from_pem(&pem).unwrap(),
        })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TokenConfig {
    pub issuer: String,
    pub service: String,
    pub realm: String,
    pub public_key: PublicKey,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PeerConfig {
    pub name: String,
    pub raft: RaftConfig,
    pub registry: RegistryConfig,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MintConfig {
    pub realm: String,
    pub service: String,
    pub username: String,
    pub password: String,
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
pub struct Configuration {
    pub identifier: String,
    pub raft: RaftConfig,
    pub registry: RegistryConfig,
    pub prometheus: PrometheusConfig,
    pub token_server: Option<TokenConfig>,
    pub mirroring: Option<MintConfig>,
    pub storage: String,
    pub peers: Vec<PeerConfig>,
    pub webhooks: Vec<WebhookConfig>,
    pub scrubber: ScrubberConfig,
}

impl Default for Configuration {
    fn default() -> Self {
        Self {
            identifier: "localhost".to_string(),
            raft: RaftConfig::default(),
            registry: RegistryConfig::default(),
            prometheus: PrometheusConfig::default(),
            token_server: None,
            mirroring: None,
            storage: "var".to_string(),
            peers: vec![],
            webhooks: vec![],
            scrubber: ScrubberConfig::default(),
        }
    }
}

pub fn config(_config: Option<PathBuf>) -> Configuration {
    Configuration::default()
}
