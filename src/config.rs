use serde::{Serialize, Deserialize};
use figment::{Figment, providers::{Format, Yaml, Env, Serialized}};


#[derive(Deserialize, Serialize)]
pub struct RaftConfig {
    pub address: String,
    pub port: u32,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            address: "127.0.0.1".to_string(),
            port: 8080,
        }
    }
}

#[derive(Deserialize, Serialize)]
pub struct RegistryConfig {
    pub address: String,
    pub port: u32,
}

impl Default for RegistryConfig {
    fn default() -> Self {
        Self {
            address: "127.0.0.1".to_string(),
            port: 8000,
        }
    }
}


#[derive(Deserialize, Serialize)]
pub struct PrometheusConfig {
    pub address: String,
    pub port: u32,
}

impl Default for PrometheusConfig {
    fn default() -> Self {
        Self {
            address: "127.0.0.1".to_string(),
            port: 7080,
        }
    }
}

#[derive(Deserialize, Serialize)]
pub struct TokenConfig {
    pub issuer: String,
    pub service: String,
    pub realm: String,
    pub public_key: String,
}

#[derive(Deserialize, Serialize)]
pub struct PeerConfig {
    pub name: String,
    pub raft: RaftConfig,
    pub registry: RegistryConfig,
}

#[derive(Deserialize, Serialize)]
pub struct Configuration {
    raft: RaftConfig,
    registry: RegistryConfig,
    prometheus: PrometheusConfig,
    token_server: Option<TokenConfig>,
    storage: String,
    peers: Vec<PeerConfig>,
}

impl Default for Configuration {
    fn default() -> Self {
        Self {
            raft: RaftConfig::default(),
            registry: RegistryConfig::default(),
            prometheus: PrometheusConfig::default(),
            token_server: None,
            storage: "var".to_string(),
            peers: vec![],
        }
    }
}


pub fn config() -> Configuration {
    Figment::from(Serialized::defaults(Configuration::default()))
        .merge(Yaml::file("config.yaml"))
        .merge(Env::prefixed("DISTRIBD_"))
        .extract()
        .expect("Failed to load config.yaml")
}


#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn defaults() {
        let defaults: Configuration = Figment::from(Serialized::defaults(Configuration::default())).extract().unwrap();
        assert_eq!(defaults.raft.address, "127.0.0.1");
        assert!(defaults.peers.is_empty());
    }
}
