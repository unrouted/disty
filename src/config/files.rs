use figment::{
    Error, Metadata, Profile, Provider,
    value::{Dict, Map, Tag, Value},
};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

pub struct RecursiveFileProvider<P> {
    inner: P,
}

impl<P> RecursiveFileProvider<P> {
    pub fn new(inner: P) -> Self {
        Self { inner }
    }
}

impl<P: Provider> Provider for RecursiveFileProvider<P> {
    fn metadata(&self) -> Metadata {
        self.inner.metadata()
    }

    fn data(&self) -> Result<Map<Profile, Dict>, Error> {
        let data = self.inner.data()?;
        let mut out = Map::new();

        // Figure out base_path dynamically
        let base_path = self
            .inner
            .metadata()
            .source
            .as_ref()
            .and_then(|s| match s {
                figment::Source::File(path) => path.parent().map(|p| p.to_path_buf()),
                _ => None,
            })
            .unwrap_or_else(|| PathBuf::from("."));

        for (profile, dict) in data {
            let transformed = transform_dict(&base_path, dict)?;
            out.insert(profile, transformed);
        }

        Ok(out)
    }

    fn profile(&self) -> Option<Profile> {
        self.inner.profile()
    }
}

fn transform_dict(base_path: &PathBuf, dict: Dict) -> Result<Dict, Error> {
    let mut new_dict = BTreeMap::new();

    for (k, v) in dict {
        let new_v = transform_value(base_path, v)?;

        if k.ends_with("_file") {
            if let Value::String(_, path_str) = &new_v {
                let path = Path::new(path_str);
                let final_path = if path.is_absolute() {
                    path.to_path_buf()
                } else {
                    base_path.join(path)
                };
                let contents = std::fs::read_to_string(path).map_err(|e| {
                    Error::from(format!("Failed to read '{:?}': {}", final_path, e))
                })?;
                let new_key = k.trim_end_matches("_file").to_string();
                // Use Tag::Default or copy the original tag
                new_dict.insert(new_key, Value::String(Tag::Default, contents));
            }
        }

        new_dict.insert(k, new_v);
    }

    Ok(new_dict)
}

fn transform_value(base_path: &PathBuf, value: Value) -> Result<Value, Error> {
    match value {
        Value::Dict(tag, dict) => {
            let transformed = transform_dict(base_path, dict)?;
            Ok(Value::Dict(tag, transformed))
        }
        Value::Array(tag, arr) => {
            let new_arr: Result<Vec<_>, _> = arr
                .into_iter()
                .map(|v| transform_value(base_path, v))
                .collect();
            Ok(Value::Array(tag, new_arr?))
        }
        other => Ok(other),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use figment::{Figment, providers::Serialized};
    use std::fs::write;
    use tempfile::tempdir;

    #[derive(Debug, serde::Deserialize)]
    struct Server {
        name: String,
        cert: String,
    }

    #[derive(Debug, serde::Deserialize)]
    struct Config {
        servers: Vec<Server>,
    }

    #[test]
    fn test_recursive_file_provider_on_vec() {
        let dir = tempdir().unwrap();
        let cert_a = dir.path().join("a.pem");
        let cert_b = dir.path().join("b.pem");
        write(&cert_a, "CERT_A_CONTENT").unwrap();
        write(&cert_b, "CERT_B_CONTENT").unwrap();

        // Fake config structure matching config.toml
        let config = serde_json::json!({
            "servers": [
                {
                    "name": "server-a",
                    "cert_file": cert_a.to_str().unwrap(),
                },
                {
                    "name": "server-b",
                    "cert_file": cert_b.to_str().unwrap(),
                }
            ]
        });

        let figment = Figment::from(RecursiveFileProvider::new(Serialized::from(
            config, "default",
        )));

        let cfg: Config = figment.extract().unwrap();

        assert_eq!(cfg.servers.len(), 2);
        assert_eq!(cfg.servers[0].name, "server-a");
        assert_eq!(cfg.servers[0].cert, "CERT_A_CONTENT");
        assert_eq!(cfg.servers[1].cert, "CERT_B_CONTENT");
    }
}
