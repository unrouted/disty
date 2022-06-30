use crate::{
    app::ExampleApp,
    config::Configuration,
    types::{Digest, RegistryAction, RepositoryName},
};
use chrono::prelude::*;
use jsonschema::JSONSchema;
use log::debug;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Serialize, Deserialize)]
struct ManifestV2Config {
    #[serde(rename = "mediaType")]
    pub media_type: String,
    pub size: Option<usize>,
    pub digest: Digest,
}

#[derive(Debug, Serialize, Deserialize)]
struct ManifestV2Layer {
    #[serde(rename = "mediaType")]
    media_type: String,
    size: Option<usize>,
    digest: Digest,
    urls: Option<Vec<String>>,
}

#[derive(Debug, Serialize, Deserialize)]
struct DistributionManifestV1Layer {
    #[serde(rename = "blobSum")]
    digest: Digest,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
enum Manifest {
    ManifestV2 {
        config: ManifestV2Config,
        layers: Vec<ManifestV2Layer>,
    },

    DistributionManifestListV2 {
        manifests: Vec<ManifestV2Layer>,
    },

    DistributionManifestV1 {
        #[serde(rename = "fsLayers")]
        layers: Vec<DistributionManifestV1Layer>,
    },
}

#[derive(Clone)]
pub struct Extractor {
    config: Configuration,
    schemas: HashMap<String, Value>,
}

#[derive(Debug)]
pub enum ExtractError {
    UnknownError,
    SchemaValidationError,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Extraction {
    digest: Digest,
    content_type: String,
}

impl Extractor {
    pub fn new(config: Configuration) -> Self {
        let mut schemas: HashMap<String, Value> = HashMap::new();

        // Load all schemas into a hashmap
        // Ideally we would pre-compile them for speed, but rust lifetimes are fiddly. Revisit.
        schemas.insert(
            "application/vnd.docker.container.image.v1+json".into(),
            serde_json::from_str(include_str!(
                "schemas/vnd.docker.container.image.v1+json.json"
            ))
            .unwrap(),
        );
        schemas.insert(
            "application/vnd.docker.distribution.manifest.v1+json".into(),
            serde_json::from_str(include_str!(
                "schemas/vnd.docker.distribution.manifest.v1+json.json"
            ))
            .unwrap(),
        );
        schemas.insert(
            "application/vnd.docker.distribution.manifest.v1+prettyjws".into(),
            serde_json::from_str(include_str!(
                "schemas/vnd.docker.distribution.manifest.v1+prettyjws.json"
            ))
            .unwrap(),
        );
        schemas.insert(
            "application/vnd.docker.distribution.manifest.v2+json".into(),
            serde_json::from_str(include_str!(
                "schemas/vnd.docker.distribution.manifest.v2+json.json"
            ))
            .unwrap(),
        );
        schemas.insert(
            "application/vnd.docker.distribution.manifest.list.v2+json".into(),
            serde_json::from_str(include_str!(
                "schemas/vnd.docker.distribution.manifest.list.v2+json.json"
            ))
            .unwrap(),
        );
        schemas.insert(
            "application/vnd.oci.image.index.v1+json".into(),
            serde_json::from_str(include_str!("schemas/vnd.oci.image.index.v1+json.json")).unwrap(),
        );
        schemas.insert(
            "application/vnd.oci.image.manifest.v1+json".into(),
            serde_json::from_str(include_str!("schemas/vnd.oci.image.manifest.v1+json.json"))
                .unwrap(),
        );

        Extractor { config, schemas }
    }

    fn validate(&self, content_type: &str, data: &str) -> bool {
        match self.schemas.get(content_type) {
            Some(schema) => {
                let compiled = JSONSchema::compile(schema).unwrap();

                match serde_json::from_str(data) {
                    Ok(value) => compiled.is_valid(&value),
                    _ => false,
                }
            }
            _ => false,
        }
    }

    fn extract_one(
        &self,
        _content_type: &str,
        data: &str,
    ) -> Result<HashSet<Extraction>, ExtractError> {
        let manifest = serde_json::from_str(data);
        let mut results = HashSet::new();

        if let Ok(manifest) = manifest {
            match manifest {
                Manifest::DistributionManifestListV2 { manifests } => {
                    for layer in manifests.iter() {
                        results.insert(Extraction {
                            digest: layer.digest.clone(),
                            content_type: layer.media_type.clone(),
                        });
                    }
                }

                Manifest::ManifestV2 { config, layers } => {
                    results.insert(Extraction {
                        digest: config.digest.clone(),
                        content_type: config.media_type,
                    });
                    for layer in layers.iter() {
                        results.insert(Extraction {
                            digest: layer.digest.clone(),
                            content_type: layer.media_type.clone(),
                        });
                    }
                }

                Manifest::DistributionManifestV1 { layers } => {
                    // all the layers are application/octet-stream
                    for layer in layers.iter() {
                        results.insert(Extraction {
                            digest: layer.digest.clone(),
                            content_type: "application/octet-string".into(),
                        });
                    }
                }
            }
        }

        Ok(results)
    }

    pub async fn extract(
        &self,
        app: &ExampleApp,
        repository: &RepositoryName,
        digest: &Digest,
        content_type: &str,
        path: &std::path::Path,
    ) -> Result<Vec<RegistryAction>, ExtractError> {
        let mut analysis: Vec<RegistryAction> = Vec::new();
        let mut pending: HashSet<Extraction> = HashSet::new();
        let mut seen: HashSet<Digest> = HashSet::new();

        let data = match tokio::fs::read_to_string(&path).await {
            Ok(data) => data,
            _ => return Err(ExtractError::UnknownError {}),
        };

        if !self.validate(content_type, &data) {
            return Err(ExtractError::SchemaValidationError {});
        }

        let dependencies = self.extract_one(content_type, &data);
        match dependencies {
            Ok(dependencies) => {
                analysis.push(RegistryAction::ManifestInfo {
                    timestamp: Utc::now(),
                    digest: digest.clone(),
                    content_type: content_type.to_string(),
                    dependencies: dependencies
                        .iter()
                        .map(|extraction| extraction.digest.clone())
                        .collect(),
                });

                pending.extend(dependencies);
            }
            _ => {
                return Err(ExtractError::UnknownError {});
            }
        }

        drop(data);

        while !pending.is_empty() {
            let drain: Vec<Extraction> = pending.drain().collect();
            for extraction in drain {
                if seen.contains(&extraction.digest) {
                    // Don't visit a node twice
                    continue;
                }

                match app.get_blob(repository, &extraction.digest).await {
                    Some(blob) => {
                        if blob.content_type.is_some() || blob.dependencies.is_some() {
                            // Was already analyzed, don't do it again!
                            continue;
                        }
                    }
                    _ => {
                        // Dependency not in this repository, so push not allowed
                        return Err(ExtractError::UnknownError {});
                    }
                }

                if !self.schemas.contains_key(&extraction.content_type) {
                    analysis.push(RegistryAction::BlobInfo {
                        timestamp: Utc::now(),
                        digest: extraction.digest.clone(),
                        content_type: extraction.content_type.clone(),
                        dependencies: vec![],
                    });

                    seen.insert(extraction.digest);

                    continue;
                }

                // Lookup extraction.digest in blob store
                let data = tokio::fs::read_to_string(crate::utils::get_blob_path(
                    &self.config.storage,
                    &extraction.digest,
                ))
                .await;

                match data {
                    Ok(data) => {
                        let dependencies = self.extract_one(&extraction.content_type, &data);
                        match dependencies {
                            Ok(dependencies) => {
                                analysis.push(RegistryAction::BlobInfo {
                                    timestamp: Utc::now(),
                                    digest: extraction.digest.clone(),
                                    content_type: extraction.content_type.clone(),
                                    dependencies: dependencies
                                        .iter()
                                        .map(|extraction| extraction.digest.clone())
                                        .collect(),
                                });

                                pending.extend(dependencies);
                            }
                            _ => {
                                return Err(ExtractError::UnknownError {});
                            }
                        }

                        seen.insert(extraction.digest);
                    }
                    _ => {
                        return Err(ExtractError::UnknownError {});
                    }
                }
            }
        }

        debug!("Processed {digest} and made analysis: {analysis:?}");

        Ok(analysis)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wrong_schema() {
        let config = Configuration::default();
        let extractor = Extractor::new(config);

        let content_type = "application/json".to_string();
        let data = r#"
          {
            "schemaVersion": 2,
            "mediaType": "application/vnd.docker.distribution.manifest.list.v2+json",
            "manifests": [
              {
                "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
                "size": 7143,
                "digest": "sha256:e692418e4cbaf90ca69d05a66403747baa33ee08806650b51fab815ad7fc331f",
                "platform": {
                  "architecture": "ppc64le",
                  "os": "linux"
                }
              }
            ]
          }
        "#
        .to_string();

        assert!(!extractor.validate(&content_type, &data));
    }

    #[test]
    fn empty_string() {
        let config = Configuration::default();
        let extractor = Extractor::new(config);

        let content_type = "application/vnd.docker.distribution.manifest.list.v2+json".to_string();
        let data = r#"
        "#
        .to_string();

        assert!(!extractor.validate(&content_type, &data));
    }

    #[test]
    fn partial() {
        let config = Configuration::default();
        let extractor = Extractor::new(config);

        let content_type = "application/vnd.docker.distribution.manifest.list.v2+json".to_string();
        let data = r#"
          {
            "schemaVersion": 2,
            "mediaType": "application/vnd.docker.distribution.manifest.list.v2+json",
            "manifests": [
              {
                "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
                "size": 7143,
                "digest": "sha256:e692418e4cbaf90ca69d05a66403747baa33ee08806650b51fab815ad7fc331f",
                "platform": {
                  "architecture": "ppc64le",
                  "os": "linux"
                }
              },
              {
                "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
                "size": 7682,
                "digest": "sha256:5b0bcabd1ed22e9fb1310cf6c2dec7cdef19f0ad69efa1f392e94a4333501270",
                "platform": {
                  "architecture": "amd64",
                  "os": "linux",
                  "features": [
                    "sse4"
                  ]
                }
              }
        "#
        .to_string();

        assert!(!extractor.validate(&content_type, &data));
    }

    #[test]
    fn manifest_list_v2() {
        let config = Configuration::default();
        let extractor = Extractor::new(config);

        let content_type = "application/vnd.docker.distribution.manifest.list.v2+json".to_string();
        let data = r#"
          {
            "schemaVersion": 2,
            "mediaType": "application/vnd.docker.distribution.manifest.list.v2+json",
            "manifests": [
              {
                "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
                "size": 7143,
                "digest": "sha256:e692418e4cbaf90ca69d05a66403747baa33ee08806650b51fab815ad7fc331f",
                "platform": {
                  "architecture": "ppc64le",
                  "os": "linux"
                }
              },
              {
                "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
                "size": 7682,
                "digest": "sha256:5b0bcabd1ed22e9fb1310cf6c2dec7cdef19f0ad69efa1f392e94a4333501270",
                "platform": {
                  "architecture": "amd64",
                  "os": "linux",
                  "features": [
                    "sse4"
                  ]
                }
              }
            ]
          }
        "#
        .to_string();

        assert!(extractor.validate(&content_type, &data));
    }

    #[test]
    fn manifestv2() {
        let config = Configuration::default();
        let extractor = Extractor::new(config);

        let content_type = "application/vnd.docker.distribution.manifest.v2+json".to_string();
        let data = r#"
            {
                "schemaVersion": 2,
                "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
                "config": {
                    "mediaType": "application/vnd.docker.container.image.v1+json",
                    "size": 7023,
                    "digest": "sha256:b5b2b2c507a0944348e0303114d8d93aaaa081732b86451d9bce1f432a537bc7"
                },
                "layers": [
                    {
                        "mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
                        "size": 32654,
                        "digest": "sha256:e692418e4cbaf90ca69d05a66403747baa33ee08806650b51fab815ad7fc331f"
                    },
                    {
                        "mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
                        "size": 16724,
                        "digest": "sha256:3c3a4604a545cdc127456d94e421cd355bca5b528f4a9c1905b15da2eb4a4c6b"
                    },
                    {
                        "mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
                        "size": 73109,
                        "digest": "sha256:ec4b8955958665577945c89419d1af06b5f7636b4ac3da7f12184802ad867736"
                    }
                ]
            }
        "#.to_string();

        assert!(extractor.validate(&content_type, &data));
    }

    #[test]
    fn signed_v2_1_manifest() {
        let config = Configuration::default();
        let extractor = Extractor::new(config);

        let content_type = "application/vnd.docker.distribution.manifest.v1+prettyjws".to_string();
        let data = r#"
            {
                "name": "hello-world",
                "tag": "latest",
                "architecture": "amd64",
                "fsLayers": [
                {
                    "blobSum": "sha256:5f70bf18a086007016e948b04aed3b82103a36bea41755b6cddfaf10ace3c6ef"
                },
                {
                    "blobSum": "sha256:5f70bf18a086007016e948b04aed3b82103a36bea41755b6cddfaf10ace3c6ef"
                },
                {
                    "blobSum": "sha256:cc8567d70002e957612902a8e985ea129d831ebe04057d88fb644857caa45d11"
                },
                {
                    "blobSum": "sha256:5f70bf18a086007016e948b04aed3b82103a36bea41755b6cddfaf10ace3c6ef"
                }
                ],
                "history": [
                ],
                "schemaVersion": 1,
                "signatures": [
                {
                    "header": {
                        "jwk": {
                            "crv": "P-256",
                            "kid": "OD6I:6DRK:JXEJ:KBM4:255X:NSAA:MUSF:E4VM:ZI6W:CUN2:L4Z6:LSF4",
                            "kty": "EC",
                            "x": "3gAwX48IQ5oaYQAYSxor6rYYc_6yjuLCjtQ9LUakg4A",
                            "y": "t72ge6kIA1XOjqjVoEOiPPAURltJFBMGDSQvEGVB010"
                        },
                        "alg": "ES256"
                    },
                    "signature": "XREm0L8WNn27Ga_iE_vRnTxVMhhYY0Zst_FfkKopg6gWSoTOZTuW4rK0fg_IqnKkEKlbD83tD46LKEGi5aIVFg",
                    "protected": "eyJmb3JtYXRMZW5ndGgiOjY2MjgsImZvcm1hdFRhaWwiOiJDbjAiLCJ0aW1lIjoiMjAxNS0wNC0wOFQxODo1Mjo1OVoifQ"
                }
                ]
            }
        "#.to_string();

        assert!(extractor.validate(&content_type, &data));
    }
}
