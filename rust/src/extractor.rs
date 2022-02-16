use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

use crate::types::{Digest, RegistryAction, RepositoryName, RegistryState};

#[derive(Debug, Serialize, Deserialize)]
struct ManifestV2Config {
    #[serde(rename = "mediaType")]
    pub media_type: String,
    pub size: usize,
    pub digest: Digest,
}

#[derive(Debug, Serialize, Deserialize)]
struct ManifestV2Layer {
    #[serde(rename = "mediaType")]
    media_type: String,
    size: usize,
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
    schemas: HashMap<String, Value>,
}

/*
def analyze(content_type, manifest):
    logger.debug(f"MANIFEST: {content_type} start")
    try:
        parsed = json.loads(manifest)
    except json.decoder.JSONDecodeError:
        logger.debug(f"MANIFEST: invalid json")
        raise ManifestInvalid()

    if content_type not in analyzers:
        logger.debug(f"MANIFEST: no analyzer for {content_type}")
        return []

    if content_type not in schemas:
        logger.debug(f"MANIFEST: no schema for {content_type}")
        raise ManifestInvalid(reason="no_schema_for_this_type")

    schema = schemas[content_type]

    try:
        validate(instance=parsed, schema=schema)
    except ValidationError:
        logger.debug(f"MANIFEST: schema check fail")
        raise ManifestInvalid(reason="schema_check_fail")

    logger.debug(f"MANIFEST: invoking dep extractor")

    return analyzers[content_type](parsed)
*/

pub enum ExtractError {
    UnknownError,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Extraction {
    digest: Digest,
    content_type: String,
}

impl Extractor {
    pub fn new() -> Self {
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

        Extractor { schemas }
    }

    fn validate(&self, content_type: &String, data: &String) -> bool {
        match self.schemas.get(content_type) {
            Some(schema) => {
                let cfg = jsonschema_valid::Config::from_schema(
                    &schema,
                    Some(jsonschema_valid::schemas::Draft::Draft7),
                )
                .unwrap();

                match serde_json::from_str(data) {
                    Ok(value) => {
                        return cfg.validate(&value).is_ok() == true;
                    }
                    _ => false,
                }
            }
            _ => false,
        }
    }

    fn extract_one(
        &self,
        content_type: &String,
        data: &String,
    ) -> Result<HashSet<Extraction>, ExtractError> {
        let manifest = serde_json::from_str(&data);
        let mut results = HashSet::new();

        match manifest {
            Ok(manifest) => {
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
            _ => {}
        }

        return Ok(results);
    }

    pub async fn extract(
        &self,
        state: &RegistryState,
        repository: &RepositoryName,
        digest: &Digest,
        content_type: &String,
        data: &String,
    ) -> Result<Vec<RegistryAction>, ExtractError> {
        let mut analysis: Vec<RegistryAction> = Vec::new();
        let mut pending: HashSet<Extraction> = HashSet::new();
        let mut seen: HashSet<Digest> = HashSet::new();

        if !self.validate(content_type, data) {
            return Err(ExtractError::UnknownError {});
        }

        let dependencies = self.extract_one(&content_type, &data);
        match dependencies {
            Ok(dependencies) => {
                analysis.push(RegistryAction::ManifestInfo {
                    digest: digest.clone(),
                    content_type: content_type.clone(),
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

        drop(content_type);
        drop(digest);
        drop(data);

        while pending.len() > 0 {
            let drain: Vec<Extraction> = pending.drain().collect();
            for extraction in drain {
                if seen.contains(&extraction.digest) {
                    // Don't visit a node twice
                    continue;
                }

                match state.get_blob( &repository, &extraction.digest) {
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
                        digest: extraction.digest.clone(),
                        content_type: extraction.content_type.clone(),
                        dependencies: vec![],
                    });

                    seen.insert(extraction.digest);

                    continue;
                }

                // Lookup extraction.digest in blob store
                let data =
                    tokio::fs::read_to_string(crate::utils::get_blob_path(&state.repository_path, &extraction.digest)).await;

                match data {
                    Ok(data) => {
                        let dependencies = self.extract_one(&extraction.content_type, &data);
                        match dependencies {
                            Ok(dependencies) => {
                                analysis.push(RegistryAction::BlobInfo {
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

        Ok(analysis)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wrong_schema() {
        let extractor = Extractor::new();

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

        assert_eq!(extractor.validate(&content_type, &data), false);
    }

    #[test]
    fn empty_string() {
        let extractor = Extractor::new();

        let content_type = "application/vnd.docker.distribution.manifest.list.v2+json".to_string();
        let data = r#"
        "#
        .to_string();

        assert_eq!(extractor.validate(&content_type, &data), false);
    }

    #[test]
    fn partial() {
        let extractor = Extractor::new();

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

        assert_eq!(extractor.validate(&content_type, &data), false);
    }

    #[test]
    fn manifest_list_v2() {
        let extractor = Extractor::new();

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

        assert_eq!(extractor.validate(&content_type, &data), true);
    }

    #[test]
    fn ManifestV2() {
        let extractor = Extractor::new();

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

        assert_eq!(extractor.validate(&content_type, &data), true);
    }

    #[test]
    fn signed_v2_1_manifest() {
        let extractor = Extractor::new();

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

        assert_eq!(extractor.validate(&content_type, &data), true);
    }
}