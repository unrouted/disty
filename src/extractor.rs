use std::collections::HashMap;

use serde::Deserialize;

use crate::digest::Digest;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FsLayer {
    blob_sum: Digest,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ManifestV1 {
    fs_layers: Vec<FsLayer>,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
pub struct Platform {
    pub architecture: String,
    pub os: String,
    #[serde(default)]
    pub variant: Option<String>,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Descriptor {
    pub media_type: String,
    pub digest: Digest,
    #[serde(default)]
    pub size: Option<u64>,
    #[serde(default)]
    pub platform: Option<Platform>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "mediaType")]
#[serde(rename_all = "camelCase")]
enum ManifestV2 {
    #[serde(rename = "application/vnd.docker.distribution.manifest.v2+json")]
    DockerImage {
        subject: Option<Descriptor>,
        config: Descriptor,
        layers: Vec<Descriptor>,
    },
    #[serde(rename = "application/vnd.docker.distribution.manifest.list.v2+json")]
    DockerList {
        subject: Option<Descriptor>,
        manifests: Vec<Descriptor>,
    },
    #[serde(rename = "application/vnd.oci.image.manifest.v1+json")]
    OciImage {
        artifact_type: Option<String>,
        #[serde(default = "HashMap::new")]
        annotations: HashMap<String, String>,
        subject: Option<Descriptor>,
        config: Descriptor,
        layers: Vec<Descriptor>,
    },
    #[serde(rename = "application/vnd.oci.image.index.v1+json")]
    OciList {
        artifact_type: Option<String>,
        #[serde(default = "HashMap::new")]
        annotations: HashMap<String, String>,
        subject: Option<Descriptor>,
        manifests: Vec<Descriptor>,
    },
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum Manifest {
    V1(ManifestV1),
    V2(ManifestV2),
}

#[derive(Debug)]
pub struct ManifestInfo {
    pub media_type: String,
    pub artifact_type: Option<String>,
    pub annotations: HashMap<String, String>,
    pub size: u32,
    pub manifests: Vec<Descriptor>,
    pub blobs: Vec<Descriptor>,
    pub subject: Option<Descriptor>,
}

pub fn parse_manifest(input: &str) -> Result<ManifestInfo, serde_json::Error> {
    let manifest: Manifest = serde_json::from_str(input)?;

    match manifest {
        Manifest::V2(manifest) => {
            let mut manifests = Vec::new();
            let mut blobs = Vec::new();

            match manifest {
                ManifestV2::DockerImage {
                    subject,
                    config,
                    layers,
                } => {
                    blobs.push(config);
                    blobs.extend(layers);

                    Ok(ManifestInfo {
                        media_type: "application/vnd.docker.distribution.manifest.v2+json".into(),
                        artifact_type: None,
                        annotations: HashMap::new(),
                        size: input.len() as u32,
                        manifests,
                        blobs,
                        subject,
                    })
                }
                ManifestV2::OciImage {
                    artifact_type,
                    annotations,
                    subject,
                    config,
                    layers,
                } => {
                    blobs.push(config);
                    blobs.extend(layers);

                    Ok(ManifestInfo {
                        media_type: "application/vnd.oci.image.manifest.v1+json".into(),
                        artifact_type,
                        annotations,
                        size: input.len() as u32,
                        manifests,
                        blobs,
                        subject,
                    })
                }
                ManifestV2::DockerList {
                    subject,
                    manifests: inner,
                } => {
                    manifests.extend(inner);

                    Ok(ManifestInfo {
                        media_type: "application/vnd.docker.distribution.manifest.list.v2+json"
                            .into(),
                        artifact_type: None,
                        annotations: HashMap::new(),
                        size: input.len() as u32,
                        manifests,
                        blobs,
                        subject,
                    })
                }
                ManifestV2::OciList {
                    artifact_type,
                    annotations,
                    subject,
                    manifests: inner,
                } => {
                    manifests.extend(inner);

                    Ok(ManifestInfo {
                        media_type: "application/vnd.oci.image.index.v1+json".into(),
                        artifact_type,
                        annotations,
                        size: input.len() as u32,
                        manifests,
                        blobs,
                        subject,
                    })
                }
            }
        }
        Manifest::V1(manifest) => {
            let blobs = manifest
                .fs_layers
                .into_iter()
                .map(|f| Descriptor {
                    media_type: "application/vnd.docker.image.rootfs.diff.tar.gzip".into(),
                    digest: f.blob_sum,
                    size: None,
                    platform: None,
                })
                .collect();

            Ok(ManifestInfo {
                media_type: "application/vnd.docker.distribution.manifest.v1+json".into(),
                artifact_type: None,
                annotations: HashMap::new(),
                size: input.len() as u32,
                manifests: vec![],
                blobs,
                subject: None,
            })
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_docker_manifest_v2() {
        let input = r#"
        {
            "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
            "config": {
                "mediaType": "application/vnd.docker.container.image.v1+json",
                "digest": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "size": 7023
            },
            "layers": [
                {
                    "mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
                    "digest": "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                    "size": 32654
                }
            ]
        }
        "#;

        let info = parse_manifest(input).unwrap();
        assert_eq!(
            info.media_type,
            "application/vnd.docker.distribution.manifest.v2+json"
        );
        assert_eq!(info.manifests.len(), 0);
        assert_eq!(info.blobs.len(), 2);
        assert_eq!(
            info.blobs[0].digest,
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .parse()
                .unwrap()
        );
        assert_eq!(
            info.blobs[1].digest,
            "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                .parse()
                .unwrap()
        );
    }

    #[test]
    fn test_oci_image_manifest() {
        let input = r#"
        {
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "config": {
                "mediaType": "application/vnd.oci.image.config.v1+json",
                "digest": "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
                "size": 1500
            },
            "layers": [
                {
                    "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
                    "digest": "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
                    "size": 5120
                }
            ]
        }
        "#;

        let info = parse_manifest(input).unwrap();
        assert_eq!(
            info.media_type,
            "application/vnd.oci.image.manifest.v1+json"
        );
        assert_eq!(info.manifests.len(), 0);
        assert_eq!(info.blobs.len(), 2);
        assert_eq!(
            info.blobs[0].digest,
            "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
                .parse()
                .unwrap()
        );
        assert_eq!(
            info.blobs[1].digest,
            "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
                .parse()
                .unwrap()
        );
    }

    #[test]
    fn test_docker_manifest_list() {
        let input = r#"
        {
            "mediaType": "application/vnd.docker.distribution.manifest.list.v2+json",
            "manifests": [
                {
                    "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
                    "digest": "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
                    "size": 7682,
                    "platform": {
                        "architecture": "amd64",
                        "os": "linux"
                    }
                }
            ]
        }
        "#;

        let info = parse_manifest(input).unwrap();
        assert_eq!(
            info.media_type,
            "application/vnd.docker.distribution.manifest.list.v2+json"
        );
        assert_eq!(info.blobs.len(), 0);
        assert_eq!(info.manifests.len(), 1);
        assert_eq!(
            info.manifests[0].digest,
            "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                .parse()
                .unwrap()
        );
    }

    #[test]
    fn test_oci_image_index() {
        let input = r#"
        {
            "mediaType": "application/vnd.oci.image.index.v1+json",
            "manifests": [
                {
                    "mediaType": "application/vnd.oci.image.manifest.v1+json",
                    "digest": "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
                    "size": 8421,
                    "platform": {
                        "architecture": "arm64",
                        "os": "linux",
                        "variant": "v8"
                    }
                }
            ]
        }
        "#;

        let info = parse_manifest(input).unwrap();
        assert_eq!(info.media_type, "application/vnd.oci.image.index.v1+json");
        assert_eq!(info.blobs.len(), 0);
        assert_eq!(info.manifests.len(), 1);
        assert_eq!(
            info.manifests[0].digest,
            "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
                .parse()
                .unwrap()
        );
    }

    #[test]
    fn partial() {
        let info = parse_manifest(
            r#"
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
        "#,
        );

        assert!(info.is_err());
    }

    #[test]
    fn manifest_list_v2() {
        let info = parse_manifest(
            r#"
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
        "#,
        )
        .unwrap();

        assert_eq!(
            info.media_type,
            "application/vnd.docker.distribution.manifest.list.v2+json"
        );
    }

    #[test]
    fn manifestv2() {
        let info = parse_manifest(r#"
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
        "#).unwrap();

        assert_eq!(
            info.media_type,
            "application/vnd.docker.distribution.manifest.v2+json"
        );
    }

    #[test]
    fn signed_v2_1_manifest() {
        let info = parse_manifest(r#"
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
        "#).unwrap();

        assert_eq!(
            info.media_type,
            "application/vnd.docker.distribution.manifest.v1+json"
        );
    }

    #[test]
    fn oci_manifest_with_annotations() {
        let data = include_str!("../fixtures/manifests/oci_with_annotations.json");
        let info = parse_manifest(data).unwrap();
        assert_eq!(
            info.media_type,
            "application/vnd.oci.image.manifest.v1+json"
        );
    }
}
