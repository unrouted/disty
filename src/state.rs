use std::{collections::HashSet, path::PathBuf};

use anyhow::{Result, bail};
use hiqlite::Client;
use hiqlite_macros::params;
use serde::Deserialize;
use uuid::Uuid;

use crate::{digest::Digest, extractor::Extractor, webhook::WebhookService};

#[derive(Debug, Deserialize)]
struct BlobRow {
    digest: Digest,
    size: u64,
    media_type: String,
    location: u32,
}

#[derive(Debug, Deserialize)]
struct ManifestRow {
    digest: Digest,
    size: u32,
    media_type: String,
    location: u32,
}

#[derive(Deserialize)]
struct RepositoryRow {
    id: u32,
    name: String,
}

#[derive(PartialEq, Debug)]
pub struct Blob {
    pub digest: Digest,
    pub size: u64,
    pub media_type: String,
    pub location: u32,
    pub repositories: HashSet<String>,
}

#[derive(PartialEq, Debug)]
pub struct Manifest {
    pub digest: Digest,
    pub size: u32,
    pub media_type: String,
    pub location: u32,
    pub repository: String,
}

pub struct RegistryState {
    pub node_id: u64,
    pub client: Client,
    pub extractor: Extractor,
    pub webhooks: WebhookService,
}

impl RegistryState {
    pub fn upload_path(&self, upload_id: &str) -> PathBuf {
        PathBuf::from(format!("uploads/{upload_id}"))
    }

    pub fn get_temp_path(&self) -> PathBuf {
        self.upload_path(&Uuid::new_v4().as_hyphenated().to_string())
    }

    pub fn get_blob_path(&self, digest: &Digest) -> PathBuf {
        PathBuf::from("blobs").join(digest.to_path())
    }

    pub fn get_manifest_path(&self, digest: &Digest) -> PathBuf {
        PathBuf::from("manifests").join(digest.to_path())
    }
    async fn get_repository(&self, repository: &str) -> Result<Option<u32>> {
        let res: Option<RepositoryRow> = self
            .client
            .query_as_optional(
                "SELECT * FROM repositories WHERE name = $1",
                params!(repository),
            )
            .await?;

        if let Some(row) = res {
            return Ok(Some(row.id));
        }

        Ok(None)
    }

    pub async fn get_or_create_repository(&self, repository: &str) -> Result<u32> {
        self.client
            .execute(
                "INSERT OR IGNORE INTO repositories(name) VALUES($1);",
                params!(repository),
            )
            .await?;

        match self.get_repository(repository).await? {
            Some(row) => Ok(row),
            None => bail!("Could not find repository"),
        }
    }

    pub async fn get_blob(&self, digest: &Digest) -> Result<Option<Blob>> {
        let res: Option<BlobRow> = self
            .client
            .query_as_optional(
                "SELECT * FROM blobs WHERE digest = $1;",
                params!(digest.to_string()),
            )
            .await?;

        let repositories: Vec<String> = self
            .client
            .query_as(
                "SELECT name FROM repositories, blobs_repositories WHERE blobs_repositories.digest = $1 AND blobs_repositories.repository_id = repositories.id;",
                params!(digest.to_string()),
            )
            .await?;

        Ok(match res {
            Some(row) => Some(Blob {
                digest: row.digest,
                size: row.size,
                media_type: row.media_type,
                location: row.location,
                repositories: repositories.into_iter().collect(),
            }),
            None => None,
        })
    }

    pub async fn insert_blob(&self, digest: &Digest, size: u32, media_type: &str) -> Result<()> {
        let location = 1 << (self.node_id - 1);
        self.client
            .execute(
                "INSERT INTO blobs (digest, size, media_type, location) VALUES ($1, $2, $3, $4);",
                params!(digest.to_string(), size, media_type, location),
            )
            .await?;

        Ok(())
    }

    pub async fn mount_blob(&self, digest: &Digest, repository: &str) -> Result<()> {
        let repository_id = self.get_or_create_repository(repository).await?;

        self.client
            .execute(
                "INSERT OR IGNORE INTO blobs_repositories(digest, repository_id) VALUES($1, $2);",
                params!(digest.to_string(), repository_id),
            )
            .await?;

        Ok(())
    }

    pub async fn unmount_blob(&self, digest: &Digest, repository: &str) -> Result<()> {
        if let Some(repository_id) = self.get_repository(repository).await? {
            self.client
                .execute(
                    "DELETE FROM blobs_repositories WHERE digest = $1 AND repository_id = $2;",
                    params!(digest.to_string(), repository_id),
                )
                .await?;
        }

        Ok(())
    }

    pub async fn get_tag(&self, repository: &str, tag: &str) -> Result<Option<Manifest>> {
        let res: Option<ManifestRow> = self
            .client
            .query_as_optional(
                "SELECT m.*
                    FROM repositories r
                    JOIN tags t ON t.repository_id = r.id
                    JOIN manifests m ON t.manifest_id = m.id
                    WHERE r.name = $1 AND t.name = $2 AND t.deleted_at IS NULL AND m.deleted_at IS NULL;",
                params!(repository, tag),
            )
            .await?;

        Ok(match res {
            Some(row) => Some(Manifest {
                digest: row.digest,
                size: row.size,
                media_type: row.media_type,
                location: row.location,
                repository: repository.to_string(),
            }),
            None => None,
        })
    }

    pub async fn get_manifest(
        &self,
        repository: &str,
        digest: &Digest,
    ) -> Result<Option<Manifest>> {
        let res: Option<ManifestRow> = self
            .client
            .query_as_optional(
                "SELECT m.*
                        FROM manifests m
                        JOIN repositories r ON m.repository_id = r.id
                        WHERE m.digest = $1
                        AND r.name = $2
                        AND m.deleted_at IS NULL;",
                params!(digest.to_string(), repository),
            )
            .await?;

        Ok(match res {
            Some(row) => Some(Manifest {
                digest: row.digest,
                size: row.size,
                media_type: row.media_type,
                location: row.location,
                repository: repository.to_string(),
            }),
            None => None,
        })
    }

    pub async fn insert_manifest(
        &self,
        repository: &str,
        tag: &str,
        digest: &Digest,
        size: u64,
        media_type: &str,
    ) -> Result<()> {
        let location = 1 << (self.node_id - 1);
        self.client
         .txn([
                (
                    "INSERT OR IGNORE INTO repositories(name) VALUES($1);",
                    params!(repository),
                ),
                (
                    "INSERT INTO manifests (digest, size, media_type, location, repository_id) VALUES ($1, $2, $3, $4, (SELECT id FROM repositories WHERE name=$5));",
                    params!(digest.to_string(), size as u32, media_type, location, repository),
                ),
                (
                    "INSERT INTO tags (name, repository_id, manifest_id) VALUES ($1, (SELECT id FROM repositories WHERE name=$2), (SELECT manifests.id FROM manifests, repositories WHERE manifests.digest=$3 AND repositories.name=$2 AND repositories.id=manifests.repository_id));",
                    params!(tag, repository, digest.to_string())
                )
         ])
         .await?;

        self.webhooks
            .send(repository, digest, tag, media_type)
            .await?;

        Ok(())
    }

    pub async fn insert_manifest_dependencies(
        &self,
        digest: &Digest,
        dependencies: Vec<Digest>,
    ) -> Result<()> {
        assert!(false);
        Ok(())
    }

    pub async fn delete_manifest(&self, repository: &str, digest: &Digest) -> Result<()> {
        self.client
            .execute(
                "DELETE FROM manifests
                      WHERE digest = $1
                      AND repository_id = (
                        SELECT id FROM repositories WHERE name = $2
                    );
                ",
                params!(digest.to_string(), repository),
            )
            .await?;
        Ok(())
    }

    pub async fn delete_tag(&self, repository: &str, tag: &str) -> Result<()> {
        self.client
            .execute(
                "DELETE FROM tags
                      WHERE name = $1
                      AND repository_id = (
                        SELECT id FROM repositories WHERE name = $2
                    );
                ",
                params!(tag, repository),
            )
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{borrow::Cow, ops::Deref};

    use hiqlite::{Node, NodeConfig};
    use once_cell::sync::Lazy;
    use prometheus_client::registry::Registry;
    use tempfile::{TempDir, tempdir};
    use test_log::test;
    use tokio::{sync::Mutex, task::JoinSet};

    use crate::Migrations;

    use super::*;

    pub static EXCLUSIVE_TEST_LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

    #[must_use = "Fixture must be used and `.teardown().await` must be called to ensure proper cleanup."]
    struct Fixture {
        _guard: Box<dyn std::any::Any + Send>,
        dirs: Vec<TempDir>,
        registries: Vec<RegistryState>,
        tasks: JoinSet<Result<()>>,
    }

    impl Fixture {
        async fn new() -> Result<Self> {
            let lock = EXCLUSIVE_TEST_LOCK.lock().await;
            unsafe {
                std::env::set_var("ENC_KEY_ACTIVE", "828W10qknpOT");
                std::env::set_var(
                    "ENC_KEYS",
                    "828W10qknpOT/CIneMTth3mnRZZq0PMtztfWrnU+5xeiS0jrTB8iq6xc=",
                );
            }

            let config = NodeConfig {
                secret_api: "aaaaaaaaaaaaaaaa".into(),
                secret_raft: "bbbbbbbbbbbbbbbb".into(),
                log_statements: true,
                nodes: vec![Node {
                    id: 1,
                    addr_raft: "127.0.0.1:9999".to_string(),
                    addr_api: "127.0.0.1:9998".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            };

            let mut tasks = JoinSet::new();
            let mut registries = vec![];
            let mut dirs = vec![];

            for node in config.nodes.iter() {
                let dir = tempdir()?;
                let data_dir = dir.path();

                let mut registry = Registry::with_prefix("disty");

                let client = hiqlite::start_node(NodeConfig {
                    node_id: node.id,
                    data_dir: Cow::Owned(data_dir.to_string_lossy().into_owned()),
                    ..config.clone()
                })
                .await?;

                dirs.push(dir);
                registries.push(RegistryState {
                    node_id: node.id,
                    client,
                    extractor: Extractor::new(),
                    webhooks: WebhookService::start(&mut tasks, vec![], &mut registry),
                });
            }

            registries[0].client.wait_until_healthy_db().await;
            registries[0].client.migrate::<Migrations>().await?;

            Ok(Fixture {
                dirs,
                registries,
                _guard: Box::new(lock),
                tasks,
            })
        }

        async fn teardown(mut self) -> Result<()> {
            for registry in self.registries {
                registry.client.shutdown().await?;
            }
            self.tasks.shutdown().await;
            Ok(())
        }
    }

    impl Deref for Fixture {
        type Target = RegistryState;

        fn deref(&self) -> &Self::Target {
            &self.registries[0]
        }
    }

    #[test(tokio::test)]
    async fn test_get_repository() -> Result<()> {
        let registry = Fixture::new().await?;

        // At the start the repository shouldn't exist
        assert_eq!(None, registry.get_repository("foo/bar").await?);

        let repository_id = registry.get_or_create_repository("foo/bar").await?;

        // But we should be able to create it
        assert_eq!(
            Some(repository_id),
            registry.get_repository("foo/bar").await?
        );

        // And we shouldn't create duplicates
        assert_eq!(
            repository_id,
            registry.get_or_create_repository("foo/bar").await?
        );

        registry.teardown().await?;

        Ok(())
    }

    #[test(tokio::test)]
    async fn test_blob() -> Result<()> {
        let registry = Fixture::new().await?;

        let digest = "sha256:a9471d8321cedbb75e823ed68a507cd5b203cdb29c56732def856ebcdc5125ea"
            .parse()
            .unwrap();

        assert_eq!(None, registry.get_blob(&digest).await?);

        registry
            .insert_blob(&digest, 55, "application/octet-stream")
            .await?;

        let blob = registry.get_blob(&digest).await?.unwrap();

        assert_eq!(blob.digest, digest);

        registry.teardown().await?;

        Ok(())
    }

    #[test(tokio::test)]
    async fn test_manifest() -> Result<()> {
        let registry = Fixture::new().await?;

        let digest = "sha256:a9471d8321cedbb75e823ed68a507cd5b203cdb29c56732def856ebcdc5125ea"
            .parse()
            .unwrap();

        assert_eq!(None, registry.get_manifest("library/nginx", &digest).await?);

        registry
            .insert_manifest(
                "library/nginx",
                "latest",
                &digest,
                55,
                "application/octet-stream",
            )
            .await?;

        let manifest = registry
            .get_manifest("library/nginx", &digest)
            .await?
            .unwrap();

        assert_eq!(manifest.digest, digest);

        registry.teardown().await?;

        Ok(())
    }

    #[test(tokio::test)]
    async fn test_blob_mount() -> Result<()> {
        let registry = Fixture::new().await?;

        let digest = "sha256:a9471d8321cedbb75e823ed68a507cd5b203cdb29c56732def856ebcdc5125ea"
            .parse()
            .unwrap();

        registry
            .insert_blob(&digest, 55, "application/octet-stream")
            .await?;

        let blob = registry.get_blob(&digest).await?.unwrap();
        assert_eq!(blob.repositories, HashSet::new());

        registry.mount_blob(&digest, "ubuntu/trusty").await?;
        let blob = registry.get_blob(&digest).await?.unwrap();
        assert_eq!(
            blob.repositories,
            ["ubuntu/trusty".to_string()].into_iter().collect()
        );

        registry.unmount_blob(&digest, "ubuntu/trusty").await?;
        let blob = registry.get_blob(&digest).await?.unwrap();
        assert_eq!(blob.repositories, HashSet::new());

        registry.teardown().await?;

        Ok(())
    }
}
