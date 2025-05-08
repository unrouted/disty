use std::{collections::HashSet, path::PathBuf};

use anyhow::{Result, bail};
use hiqlite::Client;
use hiqlite_macros::params;
use serde::Deserialize;

use crate::digest::Digest;

#[derive(Deserialize)]
struct BlobRow {
    digest: Digest,
    repository_id: u32,
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
    pub size: u32,
    pub media_type: String,
    pub location: u32,
    pub repositories: HashSet<String>,
}

pub struct RegistryState {
    pub client: Client,
}

impl RegistryState {
    pub fn upload_path(&self, upload_id: &str) -> PathBuf {
        PathBuf::from(format!("uploads/{upload_id}"))
    }

    pub fn get_blob_path(&self, digest: &Digest) -> PathBuf {
        PathBuf::from("blobs").join(digest.to_path())
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
                "SELECT * FROM blobs WHERE digest = $1",
                params!(digest.to_string()),
            )
            .await?;

        Ok(match res {
            Some(row) => Some(Blob {
                digest: row.digest,
                size: row.size,
                media_type: row.media_type,
                location: row.location,
                repositories: HashSet::new(),
            }),
            None => None,
        })
    }

    pub async fn insert_blob(&self, digest: &Digest, size: u32, media_type: &str) -> Result<()> {
        self.client
            .execute(
                "INSERT INTO blobs (digest, size, media_type, location) VALUES ($1, $2, $3, $4);",
                params!(digest.to_string(), size, media_type, 1),
            )
            .await?;

        Ok(())
    }

    pub async fn mount_blob(&self, digest: &Digest, repository: &str) -> Result<()> {
        let repository_id = self.get_or_create_repository(repository).await?;

        self.client
            .execute(
                "INSERT OR IGNORE INTO blob_repositories(digest, repository) VALUES('$1', $2);",
                params!(digest.to_string(), repository_id),
            )
            .await?;

        Ok(())
    }

    pub async fn unmount_blob(&self, digest: &Digest, repository: &str) -> Result<()> {
        if let Some(repository_id) = self.get_repository(repository).await? {
            self.client
                .execute(
                    "DELETE FROM blob_repositories WHERE digest = '$1' AND repository = $2;",
                    params!(digest.to_string(), repository_id),
                )
                .await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{borrow::Cow, ops::Deref};

    use hiqlite::{Node, NodeConfig};
    use once_cell::sync::Lazy;
    use tempfile::{TempDir, tempdir};
    use tokio::sync::Mutex;

    use crate::Migrations;

    use super::*;

    pub static EXCLUSIVE_TEST_LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

    #[must_use = "Fixture must be used and `.teardown().await` must be called to ensure proper cleanup."]
    struct Fixture {
        dir: TempDir,
        _guard: Box<dyn std::any::Any + Send>,
        registry: RegistryState,
    }

    impl Fixture {
        async fn new() -> Result<Self> {
            let lock = EXCLUSIVE_TEST_LOCK.lock().await;

            let dir = tempdir()?;

            let data_dir = dir.path();

            unsafe {
                std::env::set_var("ENC_KEY_ACTIVE", "828W10qknpOT");
                std::env::set_var(
                    "ENC_KEYS",
                    "828W10qknpOT/CIneMTth3mnRZZq0PMtztfWrnU+5xeiS0jrTB8iq6xc=",
                );
            }

            let config = NodeConfig {
                node_id: 1,
                secret_api: "aaaaaaaaaaaaaaaa".into(),
                secret_raft: "bbbbbbbbbbbbbbbb".into(),
                data_dir: Cow::Owned(data_dir.to_string_lossy().into_owned()),
                nodes: vec![Node {
                    id: 1,
                    ..Default::default()
                }],
                ..Default::default()
            };

            let client = hiqlite::start_node(config).await?;

            client.migrate::<Migrations>().await?;

            let registry = RegistryState { client };

            Ok(Fixture {
                dir,
                registry,
                _guard: Box::new(lock),
            })
        }

        async fn teardown(self) -> Result<()> {
            self.registry.client.shutdown().await?;
            Ok(())
        }
    }

    impl Deref for Fixture {
        type Target = RegistryState;

        fn deref(&self) -> &Self::Target {
            &self.registry
        }
    }

    #[tokio::test]
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

    #[tokio::test]
    async fn test_blob() -> Result<()> {
        let registry = Fixture::new().await?;

        let digest = "sha256:a9471d8321cedbb75e823ed68a507cd5b203cdb29c56732def856ebcdc5125ea"
            .parse()
            .unwrap();

        assert_eq!(None, registry.get_blob(&digest).await?);

        registry
            .insert_blob(&digest, 55, "application/octet-stream")
            .await?;

        let blob = match registry.get_blob(&digest).await? {
            Some(blob) => blob,
            None => panic!("no blob"),
        };

        assert_eq!(blob.digest, digest);
        assert_eq!(blob.size, 55);
        assert_eq!(blob.media_type, "application/octet-stream");

        registry.teardown().await?;

        Ok(())
    }
}
