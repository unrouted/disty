use std::{collections::HashSet, path::PathBuf};

use anyhow::{Context, Result, bail};
use hiqlite::{Client, StmtIndex};
use hiqlite_macros::params;
use serde::Deserialize;
use uuid::Uuid;

use crate::{config::Configuration, digest::Digest, notify::Notification, webhook::WebhookService};

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
    repository: String,
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
    pub config: Configuration,
    pub client: Client,
    pub webhooks: WebhookService,
}

impl RegistryState {
    pub fn upload_path(&self, upload_id: &str) -> PathBuf {
        self.config
            .storage
            .relative()
            .join("uploads")
            .join(upload_id)
    }

    pub fn get_temp_path(&self) -> PathBuf {
        self.upload_path(&Uuid::new_v4().as_hyphenated().to_string())
    }

    pub fn get_blob_path(&self, digest: &Digest) -> PathBuf {
        self.config
            .storage
            .relative()
            .join("blobs")
            .join(digest.to_path())
    }

    pub fn get_manifest_path(&self, digest: &Digest) -> PathBuf {
        self.config
            .storage
            .relative()
            .join("manifests")
            .join(digest.to_path())
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

    pub async fn repository_exists(&self, repository: &str) -> Result<bool> {
        let res: Option<RepositoryRow> = self
            .client
            .query_as_optional(
                "SELECT * FROM repositories WHERE name = $1",
                params!(repository),
            )
            .await?;

        Ok(res.is_some())
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
                "SELECT repositories.name
                        FROM blobs_repositories
                        JOIN repositories ON blobs_repositories.repository_id = repositories.id
                        WHERE blobs_repositories.digest = $1;",
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

    pub async fn insert_blob(
        &self,
        repository: &str,
        digest: &Digest,
        size: u32,
        media_type: &str,
        created_by: &str,
    ) -> Result<()> {
        let location = 1 << (self.node_id - 1);

        self.client
            .txn(vec![
                (
                    "INSERT OR IGNORE INTO repositories(name) VALUES($1);",
                    params!(repository),
                ),
                (
                    "INSERT INTO blobs (digest, size, media_type, location, created_by) VALUES ($1, $2, $3, $4, $5) ON CONFLICT(digest) DO UPDATE SET location = blobs.location | excluded.location;",
                    params!(digest.to_string(), size, media_type, location, created_by),
                ),
                (
                    "INSERT OR IGNORE INTO blobs_repositories(digest, repository_id) VALUES($1, (SELECT id FROM repositories WHERE name=$2));",
                    params!(digest.to_string(), repository),
                )
            ])
            .await?;

        self.client
            .notify(&Notification::BlobAdded {
                node: self.node_id,
                digest: digest.clone(),
                repository: repository.to_string(),
            })
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

    pub async fn blob_downloaded(&self, digest: &Digest) -> Result<()> {
        let location = 1 << (self.node_id - 1);
        // SET bit_field = bit_field & ~(1 << bit_position) to clear a bit
        self.client
            .execute(
                "UPDATE blobs SET location = (location | $1) WHERE digest = $2;",
                params!(location, digest.to_string()),
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
                "SELECT m.*, r.name AS repository
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
                "SELECT m.*, r.name AS repository
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
                repository: row.repository,
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
        dependencies: &[Digest],
        created_by: &str,
    ) -> Result<()> {
        let location = 1 << (self.node_id - 1);
        let mut sql = vec![
            (
                "INSERT OR IGNORE INTO repositories(name) VALUES($1);",
                params!(repository),
            ),
            (
                "INSERT INTO manifests (digest, size, media_type, location, repository_id, created_by) VALUES ($1, $2, $3, $4, (SELECT id FROM repositories WHERE name=$5), $6) ON CONFLICT(repository_id, digest) DO UPDATE SET location = manifests.location | excluded.location RETURNING manifests.id;",
                params!(
                    digest.to_string(),
                    size as u32,
                    media_type,
                    location,
                    repository,
                    created_by
                ),
            ),
            (
                "INSERT INTO tags (name, repository_id, manifest_id) VALUES ($1, (SELECT id FROM repositories WHERE name=$2), (SELECT manifests.id FROM manifests, repositories WHERE manifests.digest=$3 AND repositories.name=$2 AND repositories.id=manifests.repository_id)) ON CONFLICT(name, repository_id) DO UPDATE SET manifest_id = excluded.manifest_id;;",
                params!(tag, repository, digest.to_string()),
            ),
        ];

        sql.extend(dependencies.iter().map(|digest| {
            (
                "INSERT INTO manifest_layers(manifest_id, blob_digest) VALUES ($1, $2);",
                params!(StmtIndex(1).column("id"), digest.to_string()),
            )
        }));

        self.client.txn(sql).await?;

        self.client
            .notify(&Notification::ManifestAdded {
                node: self.node_id,
                digest: digest.clone(),
                repository: repository.to_string(),
            })
            .await?;

        self.webhooks
            .send(repository, digest, tag, media_type)
            .await?;

        Ok(())
    }

    pub async fn manifest_downloaded(&self, digest: &Digest) -> Result<()> {
        let location = 1 << (self.node_id - 1);
        // SET bit_field = bit_field & ~(1 << bit_position) to clear a bit
        self.client
            .execute(
                "UPDATE manifests SET location = (location | $1) WHERE digest = $2;",
                params!(location, digest.to_string()),
            )
            .await?;

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

    pub async fn get_tags(&self, repository: &str) -> Result<Vec<String>> {
        Ok(self
            .client
            .query_as(
                "SELECT tags.name
                FROM tags
                JOIN repositories ON tags.repository_id = repositories.id
                WHERE repositories.name = $1
                ORDER BY tags.name;",
                params!(repository),
            )
            .await?)
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

    pub async fn get_missing_blobs(&self) -> Result<Vec<Blob>> {
        let location = 1 << (self.node_id - 1);

        let blobs: Vec<BlobRow> = self
            .client
            .query_as(
                "SELECT * FROM blobs WHERE (location & $1) = 0;",
                params!(location),
            )
            .await?;
        let mut res = vec![];

        for blob in blobs.into_iter() {
            res.push(Blob {
                digest: blob.digest.clone(),
                size: blob.size,
                media_type: blob.media_type,
                location: blob.location,
                repositories: self
                    .client
                    .query_as(
                        "SELECT repositories.name
                                FROM blobs_repositories
                                JOIN repositories ON blobs_repositories.repository_id = repositories.id
                                WHERE blobs_repositories.digest = $1;",
                        params!(blob.digest.to_string()),
                    )
                    .await?
                    .into_iter()
                    .collect(),
            });
        }

        Ok(res)
    }

    pub async fn get_missing_manifests(&self) -> Result<Vec<Manifest>> {
        let location = 1 << (self.node_id - 1);

        let blobs: Vec<ManifestRow> = self
            .client
            .query_as(
                "SELECT m.*, r.name AS repository FROM manifests m JOIN repositories r ON m.repository_id = r.id WHERE (location & $1) = 0;",
                params!(location),
            )
            .await?;
        let mut res = vec![];

        for manifest in blobs.into_iter() {
            res.push(Manifest {
                digest: manifest.digest.clone(),
                size: manifest.size,
                media_type: manifest.media_type,
                location: manifest.location,
                repository: manifest.repository,
            });
        }

        Ok(res)
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.client
            .shutdown()
            .await
            .context("Failed to shutdown metadata db")
    }
}

#[cfg(test)]
mod tests {
    use test_log::test;

    use crate::tests::StateFixture;

    use super::*;

    #[test(tokio::test)]
    async fn test_get_repository() -> Result<()> {
        let registry = StateFixture::new().await?;

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
        let registry = StateFixture::new().await?;

        let digest = "sha256:a9471d8321cedbb75e823ed68a507cd5b203cdb29c56732def856ebcdc5125ea"
            .parse()
            .unwrap();

        assert_eq!(None, registry.get_blob(&digest).await?);

        registry
            .insert_blob(
                "library/nginx",
                &digest,
                55,
                "application/octet-stream",
                "bob",
            )
            .await?;

        let blob = registry.get_blob(&digest).await?.unwrap();

        assert_eq!(blob.digest, digest);

        registry.teardown().await?;

        Ok(())
    }

    /*    #[test(tokio::test)]
    async fn test_blob_mirror() -> Result<()> {
        let registry = StateFixture::with_size(3).await?;

        let digest = "sha256:a9471d8321cedbb75e823ed68a507cd5b203cdb29c56732def856ebcdc5125ea"
            .parse()
            .unwrap();

        assert_eq!(None, registry.get_blob(&digest).await?);

        registry
            .insert_blob(
                "library/nginx",
                &digest,
                55,
                "application/octet-stream",
                "bob",
            )
            .await?;

        let blob = registry.get_blob(&digest).await?.unwrap();
        assert_eq!(blob.digest, digest);

        let blobs = registry.registries[1].get_missing_blobs().await?;
        assert_eq!(blob, blobs[0]);

        registry.registries[1].blob_downloaded(&digest).await?;

        let blobs = registry.registries[1].get_missing_blobs().await?;
        assert_eq!(blobs.is_empty(), true);

        registry.teardown().await?;

        Ok(())
    }*/

    #[test(tokio::test)]
    async fn test_manifest() -> Result<()> {
        let registry = StateFixture::new().await?;

        let blob = "sha256:b9471d8321cedbb75e823ed68a507cd5b203cdb29c56732def856ebcdc5125ea"
            .parse()
            .unwrap();

        registry
            .insert_blob("library/nginx", &blob, 1, "application/octet-stream", "bob")
            .await?;

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
                &[blob],
                "bob",
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
        let registry = StateFixture::new().await?;

        let digest = "sha256:a9471d8321cedbb75e823ed68a507cd5b203cdb29c56732def856ebcdc5125ea"
            .parse()
            .unwrap();

        registry
            .insert_blob(
                "library/nginx",
                &digest,
                55,
                "application/octet-stream",
                "blob",
            )
            .await?;

        let blob = registry.get_blob(&digest).await?.unwrap();
        assert_eq!(
            blob.repositories,
            ["library/nginx".to_string()].into_iter().collect()
        );

        registry.mount_blob(&digest, "ubuntu/trusty").await?;
        let blob = registry.get_blob(&digest).await?.unwrap();
        assert_eq!(
            blob.repositories,
            ["ubuntu/trusty".to_string(), "library/nginx".to_string()]
                .into_iter()
                .collect()
        );

        registry.unmount_blob(&digest, "ubuntu/trusty").await?;
        let blob = registry.get_blob(&digest).await?.unwrap();
        assert_eq!(
            blob.repositories,
            ["library/nginx".to_string()].into_iter().collect()
        );

        registry.teardown().await?;

        Ok(())
    }
}
