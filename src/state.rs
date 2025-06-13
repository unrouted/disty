use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
};

use anyhow::{Context, Result};
use hiqlite::{Client, StmtIndex};
use hiqlite_macros::params;
use prometheus_client::registry::Registry;
use serde::Deserialize;
use tracing::info;
use uuid::Uuid;

use crate::{
    config::{Configuration, lifecycle::DeletionRule},
    digest::Digest,
    extractor::ManifestInfo,
    notify::Notification,
    webhook::WebhookService,
};

#[derive(Debug, Deserialize)]
struct BlobRow {
    id: u32,
    digest: Digest,
    size: u64,
    location: u32,
}

#[derive(Debug, Deserialize)]
struct ManifestRow {
    id: u64,
    digest: Digest,
    size: u32,
    media_type: String,
    location: u32,
}

#[derive(PartialEq, Debug)]
pub struct Blob {
    pub digest: Digest,
    pub size: u64,
    pub location: u32,
    pub repositories: HashSet<String>,
}

#[derive(PartialEq, Debug)]
pub struct Manifest {
    pub digest: Digest,
    pub size: u32,
    pub media_type: String,
    pub location: u32,
    pub repositories: HashSet<String>,
}

pub struct RegistryState {
    pub node_id: u64,
    pub config: Configuration,
    pub client: Client,
    pub webhooks: WebhookService,
    pub registry: Registry,
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

    pub async fn repository_exists(&self, repository: &str) -> Result<bool> {
        let res: usize = self
            .client
            .query_as_one(
                "SELECT COUNT(*) AS count FROM repositories WHERE name = $1",
                params!(repository),
            )
            .await?;

        Ok(res > 0)
    }

    pub async fn get_blob(&self, digest: &Digest) -> Result<Option<Blob>> {
        let res: Option<BlobRow> = self
            .client
            .query_as_optional(
                "SELECT * FROM blobs WHERE digest = $1;",
                params!(digest.to_string()),
            )
            .await?;

        if let Some(row) = res {
            let repositories: Vec<String> = self
                .client
                .query_as(
                    "SELECT repositories.name
                            FROM blobs_repositories
                            JOIN repositories ON blobs_repositories.repository_id = repositories.id
                            WHERE blobs_repositories.blob_id = $1;",
                    params!(row.id),
                )
                .await?;

            return Ok(Some(Blob {
                digest: row.digest,
                size: row.size,
                location: row.location,
                repositories: repositories.into_iter().collect(),
            }));
        }

        Ok(None)
    }

    pub async fn insert_blob(
        &self,
        repository: &str,
        digest: &Digest,
        size: u32,
        created_by: &str,
    ) -> Result<()> {
        let location = 1 << (self.node_id - 1);
        let cluster_size_mask = (1 << self.config.nodes.len()) - 1;

        self.client
            .txn(vec![
                (
                    "INSERT INTO repositories(name) VALUES($1) ON CONFLICT DO UPDATE SET id=id RETURNING id;",
                    params!(repository),
                ),
                (
                    "INSERT INTO blobs (digest, size, location, created_by, state) VALUES ($1, $2, $3, $4, CASE WHEN $3 = $5 THEN 1 ELSE 0 END) ON CONFLICT(digest) DO UPDATE SET location = blobs.location | excluded.location, state = CASE WHEN (blobs.location | excluded.location) = $5 THEN 1 ELSE blobs.state END RETURNING id;",
                    params!(digest.to_string(), size, location, created_by, cluster_size_mask),
                ),
                (
                    "INSERT OR IGNORE INTO blobs_repositories(blob_id, repository_id) VALUES($1, $2);",
                    params!(StmtIndex(1).column("id"), StmtIndex(0).column("id")),
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
        self.client
            .txn(vec![
                (
                    "INSERT OR IGNORE INTO repositories(name) VALUES($1) RETURNING id;",
                    params!(repository),
                ),
                (
                    "INSERT OR IGNORE INTO blobs_repositories(blob_id, repository_id) SELECT id, $1 FROM blobs WHERE digest = $2;",
                    params!(StmtIndex(0).column("id"), digest.to_string()),
                )
            ])
            .await?;

        Ok(())
    }

    pub async fn blob_downloaded(&self, digest: &Digest) -> Result<()> {
        let location = 1 << (self.node_id - 1);
        let cluster_size_mask = (1 << self.config.nodes.len()) - 1;
        // SET bit_field = bit_field & ~(1 << bit_position) to clear a bit
        self.client
            .execute(
                "UPDATE blobs SET location = (location | $1), state = CASE WHEN (location | $1) = $2 THEN 1 ELSE state END WHERE digest = $3;",
                params!(location, cluster_size_mask, digest.to_string()),
            )
            .await?;

        Ok(())
    }

    pub async fn unmount_blob(&self, digest: &Digest, repository: &str) -> Result<()> {
        self.client
            .execute(
                "DELETE FROM blobs_repositories WHERE blob_id = (SELECT id FROM blobs WHERE digest = $1) AND repository_id = (SELECT id FROM repositories WHERE name = $2);",
                params!(digest.to_string(), repository),
            )
            .await?;

        Ok(())
    }

    pub async fn get_tag(&self, repository: &str, tag: &str) -> Result<Option<Manifest>> {
        let res: Option<ManifestRow> = self
            .client
            .query_as_optional(
                "SELECT m.*, r.name AS repository
                        FROM tags t
                        JOIN manifests_repositories mr ON t.manifest_repository_id = mr.id
                        JOIN manifests m ON mr.manifest_id = m.id
                        JOIN repositories r ON mr.repository_id = r.id
                        WHERE r.name = $1 AND t.name = $2;",
                params!(repository, tag),
            )
            .await?;

        if let Some(row) = res {
            let repositories: Vec<String> = self
                .client
                .query_as(
                    "SELECT repositories.name
                            FROM manifests_repositories
                            JOIN repositories ON manifests_repositories.repository_id = repositories.id
                            WHERE manifests_repositories.manifest_id = $1;",
                    params!(row.id as u32),
                )
                .await?;

            return Ok(Some(Manifest {
                digest: row.digest,
                size: row.size,
                media_type: row.media_type,
                location: row.location,
                repositories: repositories.into_iter().collect(),
            }));
        }

        Ok(None)
    }

    pub async fn get_manifest(&self, digest: &Digest) -> Result<Option<Manifest>> {
        let res: Option<ManifestRow> = self
            .client
            .query_as_optional(
                "SELECT m.*
                        FROM manifests m
                        WHERE m.digest = $1;",
                params!(digest.to_string()),
            )
            .await?;

        if let Some(row) = res {
            let repositories: Vec<String> = self
                .client
                .query_as(
                    "SELECT repositories.name
                            FROM manifests_repositories
                            JOIN repositories ON manifests_repositories.repository_id = repositories.id
                            WHERE manifests_repositories.manifest_id = $1;",
                    params!(row.id as u32),
                )
                .await?;

            return Ok(Some(Manifest {
                digest: row.digest,
                size: row.size,
                media_type: row.media_type,
                location: row.location,
                repositories: repositories.into_iter().collect(),
            }));
        }

        Ok(None)
    }

    pub async fn insert_manifest(
        &self,
        repository: &str,
        tag: &str,
        digest: &Digest,
        info: &ManifestInfo,
        created_by: &str,
    ) -> Result<()> {
        let location = 1 << (self.node_id - 1);
        let cluster_size_mask = (1 << self.config.nodes.len()) - 1;

        let mut sql = vec![
            (
                "INSERT INTO repositories(name) VALUES ($1) ON CONFLICT DO UPDATE SET id=id RETURNING id",
                params!(repository),
            ),
            (
                "INSERT INTO manifests (digest, size, media_type, location, state, created_by, artifact_type, annotations) VALUES ($1, $2, $3, $4, CASE WHEN $4 = $5 THEN 1 ELSE 0 END, $6, $7, $8) ON CONFLICT(digest) DO UPDATE SET location = manifests.location | excluded.location, state = CASE WHEN (manifests.location | excluded.location) = $6 THEN 1 ELSE manifests.state END RETURNING manifests.id;",
                params!(
                    digest.to_string(),
                    info.size,
                    &info.media_type,
                    location,
                    cluster_size_mask,
                    created_by,
                    &info.artifact_type,
                    serde_json::to_string(&info.annotations)?
                ),
            ),
            (
                "INSERT INTO manifests_repositories (repository_id, manifest_id) VALUES ($1, $2) ON CONFLICT(manifest_id, repository_id) DO UPDATE SET id=id RETURNING id;",
                params!(StmtIndex(0).column("id"), StmtIndex(1).column("id")),
            ),
            (
                "INSERT INTO tags (name, manifest_repository_id) VALUES ($1, $2) ON CONFLICT(name, manifest_repository_id) DO UPDATE SET manifest_repository_id = excluded.manifest_repository_id;",
                params!(tag, StmtIndex(2).column("id")),
            ),
        ];

        sql.extend(info.blobs.iter().map(|descriptor| {
            (
                "INSERT OR IGNORE INTO manifest_layers(manifest_id, blob_id) SELECT $1, id FROM blobs WHERE digest = $2;",
                params!(StmtIndex(1).column("id"), descriptor.digest.to_string()),
            )
        }));

        sql.extend(info.manifests.iter().map(|descriptor| {
            (
                "INSERT OR IGNORE INTO manifest_references(manifest_id, child_id) VALUES ($1, (SELECT manifests.id FROM manifests WHERE digest=$2));",
                params!(StmtIndex(1).column("id"), descriptor.digest.to_string()),
            )
        }));

        sql.extend(info.subject.iter().map(|descriptor| {
            (
                "INSERT OR IGNORE INTO manifest_subject(manifest_id, subject_id) VALUES ($1, (SELECT manifests.id FROM manifests WHERE digest=$2));",
                params!(StmtIndex(1).column("id"), descriptor.digest.to_string()),
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
            .send(repository, digest, tag, &info.media_type)
            .await?;

        Ok(())
    }

    pub async fn manifest_downloaded(&self, digest: &Digest) -> Result<()> {
        let location = 1 << (self.node_id - 1);
        let cluster_size_mask = (1 << self.config.nodes.len()) - 1;
        // SET bit_field = bit_field & ~(1 << bit_position) to clear a bit
        self.client
            .execute(
                "UPDATE manifests SET location = (location | $1), state = CASE WHEN (location | $1) = $2 THEN 1 ELSE state END WHERE digest = $3;",
                params!(location, cluster_size_mask, digest.to_string()),
            )
            .await?;

        Ok(())
    }

    pub async fn delete_manifest(&self, repository: &str, digest: &Digest) -> Result<()> {
        self.client
            .execute(
                "DELETE FROM manifests_repositories
                    WHERE manifest_id = (SELECT id FROM manifests WHERE digest = $1)
                    AND repository_id = (SELECT id FROM repositories WHERE name = $2);
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
                        JOIN manifests_repositories mr ON tags.manifest_repository_id = mr.id
                        JOIN repositories ON mr.repository_id = repositories.id
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
                        AND manifest_repository_id IN (
                            SELECT mr.id
                            FROM manifests_repositories mr
                            JOIN repositories r ON mr.repository_id = r.id
                            WHERE r.name = $2
                        );
                ",
                params!(tag, repository),
            )
            .await?;
        Ok(())
    }

    pub async fn get_referrer(&self, digest: &Digest) -> Result<Vec<Manifest>> {
        let location = 1 << (self.node_id - 1);

        let blobs: Vec<ManifestRow> = self
            .client
            .query_as(
                "SELECT m.* FROM manifests JOIN manifest_subject ms ON m.id = ms.manifest_id JOIN manifests s ON s.id = ms.subject_id WHERE s.digest=$1;",
                params!(digest.to_string()),
            )
            .await?;
        let mut res = vec![];

        for manifest in blobs.into_iter() {
            let repositories: Vec<String> = self
                .client
                .query_as(
                    "SELECT repositories.name
                            FROM manifests_repositories
                            JOIN repositories ON manifests_repositories.repository_id = repositories.id
                            WHERE manifests_repositories.manifest_id = $1;",
                    params!(manifest.id as u32),
                )
                .await?;

            res.push(Manifest {
                digest: manifest.digest.clone(),
                size: manifest.size,
                media_type: manifest.media_type,
                location: manifest.location,
                repositories: repositories.into_iter().collect(),
            });
        }

        Ok(res)
    }

    pub async fn get_missing_blobs(&self) -> Result<Vec<Blob>> {
        let location = 1 << (self.node_id - 1);

        let blobs: Vec<BlobRow> = self
            .client
            .query_as(
                "SELECT * FROM blobs WHERE state = 0 AND (location & $1) = 0;",
                params!(location),
            )
            .await?;
        let mut res = vec![];

        for blob in blobs.into_iter() {
            res.push(Blob {
                digest: blob.digest.clone(),
                size: blob.size,
                location: blob.location,
                repositories: self
                    .client
                    .query_as(
                        "SELECT repositories.name
                                FROM blobs_repositories
                                JOIN repositories ON blobs_repositories.repository_id = repositories.id
                                WHERE blobs_repositories.blob_id = $1;",
                        params!(blob.id),
                    )
                    .await?
                    .into_iter()
                    .collect(),
            });
        }

        Ok(res)
    }

    pub async fn untag_old_tags(&self) -> Result<usize> {
        let mut total_deleted = 0;

        for rule in &self.config.cleanup {
            let DeletionRule::Tag {
                repository,
                tag,
                older_than,
            } = rule;

            total_deleted += self
                .client
                .execute(
                    "DELETE FROM tags
                            WHERE id IN (
                                SELECT t.id
                                FROM tags t
                                JOIN manifests_repositories mr ON t.manifest_repository_id = mr.id
                                JOIN repositories r ON mr.repository_id = r.id
                                WHERE t.updated_at < datetime('now', '-' || $1 || ' days')
                                AND t.name GLOB $2
                                AND r.name GLOB $3
                            );",
                    params!(
                        *older_than,
                        tag.clone().map_or("*".to_string(), |m| m.to_sqlite_glob()),
                        repository
                            .clone()
                            .map_or("*".to_string(), |m| m.to_sqlite_glob())
                    ),
                )
                .await?;
        }

        Ok(total_deleted)
    }

    pub async fn delete_unreferenced_manifest_repositories(&self) -> Result<usize> {
        self.client
            .execute(
                "WITH orphaned AS (
                        SELECT mr.id
                        FROM manifests_repositories mr
                        JOIN manifests m ON mr.manifest_id = m.id
                        LEFT JOIN tags t ON t.manifest_repository_id = mr.id
                        LEFT JOIN manifest_references r ON r.child_id = m.id
                        LEFT JOIN manifest_subject ms ON ms.manifest_id = m.id
                        WHERE t.id IS NULL
                        AND r.manifest_id IS NULL
                        AND ms.manifest_id IS NULL
                        AND m.state = 1
                        AND mr.created_at < datetime('now', '-15 minutes')
                    )
                    DELETE FROM manifests_repositories
                    WHERE id IN (SELECT id FROM orphaned);",
                vec![],
            )
            .await
            .context(
                "Unable to remove manifests from repositories where they are no longer referenced",
            )
    }

    pub async fn unstore_unreferenced_manifests(&self) -> Result<usize> {
        let location = 1 << (self.node_id - 1);
        let mut deleted = vec![];

        let manifests: Vec<ManifestRow> = self
            .client
            .query_as(
                "SELECT m.*
                    FROM manifests m
                    LEFT JOIN manifests_repositories mr ON m.id = mr.manifest_id
                    WHERE m.state = 1
                    AND (m.location & $1) != 0
                    AND mr.id IS NULL;",
                params!(location),
            )
            .await?;

        for manifest in manifests {
            let path = self.get_manifest_path(&manifest.digest);
            if tokio::fs::try_exists(&path).await? {
                info!("Deleting {:?}", path);
                tokio::fs::remove_file(&path).await?;
            }

            deleted.push(manifest.id);
        }

        let statements = deleted
            .into_iter()
            .map(|id| {
                (
                    "UPDATE manifests SET location = location & ~$1 WHERE id=$2;",
                    params!(location, id as u32),
                )
            })
            .collect::<Vec<_>>();

        let res = self.client.txn(statements).await?;

        Ok(res.len())
    }

    pub async fn delete_unreferenced_manifests(&self) -> Result<usize> {
        self.client
            .execute(
                "WITH unreferenced_manifests AS (
                    SELECT m.id
                    FROM manifests m
                    LEFT JOIN manifests_repositories mr ON m.id = mr.manifest_id
                    WHERE m.location = 0
                    AND mr.id IS NULL
                )
                DELETE FROM manifests
                WHERE id IN (SELECT id FROM unreferenced_manifests);
                ",
                vec![],
            )
            .await
            .context("Unable to delete unreferenced manifests")
    }

    pub async fn delete_unreferenced_blob_repositories(&self) -> Result<usize> {
        self.client
            .execute(
                "WITH orphaned AS (
                        SELECT br.blob_id, br.repository_id
                        FROM blobs_repositories br
                        JOIN blobs b ON br.blob_id = b.id
                        LEFT JOIN manifest_layers ml ON br.blob_id = ml.blob_id
                        LEFT JOIN manifests m ON ml.manifest_id = m.id
                        WHERE m.id IS NULL
                        AND b.state = 1
                        AND br.created_at < datetime('now', '-30 minutes')
                    )
                    DELETE FROM blobs_repositories
                    WHERE (blob_id, repository_id) IN (
                        SELECT blob_id, repository_id FROM orphaned
                    );",
                vec![],
            )
            .await
            .context("Unable to remove blobs from repositories where they are no longer referenced")
    }

    pub async fn unstore_unreachable_blobs(&self) -> Result<usize> {
        let location = 1 << (self.node_id - 1);

        let blobs: Vec<BlobRow> = self
            .client
            .query_as(
                "SELECT b.*
                        FROM blobs b
                        LEFT JOIN blobs_repositories br ON b.id = br.blob_id
                        WHERE b.state = 1
                        AND (b.location & $1) != 0
                        AND br.blob_id IS NULL;
                        ",
                params!(location),
            )
            .await?;

        for blob in blobs.iter() {
            let path = self.get_blob_path(&blob.digest);
            if tokio::fs::try_exists(&path).await? {
                info!("Deleting {:?}", path);
                tokio::fs::remove_file(&path).await?;
            }
        }

        let statements = blobs
            .into_iter()
            .map(|blob| {
                (
                    "UPDATE blobs SET location = location & (~$1) WHERE id = $2;",
                    params!(location, blob.id),
                )
            })
            .collect::<Vec<_>>();

        let res = self.client.txn(statements).await?;

        Ok(res.len())
    }

    pub async fn delete_unstored_blobs(&self) -> Result<usize> {
        self.client
            .execute(
                "
                WITH unreferenced_blobs AS (
                    SELECT b.id
                    FROM blobs b
                    LEFT JOIN blobs_repositories br ON b.id = br.blob_id
                    WHERE b.location = 0
                        AND br.blob_id IS NULL
                    )
                    DELETE FROM blobs
                    WHERE id IN (SELECT id FROM unreferenced_blobs);
                ",
                vec![],
            )
            .await
            .context("Unable to delete unreferenced manifests")
    }

    pub async fn garbage_collection(&self) -> Result<()> {
        self.untag_old_tags().await?;

        self.delete_unreferenced_manifest_repositories().await?;
        self.unstore_unreferenced_manifests().await?;
        self.delete_unreferenced_manifests().await?;

        self.delete_unreferenced_blob_repositories().await?;
        self.unstore_unreachable_blobs().await?;
        self.delete_unstored_blobs().await?;

        Ok(())
    }

    pub async fn get_missing_manifests(&self) -> Result<Vec<Manifest>> {
        let location = 1 << (self.node_id - 1);

        let blobs: Vec<ManifestRow> = self
            .client
            .query_as(
                "SELECT m.* FROM manifests WHERE state = 0 AND (location & $1) = 0;",
                params!(location),
            )
            .await?;
        let mut res = vec![];

        for manifest in blobs.into_iter() {
            let repositories: Vec<String> = self
                .client
                .query_as(
                    "SELECT repositories.name
                            FROM manifests_repositories
                            JOIN repositories ON manifests_repositories.repository_id = repositories.id
                            WHERE manifests_repositories.manifest_id = $1;",
                    params!(manifest.id as u32),
                )
                .await?;

            res.push(Manifest {
                digest: manifest.digest.clone(),
                size: manifest.size,
                media_type: manifest.media_type,
                location: manifest.location,
                repositories: repositories.into_iter().collect(),
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

    use crate::{
        extractor::Descriptor,
        tests::{FixtureBuilder, StateFixture},
    };

    use super::*;

    #[test(tokio::test)]
    async fn test_blob() -> Result<()> {
        let registry = StateFixture::new().await?;

        let digest = "sha256:a9471d8321cedbb75e823ed68a507cd5b203cdb29c56732def856ebcdc5125ea"
            .parse()
            .unwrap();

        assert_eq!(None, registry.get_blob(&digest).await?);

        registry
            .insert_blob("library/nginx", &digest, 55, "bob")
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
            .insert_blob("library/nginx", &blob, 1, "bob")
            .await?;

        let digest = "sha256:a9471d8321cedbb75e823ed68a507cd5b203cdb29c56732def856ebcdc5125ea"
            .parse()
            .unwrap();

        assert_eq!(None, registry.get_manifest(&digest).await?);

        let info = ManifestInfo {
            media_type: "application/octet-stream".into(),
            artifact_type: None,
            annotations: HashMap::new(),
            size: 55,
            manifests: vec![],
            blobs: vec![Descriptor {
                digest: blob,
                media_type: "application/octet-stream".into(),
                size: None,
                platform: None,
            }],
            subject: None,
        };

        registry
            .insert_manifest("library/nginx", "latest", &digest, &info, "bob")
            .await?;

        let manifest = registry.get_manifest(&digest).await?.unwrap();

        assert_eq!(manifest.digest, digest);
        assert!(manifest.repositories.contains("library/nginx"));

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
            .insert_blob("library/nginx", &digest, 55, "blob")
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

    #[test(tokio::test)]
    async fn test_garbage_collect() -> Result<()> {
        let registry = StateFixture::new().await?;

        registry.garbage_collection().await?;

        registry.teardown().await?;

        Ok(())
    }

    #[test(tokio::test)]
    async fn old_tags() -> Result<()> {
        /*
        Registry is configured to drop tags in the "foo" repository after 5 days.

        1 tag should be dropped.
        */
        let registry =
            StateFixture::with_builder(FixtureBuilder::new().cleanup(DeletionRule::Tag {
                repository: Some(crate::config::lifecycle::StringMatch::Exact("foo".into())),
                tag: None,
                older_than: 5,
            }))
            .await?;

        registry.untag_old_tags().await?;

        registry.client.txn(
            [
                (
                    "INSERT INTO repositories(name) VALUES ('foo') RETURNING id;",
                    vec![],
                ),
                (
                    "INSERT INTO manifests(digest, size, media_type, location, created_by, state) VALUES ('sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5', 0, 'foo', 0, 'george', 1) RETURNING id;",
                    params!(),
                ),
                (
                    "INSERT INTO manifests_repositories(repository_id, manifest_id) VALUES ($1, $2) RETURNING id;",
                    params!(StmtIndex(0).column("id"), StmtIndex(1).column("id")),
                ),
                (
                    "INSERT INTO tags(name, manifest_repository_id, created_at, updated_at) VALUES ('latest', $1, datetime('now', '-50 days'), datetime('now', '-50 days')) RETURNING id;",
                    params!(StmtIndex(2).column("id")),
                ),
            ]
        ).await?;

        assert!(registry.get_tag("foo", "latest").await?.is_some());

        registry.untag_old_tags().await?;

        assert!(registry.get_tag("foo", "latest").await?.is_none());

        registry.teardown().await?;

        Ok(())
    }

    #[test(tokio::test)]
    async fn old_tags_pending() -> Result<()> {
        /*
        Registry is configured to drop tags in the "foo" repository after 500 days.

        Nothing should be dropped.
        */
        let registry =
            StateFixture::with_builder(FixtureBuilder::new().cleanup(DeletionRule::Tag {
                repository: Some(crate::config::lifecycle::StringMatch::Exact("foo".into())),
                tag: None,
                older_than: 500,
            }))
            .await?;

        registry.untag_old_tags().await?;

        registry.client.txn(
            [
                (
                    "INSERT INTO repositories(name) VALUES ('foo') RETURNING id;",
                    vec![],
                ),
                (
                    "INSERT INTO manifests(digest, size, media_type, location, created_by, state) VALUES ('sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5', 0, 'foo', 0, 'george', 1) RETURNING id;",
                    params!(),
                ),
                (
                    "INSERT INTO manifests_repositories(repository_id, manifest_id) VALUES ($1, $2) RETURNING id;",
                    params!(StmtIndex(0).column("id"), StmtIndex(1).column("id")),
                ),
                (
                    "INSERT INTO tags(name, manifest_repository_id, created_at, updated_at) VALUES ('latest', $1, datetime('now', '-50 days'), datetime('now', '-50 days')) RETURNING id;",
                    params!(StmtIndex(2).column("id")),
                ),
            ]
        ).await?;

        assert!(registry.get_tag("foo", "latest").await?.is_some());

        registry.untag_old_tags().await?;

        assert!(registry.get_tag("foo", "latest").await?.is_some());

        registry.teardown().await?;

        Ok(())
    }

    #[test(tokio::test)]
    async fn delete_unreferenced_manifest_repositories_tag_ref() -> Result<()> {
        /*
        Manifest is referenced by a tag so shouldn't be deleted
        */
        let registry = StateFixture::new().await?;

        registry.delete_unreferenced_manifest_repositories().await?;

        registry.client.txn(
            [
                (
                    "INSERT INTO repositories(name) VALUES ('foo') RETURNING id;",
                    vec![],
                ),
                (
                    "INSERT INTO manifests(digest, size, media_type, location, created_by, created_at, state) VALUES ('sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5', 0, 'foo', 1, 'george', datetime('now', '-50 days'), 1) RETURNING id;",
                    params!(),
                ),
                (
                    "INSERT INTO manifests_repositories(repository_id, manifest_id) VALUES ($1, $2) RETURNING id;",
                    params!(StmtIndex(0).column("id"), StmtIndex(1).column("id")),
                ),
                (
                    "INSERT INTO tags(name, manifest_repository_id, created_at, updated_at) VALUES ('latest', $1, datetime('now', '-50 days'), datetime('now', '-50 days')) RETURNING id;",
                    params!(StmtIndex(2).column("id")),
                ),
            ]
        ).await?;

        registry.delete_unreferenced_manifest_repositories().await?;

        let manifest = registry
            .get_manifest(
                &"sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5"
                    .parse()
                    .unwrap(),
            )
            .await?
            .unwrap();

        assert_eq!(manifest.location, 1);
        assert!(manifest.repositories.contains("foo"));

        registry.teardown().await?;

        Ok(())
    }

    #[test(tokio::test)]
    async fn unstore_unreferenced_manifests() -> Result<()> {
        /*
        Manifest isn't referenced by a manifests_repositories entry so should be removed from disk
        */
        let registry = StateFixture::new().await?;

        registry.unstore_unreferenced_manifests().await?;

        registry.client.txn(
            [
                (
                    "INSERT INTO repositories(name) VALUES ('foo') RETURNING id;",
                    vec![],
                ),
                (
                    "INSERT INTO manifests(digest, size, media_type, location, created_by, created_at, state) VALUES ('sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5', 0, 'foo', 1, 'george', datetime('now', '-50 days'), 1) RETURNING id;",
                    params!(),
                ),
            ]
        ).await?;

        registry.unstore_unreferenced_manifests().await?;

        let manifest = registry
            .get_manifest(
                &"sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5"
                    .parse()
                    .unwrap(),
            )
            .await?
            .unwrap();

        assert_eq!(manifest.location, 0);

        registry.teardown().await?;

        Ok(())
    }

    #[test(tokio::test)]
    async fn unstore_unreferenced_manifests_state() -> Result<()> {
        /*
        Manifest isn't referenced by a tag so should be deleted - but its state is 0 so leave it alone
        */
        let registry = StateFixture::new().await?;

        registry.unstore_unreferenced_manifests().await?;

        registry.client.txn(
            [
                (
                    "INSERT INTO repositories(name) VALUES ('foo') RETURNING id;",
                    vec![],
                ),
                (
                    "INSERT INTO manifests(digest, size, media_type, location, created_by, created_at, state) VALUES ('sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5', 0, 'foo', 1, 'george', datetime('now', '-50 days'), 0) RETURNING id;",
                    params!(),
                ),
            ]
        ).await?;

        registry.unstore_unreferenced_manifests().await?;

        let manifest = registry
            .get_manifest(
                &"sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5"
                    .parse()
                    .unwrap(),
            )
            .await?
            .unwrap();

        assert_eq!(manifest.location, 1);

        registry.teardown().await?;

        Ok(())
    }

    #[test(tokio::test)]
    async fn delete_unstored_manifests() -> Result<()> {
        /*
        Manifest isn't referenced by a tag etc and has no copies, drop its database entry
        */
        let registry = StateFixture::new().await?;

        registry.delete_unreferenced_manifests().await?;

        registry.client.txn(
            [
                (
                    "INSERT INTO repositories(name) VALUES ('foo') RETURNING id;",
                    vec![],
                ),
                (
                    "INSERT INTO manifests(digest, size, media_type, location, created_by, created_at, state) VALUES ('sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5', 0, 'foo', 0, 'george', datetime('now', '-50 days'), 1) RETURNING id;",
                    params!(),
                ),
            ]
        ).await?;

        registry.delete_unreferenced_manifests().await?;

        assert!(
            registry
                .get_manifest(
                    &"sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5"
                        .parse()
                        .unwrap()
                )
                .await?
                .is_none()
        );

        registry.teardown().await?;

        Ok(())
    }

    #[test(tokio::test)]
    async fn delete_unstored_manifests_still_mounted() -> Result<()> {
        /*
        Manifest isn't deleted because its still present on a node
        */
        let registry = StateFixture::new().await?;

        registry.delete_unreferenced_manifests().await?;

        registry.client.txn(
            [
                (
                    "INSERT INTO repositories(name) VALUES ('foo') RETURNING id;",
                    vec![],
                ),
                (
                    "INSERT INTO manifests(digest, size, media_type, location, created_by, created_at, state) VALUES ('sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5', 0, 'foo', 1, 'george', datetime('now', '-50 days'), 1) RETURNING id;",
                    params!(),
                ),
            ]
        ).await?;

        registry.delete_unreferenced_manifests().await?;

        assert_eq!(
            registry
                .get_manifest(
                    &"sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5"
                        .parse()
                        .unwrap()
                )
                .await?
                .unwrap()
                .location,
            1
        );

        registry.teardown().await?;

        Ok(())
    }

    #[test(tokio::test)]
    async fn delete_unreferenced_blob_repositories() -> Result<()> {
        /*
        Blob is unmounted because nothing in repository points to it.
        */
        let registry = StateFixture::new().await?;

        registry.delete_unreferenced_manifests().await?;

        registry.client.txn(
            [
                (
                    "INSERT INTO repositories(name) VALUES ('foo') RETURNING id;",
                    vec![],
                ),
                (
                    "INSERT INTO blobs(digest, size, location, created_by, created_at, state) VALUES ('sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5', 0, 0, 'george', datetime('now', '-50 days'), 1) RETURNING id;",
                    vec![],
                ),
                (
                    "INSERT INTO blobs_repositories(blob_id, repository_id, created_at) VALUES($1, $2, datetime('now', '-50 days'));",
                    params!(StmtIndex(1).column("id"), StmtIndex(0).column("id")),
                ),
            ]
        ).await?;

        let blob = registry
            .get_blob(
                &"sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5"
                    .parse()
                    .unwrap(),
            )
            .await?
            .unwrap();
        assert_eq!(blob.repositories, ["foo".to_string()].into_iter().collect());

        assert_eq!(registry.delete_unreferenced_blob_repositories().await?, 1);

        let blob = registry
            .get_blob(
                &"sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5"
                    .parse()
                    .unwrap(),
            )
            .await?
            .unwrap();
        assert_eq!(blob.repositories.len(), 0);

        registry.teardown().await?;

        Ok(())
    }

    #[test(tokio::test)]
    async fn delete_unreferenced_blob_repositories_pending() -> Result<()> {
        /*
        Blob is not unmounted because blob is pending.
        */
        let registry = StateFixture::new().await?;

        registry.delete_unreferenced_manifests().await?;

        registry.client.txn(
            [
                (
                    "INSERT INTO repositories(name) VALUES ('foo') RETURNING id;",
                    vec![],
                ),
                (
                    "INSERT INTO blobs(digest, size, location, created_by, created_at, state) VALUES ('sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5', 0, 0, 'george', datetime('now', '-50 days'), 0) RETURNING id;",
                    vec![],
                ),
                (
                    "INSERT INTO blobs_repositories(blob_id, repository_id, created_at) VALUES($1, $2, datetime('now', '-50 days'));",
                    params!(StmtIndex(1).column("id"), StmtIndex(0).column("id")),
                ),
            ]
        ).await?;

        let blob = registry
            .get_blob(
                &"sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5"
                    .parse()
                    .unwrap(),
            )
            .await?
            .unwrap();
        assert_eq!(blob.repositories, ["foo".to_string()].into_iter().collect());

        assert_eq!(registry.delete_unreferenced_blob_repositories().await?, 0);

        let blob = registry
            .get_blob(
                &"sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5"
                    .parse()
                    .unwrap(),
            )
            .await?
            .unwrap();
        assert_eq!(blob.repositories, ["foo".to_string()].into_iter().collect());

        registry.teardown().await?;

        Ok(())
    }

    #[test(tokio::test)]
    async fn dont_delete_unreferenced_blob_repositories() -> Result<()> {
        /*
        Blob is left alone because its still referenced
        */
        let registry = StateFixture::new().await?;

        registry.delete_unreferenced_manifests().await?;

        registry.client.txn(
            [
                (
                    "INSERT INTO repositories(name) VALUES ('foo') RETURNING id;",
                    vec![],
                ),
                (
                    "INSERT INTO blobs(digest, size, location, created_by, created_at, state) VALUES ('sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5', 0, 0, 'george', datetime('now', '-50 days'), 1) RETURNING id;",
                    vec![],
                ),
                (
                    "INSERT INTO blobs_repositories(blob_id, repository_id, created_at) VALUES($1, $2, datetime('now', '-50 days'));",
                    params!(StmtIndex(1).column("id"), StmtIndex(0).column("id")),
                ),
                (
                    "INSERT INTO manifests(digest, size, media_type, location, created_by, created_at, state) VALUES ('sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5', 0, 'foo', 1, 'george', datetime('now', '-50 days'), 1) RETURNING id;",
                    params!(),
                ),
                (
                    "INSERT INTO manifest_layers(manifest_id, blob_id) VALUES($1, $2);",
                    params!(StmtIndex(3).column("id"), StmtIndex(1).column("id")),
                )
            ]
        ).await?;

        let blob = registry
            .get_blob(
                &"sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5"
                    .parse()
                    .unwrap(),
            )
            .await?
            .unwrap();
        assert_eq!(blob.repositories, ["foo".to_string()].into_iter().collect());

        registry.delete_unreferenced_blob_repositories().await?;

        let blob = registry
            .get_blob(
                &"sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5"
                    .parse()
                    .unwrap(),
            )
            .await?
            .unwrap();
        assert_eq!(blob.repositories, ["foo".to_string()].into_iter().collect());

        registry.teardown().await?;

        Ok(())
    }

    #[test(tokio::test)]
    async fn unstore_unreachable_blobs() -> Result<()> {
        /*
        Blob is deleted because its not in a repo.
        */
        let registry = StateFixture::new().await?;

        registry.unstore_unreachable_blobs().await?;

        registry.client.txn(
            [
                (
                    "INSERT INTO repositories(name) VALUES ('foo') RETURNING id;",
                    vec![],
                ),
                (
                    "INSERT INTO blobs(digest, size, location, created_by, created_at, state) VALUES ('sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5', 0, 1, 'george', datetime('now', '-50 days'), 1) RETURNING id;",
                    vec![],
                )
            ]
        ).await?;

        let blob = registry
            .get_blob(
                &"sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5"
                    .parse()
                    .unwrap(),
            )
            .await?
            .unwrap();
        assert_eq!(blob.repositories, [].into_iter().collect());
        assert_eq!(blob.location, 1);

        assert_eq!(registry.unstore_unreachable_blobs().await?, 1);

        let blob = registry
            .get_blob(
                &"sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5"
                    .parse()
                    .unwrap(),
            )
            .await?
            .unwrap();
        assert_eq!(blob.repositories, [].into_iter().collect());
        assert_eq!(blob.location, 0);

        registry.teardown().await?;

        Ok(())
    }

    #[test(tokio::test)]
    async fn unstore_unreachable_blobs_pending() -> Result<()> {
        /*
        Blob is not deleted because its in pending state
        */
        let registry = StateFixture::new().await?;

        registry.unstore_unreachable_blobs().await?;

        registry.client.txn(
            [
                (
                    "INSERT INTO repositories(name) VALUES ('foo') RETURNING id;",
                    vec![],
                ),
                (
                    "INSERT INTO blobs(digest, size, location, created_by, created_at, state) VALUES ('sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5', 0, 1, 'george', datetime('now', '-50 days'), 0) RETURNING id;",
                    vec![],
                )
            ]
        ).await?;

        let blob = registry
            .get_blob(
                &"sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5"
                    .parse()
                    .unwrap(),
            )
            .await?
            .unwrap();
        assert_eq!(blob.repositories, [].into_iter().collect());
        assert_eq!(blob.location, 1);

        assert_eq!(registry.unstore_unreachable_blobs().await?, 0);

        let blob = registry
            .get_blob(
                &"sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5"
                    .parse()
                    .unwrap(),
            )
            .await?
            .unwrap();
        assert_eq!(blob.repositories, [].into_iter().collect());
        assert_eq!(blob.location, 1);

        registry.teardown().await?;

        Ok(())
    }

    #[test(tokio::test)]
    async fn dont_unstore_unreachable_blobs() -> Result<()> {
        /*
        Blob is left alone because its still in a repo
        */
        let registry = StateFixture::new().await?;

        registry.unstore_unreachable_blobs().await?;

        registry.client.txn(
            [
                (
                    "INSERT INTO repositories(name) VALUES ('foo') RETURNING id;",
                    vec![],
                ),
                (
                    "INSERT INTO blobs(digest, size, location, created_by, created_at, state) VALUES ('sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5', 0, 1, 'george', datetime('now', '-50 days'), 1) RETURNING id;",
                    vec![],
                ),
                (
                    "INSERT INTO blobs_repositories(blob_id, repository_id, created_at) VALUES($1, $2, datetime('now', '-50 days'));",
                    params!(StmtIndex(1).column("id"), StmtIndex(0).column("id")),
                ),
            ]
        ).await?;

        let blob = registry
            .get_blob(
                &"sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5"
                    .parse()
                    .unwrap(),
            )
            .await?
            .unwrap();
        assert_eq!(blob.repositories, ["foo".to_string()].into_iter().collect());
        assert_eq!(blob.location, 1);

        assert_eq!(registry.unstore_unreachable_blobs().await?, 0);

        let blob = registry
            .get_blob(
                &"sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5"
                    .parse()
                    .unwrap(),
            )
            .await?
            .unwrap();
        assert_eq!(blob.repositories, ["foo".to_string()].into_iter().collect());
        assert_eq!(blob.location, 1);

        registry.teardown().await?;

        Ok(())
    }

    #[test(tokio::test)]
    async fn delete_unstored_blobs() -> Result<()> {
        /*
        Blob is deleted because its not in a repo.
        */
        let registry = StateFixture::new().await?;

        registry.delete_unstored_blobs().await?;

        registry.client.txn(
            [
                (
                    "INSERT INTO repositories(name) VALUES ('foo') RETURNING id;",
                    vec![],
                ),
                (
                    "INSERT INTO blobs(digest, size, location, created_by, created_at, state) VALUES ('sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5', 0, 0, 'george', datetime('now', '-50 days'), 1) RETURNING id;",
                    vec![],
                )
            ]
        ).await?;

        let blob = registry
            .get_blob(
                &"sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5"
                    .parse()
                    .unwrap(),
            )
            .await?
            .unwrap();
        assert_eq!(blob.repositories, [].into_iter().collect());

        assert_eq!(registry.delete_unstored_blobs().await?, 1);

        let blob = registry
            .get_blob(
                &"sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5"
                    .parse()
                    .unwrap(),
            )
            .await?;

        assert!(blob.is_none());

        registry.teardown().await?;

        Ok(())
    }

    #[test(tokio::test)]
    async fn dont_delete_unstored_blobs() -> Result<()> {
        /*
        Blob is deleted because its not in a repo.
        */
        let registry = StateFixture::new().await?;

        registry.delete_unstored_blobs().await?;

        registry.client.txn(
            [
                (
                    "INSERT INTO repositories(name) VALUES ('foo') RETURNING id;",
                    vec![],
                ),
                (
                    "INSERT INTO blobs(digest, size, location, created_by, created_at, state) VALUES ('sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5', 0, 1, 'george', datetime('now', '-50 days'), 1) RETURNING id;",
                    vec![],
                )
            ]
        ).await?;

        let blob = registry
            .get_blob(
                &"sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5"
                    .parse()
                    .unwrap(),
            )
            .await?
            .unwrap();
        assert_eq!(blob.repositories, [].into_iter().collect());
        assert_eq!(blob.location, 1);

        assert_eq!(registry.delete_unstored_blobs().await?, 0);

        let blob = registry
            .get_blob(
                &"sha256:24c422e681f1c1bd08286c7aaf5d23a5f088dcdb0b219806b3a9e579244f00c5"
                    .parse()
                    .unwrap(),
            )
            .await?
            .unwrap();
        assert_eq!(blob.repositories, [].into_iter().collect());
        assert_eq!(blob.location, 1);

        registry.teardown().await?;

        Ok(())
    }
}
