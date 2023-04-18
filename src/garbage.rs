//! Garbage collection for manifests and blobs.
//!
//! distribd automatically garbage collects blobs and manifests that are no longer referenced by other objects in the DAG.

use std::io::ErrorKind;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use chrono::Utc;
use tokio::fs::remove_file;
use tracing::{debug, error, info};

use crate::app::RegistryApp;
use crate::types::RegistryAction;

const MINIMUM_GARBAGE_AGE: i64 = 60 * 60 * 12;

async fn do_garbage_collect_phase1(app: &Arc<RegistryApp>) -> anyhow::Result<()> {
    debug!("Garbage collection: Phase 1: Sweeping for mounted objects with no dependents");

    let minimum_age = chrono::Duration::seconds(MINIMUM_GARBAGE_AGE);
    let mut actions = vec![];

    for (digest, manifest) in app.store.get_orphaned_manifests()? {
        let age = Utc::now() - manifest.created;
        if age < minimum_age {
            debug!(
                "Garbage collection: Phase 1: {} is orphaned but less than 12 hours old",
                &digest,
            );
            continue;
        }

        for repository in manifest.repositories {
            actions.push(RegistryAction::ManifestUnmounted {
                timestamp: Utc::now(),
                digest: digest.clone(),
                repository,
                user: "$system".to_string(),
            })
        }
    }
    for (digest, blob) in app.store.get_orphaned_blobs()? {
        let age = Utc::now() - blob.created;
        if age < minimum_age {
            info!(
                "Garbage collection: Phase 1: {} is orphaned but less than 12 hours old",
                &digest,
            );
            continue;
        }
        for repository in blob.repositories {
            actions.push(RegistryAction::BlobUnmounted {
                timestamp: Utc::now(),
                digest: digest.clone(),
                repository,
                user: "$system".to_string(),
            })
        }
    }

    if !actions.is_empty() {
        info!(
            "Garbage collection: Phase 1: Reaped {} mounts",
            actions.len()
        );
        app.submit_write(actions).await;
    }

    Ok(())
}

async fn cleanup_object(path: &PathBuf) -> anyhow::Result<()> {
    if path.exists() {
        remove_file(&path)
            .await
            .context(format!("Error while removing {path:?}"))?;

        info!("Garbage collection: Removed file {path:?}");
    }

    for path in path.parent().unwrap().ancestors().take(3) {
        match path.read_dir() {
            Ok(mut iter) => {
                if iter.next().is_some() {
                    // We've hit a shared directory
                    // This counts as a win
                    return Ok(());
                }
            }
            Err(err) => match err.kind() {
                ErrorKind::NotFound => {
                    continue;
                }
                _ => {
                    Err(err).context(format!("Error whilst reading contents of {path:?}"))?;
                }
            },
        }

        match tokio::fs::remove_dir(path).await {
            Ok(_) => {
                info!("Garbage collection: Removed directory {path:?}");
            }
            Err(err) => match err.kind() {
                ErrorKind::NotFound => {
                    continue;
                }
                _ => {
                    Err(err).context(format!("Error whilst removing {path:?}"))?;
                }
            },
        }
    }

    Ok(())
}

async fn do_garbage_collect_phase2(app: &Arc<RegistryApp>) -> anyhow::Result<()> {
    debug!("Garbage collection: Phase 2: Sweeping for unmounted objects that can be unstored");

    let mut actions = vec![];

    for (digest, manifest) in app.store.get_orphaned_manifests()? {
        if !manifest.locations.contains(&app.config.identifier) {
            continue;
        }

        if !manifest.repositories.is_empty() {
            continue;
        }

        let path = app.get_manifest_path(&digest);
        if let Err(err) = cleanup_object(&path).await {
            error!("Unable to cleanup filesystem for: {path:?}: {err:?}");
            continue;
        }

        actions.push(RegistryAction::ManifestUnstored {
            timestamp: Utc::now(),
            digest: digest.clone(),
            location: app.config.identifier.clone(),
            user: "$system".to_string(),
        });
    }

    for (digest, blob) in app.store.get_orphaned_blobs()? {
        if !blob.locations.contains(&app.config.identifier) {
            continue;
        }

        if !blob.repositories.is_empty() {
            continue;
        }

        let path = app.get_blob_path(&digest);
        if let Err(err) = cleanup_object(&path).await {
            error!("Unable to cleanup filesystem for: {path:?}: {err:?}");
            continue;
        }

        actions.push(RegistryAction::BlobUnstored {
            timestamp: Utc::now(),
            digest: digest.clone(),
            location: app.config.identifier.clone(),
            user: "$system".to_string(),
        });
    }

    if !actions.is_empty() {
        info!(
            "Garbage collection: Phase 2: Reaped {} stores",
            actions.len()
        );
        app.submit_write(actions).await;
    }

    Ok(())
}

pub async fn do_garbage_collect(app: Arc<RegistryApp>) -> anyhow::Result<()> {
    loop {
        let metrics = app.raft.metrics().borrow().clone();

        if matches!(metrics.state, openraft::ServerState::Leader) {
            do_garbage_collect_phase1(&app).await?;
        }

        if matches!(metrics.state, openraft::ServerState::Leader | openraft::ServerState::Follower) {
            do_garbage_collect_phase2(&app).await?;
        }

        if matches!(metrics.state, openraft::ServerState::Shutdown) {
            break;
        }

        tokio::time::sleep(core::time::Duration::from_secs(60)).await;
    }

    Ok(())
}
