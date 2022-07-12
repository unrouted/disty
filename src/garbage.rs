//! Garbage collection for manifests and blobs.
//!
//! distribd automatically garbage collects blobs and manifests that are no longer referenced by other objects in the DAG.

use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context;
use chrono::Utc;
use log::{debug, error, info};
use tokio::fs::remove_file;
use tokio::select;

use crate::app::RegistryApp;
use crate::{
    types::RegistryAction,
    utils::{get_blob_path, get_manifest_path},
};

const MINIMUM_GARBAGE_AGE: i64 = 60 * 60 * 12;

async fn do_garbage_collect_phase1(app: &Arc<RegistryApp>) {
    if !app.is_leader().await {
        debug!("Garbage collection: Phase 1: Not leader");
        return;
    }

    debug!("Garbage collection: Phase 1: Sweeping for mounted objects with no dependents");

    let minimum_age = chrono::Duration::seconds(MINIMUM_GARBAGE_AGE);
    let mut actions = vec![];

    for entry in app.get_orphaned_manifests().await {
        let age = Utc::now() - entry.manifest.created;
        if age < minimum_age {
            debug!(
                "Garbage collection: Phase 1: {} is orphaned but less than 12 hours old",
                &entry.digest,
            );
            continue;
        }

        for repository in entry.manifest.repositories {
            actions.push(RegistryAction::ManifestUnmounted {
                timestamp: Utc::now(),
                digest: entry.digest.clone(),
                repository,
                user: "$system".to_string(),
            })
        }
    }
    for entry in app.get_orphaned_blobs().await {
        let age = Utc::now() - entry.blob.created;
        if age < minimum_age {
            info!(
                "Garbage collection: Phase 1: {} is orphaned but less than 12 hours old",
                &entry.digest,
            );
            continue;
        }
        for repository in entry.blob.repositories {
            actions.push(RegistryAction::BlobUnmounted {
                timestamp: Utc::now(),
                digest: entry.digest.clone(),
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
        app.submit(actions).await;
    }
}

async fn cleanup_object(image_directory: &str, path: &PathBuf) -> anyhow::Result<()> {
    if path.exists() {
        remove_file(&path)
            .await
            .context(format!("Error while removing {path:?}"))?;
    }

    while let Some(path) = path.parent() {
        let stripped = path
            .strip_prefix(image_directory)
            .context(format!("Error whilst stripping: {path:?}"))?;
        if stripped == Path::new("") {
            // We've hit the root directory
            return Ok(());
        }

        match path.read_dir() {
            Ok(mut iter) => {
                if iter.next().is_some() {
                    // We've hit a shared directory
                    // This counts as a win
                    return Ok(());
                }
            }
            Err(err) => match err.kind() {
                ErrorKind::NotFound => {}
                _ => {
                    Err(err).context(format!("Error whilst reading contents of {path:?}"))?;
                }
            },
        }

        match tokio::fs::remove_dir(path).await {
            Ok(_) => {}
            Err(err) => match err.kind() {
                ErrorKind::NotFound => {}
                _ => {
                    Err(err).context(format!("Error whilst removing {path:?}"))?;
                }
            },
        }
    }

    Ok(())
}

async fn do_garbage_collect_phase2(app: &Arc<RegistryApp>) {
    debug!("Garbage collection: Phase 2: Sweeping for unmounted objects that can be unstored");

    let images_directory = &app.settings.storage;

    let mut actions = vec![];

    for entry in app.get_orphaned_manifests().await {
        if !entry.manifest.locations.contains(&app.settings.identifier) {
            continue;
        }

        if !entry.manifest.repositories.is_empty() {
            continue;
        }

        let path = get_manifest_path(images_directory, &entry.digest);
        if let Err(err) = cleanup_object(images_directory, &path).await {
            error!("Unable to cleanup filesystem for: {path:?}: {err:?}");
            continue;
        }

        actions.push(RegistryAction::ManifestUnstored {
            timestamp: Utc::now(),
            digest: entry.digest.clone(),
            location: app.settings.identifier.clone(),
            user: "$system".to_string(),
        });
    }

    for entry in app.get_orphaned_blobs().await {
        if !entry.blob.locations.contains(&app.settings.identifier) {
            continue;
        }

        if !entry.blob.repositories.is_empty() {
            continue;
        }

        let path = get_blob_path(images_directory, &entry.digest);
        if let Err(err) = cleanup_object(images_directory, &path).await {
            error!("Unable to cleanup filesystem for: {path:?}: {err:?}");
            continue;
        }

        actions.push(RegistryAction::BlobUnstored {
            timestamp: Utc::now(),
            digest: entry.digest.clone(),
            location: app.settings.identifier.clone(),
            user: "$system".to_string(),
        });
    }

    if !actions.is_empty() {
        info!(
            "Garbage collection: Phase 2: Reaped {} stores",
            actions.len()
        );
        app.submit(actions).await;
    }
}

pub async fn do_garbage_collect(app: Arc<RegistryApp>) -> anyhow::Result<()> {
    let mut lifecycle = app.subscribe_lifecycle();

    loop {
        let leader_id = app.group.read().await.raft.leader_id;

        if leader_id > 0 {
            info!("Garbage collection: Skipped as leader not known");
        }

        do_garbage_collect_phase1(&app).await;
        do_garbage_collect_phase2(&app).await;

        select! {
            _ = tokio::time::sleep(core::time::Duration::from_secs(60)) => {},
            Ok(_ev) = lifecycle.recv() => {
                info!("Garbage collection: Graceful shutdown");
                break;
            }
        };
    }

    Ok(())
}
