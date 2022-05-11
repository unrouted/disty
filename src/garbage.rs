//! Garbage collection for manifests and blobs.
//!
//! distribd automatically garbage collects blobs and manifests that are no longer referenced by other objects in the DAG.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::Utc;
use log::info;
use tokio::fs::remove_file;
use tokio::time::{sleep, Duration};

use crate::config::Configuration;
use crate::{
    machine::Machine,
    types::{RegistryAction, RegistryState},
    utils::{get_blob_path, get_manifest_path},
};

const MINIMUM_GARBAGE_AGE: i64 = 60 * 60 * 12;

async fn do_garbage_collect_phase1(machine: &Machine, state: &RegistryState) {
    if !machine.is_leader() {
        info!("Garbage collection: Phase 1: Not leader");
        return;
    }

    info!("Garbage collection: Phase 1: Sweeping for mounted objects with no dependents");

    let minimum_age = chrono::Duration::seconds(MINIMUM_GARBAGE_AGE);
    let mut actions = vec![];

    for entry in state.get_orphaned_manifests().await {
        if (Utc::now() - entry.manifest.created) > minimum_age {
            info!(
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
    for entry in state.get_orphaned_blobs().await {
        if (Utc::now() - entry.blob.created) > minimum_age {
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
        state.send_actions(actions).await;
    }
}

async fn cleanup_object(image_directory: &str, path: PathBuf) -> bool {
    let image_directory = Path::new(image_directory).canonicalize().unwrap();
    let path = path.canonicalize().unwrap();

    if path.exists() && remove_file(&path).await.is_err() {
        warn!("Error whilst removing: {:?}", path);
        return false;
    }

    while let Some(path) = path.parent() {
        if path == image_directory {
            // We've hit the root directory
            return true;
        }

        match path.read_dir() {
            Ok(mut iter) => {
                if iter.next().is_some() {
                    // We've hit a shared directory
                    // This counts as a win
                    return true;
                }
            }
            Err(_) => {
                warn!("Error whilst checking contents of {:?}", path);
                return false;
            }
        }

        if tokio::fs::remove_dir(path).await.is_err() {
            warn!("Error whilst removing: {:?}", path);
            return false;
        }
    }

    true
}

async fn do_garbage_collect_phase2(
    machine: &Machine,
    state: &RegistryState,
    images_directory: &str,
) {
    info!("Garbage collection: Phase 2: Sweeping for unmounted objects that can be unstored");

    let mut actions = vec![];

    for entry in state.get_orphaned_manifests().await {
        if !entry.manifest.locations.contains(&machine.identifier) {
            continue;
        }

        if !entry.manifest.repositories.is_empty() {
            continue;
        }

        let path = get_manifest_path(images_directory, &entry.digest);
        if !cleanup_object(images_directory, path).await {
            continue;
        }

        actions.push(RegistryAction::ManifestUnstored {
            timestamp: Utc::now(),
            digest: entry.digest.clone(),
            location: machine.identifier.clone(),
            user: "$system".to_string(),
        });
    }

    for entry in state.get_orphaned_blobs().await {
        if !entry.blob.locations.contains(&machine.identifier) {
            continue;
        }

        if !entry.blob.repositories.is_empty() {
            continue;
        }

        let path = get_blob_path(images_directory, &entry.digest);
        if !cleanup_object(images_directory, path).await {
            continue;
        }

        actions.push(RegistryAction::BlobUnstored {
            timestamp: Utc::now(),
            digest: entry.digest.clone(),
            location: machine.identifier.clone(),
            user: "$system".to_string(),
        });
    }

    if !actions.is_empty() {
        info!(
            "Garbage collection: Phase 2: Reaped {} stores",
            actions.len()
        );
        state.send_actions(actions).await;
    }
}

pub async fn do_garbage_collect(
    config: Configuration,
    machine: Arc<Machine>,
    state: Arc<RegistryState>,
) {
    loop {
        do_garbage_collect_phase1(&machine, &state).await;
        do_garbage_collect_phase2(&machine, &state, &config.storage).await;
        sleep(Duration::from_secs(60)).await;
    }
}
