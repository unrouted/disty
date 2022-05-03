//! Garbage collection for manifests and blobs.
//!
//! distribd automatically garbage collects blobs and manifests that are no longer referenced by other objects in the DAG.

use chrono::Utc;
use log::info;
use std::path::PathBuf;
use tokio::time::{sleep, Duration};

use crate::{
    machine::Machine,
    types::{RegistryAction, RegistryState},
    utils::{get_blob_path, get_manifest_path},
};

const MINIMUM_GARBAGE_AGE: chrono::Duration = chrono::Duration::seconds(60 * 60 * 12);

async fn do_garbage_collect_phase1(machine: &Machine, state: &RegistryState) {
    /*if !machine.is_leader() {
        info!("Garbage collection: Phase 1: Not leader");
        return;
    }*/

    info!("Garbage collection: Phase 1: Sweeping for mounted objects with no dependents");

    let actions = vec![];

    for manifest in state.get_orphaned_manifests() {
        if (Utc::now() - manifest.created) > MINIMUM_GARBAGE_AGE {
            info!(
                "Garbage collection: Phase 1: {} is orphaned but less than 12 hours old",
                manifest.digest,
            );
            continue;
        }

        for repository in manifest.repositories {
            actions.push(RegistryAction::ManifestUnmounted {
                timestamp: Utc::now(),
                digest: manifest.digest,
                repository: repository,
                user: "$system".to_string(),
            })
        }
    }
    for blob in state.get_orphaned_blobs() {
        if (Utc::now() - blob.created) > MINIMUM_GARBAGE_AGE {
            info!(
                "Garbage collection: Phase 1: {} is orphaned but less than 12 hours old",
                blob.digest,
            );
            continue;
        }
        for repository in blob.repositories {
            actions.push(RegistryAction::BlobUnmounted {
                timestamp: Utc::now(),
                digest: blob.digest,
                repository: repository,
                user: "$system".to_string(),
            })
        }
    }

    if actions.len() > 0 {
        info!(
            "Garbage collection: Phase 1: Reaped {} mounts",
            actions.len()
        );
        send_action(actions);
    }
}

/*
def cleanup_object(image_directory: pathlib.Path, path: pathlib.Path):
if path.exists():
try:
logger.info("Unlinking orphaned object: %s", path)
path.unlink()
except pathlib.FileNotFoundError:
pass

# Clean empty directories caused by cleanup. Given
#
# a = pathlib.Path("/etc/foo")
# b = pathlib.Path("/etc")
#
# a.is_relative_to(b) == True
# a.parent.is_relative_to(b) == True
# a.parent.parent.is_relative_to(b) == False
#
# Then traverse up until the parent directory is not relative to image_directory
# As that means the current path *is* the image directory.

while path.parent.is_relative_to(image_directory):
try:
next(path.iterdir())

except StopIteration:
logger.info("Unlinking empty directory: %s", path)
path.rmdir()

path = path.parent

finally:
break

return True
*/

async fn do_garbage_collect_phase2(
    machine: &Machine,
    state: &RegistryState,
    images_directory: &PathBuf,
) {
    info!("Garbage collection: Phase 2: Sweeping for unmounted objects that can be unstored");

    let actions = vec![];

    for manifest in state.get_orphaned_manifests() {
        if manifest.repositories.len() > 0 {
            continue;
        }

        let path = get_manifest_path(images_directory, manifest.digest);
        if !cleanup_object(images_directory, path) {
            continue;
        }

        actions.push(RegistryAction::ManifestUnstored {
            timestamp: Utc::now(),
            digest: manifest.digest,
            location: machine.identifier,
            user: "$system".to_string(),
        });
    }

    for blob in state.get_orphaned_blobs() {
        if blob.repositories.len() > 0 {
            continue;
        }

        let path = get_blob_path(images_directory, blob.digest);
        if !cleanup_object(images_directory, path) {
            continue;
        }

        actions.push(RegistryAction::BlobUnstored {
            timestamp: Utc::now(),
            digest: blob.digest,
            location: machine.identifier,
            user: "$system".to_string(),
        });
    }

    if actions.len() > 0 {
        info!(
            "Garbage collection: Phase 2: Reaped {} stores",
            actions.len()
        );
        send_action(actions);
    }
}

pub async fn do_garbage_collect(
    machine: &Machine,
    state: &RegistryState,
    image_directory: &PathBuf,
) {
    loop {
        do_garbage_collect_phase1(machine, state).await;
        do_garbage_collect_phase2(machine, state, image_directory).await;
        sleep(Duration::from_secs(60)).await;
    }
}
