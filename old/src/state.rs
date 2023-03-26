use std::collections::{HashMap, HashSet};
use std::fmt::Debug;

use chrono::{DateTime, Utc};
use log::warn;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Registry;
use serde::Deserialize;
use serde::Serialize;

use crate::types::{
    Blob, BlobEntry, Digest, Manifest, ManifestEntry, RegistryAction, RepositoryName,
};

#[derive(Clone, Debug)]
pub struct RegistryStateMetrics {
    blob_entry: Gauge,
    blob_disk_usage: Gauge,
    manifest_entry: Gauge,
    manifest_disk_usage: Gauge,
}

impl RegistryStateMetrics {
    pub fn new(registry: &mut Registry) -> Self {
        let registry = registry.sub_registry_with_prefix("distribd_state");

        let blob_entry = Gauge::default();
        registry.register(
            "blob_entries",
            "How many blobs are registered in the store",
            blob_entry.clone(),
        );

        let blob_disk_usage = Gauge::default();
        registry.register(
            "blob_disk_usage",
            "How much disk space the full collection of blobs uses",
            blob_disk_usage.clone(),
        );

        let manifest_entry = Gauge::default();
        registry.register(
            "manifest_entries",
            "How many manifests are registered in the store",
            manifest_entry.clone(),
        );

        let manifest_disk_usage = Gauge::default();
        registry.register(
            "manifest_disk_usage",
            "How much disk space the full collection of manifests uses",
            manifest_disk_usage.clone(),
        );

        Self {
            blob_entry,
            blob_disk_usage,
            manifest_entry,
            manifest_disk_usage,
        }
    }
}

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct RegistryState {
    blobs: HashMap<Digest, Blob>,
    manifests: HashMap<Digest, Manifest>,
    tags: HashMap<RepositoryName, HashMap<String, Digest>>,
}

impl RegistryState {
    fn get_mut_blob(&mut self, digest: &Digest, timestamp: DateTime<Utc>) -> Option<&mut Blob> {
        if let Some(mut blob) = self.blobs.get_mut(digest) {
            blob.updated = timestamp;
            return Some(blob);
        }

        None
    }

    fn get_or_insert_blob(
        &mut self,
        digest: Digest,
        timestamp: DateTime<Utc>,
        metrics: &RegistryStateMetrics,
    ) -> &mut Blob {
        let mut blob = self.blobs.entry(digest).or_insert_with(|| {
            metrics.blob_entry.inc();
            Blob {
                created: timestamp,
                updated: timestamp,
                content_type: None,
                size: None,
                dependencies: Some(vec![]),
                locations: HashSet::new(),
                repositories: HashSet::new(),
            }
        });

        blob.updated = timestamp;

        blob
    }

    fn get_mut_manifest(
        &mut self,
        digest: &Digest,
        timestamp: DateTime<Utc>,
    ) -> Option<&mut Manifest> {
        if let Some(mut manifest) = self.manifests.get_mut(digest) {
            manifest.updated = timestamp;
            return Some(manifest);
        }

        None
    }

    fn get_or_insert_manifest(
        &mut self,
        digest: Digest,
        timestamp: DateTime<Utc>,
        metrics: &RegistryStateMetrics,
    ) -> &mut Manifest {
        let mut manifest = self.manifests.entry(digest).or_insert_with(|| {
            metrics.manifest_entry.inc();

            Manifest {
                created: timestamp,
                updated: timestamp,
                content_type: None,
                size: None,
                dependencies: Some(vec![]),
                locations: HashSet::new(),
                repositories: HashSet::new(),
            }
        });

        manifest.updated = timestamp;

        manifest
    }

    pub fn is_blob_available(&self, repository: &RepositoryName, hash: &Digest) -> bool {
        match self.blobs.get(hash) {
            None => false,
            Some(blob) => blob.repositories.contains(repository),
        }
    }

    pub fn get_blob_directly(&self, hash: &Digest) -> Option<Blob> {
        self.blobs.get(hash).cloned()
    }

    pub fn get_blob(&self, repository: &RepositoryName, hash: &Digest) -> Option<Blob> {
        match self.blobs.get(hash) {
            None => None,
            Some(blob) => {
                if blob.repositories.contains(repository) {
                    return Some(blob.clone());
                }
                None
            }
        }
    }

    pub fn get_manifest_directly(&self, hash: &Digest) -> Option<Manifest> {
        self.manifests.get(hash).cloned()
    }
    pub fn get_manifest(&self, repository: &RepositoryName, hash: &Digest) -> Option<Manifest> {
        match self.manifests.get(hash) {
            None => None,
            Some(manifest) => {
                if manifest.repositories.contains(repository) {
                    return Some(manifest.clone());
                }
                None
            }
        }
    }
    pub fn get_tag(&self, repository: &RepositoryName, tag: &str) -> Option<Digest> {
        match self.tags.get(repository) {
            Some(repository) => repository.get(tag).cloned(),
            None => None,
        }
    }

    pub fn get_tags(&self, repository: &RepositoryName) -> Option<Vec<String>> {
        self.tags
            .get(repository)
            .map(|repository| repository.keys().cloned().collect())
    }

    pub fn is_manifest_available(&self, repository: &RepositoryName, hash: &Digest) -> bool {
        match self.manifests.get(hash) {
            None => false,
            Some(manifest) => manifest.repositories.contains(repository),
        }
    }

    pub fn get_orphaned_blobs(&self) -> Vec<BlobEntry> {
        let blobs: HashSet<Digest> = self.blobs.keys().cloned().collect();

        let mut visited: HashSet<Digest> = HashSet::new();
        let mut visiting: HashSet<Digest> = HashSet::new();

        for manifest in self.manifests.values() {
            if let Some(dependencies) = &manifest.dependencies {
                visiting.extend(dependencies.iter().cloned());
            }
        }

        while let Some(digest) = visiting.iter().next().cloned() {
            match self.blobs.get(&digest) {
                Some(blob) => match &blob.dependencies {
                    Some(dependencies) => {
                        visiting.extend(
                            dependencies
                                .iter()
                                .cloned()
                                .filter(|digest| !visited.contains(digest)),
                        );
                    }
                    None => {}
                },
                _ => {
                    warn!("Dangling dependency found: {digest} missing");
                }
            }

            visiting.remove(&digest);
            visited.insert(digest);
        }

        blobs
            .difference(&visited)
            .cloned()
            .map(|digest| BlobEntry {
                blob: self.blobs.get(&digest).unwrap().clone(),
                digest,
            })
            .collect::<Vec<BlobEntry>>()
    }

    pub fn get_all_blobs(&self) -> Vec<Digest> {
        self.blobs.keys().cloned().collect()
    }

    pub fn get_orphaned_manifests(&self) -> Vec<ManifestEntry> {
        let manifests: HashSet<Digest> = self.manifests.keys().cloned().collect();
        let mut tags: HashSet<Digest> = HashSet::new();

        for repo_tags in self.tags.values() {
            tags.extend(repo_tags.values().cloned());
        }

        manifests
            .difference(&tags)
            .cloned()
            .map(|digest| ManifestEntry {
                manifest: self.manifests.get(&digest).unwrap().clone(),
                digest,
            })
            .collect::<Vec<ManifestEntry>>()
    }

    pub fn get_all_manifests(&self) -> Vec<Digest> {
        self.manifests.keys().cloned().collect()
    }

    pub fn apply_state_metrics(&self, metrics: &RegistryStateMetrics) {
        metrics.blob_disk_usage.set(0);
        metrics.blob_entry.set(0);
        for blob in self.get_all_blobs() {
            if let Some(entry) = self.get_blob_directly(&blob) {
                metrics.blob_entry.inc();
                if let Some(size) = entry.size {
                    metrics.blob_disk_usage.inc_by(size.try_into().unwrap());
                }
            }
        }

        metrics.manifest_disk_usage.set(0);
        metrics.manifest_entry.set(0);
        for blob in self.get_all_manifests() {
            if let Some(entry) = self.get_manifest_directly(&blob) {
                metrics.manifest_entry.inc();
                if let Some(size) = entry.size {
                    metrics.manifest_disk_usage.inc_by(size.try_into().unwrap());
                }
            }
        }
    }

    pub(crate) fn dispatch_actions(
        &mut self,
        actions: &Vec<RegistryAction>,
        metrics: &RegistryStateMetrics,
    ) {
        for action in actions {
            match action {
                RegistryAction::Empty {} => {}
                RegistryAction::BlobStored {
                    timestamp,
                    user: _,
                    digest,
                    location,
                } => {
                    let blob = self.get_or_insert_blob(digest.clone(), *timestamp, metrics);
                    blob.locations.insert(location.clone());

                    //if location == self.machine_identifier {
                    //    self.blob_available(&digest);
                    //}
                }
                RegistryAction::BlobUnstored {
                    timestamp,
                    user: _,
                    digest,
                    location,
                } => {
                    if let Some(blob) = self.get_mut_blob(digest, *timestamp) {
                        blob.locations.remove(location);

                        if blob.locations.is_empty() {
                            metrics.blob_entry.dec();
                            if let Some(size) = blob.size {
                                metrics.blob_disk_usage.dec_by(size.try_into().unwrap());
                            }
                            self.blobs.remove(digest);
                        }
                    }
                }
                RegistryAction::BlobMounted {
                    timestamp,
                    user: _,
                    digest,
                    repository,
                } => {
                    let blob = self.get_or_insert_blob(digest.clone(), *timestamp, metrics);
                    blob.repositories.insert(repository.clone());
                }
                RegistryAction::BlobUnmounted {
                    timestamp,
                    user: _,
                    digest,
                    repository,
                } => {
                    if let Some(blob) = self.get_mut_blob(digest, *timestamp) {
                        blob.repositories.remove(repository);
                    }
                }
                RegistryAction::BlobInfo {
                    timestamp,
                    digest,
                    dependencies,
                    content_type,
                } => {
                    if let Some(mut blob) = self.get_mut_blob(digest, *timestamp) {
                        blob.dependencies = Some(dependencies.clone());
                        blob.content_type = Some(content_type.clone());
                    }
                }
                RegistryAction::BlobStat {
                    timestamp,
                    digest,
                    size,
                } => {
                    if let Some(mut blob) = self.get_mut_blob(digest, *timestamp) {
                        if let Some(old_size) = blob.size {
                            metrics.blob_disk_usage.dec_by(old_size.try_into().unwrap());
                        }
                        blob.size = Some(*size);
                        metrics.blob_disk_usage.inc_by((*size).try_into().unwrap());
                    }
                }
                RegistryAction::ManifestStored {
                    timestamp,
                    user: _,
                    digest,
                    location,
                } => {
                    let manifest = self.get_or_insert_manifest(digest.clone(), *timestamp, metrics);
                    manifest.locations.insert(location.clone());

                    //if location == self.machine_identifier {
                    //    self.manifest_available(&digest);
                    // }
                }
                RegistryAction::ManifestUnstored {
                    timestamp,
                    user: _,
                    digest,
                    location,
                } => {
                    if let Some(manifest) = self.get_mut_manifest(digest, *timestamp) {
                        manifest.locations.remove(location);

                        if manifest.locations.is_empty() {
                            metrics.manifest_entry.dec();
                            if let Some(size) = manifest.size {
                                metrics.manifest_disk_usage.dec_by(size.try_into().unwrap());
                            }
                            self.manifests.remove(digest);
                        }
                    }
                }
                RegistryAction::ManifestMounted {
                    timestamp,
                    user: _,
                    digest,
                    repository,
                } => {
                    let manifest = self.get_or_insert_manifest(digest.clone(), *timestamp, metrics);
                    manifest.repositories.insert(repository.clone());
                }
                RegistryAction::ManifestUnmounted {
                    timestamp,
                    user: _,
                    digest,
                    repository,
                } => {
                    if let Some(manifest) = self.get_mut_manifest(digest, *timestamp) {
                        manifest.repositories.remove(repository);

                        if let Some(tags) = self.tags.get_mut(repository) {
                            tags.retain(|_, value| value != digest);
                        }
                    }
                }
                RegistryAction::ManifestInfo {
                    timestamp,
                    digest,
                    dependencies,
                    content_type,
                } => {
                    if let Some(mut manifest) = self.get_mut_manifest(digest, *timestamp) {
                        manifest.dependencies = Some(dependencies.clone());
                        manifest.content_type = Some(content_type.clone());
                    }
                }
                RegistryAction::ManifestStat {
                    timestamp,
                    digest,
                    size,
                } => {
                    if let Some(mut manifest) = self.get_mut_manifest(digest, *timestamp) {
                        if let Some(old_size) = manifest.size {
                            metrics
                                .manifest_disk_usage
                                .dec_by(old_size.try_into().unwrap());
                        }
                        manifest.size = Some(*size);
                        metrics
                            .manifest_disk_usage
                            .inc_by((*size).try_into().unwrap());
                    }
                }
                RegistryAction::HashTagged {
                    timestamp: _,
                    user: _,
                    digest,
                    repository,
                    tag,
                } => {
                    let repository = self
                        .tags
                        .entry(repository.clone())
                        .or_insert_with(HashMap::new);
                    repository.insert(tag.clone(), digest.clone());
                }
            }
        }
    }
}
