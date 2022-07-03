use std::collections::{HashMap, HashSet};
use std::fmt::Debug;

use chrono::{DateTime, Utc};
use log::warn;
use serde::Deserialize;
use serde::Serialize;

use crate::types::{
    Blob, BlobEntry, Digest, Manifest, ManifestEntry, RegistryAction, RepositoryName,
};

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
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

    fn get_or_insert_blob(&mut self, digest: Digest, timestamp: DateTime<Utc>) -> &mut Blob {
        let mut blob = self.blobs.entry(digest).or_insert_with(|| Blob {
            created: timestamp,
            updated: timestamp,
            content_type: None,
            size: None,
            dependencies: Some(vec![]),
            locations: HashSet::new(),
            repositories: HashSet::new(),
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
    ) -> &mut Manifest {
        let mut manifest = self.manifests.entry(digest).or_insert_with(|| Manifest {
            created: timestamp,
            updated: timestamp,
            content_type: None,
            size: None,
            dependencies: Some(vec![]),
            locations: HashSet::new(),
            repositories: HashSet::new(),
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

    pub(crate) fn dispatch_actions(&mut self, actions: &Vec<RegistryAction>) {
        for action in actions {
            match action {
                RegistryAction::Empty {} => {}
                RegistryAction::BlobStored {
                    timestamp,
                    user: _,
                    digest,
                    location,
                } => {
                    let blob = self.get_or_insert_blob(digest.clone(), *timestamp);
                    blob.locations.insert(location.clone());

                    //if location == self.machine_identifier {
                    //    self.blob_available(&digest).await;
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
                    let blob = self.get_or_insert_blob(digest.clone(), *timestamp);
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
                        blob.size = Some(*size);
                    }
                }
                RegistryAction::ManifestStored {
                    timestamp,
                    user: _,
                    digest,
                    location,
                } => {
                    let manifest = self.get_or_insert_manifest(digest.clone(), *timestamp);
                    manifest.locations.insert(location.clone());

                    //if location == self.machine_identifier {
                    //    self.manifest_available(&digest).await;
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
                    let manifest = self.get_or_insert_manifest(digest.clone(), *timestamp);
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
                        manifest.size = Some(*size);
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
