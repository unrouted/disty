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

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_state() -> RegistryState {
        RegistryState::default()
    }

    // BLOB TESTS

    #[test]
    fn blob_not_available_initially() {
        let state = setup_state();

        let repository = "myrepo".parse().unwrap();
        let digest = "sha256:abcdefg".parse().unwrap();

        assert!(!state.is_blob_available(&repository, &digest))
    }

    #[test]
    fn blob_becomes_available() {
        let mut state = setup_state();

        let repository = "myrepo".parse().unwrap();
        let digest = "sha256:abcdefg".parse().unwrap();

        state.dispatch_actions(&vec![RegistryAction::BlobMounted {
            timestamp: Utc::now(),
            user: "test".to_string(),
            repository,
            digest,
        }]);

        let repository = "myrepo".parse().unwrap();
        let digest = "sha256:abcdefg".parse().unwrap();

        assert!(state.is_blob_available(&repository, &digest));
    }

    #[test]
    fn blob_metadata() {
        let mut state = setup_state();

        let repository: RepositoryName = "myrepo".parse().unwrap();
        let digest: Digest = "sha256:abcdefg".parse().unwrap();
        let dependency: Digest = "sha256:zxyjkl".parse().unwrap();

        state.dispatch_actions(&vec![
            RegistryAction::BlobMounted {
                timestamp: Utc::now(),
                user: "test".to_string(),
                repository,
                digest: digest.clone(),
            },
            RegistryAction::BlobInfo {
                timestamp: Utc::now(),
                digest,
                content_type: "application/json".to_string(),
                dependencies: vec![dependency],
            },
        ]);

        let digest: Digest = "sha256:abcdefg".parse().unwrap();

        let item = state.get_blob_directly(&digest).unwrap();
        assert_eq!(item.content_type, Some("application/json".to_string()));
        assert_eq!(item.dependencies.as_ref().unwrap().len(), 1);

        let dependencies = vec!["sha256:zxyjkl".parse().unwrap()];
        assert_eq!(item.dependencies, Some(dependencies));
    }

    #[test]
    fn blob_size() {
        let mut state = setup_state();

        let repository = "myrepo".parse().unwrap();
        let digest = "sha256:abcdefg".parse().unwrap();

        state.dispatch_actions(&vec![RegistryAction::BlobMounted {
            timestamp: Utc::now(),
            user: "test".to_string(),
            repository,
            digest,
        }]);

        let digest = "sha256:abcdefg".parse().unwrap();

        state.dispatch_actions(&vec![RegistryAction::BlobStat {
            timestamp: Utc::now(),
            digest,
            size: 1234,
        }]);

        let digest: Digest = "sha256:abcdefg".parse().unwrap();
        let item = state.get_blob_directly(&digest).unwrap();

        assert_eq!(item.size, Some(1234));
    }

    #[test]
    fn blob_becomes_unavailable() {
        let mut state = setup_state();

        let repository = "myrepo".parse().unwrap();
        let digest = "sha256:abcdefg".parse().unwrap();

        state.dispatch_actions(&vec![RegistryAction::BlobMounted {
            timestamp: Utc::now(),
            user: "test".to_string(),
            repository,
            digest,
        }]);

        let repository = "myrepo".parse().unwrap();
        let digest = "sha256:abcdefg".parse().unwrap();

        state.dispatch_actions(&vec![RegistryAction::BlobUnmounted {
            timestamp: Utc::now(),
            user: "test".to_string(),
            repository,
            digest,
        }]);

        let repository = "myrepo".parse().unwrap();
        let digest = "sha256:abcdefg".parse().unwrap();

        assert!(!state.is_blob_available(&repository, &digest));
    }

    #[test]
    fn blob_becomes_available_again() {
        let mut state = setup_state();

        // Create node
        let repository = "myrepo".parse().unwrap();
        let digest = "sha256:abcdefg".parse().unwrap();

        state.dispatch_actions(&vec![RegistryAction::BlobMounted {
            timestamp: Utc::now(),
            user: "test".to_string(),
            repository,
            digest,
        }]);

        // Make node unavailable
        let repository = "myrepo".parse().unwrap();
        let digest = "sha256:abcdefg".parse().unwrap();

        state.dispatch_actions(&vec![RegistryAction::BlobUnmounted {
            timestamp: Utc::now(),
            user: "test".to_string(),
            repository,
            digest,
        }]);

        // Make node available again
        let repository = "myrepo".parse().unwrap();
        let digest = "sha256:abcdefg".parse().unwrap();

        state.dispatch_actions(&vec![RegistryAction::BlobMounted {
            timestamp: Utc::now(),
            user: "test".to_string(),
            repository,
            digest,
        }]);

        // Should be visible...
        let repository = "myrepo".parse().unwrap();
        let digest = "sha256:abcdefg".parse().unwrap();

        assert!(state.is_blob_available(&repository, &digest));
    }

    // MANIFEST TESTS

    #[test]
    fn manifest_not_available_initially() {
        let state = setup_state();

        let repository = "myrepo".parse().unwrap();
        let digest = "sha256:abcdefg".parse().unwrap();

        assert!(!state.is_manifest_available(&repository, &digest))
    }

    #[test]
    fn manifest_becomes_available() {
        let mut state = setup_state();

        let repository = "myrepo".parse().unwrap();
        let digest = "sha256:abcdefg".parse().unwrap();

        state.dispatch_actions(&vec![RegistryAction::ManifestMounted {
            timestamp: Utc::now(),
            user: "test".to_string(),
            repository,
            digest,
        }]);

        let repository = "myrepo".parse().unwrap();
        let digest = "sha256:abcdefg".parse().unwrap();

        assert!(state.is_manifest_available(&repository, &digest));
    }

    #[test]
    fn manifest_metadata() {
        let mut state = setup_state();

        let repository = "myrepo".parse().unwrap();
        let digest = "sha256:abcdefg".parse().unwrap();

        state.dispatch_actions(&vec![RegistryAction::ManifestMounted {
            timestamp: Utc::now(),
            user: "test".to_string(),
            repository,
            digest,
        }]);

        let digest = "sha256:abcdefg".parse().unwrap();
        let dependency: Digest = "sha256:zxyjkl".parse().unwrap();

        state.dispatch_actions(&vec![RegistryAction::ManifestInfo {
            timestamp: Utc::now(),
            digest,
            content_type: "application/json".to_string(),
            dependencies: vec![dependency],
        }]);

        let digest: Digest = "sha256:abcdefg".parse().unwrap();
        let item = state.get_manifest_directly(&digest).unwrap();

        assert_eq!(item.content_type, Some("application/json".to_string()));
        assert_eq!(item.dependencies.as_ref().unwrap().len(), 1);

        let dependencies = vec!["sha256:zxyjkl".parse().unwrap()];
        assert_eq!(item.dependencies, Some(dependencies));
    }

    #[test]
    fn manifest_size() {
        let mut state = setup_state();

        let repository = "myrepo".parse().unwrap();
        let digest = "sha256:abcdefg".parse().unwrap();

        state.dispatch_actions(&vec![RegistryAction::ManifestMounted {
            timestamp: Utc::now(),
            user: "test".to_string(),
            repository,
            digest,
        }]);

        let digest = "sha256:abcdefg".parse().unwrap();

        state.dispatch_actions(&vec![RegistryAction::ManifestStat {
            timestamp: Utc::now(),
            digest,
            size: 1234,
        }]);

        let digest: Digest = "sha256:abcdefg".parse().unwrap();
        let item = state.get_manifest_directly(&digest).unwrap();

        assert_eq!(item.size, Some(1234));
    }

    #[test]
    fn manifest_becomes_unavailable() {
        let mut state = setup_state();

        let repository = "myrepo".parse().unwrap();
        let digest = "sha256:abcdefg".parse().unwrap();

        state.dispatch_actions(&vec![RegistryAction::ManifestMounted {
            timestamp: Utc::now(),
            user: "test".to_string(),
            repository,
            digest,
        }]);

        let repository = "myrepo".parse().unwrap();
        let digest = "sha256:abcdefg".parse().unwrap();

        state.dispatch_actions(&vec![RegistryAction::ManifestUnmounted {
            timestamp: Utc::now(),
            user: "test".to_string(),
            repository,
            digest,
        }]);

        let repository = "myrepo".parse().unwrap();
        let digest = "sha256:abcdefg".parse().unwrap();

        assert!(!state.is_manifest_available(&repository, &digest));
    }

    #[test]
    fn manifest_becomes_available_again() {
        let mut state = setup_state();

        // Create node
        let repository = "myrepo".parse().unwrap();
        let digest = "sha256:abcdefg".parse().unwrap();

        state.dispatch_actions(&vec![RegistryAction::ManifestMounted {
            timestamp: Utc::now(),
            user: "test".to_string(),
            repository,
            digest,
        }]);

        // Make node unavailable
        let repository = "myrepo".parse().unwrap();
        let digest = "sha256:abcdefg".parse().unwrap();

        state.dispatch_actions(&vec![RegistryAction::ManifestUnmounted {
            timestamp: Utc::now(),
            user: "test".to_string(),
            repository,
            digest,
        }]);

        // Make node available again
        let repository = "myrepo".parse().unwrap();
        let digest = "sha256:abcdefg".parse().unwrap();

        state.dispatch_actions(&vec![RegistryAction::ManifestMounted {
            timestamp: Utc::now(),
            user: "test".to_string(),
            repository,
            digest,
        }]);

        // Should be visible...
        let repository = "myrepo".parse().unwrap();
        let digest = "sha256:abcdefg".parse().unwrap();

        assert!(state.is_manifest_available(&repository, &digest));
    }

    #[test]
    fn can_tag_manifest() {
        let mut state = setup_state();

        // Create node
        let repository = "myrepo".parse().unwrap();
        let digest = "sha256:abcdefg".parse().unwrap();

        state.dispatch_actions(&vec![RegistryAction::HashTagged {
            timestamp: Utc::now(),
            user: "test".to_string(),
            repository,
            digest,
            tag: "latest".to_string(),
        }]);

        let repository = "myrepo2".parse().unwrap();
        assert!(matches!(state.get_tags(&repository), None));

        let repository = "myrepo".parse().unwrap();
        assert_eq!(
            state.get_tags(&repository).unwrap(),
            vec!["latest".to_string()]
        );
    }

    #[test]
    fn can_collect_orphaned_manifests() {
        let mut state = setup_state();

        // Create node
        let repository: RepositoryName = "myrepo".parse().unwrap();
        let digest1: Digest = "sha256:abcdefg".parse().unwrap();
        let digest2: Digest = "sha256:gfedcba".parse().unwrap();

        state.dispatch_actions(&vec![
            RegistryAction::ManifestStored {
                timestamp: Utc::now(),
                user: "test".to_string(),
                location: "test".to_string(),
                digest: digest1.clone(),
            },
            RegistryAction::ManifestMounted {
                timestamp: Utc::now(),
                user: "test".to_string(),
                repository: repository.clone(),
                digest: digest1.clone(),
            },
            RegistryAction::HashTagged {
                timestamp: Utc::now(),
                user: "test".to_string(),
                repository: repository.clone(),
                digest: digest1.clone(),
                tag: "latest".to_string(),
            },
            RegistryAction::ManifestStored {
                timestamp: Utc::now(),
                user: "test".to_string(),
                location: "test".to_string(),
                digest: digest2.clone(),
            },
            RegistryAction::ManifestMounted {
                timestamp: Utc::now(),
                user: "test".to_string(),
                repository: repository.clone(),
                digest: digest2.clone(),
            },
            RegistryAction::HashTagged {
                timestamp: Utc::now(),
                user: "test".to_string(),
                repository,
                digest: digest2,
                tag: "latest".to_string(),
            },
        ]);

        let collected = state.get_orphaned_manifests();
        assert_eq!(collected.len(), 1);

        let entry = collected.first().unwrap();
        assert_eq!(entry.digest, digest1);
        assert!(entry.manifest.locations.contains("test"));
    }

    #[test]
    fn can_collect_orphaned_blobs() {
        let mut state = setup_state();

        // Create node
        let repository: RepositoryName = "myrepo".parse().unwrap();
        let digest1: Digest = "sha256:abcdefg".parse().unwrap();
        let digest2: Digest = "sha256:gfedcba".parse().unwrap();
        let digest3: Digest = "sha256:aaaaaaa".parse().unwrap();
        let digest4: Digest = "sha256:bbbbbbb".parse().unwrap();
        let manifest_digest: Digest = "sha256:ababababababa".parse().unwrap();

        state.dispatch_actions(&vec![
            // BLOB 1 DAG
            RegistryAction::BlobStored {
                timestamp: Utc::now(),
                user: "test".to_string(),
                location: "test".to_string(),
                digest: digest1.clone(),
            },
            RegistryAction::BlobMounted {
                timestamp: Utc::now(),
                user: "test".to_string(),
                repository: repository.clone(),
                digest: digest1.clone(),
            },
            RegistryAction::BlobStored {
                timestamp: Utc::now(),
                user: "test".to_string(),
                location: "test".to_string(),
                digest: digest2.clone(),
            },
            RegistryAction::BlobMounted {
                timestamp: Utc::now(),
                user: "test".to_string(),
                repository: repository.clone(),
                digest: digest2.clone(),
            },
            RegistryAction::BlobInfo {
                timestamp: Utc::now(),
                digest: digest2.clone(),
                content_type: "foo".to_string(),
                dependencies: vec![digest1.clone()],
            },
            // BLOB 2 DAG
            RegistryAction::BlobStored {
                timestamp: Utc::now(),
                user: "test".to_string(),
                location: "test".to_string(),
                digest: digest3.clone(),
            },
            RegistryAction::BlobMounted {
                timestamp: Utc::now(),
                user: "test".to_string(),
                repository: repository.clone(),
                digest: digest3.clone(),
            },
            RegistryAction::BlobStored {
                timestamp: Utc::now(),
                user: "test".to_string(),
                location: "test".to_string(),
                digest: digest4.clone(),
            },
            RegistryAction::BlobMounted {
                timestamp: Utc::now(),
                user: "test".to_string(),
                repository: repository.clone(),
                digest: digest4.clone(),
            },
            RegistryAction::BlobInfo {
                timestamp: Utc::now(),
                digest: digest4.clone(),
                content_type: "foo".to_string(),
                dependencies: vec![digest3],
            },
            // MANIFEST DAG
            RegistryAction::ManifestStored {
                timestamp: Utc::now(),
                user: "test".to_string(),
                location: "test".to_string(),
                digest: manifest_digest.clone(),
            },
            RegistryAction::ManifestMounted {
                timestamp: Utc::now(),
                user: "test".to_string(),
                repository: repository.clone(),
                digest: manifest_digest.clone(),
            },
            RegistryAction::ManifestInfo {
                timestamp: Utc::now(),
                digest: manifest_digest.clone(),
                content_type: "foo".to_string(),
                dependencies: vec![digest4],
            },
        ]);

        let collected = state.get_orphaned_blobs();
        assert_eq!(collected.len(), 2);

        for blob in collected {
            match &blob {
                BlobEntry { blob, digest } if digest == &digest1 => {
                    assert_eq!(blob.dependencies.as_ref().unwrap().len(), 0);
                }
                BlobEntry { blob, digest } if digest == &digest2 => {
                    assert_eq!(blob.dependencies.as_ref().unwrap().len(), 1);
                }
                _ => {
                    panic!("Unexpected digest was collected")
                }
            }
        }

        // If we delete the manifest all blobs should now be garbage collected

        state.dispatch_actions(&vec![
            RegistryAction::ManifestUnmounted {
                timestamp: Utc::now(),
                user: "test".to_string(),
                repository,
                digest: manifest_digest.clone(),
            },
            RegistryAction::ManifestUnstored {
                timestamp: Utc::now(),
                user: "test".to_string(),
                location: "test".to_string(),
                digest: manifest_digest,
            },
        ]);

        let collected = state.get_orphaned_blobs();
        assert_eq!(collected.len(), 4);
    }
}
