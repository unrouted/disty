use super::digest::Digest;
use super::RegistryAction;
use super::{Repository, RepositoryName};
use std::collections::{HashMap, HashSet};

struct BlobUpload {
    pub repository: RepositoryName,
    pub position: usize,
    pub deleted: bool,
    pub finished: bool,
}

impl Default for BlobUpload {
    fn default() -> Self {
        BlobUpload {
            repository: RepositoryName {
                name: "".to_string(),
            },
            position: 0,
            deleted: false,
            finished: false,
        }
    }
}

struct Blob {
    pub locations: HashSet<String>,
    pub repositories: HashSet<RepositoryName>,
    pub content_type: Option<String>,
    pub size: Option<usize>,
    pub dependencies: Vec<Digest>,
}

impl Default for Blob {
    fn default() -> Self {
        Blob {
            locations: HashSet::new(),
            repositories: HashSet::new(),
            content_type: None,
            size: None,
            dependencies: vec![],
        }
    }
}

struct Manifest {
    pub repositories: HashSet<RepositoryName>,
    pub locations: HashSet<String>,
    pub content_type: Option<String>,
    pub size: Option<usize>,
    pub dependencies: Vec<Digest>,
}

impl Default for Manifest {
    fn default() -> Self {
        Manifest {
            repositories: HashSet::new(),
            locations: HashSet::new(),
            content_type: None,
            size: None,
            dependencies: vec![],
        }
    }
}
pub struct Repositories {
    blobs: HashMap<Digest, Blob>,
    manifests: HashMap<Digest, Manifest>,
    uploads: HashMap<String, BlobUpload>,
    repositories: HashMap<RepositoryName, Repository>,
}

impl Default for Repositories {
    fn default() -> Self {
        Repositories {
            blobs: HashMap::new(),
            manifests: HashMap::new(),
            uploads: HashMap::new(),
            repositories: HashMap::new(),
        }
    }
}

impl Repositories {
    pub fn new() -> Self {
        Repositories {
            ..Default::default()
        }
    }

    pub fn apply(&mut self, action: RegistryAction) {
        match action {
            RegistryAction::BlobStored { digest, location } => match self.blobs.get_mut(&digest) {
                Some(item) => {
                    item.locations.insert(location);
                }
                None => {}
            },
            RegistryAction::BlobUnstored { digest, location } => {
                match self.blobs.get_mut(&digest) {
                    Some(item) => {
                        item.locations.remove(&location);
                    }
                    None => {}
                }
            }
            RegistryAction::BlobMounted { digest, repository } => match self.blobs.get_mut(&digest)
            {
                Some(item) => {
                    item.repositories.insert(repository);
                }
                None => {
                    let mut repositories = HashSet::new();
                    repositories.insert(repository);

                    let blob = Blob {
                        repositories,
                        ..Default::default()
                    };

                    self.blobs.insert(digest, blob);
                }
            },
            RegistryAction::BlobUnmounted { digest, repository } => {
                match self.blobs.get_mut(&digest) {
                    Some(item) => {
                        item.repositories.remove(&repository);
                    }
                    None => {}
                }
            }
            RegistryAction::BlobInfo {
                digest,
                dependencies,
                content_type,
            } => match self.blobs.get_mut(&digest) {
                Some(item) => {
                    item.content_type = Some(content_type);
                    item.dependencies.extend(dependencies);
                }
                None => {}
            },
            RegistryAction::BlobStat { digest, size } => match self.blobs.get_mut(&digest) {
                Some(item) => {
                    item.size = Some(size);
                }
                None => {}
            },
            RegistryAction::ManifestStored { digest, location } => {
                match self.manifests.get_mut(&digest) {
                    Some(item) => {
                        item.locations.insert(location);
                    }
                    None => {}
                }
            }
            RegistryAction::ManifestUnstored { digest, location } => {
                match self.manifests.get_mut(&digest) {
                    Some(item) => {
                        item.locations.remove(&location);
                    }
                    None => {}
                }
            }
            RegistryAction::ManifestMounted { digest, repository } => {
                match self.manifests.get_mut(&digest) {
                    Some(item) => {
                        item.repositories.insert(repository);
                    }
                    None => {
                        let mut repositories = HashSet::new();
                        repositories.insert(repository);

                        self.manifests.insert(
                            digest,
                            Manifest {
                                repositories,
                                ..Default::default()
                            },
                        );
                    }
                }
            }
            RegistryAction::ManifestUnmounted { digest, repository } => {
                match self.manifests.get_mut(&digest) {
                    Some(item) => {
                        item.repositories.remove(&repository);
                    }
                    None => {}
                }
            }
            RegistryAction::ManifestInfo {
                digest,
                dependencies,
                content_type,
            } => match self.manifests.get_mut(&digest) {
                Some(item) => {
                    item.content_type = Some(content_type);
                    item.dependencies.extend(dependencies);
                }
                None => {}
            },
            RegistryAction::ManifestStat { digest, size } => {
                match self.manifests.get_mut(&digest) {
                    Some(item) => {
                        item.size = Some(size);
                    }
                    None => {}
                }
            }
            RegistryAction::HashTagged {
                digest,
                repository,
                tag,
            } => {
                let repo = match self.repositories.get_mut(&repository) {
                    Some(repository) => repository,
                    None => {
                        let repo = Repository {
                            ..Default::default()
                        };
                        self.repositories.insert(repository.clone(), repo);
                        self.repositories.get_mut(&repository).unwrap()
                    }
                };
                repo.tags.insert(tag, digest);
            }
            RegistryAction::BlobUploadStarted {
                session_id,
                repository,
            } => {
                self.uploads.insert(
                    session_id,
                    BlobUpload {
                        repository,
                        ..Default::default()
                    },
                );
            }
            RegistryAction::BlobUploadPart {
                session_id,
                start: _,
                finish,
            } => {
                if let Some(upload) = self.uploads.get_mut(&session_id) {
                    upload.position = finish;
                }
            }
            RegistryAction::BlobUploadLocation {
                session_id: _,
                location: _,
            } => {}
            RegistryAction::BlobUploadDelete { session_id } => {
                if let Some(upload) = self.uploads.get_mut(&session_id) {
                    upload.finished = true;
                }
            }
            RegistryAction::BlobUploadFinish { session_id } => {
                if let Some(upload) = self.uploads.get_mut(&session_id) {
                    upload.finished = true;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn init() {
        let r = Repositories {
            ..Default::default()
        };

        assert_eq!(r.uploads.len(), 0);
    }
}
