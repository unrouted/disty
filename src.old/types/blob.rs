use chrono::{DateTime, Utc};
use std::collections::HashSet;

use super::{Digest, RepositoryName};

#[derive(Debug, Clone)]
pub struct Blob {
    pub size: Option<u64>,
    pub content_type: Option<String>,
    pub dependencies: Option<Vec<Digest>>,
    pub repositories: HashSet<RepositoryName>,
    pub locations: HashSet<String>,
    pub created: DateTime<Utc>,
    pub updated: DateTime<Utc>,
}
