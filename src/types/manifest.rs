use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

use super::{Digest, RepositoryName};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub size: Option<u64>,
    pub content_type: Option<String>,
    pub dependencies: Option<Vec<Digest>>,
    pub repositories: HashSet<RepositoryName>,
    pub locations: HashSet<String>,
    pub created: DateTime<Utc>,
    pub updated: DateTime<Utc>,
}
