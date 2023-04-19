use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

use crate::store::RegistryNodeId;

use super::{Digest, RepositoryName};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Blob {
    pub size: Option<u64>,
    pub content_type: Option<String>,
    pub dependencies: Option<Vec<Digest>>,
    pub repositories: HashSet<RepositoryName>,
    pub locations: HashSet<RegistryNodeId>,
    pub created: DateTime<Utc>,
    pub updated: DateTime<Utc>,
}
