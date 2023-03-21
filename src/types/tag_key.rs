use serde::{Deserialize, Serialize};

use super::RepositoryName;

#[derive(Clone, Debug, Hash, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct TagKey {
    pub repository: RepositoryName,
    pub tag: String,
}
