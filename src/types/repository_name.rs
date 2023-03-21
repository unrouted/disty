use std::convert::TryFrom;
use std::fmt;

use std::str::FromStr;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Hash, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(try_from = "String", into = "String")]
pub struct RepositoryName {
    pub name: String,
}

impl FromStr for RepositoryName {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(RepositoryName {
            name: s.to_string(),
        })
    }
}

// We implement this so that serde_json can parse a RepositoryName from a straight string
impl TryFrom<String> for RepositoryName {
    type Error = &'static str;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Ok(RepositoryName { name: value })
    }
}

// We implement this so that serde_json can serialize a RepositoryName struct into a string
impl From<RepositoryName> for String {
    fn from(name: RepositoryName) -> Self {
        name.name
    }
}

impl fmt::Display for RepositoryName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_str() {
        let name: RepositoryName = "a/b/c".parse().unwrap();
        assert_eq!(name.name, "a/b/c");
    }

    #[test]
    fn to_str() {
        let name: RepositoryName = "a/b/c".parse().unwrap();
        assert_eq!(name.to_string(), "a/b/c");
    }

    #[test]
    fn from_json() {
        let data = r#"
        "a/b/c"
        "#;
        let parsed: RepositoryName = serde_json::from_str(data).unwrap();
        let name: RepositoryName = "a/b/c".parse().unwrap();
        assert_eq!(parsed, name);
    }

    #[test]
    fn to_json() {
        let data = r#""a/b/c""#;
        let name: RepositoryName = "a/b/c".parse().unwrap();
        let serialized = serde_json::to_string(&name).unwrap();

        assert_eq!(data, serialized);
    }

    #[test]
    fn equality() {
        let name1: RepositoryName = "a/b/c".parse().unwrap();
        let name2: RepositoryName = "c/b/a".parse().unwrap();

        assert_eq!(name1, name1.clone());
        assert_ne!(name1, name2);
    }
}
