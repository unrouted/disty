use std::convert::TryFrom;
use std::fmt;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use data_encoding::HEXLOWER;
use ring::digest;
use rocket::form::{FromFormField, ValueField};
use rocket::request::FromParam;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Hash, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(try_from = "String", into = "String")]
pub struct Digest {
    pub algo: String,
    pub hash: String,
}

impl Digest {
    pub fn from_sha256(digest: &digest::Digest) -> Digest {
        Digest {
            algo: "sha256".parse().unwrap(),
            hash: HEXLOWER.encode(digest.as_ref()),
        }
    }

    pub fn to_path(&self) -> PathBuf {
        Path::new(&self.hash[0..2])
            .join(&self.hash[2..4])
            .join(&self.hash[4..6])
            .join(&self.hash[6..])
    }
}

impl<'r> FromParam<'r> for Digest {
    type Error = &'r str;

    fn from_param(param: &'r str) -> Result<Self, Self::Error> {
        if !param.starts_with("sha256:") {
            return Err(param);
        }

        match param.to_string().split_once(':') {
            Some((algo, digest)) => Ok(Digest {
                algo: algo.to_string(),
                hash: digest.to_string(),
            }),
            _ => Err(param),
        }
    }
}

impl<'v> FromFormField<'v> for Digest {
    fn from_value(field: ValueField<'v>) -> rocket::form::Result<'v, Self> {
        match field.value.parse() {
            Ok(value) => Ok(value),
            _ => std::result::Result::Err(
                rocket::form::Error::validation("Invalid digest value").into(),
            ),
        }
    }
}

impl FromStr for Digest {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if !s.starts_with("sha256:") {
            return Err(());
        }

        let value = String::from_str(s).unwrap();
        let mut split = value.split(':');

        Ok(Digest {
            algo: String::from_str(split.next().unwrap()).unwrap(),
            hash: String::from_str(split.next().unwrap()).unwrap(),
        })
    }
}

// We implement this so that serde_json can parse a Digest from a straight string
impl TryFrom<String> for Digest {
    type Error = &'static str;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if !value.starts_with("sha256:") {
            return Err("Not a sha256");
        }

        let mut split = value.split(':');

        Ok(Digest {
            algo: String::from_str(split.next().unwrap()).unwrap(),
            hash: String::from_str(split.next().unwrap()).unwrap(),
        })
    }
}

// We implement this so that serde_json can serialize a Digest struct into a string
impl From<Digest> for String {
    fn from(digest: Digest) -> Self {
        format!("{}", digest)
    }
}

impl fmt::Display for Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.algo, self.hash)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_str() {
        let digest: Digest = "sha256:abcdef0123456789".parse().unwrap();

        assert_eq!(digest.algo, "sha256");
        assert_eq!(digest.hash, "abcdef0123456789");
    }

    #[test]
    fn to_str() {
        let digest: Digest = "sha256:abcdef0123456789".parse().unwrap();

        assert_eq!(digest.to_string(), "sha256:abcdef0123456789");
    }

    #[test]
    fn from_json() {
        let data = r#"
        "sha256:abcdef0123456789"
        "#;
        let parsed: Digest = serde_json::from_str(data).unwrap();
        let digest: Digest = "sha256:abcdef0123456789".parse().unwrap();

        assert_eq!(parsed.algo, digest.algo);
        assert_eq!(parsed.hash, digest.hash);
    }

    #[test]
    fn to_json() {
        let data = r#""sha256:abcdef0123456789""#;
        let digest: Digest = "sha256:abcdef0123456789".parse().unwrap();
        let serialized = serde_json::to_string(&digest).unwrap();

        assert_eq!(data, serialized);
    }

    #[test]
    fn from_sha256() {
        let one_shot = digest::digest(&digest::SHA256, b"hello, world");
        let digest = Digest::from_sha256(&one_shot);

        assert_eq!(digest.algo, "sha256");
        assert_eq!(
            digest.hash,
            "09ca7e4eaa6e8ae9c7d261167129184883644d07dfba7cbfbc4c8a2e08360d5b"
        );

        let one_shot = digest::digest(&digest::SHA256, b"");
        let digest = Digest::from_sha256(&one_shot);

        assert_eq!(digest.algo, "sha256");
        assert_eq!(
            digest.hash,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn equality() {
        let digest1: Digest = "sha256:abcdef0123456789".parse().unwrap();
        let digest2: Digest = "sha256:9876543210fedcba".parse().unwrap();

        assert_eq!(digest1, digest1);
        assert_ne!(digest1, digest2);
    }

    #[test]
    fn to_path() {
        let digest: Digest = "sha256:abcdef0123456789".parse().unwrap();
        let path = digest.to_path();

        assert_eq!(path, PathBuf::from("ab/cd/ef/0123456789"))
    }
}
