use std::{
    collections::{HashMap, HashSet},
    net::IpAddr,
};

use ip_network::IpNetwork;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Represents an action that can be allowed.
#[derive(Debug, Hash, Eq, PartialEq, Clone, Deserialize, Serialize, PartialOrd, Ord)]
#[serde(rename_all = "lowercase")]
pub(crate) enum Action {
    Pull,
    Push,
}

impl TryFrom<String> for Action {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.as_str() {
            "push" => Ok(Action::Push),
            "pull" => Ok(Action::Pull),
            _ => Err(format!("Invalid variant: {}", value)),
        }
    }
}

impl std::fmt::Display for Action {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Push => write!(f, "push"),
            Self::Pull => write!(f, "pull"),
        }
    }
}

/// String matching strategies.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged, rename_all_fields = "camelCase")]
pub enum StringMatch {
    Exact(String),
    Regex {
        #[serde(with = "serde_regex")]
        regex: Regex,
    },
    OneOf {
        one_of: HashSet<String>,
    },
}

impl StringMatch {
    fn matches(&self, input: &str) -> bool {
        match self {
            StringMatch::Exact(s) => s == input,
            StringMatch::Regex { regex } => regex.is_match(input),
            StringMatch::OneOf { one_of } => one_of.contains(input),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ClaimsMatch {
    /// Match directly on a string (exact, regex, one_of)
    String(StringMatch),
    /// Match recursively on nested object keys
    Object(HashMap<String, ClaimsMatch>),
    /// Match if any element of an array matches
    Array(Vec<ClaimsMatch>),
}

impl ClaimsMatch {
    pub fn matches(&self, value: &Value) -> bool {
        match (self, value) {
            // direct string match
            (ClaimsMatch::String(matcher), Value::String(s)) => matcher.matches(s),

            // claim value is number, try matching as string
            (ClaimsMatch::String(matcher), Value::Number(n)) => matcher.matches(&n.to_string()),

            // claim value is array of strings, matcher is String
            (ClaimsMatch::String(matcher), Value::Array(arr)) => arr.iter().any(|v| {
                if let Value::String(s) = v {
                    matcher.matches(s)
                } else if let Value::Number(n) = v {
                    matcher.matches(&n.to_string())
                } else {
                    false
                }
            }),

            // matcher is Object, claim value must be Object
            (ClaimsMatch::Object(obj), Value::Object(map)) => obj
                .iter()
                .all(|(k, submatcher)| map.get(k).map_or(false, |v| submatcher.matches(v))),

            // matcher is Array of matchers; claim value is Array; match if any element matches any matcher
            (ClaimsMatch::Array(matchers), Value::Array(arr)) => {
                arr.iter().any(|v| matchers.iter().any(|m| m.matches(v)))
            }

            // matcher is Array; claim value is single value; match if any matcher matches the value
            (ClaimsMatch::Array(matchers), v) => matchers.iter().any(|m| m.matches(v)),

            _ => false,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SubjectContext {
    pub username: String,
    pub claims: Value,
    pub ip: IpAddr,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ResourceContext {
    pub repository: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, Default)]
pub struct SubjectMatch {
    pub network: Option<IpNetwork>,
    pub username: Option<StringMatch>,
    pub claims: Option<ClaimsMatch>,
}

impl SubjectMatch {
    fn matches(&self, ctx: &SubjectContext) -> bool {
        self.username
            .as_ref()
            .is_none_or(|m| m.matches(&ctx.username))
            && self.network.as_ref().is_none_or(|net| net.contains(ctx.ip))
            && self
                .claims
                .as_ref()
                .is_none_or(|matcher| matcher.matches(&ctx.claims))
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ResourceMatch {
    pub repository: Option<StringMatch>,
}

impl ResourceMatch {
    fn matches(&self, ctx: &ResourceContext) -> bool {
        self.repository
            .as_ref()
            .is_none_or(|m| m.matches(&ctx.repository))
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, Default)]
pub(crate) struct AccessRule {
    pub subject: Option<SubjectMatch>,
    pub resource: Option<ResourceMatch>,
    pub actions: HashSet<Action>,
    pub comment: Option<String>,
}

pub(crate) trait AclCheck {
    fn check_access(&self, subject: &SubjectContext, resource: &ResourceContext)
    -> HashSet<Action>;
}

impl AclCheck for [AccessRule] {
    fn check_access(
        &self,
        subject: &SubjectContext,
        resource: &ResourceContext,
    ) -> HashSet<Action> {
        let mut result = HashSet::new();

        for acl in self {
            if acl.subject.as_ref().is_none_or(|sub| sub.matches(subject))
                && acl
                    .resource
                    .as_ref()
                    .is_none_or(|repo| repo.matches(resource))
            {
                result.extend(acl.actions.iter().cloned());
            }
        }

        result
    }
}

/// Utility: trait to simplify Option checks
trait OptionExt<T> {
    fn is_none_or<F: FnOnce(&T) -> bool>(&self, f: F) -> bool;
}

impl<T> OptionExt<T> for Option<T> {
    fn is_none_or<F: FnOnce(&T) -> bool>(&self, f: F) -> bool {
        match self {
            None => true,
            Some(v) => f(v),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use super::*;
    use serde_json::json;
    use serde_yaml;

    fn mk_context(claims: Value) -> SubjectContext {
        SubjectContext {
            username: "test-user".into(),
            claims,
            ip: IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
        }
    }

    #[test]
    fn test_nested_object_match_yaml() {
        // matcher config written in YAML as you would in config file
        let yaml = r#"
kubernetes.io:
  pod:
    name: my-pod
"#;

        let matcher: ClaimsMatch = serde_yaml::from_str(yaml).unwrap();

        let claims = json!({
            "kubernetes.io": {
                "pod": { "name": "my-pod" }
            }
        });

        println!("matcher = {:#?}", matcher);
        println!("claims  = {:#?}", claims);

        assert!(matcher.matches(&claims));
    }

    #[test]
    fn test_array_of_strings_match_yaml() {
        let yaml = r#"
aud: bar
"#;
        let matcher: ClaimsMatch = serde_yaml::from_str(yaml).unwrap();

        let claims = json!({ "aud": ["foo", "bar", "baz"] });

        assert!(matcher.matches(&claims));
    }

    #[test]
    fn test_array_of_objects_match_yaml() {
        let yaml = r#"
groups:
  - name: ops
"#;

        let matcher: ClaimsMatch = serde_yaml::from_str(yaml).unwrap();

        let claims = json!({
            "groups": [
                { "name": "dev" },
                { "name": "ops" }
            ]
        });

        assert!(matcher.matches(&claims));
    }

    #[test]
    fn test_negative_case() {
        let yaml = r#"
aud:
  exact: not-present
"#;
        let matcher: ClaimsMatch = serde_yaml::from_str(yaml).unwrap();

        let claims = json!({ "aud": ["foo", "bar"] });

        assert!(!matcher.matches(&claims));
    }

    #[test]
    fn test_deserialize_yaml_with_regex_and_one_of() {
        let yaml = r#"
foo:
  regex: "^bar.*"
bar:
  oneOf: ["a", "b", "c"]
"#;
        let matcher: ClaimsMatch = serde_yaml::from_str(yaml).unwrap();

        let claims = json!({
            "foo": "barbaz",
            "bar": "b"
        });
        assert!(matcher.matches(&claims));

        let wrong_claims = json!({
            "foo": "no-match",
            "bar": "d"
        });
        assert!(!matcher.matches(&wrong_claims));
    }
}
