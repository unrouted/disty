use std::{
    collections::{HashMap, HashSet},
    net::IpAddr,
};

use ip_network::IpNetwork;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use jsonpath_rust::JsonPathQuery;

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

/// Value matcher used in ClaimMatch
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", untagged)]
pub enum ValueMatch {
    Exact(String),
    Regex { #[serde(with = "serde_regex")] regex: Regex },
    Gt(String),
    Lt(String),
    Exists(bool),
    Not(Box<ValueMatch>),
    And { and: Vec<ValueMatch> },
    Or  { or: Vec<ValueMatch> },
}

impl ValueMatch {
    pub fn matches(&self, input: &str) -> bool {
        match self {
            ValueMatch::Exact(s) => s == input,
            ValueMatch::Regex { regex } => regex.is_match(input),
            ValueMatch::Gt(s) => input > s,
            ValueMatch::Lt(s) => input < s,
            ValueMatch::Exists(true) => true,
            ValueMatch::Exists(false) => false,
            ValueMatch::Not(inner) => !inner.matches(input),
            ValueMatch::And { and } => and.iter().all(|m| m.matches(input)),
            ValueMatch::Or { or } => or.iter().any(|m| m.matches(input)),
        }
    }
}

/// Single claim matcher, can target via pointer or jsonpath
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClaimMatch {
    pub pointer: Option<String>,
    pub path: Option<String>,
    pub any: Option<ValueMatch>,
    pub all: Option<ValueMatch>,
    #[serde(rename = "match")]
    pub match_: Option<ValueMatch>,
}

impl ClaimMatch {
    pub fn matches(&self, claims: &Value) -> bool {
        if let Some(ptr) = &self.pointer {
            if let Some(matcher) = &self.match_ {
                if let Some(target) = claims.pointer(ptr) {
                    if let Some(s) = Self::value_as_string(target) {
                        return matcher.matches(&s);
                    }
                }
            }
            return false;
        }

        if let Some(path) = &self.path {
            let mut finder = JsonPathQuery::from_str(claims, path);
            if let Ok(values) = finder.find() {
                if let Some(matcher) = &self.any {
                    return values.iter()
                        .filter_map(Self::value_as_string)
                        .any(|s| matcher.matches(&s));
                }
                if let Some(matcher) = &self.all {
                    let strs: Vec<String> = values.iter().filter_map(Self::value_as_string).collect();
                    return !strs.is_empty() && strs.iter().all(|s| matcher.matches(s));
                }
            }
            return false;
        }

        false
    }

    fn value_as_string(v: &Value) -> Option<String> {
        match v {
            Value::String(s) => Some(s.clone()),
            Value::Number(n) => Some(n.to_string()),
            _ => None,
        }
    }
}

/// Claims matcher that combines multiple ClaimMatch with logic
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", untagged)]
pub enum ClaimsMatch {
    Single(ClaimMatch),
    List(Vec<ClaimMatch>),
    And { and: Vec<ClaimsMatch> },
    Or  { or: Vec<ClaimsMatch> },
    Not { not: Box<ClaimsMatch> },
}

impl ClaimsMatch {
    pub fn matches(&self, claims: &Value) -> bool {
        match self {
            ClaimsMatch::Single(c) => c.matches(claims),
            ClaimsMatch::List(list) => list.iter().all(|c| c.matches(claims)),
            ClaimsMatch::And { and } => and.iter().all(|c| c.matches(claims)),
            ClaimsMatch::Or { or } => or.iter().any(|c| c.matches(claims)),
            ClaimsMatch::Not { not } => !not.matches(claims),
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
    use super::*;
    use serde_json::json;
    use serde_yaml;

    #[test]
    fn test_pointer_exact_match() {
        let yaml = r#"
pointer: /foo
match: "bar"
"#;
        let matcher: ClaimMatch = serde_yaml::from_str(yaml).unwrap();
        let claims = json!({"foo": "bar"});
        assert!(matcher.matches(&claims));
    }

    #[test]
    fn test_pointer_or_match() {
        let yaml = r#"
pointer: /foo
match:
  or:
    - "bar"
    - "baz"
"#;
        let matcher: ClaimMatch = serde_yaml::from_str(yaml).unwrap();
        let claims = json!({"foo": "baz"});
        assert!(matcher.matches(&claims));
    }

    #[test]
    fn test_pointer_not_match() {
        let yaml = r#"
pointer: /foo
match:
  not: "baz"
"#;
        let matcher: ClaimMatch = serde_yaml::from_str(yaml).unwrap();
        let claims = json!({"foo": "bar"});
        assert!(matcher.matches(&claims));
    }

    #[test]
    fn test_pointer_gt_lt_match() {
        let yaml = r#"
pointer: /foo
match:
  and:
    - gt: "apple"
    - lt: "dog"
"#;
        let matcher: ClaimMatch = serde_yaml::from_str(yaml).unwrap();
        let claims = json!({"foo": "banana"});
        assert!(matcher.matches(&claims));
    }

    #[test]
    fn test_path_any_regex() {
        let yaml = r#"
path: "$.items[*].name"
any:
  regex: "^dev.*"
"#;
        let matcher: ClaimMatch = serde_yaml::from_str(yaml).unwrap();
        let claims = json!({"items": [{"name": "prod"}, {"name": "dev-123"}]});
        assert!(matcher.matches(&claims));
    }

    #[test]
    fn test_path_all_exact() {
        let yaml = r#"
path: "$.items[*].env"
all: "prod"
"#;
        let matcher: ClaimMatch = serde_yaml::from_str(yaml).unwrap();
        let claims = json!({"items": [{"env": "prod"}, {"env": "prod"}]});
        assert!(matcher.matches(&claims));
    }

    #[test]
    fn test_claimsmatch_and_or() {
        let yaml = r#"
and:
  - single:
      pointer: /foo
      match: "bar"
  - or:
      - single:
          pointer: /baz
          match: "qux"
      - single:
          pointer: /baz
          match: "quux"
"#;
        let matcher: ClaimsMatch = serde_yaml::from_str(yaml).unwrap();
        let claims = json!({"foo": "bar", "baz": "quux"});
        assert!(matcher.matches(&claims));
    }

    #[test]
    fn test_value_match_exists_true() {
        let matcher = ValueMatch::Exists(true);
        assert!(matcher.matches("anything"));
    }

    #[test]
    fn test_value_match_exists_false() {
        let matcher = ValueMatch::Exists(false);
        assert!(!matcher.matches("anything"));
    }
}
