use std::{collections::HashSet, net::IpAddr};

use ip_network::IpNetwork;
use jsonpath_rust::JsonPath;
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
#[serde(untagged)]
pub enum StringMatch {
    Exact(String),
    Regex {
        #[serde(with = "serde_regex")]
        regex: Regex,
    },
}

impl StringMatch {
    fn matches(&self, input: &str) -> bool {
        match self {
            StringMatch::Exact(s) => s == input,
            StringMatch::Regex { regex } => regex.is_match(input),
        }
    }
}

/// Number matching strategies.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum NumberMatch {
    Exact(serde_json::Number),
    Gt { gt: serde_json::Number },
    Lt { lt: serde_json::Number },
}

impl NumberMatch {
    fn matches(&self, value: &Value) -> bool {
        let val = if let Some(n) = value.as_i64() {
            serde_json::Number::from(n)
        } else if let Some(n) = value.as_u64() {
            serde_json::Number::from(n)
        } else if let Some(f) = value.as_f64() {
            return match self {
                NumberMatch::Exact(num) => num
                    .as_f64()
                    .map(|v| (v - f).abs() < f64::EPSILON)
                    .unwrap_or(false),
                NumberMatch::Gt { gt } => gt.as_f64().map(|v| f > v).unwrap_or(false),
                NumberMatch::Lt { lt } => lt.as_f64().map(|v| f < v).unwrap_or(false),
            };
        } else {
            return false;
        };

        match self {
            NumberMatch::Exact(num) => num == &val,
            NumberMatch::Gt { gt } => {
                if let (Some(gt_i), Some(val_i)) = (gt.as_i64(), val.as_i64()) {
                    val_i > gt_i
                } else if let (Some(gt_u), Some(val_u)) = (gt.as_u64(), val.as_u64()) {
                    val_u > gt_u
                } else {
                    false
                }
            }
            NumberMatch::Lt { lt } => {
                if let (Some(lt_i), Some(val_i)) = (lt.as_i64(), val.as_i64()) {
                    val_i < lt_i
                } else if let (Some(lt_u), Some(val_u)) = (lt.as_u64(), val.as_u64()) {
                    val_u < lt_u
                } else {
                    false
                }
            }
        }
    }
}

/// IP matching strategies.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum IpMatch {
    Exact(IpAddr),
    Network(IpNetwork),
}

impl IpMatch {
    fn matches(&self, ip: &IpAddr) -> bool {
        match self {
            IpMatch::Exact(addr) => addr == ip,
            IpMatch::Network(net) => net.contains(*ip),
        }
    }
}

/// Value matcher: supports string, number, ip, and logical operators.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ValueMatch {
    Ip(IpMatch),
    String(StringMatch),
    Number(NumberMatch),
    Exists(bool),
    Not { not: Box<ValueMatch> },
    And { and: Vec<ValueMatch> },
    Or { or: Vec<ValueMatch> },
}

impl ValueMatch {
    pub fn matches(&self, value: &Value) -> bool {
        match self {
            ValueMatch::String(sm) => value.as_str().map(|s| sm.matches(s)).unwrap_or(false),
            ValueMatch::Number(nm) => nm.matches(value),
            ValueMatch::Ip(ipm) => value
                .as_str()
                .and_then(|s| s.parse::<IpAddr>().ok())
                .map(|ip| ipm.matches(&ip))
                .unwrap_or(false),
            ValueMatch::Exists(true) => !value.is_null(),
            ValueMatch::Exists(false) => value.is_null(),
            ValueMatch::Not { not } => !not.matches(value),
            ValueMatch::And { and } => and.iter().all(|m| m.matches(value)),
            ValueMatch::Or { or } => or.iter().any(|m| m.matches(value)),
        }
    }
}

/// Single claim matcher.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
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
                    return matcher.matches(target);
                }
            }
            return false;
        }

        if let Some(path) = &self.path {
            let values = claims.query(path).unwrap();
            if let Some(matcher) = &self.any {
                return values.iter().any(|s| matcher.matches(s));
            }
            if let Some(matcher) = &self.all {
                return values.iter().all(|s| matcher.matches(s));
            }
            return false;
        }

        false
    }
}

/// Claims matcher that combines multiple ClaimMatch.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", untagged)]
pub enum ClaimsMatch {
    And { and: Vec<ClaimsMatch> },
    Or { or: Vec<ClaimsMatch> },
    Not { not: Box<ClaimsMatch> },
    List(Vec<ClaimMatch>),
    Single(ClaimMatch),
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
    pub network: Option<ValueMatch>,
    pub username: Option<ValueMatch>,
    pub claims: Option<ClaimsMatch>,
}

impl SubjectMatch {
    fn matches(&self, ctx: &SubjectContext) -> bool {
        self.username
            .as_ref()
            .is_none_or(|m| m.matches(&Value::String(ctx.username.clone())))
            && self
                .network
                .as_ref()
                .is_none_or(|m| m.matches(&Value::String(ctx.ip.to_string())))
            && self
                .claims
                .as_ref()
                .is_none_or(|matcher| matcher.matches(&ctx.claims))
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ResourceMatch {
    pub repository: Option<ValueMatch>,
}

impl ResourceMatch {
    fn matches(&self, ctx: &ResourceContext) -> bool {
        self.repository
            .as_ref()
            .is_none_or(|m| m.matches(&Value::String(ctx.repository.clone())))
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

#[cfg(test)]
mod tests {
    use super::*;
    use indoc::indoc;
    use serde_json::json;

    #[test]
    fn test_pointer_exact_match() {
        let yaml = indoc! {r#"
            pointer: /foo
            match: "bar"
        "#};
        let matcher: ClaimMatch = serde_yaml::from_str(yaml).unwrap();
        let claims = json!({"foo": "bar"});
        assert!(matcher.matches(&claims));
    }

    #[test]
    fn test_pointer_or_match() {
        let yaml = indoc! {r#"
            pointer: /foo
            match:
              or:
                - "bar"
                - "baz"
        "#};
        let matcher: ClaimMatch = serde_yaml::from_str(yaml).unwrap();
        let claims = json!({"foo": "baz"});
        assert!(matcher.matches(&claims));
    }

    #[test]
    fn test_pointer_not_match() {
        let yaml = indoc! {r#"
            pointer: /foo
            match:
              not: "baz"
        "#};
        let matcher: ClaimMatch = serde_yaml::from_str(yaml).unwrap();
        let claims = json!({"foo": "bar"});
        assert!(matcher.matches(&claims));
    }

    #[test]
    fn test_pointer_gt_lt_match() {
        let yaml = indoc! {r#"
            pointer: /foo
            match:
              and:
                - gt: 0
                - lt: 10
        "#};
        let matcher: ClaimMatch = serde_yaml::from_str(yaml).unwrap();
        let claims = json!({"foo": 5});
        assert!(matcher.matches(&claims));
    }

    #[test]
    fn test_path_any_regex() {
        let yaml = indoc! {r#"
            path: "$.items[*].name"
            any:
              regex: "^dev.*"
        "#};
        let matcher: ClaimMatch = serde_yaml::from_str(yaml).unwrap();
        let claims = json!({"items": [{"name": "prod"}, {"name": "dev-123"}]});
        assert!(matcher.matches(&claims));
    }

    #[test]
    fn test_path_all_exact() {
        let yaml = indoc! {r#"
            path: "$.items[*].env"
            all: "prod"
        "#};
        let matcher: ClaimMatch = serde_yaml::from_str(yaml).unwrap();
        let claims = json!({"items": [{"env": "prod"}, {"env": "prod"}]});
        assert!(matcher.matches(&claims));
    }

    #[test]
    fn test_claimsmatch_and_or() {
        let yaml = indoc! {r#"
            and:
              - pointer: /foo
                match: "bar"
              - or:
                  - pointer: /baz
                    match: "qux"
                  - pointer: /baz
                    match: "quux"
        "#};
        let matcher: ClaimsMatch = serde_yaml::from_str(yaml).unwrap();
        let claims = json!({"foo": "bar", "baz": "quux"});
        assert!(matcher.matches(&claims));
    }

    #[test]
    fn test_value_match_exists_true() {
        let matcher = ValueMatch::Exists(true);
        assert!(matcher.matches(&json!("anything")));
    }

    #[test]
    fn test_value_match_exists_false() {
        let matcher = ValueMatch::Exists(false);
        assert!(!matcher.matches(&json!("anything")));
    }

    #[test]
    fn test_subject_match_network() {
        let yaml = indoc! {r#"
            network: "10.0.0.0/24"
        "#};
        let matcher: SubjectMatch = serde_yaml::from_str(yaml).unwrap();

        let ctx = SubjectContext {
            username: "alice".into(),
            claims: json!({}),
            ip: "10.0.0.42".parse().unwrap(),
        };
        assert!(matcher.matches(&ctx));
    }

    #[test]
    fn test_subject_match_network_or() {
        let yaml = indoc! {r#"
            network:
              or:
                - "192.168.1.0/24"
                - "10.0.0.42"
        "#};
        let matcher: SubjectMatch = serde_yaml::from_str(yaml).unwrap();

        let ctx = SubjectContext {
            username: "alice".into(),
            claims: json!({}),
            ip: "10.0.0.42".parse().unwrap(),
        };
        assert!(matcher.matches(&ctx));
    }
}
