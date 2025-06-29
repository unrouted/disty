use std::{
    collections::{HashMap, HashSet},
    net::IpAddr,
};

use ip_network::IpNetwork;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::warn;

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

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SubjectContext {
    pub username: String,
    pub claims: HashMap<String, Value>,
    pub ip: IpAddr,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ResourceContext {
    pub repository: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, Default)]
pub struct SubjectMatch {
    /// IP address that token can be requested from
    pub network: Option<IpNetwork>,

    /// Username or token subject
    pub username: Option<StringMatch>,

    /// JWT specific matching rules
    pub claims: Option<HashMap<String, StringMatch>>,
}

impl SubjectMatch {
    fn matches(&self, ctx: &SubjectContext) -> bool {
        self.username
            .as_ref()
            .is_none_or(|m| m.matches(&ctx.username))
            && self.network.is_none_or(|net| net.contains(ctx.ip))
            && self.claims.as_ref().is_none_or(|required| {
                required.iter().all(|(k, matcher)| {
                    ctx.claims.get(k).is_some_and(|v| {
                        if let Value::String(v) = v {
                            return matcher.matches(v);
                        }
                        warn!("claim '{}' not a string so can't be validated yet", k);
                        false
                    })
                })
            })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ResourceMatch {
    /// Name of the registry
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
    fn check_access(
        &self,
        subject: &SubjectContext,
        repository: &ResourceContext,
    ) -> HashSet<Action>;
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
