use std::{collections::{HashMap, HashSet}, net::IpAddr};

use ip_network::IpNetwork;
use regex::Regex;
use serde::{Deserialize, Serialize};


#[derive(Debug, Hash, Eq, PartialEq, Clone, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
enum Action {
    Push,
    Pull,
}


#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
enum StringMatch {
    Exact(String),
    Regex {
        #[serde(with = "serde_regex")]
        regex: Regex
    },
}

impl StringMatch {
    fn matches(&self, input: &str) -> bool {
        match self {
            StringMatch::Exact(s) => s == input,
            StringMatch::Regex { regex } => {
                regex.is_match(input)
            }
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RequestContext {
    pub username: String,
    pub claims: HashMap<String, String>,
    pub ip: IpAddr,
    pub repository: String,    
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MatchRule {
    /// Name of the registry
    pub repository: Option<StringMatch>,

    /// IP address that token can be requested from
    pub network: Option<IpNetwork>,

    /// Username or token subject
    pub username: Option<StringMatch>,
 
    /// JWT specific matching rules
    pub claims: Option<HashMap<String, StringMatch>>,
}

impl MatchRule {
    fn matches(&self, ctx: &RequestContext) -> bool {
        self.username.as_ref().map_or(true, |m| m.matches(&ctx.username)) &&
        self.network.map_or(true, |net| net.contains(ctx.ip)) &&
        self.repository.as_ref().map_or(true, |m| m.matches(&ctx.repository)) &&
        self.claims.as_ref().map_or(true, |required| {
            required.iter().all(|(k, matcher)| {
                ctx.claims.get(k).map_or(false, |v| matcher.matches(v))
            })
        })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AccessRule {
    pub check: MatchRule,
    pub actions: HashSet<Action>,
    pub comment: String,
}

trait AclCheck {
    fn check_access(&self, ctx: &RequestContext) -> HashSet<Action>;
}

impl AclCheck for [AccessRule] {
    fn check_access(&self, ctx: &RequestContext) -> HashSet<Action> {
        let mut result = HashSet::new();

        for acl in self {
            if acl.check.matches(ctx) {
                result.extend(acl.actions.iter().cloned());
            }
        }

        result
    }
}