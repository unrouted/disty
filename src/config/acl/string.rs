use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::leaf::MatchLeaf;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum StringMatch {
    Exact(String),
    Regex {
        #[serde(with = "serde_regex")]
        regex: Regex,
    },
}

impl MatchLeaf for StringMatch {
    fn matches(&self, value: &Value) -> bool {
        value
            .as_str()
            .map(|s| match self {
                StringMatch::Exact(exact) => exact == s,
                StringMatch::Regex { regex } => regex.is_match(s),
            })
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use indoc::indoc;
    use serde_json::json;

    /// Test StringMatch: regex pattern does not match
    #[test]
    fn test_string_match_regex_no_match() {
        let yaml = indoc! {r#"
            regex: "^abc$"
        "#};
        let matcher: StringMatch = serde_yaml::from_str(yaml).unwrap();
        assert!(!matcher.matches(&json!("def")));
    }
}
