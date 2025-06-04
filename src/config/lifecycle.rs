use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) enum StringMatch {
    Exact(String),
    Starts { starts: String },
    Ends { ends: String },
}

fn escape_glob(input: &str) -> String {
    let mut out = String::new();
    for c in input.chars() {
        match c {
            '*' => out.push_str("[*]"),
            '?' => out.push_str("[?]"),
            '[' => out.push_str("[[]"),
            ']' => out.push_str("[]]"),
            _ => out.push(c),
        }
    }
    out
}

impl StringMatch {
    pub fn matches(&self, value: &str) -> bool {
        match self {
            Self::Exact(exact) => value == exact,
            Self::Starts { starts } => value.starts_with(starts),
            Self::Ends { ends } => value.ends_with(ends),
        }
    }

    pub fn to_sqlite_glob(&self) -> String {
        match self {
            StringMatch::Exact(s) => escape_glob(s),
            StringMatch::Starts { starts } => format!("{}*", escape_glob(starts)),
            StringMatch::Ends { ends } => format!("*{}", escape_glob(ends)),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type",  rename_all = "lowercase")]
pub(crate) enum DeletionRule {
    Tag {
        repository: Option<StringMatch>,
        tag: Option<StringMatch>,
        older_than: u32,
    },
}
