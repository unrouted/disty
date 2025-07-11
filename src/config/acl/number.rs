use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::leaf::MatchLeaf;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum NumberMatch {
    Exact(serde_json::Number),
    Gt { gt: serde_json::Number },
    Lt { lt: serde_json::Number },
}

impl MatchLeaf for NumberMatch {
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

#[cfg(test)]
mod tests {
    use super::*;
    use indoc::indoc;
    use serde_json::json;
    /// Test NumberMatch: float vs integer and exact matching
    #[test]
    fn test_number_match_float_and_int() {
        let yaml = indoc! {r#"
            gt: 1.5
        "#};
        let matcher: NumberMatch = serde_yaml::from_str(yaml).unwrap();
        assert!(matcher.matches(&json!(2.0)));

        let yaml = indoc! {r#"
        5
    "#};
        let matcher: NumberMatch = serde_yaml::from_str(yaml).unwrap();
        assert!(matcher.matches(&json!(5)));
        assert!(!matcher.matches(&json!(4)));
    }
}
