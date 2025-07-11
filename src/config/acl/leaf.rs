use serde_json::Value;

pub trait MatchLeaf {
    fn matches(&self, value: &Value) -> bool;
}
