use super::digest::Digest;
use std::collections::HashMap;

#[derive(Default)]
pub struct Repository {
    pub tags: HashMap<String, Digest>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn init() {
        let r = Repository {
            ..Default::default()
        };
        assert_eq!(r.tags.len(), 0);
    }
}
