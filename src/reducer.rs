use crate::types::RegistryAction;

#[derive(Debug)]
pub struct ReducerDispatch(pub u64, pub RegistryAction);
