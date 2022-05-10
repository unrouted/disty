use pyo3::FromPyObject;

use crate::types::RegistryAction;

#[derive(Debug, FromPyObject)]
pub struct ReducerDispatch(pub u64, pub RegistryAction);
