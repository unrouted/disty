use pyo3::prelude::*;

pub struct TokenConfig {
    pub enabled: bool,
    pub issuer: Option<String>,
    pub service: Option<String>,
    pub realm: Option<String>,
    pub public_key: Option<String>,
}

impl FromPyObject<'_> for TokenConfig {
    fn extract(dict: &'_ PyAny) -> PyResult<Self> {
        // FIXME: This should send nice errors back to python if any of the unwraps fail...
        let enabled: bool = dict.get_item("enabled").unwrap().extract().unwrap();
        if !enabled {
            return Ok(TokenConfig {
                enabled: false,
                issuer: None,
                service: None,
                realm: None,
                public_key: None,
            });
        }

        Ok(TokenConfig {
            enabled: true,
            issuer: Some(dict.get_item("issuer").unwrap().extract().unwrap()),
            service: Some(dict.get_item("service").unwrap().extract().unwrap()),
            realm: Some(dict.get_item("realm").unwrap().extract().unwrap()),
            public_key: Some(dict.get_item("public_key").unwrap().extract().unwrap()),
        })
    }
}
