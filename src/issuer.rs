use anyhow::Result;
use jwt_simple::prelude::*;

use crate::config::TokenConfig;
use crate::token::{Access, AdditionalClaims};

pub(crate) fn issue_token(config: &TokenConfig, access: Vec<Access>) -> Result<String> {
    // FIXME: Acquire the private key from TokenConfig
    let key = HS256Key::generate();

    let custom_claims = AdditionalClaims { access };
    let claims = Claims::with_custom_claims(custom_claims, Duration::from_mins(10))
        .with_issuer(&config.issuer)
        .with_audience(&config.service)
        .with_subject("$mirror");

    let token = key.authenticate(claims)?;

    println!("{}", token);

    Ok(token)
}
