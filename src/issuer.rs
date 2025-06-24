use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use jwt_simple::prelude::*;

use crate::config::Configuration;
use crate::context::{Access, AdditionalClaims};

pub(crate) struct Token {
    pub token: String,
    pub expires_in: std::time::Duration,
    pub issued_at: DateTime<Utc>,
}

fn to_system_time(timestamp: UnixTimeStamp) -> SystemTime {
    UNIX_EPOCH + std::time::Duration::from_secs(timestamp.as_secs())
}

fn duration_until(unix_timestamp: UnixTimeStamp) -> Result<std::time::Duration> {
    let now = SystemTime::now();
    to_system_time(unix_timestamp)
        .duration_since(now)
        .context("Time is in the past")
}

pub(crate) fn issue_token(
    config: &Configuration,
    subject: &str,
    access: Vec<Access>,
) -> Result<Token> {
    let Some(authentication) = config.authentication.as_ref() else {
        bail!("Tried to issue token when auth is not turned on");
    };
    let custom_claims = AdditionalClaims { access };
    let claims = Claims::with_custom_claims(custom_claims, Duration::from_mins(10))
        .with_issuer(&config.url)
        .with_audience(&config.url)
        .with_subject(subject);

    let expires_in = duration_until(claims.expires_at.context("Failed to set expiry")?)?;
    let issued_at = to_system_time(
        claims
            .issued_at
            .context("Failed to get issuance timestamp")?,
    )
    .into();

    let token = authentication.key_pair.key_pair.sign(claims)?;

    Ok(Token {
        token,
        expires_in,
        issued_at,
    })
}
