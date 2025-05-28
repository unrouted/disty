use anyhow::{Context, Result};
use base64::engine::Engine;
use base64::engine::general_purpose::STANDARD;
use jwt_simple::prelude::*;
use reqwest::header::{CACHE_CONTROL, EXPIRES};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use thiserror::Error;
use tokio::sync::RwLock;
use tokio_retry::Retry;
use tokio_retry::strategy::{ExponentialBackoff, jitter};
use tracing::{error, info, trace};

#[derive(Error, Debug)]
pub enum JwksCacheError {
    #[error("No key found for kid: {0}")]
    KeyNotFound(String),

    #[error("Failed to decode base64url for modulus or exponent")]
    Base64DecodeError,
}

#[derive(Debug)]
struct JWKSCache {
    keys: Vec<Value>,
    expires_at: Instant,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct JWKSPublicKey {
    #[serde(skip, default = "default_inner")]
    inner: Arc<RwLock<JWKSCache>>,
    jwks_url: String,
    issuer: String,
    audience: String,
}

fn default_inner() -> Arc<RwLock<JWKSCache>> {
    Arc::new(RwLock::new(JWKSCache {
        keys: Vec::new(),
        expires_at: Instant::now(),
    }))
}

impl JWKSPublicKey {
    pub fn new(jwks_url: &str, issuer: &str, audience: &str) -> Self {
        Self {
            inner: Arc::new(RwLock::new(JWKSCache {
                keys: Vec::new(),
                expires_at: Instant::now(),
            })),
            jwks_url: jwks_url.to_string(),
            issuer: issuer.to_string(),
            audience: audience.to_string(),
        }
    }

    async fn fetch_jwks(&self) -> Result<(Vec<Value>, Instant)> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .connect_timeout(Duration::from_secs(5))
            .build()
            .context("Failed to build HTTP client")?;

        let retry_strategy = ExponentialBackoff::from_millis(300).map(jitter).take(3);

        let (body, headers) = Retry::spawn(retry_strategy, || async {
            let resp = client
                .get(&self.jwks_url)
                .send()
                .await
                .context("Request failed")?
                .error_for_status()
                .context("Non-success status")?;

            let headers = resp.headers().clone();
            let body = resp.text().await.context("Failed to read body")?;
            Ok::<_, anyhow::Error>((body, headers))
        })
        .await?;

        let jwks: Value = serde_json::from_str(&body).context("Failed to parse JWKS JSON")?;
        let keys = jwks["keys"]
            .as_array()
            .context("Invalid JWKS: 'keys' is not an array")?
            .clone();

        let expires_at = parse_cache_headers(&headers)
            .unwrap_or_else(|| Instant::now() + Duration::from_secs(600));

        Ok((keys, expires_at))
    }

    async fn refresh_if_needed(&self) -> Result<()> {
        {
            let inner = self.inner.read().await;
            if inner.expires_at > Instant::now() && !inner.keys.is_empty() {
                trace!("JWKS cache still fresh");
                return Ok(());
            }
        }

        let mut inner = self.inner.write().await;

        if inner.expires_at > Instant::now() && !inner.keys.is_empty() {
            trace!("JWKS cache was refreshed concurrently");
            return Ok(());
        }

        match self.fetch_jwks().await {
            Ok((keys, expires_at)) => {
                info!("JWKS refreshed, expires at {:?}", expires_at);
                inner.keys = keys;
                inner.expires_at = expires_at;
            }
            Err(e) => {
                error!("Failed to refresh JWKS: {:?}", e);
                let hard_expiry = Instant::now() + Duration::from_secs(60);
                if inner.expires_at < Instant::now() {
                    inner.expires_at = hard_expiry;
                }
            }
        }

        Ok(())
    }

    async fn get_key(&self, kid: &str) -> Result<RS256PublicKey> {
        self.refresh_if_needed().await?;

        let inner = self.inner.read().await;
        let key_obj = inner
            .keys
            .iter()
            .find(|k| k.get("kid").and_then(Value::as_str) == Some(kid))
            .ok_or_else(|| JwksCacheError::KeyNotFound(kid.to_string()))?;

        let n_b64 = key_obj
            .get("n")
            .and_then(Value::as_str)
            .context("Missing modulus 'n'")?;

        let e_b64 = key_obj
            .get("e")
            .and_then(Value::as_str)
            .context("Missing exponent 'e'")?;

        let n_bytes = STANDARD
            .decode(n_b64)
            .context("Failed to decode base64url modulus")?;

        let e_bytes = STANDARD
            .decode(e_b64)
            .context("Failed to decode base64url exponent")?;

        let key = RS256PublicKey::from_components(&n_bytes, &e_bytes)
            .context("Failed to build RS256PublicKey")?;

        Ok(key)
    }

    fn extract_kid_from_token(token: &str) -> Result<String> {
        let header_part = token.split('.').next().context("Missing JWT header")?;
        let decoded = STANDARD
            .decode(header_part)
            .context("Base64 decode failed")?;
        let header: Value =
            serde_json::from_slice(&decoded).context("Failed to parse JWT header")?;
        Ok(header["kid"]
            .as_str()
            .context("Missing 'kid' field in JWT header")?
            .to_string())
    }

    /// Verify token and return Ok(Some(claims)) if valid,
    /// Ok(None) if verification fails,
    /// Err(_) if a runtime error occurred (e.g. network error)
    pub async fn verify<C>(&self, token: &str) -> Result<Option<JWTClaims<C>>>
    where
        C: DeserializeOwned + Serialize,
    {
        let kid = Self::extract_kid_from_token(token)
            .context("Failed to extract 'kid' from JWT header")?;

        let key = self.get_key(&kid).await?;

        match key.verify_token::<C>(
            token,
            Some(VerificationOptions {
                allowed_issuers: Some([self.issuer.clone()].into_iter().collect()),
                allowed_audiences: Some([self.audience.clone()].into_iter().collect()),
                ..Default::default()
            }),
        ) {
            Ok(claims) => Ok(Some(claims)),
            Err(_e) => Ok(None),
        }
    }
}

fn parse_cache_headers(headers: &reqwest::header::HeaderMap) -> Option<Instant> {
    if let Some(cache_control) = headers.get(CACHE_CONTROL) {
        if let Ok(value) = cache_control.to_str() {
            for directive in value.split(',') {
                let directive = directive.trim();
                if let Some(stripped) = directive.strip_prefix("max-age=") {
                    if let Ok(seconds) = stripped.parse::<u64>() {
                        return Some(Instant::now() + Duration::from_secs(seconds));
                    }
                }
            }
        }
    }

    if let Some(expires) = headers.get(EXPIRES) {
        if let Ok(expires_str) = expires.to_str() {
            if let Ok(expire_time) = httpdate::parse_http_date(expires_str) {
                let dur = expire_time
                    .duration_since(std::time::SystemTime::now())
                    .ok()?;
                return Some(Instant::now() + dur);
            }
        }
    }

    None
}
