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

mod base64url {
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    use serde::{self, Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let encoded = URL_SAFE_NO_PAD.encode(bytes);
        serializer.serialize_str(&encoded)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        URL_SAFE_NO_PAD
            .decode(s.as_bytes())
            .map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct JwkDocument {
    keys: Vec<Jwk>,
}

#[derive(Debug, Deserialize, Serialize)]
struct Jwk {
    kty: String,
    use_: String,
    kid: String,
    alg: String,
    #[serde(with = "base64url")]
    n: Vec<u8>,
    #[serde(with = "base64url")]
    e: Vec<u8>,
}

impl From<RS256PublicKey> for Jwk {
    fn from(value: RS256PublicKey) -> Self {
        Jwk {
            kty: "RSA".into(),
            use_: "sig".into(),
            kid: "bob".into(),
            alg: "RS256".into(),
            n: value.to_components().n,
            e: value.to_components().e,
        }
    }
}

impl TryInto<RS256PublicKey> for &Jwk {
    type Error = anyhow::Error;

    fn try_into(self) -> std::result::Result<RS256PublicKey, Self::Error> {
        Ok(RS256PublicKey::from_components(&self.n, &self.e)?.with_key_id(&self.kid))
    }
}

#[derive(Error, Debug)]
pub enum JwksCacheError {
    #[error("No key found for kid: {0}")]
    KeyNotFound(String),
}

#[derive(Debug)]
struct JWKSCache {
    keys: JwkDocument,
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
        keys: JwkDocument { keys: vec![] },
        expires_at: Instant::now(),
    }))
}

impl JWKSPublicKey {
    #[cfg(test)]
    pub fn new(jwks_url: &str, issuer: &str, audience: &str) -> Self {
        Self {
            inner: Arc::new(RwLock::new(JWKSCache {
                keys: JwkDocument { keys: vec![] },
                expires_at: Instant::now(),
            })),
            jwks_url: jwks_url.to_string(),
            issuer: issuer.to_string(),
            audience: audience.to_string(),
        }
    }

    #[cfg(test)]
    pub async fn with_cache(self, public_key: RS256PublicKey) -> Self {
        let mut guard = self.inner.write().await;
        guard.keys = JwkDocument {
            keys: vec![public_key.into()],
        };
        guard.expires_at = Instant::now() + Duration::from_secs(60 * 60);
        drop(guard);

        self
    }

    async fn fetch_jwks(&self) -> Result<(JwkDocument, Instant)> {
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

        let jwks: JwkDocument = serde_json::from_str(&body).context("Failed to parse JWKS JSON")?;

        let expires_at = parse_cache_headers(&headers)
            .unwrap_or_else(|| Instant::now() + Duration::from_secs(600));

        Ok((jwks, expires_at))
    }

    async fn refresh_if_needed(&self) -> Result<()> {
        {
            let inner = self.inner.read().await;
            if inner.expires_at > Instant::now() && !inner.keys.keys.is_empty() {
                trace!("JWKS cache still fresh");
                return Ok(());
            }
        }

        let mut inner = self.inner.write().await;
        if inner.expires_at > Instant::now() && !inner.keys.keys.is_empty() {
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
            .keys
            .iter()
            .find(|k| k.kid == kid)
            .ok_or_else(|| JwksCacheError::KeyNotFound(kid.to_string()))?;

        key_obj.try_into()
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
        error!("TOKEN: {token}");

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
