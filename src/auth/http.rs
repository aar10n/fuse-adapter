//! HTTP-based token provider.
//!
//! This provider fetches tokens from an HTTP endpoint, typically an internal
//! service that manages OAuth tokens for users. It implements token caching
//! with automatic refresh before expiry.

use async_trait::async_trait;
use parking_lot::RwLock;
use serde::Deserialize;
use std::error::Error as StdError;
use std::time::Instant;

use super::TokenProviderInner;

/// Buffer time before token expiry to trigger refresh (60 seconds).
const EXPIRY_BUFFER_SECS: u64 = 60;

/// Response structure from the token endpoint.
#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    /// Token expiry in seconds from now
    expires_in: Option<u64>,
}

/// Cached token with expiry tracking.
struct CachedToken {
    token: String,
    fetched_at: Instant,
    expires_in_secs: u64,
}

impl CachedToken {
    /// Check if the token is still valid (with buffer).
    fn is_valid(&self) -> bool {
        let elapsed = self.fetched_at.elapsed().as_secs();
        let effective_expiry = self.expires_in_secs.saturating_sub(EXPIRY_BUFFER_SECS);
        elapsed < effective_expiry
    }
}

/// Configuration for the HTTP token provider.
#[derive(Debug, Clone)]
pub struct HttpTokenProviderConfig {
    /// The HTTP endpoint to fetch tokens from.
    pub endpoint: String,
    /// HTTP method (GET, POST, etc.).
    pub method: String,
    /// HTTP headers to send with the request.
    pub headers: std::collections::HashMap<String, String>,
}

/// A token provider that fetches tokens from an HTTP endpoint.
///
/// This provider is designed to work with internal token services that
/// manage OAuth tokens on behalf of users. It includes:
/// - Token caching to minimize API calls
/// - Automatic refresh 60 seconds before expiry
/// - Support for custom authentication headers
pub struct HttpTokenProvider {
    config: HttpTokenProviderConfig,
    client: reqwest::Client,
    cached_token: RwLock<Option<CachedToken>>,
}

impl HttpTokenProvider {
    /// Create a new HTTP token provider.
    pub fn new(config: HttpTokenProviderConfig) -> Self {
        Self {
            config,
            client: reqwest::Client::new(),
            cached_token: RwLock::new(None),
        }
    }

    /// Fetch a fresh token from the endpoint.
    async fn fetch_token(&self) -> Result<(String, u64), Box<dyn StdError + Send + Sync>> {
        let method = reqwest::Method::from_bytes(self.config.method.as_bytes())
            .map_err(|_| format!("Invalid HTTP method: {}", self.config.method))?;

        let mut request = self.client.request(method, &self.config.endpoint);

        for (key, value) in &self.config.headers {
            request = request.header(key, value);
        }

        let response = request.send().await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(format!("Token endpoint returned error {}: {}", status, body).into());
        }

        let token_response: TokenResponse = response.json().await?;

        // Default to 1 hour if expires_in not provided
        let expires_in = token_response.expires_in.unwrap_or(3600);

        Ok((token_response.access_token, expires_in))
    }
}

#[async_trait]
impl TokenProviderInner for HttpTokenProvider {
    async fn get_token(
        &self,
        _scopes: &[&str],
    ) -> Result<Option<String>, Box<dyn StdError + Send + Sync>> {
        // Check if we have a valid cached token
        {
            let cache = self.cached_token.read();
            if let Some(cached) = cache.as_ref() {
                if cached.is_valid() {
                    return Ok(Some(cached.token.clone()));
                }
            }
        }

        // Need to fetch a new token
        let (token, expires_in) = self.fetch_token().await?;

        // Cache the new token
        {
            let mut cache = self.cached_token.write();
            *cache = Some(CachedToken {
                token: token.clone(),
                fetched_at: Instant::now(),
                expires_in_secs: expires_in,
            });
        }

        Ok(Some(token))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_cached_token_validity() {
        let cached = CachedToken {
            token: "test".to_string(),
            fetched_at: Instant::now(),
            expires_in_secs: 3600,
        };
        assert!(cached.is_valid());
    }

    #[test]
    fn test_cached_token_with_buffer() {
        // Token that expires in exactly EXPIRY_BUFFER_SECS should be invalid
        let cached = CachedToken {
            token: "test".to_string(),
            fetched_at: Instant::now() - Duration::from_secs(3600 - EXPIRY_BUFFER_SECS),
            expires_in_secs: 3600,
        };
        assert!(!cached.is_valid());
    }

    #[test]
    fn test_expired_token_invalid() {
        let cached = CachedToken {
            token: "test".to_string(),
            fetched_at: Instant::now() - Duration::from_secs(4000),
            expires_in_secs: 3600,
        };
        assert!(!cached.is_valid());
    }
}
