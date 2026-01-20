//! Static token provider for testing and simple use cases.
//!
//! This provider returns a fixed token string without any validation
//! or refresh logic. Useful for testing or when you have a long-lived
//! token from another source.

use async_trait::async_trait;
use std::error::Error as StdError;

use super::TokenProviderInner;

/// A token provider that returns a static token.
///
/// This is useful for testing or when using a pre-obtained access token.
/// Note that static tokens will eventually expire and this provider
/// does not handle refresh.
pub struct StaticTokenProvider {
    token: String,
}

impl StaticTokenProvider {
    /// Create a new static token provider.
    pub fn new(token: String) -> Self {
        Self { token }
    }
}

#[async_trait]
impl TokenProviderInner for StaticTokenProvider {
    async fn get_token(
        &self,
        _scopes: &[&str],
    ) -> Result<Option<String>, Box<dyn StdError + Send + Sync>> {
        Ok(Some(self.token.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_static_token_provider() {
        let provider = StaticTokenProvider::new("test_token_123".to_string());
        let token = provider.get_token(&["scope1", "scope2"]).await.unwrap();
        assert_eq!(token, Some("test_token_123".to_string()));
    }

    #[tokio::test]
    async fn test_static_token_ignores_scopes() {
        let provider = StaticTokenProvider::new("my_token".to_string());

        // Token should be the same regardless of scopes
        let token1 = provider.get_token(&["scope1"]).await.unwrap();
        let token2 = provider.get_token(&["scope2", "scope3"]).await.unwrap();
        let token3 = provider.get_token(&[]).await.unwrap();

        assert_eq!(token1, token2);
        assert_eq!(token2, token3);
    }
}
