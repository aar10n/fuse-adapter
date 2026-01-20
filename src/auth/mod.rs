//! Token provider abstractions for Google Drive authentication
//!
//! This module provides a flexible authentication system supporting multiple
//! token sources:
//! - Service account credentials (existing behavior)
//! - HTTP-based token providers (for dynamic token fetching)
//! - Static tokens (for testing)

pub mod http;
pub mod service_account;
pub mod static_token;

use async_trait::async_trait;
use google_apis_common::GetToken;
use std::error::Error as StdError;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

pub use http::HttpTokenProvider;
pub use service_account::ServiceAccountProvider;
pub use static_token::StaticTokenProvider;

/// Inner trait for token providers.
///
/// This trait defines the interface that all token providers must implement.
/// It's designed to be object-safe and async-compatible.
#[async_trait]
pub trait TokenProviderInner: Send + Sync {
    /// Get a token for the given scopes.
    ///
    /// Returns `Ok(Some(token))` if a token is available,
    /// `Ok(None)` if no token is needed/available,
    /// or `Err` if token fetching failed.
    async fn get_token(
        &self,
        scopes: &[&str],
    ) -> Result<Option<String>, Box<dyn StdError + Send + Sync>>;
}

/// Wrapper that implements google-apis-common's GetToken trait.
///
/// This wrapper allows our TokenProviderInner implementations to be used
/// with the Google Drive API client.
#[derive(Clone)]
pub struct TokenProviderWrapper {
    inner: Arc<dyn TokenProviderInner>,
}

impl TokenProviderWrapper {
    /// Create a new wrapper around a token provider.
    pub fn new<T: TokenProviderInner + 'static>(provider: T) -> Self {
        Self {
            inner: Arc::new(provider),
        }
    }
}

impl GetToken for TokenProviderWrapper {
    fn get_token<'a>(
        &'a self,
        scopes: &'a [&str],
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<String>, Box<dyn StdError + Send + Sync>>>
                + Send
                + 'a,
        >,
    > {
        let inner = self.inner.clone();
        Box::pin(async move { inner.get_token(scopes).await })
    }
}
