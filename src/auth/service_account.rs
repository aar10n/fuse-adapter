//! Service account token provider.
//!
//! This provider wraps yup_oauth2's ServiceAccountAuthenticator to provide
//! tokens using Google Cloud service account credentials.

use async_trait::async_trait;
use google_drive3::yup_oauth2::{read_service_account_key, ServiceAccountAuthenticator};
use hyper_util::client::legacy::connect::HttpConnector;
use std::error::Error as StdError;
use std::path::Path;

use super::TokenProviderInner;

type Authenticator = google_drive3::yup_oauth2::authenticator::Authenticator<
    hyper_rustls::HttpsConnector<HttpConnector>,
>;

/// A token provider using Google Cloud service account credentials.
///
/// This wraps the yup_oauth2 ServiceAccountAuthenticator and handles
/// token refresh automatically.
pub struct ServiceAccountProvider {
    auth: Authenticator,
}

impl ServiceAccountProvider {
    /// Create a new service account provider from a credentials file.
    ///
    /// The credentials file should be a JSON file downloaded from the
    /// Google Cloud Console containing service account keys.
    pub async fn from_file(
        credentials_path: &Path,
    ) -> Result<Self, Box<dyn StdError + Send + Sync>> {
        let creds = read_service_account_key(credentials_path).await?;

        let auth = ServiceAccountAuthenticator::builder(creds).build().await?;

        Ok(Self { auth })
    }
}

#[async_trait]
impl TokenProviderInner for ServiceAccountProvider {
    async fn get_token(
        &self,
        scopes: &[&str],
    ) -> Result<Option<String>, Box<dyn StdError + Send + Sync>> {
        let token = self.auth.token(scopes).await?;
        Ok(token.token().map(|t| t.to_string()))
    }
}
