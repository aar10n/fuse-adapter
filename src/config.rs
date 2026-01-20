//! Configuration parsing and structures

use std::path::PathBuf;

use serde::Deserialize;

use crate::cache::CacheConfig;
use crate::env::substitute_env_vars;

/// Error handling mode for connector failures during startup
#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ErrorMode {
    /// Log errors but continue running with remaining successful mounts
    #[default]
    Continue,
    /// Exit with error code on first connector failure
    Exit,
}

/// Status overlay configuration for virtual status directory
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct StatusOverlayConfig {
    /// Virtual directory name (default: ".fuse-adapter")
    pub prefix: String,
    /// Maximum number of error log entries to retain (default: 1000)
    pub max_log_entries: usize,
}

impl Default for StatusOverlayConfig {
    fn default() -> Self {
        Self {
            prefix: ".fuse-adapter".to_string(),
            max_log_entries: 1000,
        }
    }
}

// =============================================================================
// Raw Config (Deserialized from YAML)
// =============================================================================

/// Raw configuration as deserialized from YAML.
/// This is converted to `Config` via `resolve()`.
#[derive(Debug, Clone, Deserialize)]
pub struct RawConfig {
    /// Logging configuration
    #[serde(default)]
    pub logging: LoggingConfig,

    /// Error handling mode for connector failures
    #[serde(default)]
    pub error_mode: ErrorMode,

    /// Top-level connector defaults
    #[serde(default)]
    pub connectors: ConnectorDefaults,

    /// Mount points
    pub mounts: Vec<RawMountConfig>,
}

/// Top-level connector defaults section
#[derive(Debug, Clone, Deserialize, Default)]
pub struct ConnectorDefaults {
    /// S3 connector defaults
    pub s3: Option<S3ConnectorDefaults>,

    /// Google Drive connector defaults
    pub gdrive: Option<GDriveConnectorDefaults>,
}

/// S3 connector defaults (bucket is required)
#[derive(Debug, Clone, Deserialize)]
pub struct S3ConnectorDefaults {
    /// S3 bucket name
    pub bucket: String,

    /// AWS region
    pub region: Option<String>,

    /// Key prefix for mounting a subpath
    pub prefix: Option<String>,

    /// Custom endpoint URL (for S3-compatible stores)
    pub endpoint: Option<String>,

    /// Force path-style addressing (for MinIO, LocalStack, etc.)
    #[serde(default)]
    pub force_path_style: bool,

    /// Mount as read-only by default (disables all write operations)
    #[serde(default)]
    pub read_only: bool,

    /// Default cache configuration for S3 mounts
    pub cache: Option<CacheConfig>,
}

/// Google Drive connector defaults
#[derive(Debug, Clone, Deserialize)]
pub struct GDriveConnectorDefaults {
    /// Authentication configuration
    pub auth: Option<RawGDriveAuthConfig>,

    /// Root folder ID (defaults to "root" for My Drive)
    pub root_folder_id: Option<String>,

    /// Mount as read-only (disables all write operations)
    #[serde(default)]
    pub read_only: bool,

    /// Default cache configuration
    pub cache: Option<CacheConfig>,
}

/// Raw authentication configuration for Google Drive (deserialized from YAML).
/// Environment variable substitution is applied during resolution.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RawGDriveAuthConfig {
    /// Service account credentials file
    ServiceAccount {
        /// Path to the service account JSON credentials file
        credentials_path: String,
    },
    /// HTTP-based token provider with arbitrary headers
    Http {
        /// Token endpoint URL
        endpoint: String,
        /// HTTP method (GET, POST, etc.). Defaults to GET.
        method: Option<String>,
        /// HTTP headers to send with token requests (supports env var substitution)
        #[serde(default)]
        headers: std::collections::HashMap<String, String>,
    },
    /// Static access token (for testing)
    Token {
        /// The access token to use
        access_token: String,
    },
}

/// Raw mount configuration before resolution
#[derive(Debug, Clone, Deserialize)]
pub struct RawMountConfig {
    /// Path where the filesystem will be mounted
    pub path: PathBuf,

    /// Per-mount error mode (overrides global error_mode)
    pub error_mode: Option<ErrorMode>,

    /// Status overlay configuration (opt-in)
    pub status_overlay: Option<StatusOverlayConfig>,

    /// Connector configuration (may be partial, inheriting from defaults)
    pub connector: MountConnectorConfig,

    /// Cache configuration (overrides connector default)
    pub cache: Option<CacheConfig>,
}

/// Mount-level connector configuration (tagged enum)
/// All fields except `type` are optional - missing values inherit from top-level defaults
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum MountConnectorConfig {
    /// S3 connector
    S3(S3MountConnectorConfig),

    /// Google Drive connector
    #[serde(rename = "gdrive")]
    GDrive(GDriveMountConnectorConfig),
}

/// S3 mount connector - all fields optional for override mode
#[derive(Debug, Clone, Deserialize, Default)]
pub struct S3MountConnectorConfig {
    /// S3 bucket name (if present, full config mode; otherwise inherits from defaults)
    pub bucket: Option<String>,

    /// AWS region
    pub region: Option<String>,

    /// Key prefix for mounting a subpath
    pub prefix: Option<String>,

    /// Custom endpoint URL
    pub endpoint: Option<String>,

    /// Force path-style addressing
    pub force_path_style: Option<bool>,

    /// Mount as read-only (disables all write operations)
    pub read_only: Option<bool>,
}

/// Google Drive mount connector - all fields optional
#[derive(Debug, Clone, Deserialize, Default)]
pub struct GDriveMountConnectorConfig {
    /// Authentication configuration (overrides default if present)
    pub auth: Option<RawGDriveAuthConfig>,

    /// Root folder ID (defaults to "root" for My Drive)
    pub root_folder_id: Option<String>,

    /// Mount as read-only (disables all write operations)
    pub read_only: Option<bool>,
}

// =============================================================================
// Resolved Config (Ready for use)
// =============================================================================

/// Top-level configuration (resolved from RawConfig)
#[derive(Debug, Clone)]
pub struct Config {
    /// Logging configuration
    pub logging: LoggingConfig,

    /// Error handling mode for connector failures
    pub error_mode: ErrorMode,

    /// Mount points (fully resolved)
    pub mounts: Vec<MountConfig>,
}

/// Logging configuration
#[derive(Debug, Clone, Deserialize)]
pub struct LoggingConfig {
    /// Log level (trace, debug, info, warn, error)
    #[serde(default = "default_log_level")]
    pub level: String,
}

fn default_log_level() -> String {
    "info".to_string()
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
        }
    }
}

/// Mount point configuration (resolved)
#[derive(Debug, Clone)]
pub struct MountConfig {
    /// Path where the filesystem will be mounted
    pub path: PathBuf,

    /// Error mode for this mount (resolved from per-mount or global)
    pub error_mode: ErrorMode,

    /// Status overlay configuration (None if not enabled)
    pub status_overlay: Option<StatusOverlayConfig>,

    /// Connector configuration (fully resolved)
    pub connector: ConnectorConfig,

    /// Cache configuration (resolved from inheritance chain)
    pub cache: CacheConfig,
}

/// Connector configuration (tagged enum, fully resolved)
#[derive(Debug, Clone)]
pub enum ConnectorConfig {
    /// S3 connector
    S3(S3ConnectorConfig),

    /// Google Drive connector
    GDrive(GDriveConnectorConfig),
}

/// S3 connector configuration (fully resolved)
#[derive(Debug, Clone)]
pub struct S3ConnectorConfig {
    /// S3 bucket name
    pub bucket: String,

    /// AWS region
    pub region: Option<String>,

    /// Key prefix for mounting a subpath within the bucket.
    pub prefix: Option<String>,

    /// Custom endpoint URL (for S3-compatible stores)
    pub endpoint: Option<String>,

    /// Force path-style addressing (for MinIO, LocalStack, etc.)
    pub force_path_style: bool,

    /// Mount as read-only (disables all write operations)
    pub read_only: bool,
}

/// Google Drive connector configuration (fully resolved)
#[derive(Debug, Clone)]
pub struct GDriveConnectorConfig {
    /// Authentication configuration
    pub auth: GDriveAuthConfig,

    /// Root folder ID (defaults to "root" for My Drive)
    pub root_folder_id: String,

    /// Mount as read-only (disables all write operations)
    pub read_only: bool,
}

/// Resolved authentication configuration for Google Drive.
/// Environment variables have been substituted.
#[derive(Debug, Clone)]
pub enum GDriveAuthConfig {
    /// Service account credentials file
    ServiceAccount {
        /// Path to the service account JSON credentials file
        credentials_path: PathBuf,
    },
    /// HTTP-based token provider with arbitrary headers
    Http {
        /// Token endpoint URL
        endpoint: String,
        /// HTTP method (GET, POST, etc.)
        method: String,
        /// HTTP headers to send with token requests
        headers: std::collections::HashMap<String, String>,
    },
    /// Static access token (for testing)
    Token {
        /// The access token to use
        access_token: String,
    },
}

// =============================================================================
// Resolution Logic
// =============================================================================

impl RawConfig {
    /// Resolve raw config into final config by merging mount overrides with defaults
    pub fn resolve(self) -> Result<Config, ConfigError> {
        let RawConfig {
            logging,
            error_mode,
            connectors,
            mounts,
        } = self;

        let mut resolved_mounts = Vec::with_capacity(mounts.len());

        for raw_mount in mounts {
            let resolved = Self::resolve_mount(&connectors, raw_mount, error_mode)?;
            resolved_mounts.push(resolved);
        }

        Ok(Config {
            logging,
            error_mode,
            mounts: resolved_mounts,
        })
    }

    fn resolve_mount(
        connectors: &ConnectorDefaults,
        raw: RawMountConfig,
        global_error_mode: ErrorMode,
    ) -> Result<MountConfig, ConfigError> {
        // Resolve per-mount error_mode with inheritance from global
        let error_mode = raw.error_mode.unwrap_or(global_error_mode);
        // Pass through status_overlay as-is (already has defaults via serde)
        let status_overlay = raw.status_overlay;

        match raw.connector {
            MountConnectorConfig::S3(mount_s3) => {
                let resolved_connector =
                    Self::resolve_s3_connector(connectors, mount_s3, &raw.path)?;
                let cache = Self::resolve_s3_cache(connectors, &raw.cache);
                Ok(MountConfig {
                    path: raw.path,
                    error_mode,
                    status_overlay,
                    connector: ConnectorConfig::S3(resolved_connector),
                    cache,
                })
            }
            MountConnectorConfig::GDrive(mount_gdrive) => {
                let resolved_connector =
                    Self::resolve_gdrive_connector(connectors, mount_gdrive, &raw.path)?;
                let cache = Self::resolve_gdrive_cache(connectors, &raw.cache);
                Ok(MountConfig {
                    path: raw.path,
                    error_mode,
                    status_overlay,
                    connector: ConnectorConfig::GDrive(resolved_connector),
                    cache,
                })
            }
        }
    }

    fn resolve_s3_connector(
        connectors: &ConnectorDefaults,
        mount: S3MountConnectorConfig,
        mount_path: &PathBuf,
    ) -> Result<S3ConnectorConfig, ConfigError> {
        let defaults = connectors.s3.as_ref();

        // Mount values override defaults; bucket must be specified somewhere
        let bucket = mount
            .bucket
            .or_else(|| defaults.map(|d| d.bucket.clone()))
            .ok_or_else(|| {
                ConfigError::ValidationError(format!(
                    "Mount {:?} uses S3 connector but no bucket specified (either on mount or in connectors.s3 defaults)",
                    mount_path
                ))
            })?;

        // Apply environment variable substitution to string fields
        let bucket = substitute_env_vars(&bucket)?;
        let region = mount
            .region
            .or_else(|| defaults.and_then(|d| d.region.clone()))
            .map(|r| substitute_env_vars(&r))
            .transpose()?;
        let prefix = mount
            .prefix
            .or_else(|| defaults.and_then(|d| d.prefix.clone()))
            .map(|p| substitute_env_vars(&p))
            .transpose()?;
        let endpoint = mount
            .endpoint
            .or_else(|| defaults.and_then(|d| d.endpoint.clone()))
            .map(|e| substitute_env_vars(&e))
            .transpose()?;

        Ok(S3ConnectorConfig {
            bucket,
            region,
            prefix,
            endpoint,
            force_path_style: mount
                .force_path_style
                .or_else(|| defaults.map(|d| d.force_path_style))
                .unwrap_or(false),
            read_only: mount
                .read_only
                .or_else(|| defaults.map(|d| d.read_only))
                .unwrap_or(false),
        })
    }

    fn resolve_s3_cache(
        connectors: &ConnectorDefaults,
        mount_cache: &Option<CacheConfig>,
    ) -> CacheConfig {
        // Priority: mount cache > connector default cache > None
        if let Some(cache) = mount_cache {
            return cache.clone();
        }
        if let Some(s3_defaults) = &connectors.s3 {
            if let Some(cache) = &s3_defaults.cache {
                return cache.clone();
            }
        }
        CacheConfig::None
    }

    fn resolve_gdrive_connector(
        connectors: &ConnectorDefaults,
        mount: GDriveMountConnectorConfig,
        mount_path: &PathBuf,
    ) -> Result<GDriveConnectorConfig, ConfigError> {
        let defaults = connectors.gdrive.as_ref();

        // Mount auth overrides default auth; auth must be specified somewhere
        let raw_auth = mount
            .auth
            .or_else(|| defaults.and_then(|d| d.auth.clone()))
            .ok_or_else(|| {
                ConfigError::ValidationError(format!(
                    "Mount {:?} uses GDrive connector but no auth specified (either on mount or in connectors.gdrive defaults)",
                    mount_path
                ))
            })?;

        // Resolve auth with environment variable substitution
        let auth = Self::resolve_gdrive_auth(raw_auth)?;

        // root_folder_id defaults to "root" (My Drive)
        let root_folder_id = mount
            .root_folder_id
            .or_else(|| defaults.and_then(|d| d.root_folder_id.clone()))
            .unwrap_or_else(|| "root".to_string());

        let read_only = mount
            .read_only
            .or_else(|| defaults.map(|d| d.read_only))
            .unwrap_or(false);

        Ok(GDriveConnectorConfig {
            auth,
            root_folder_id,
            read_only,
        })
    }

    fn resolve_gdrive_auth(raw: RawGDriveAuthConfig) -> Result<GDriveAuthConfig, ConfigError> {
        match raw {
            RawGDriveAuthConfig::ServiceAccount { credentials_path } => {
                let resolved_path = substitute_env_vars(&credentials_path)?;
                Ok(GDriveAuthConfig::ServiceAccount {
                    credentials_path: PathBuf::from(resolved_path),
                })
            }
            RawGDriveAuthConfig::Http {
                endpoint,
                method,
                headers,
            } => {
                let endpoint = substitute_env_vars(&endpoint)?;
                let method = method.unwrap_or_else(|| "GET".to_string());

                // Substitute env vars in all header values
                let mut resolved_headers = std::collections::HashMap::new();
                for (key, value) in headers {
                    resolved_headers.insert(key, substitute_env_vars(&value)?);
                }

                Ok(GDriveAuthConfig::Http {
                    endpoint,
                    method,
                    headers: resolved_headers,
                })
            }
            RawGDriveAuthConfig::Token { access_token } => {
                let access_token = substitute_env_vars(&access_token)?;
                Ok(GDriveAuthConfig::Token { access_token })
            }
        }
    }

    fn resolve_gdrive_cache(
        connectors: &ConnectorDefaults,
        mount_cache: &Option<CacheConfig>,
    ) -> CacheConfig {
        if let Some(cache) = mount_cache {
            return cache.clone();
        }
        if let Some(gdrive_defaults) = &connectors.gdrive {
            if let Some(cache) = &gdrive_defaults.cache {
                return cache.clone();
            }
        }
        CacheConfig::None
    }
}

impl Config {
    /// Load configuration from a YAML file
    pub fn from_file(path: &PathBuf) -> Result<Self, ConfigError> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| ConfigError::ReadError(path.clone(), e.to_string()))?;

        Self::parse(&content)
    }

    /// Parse configuration from a YAML string
    pub fn parse(content: &str) -> Result<Self, ConfigError> {
        let raw: RawConfig =
            serde_yaml::from_str(content).map_err(|e| ConfigError::ParseError(e.to_string()))?;
        raw.resolve()
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.mounts.is_empty() {
            return Err(ConfigError::ValidationError(
                "At least one mount point is required".to_string(),
            ));
        }

        // Check for duplicate mount paths
        let mut paths = std::collections::HashSet::new();
        for mount in &self.mounts {
            if !paths.insert(&mount.path) {
                return Err(ConfigError::ValidationError(format!(
                    "Duplicate mount path: {:?}",
                    mount.path
                )));
            }
        }

        // Validate connector configs
        for mount in &self.mounts {
            match &mount.connector {
                ConnectorConfig::S3(s3) => {
                    if s3.bucket.is_empty() {
                        return Err(ConfigError::ValidationError(format!(
                            "Mount {:?}: S3 bucket cannot be empty",
                            mount.path
                        )));
                    }
                }
                ConnectorConfig::GDrive(_) => {
                    // No validation needed - root_folder_id defaults to "root"
                }
            }
        }

        Ok(())
    }
}

/// Configuration error types
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("Failed to read config file {0}: {1}")]
    ReadError(PathBuf, String),

    #[error("Failed to parse config: {0}")]
    ParseError(String),

    #[error("Configuration validation error: {0}")]
    ValidationError(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_config_backward_compat() {
        // Test that existing config format still works
        let yaml = r#"
logging:
  level: debug

mounts:
  - path: /mnt/data
    connector:
      type: s3
      bucket: my-bucket
      region: us-east-1
      prefix: "data/"
    cache:
      type: none
"#;

        let config = Config::parse(yaml).unwrap();
        assert_eq!(config.logging.level, "debug");
        assert_eq!(config.mounts.len(), 1);

        match &config.mounts[0].connector {
            ConnectorConfig::S3(s3) => {
                assert_eq!(s3.bucket, "my-bucket");
                assert_eq!(s3.region, Some("us-east-1".to_string()));
                assert_eq!(s3.prefix, Some("data/".to_string()));
            }
            _ => panic!("Expected S3 connector"),
        }
    }

    #[test]
    fn test_connector_defaults_with_overrides() {
        let yaml = r#"
connectors:
  s3:
    bucket: shared-bucket
    region: us-west-2
    endpoint: "http://localhost:4566"
    force_path_style: true
    cache:
      type: filesystem
      path: /tmp/cache/s3

mounts:
  - path: /mnt/exports
    connector:
      type: s3
      prefix: "exports/"
  - path: /mnt/imports
    connector:
      type: s3
      prefix: "imports/"
      region: us-east-1
    cache:
      type: memory
"#;

        let config = Config::parse(yaml).unwrap();
        assert_eq!(config.mounts.len(), 2);

        // First mount inherits everything from defaults
        match &config.mounts[0].connector {
            ConnectorConfig::S3(s3) => {
                assert_eq!(s3.bucket, "shared-bucket");
                assert_eq!(s3.region, Some("us-west-2".to_string()));
                assert_eq!(s3.endpoint, Some("http://localhost:4566".to_string()));
                assert!(s3.force_path_style);
                assert_eq!(s3.prefix, Some("exports/".to_string()));
            }
            _ => panic!("Expected S3 connector"),
        }
        // First mount inherits cache from connector defaults
        match &config.mounts[0].cache {
            CacheConfig::Filesystem { path, .. } => {
                assert_eq!(path, "/tmp/cache/s3");
            }
            _ => panic!("Expected filesystem cache"),
        }

        // Second mount overrides region and cache
        match &config.mounts[1].connector {
            ConnectorConfig::S3(s3) => {
                assert_eq!(s3.bucket, "shared-bucket");
                assert_eq!(s3.region, Some("us-east-1".to_string())); // overridden
                assert_eq!(s3.endpoint, Some("http://localhost:4566".to_string()));
                assert!(s3.force_path_style);
                assert_eq!(s3.prefix, Some("imports/".to_string()));
            }
            _ => panic!("Expected S3 connector"),
        }
        // Second mount overrides cache
        match &config.mounts[1].cache {
            CacheConfig::Memory { .. } => {}
            _ => panic!("Expected memory cache"),
        }
    }

    #[test]
    fn test_missing_defaults_error() {
        let yaml = r#"
mounts:
  - path: /mnt/data
    connector:
      type: s3
      prefix: "data/"
"#;

        let result = Config::parse(yaml);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("no bucket specified"),
            "Error should mention missing bucket: {}",
            err
        );
    }

    #[test]
    fn test_validate_empty_mounts() {
        let config = Config {
            logging: LoggingConfig::default(),
            error_mode: ErrorMode::default(),
            mounts: vec![],
        };

        assert!(config.validate().is_err());
    }

    #[test]
    fn test_mixed_full_and_override_configs() {
        // Test that some mounts can use full config while others use defaults
        let yaml = r#"
connectors:
  s3:
    bucket: default-bucket
    region: us-west-2

mounts:
  - path: /mnt/default
    connector:
      type: s3
      prefix: "default/"
  - path: /mnt/custom
    connector:
      type: s3
      bucket: custom-bucket
      region: eu-west-1
"#;

        let config = Config::parse(yaml).unwrap();
        assert_eq!(config.mounts.len(), 2);

        // First uses defaults
        match &config.mounts[0].connector {
            ConnectorConfig::S3(s3) => {
                assert_eq!(s3.bucket, "default-bucket");
                assert_eq!(s3.region, Some("us-west-2".to_string()));
            }
            _ => panic!("Expected S3 connector"),
        }

        // Second overrides bucket and region from defaults
        match &config.mounts[1].connector {
            ConnectorConfig::S3(s3) => {
                assert_eq!(s3.bucket, "custom-bucket");
                assert_eq!(s3.region, Some("eu-west-1".to_string()));
            }
            _ => panic!("Expected S3 connector"),
        }
    }

    #[test]
    fn test_different_bucket_inherits_other_settings() {
        // Test that specifying a different bucket still inherits endpoint, force_path_style, etc.
        let yaml = r#"
connectors:
  s3:
    bucket: default-bucket
    region: us-west-2
    endpoint: "http://minio:9000"
    force_path_style: true

mounts:
  - path: /mnt/other-bucket
    connector:
      type: s3
      bucket: other-bucket
"#;

        let config = Config::parse(yaml).unwrap();
        assert_eq!(config.mounts.len(), 1);

        match &config.mounts[0].connector {
            ConnectorConfig::S3(s3) => {
                assert_eq!(s3.bucket, "other-bucket"); // overridden
                assert_eq!(s3.region, Some("us-west-2".to_string())); // inherited
                assert_eq!(s3.endpoint, Some("http://minio:9000".to_string())); // inherited
                assert!(s3.force_path_style); // inherited
            }
            _ => panic!("Expected S3 connector"),
        }
    }

    #[test]
    fn test_read_only_mount() {
        // Test read_only can be set at mount level and inherits from defaults
        let yaml = r#"
connectors:
  s3:
    bucket: default-bucket
    read_only: true

mounts:
  - path: /mnt/readonly
    connector:
      type: s3
      prefix: "ro/"
  - path: /mnt/writable
    connector:
      type: s3
      prefix: "rw/"
      read_only: false
"#;

        let config = Config::parse(yaml).unwrap();
        assert_eq!(config.mounts.len(), 2);

        // First mount inherits read_only from defaults
        match &config.mounts[0].connector {
            ConnectorConfig::S3(s3) => {
                assert!(s3.read_only);
            }
            _ => panic!("Expected S3 connector"),
        }

        // Second mount overrides read_only to false
        match &config.mounts[1].connector {
            ConnectorConfig::S3(s3) => {
                assert!(!s3.read_only);
            }
            _ => panic!("Expected S3 connector"),
        }
    }

    #[test]
    fn test_gdrive_service_account_auth() {
        let yaml = r#"
mounts:
  - path: /mnt/gdrive
    connector:
      type: gdrive
      root_folder_id: "folder123"
      auth:
        type: service_account
        credentials_path: "/path/to/creds.json"
"#;

        let config = Config::parse(yaml).unwrap();
        assert_eq!(config.mounts.len(), 1);

        match &config.mounts[0].connector {
            ConnectorConfig::GDrive(gdrive) => {
                assert_eq!(gdrive.root_folder_id, "folder123");
                match &gdrive.auth {
                    GDriveAuthConfig::ServiceAccount { credentials_path } => {
                        assert_eq!(credentials_path, &PathBuf::from("/path/to/creds.json"));
                    }
                    _ => panic!("Expected ServiceAccount auth"),
                }
            }
            _ => panic!("Expected GDrive connector"),
        }
    }

    #[test]
    fn test_gdrive_http_auth() {
        let yaml = r#"
mounts:
  - path: /mnt/gdrive
    connector:
      type: gdrive
      root_folder_id: "root"
      auth:
        type: http
        endpoint: "https://api.example.com/token"
        method: POST
        headers:
          Authorization: "Bearer my-token"
          X-User-Id: "user-123"
"#;

        let config = Config::parse(yaml).unwrap();
        assert_eq!(config.mounts.len(), 1);

        match &config.mounts[0].connector {
            ConnectorConfig::GDrive(gdrive) => {
                assert_eq!(gdrive.root_folder_id, "root");
                match &gdrive.auth {
                    GDriveAuthConfig::Http {
                        endpoint,
                        method,
                        headers,
                    } => {
                        assert_eq!(endpoint, "https://api.example.com/token");
                        assert_eq!(method, "POST");
                        assert_eq!(
                            headers.get("Authorization"),
                            Some(&"Bearer my-token".to_string())
                        );
                        assert_eq!(headers.get("X-User-Id"), Some(&"user-123".to_string()));
                    }
                    _ => panic!("Expected Http auth"),
                }
            }
            _ => panic!("Expected GDrive connector"),
        }
    }

    #[test]
    fn test_gdrive_http_auth_default_method() {
        let yaml = r#"
mounts:
  - path: /mnt/gdrive
    connector:
      type: gdrive
      root_folder_id: "root"
      auth:
        type: http
        endpoint: "https://api.example.com/token"
"#;

        let config = Config::parse(yaml).unwrap();

        match &config.mounts[0].connector {
            ConnectorConfig::GDrive(gdrive) => match &gdrive.auth {
                GDriveAuthConfig::Http {
                    method, headers, ..
                } => {
                    assert_eq!(method, "GET");
                    assert!(headers.is_empty());
                }
                _ => panic!("Expected Http auth"),
            },
            _ => panic!("Expected GDrive connector"),
        }
    }

    #[test]
    fn test_gdrive_static_token_auth() {
        let yaml = r#"
mounts:
  - path: /mnt/gdrive
    connector:
      type: gdrive
      root_folder_id: "root"
      auth:
        type: token
        access_token: "ya29.test_token"
"#;

        let config = Config::parse(yaml).unwrap();
        assert_eq!(config.mounts.len(), 1);

        match &config.mounts[0].connector {
            ConnectorConfig::GDrive(gdrive) => match &gdrive.auth {
                GDriveAuthConfig::Token { access_token } => {
                    assert_eq!(access_token, "ya29.test_token");
                }
                _ => panic!("Expected Token auth"),
            },
            _ => panic!("Expected GDrive connector"),
        }
    }

    #[test]
    fn test_gdrive_auth_env_var_substitution() {
        use std::env;
        env::set_var("TEST_GDRIVE_SECRET", "secret_from_env");

        let yaml = r#"
mounts:
  - path: /mnt/gdrive
    connector:
      type: gdrive
      root_folder_id: "root"
      auth:
        type: http
        endpoint: "https://api.example.com/token"
        headers:
          Authorization: "Bearer ${TEST_GDRIVE_SECRET}"
"#;

        let config = Config::parse(yaml).unwrap();

        match &config.mounts[0].connector {
            ConnectorConfig::GDrive(gdrive) => match &gdrive.auth {
                GDriveAuthConfig::Http { headers, .. } => {
                    assert_eq!(
                        headers.get("Authorization"),
                        Some(&"Bearer secret_from_env".to_string())
                    );
                }
                _ => panic!("Expected Http auth"),
            },
            _ => panic!("Expected GDrive connector"),
        }

        env::remove_var("TEST_GDRIVE_SECRET");
    }

    #[test]
    fn test_gdrive_auth_inheritance_from_defaults() {
        let yaml = r#"
connectors:
  gdrive:
    root_folder_id: "default-folder"
    auth:
      type: service_account
      credentials_path: "/default/creds.json"

mounts:
  - path: /mnt/gdrive1
    connector:
      type: gdrive
  - path: /mnt/gdrive2
    connector:
      type: gdrive
      root_folder_id: "custom-folder"
      auth:
        type: token
        access_token: "custom-token"
"#;

        let config = Config::parse(yaml).unwrap();
        assert_eq!(config.mounts.len(), 2);

        // First mount inherits everything from defaults
        match &config.mounts[0].connector {
            ConnectorConfig::GDrive(gdrive) => {
                assert_eq!(gdrive.root_folder_id, "default-folder");
                match &gdrive.auth {
                    GDriveAuthConfig::ServiceAccount { credentials_path } => {
                        assert_eq!(credentials_path, &PathBuf::from("/default/creds.json"));
                    }
                    _ => panic!("Expected ServiceAccount auth"),
                }
            }
            _ => panic!("Expected GDrive connector"),
        }

        // Second mount overrides both root_folder_id and auth
        match &config.mounts[1].connector {
            ConnectorConfig::GDrive(gdrive) => {
                assert_eq!(gdrive.root_folder_id, "custom-folder");
                match &gdrive.auth {
                    GDriveAuthConfig::Token { access_token } => {
                        assert_eq!(access_token, "custom-token");
                    }
                    _ => panic!("Expected Token auth"),
                }
            }
            _ => panic!("Expected GDrive connector"),
        }
    }

    #[test]
    fn test_gdrive_missing_auth_error() {
        let yaml = r#"
mounts:
  - path: /mnt/gdrive
    connector:
      type: gdrive
      root_folder_id: "root"
"#;

        let result = Config::parse(yaml);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("no auth specified"),
            "Error should mention missing auth: {}",
            err
        );
    }

    #[test]
    fn test_error_mode_default() {
        let yaml = r#"
mounts:
  - path: /mnt/data
    connector:
      type: s3
      bucket: my-bucket
"#;

        let config = Config::parse(yaml).unwrap();
        assert_eq!(config.error_mode, ErrorMode::Continue);
    }

    #[test]
    fn test_error_mode_continue() {
        let yaml = r#"
error_mode: continue

mounts:
  - path: /mnt/data
    connector:
      type: s3
      bucket: my-bucket
"#;

        let config = Config::parse(yaml).unwrap();
        assert_eq!(config.error_mode, ErrorMode::Continue);
    }

    #[test]
    fn test_error_mode_exit() {
        let yaml = r#"
error_mode: exit

mounts:
  - path: /mnt/data
    connector:
      type: s3
      bucket: my-bucket
"#;

        let config = Config::parse(yaml).unwrap();
        assert_eq!(config.error_mode, ErrorMode::Exit);
    }

    #[test]
    fn test_per_mount_error_mode_override() {
        let yaml = r#"
error_mode: continue

mounts:
  - path: /mnt/critical
    error_mode: exit
    connector:
      type: s3
      bucket: production
  - path: /mnt/optional
    connector:
      type: s3
      bucket: cache
"#;

        let config = Config::parse(yaml).unwrap();
        assert_eq!(config.error_mode, ErrorMode::Continue); // Global default

        // First mount overrides to exit
        assert_eq!(config.mounts[0].error_mode, ErrorMode::Exit);
        // Second mount inherits global default
        assert_eq!(config.mounts[1].error_mode, ErrorMode::Continue);
    }

    #[test]
    fn test_status_overlay_with_defaults() {
        let yaml = r#"
mounts:
  - path: /mnt/data
    status_overlay: {}
    connector:
      type: s3
      bucket: my-bucket
"#;

        let config = Config::parse(yaml).unwrap();
        let overlay = config.mounts[0].status_overlay.as_ref().unwrap();
        assert_eq!(overlay.prefix, ".fuse-adapter");
        assert_eq!(overlay.max_log_entries, 1000);
    }

    #[test]
    fn test_status_overlay_with_custom_values() {
        let yaml = r#"
mounts:
  - path: /mnt/data
    status_overlay:
      prefix: ".status"
      max_log_entries: 500
    connector:
      type: s3
      bucket: my-bucket
"#;

        let config = Config::parse(yaml).unwrap();
        let overlay = config.mounts[0].status_overlay.as_ref().unwrap();
        assert_eq!(overlay.prefix, ".status");
        assert_eq!(overlay.max_log_entries, 500);
    }

    #[test]
    fn test_status_overlay_not_present() {
        let yaml = r#"
mounts:
  - path: /mnt/data
    connector:
      type: s3
      bucket: my-bucket
"#;

        let config = Config::parse(yaml).unwrap();
        assert!(config.mounts[0].status_overlay.is_none());
    }

    #[test]
    fn test_combined_per_mount_error_mode_and_status_overlay() {
        let yaml = r#"
error_mode: exit

mounts:
  - path: /mnt/critical
    connector:
      type: s3
      bucket: production
  - path: /mnt/optional
    error_mode: continue
    status_overlay:
      prefix: ".health"
    connector:
      type: s3
      bucket: cache
"#;

        let config = Config::parse(yaml).unwrap();

        // First mount inherits global exit mode, no overlay
        assert_eq!(config.mounts[0].error_mode, ErrorMode::Exit);
        assert!(config.mounts[0].status_overlay.is_none());

        // Second mount overrides to continue with custom overlay
        assert_eq!(config.mounts[1].error_mode, ErrorMode::Continue);
        let overlay = config.mounts[1].status_overlay.as_ref().unwrap();
        assert_eq!(overlay.prefix, ".health");
    }
}
