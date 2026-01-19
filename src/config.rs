//! Configuration parsing and structures

use std::path::PathBuf;

use serde::Deserialize;

use crate::cache::CacheConfig;

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
    /// Path to credentials JSON file
    pub credentials_path: PathBuf,

    /// Root folder ID in Google Drive
    pub root_folder_id: String,

    /// Default cache configuration
    pub cache: Option<CacheConfig>,
}

/// Raw mount configuration before resolution
#[derive(Debug, Clone, Deserialize)]
pub struct RawMountConfig {
    /// Path where the filesystem will be mounted
    pub path: PathBuf,

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
    /// Path to credentials JSON file
    pub credentials_path: Option<PathBuf>,

    /// Root folder ID in Google Drive
    pub root_folder_id: Option<String>,
}

// =============================================================================
// Resolved Config (Ready for use)
// =============================================================================

/// Top-level configuration (resolved from RawConfig)
#[derive(Debug, Clone)]
pub struct Config {
    /// Logging configuration
    pub logging: LoggingConfig,

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
    /// Path to credentials JSON file
    pub credentials_path: PathBuf,

    /// Root folder ID in Google Drive
    pub root_folder_id: String,
}

// =============================================================================
// Resolution Logic
// =============================================================================

impl RawConfig {
    /// Resolve raw config into final config by merging mount overrides with defaults
    pub fn resolve(self) -> Result<Config, ConfigError> {
        let RawConfig {
            logging,
            connectors,
            mounts,
        } = self;

        let mut resolved_mounts = Vec::with_capacity(mounts.len());

        for raw_mount in mounts {
            let resolved = Self::resolve_mount(&connectors, raw_mount)?;
            resolved_mounts.push(resolved);
        }

        Ok(Config {
            logging,
            mounts: resolved_mounts,
        })
    }

    fn resolve_mount(
        connectors: &ConnectorDefaults,
        raw: RawMountConfig,
    ) -> Result<MountConfig, ConfigError> {
        match raw.connector {
            MountConnectorConfig::S3(mount_s3) => {
                let resolved_connector =
                    Self::resolve_s3_connector(connectors, mount_s3, &raw.path)?;
                let cache = Self::resolve_s3_cache(connectors, &raw.cache);
                Ok(MountConfig {
                    path: raw.path,
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

        Ok(S3ConnectorConfig {
            bucket,
            region: mount.region.or_else(|| defaults.and_then(|d| d.region.clone())),
            prefix: mount.prefix.or_else(|| defaults.and_then(|d| d.prefix.clone())),
            endpoint: mount
                .endpoint
                .or_else(|| defaults.and_then(|d| d.endpoint.clone())),
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

        // Mount values override defaults; credentials_path must be specified somewhere
        let credentials_path = mount
            .credentials_path
            .or_else(|| defaults.map(|d| d.credentials_path.clone()))
            .ok_or_else(|| {
                ConfigError::ValidationError(format!(
                    "Mount {:?} uses GDrive connector but no credentials_path specified (either on mount or in connectors.gdrive defaults)",
                    mount_path
                ))
            })?;

        // root_folder_id must also be specified somewhere
        let root_folder_id = mount
            .root_folder_id
            .or_else(|| defaults.map(|d| d.root_folder_id.clone()))
            .ok_or_else(|| {
                ConfigError::ValidationError(format!(
                    "Mount {:?} uses GDrive connector but no root_folder_id specified (either on mount or in connectors.gdrive defaults)",
                    mount_path
                ))
            })?;

        Ok(GDriveConnectorConfig {
            credentials_path,
            root_folder_id,
        })
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

        Self::from_str(&content)
    }

    /// Parse configuration from a YAML string
    pub fn from_str(content: &str) -> Result<Self, ConfigError> {
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
                ConnectorConfig::GDrive(gdrive) => {
                    if gdrive.root_folder_id.is_empty() {
                        return Err(ConfigError::ValidationError(format!(
                            "Mount {:?}: GDrive root_folder_id cannot be empty",
                            mount.path
                        )));
                    }
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

        let config = Config::from_str(yaml).unwrap();
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

        let config = Config::from_str(yaml).unwrap();
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

        let result = Config::from_str(yaml);
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

        let config = Config::from_str(yaml).unwrap();
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

        let config = Config::from_str(yaml).unwrap();
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

        let config = Config::from_str(yaml).unwrap();
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
}
