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

    /// Database connector defaults
    pub database: Option<DatabaseConnectorDefaults>,

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

    /// Default cache configuration for S3 mounts
    pub cache: Option<CacheConfig>,
}

/// Database connector defaults
#[derive(Debug, Clone, Deserialize)]
pub struct DatabaseConnectorDefaults {
    /// Database connection string
    pub connection_string: String,

    /// Table name for file storage
    pub table: Option<String>,

    /// Default cache configuration
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

    /// Database connector
    #[serde(rename = "database")]
    Database(DatabaseMountConnectorConfig),

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
}

/// Database mount connector - all fields optional
#[derive(Debug, Clone, Deserialize, Default)]
pub struct DatabaseMountConnectorConfig {
    /// Database connection string
    pub connection_string: Option<String>,

    /// Table name for file storage
    pub table: Option<String>,
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

    /// Database connector
    Database(DatabaseConnectorConfig),

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
}

/// Database connector configuration (fully resolved)
#[derive(Debug, Clone)]
pub struct DatabaseConnectorConfig {
    /// Database connection string
    pub connection_string: String,

    /// Table name for file storage
    pub table: Option<String>,
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
            MountConnectorConfig::Database(mount_db) => {
                let resolved_connector =
                    Self::resolve_database_connector(connectors, mount_db, &raw.path)?;
                let cache = Self::resolve_database_cache(connectors, &raw.cache);
                Ok(MountConfig {
                    path: raw.path,
                    connector: ConnectorConfig::Database(resolved_connector),
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
        // Check if this is full config mode (has bucket) or override mode
        if let Some(bucket) = mount.bucket {
            // Full config mode - no defaults needed
            Ok(S3ConnectorConfig {
                bucket,
                region: mount.region,
                prefix: mount.prefix,
                endpoint: mount.endpoint,
                force_path_style: mount.force_path_style.unwrap_or(false),
            })
        } else {
            // Override mode - require defaults
            let defaults = connectors.s3.as_ref().ok_or_else(|| {
                ConfigError::ValidationError(format!(
                    "Mount {:?} uses S3 connector without bucket; requires connectors.s3 defaults",
                    mount_path
                ))
            })?;

            Ok(S3ConnectorConfig {
                bucket: defaults.bucket.clone(),
                region: mount.region.or_else(|| defaults.region.clone()),
                prefix: mount.prefix.or_else(|| defaults.prefix.clone()),
                endpoint: mount.endpoint.or_else(|| defaults.endpoint.clone()),
                force_path_style: mount
                    .force_path_style
                    .unwrap_or(defaults.force_path_style),
            })
        }
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

    fn resolve_database_connector(
        connectors: &ConnectorDefaults,
        mount: DatabaseMountConnectorConfig,
        mount_path: &PathBuf,
    ) -> Result<DatabaseConnectorConfig, ConfigError> {
        if let Some(connection_string) = mount.connection_string {
            // Full config mode
            Ok(DatabaseConnectorConfig {
                connection_string,
                table: mount.table,
            })
        } else {
            // Override mode
            let defaults = connectors.database.as_ref().ok_or_else(|| {
                ConfigError::ValidationError(format!(
                    "Mount {:?} uses Database connector without connection_string; requires connectors.database defaults",
                    mount_path
                ))
            })?;

            Ok(DatabaseConnectorConfig {
                connection_string: defaults.connection_string.clone(),
                table: mount.table.or_else(|| defaults.table.clone()),
            })
        }
    }

    fn resolve_database_cache(
        connectors: &ConnectorDefaults,
        mount_cache: &Option<CacheConfig>,
    ) -> CacheConfig {
        if let Some(cache) = mount_cache {
            return cache.clone();
        }
        if let Some(db_defaults) = &connectors.database {
            if let Some(cache) = &db_defaults.cache {
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
        if let Some(credentials_path) = mount.credentials_path {
            // Full config mode - need root_folder_id too
            let root_folder_id = mount.root_folder_id.ok_or_else(|| {
                ConfigError::ValidationError(format!(
                    "Mount {:?} has credentials_path but missing root_folder_id",
                    mount_path
                ))
            })?;
            Ok(GDriveConnectorConfig {
                credentials_path,
                root_folder_id,
            })
        } else {
            // Override mode
            let defaults = connectors.gdrive.as_ref().ok_or_else(|| {
                ConfigError::ValidationError(format!(
                    "Mount {:?} uses GDrive connector without credentials_path; requires connectors.gdrive defaults",
                    mount_path
                ))
            })?;

            Ok(GDriveConnectorConfig {
                credentials_path: defaults.credentials_path.clone(),
                root_folder_id: mount
                    .root_folder_id
                    .clone()
                    .unwrap_or_else(|| defaults.root_folder_id.clone()),
            })
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
                ConnectorConfig::Database(db) => {
                    if db.connection_string.is_empty() {
                        return Err(ConfigError::ValidationError(format!(
                            "Mount {:?}: Database connection_string cannot be empty",
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
            err.to_string().contains("requires connectors.s3 defaults"),
            "Error should mention missing defaults: {}",
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

        // Second uses full config (ignores defaults)
        match &config.mounts[1].connector {
            ConnectorConfig::S3(s3) => {
                assert_eq!(s3.bucket, "custom-bucket");
                assert_eq!(s3.region, Some("eu-west-1".to_string()));
            }
            _ => panic!("Expected S3 connector"),
        }
    }
}
