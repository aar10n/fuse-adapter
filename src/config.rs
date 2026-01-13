//! Configuration parsing and structures

use std::path::PathBuf;

use serde::Deserialize;

use crate::cache::CacheConfig;

/// Top-level configuration
#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    /// Logging configuration
    #[serde(default)]
    pub logging: LoggingConfig,

    /// Mount points
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

/// Mount point configuration
#[derive(Debug, Clone, Deserialize)]
pub struct MountConfig {
    /// Path where the filesystem will be mounted
    pub path: PathBuf,

    /// Connector configuration
    pub connector: ConnectorConfig,

    /// Cache configuration (optional)
    #[serde(default)]
    pub cache: CacheConfig,
}

/// Connector configuration (tagged enum)
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum ConnectorConfig {
    /// S3 connector
    S3(S3ConnectorConfig),

    /// Database connector (future)
    #[serde(rename = "database")]
    Database(DatabaseConnectorConfig),

    /// Google Drive connector (future)
    #[serde(rename = "gdrive")]
    GDrive(GDriveConnectorConfig),
}

/// S3 connector configuration
#[derive(Debug, Clone, Deserialize)]
pub struct S3ConnectorConfig {
    /// S3 bucket name
    pub bucket: String,

    /// AWS region
    pub region: Option<String>,

    /// Key prefix (prepended to all paths)
    pub prefix: Option<String>,

    /// Custom endpoint URL (for S3-compatible stores)
    pub endpoint: Option<String>,

    /// Force path-style addressing (for MinIO, LocalStack, etc.)
    #[serde(default)]
    pub force_path_style: bool,
}

/// Database connector configuration (placeholder)
#[derive(Debug, Clone, Deserialize)]
pub struct DatabaseConnectorConfig {
    /// Database connection string
    pub connection_string: String,

    /// Table name for file storage
    pub table: Option<String>,
}

/// Google Drive connector configuration (placeholder)
#[derive(Debug, Clone, Deserialize)]
pub struct GDriveConnectorConfig {
    /// Path to credentials JSON file
    pub credentials_path: PathBuf,

    /// Root folder ID in Google Drive
    pub root_folder_id: String,
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
        serde_yaml::from_str(content)
            .map_err(|e| ConfigError::ParseError(e.to_string()))
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
    fn test_parse_config() {
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
    fn test_validate_empty_mounts() {
        let config = Config {
            logging: LoggingConfig::default(),
            mounts: vec![],
        };

        assert!(config.validate().is_err());
    }
}
