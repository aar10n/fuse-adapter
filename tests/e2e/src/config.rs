//! Configuration builder for e2e tests
//!
//! Generates YAML configuration files that match fuse-adapter's expected format.

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Status overlay configuration for virtual status directory
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusOverlayConfig {
    /// Virtual directory name (default: ".fuse-adapter")
    #[serde(default = "default_prefix")]
    pub prefix: String,
    /// Maximum number of error log entries to retain (default: 1000)
    #[serde(default = "default_max_log_entries")]
    pub max_log_entries: usize,
}

fn default_prefix() -> String {
    ".fuse-adapter".to_string()
}

fn default_max_log_entries() -> usize {
    1000
}

impl Default for StatusOverlayConfig {
    fn default() -> Self {
        Self {
            prefix: default_prefix(),
            max_log_entries: default_max_log_entries(),
        }
    }
}

/// Cache configuration for a mount
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum CacheConfig {
    None,
    Memory {
        max_entries: usize,
    },
    Filesystem {
        path: PathBuf,
        max_size: String,
        #[serde(default = "default_flush_interval")]
        flush_interval: String,
        #[serde(default = "default_metadata_ttl")]
        metadata_ttl: Option<String>,
    },
}

fn default_flush_interval() -> String {
    "5s".to_string()
}

fn default_metadata_ttl() -> Option<String> {
    Some("60s".to_string())
}

impl Default for CacheConfig {
    fn default() -> Self {
        CacheConfig::None
    }
}

/// S3 connector configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3ConnectorConfig {
    #[serde(rename = "type")]
    pub connector_type: String,
    pub bucket: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub force_path_style: Option<bool>,
}

impl Default for S3ConnectorConfig {
    fn default() -> Self {
        Self {
            connector_type: "s3".to_string(),
            bucket: String::new(),
            region: Some("us-east-1".to_string()),
            prefix: None,
            endpoint: None,
            force_path_style: None,
        }
    }
}

/// Mount point configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MountConfig {
    pub path: PathBuf,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub read_only: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uid: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gid: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_mode: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status_overlay: Option<StatusOverlayConfig>,
    pub connector: S3ConnectorConfig,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache: Option<CacheConfig>,
}

/// Logging configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    pub level: String,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "debug".to_string(),
        }
    }
}

/// Full test configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestConfig {
    pub logging: LoggingConfig,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_mode: Option<String>,
    pub mounts: Vec<MountConfig>,
}

impl Default for TestConfig {
    fn default() -> Self {
        Self {
            logging: LoggingConfig::default(),
            error_mode: Some("exit".to_string()),
            mounts: Vec::new(),
        }
    }
}

impl TestConfig {
    /// Create a new configuration builder
    pub fn builder() -> TestConfigBuilder {
        TestConfigBuilder::new()
    }

    /// Serialize to YAML string
    pub fn to_yaml(&self) -> Result<String> {
        Ok(serde_yaml::to_string(self)?)
    }

    /// Write configuration to a file
    pub fn write_to_file(&self, path: &Path) -> Result<()> {
        let yaml = self.to_yaml()?;
        std::fs::write(path, yaml)?;
        Ok(())
    }
}

/// Builder for test configurations
pub struct TestConfigBuilder {
    config: TestConfig,
    default_endpoint: Option<String>,
    default_bucket: Option<String>,
    default_cache_dir: Option<PathBuf>,
}

impl TestConfigBuilder {
    pub fn new() -> Self {
        Self {
            config: TestConfig::default(),
            default_endpoint: None,
            default_bucket: None,
            default_cache_dir: None,
        }
    }

    /// Set the logging level
    pub fn logging_level(mut self, level: &str) -> Self {
        self.config.logging.level = level.to_string();
        self
    }

    /// Set the error mode
    pub fn error_mode(mut self, mode: &str) -> Self {
        self.config.error_mode = Some(mode.to_string());
        self
    }

    /// Set the default S3 endpoint for all mounts
    pub fn default_endpoint(mut self, endpoint: &str) -> Self {
        self.default_endpoint = Some(endpoint.to_string());
        self
    }

    /// Set the default bucket for all mounts
    pub fn default_bucket(mut self, bucket: &str) -> Self {
        self.default_bucket = Some(bucket.to_string());
        self
    }

    /// Set the default cache directory
    pub fn default_cache_dir(mut self, dir: PathBuf) -> Self {
        self.default_cache_dir = Some(dir);
        self
    }

    /// Add a mount with full configuration
    pub fn add_mount(mut self, mount: MountConfig) -> Self {
        self.config.mounts.push(mount);
        self
    }

    /// Add a simple S3 mount
    pub fn add_s3_mount(mut self, path: PathBuf) -> Self {
        let mount = MountConfig {
            path,
            read_only: None,
            uid: None,
            gid: None,
            error_mode: None,
            status_overlay: None,
            connector: S3ConnectorConfig {
                connector_type: "s3".to_string(),
                bucket: self.default_bucket.clone().unwrap_or_default(),
                region: Some("us-east-1".to_string()),
                prefix: None,
                endpoint: self.default_endpoint.clone(),
                force_path_style: Some(true),
            },
            cache: None,
        };
        self.config.mounts.push(mount);
        self
    }

    /// Add a mount with filesystem cache
    pub fn add_s3_mount_with_cache(self, path: PathBuf, cache_config: CacheConfig) -> Self {
        let mut builder = self.add_s3_mount(path);
        if let Some(mount) = builder.config.mounts.last_mut() {
            mount.cache = Some(cache_config);
        }
        builder
    }

    /// Add a read-only mount
    pub fn add_read_only_mount(mut self, path: PathBuf, prefix: Option<String>) -> Self {
        let mount = MountConfig {
            path,
            read_only: Some(true),
            uid: None,
            gid: None,
            error_mode: None,
            status_overlay: None,
            connector: S3ConnectorConfig {
                connector_type: "s3".to_string(),
                bucket: self.default_bucket.clone().unwrap_or_default(),
                region: Some("us-east-1".to_string()),
                prefix,
                endpoint: self.default_endpoint.clone(),
                force_path_style: Some(true),
            },
            cache: None,
        };
        self.config.mounts.push(mount);
        self
    }

    /// Build the final configuration
    pub fn build(mut self) -> TestConfig {
        // Apply defaults to all mounts that don't have explicit values
        for mount in &mut self.config.mounts {
            if mount.connector.endpoint.is_none() {
                mount.connector.endpoint = self.default_endpoint.clone();
            }
            if mount.connector.bucket.is_empty() {
                mount.connector.bucket = self.default_bucket.clone().unwrap_or_default();
            }
        }
        self.config
    }
}

impl Default for TestConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Create a standard test configuration for a single mount
pub fn standard_test_config(
    mount_path: PathBuf,
    bucket: &str,
    endpoint: &str,
    cache: Option<CacheConfig>,
) -> TestConfig {
    let mut builder = TestConfigBuilder::new()
        .logging_level("debug")
        .error_mode("exit")
        .default_endpoint(endpoint)
        .default_bucket(bucket);

    let mount = MountConfig {
        path: mount_path,
        read_only: None,
        uid: None,
        gid: None,
        error_mode: None,
        status_overlay: None,
        connector: S3ConnectorConfig {
            connector_type: "s3".to_string(),
            bucket: bucket.to_string(),
            region: Some("us-east-1".to_string()),
            prefix: None,
            endpoint: Some(endpoint.to_string()),
            force_path_style: Some(true),
        },
        cache,
    };

    builder.add_mount(mount).build()
}

/// Default flush interval for tests (in seconds)
pub const DEFAULT_TEST_FLUSH_INTERVAL_SECS: u64 = 5;

/// Fast flush interval for tests (in seconds)
pub const FAST_FLUSH_INTERVAL_SECS: u64 = 1;

/// Create a filesystem cache config with sensible test defaults
pub fn filesystem_cache(cache_dir: PathBuf) -> CacheConfig {
    filesystem_cache_with_interval(cache_dir, DEFAULT_TEST_FLUSH_INTERVAL_SECS)
}

/// Create a filesystem cache config with a fast flush interval for quicker tests
pub fn filesystem_cache_fast(cache_dir: PathBuf) -> CacheConfig {
    filesystem_cache_with_interval(cache_dir, FAST_FLUSH_INTERVAL_SECS)
}

/// Create a filesystem cache config with a custom flush interval
pub fn filesystem_cache_with_interval(cache_dir: PathBuf, flush_interval_secs: u64) -> CacheConfig {
    CacheConfig::Filesystem {
        path: cache_dir,
        max_size: "256MB".to_string(),
        flush_interval: format!("{}s", flush_interval_secs),
        metadata_ttl: Some("30s".to_string()),
    }
}

/// Create a memory cache config
pub fn memory_cache(max_entries: usize) -> CacheConfig {
    CacheConfig::Memory { max_entries }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_builder() {
        let config = TestConfigBuilder::new()
            .logging_level("trace")
            .default_endpoint("http://localhost:9000")
            .default_bucket("test-bucket")
            .add_s3_mount("/mnt/test".into())
            .build();

        assert_eq!(config.logging.level, "trace");
        assert_eq!(config.mounts.len(), 1);
        assert_eq!(config.mounts[0].connector.bucket, "test-bucket");
    }

    #[test]
    fn test_config_to_yaml() {
        let config = standard_test_config(
            "/mnt/test".into(),
            "test-bucket",
            "http://localhost:9000",
            Some(filesystem_cache("/tmp/cache".into())),
        );

        let yaml = config.to_yaml().unwrap();
        assert!(yaml.contains("test-bucket"));
        assert!(yaml.contains("localhost:9000"));
        assert!(yaml.contains("filesystem"));
    }
}
