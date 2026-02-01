//! Test harness for e2e tests
//!
//! Provides a high-level API for setting up and tearing down test environments.
//!
//! ## Usage Patterns
//!
//! ### Per-test harness (legacy, slower)
//! ```ignore
//! let harness = TestHarness::new().await?;
//! // ... test code ...
//! harness.cleanup().await?;
//! ```
//!
//! ### Shared harness with test contexts (faster, parallel-safe)
//! ```ignore
//! use std::sync::OnceLock;
//! static HARNESS: OnceLock<SharedHarness> = OnceLock::new();
//!
//! async fn get_harness() -> &'static SharedHarness {
//!     // Initialize once, reuse across tests
//!     HARNESS.get_or_init(|| {
//!         tokio::runtime::Handle::current()
//!             .block_on(SharedHarness::new())
//!             .unwrap()
//!     })
//! }
//!
//! #[tokio::test]
//! async fn my_test() -> Result<()> {
//!     let ctx = get_harness().await.context().await?;
//!     // ctx.mount() returns an isolated directory for this test
//!     // ... test code ...
//!     ctx.cleanup().await?;
//!     Ok(())
//! }
//! ```

use crate::config::{
    filesystem_cache, filesystem_cache_fast, CacheConfig, MountConfig, S3ConnectorConfig,
    StatusOverlayConfig, TestConfig, TestConfigBuilder, FAST_FLUSH_INTERVAL_SECS,
};
use crate::minio::{MinioContainer, TestBucket};
use crate::mount::MountedAdapter;
use anyhow::{Context, Result};
use std::env;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::time::sleep;
use tracing::{debug, info};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

/// Initialize logging for tests (call once per test run)
pub fn init_logging() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_test_writer()
        .try_init();
}

/// Cache type to use for tests
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TestCacheType {
    None,
    Memory,
    Filesystem,
    /// Filesystem cache with fast (1s) flush interval
    FilesystemFast,
}

impl TestCacheType {
    /// Get from environment variable, defaulting to FilesystemFast
    pub fn from_env() -> Self {
        match env::var("CACHE_TYPE").as_deref() {
            Ok("none") => TestCacheType::None,
            Ok("memory") => TestCacheType::Memory,
            Ok("filesystem") => TestCacheType::Filesystem,
            Ok("filesystem_fast") | _ => TestCacheType::FilesystemFast,
        }
    }

    /// Get the flush interval in seconds for this cache type
    pub fn flush_interval_secs(&self) -> u64 {
        match self {
            TestCacheType::FilesystemFast => FAST_FLUSH_INTERVAL_SECS,
            TestCacheType::Filesystem => crate::config::DEFAULT_TEST_FLUSH_INTERVAL_SECS,
            _ => 0, // No cache or memory cache don't have flush intervals
        }
    }
}

// ============================================================================
// SharedHarness - Efficient shared harness for parallel tests
// ============================================================================

/// A shared test harness that can be reused across multiple tests.
///
/// This harness maintains a single MinIO connection, bucket, and adapter process,
/// allowing tests to run in parallel with prefix-based isolation.
///
/// Use `context()` to create an isolated `TestContext` for each test.
pub struct SharedHarness {
    minio: Arc<MinioContainer>,
    bucket: TestBucket,
    adapter: MountedAdapter,
    #[allow(dead_code)]
    config: TestConfig,
    #[allow(dead_code)]
    temp_dir: TempDir, // Kept alive to preserve mount/cache directories
    mount_path: PathBuf,
    #[allow(dead_code)]
    cache_path: PathBuf,
    flush_interval_secs: u64,
    context_counter: AtomicU64,
}

impl SharedHarness {
    /// Create a new shared harness with default (fast) cache configuration
    pub async fn new() -> Result<Self> {
        Self::with_cache(TestCacheType::FilesystemFast).await
    }

    /// Create a shared harness with a specific cache type
    pub async fn with_cache(cache_type: TestCacheType) -> Result<Self> {
        init_logging();

        info!(
            "Setting up shared test harness with cache type: {:?}",
            cache_type
        );

        // Get shared MinIO container (safe for parallel tests)
        let minio = MinioContainer::shared().await?;

        // Create a shared test bucket
        let bucket = minio.create_test_bucket().await?;

        // Create temp directories
        let temp_dir = TempDir::new().context("Failed to create temp directory")?;
        let mount_path = temp_dir.path().join("mount");
        let cache_path = temp_dir.path().join("cache");
        let config_path = temp_dir.path().join("config.yaml");

        std::fs::create_dir_all(&mount_path)?;
        std::fs::create_dir_all(&cache_path)?;

        let flush_interval_secs = cache_type.flush_interval_secs();

        // Build cache config
        let cache_config = match cache_type {
            TestCacheType::None => None,
            TestCacheType::Memory => Some(CacheConfig::Memory { max_entries: 1000 }),
            TestCacheType::Filesystem => Some(filesystem_cache(cache_path.clone())),
            TestCacheType::FilesystemFast => Some(filesystem_cache_fast(cache_path.clone())),
        };

        // Build test config - mount bucket root (no prefix) so tests can use subdirectories
        let config = TestConfigBuilder::new()
            .logging_level("debug")
            .error_mode("exit")
            .add_mount(MountConfig {
                path: mount_path.clone(),
                read_only: None,
                uid: None,
                gid: None,
                error_mode: None,
                status_overlay: None,
                connector: S3ConnectorConfig {
                    connector_type: "s3".to_string(),
                    bucket: bucket.name().to_string(),
                    region: Some("us-east-1".to_string()),
                    prefix: None, // No prefix - tests use subdirectories
                    endpoint: Some(minio.endpoint().to_string()),
                    force_path_style: Some(true),
                },
                cache: cache_config,
            })
            .build();

        // Start the adapter
        let adapter = MountedAdapter::start(&config, &config_path).await?;

        info!("Shared harness ready, mount at {:?}", mount_path);

        Ok(Self {
            minio,
            bucket,
            adapter,
            config,
            temp_dir,
            mount_path,
            cache_path,
            flush_interval_secs,
            context_counter: AtomicU64::new(0),
        })
    }

    /// Create an isolated test context for a single test.
    ///
    /// Each context gets a unique subdirectory within the mount, providing
    /// isolation for parallel test execution.
    pub async fn context(&self) -> Result<TestContext<'_>> {
        let id = self.context_counter.fetch_add(1, Ordering::SeqCst);
        let prefix = format!("test-{}-{}", id, Uuid::new_v4());

        let test_dir = self.mount_path.join(&prefix);
        std::fs::create_dir_all(&test_dir)
            .with_context(|| format!("Failed to create test directory: {:?}", test_dir))?;

        debug!("Created test context with prefix: {}", prefix);

        Ok(TestContext {
            harness: self,
            prefix,
            test_dir,
        })
    }

    /// Get the base mount point path
    pub fn mount(&self) -> &Path {
        &self.mount_path
    }

    /// Get the MinIO container
    pub fn minio(&self) -> &MinioContainer {
        &self.minio
    }

    /// Get the test bucket for direct S3 operations
    pub fn bucket(&self) -> &TestBucket {
        &self.bucket
    }

    /// Get the flush interval in seconds
    pub fn flush_interval_secs(&self) -> u64 {
        self.flush_interval_secs
    }

    /// Force a cache sync by waiting for the flush interval plus buffer
    pub async fn force_sync(&self) -> Result<()> {
        if self.flush_interval_secs == 0 {
            // No cache or memory cache - no sync needed
            return Ok(());
        }
        let wait_secs = self.flush_interval_secs + 1;
        info!("Forcing cache sync (waiting {}s)...", wait_secs);
        sleep(Duration::from_secs(wait_secs)).await;
        Ok(())
    }

    /// Shutdown the shared harness.
    ///
    /// This stops the adapter and cleans up resources.
    /// Call this at the end of your test suite if needed.
    pub async fn shutdown(mut self) -> Result<()> {
        info!("Shutting down shared harness...");
        self.adapter.stop().await?;

        // Clean up the bucket
        let bucket_name = self.bucket.name().to_string();
        let s3_client = self.minio.s3_client().clone();

        let objects = self.bucket.list_objects(None).await?;
        for key in objects {
            s3_client
                .delete_object()
                .bucket(&bucket_name)
                .key(&key)
                .send()
                .await?;
        }

        s3_client
            .delete_bucket()
            .bucket(&bucket_name)
            .send()
            .await?;

        info!("Shared harness shut down");
        Ok(())
    }
}

// ============================================================================
// TestContext - Per-test isolation within a SharedHarness
// ============================================================================

/// An isolated test context within a shared harness.
///
/// Each `TestContext` has its own subdirectory and S3 prefix, allowing
/// parallel tests to run without interfering with each other.
pub struct TestContext<'a> {
    harness: &'a SharedHarness,
    prefix: String,
    test_dir: PathBuf,
}

impl<'a> TestContext<'a> {
    /// Get the isolated mount point for this test
    pub fn mount(&self) -> &Path {
        &self.test_dir
    }

    /// Get the S3 prefix for this test's objects
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// Get the underlying harness
    pub fn harness(&self) -> &'a SharedHarness {
        self.harness
    }

    /// Get the MinIO container
    pub fn minio(&self) -> &MinioContainer {
        self.harness.minio()
    }

    /// Get the test bucket
    pub fn bucket(&self) -> &TestBucket {
        self.harness.bucket()
    }

    /// Check if an object exists in this test's prefix
    pub async fn object_exists(&self, name: &str) -> Result<bool> {
        let key = format!("{}/{}", self.prefix, name);
        self.harness.bucket.object_exists(&key).await
    }

    /// Get object content from this test's prefix
    pub async fn get_object(&self, name: &str) -> Result<Vec<u8>> {
        let key = format!("{}/{}", self.prefix, name);
        self.harness.bucket.get_object(&key).await
    }

    /// List objects in this test's prefix
    pub async fn list_objects(&self) -> Result<Vec<String>> {
        let prefix = format!("{}/", self.prefix);
        let objects = self.harness.bucket.list_objects(Some(&prefix)).await?;
        // Strip the prefix from the returned keys
        Ok(objects
            .into_iter()
            .filter_map(|key| key.strip_prefix(&prefix).map(|s| s.to_string()))
            .collect())
    }

    /// Force a cache sync
    pub async fn force_sync(&self) -> Result<()> {
        self.harness.force_sync().await
    }

    /// Cleanup this test context.
    ///
    /// This removes all files created by this test from both the filesystem
    /// and S3. The shared harness remains running for other tests.
    pub async fn cleanup(self) -> Result<()> {
        debug!("Cleaning up test context: {}", self.prefix);

        // Remove local test directory
        if self.test_dir.exists() {
            // Use remove_dir_all which handles non-empty directories
            if let Err(e) = std::fs::remove_dir_all(&self.test_dir) {
                debug!("Failed to remove test directory {:?}: {}", self.test_dir, e);
            }
        }

        // Give a moment for any pending operations
        sleep(Duration::from_millis(50)).await;

        // Clean up S3 objects with this prefix (after sync)
        let prefix = format!("{}/", self.prefix);
        let objects = self.harness.bucket.list_objects(Some(&prefix)).await?;

        if !objects.is_empty() {
            let s3_client = self.harness.minio.s3_client();
            let bucket_name = self.harness.bucket.name();

            for key in objects {
                let _ = s3_client
                    .delete_object()
                    .bucket(bucket_name)
                    .key(&key)
                    .send()
                    .await;
            }
        }

        debug!("Test context {} cleaned up", self.prefix);
        Ok(())
    }
}

// ============================================================================
// TestHarness - Legacy per-test harness (backwards compatible)
// ============================================================================

/// Main test harness that manages the entire test lifecycle.
///
/// This is the legacy harness that creates a new MinIO bucket and adapter
/// for each test. For better performance with parallel tests, consider
/// using `SharedHarness` with `TestContext` instead.
pub struct TestHarness {
    minio: Arc<MinioContainer>,
    bucket: TestBucket,
    adapter: Option<MountedAdapter>,
    config: TestConfig,
    #[allow(dead_code)]
    temp_dir: TempDir, // Kept alive to preserve mount/cache directories
    mount_path: PathBuf,
    cache_path: PathBuf,
    config_path: PathBuf,
    flush_interval_secs: u64,
}

impl TestHarness {
    /// Create a new test harness with default configuration
    pub async fn new() -> Result<Self> {
        Self::with_cache(TestCacheType::from_env()).await
    }

    /// Create a harness with a specific cache type
    pub async fn with_cache(cache_type: TestCacheType) -> Result<Self> {
        init_logging();

        info!("Setting up test harness with cache type: {:?}", cache_type);

        // Get shared MinIO container (safe for parallel tests)
        let minio = MinioContainer::shared().await?;

        // Create test bucket
        let bucket = minio.create_test_bucket().await?;

        // Create temp directories
        let temp_dir = TempDir::new().context("Failed to create temp directory")?;
        let mount_path = temp_dir.path().join("mount");
        let cache_path = temp_dir.path().join("cache");
        let config_path = temp_dir.path().join("config.yaml");

        std::fs::create_dir_all(&mount_path)?;
        std::fs::create_dir_all(&cache_path)?;

        let flush_interval_secs = cache_type.flush_interval_secs();

        // Build cache config
        let cache_config = match cache_type {
            TestCacheType::None => None,
            TestCacheType::Memory => Some(CacheConfig::Memory { max_entries: 1000 }),
            TestCacheType::Filesystem => Some(filesystem_cache(cache_path.clone())),
            TestCacheType::FilesystemFast => Some(filesystem_cache_fast(cache_path.clone())),
        };

        // Build test config
        let config = TestConfigBuilder::new()
            .logging_level("debug")
            .error_mode("exit")
            .add_mount(MountConfig {
                path: mount_path.clone(),
                read_only: None,
                uid: None,
                gid: None,
                error_mode: None,
                status_overlay: None,
                connector: S3ConnectorConfig {
                    connector_type: "s3".to_string(),
                    bucket: bucket.name().to_string(),
                    region: Some("us-east-1".to_string()),
                    prefix: None,
                    endpoint: Some(minio.endpoint().to_string()),
                    force_path_style: Some(true),
                },
                cache: cache_config,
            })
            .build();

        // Start the adapter
        let adapter = MountedAdapter::start(&config, &config_path).await?;

        Ok(Self {
            minio,
            bucket,
            adapter: Some(adapter),
            config,
            temp_dir,
            mount_path,
            cache_path,
            config_path,
            flush_interval_secs,
        })
    }

    /// Create a harness with custom configuration
    pub async fn with_config<F>(config_fn: F) -> Result<Self>
    where
        F: FnOnce(&mut HarnessBuilder) -> &mut HarnessBuilder,
    {
        init_logging();

        let mut builder = HarnessBuilder::new().await?;
        config_fn(&mut builder);
        builder.build().await
    }

    /// Get the mount point path
    pub fn mount(&self) -> &Path {
        &self.mount_path
    }

    /// Get the cache directory path
    pub fn cache_dir(&self) -> &Path {
        &self.cache_path
    }

    /// Get the MinIO container
    pub fn minio(&self) -> &MinioContainer {
        &self.minio
    }

    /// Get the test bucket for direct S3 operations
    pub fn bucket(&self) -> &TestBucket {
        &self.bucket
    }

    /// Get the adapter (for restart tests)
    pub fn adapter(&self) -> Option<&MountedAdapter> {
        self.adapter.as_ref()
    }

    /// Get the adapter mutably
    pub fn adapter_mut(&mut self) -> Option<&mut MountedAdapter> {
        self.adapter.as_mut()
    }

    /// Get the flush interval in seconds
    pub fn flush_interval_secs(&self) -> u64 {
        self.flush_interval_secs
    }

    /// Force a cache sync by waiting for the flush interval plus buffer
    pub async fn force_sync(&self) -> Result<()> {
        if self.flush_interval_secs == 0 {
            // No cache or memory cache - no sync needed
            return Ok(());
        }
        let wait_secs = self.flush_interval_secs + 1;
        info!("Forcing cache sync (waiting {}s)...", wait_secs);
        sleep(Duration::from_secs(wait_secs)).await;
        Ok(())
    }

    /// Restart the adapter (for persistence tests)
    pub async fn restart(&mut self) -> Result<()> {
        info!("Restarting adapter...");
        if let Some(adapter) = self.adapter.take() {
            self.adapter = Some(adapter.restart(&self.config).await?);
        }
        Ok(())
    }

    /// Stop the adapter without cleanup (for testing restart scenarios)
    pub async fn stop_adapter(&mut self) -> Result<()> {
        if let Some(mut adapter) = self.adapter.take() {
            adapter.stop().await?;
        }
        Ok(())
    }

    /// Start the adapter again after stopping
    pub async fn start_adapter(&mut self) -> Result<()> {
        if self.adapter.is_none() {
            self.adapter = Some(MountedAdapter::start(&self.config, &self.config_path).await?);
        }
        Ok(())
    }

    /// Cleanup the test environment
    pub async fn cleanup(mut self) -> Result<()> {
        info!("Cleaning up test harness...");

        // Stop the adapter
        if let Some(mut adapter) = self.adapter.take() {
            adapter.stop().await?;
        }

        // Clean up the bucket
        let bucket_name = self.bucket.name().to_string();
        let s3_client = self.minio.s3_client().clone();

        // List and delete all objects
        let objects = self.bucket.list_objects(None).await?;
        for key in objects {
            s3_client
                .delete_object()
                .bucket(&bucket_name)
                .key(&key)
                .send()
                .await?;
        }

        // Delete the bucket
        s3_client
            .delete_bucket()
            .bucket(&bucket_name)
            .send()
            .await?;

        info!("Bucket {} cleaned up", bucket_name);

        // Temp dir is cleaned up on drop
        Ok(())
    }
}

impl Drop for TestHarness {
    fn drop(&mut self) {
        // Best-effort cleanup in drop
        // The async cleanup should be called explicitly when possible
    }
}

// ============================================================================
// HarnessBuilder - Builder for custom harness configurations
// ============================================================================

/// Builder for custom test harness configurations
pub struct HarnessBuilder {
    minio: Arc<MinioContainer>,
    bucket: TestBucket,
    temp_dir: TempDir,
    mounts: Vec<MountConfig>,
    logging_level: String,
    error_mode: String,
    flush_interval_secs: u64,
}

impl HarnessBuilder {
    /// Create a new harness builder with shared MinIO container
    pub async fn new() -> Result<Self> {
        let minio = MinioContainer::shared().await?;
        let bucket = minio.create_test_bucket().await?;
        let temp_dir = TempDir::new()?;

        Ok(Self {
            minio,
            bucket,
            temp_dir,
            mounts: Vec::new(),
            logging_level: "debug".to_string(),
            error_mode: "exit".to_string(),
            flush_interval_secs: FAST_FLUSH_INTERVAL_SECS,
        })
    }

    /// Set the logging level
    pub fn logging_level(&mut self, level: &str) -> &mut Self {
        self.logging_level = level.to_string();
        self
    }

    /// Set the error mode
    pub fn error_mode(&mut self, mode: &str) -> &mut Self {
        self.error_mode = mode.to_string();
        self
    }

    /// Add a standard mount with optional cache
    pub fn add_mount(&mut self, name: &str, cache: Option<CacheConfig>) -> &mut Self {
        let mount_path = self.temp_dir.path().join(format!("mount-{}", name));
        std::fs::create_dir_all(&mount_path).ok();

        self.mounts.push(MountConfig {
            path: mount_path,
            read_only: None,
            uid: None,
            gid: None,
            error_mode: None,
            status_overlay: None,
            connector: S3ConnectorConfig {
                connector_type: "s3".to_string(),
                bucket: self.bucket.name().to_string(),
                region: Some("us-east-1".to_string()),
                prefix: Some(format!("{}/", name)),
                endpoint: Some(self.minio.endpoint().to_string()),
                force_path_style: Some(true),
            },
            cache,
        });
        self
    }

    /// Add a mount with filesystem cache (fast)
    pub fn add_cached_mount(&mut self, name: &str) -> &mut Self {
        let cache_path = self.temp_dir.path().join(format!("cache-{}", name));
        std::fs::create_dir_all(&cache_path).ok();
        self.add_mount(name, Some(filesystem_cache_fast(cache_path)))
    }

    /// Add a mount without cache
    pub fn add_uncached_mount(&mut self, name: &str) -> &mut Self {
        self.add_mount(name, None)
    }

    /// Add a read-only mount
    pub fn add_read_only_mount(&mut self, name: &str) -> &mut Self {
        let mount_path = self.temp_dir.path().join(format!("mount-{}", name));
        std::fs::create_dir_all(&mount_path).ok();

        self.mounts.push(MountConfig {
            path: mount_path,
            read_only: Some(true),
            uid: None,
            gid: None,
            error_mode: None,
            status_overlay: None,
            connector: S3ConnectorConfig {
                connector_type: "s3".to_string(),
                bucket: self.bucket.name().to_string(),
                region: Some("us-east-1".to_string()),
                prefix: Some(format!("{}/", name)),
                endpoint: Some(self.minio.endpoint().to_string()),
                force_path_style: Some(true),
            },
            cache: None,
        });
        self
    }

    /// Add a mount with custom UID/GID
    pub fn add_mount_with_uid_gid(
        &mut self,
        name: &str,
        uid: Option<u32>,
        gid: Option<u32>,
    ) -> &mut Self {
        let mount_path = self.temp_dir.path().join(format!("mount-{}", name));
        let cache_path = self.temp_dir.path().join(format!("cache-{}", name));
        std::fs::create_dir_all(&mount_path).ok();
        std::fs::create_dir_all(&cache_path).ok();

        self.mounts.push(MountConfig {
            path: mount_path,
            read_only: None,
            uid,
            gid,
            error_mode: None,
            status_overlay: None,
            connector: S3ConnectorConfig {
                connector_type: "s3".to_string(),
                bucket: self.bucket.name().to_string(),
                region: Some("us-east-1".to_string()),
                prefix: Some(format!("{}/", name)),
                endpoint: Some(self.minio.endpoint().to_string()),
                force_path_style: Some(true),
            },
            cache: Some(filesystem_cache_fast(cache_path)),
        });
        self
    }

    /// Add a mount with a specific S3 prefix
    pub fn add_mount_with_prefix(&mut self, name: &str, prefix: &str) -> &mut Self {
        let mount_path = self.temp_dir.path().join(format!("mount-{}", name));
        let cache_path = self.temp_dir.path().join(format!("cache-{}", name));
        std::fs::create_dir_all(&mount_path).ok();
        std::fs::create_dir_all(&cache_path).ok();

        self.mounts.push(MountConfig {
            path: mount_path,
            read_only: None,
            uid: None,
            gid: None,
            error_mode: None,
            status_overlay: None,
            connector: S3ConnectorConfig {
                connector_type: "s3".to_string(),
                bucket: self.bucket.name().to_string(),
                region: Some("us-east-1".to_string()),
                prefix: Some(prefix.to_string()),
                endpoint: Some(self.minio.endpoint().to_string()),
                force_path_style: Some(true),
            },
            cache: Some(filesystem_cache_fast(cache_path)),
        });
        self
    }

    /// Add a mount with a small cache size limit (for testing eviction)
    pub fn add_mount_with_small_cache(&mut self, name: &str, max_size: &str) -> &mut Self {
        let mount_path = self.temp_dir.path().join(format!("mount-{}", name));
        let cache_path = self.temp_dir.path().join(format!("cache-{}", name));
        std::fs::create_dir_all(&mount_path).ok();
        std::fs::create_dir_all(&cache_path).ok();

        self.mounts.push(MountConfig {
            path: mount_path,
            read_only: None,
            uid: None,
            gid: None,
            error_mode: None,
            status_overlay: None,
            connector: S3ConnectorConfig {
                connector_type: "s3".to_string(),
                bucket: self.bucket.name().to_string(),
                region: Some("us-east-1".to_string()),
                prefix: Some(format!("{}/", name)),
                endpoint: Some(self.minio.endpoint().to_string()),
                force_path_style: Some(true),
            },
            cache: Some(CacheConfig::Filesystem {
                path: cache_path,
                max_size: max_size.to_string(),
                flush_interval: format!("{}s", FAST_FLUSH_INTERVAL_SECS),
                metadata_ttl: Some("30s".to_string()),
            }),
        });
        self
    }

    /// Add a mount with status overlay enabled (default settings)
    pub fn add_mount_with_status_overlay(&mut self, name: &str) -> &mut Self {
        let mount_path = self.temp_dir.path().join(format!("mount-{}", name));
        let cache_path = self.temp_dir.path().join(format!("cache-{}", name));
        std::fs::create_dir_all(&mount_path).ok();
        std::fs::create_dir_all(&cache_path).ok();

        self.mounts.push(MountConfig {
            path: mount_path,
            read_only: None,
            uid: None,
            gid: None,
            error_mode: None,
            status_overlay: Some(StatusOverlayConfig::default()),
            connector: S3ConnectorConfig {
                connector_type: "s3".to_string(),
                bucket: self.bucket.name().to_string(),
                region: Some("us-east-1".to_string()),
                prefix: Some(format!("{}/", name)),
                endpoint: Some(self.minio.endpoint().to_string()),
                force_path_style: Some(true),
            },
            cache: Some(filesystem_cache_fast(cache_path)),
        });
        self
    }

    /// Add a mount with status overlay using a custom prefix
    pub fn add_mount_with_status_overlay_prefix(&mut self, name: &str, prefix: &str) -> &mut Self {
        let mount_path = self.temp_dir.path().join(format!("mount-{}", name));
        let cache_path = self.temp_dir.path().join(format!("cache-{}", name));
        std::fs::create_dir_all(&mount_path).ok();
        std::fs::create_dir_all(&cache_path).ok();

        self.mounts.push(MountConfig {
            path: mount_path,
            read_only: None,
            uid: None,
            gid: None,
            error_mode: None,
            status_overlay: Some(StatusOverlayConfig {
                prefix: prefix.to_string(),
                ..Default::default()
            }),
            connector: S3ConnectorConfig {
                connector_type: "s3".to_string(),
                bucket: self.bucket.name().to_string(),
                region: Some("us-east-1".to_string()),
                prefix: Some(format!("{}/", name)),
                endpoint: Some(self.minio.endpoint().to_string()),
                force_path_style: Some(true),
            },
            cache: Some(filesystem_cache_fast(cache_path)),
        });
        self
    }

    /// Add a mount with an invalid S3 endpoint (for testing error modes)
    pub fn add_invalid_s3_mount(&mut self, name: &str) -> &mut Self {
        let mount_path = self.temp_dir.path().join(format!("mount-{}", name));
        std::fs::create_dir_all(&mount_path).ok();

        self.mounts.push(MountConfig {
            path: mount_path,
            read_only: None,
            uid: None,
            gid: None,
            error_mode: None,
            status_overlay: None,
            connector: S3ConnectorConfig {
                connector_type: "s3".to_string(),
                bucket: "nonexistent-bucket".to_string(),
                region: Some("us-east-1".to_string()),
                prefix: None,
                endpoint: Some("http://invalid-endpoint:9999".to_string()),
                force_path_style: Some(true),
            },
            cache: None,
        });
        self
    }

    /// Add a mount with an invalid S3 endpoint and status overlay
    pub fn add_invalid_s3_mount_with_overlay(&mut self, name: &str) -> &mut Self {
        let mount_path = self.temp_dir.path().join(format!("mount-{}", name));
        std::fs::create_dir_all(&mount_path).ok();

        self.mounts.push(MountConfig {
            path: mount_path,
            read_only: None,
            uid: None,
            gid: None,
            error_mode: None,
            status_overlay: Some(StatusOverlayConfig::default()),
            connector: S3ConnectorConfig {
                connector_type: "s3".to_string(),
                bucket: "nonexistent-bucket".to_string(),
                region: Some("us-east-1".to_string()),
                prefix: None,
                endpoint: Some("http://invalid-endpoint:9999".to_string()),
                force_path_style: Some(true),
            },
            cache: None,
        });
        self
    }

    /// Add a mount with an invalid S3 endpoint and per-mount error mode
    pub fn add_invalid_s3_mount_with_error_mode(
        &mut self,
        name: &str,
        error_mode: &str,
    ) -> &mut Self {
        let mount_path = self.temp_dir.path().join(format!("mount-{}", name));
        std::fs::create_dir_all(&mount_path).ok();

        self.mounts.push(MountConfig {
            path: mount_path,
            read_only: None,
            uid: None,
            gid: None,
            error_mode: Some(error_mode.to_string()),
            status_overlay: None,
            connector: S3ConnectorConfig {
                connector_type: "s3".to_string(),
                bucket: "nonexistent-bucket".to_string(),
                region: Some("us-east-1".to_string()),
                prefix: None,
                endpoint: Some("http://invalid-endpoint:9999".to_string()),
                force_path_style: Some(true),
            },
            cache: None,
        });
        self
    }

    /// Build the harness with the configured mounts
    async fn build(mut self) -> Result<TestHarness> {
        // If no mounts configured, add a default one
        if self.mounts.is_empty() {
            self.add_cached_mount("default");
        }

        let mount_path = self.mounts[0].path.clone();
        let cache_path = self.temp_dir.path().join("cache-default");
        let config_path = self.temp_dir.path().join("config.yaml");

        let config = TestConfig {
            logging: crate::config::LoggingConfig {
                level: self.logging_level,
            },
            error_mode: Some(self.error_mode),
            mounts: self.mounts,
        };

        let adapter = MountedAdapter::start(&config, &config_path).await?;

        Ok(TestHarness {
            minio: self.minio,
            bucket: self.bucket,
            adapter: Some(adapter),
            config,
            temp_dir: self.temp_dir,
            mount_path,
            cache_path,
            config_path,
            flush_interval_secs: self.flush_interval_secs,
        })
    }

    /// Build the harness using try_start (for testing error modes)
    ///
    /// This method uses `MountedAdapter::try_start` which returns `StartResult`
    /// instead of an error when the adapter fails to start.
    pub async fn try_build(
        mut self,
    ) -> Result<(TestConfig, std::path::PathBuf, crate::mount::StartResult)> {
        // If no mounts configured, add a default one
        if self.mounts.is_empty() {
            self.add_cached_mount("default");
        }

        let config_path = self.temp_dir.path().join("config.yaml");

        let config = TestConfig {
            logging: crate::config::LoggingConfig {
                level: self.logging_level,
            },
            error_mode: Some(self.error_mode),
            mounts: self.mounts,
        };

        let result = MountedAdapter::try_start(&config, &config_path).await?;

        // Keep temp_dir alive by storing in static or returning
        // For simplicity, we leak the temp_dir to keep directories alive
        let _temp_dir = Box::leak(Box::new(self.temp_dir));

        Ok((config, config_path, result))
    }
}

// ============================================================================
// Macros for test convenience
// ============================================================================

/// Macro for running async tests with the harness
#[macro_export]
macro_rules! async_test {
    ($test_name:ident, $body:expr) => {
        #[tokio::test]
        async fn $test_name() -> anyhow::Result<()> {
            let harness = $crate::TestHarness::new().await?;
            let result = async { $body(&harness).await }.await;
            harness.cleanup().await?;
            result
        }
    };
}

/// Macro for running tests with custom cache types
#[macro_export]
macro_rules! test_with_cache {
    ($test_name:ident, $cache_type:expr, $body:expr) => {
        #[tokio::test]
        async fn $test_name() -> anyhow::Result<()> {
            let harness = $crate::TestHarness::with_cache($cache_type).await?;
            let result = async { $body(&harness).await }.await;
            harness.cleanup().await?;
            result
        }
    };
}
