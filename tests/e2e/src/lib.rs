//! E2E test harness for fuse-adapter
//!
//! This crate provides infrastructure for comprehensive end-to-end testing
//! of the fuse-adapter with MinIO as the S3 backend.
//!
//! ## Quick Start
//!
//! ### Legacy per-test harness (simple, slower)
//! ```ignore
//! use fuse_adapter_e2e::TestHarness;
//!
//! #[tokio::test]
//! async fn my_test() -> anyhow::Result<()> {
//!     let harness = TestHarness::new().await?;
//!     // ... test using harness.mount() ...
//!     harness.cleanup().await
//! }
//! ```
//!
//! ### Shared harness with test contexts (faster, parallel-safe)
//! ```ignore
//! use fuse_adapter_e2e::{SharedHarness, TestContext};
//! use std::sync::OnceLock;
//! use tokio::sync::OnceCell;
//!
//! static HARNESS: OnceCell<SharedHarness> = OnceCell::const_new();
//!
//! async fn get_harness() -> &'static SharedHarness {
//!     HARNESS.get_or_init(|| async {
//!         SharedHarness::new().await.unwrap()
//!     }).await
//! }
//!
//! #[tokio::test]
//! async fn my_test() -> anyhow::Result<()> {
//!     let ctx = get_harness().await.context().await?;
//!     // ctx.mount() returns an isolated directory
//!     // ... test ...
//!     ctx.cleanup().await
//! }
//! ```

pub mod assertions;
pub mod config;
pub mod harness;
pub mod minio;
pub mod mount;

pub use assertions::*;
pub use config::{
    filesystem_cache, filesystem_cache_fast, filesystem_cache_with_interval, CacheConfig,
    MountConfig, S3ConnectorConfig, StatusOverlayConfig, TestConfig, TestConfigBuilder,
    DEFAULT_TEST_FLUSH_INTERVAL_SECS, FAST_FLUSH_INTERVAL_SECS,
};
pub use harness::{HarnessBuilder, SharedHarness, TestCacheType, TestContext, TestHarness};
pub use minio::{MinioContainer, TestBucket};
pub use mount::{MountedAdapter, StartResult};
