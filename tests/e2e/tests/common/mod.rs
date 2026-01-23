//! Common test utilities and fixtures
//!
//! This module provides two patterns for test setup:
//!
//! 1. **Legacy per-test harness**: Each test creates its own harness.
//!    Simple but slower. Use `TestHarness::new()`.
//!
//! 2. **Shared harness with contexts**: Tests share one harness but get
//!    isolated directories. Faster and parallel-safe. Use `shared_harness()`
//!    and `SharedHarness::context()`.

pub use fuse_adapter_e2e::*;

use std::fs;
use std::path::Path;
use tokio::sync::OnceCell;

// ============================================================================
// Shared harness infrastructure for parallel tests
// ============================================================================

/// Global shared harness instance.
///
/// This is lazily initialized on first use and shared across all tests
/// in the same test binary. Each test should call `context()` to get
/// an isolated test directory.
static SHARED_HARNESS: OnceCell<SharedHarness> = OnceCell::const_new();

/// Get the shared harness, initializing it if necessary.
///
/// This returns a reference to a global `SharedHarness` that is reused
/// across all tests. Use `.context().await` to get an isolated test
/// directory for your specific test.
///
/// # Example
/// ```ignore
/// let ctx = shared_harness().await.context().await?;
/// let mount = ctx.mount();
/// // ... test using mount ...
/// ctx.cleanup().await?;
/// ```
pub async fn shared_harness() -> &'static SharedHarness {
    SHARED_HARNESS
        .get_or_init(|| async {
            SharedHarness::new()
                .await
                .expect("Failed to initialize shared harness")
        })
        .await
}

// ============================================================================
// File utilities
// ============================================================================

/// Create a test file with content
pub fn create_file(path: &Path, content: &[u8]) -> std::io::Result<()> {
    // Ensure parent directory exists
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, content)
}

/// Create a test file with string content
pub fn create_file_str(path: &Path, content: &str) -> std::io::Result<()> {
    // Ensure parent directory exists
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, content)
}

/// Read file content as bytes
pub fn read_file(path: &Path) -> std::io::Result<Vec<u8>> {
    fs::read(path)
}

/// Read file content as string
pub fn read_file_str(path: &Path) -> std::io::Result<String> {
    fs::read_to_string(path)
}

/// Generate a unique test name
pub fn unique_name(prefix: &str) -> String {
    format!("{}-{}", prefix, uuid::Uuid::new_v4())
}

// ============================================================================
// Macros for test patterns
// ============================================================================

/// Macro for tests using the shared harness pattern.
///
/// This creates a test that uses the shared harness with an isolated context,
/// enabling parallel test execution.
///
/// # Example
/// ```ignore
/// shared_test!(test_my_feature, |ctx| async move {
///     let mount = ctx.mount();
///     create_file_str(&mount.join("test.txt"), "hello")?;
///     assert_file_exists(&mount.join("test.txt"));
///     Ok(())
/// });
/// ```
#[macro_export]
macro_rules! shared_test {
    ($test_name:ident, $body:expr) => {
        #[tokio::test]
        async fn $test_name() -> anyhow::Result<()> {
            let ctx = $crate::common::shared_harness().await.context().await?;
            let result: anyhow::Result<()> = $body(&ctx).await;
            ctx.cleanup().await?;
            result
        }
    };
}

/// Macro for tests that need the legacy per-test harness.
///
/// Use this for tests that need adapter restart, custom configuration,
/// or other features not supported by the shared harness.
#[macro_export]
macro_rules! isolated_test {
    ($test_name:ident, $body:expr) => {
        #[tokio::test]
        async fn $test_name() -> anyhow::Result<()> {
            let harness = $crate::TestHarness::new().await?;
            let result: anyhow::Result<()> = $body(&harness).await;
            harness.cleanup().await?;
            result
        }
    };
}
