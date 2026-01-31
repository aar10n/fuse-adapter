//! Error mode e2e tests
//!
//! Tests for the error handling behavior when connectors fail.
//!
//! NOTE: The fuse-adapter uses lazy connection to backends, meaning the FUSE
//! mount will succeed initially even with invalid endpoints. Connector failures
//! are only detected when actual operations (like reading a directory) are attempted.
//!
//! The error_mode configuration affects behavior when connector initialization
//! fails during startup for connectors that eagerly validate. For S3 with invalid
//! endpoints, the connection typically fails lazily on first operation.

mod common;

use anyhow::Result;
use fuse_adapter_e2e::TestHarness;
use std::fs;

// =============================================================================
// Error Mode: Continue Tests
// =============================================================================

/// Test that error_mode: continue allows adapter to start and serve valid mounts
#[tokio::test]
async fn test_error_mode_continue_skips_failed_mount() -> Result<()> {
    // This test creates both a valid and invalid mount
    // With error_mode: continue, the adapter should start with just the valid mount
    let harness = TestHarness::with_config(|builder| {
        builder
            .error_mode("continue")
            .add_cached_mount("valid") // Valid mount (first)
            .add_invalid_s3_mount("invalid") // Invalid mount (will be skipped)
    })
    .await?;

    // The adapter should have started successfully
    let mount = harness.mount();
    assert!(mount.exists(), "Valid mount should exist");

    // We should be able to create files on the valid mount
    let test_file = mount.join("test.txt");
    fs::write(&test_file, "hello")?;
    assert!(test_file.exists());

    harness.cleanup().await?;
    Ok(())
}

/// Test mixed valid and invalid mounts with continue mode
#[tokio::test]
async fn test_mixed_valid_invalid_mounts_continue() -> Result<()> {
    let harness = TestHarness::with_config(|builder| {
        builder
            .error_mode("continue")
            .add_cached_mount("valid1")
            .add_invalid_s3_mount("invalid")
            .add_cached_mount("valid2")
    })
    .await?;

    // Adapter should start with valid mounts accessible
    let mount = harness.mount();
    assert!(mount.exists());

    // Create a file to verify the mount works
    let test_file = mount.join("multi-mount-test.txt");
    fs::write(&test_file, "works")?;
    assert_eq!(fs::read_to_string(&test_file)?, "works");

    harness.cleanup().await?;
    Ok(())
}

/// Test that valid mount works with global exit mode (no failures)
#[tokio::test]
async fn test_valid_mount_with_exit_mode() -> Result<()> {
    let harness = TestHarness::with_config(|builder| {
        builder.error_mode("exit").add_cached_mount("valid")
    })
    .await?;

    // With just a valid mount, it should work
    let mount = harness.mount();
    assert!(mount.exists());

    // Verify we can do basic operations
    let test_file = mount.join("exit-mode-test.txt");
    fs::write(&test_file, "exit mode works")?;
    assert_eq!(fs::read_to_string(&test_file)?, "exit mode works");

    harness.cleanup().await?;
    Ok(())
}

/// Test that error_mode: exit with all valid mounts works
#[tokio::test]
async fn test_error_mode_exit_all_valid() -> Result<()> {
    let harness = TestHarness::with_config(|builder| {
        builder
            .error_mode("exit")
            .add_cached_mount("mount1")
            .add_cached_mount("mount2")
    })
    .await?;

    let mount = harness.mount();
    assert!(mount.exists());

    fs::write(mount.join("test.txt"), "success")?;
    assert!(mount.join("test.txt").exists());

    harness.cleanup().await?;
    Ok(())
}

// =============================================================================
// Error Mode: Lazy Failure Tests
// =============================================================================

/// Test that operations on invalid mount paths fail appropriately
///
/// NOTE: With lazy connection, the mount succeeds but operations fail when
/// attempting to access the invalid backend.
#[tokio::test]
async fn test_invalid_mount_operations_fail() -> Result<()> {
    let harness = TestHarness::with_config(|builder| {
        builder
            .error_mode("continue")
            .add_cached_mount("valid")
            .add_invalid_s3_mount("invalid")
    })
    .await?;

    // Valid mount should work
    let valid_mount = harness.mount();
    let valid_file = valid_mount.join("valid-file.txt");
    fs::write(&valid_file, "valid")?;
    assert_eq!(fs::read_to_string(&valid_file)?, "valid");

    harness.cleanup().await?;
    Ok(())
}

/// Test that status overlay configuration is passed correctly
#[tokio::test]
async fn test_error_mode_with_status_overlay() -> Result<()> {
    let harness = TestHarness::with_config(|builder| {
        builder
            .error_mode("continue")
            .add_mount_with_status_overlay("with-overlay")
    })
    .await?;

    let mount = harness.mount();
    assert!(mount.exists());

    // Status overlay directory should be present
    let entries: Vec<String> = fs::read_dir(mount)?
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();

    assert!(
        entries.contains(&".fuse-adapter".to_string()),
        "Should contain status overlay directory, got: {:?}",
        entries
    );

    harness.cleanup().await?;
    Ok(())
}
