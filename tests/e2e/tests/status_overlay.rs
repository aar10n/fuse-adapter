//! Status overlay e2e tests
//!
//! Tests for the virtual status overlay directory that provides
//! mount health information at a configurable path (default: .fuse-adapter/).
//!
//! NOTE: Some tests are marked with #[ignore] because the status overlay
//! implementation currently doesn't implement `stat()` for virtual files,
//! which means they can be listed but not read directly via open/read syscalls.
//! This is a known limitation tracked as a TODO in the main codebase.

mod common;

use anyhow::Result;
use common::create_file_str;
use fuse_adapter_e2e::TestHarness;
use std::fs;

// =============================================================================
// Status Overlay Basic Tests
// =============================================================================

/// Test that status overlay shows healthy status on successful mount
///
/// NOTE: Currently ignored because virtual files can't be read via open/read
/// until the StatusOverlay implements stat() for virtual paths.
#[tokio::test]
#[ignore = "StatusOverlay needs to implement stat() for virtual files"]
async fn test_status_overlay_shows_healthy_on_success() -> Result<()> {
    let harness =
        TestHarness::with_config(|builder| builder.add_mount_with_status_overlay("overlay-test"))
            .await?;

    let mount = harness.mount();
    let status_dir = mount.join(".fuse-adapter");

    // Read status file
    let status_path = status_dir.join("status");
    let status = fs::read_to_string(&status_path)?;
    assert_eq!(
        status.trim(),
        "healthy",
        "Status should be 'healthy' on successful mount"
    );

    // Error file should be empty for healthy mount
    let error_path = status_dir.join("error");
    let error = fs::read_to_string(&error_path)?;
    assert!(
        error.is_empty(),
        "Error file should be empty for healthy mount"
    );

    harness.cleanup().await?;
    Ok(())
}

/// Test that status overlay directory listing contains expected files
///
/// This test works because readdir is implemented for virtual directories.
#[tokio::test]
async fn test_status_overlay_directory_listing() -> Result<()> {
    let harness =
        TestHarness::with_config(|builder| builder.add_mount_with_status_overlay("overlay-list"))
            .await?;

    let mount = harness.mount();
    let status_dir = mount.join(".fuse-adapter");

    // List the status directory
    let entries: Vec<String> = fs::read_dir(&status_dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();

    // Should contain at least: status, error, error_log
    assert!(
        entries.contains(&"status".to_string()),
        "Should contain 'status' file, got: {:?}",
        entries
    );
    assert!(
        entries.contains(&"error".to_string()),
        "Should contain 'error' file, got: {:?}",
        entries
    );
    assert!(
        entries.contains(&"error_log".to_string()),
        "Should contain 'error_log' file, got: {:?}",
        entries
    );

    harness.cleanup().await?;
    Ok(())
}

/// Test that status overlay directory appears in root listing
#[tokio::test]
async fn test_status_overlay_appears_in_root_listing() -> Result<()> {
    let harness =
        TestHarness::with_config(|builder| builder.add_mount_with_status_overlay("overlay-root"))
            .await?;

    let mount = harness.mount();

    // Create a regular file so we have something in the listing
    create_file_str(&mount.join("test.txt"), "hello")?;

    // List root directory
    let entries: Vec<String> = fs::read_dir(mount)?
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();

    // Should contain the status overlay directory
    assert!(
        entries.contains(&".fuse-adapter".to_string()),
        "Root listing should contain '.fuse-adapter', got: {:?}",
        entries
    );

    // Should also contain our test file
    assert!(
        entries.contains(&"test.txt".to_string()),
        "Root listing should contain 'test.txt', got: {:?}",
        entries
    );

    harness.cleanup().await?;
    Ok(())
}

/// Test that status overlay works with custom prefix
#[tokio::test]
async fn test_status_overlay_custom_prefix() -> Result<()> {
    let harness = TestHarness::with_config(|builder| {
        builder.add_mount_with_status_overlay_prefix("overlay-custom", ".status")
    })
    .await?;

    let mount = harness.mount();

    // Default path should NOT exist in root listing
    let entries: Vec<String> = fs::read_dir(mount)?
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();

    assert!(
        !entries.contains(&".fuse-adapter".to_string()),
        "Default status directory should not exist with custom prefix"
    );

    // Custom path should appear in listing
    assert!(
        entries.contains(&".status".to_string()),
        "Custom status directory should exist, got: {:?}",
        entries
    );

    // List the custom status directory
    let custom_status_dir = mount.join(".status");
    let status_entries: Vec<String> = fs::read_dir(&custom_status_dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();

    assert!(
        status_entries.contains(&"status".to_string()),
        "Custom status dir should contain 'status', got: {:?}",
        status_entries
    );

    harness.cleanup().await?;
    Ok(())
}

/// Test that status overlay files are read-only
///
/// NOTE: Currently ignored because virtual files can't be accessed via open
/// until the StatusOverlay implements stat() for virtual paths.
#[tokio::test]
#[ignore = "StatusOverlay needs to implement stat() for virtual files"]
async fn test_status_overlay_files_are_readonly() -> Result<()> {
    let harness =
        TestHarness::with_config(|builder| builder.add_mount_with_status_overlay("overlay-ro"))
            .await?;

    let mount = harness.mount();
    let status_path = mount.join(".fuse-adapter").join("status");

    // Try to write to the status file - should fail
    let result = fs::write(&status_path, "hacked");
    assert!(
        result.is_err(),
        "Writing to status overlay files should fail"
    );

    // Verify the content wasn't changed
    let status = fs::read_to_string(&status_path)?;
    assert_eq!(
        status.trim(),
        "healthy",
        "Status content should not be modified"
    );

    harness.cleanup().await?;
    Ok(())
}

/// Test that error_log file exists in the status directory
#[tokio::test]
async fn test_status_overlay_error_log_exists() -> Result<()> {
    let harness =
        TestHarness::with_config(|builder| builder.add_mount_with_status_overlay("overlay-errors"))
            .await?;

    let mount = harness.mount();
    let status_dir = mount.join(".fuse-adapter");

    // Verify error_log appears in directory listing
    let entries: Vec<String> = fs::read_dir(&status_dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();

    assert!(
        entries.contains(&"error_log".to_string()),
        "Should contain 'error_log', got: {:?}",
        entries
    );

    harness.cleanup().await?;
    Ok(())
}
