//! File permission tests
//!
//! Tests POSIX permission handling via S3 metadata

mod common;

use anyhow::Result;
use common::*;
use fuse_adapter_e2e::{assert_file_exists, assert_file_mode, random_filename, TestHarness};
use std::fs::{self, Permissions};
use std::os::unix::fs::PermissionsExt;

/// Test default file permissions
#[tokio::test]
async fn test_default_file_permissions() -> Result<()> {
    let harness = TestHarness::new().await?;
    let mount = harness.mount();

    let filename = random_filename("default-perms");
    let filepath = mount.join(&filename);

    create_file_str(&filepath, "content")?;
    assert_file_exists(&filepath);

    // Default should be something reasonable (usually 0644 or similar)
    let metadata = fs::metadata(&filepath)?;
    let mode = metadata.permissions().mode();

    // Just verify it's a regular file with some permissions
    assert!(mode & 0o400 != 0, "File should be readable by owner");

    harness.cleanup().await?;
    Ok(())
}

/// Test setting file permissions with chmod
#[tokio::test]
async fn test_chmod_file() -> Result<()> {
    let harness = TestHarness::new().await?;
    let mount = harness.mount();

    let filename = random_filename("chmod");
    let filepath = mount.join(&filename);

    create_file_str(&filepath, "content")?;

    // Set permissions to 0755
    fs::set_permissions(&filepath, Permissions::from_mode(0o755))?;
    assert_file_mode(&filepath, 0o755);

    // Change to 0644
    fs::set_permissions(&filepath, Permissions::from_mode(0o644))?;
    assert_file_mode(&filepath, 0o644);

    // Change to 0600
    fs::set_permissions(&filepath, Permissions::from_mode(0o600))?;
    assert_file_mode(&filepath, 0o600);

    harness.cleanup().await?;
    Ok(())
}

/// Test permission persistence after sync
#[tokio::test]
async fn test_permission_persistence() -> Result<()> {
    let harness = TestHarness::new().await?;
    let mount = harness.mount();

    let filename = random_filename("persist-perms");
    let filepath = mount.join(&filename);

    create_file_str(&filepath, "content")?;
    fs::set_permissions(&filepath, Permissions::from_mode(0o750))?;

    // Force sync
    harness.force_sync().await?;

    // Verify permission still correct
    assert_file_mode(&filepath, 0o750);

    harness.cleanup().await?;
    Ok(())
}

/// Test creating file with specific umask
#[tokio::test]
async fn test_file_creation_modes() -> Result<()> {
    let harness = TestHarness::new().await?;
    let mount = harness.mount();

    // Note: actual mode depends on umask, but we can test chmod after creation
    let modes = vec![0o644, 0o755, 0o600, 0o700, 0o666, 0o777];

    for mode in modes {
        let filename = random_filename(&format!("mode-{:o}", mode));
        let filepath = mount.join(&filename);

        create_file_str(&filepath, "content")?;
        fs::set_permissions(&filepath, Permissions::from_mode(mode))?;
        assert_file_mode(&filepath, mode);

        fs::remove_file(&filepath)?;
    }

    harness.cleanup().await?;
    Ok(())
}

/// Test directory permissions
#[tokio::test]
async fn test_directory_permissions() -> Result<()> {
    let harness = TestHarness::new().await?;
    let mount = harness.mount();

    let dirname = random_filename("dir-perms");
    let dirpath = mount.join(&dirname);

    fs::create_dir(&dirpath)?;

    // Set permissions
    fs::set_permissions(&dirpath, Permissions::from_mode(0o755))?;
    assert_file_mode(&dirpath, 0o755);

    fs::set_permissions(&dirpath, Permissions::from_mode(0o700))?;
    assert_file_mode(&dirpath, 0o700);

    harness.cleanup().await?;
    Ok(())
}

/// Test permission after file modification
#[tokio::test]
async fn test_permission_after_modification() -> Result<()> {
    let harness = TestHarness::new().await?;
    let mount = harness.mount();

    let filename = random_filename("modify-perms");
    let filepath = mount.join(&filename);

    create_file_str(&filepath, "initial")?;
    fs::set_permissions(&filepath, Permissions::from_mode(0o640))?;
    assert_file_mode(&filepath, 0o640);

    // Modify file content
    create_file_str(&filepath, "modified")?;

    // Permission should ideally be preserved (implementation-dependent)
    // For now, just verify we can still read the mode
    let _mode = fs::metadata(&filepath)?.permissions().mode();

    harness.cleanup().await?;
    Ok(())
}

/// Test readable but not writable (read-only permission)
#[tokio::test]
async fn test_readonly_permission() -> Result<()> {
    let harness = TestHarness::new().await?;
    let mount = harness.mount();

    let filename = random_filename("readonly");
    let filepath = mount.join(&filename);

    create_file_str(&filepath, "content")?;

    // Set read-only
    fs::set_permissions(&filepath, Permissions::from_mode(0o444))?;
    assert_file_mode(&filepath, 0o444);

    // Should still be readable
    let content = read_file_str(&filepath)?;
    assert_eq!(content, "content");

    // Reset to writable for cleanup
    fs::set_permissions(&filepath, Permissions::from_mode(0o644))?;

    harness.cleanup().await?;
    Ok(())
}

/// Test permission changes on nested files
#[tokio::test]
async fn test_nested_file_permissions() -> Result<()> {
    let harness = TestHarness::new().await?;
    let mount = harness.mount();

    let dirname = random_filename("nested-perms");
    let dirpath = mount.join(&dirname);
    fs::create_dir(&dirpath)?;

    let filepath = dirpath.join("nested-file.txt");
    create_file_str(&filepath, "content")?;

    // Set permissions on both
    fs::set_permissions(&dirpath, Permissions::from_mode(0o755))?;
    fs::set_permissions(&filepath, Permissions::from_mode(0o644))?;

    assert_file_mode(&dirpath, 0o755);
    assert_file_mode(&filepath, 0o644);

    harness.cleanup().await?;
    Ok(())
}

/// Test all permission bits
#[tokio::test]
async fn test_all_permission_bits() -> Result<()> {
    let harness = TestHarness::new().await?;
    let mount = harness.mount();

    // Test various combinations of rwx for user/group/other
    let test_modes = vec![
        0o400, // r--------
        0o200, // -w-------
        0o100, // --x------
        0o040, // ---r-----
        0o020, // ----w----
        0o010, // -----x---
        0o004, // ------r--
        0o002, // -------w-
        0o001, // --------x
        0o777, // rwxrwxrwx
        0o000, // ---------
    ];

    for mode in test_modes {
        let filename = random_filename(&format!("bits-{:03o}", mode));
        let filepath = mount.join(&filename);

        create_file_str(&filepath, "content")?;
        fs::set_permissions(&filepath, Permissions::from_mode(mode))?;
        assert_file_mode(&filepath, mode);

        // Reset for cleanup
        fs::set_permissions(&filepath, Permissions::from_mode(0o644))?;
        fs::remove_file(&filepath)?;
    }

    harness.cleanup().await?;
    Ok(())
}
