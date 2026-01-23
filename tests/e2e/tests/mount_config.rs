//! Mount configuration tests
//!
//! Tests various mount configuration options including:
//! - Read-only mounts
//! - UID/GID mapping
//! - S3 prefix mounting
//! - Multiple mounts with different configurations
//! - Error mode behavior

mod common;

use anyhow::Result;
use common::*;
use fuse_adapter_e2e::{
    assert_file_content_str, assert_file_exists, random_filename, TestCacheType, TestHarness,
};
use std::fs::{self, OpenOptions};
use std::os::unix::fs::MetadataExt;

// =============================================================================
// Read-Only Mount Tests
// =============================================================================

/// Test that read-only mount allows reading files
#[tokio::test]
async fn test_read_only_mount_allows_reads() -> Result<()> {
    // Use the builder pattern to create a read-only mount
    let harness = TestHarness::with_config(|builder| builder.add_read_only_mount("readonly"))
        .await?;

    let mount = harness.mount();

    // Pre-populate a file via S3 directly
    let filename = "readable.txt";
    let content = b"This content can be read";
    harness
        .bucket()
        .put_object(&format!("readonly/{}", filename), content)
        .await?;

    // Small delay to ensure S3 consistency
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // Reading should succeed
    let filepath = mount.join(filename);
    let read_content = fs::read_to_string(&filepath)?;
    assert_eq!(read_content, "This content can be read");

    harness.cleanup().await?;
    Ok(())
}

/// Test that read-only mount rejects file creation
#[tokio::test]
async fn test_read_only_mount_rejects_create() -> Result<()> {
    let harness =
        TestHarness::with_config(|builder| builder.add_read_only_mount("readonly-create")).await?;

    let mount = harness.mount();
    let filepath = mount.join("should_fail.txt");

    // Attempting to create a file should fail with EROFS
    let result = fs::write(&filepath, "content");
    assert!(result.is_err(), "Write should fail on read-only mount");

    let err = result.unwrap_err();
    assert!(
        err.raw_os_error() == Some(libc::EROFS) || err.raw_os_error() == Some(libc::EACCES),
        "Expected EROFS or EACCES, got {:?}",
        err
    );

    harness.cleanup().await?;
    Ok(())
}

/// Test that read-only mount rejects file deletion
#[tokio::test]
async fn test_read_only_mount_rejects_delete() -> Result<()> {
    let harness =
        TestHarness::with_config(|builder| builder.add_read_only_mount("readonly-delete")).await?;

    let mount = harness.mount();

    // Pre-populate a file
    let filename = "cannot_delete.txt";
    harness
        .bucket()
        .put_object(&format!("readonly-delete/{}", filename), b"content")
        .await?;

    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    let filepath = mount.join(filename);

    // Verify file exists
    assert!(filepath.exists());

    // Attempting to delete should fail
    let result = fs::remove_file(&filepath);
    assert!(result.is_err(), "Delete should fail on read-only mount");

    harness.cleanup().await?;
    Ok(())
}

/// Test that read-only mount rejects directory creation
#[tokio::test]
async fn test_read_only_mount_rejects_mkdir() -> Result<()> {
    let harness =
        TestHarness::with_config(|builder| builder.add_read_only_mount("readonly-mkdir")).await?;

    let mount = harness.mount();
    let dirpath = mount.join("new_directory");

    let result = fs::create_dir(&dirpath);
    assert!(result.is_err(), "mkdir should fail on read-only mount");

    harness.cleanup().await?;
    Ok(())
}

/// Test that read-only mount rejects file modification
#[tokio::test]
async fn test_read_only_mount_rejects_modification() -> Result<()> {
    let harness =
        TestHarness::with_config(|builder| builder.add_read_only_mount("readonly-modify")).await?;

    let mount = harness.mount();

    // Pre-populate a file
    let filename = "immutable.txt";
    harness
        .bucket()
        .put_object(
            &format!("readonly-modify/{}", filename),
            b"original content",
        )
        .await?;

    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    let filepath = mount.join(filename);

    // Attempting to open for write should fail
    let result = OpenOptions::new().write(true).open(&filepath);
    assert!(
        result.is_err(),
        "Opening for write should fail on read-only mount"
    );

    harness.cleanup().await?;
    Ok(())
}

// =============================================================================
// UID/GID Mapping Tests
// =============================================================================

/// Test that UID mapping is applied to files
#[tokio::test]
async fn test_uid_mapping() -> Result<()> {
    let harness = TestHarness::with_config(|builder| {
        builder.add_mount_with_uid_gid("uid-test", Some(1234), None)
    })
    .await?;

    let mount = harness.mount();

    // Create a file
    let filename = random_filename("uid-test");
    let filepath = mount.join(&filename);
    create_file_str(&filepath, "content")?;

    // Check the UID
    let metadata = fs::metadata(&filepath)?;
    assert_eq!(metadata.uid(), 1234, "File should have configured UID");

    harness.cleanup().await?;
    Ok(())
}

/// Test that GID mapping is applied to files
#[tokio::test]
async fn test_gid_mapping() -> Result<()> {
    let harness = TestHarness::with_config(|builder| {
        builder.add_mount_with_uid_gid("gid-test", None, Some(5678))
    })
    .await?;

    let mount = harness.mount();

    // Create a file
    let filename = random_filename("gid-test");
    let filepath = mount.join(&filename);
    create_file_str(&filepath, "content")?;

    // Check the GID
    let metadata = fs::metadata(&filepath)?;
    assert_eq!(metadata.gid(), 5678, "File should have configured GID");

    harness.cleanup().await?;
    Ok(())
}

/// Test that both UID and GID mapping work together
#[tokio::test]
async fn test_uid_gid_combined() -> Result<()> {
    let harness = TestHarness::with_config(|builder| {
        builder.add_mount_with_uid_gid("uidgid-test", Some(1000), Some(2000))
    })
    .await?;

    let mount = harness.mount();

    // Create a file
    let filename = random_filename("uidgid");
    let filepath = mount.join(&filename);
    create_file_str(&filepath, "content")?;

    let metadata = fs::metadata(&filepath)?;
    assert_eq!(metadata.uid(), 1000, "File should have configured UID");
    assert_eq!(metadata.gid(), 2000, "File should have configured GID");

    // Create a directory and check its ownership too
    let dirpath = mount.join(random_filename("uidgid-dir"));
    fs::create_dir(&dirpath)?;

    let dir_meta = fs::metadata(&dirpath)?;
    assert_eq!(dir_meta.uid(), 1000, "Directory should have configured UID");
    assert_eq!(dir_meta.gid(), 2000, "Directory should have configured GID");

    harness.cleanup().await?;
    Ok(())
}

// =============================================================================
// Prefix Mounting Tests
// =============================================================================

/// Test that prefix mounting shows only prefixed objects
#[tokio::test]
async fn test_prefix_mounting_isolation() -> Result<()> {
    let harness =
        TestHarness::with_config(|builder| builder.add_mount_with_prefix("prefix-test", "subdir/"))
            .await?;

    let mount = harness.mount();

    // Create files at different levels in S3
    // File outside prefix (should NOT be visible)
    harness
        .bucket()
        .put_object("outside.txt", b"outside")
        .await?;
    // File inside prefix (SHOULD be visible)
    harness
        .bucket()
        .put_object("subdir/inside.txt", b"inside")
        .await?;

    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // The mount should only see the file inside the prefix
    assert!(
        mount.join("inside.txt").exists(),
        "File inside prefix should be visible"
    );

    // Root listing should only contain prefixed content
    let entries: Vec<_> = fs::read_dir(mount)?
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();

    assert!(
        !entries.contains(&"outside.txt".to_string()),
        "File outside prefix should not be visible. Entries: {:?}",
        entries
    );

    harness.cleanup().await?;
    Ok(())
}

/// Test that writes go to the correct prefix
#[tokio::test]
async fn test_prefix_mounting_writes() -> Result<()> {
    let harness =
        TestHarness::with_config(|builder| builder.add_mount_with_prefix("prefix-write", "data/"))
            .await?;

    let mount = harness.mount();

    // Create a file through the mount
    let filename = random_filename("prefixed");
    let filepath = mount.join(&filename);
    create_file_str(&filepath, "prefixed content")?;

    // Force sync
    harness.force_sync().await?;

    // Verify the file exists at the correct S3 path (with prefix)
    let s3_path = format!("data/{}", filename);
    let exists = harness.bucket().object_exists(&s3_path).await?;
    assert!(exists, "File should exist at prefixed S3 path: {}", s3_path);

    // Verify file does NOT exist at root (without prefix)
    let root_exists = harness.bucket().object_exists(&filename).await?;
    assert!(
        !root_exists,
        "File should NOT exist at S3 root without prefix"
    );

    harness.cleanup().await?;
    Ok(())
}

/// Test nested prefix mounting
#[tokio::test]
async fn test_nested_prefix_mounting() -> Result<()> {
    let harness = TestHarness::with_config(|builder| {
        builder.add_mount_with_prefix("nested-prefix", "level1/level2/level3/")
    })
    .await?;

    let mount = harness.mount();

    // Create a file
    let filename = random_filename("deeply-nested");
    let filepath = mount.join(&filename);
    create_file_str(&filepath, "deep content")?;

    harness.force_sync().await?;

    // Verify the S3 path
    let s3_path = format!("level1/level2/level3/{}", filename);
    let exists = harness.bucket().object_exists(&s3_path).await?;
    assert!(exists, "File should exist at deeply nested S3 path");

    harness.cleanup().await?;
    Ok(())
}

// =============================================================================
// Multiple Mounts Tests
// =============================================================================

/// Test multiple mounts with different prefixes to the same bucket
#[tokio::test]
async fn test_multiple_mounts_different_prefixes() -> Result<()> {
    let harness = TestHarness::with_config(|builder| {
        builder
            .add_mount_with_prefix("mount-a", "prefix-a/")
            .add_mount_with_prefix("mount-b", "prefix-b/")
    })
    .await?;

    let mount_a = harness.adapter().unwrap().mount_path(0).unwrap();
    let mount_b = harness.adapter().unwrap().mount_path(1).unwrap();

    // Create files in each mount
    let file_a = mount_a.join("file_a.txt");
    let file_b = mount_b.join("file_b.txt");

    create_file_str(&file_a, "content A")?;
    create_file_str(&file_b, "content B")?;

    // Files should be isolated
    assert!(file_a.exists());
    assert!(file_b.exists());

    // Each mount should only see its own file
    assert!(!mount_a.join("file_b.txt").exists());
    assert!(!mount_b.join("file_a.txt").exists());

    harness.cleanup().await?;
    Ok(())
}

/// Test mounts with different cache configurations
#[tokio::test]
async fn test_mounts_with_different_cache_configs() -> Result<()> {
    let harness = TestHarness::with_config(|builder| {
        builder
            .add_cached_mount("cached-mount")
            .add_uncached_mount("uncached-mount")
    })
    .await?;

    let cached_mount = harness.adapter().unwrap().mount_path(0).unwrap();
    let uncached_mount = harness.adapter().unwrap().mount_path(1).unwrap();

    // Both mounts should work correctly
    let cached_file = cached_mount.join("cached.txt");
    let uncached_file = uncached_mount.join("uncached.txt");

    create_file_str(&cached_file, "cached content")?;
    create_file_str(&uncached_file, "uncached content")?;

    assert_file_content_str(&cached_file, "cached content");
    assert_file_content_str(&uncached_file, "uncached content");

    harness.cleanup().await?;
    Ok(())
}

// =============================================================================
// Cache Configuration Tests
// =============================================================================

/// Test filesystem cache with custom flush interval
#[tokio::test]
async fn test_cache_custom_flush_interval() -> Result<()> {
    // Create harness with 2-second flush interval
    let harness = TestHarness::with_cache(TestCacheType::Filesystem).await?;
    let mount = harness.mount();

    let filename = random_filename("flush-interval");
    let filepath = mount.join(&filename);

    // Write a file
    create_file_str(&filepath, "content")?;

    // File should be readable immediately (from cache)
    assert_file_content_str(&filepath, "content");

    // But S3 might not have it yet (before flush)
    // After flush interval + buffer, S3 should have it
    harness.force_sync().await?;

    let s3_content = harness.bucket().get_object(&filename).await?;
    assert_eq!(String::from_utf8(s3_content)?, "content");

    harness.cleanup().await?;
    Ok(())
}

/// Test no cache mode (direct S3 operations)
#[tokio::test]
async fn test_no_cache_mode() -> Result<()> {
    let harness = TestHarness::with_cache(TestCacheType::None).await?;
    let mount = harness.mount();

    let filename = random_filename("no-cache");
    let filepath = mount.join(&filename);

    // Write should still work (goes directly to S3)
    create_file_str(&filepath, "direct content")?;

    // Read should fetch from S3
    assert_file_content_str(&filepath, "direct content");

    harness.cleanup().await?;
    Ok(())
}

/// Test memory cache mode
#[tokio::test]
async fn test_memory_cache_mode() -> Result<()> {
    let harness = TestHarness::with_cache(TestCacheType::Memory).await?;
    let mount = harness.mount();

    let filename = random_filename("mem-cache");
    let filepath = mount.join(&filename);

    create_file_str(&filepath, "memory cached")?;
    assert_file_content_str(&filepath, "memory cached");

    harness.cleanup().await?;
    Ok(())
}
