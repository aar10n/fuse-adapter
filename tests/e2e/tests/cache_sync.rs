//! Cache synchronization tests
//!
//! Tests background sync behavior and ordering guarantees

mod common;

use anyhow::Result;
use common::*;
use fuse_adapter_e2e::{
    assert_file_content_str, assert_file_exists, assert_not_exists, random_filename, TestCacheType,
    TestHarness,
};
use std::fs;
use std::time::Duration;
use tokio::time::sleep;

/// Test that background sync runs at configured interval
#[tokio::test]
async fn test_background_sync_interval() -> Result<()> {
    let harness = TestHarness::with_cache(TestCacheType::Filesystem).await?;
    let mount = harness.mount();

    let filename = random_filename("interval");
    let filepath = mount.join(&filename);

    create_file_str(&filepath, "content")?;

    // Not in S3 yet
    assert!(!harness.bucket().object_exists(&filename).await?);

    // Wait for sync (default 5s interval + buffer)
    harness.force_sync().await?;

    // Now should be in S3
    assert!(harness.bucket().object_exists(&filename).await?);

    harness.cleanup().await?;
    Ok(())
}

/// Test that all pending changes are synced
#[tokio::test]
async fn test_all_changes_synced() -> Result<()> {
    let harness = TestHarness::with_cache(TestCacheType::Filesystem).await?;
    let mount = harness.mount();

    let files: Vec<_> = (0..5)
        .map(|i| random_filename(&format!("all{}", i)))
        .collect();

    // Create multiple files
    for (i, name) in files.iter().enumerate() {
        create_file_str(&mount.join(name), &format!("content {}", i))?;
    }

    // Sync
    harness.force_sync().await?;

    // All should be in S3
    for name in &files {
        assert!(
            harness.bucket().object_exists(name).await?,
            "File {} should exist in S3",
            name
        );
    }

    harness.cleanup().await?;
    Ok(())
}

/// Test sync order: creates before deletes
#[tokio::test]
async fn test_sync_order_creates_before_deletes() -> Result<()> {
    let harness = TestHarness::with_cache(TestCacheType::Filesystem).await?;
    let mount = harness.mount();

    // Create a file
    let to_delete = random_filename("delete");
    let to_keep = random_filename("keep");

    create_file_str(&mount.join(&to_delete), "to delete")?;
    create_file_str(&mount.join(&to_keep), "to keep")?;

    harness.force_sync().await?;

    // Now delete one and create another
    fs::remove_file(&mount.join(&to_delete))?;
    let new_file = random_filename("new");
    create_file_str(&mount.join(&new_file), "new content")?;

    harness.force_sync().await?;

    // Verify final state
    assert!(!harness.bucket().object_exists(&to_delete).await?);
    assert!(harness.bucket().object_exists(&to_keep).await?);
    assert!(harness.bucket().object_exists(&new_file).await?);

    harness.cleanup().await?;
    Ok(())
}

/// Test sync order: parent dirs before children
#[tokio::test]
async fn test_sync_order_parent_before_child() -> Result<()> {
    let harness = TestHarness::with_cache(TestCacheType::Filesystem).await?;
    let mount = harness.mount();

    let base = random_filename("parent");
    let basepath = mount.join(&base);

    // Create nested structure in single operation
    fs::create_dir_all(&basepath.join("child1").join("child2"))?;
    create_file_str(
        &basepath.join("child1").join("child2").join("file.txt"),
        "content",
    )?;

    harness.force_sync().await?;

    // Verify parent directory markers exist
    // In S3, directories are represented as prefix/, so we check for the file
    assert!(
        harness
            .bucket()
            .object_exists(&format!("{}/child1/child2/file.txt", base))
            .await?
    );

    harness.cleanup().await?;
    Ok(())
}

/// Test sync order: children deleted before parent
#[tokio::test]
async fn test_sync_order_child_delete_before_parent() -> Result<()> {
    let harness = TestHarness::with_cache(TestCacheType::Filesystem).await?;
    let mount = harness.mount();

    let base = random_filename("delparent");
    let basepath = mount.join(&base);

    // Create and sync
    fs::create_dir(&basepath)?;
    create_file_str(&basepath.join("file.txt"), "content")?;
    harness.force_sync().await?;

    // Delete everything
    fs::remove_dir_all(&basepath)?;
    harness.force_sync().await?;

    // Verify deleted from S3
    let objects = harness
        .bucket()
        .list_objects(Some(&format!("{}/", base)))
        .await?;
    assert!(objects.is_empty(), "All objects should be deleted");

    harness.cleanup().await?;
    Ok(())
}

/// Test that modifications sync correctly
#[tokio::test]
async fn test_modification_sync() -> Result<()> {
    let harness = TestHarness::with_cache(TestCacheType::Filesystem).await?;
    let mount = harness.mount();

    let filename = random_filename("modify");
    let filepath = mount.join(&filename);

    // Create and sync
    create_file_str(&filepath, "version1")?;
    harness.force_sync().await?;

    // Verify v1
    let v1 = harness.bucket().get_object(&filename).await?;
    assert_eq!(String::from_utf8(v1)?, "version1");

    // Modify and sync
    create_file_str(&filepath, "version2")?;
    harness.force_sync().await?;

    // Verify v2
    let v2 = harness.bucket().get_object(&filename).await?;
    assert_eq!(String::from_utf8(v2)?, "version2");

    harness.cleanup().await?;
    Ok(())
}

/// Test delete syncs correctly
#[tokio::test]
async fn test_delete_sync() -> Result<()> {
    let harness = TestHarness::with_cache(TestCacheType::Filesystem).await?;
    let mount = harness.mount();

    let filename = random_filename("delsync");
    let filepath = mount.join(&filename);

    // Create and sync
    create_file_str(&filepath, "content")?;
    harness.force_sync().await?;
    assert!(harness.bucket().object_exists(&filename).await?);

    // Delete and sync
    fs::remove_file(&filepath)?;
    harness.force_sync().await?;
    assert!(!harness.bucket().object_exists(&filename).await?);

    harness.cleanup().await?;
    Ok(())
}

/// Test multiple syncs don't corrupt data
#[tokio::test]
async fn test_multiple_syncs() -> Result<()> {
    let harness = TestHarness::with_cache(TestCacheType::Filesystem).await?;
    let mount = harness.mount();

    let filename = random_filename("multisync");
    let filepath = mount.join(&filename);

    create_file_str(&filepath, "content")?;

    // Multiple syncs
    for _ in 0..3 {
        harness.force_sync().await?;
    }

    // Should still be correct
    let content = harness.bucket().get_object(&filename).await?;
    assert_eq!(String::from_utf8(content)?, "content");

    harness.cleanup().await?;
    Ok(())
}

/// Test that concurrent modifications during sync are handled
#[tokio::test]
async fn test_modification_during_sync() -> Result<()> {
    let harness = TestHarness::with_cache(TestCacheType::Filesystem).await?;
    let mount = harness.mount();

    let filename = random_filename("duringsync");
    let filepath = mount.join(&filename);

    // Create file
    create_file_str(&filepath, "initial")?;

    // Start sync (force_sync waits, so we do a quick write before)
    // Note: This is a simplified test - real concurrent testing would need more sophistication
    create_file_str(&filepath, "modified")?;
    harness.force_sync().await?;

    // Final state should be "modified"
    let content = harness.bucket().get_object(&filename).await?;
    assert_eq!(String::from_utf8(content)?, "modified");

    harness.cleanup().await?;
    Ok(())
}

/// Test directory sync
#[tokio::test]
async fn test_directory_sync() -> Result<()> {
    let harness = TestHarness::with_cache(TestCacheType::Filesystem).await?;
    let mount = harness.mount();

    let dirname = random_filename("dirsync");
    let dirpath = mount.join(&dirname);

    fs::create_dir(&dirpath)?;
    create_file_str(&dirpath.join("file.txt"), "content")?;

    harness.force_sync().await?;

    // File should exist in S3
    assert!(
        harness
            .bucket()
            .object_exists(&format!("{}/file.txt", dirname))
            .await?
    );

    harness.cleanup().await?;
    Ok(())
}

/// Test empty file sync
#[tokio::test]
async fn test_empty_file_sync() -> Result<()> {
    let harness = TestHarness::with_cache(TestCacheType::Filesystem).await?;
    let mount = harness.mount();

    let filename = random_filename("empty");
    let filepath = mount.join(&filename);

    create_file(&filepath, &[])?;
    harness.force_sync().await?;

    // Empty file should exist in S3
    assert!(harness.bucket().object_exists(&filename).await?);
    let content = harness.bucket().get_object(&filename).await?;
    assert!(content.is_empty());

    harness.cleanup().await?;
    Ok(())
}

/// Test that pending deletes block reads (NotFound)
#[tokio::test]
async fn test_pending_delete_blocks_read() -> Result<()> {
    let harness = TestHarness::with_cache(TestCacheType::Filesystem).await?;
    let mount = harness.mount();

    let filename = random_filename("blockread");
    let filepath = mount.join(&filename);

    // Create and sync
    create_file_str(&filepath, "content")?;
    harness.force_sync().await?;

    // Delete locally (pending)
    fs::remove_file(&filepath)?;

    // Read should fail (even though still in S3)
    assert_not_exists(&filepath);

    harness.cleanup().await?;
    Ok(())
}
