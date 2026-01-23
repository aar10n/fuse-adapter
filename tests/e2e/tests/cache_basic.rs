//! Basic cache operation tests
//!
//! Tests fundamental caching behavior: read/write through cache

mod common;

use anyhow::Result;
use common::*;
use fuse_adapter_e2e::{
    assert_file_content, assert_file_content_str, assert_file_exists, assert_not_exists,
    random_bytes, random_filename, TestCacheType, TestHarness,
};
use std::fs;
use std::time::Duration;
use tokio::time::sleep;

/// Test that writes are immediately visible through cache
#[tokio::test]
async fn test_write_read_immediate() -> Result<()> {
    let harness = TestHarness::with_cache(TestCacheType::Filesystem).await?;
    let mount = harness.mount();

    let filename = random_filename("immediate");
    let filepath = mount.join(&filename);

    // Write should be immediately readable (from cache)
    create_file_str(&filepath, "content")?;
    assert_file_content_str(&filepath, "content");

    harness.cleanup().await?;
    Ok(())
}

/// Test that modifications are visible before sync
#[tokio::test]
async fn test_modification_before_sync() -> Result<()> {
    let harness = TestHarness::with_cache(TestCacheType::Filesystem).await?;
    let mount = harness.mount();

    let filename = random_filename("dirty");
    let filepath = mount.join(&filename);

    // Initial write
    create_file_str(&filepath, "version1")?;

    // Modify without waiting for sync
    create_file_str(&filepath, "version2")?;

    // Should see modified version
    assert_file_content_str(&filepath, "version2");

    harness.cleanup().await?;
    Ok(())
}

/// Test that data is synced to backend
#[tokio::test]
async fn test_sync_to_backend() -> Result<()> {
    let harness = TestHarness::with_cache(TestCacheType::Filesystem).await?;
    let mount = harness.mount();

    let filename = random_filename("sync");
    let filepath = mount.join(&filename);
    let content = "synced content";

    create_file_str(&filepath, content)?;

    // Force sync
    harness.force_sync().await?;

    // Verify in S3
    let s3_content = harness.bucket().get_object(&filename).await?;
    assert_eq!(String::from_utf8(s3_content)?, content);

    harness.cleanup().await?;
    Ok(())
}

/// Test metadata caching (stat after create)
#[tokio::test]
async fn test_metadata_caching() -> Result<()> {
    let harness = TestHarness::with_cache(TestCacheType::Filesystem).await?;
    let mount = harness.mount();

    let filename = random_filename("meta");
    let filepath = mount.join(&filename);
    let content = random_bytes(1024);

    create_file(&filepath, &content)?;

    // Multiple stat calls should be fast (cached)
    for _ in 0..10 {
        let metadata = fs::metadata(&filepath)?;
        assert_eq!(metadata.len(), 1024);
    }

    harness.cleanup().await?;
    Ok(())
}

/// Test negative caching (stat after delete)
#[tokio::test]
async fn test_negative_caching() -> Result<()> {
    let harness = TestHarness::with_cache(TestCacheType::Filesystem).await?;
    let mount = harness.mount();

    let filename = random_filename("negative");
    let filepath = mount.join(&filename);

    // Create, sync, then delete
    create_file_str(&filepath, "content")?;
    harness.force_sync().await?;

    fs::remove_file(&filepath)?;

    // Should immediately see as not existing
    assert_not_exists(&filepath);

    // Repeated checks should also fail (negative cache)
    for _ in 0..5 {
        assert!(!filepath.exists());
    }

    harness.cleanup().await?;
    Ok(())
}

/// Test cache with binary content
#[tokio::test]
async fn test_cache_binary_content() -> Result<()> {
    let harness = TestHarness::with_cache(TestCacheType::Filesystem).await?;
    let mount = harness.mount();

    let filename = random_filename("binary");
    let filepath = mount.join(&filename);
    let content = random_bytes(10 * 1024); // 10KB binary

    create_file(&filepath, &content)?;
    assert_file_content(&filepath, &content);

    // Sync and verify
    harness.force_sync().await?;
    let s3_content = harness.bucket().get_object(&filename).await?;
    assert_eq!(s3_content, content);

    harness.cleanup().await?;
    Ok(())
}

/// Test cache with multiple files
#[tokio::test]
async fn test_cache_multiple_files() -> Result<()> {
    let harness = TestHarness::with_cache(TestCacheType::Filesystem).await?;
    let mount = harness.mount();

    let files: Vec<_> = (0..10)
        .map(|i| {
            let name = random_filename(&format!("multi{}", i));
            let content = format!("content for file {}", i);
            (name, content)
        })
        .collect();

    // Create all files
    for (name, content) in &files {
        create_file_str(&mount.join(name), content)?;
    }

    // Verify all readable before sync
    for (name, content) in &files {
        assert_file_content_str(&mount.join(name), content);
    }

    // Sync and verify in S3
    harness.force_sync().await?;

    for (name, content) in &files {
        let s3_content = harness.bucket().get_object(name).await?;
        assert_eq!(String::from_utf8(s3_content)?, *content);
    }

    harness.cleanup().await?;
    Ok(())
}

/// Test rapid read/write cycles
#[tokio::test]
async fn test_cache_rapid_cycles() -> Result<()> {
    let harness = TestHarness::with_cache(TestCacheType::Filesystem).await?;
    let mount = harness.mount();

    let filename = random_filename("rapid");
    let filepath = mount.join(&filename);

    for i in 0..20 {
        let content = format!("iteration {}", i);
        create_file_str(&filepath, &content)?;
        assert_file_content_str(&filepath, &content);
    }

    harness.cleanup().await?;
    Ok(())
}

/// Test cache behavior with no cache configured
#[tokio::test]
async fn test_no_cache() -> Result<()> {
    let harness = TestHarness::with_cache(TestCacheType::None).await?;
    let mount = harness.mount();

    let filename = random_filename("nocache");
    let filepath = mount.join(&filename);

    // Operations should still work, just slower
    create_file_str(&filepath, "content")?;
    assert_file_content_str(&filepath, "content");

    harness.cleanup().await?;
    Ok(())
}

/// Test memory cache behavior
#[tokio::test]
async fn test_memory_cache() -> Result<()> {
    let harness = TestHarness::with_cache(TestCacheType::Memory).await?;
    let mount = harness.mount();

    let filename = random_filename("memcache");
    let filepath = mount.join(&filename);

    create_file_str(&filepath, "content")?;
    assert_file_content_str(&filepath, "content");

    harness.cleanup().await?;
    Ok(())
}

/// Test large file caching
#[tokio::test]
async fn test_cache_large_file() -> Result<()> {
    let harness = TestHarness::with_cache(TestCacheType::Filesystem).await?;
    let mount = harness.mount();

    let filename = random_filename("large");
    let filepath = mount.join(&filename);
    let size = 5 * 1024 * 1024; // 5MB
    let content = random_bytes(size);

    create_file(&filepath, &content)?;
    assert_file_content(&filepath, &content);

    harness.force_sync().await?;

    // Verify synced correctly
    let s3_content = harness.bucket().get_object(&filename).await?;
    assert_eq!(s3_content.len(), size);

    harness.cleanup().await?;
    Ok(())
}

/// Test cache directory creation
#[tokio::test]
async fn test_cache_directory_creation() -> Result<()> {
    let harness = TestHarness::with_cache(TestCacheType::Filesystem).await?;
    let mount = harness.mount();

    let dirname = random_filename("cachedir");
    let dirpath = mount.join(&dirname);

    fs::create_dir(&dirpath)?;

    // Directory should be visible
    assert!(dirpath.is_dir());

    // Create file in directory
    create_file_str(&dirpath.join("file.txt"), "content")?;
    assert_file_content_str(&dirpath.join("file.txt"), "content");

    harness.cleanup().await?;
    Ok(())
}

/// Test that dirty flag is set correctly
#[tokio::test]
async fn test_dirty_state() -> Result<()> {
    let harness = TestHarness::with_cache(TestCacheType::Filesystem).await?;
    let mount = harness.mount();

    let filename = random_filename("dirty");
    let filepath = mount.join(&filename);

    // Create file (dirty)
    create_file_str(&filepath, "content")?;

    // Should be readable even though dirty
    assert_file_content_str(&filepath, "content");

    // Modify (still dirty)
    create_file_str(&filepath, "modified")?;
    assert_file_content_str(&filepath, "modified");

    // Sync (clean)
    harness.force_sync().await?;

    // Still readable
    assert_file_content_str(&filepath, "modified");

    harness.cleanup().await?;
    Ok(())
}
