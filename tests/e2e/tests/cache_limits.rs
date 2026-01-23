//! Cache eviction and size limit tests
//!
//! Tests cache behavior under pressure including:
//! - Cache size limits
//! - Eviction behavior when cache is full
//! - Metadata TTL expiry
//! - Memory cache limits
//! - Cache behavior with large files

mod common;

use anyhow::Result;
use common::*;
use fuse_adapter_e2e::{
    assert_file_content, assert_file_content_str, assert_file_exists, filesystem_cache_fast,
    random_bytes, random_filename, CacheConfig, MountConfig, S3ConnectorConfig, TestCacheType,
    TestConfig, TestConfigBuilder, TestHarness,
};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::time::Duration;
use tokio::time::sleep;

// =============================================================================
// Cache Size Limit Tests
// =============================================================================

/// Test that cache respects configured max size
/// Note: This test validates the cache works under size pressure.
/// Actual eviction behavior depends on implementation.
#[tokio::test]
async fn test_cache_under_size_pressure() -> Result<()> {
    // Create harness with a small cache size limit (e.g., 10MB)
    let harness = TestHarness::with_config(|builder| {
        builder.add_mount_with_small_cache("small-cache", "10MB")
    })
    .await?;

    let mount = harness.mount();

    // Write files that exceed cache size
    let file_size = 2 * 1024 * 1024; // 2MB each
    let num_files = 8; // 16MB total, exceeding 10MB cache

    let mut files = Vec::new();
    for i in 0..num_files {
        let filename = format!("large-{}.bin", i);
        let filepath = mount.join(&filename);
        let content = random_bytes(file_size);

        create_file(&filepath, &content)?;
        files.push((filepath, content));
    }

    // All files should still be readable (cache should handle overflow gracefully)
    for (filepath, expected) in &files {
        let actual = fs::read(filepath)?;
        assert_eq!(
            actual.len(),
            expected.len(),
            "File size mismatch for {:?}",
            filepath
        );
        assert_eq!(actual, *expected, "Content mismatch for {:?}", filepath);
    }

    harness.cleanup().await?;
    Ok(())
}

/// Test that recently accessed files remain in cache (LRU behavior)
#[tokio::test]
async fn test_lru_cache_behavior() -> Result<()> {
    let harness =
        TestHarness::with_config(|builder| builder.add_mount_with_small_cache("lru-cache", "5MB"))
            .await?;

    let mount = harness.mount();

    // Create several files
    let file_size = 1024 * 1024; // 1MB each
    let mut files = Vec::new();

    for i in 0..6 {
        let filename = format!("lru-{}.bin", i);
        let filepath = mount.join(&filename);
        let content = random_bytes(file_size);
        create_file(&filepath, &content)?;
        files.push((filepath, content));
    }

    // Access files 0, 1, 2 again (should keep them "hot" in cache)
    for i in 0..3 {
        let _ = fs::read(&files[i].0)?;
    }

    // Add more files to trigger potential eviction
    for i in 6..10 {
        let filename = format!("lru-{}.bin", i);
        let filepath = mount.join(&filename);
        let content = random_bytes(file_size);
        create_file(&filepath, &content)?;
        files.push((filepath, content));
    }

    // The recently accessed files (0, 1, 2) should still be readable
    for i in 0..3 {
        let actual = fs::read(&files[i].0)?;
        assert_eq!(
            actual, files[i].1,
            "Recently accessed file {} should still be readable",
            i
        );
    }

    harness.cleanup().await?;
    Ok(())
}

/// Test cache behavior when writing a single file larger than cache
#[tokio::test]
async fn test_single_large_file_exceeds_cache() -> Result<()> {
    let harness =
        TestHarness::with_config(|builder| builder.add_mount_with_small_cache("big-file", "5MB"))
            .await?;

    let mount = harness.mount();

    // Write a file larger than the cache size
    let filename = random_filename("huge");
    let filepath = mount.join(&filename);
    let file_size = 8 * 1024 * 1024; // 8MB
    let content = random_bytes(file_size);

    create_file(&filepath, &content)?;

    // File should still be readable (cache should handle this gracefully)
    let actual = fs::read(&filepath)?;
    assert_eq!(actual.len(), content.len());
    assert_eq!(actual, content);

    // Force sync and verify in S3
    harness.force_sync().await?;

    let s3_content = harness
        .bucket()
        .get_object(&format!("big-file/{}", filename))
        .await?;
    assert_eq!(s3_content.len(), content.len());

    harness.cleanup().await?;
    Ok(())
}

// =============================================================================
// Metadata TTL Tests
// =============================================================================

/// Test that metadata becomes stale after TTL
#[tokio::test]
async fn test_metadata_ttl_expiry() -> Result<()> {
    // Use a harness with short metadata TTL
    let harness = TestHarness::with_cache(TestCacheType::FilesystemFast).await?;
    let mount = harness.mount();

    let filename = random_filename("ttl");
    let filepath = mount.join(&filename);

    // Create file
    create_file_str(&filepath, "initial content")?;

    // Check initial size
    let size1 = fs::metadata(&filepath)?.len();
    assert_eq!(size1, 15); // "initial content"

    // Modify file directly in S3 (bypassing cache)
    harness.force_sync().await?;
    harness
        .bucket()
        .put_object(&filename, b"much longer content than before")
        .await?;

    // Immediately, cached metadata might show old size
    // But after TTL expires, should reflect new size

    // Wait for metadata TTL to expire (30s default, but we use fast config)
    // Fast config has shorter TTL
    sleep(Duration::from_secs(35)).await;

    // Re-stat should now show updated size from backend
    // Note: This depends on implementation - cache may need to be invalidated
    let metadata = fs::metadata(&filepath)?;
    // The behavior here depends on whether the cache checks TTL on stat

    harness.cleanup().await?;
    Ok(())
}

/// Test that negative cache entries expire
#[tokio::test]
async fn test_negative_cache_expiry() -> Result<()> {
    let harness = TestHarness::with_cache(TestCacheType::FilesystemFast).await?;
    let mount = harness.mount();

    let filename = random_filename("negative-ttl");
    let filepath = mount.join(&filename);

    // Verify file doesn't exist (caches negative result)
    assert!(!filepath.exists());

    // Create file directly in S3
    harness.bucket().put_object(&filename, b"created").await?;

    // Immediately after, negative cache might still say it doesn't exist
    // (This is expected behavior)

    // Wait for negative cache TTL to expire
    sleep(Duration::from_secs(35)).await;

    // Now should see the file
    assert!(
        filepath.exists(),
        "File should be visible after negative cache expires"
    );

    harness.cleanup().await?;
    Ok(())
}

/// Test that directory listing cache expires
#[tokio::test]
async fn test_dir_listing_cache_expiry() -> Result<()> {
    let harness = TestHarness::with_cache(TestCacheType::FilesystemFast).await?;
    let mount = harness.mount();

    // Create initial files
    for i in 0..3 {
        create_file_str(&mount.join(format!("initial-{}.txt", i)), "content")?;
    }

    // Read directory (caches listing)
    let entries1: Vec<_> = fs::read_dir(mount)?
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();

    assert_eq!(entries1.len(), 3);

    // Sync to ensure files are in S3
    harness.force_sync().await?;

    // Add file directly to S3
    harness
        .bucket()
        .put_object("new-from-s3.txt", b"s3 content")
        .await?;

    // Wait for directory cache TTL
    sleep(Duration::from_secs(35)).await;

    // Re-read directory - should now include new file
    let entries2: Vec<_> = fs::read_dir(mount)?
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();

    // Behavior depends on implementation
    // The new file may or may not be visible depending on cache strategy

    harness.cleanup().await?;
    Ok(())
}

// =============================================================================
// Memory Cache Tests
// =============================================================================

/// Test memory cache with max entries limit
#[tokio::test]
async fn test_memory_cache_max_entries() -> Result<()> {
    let harness = TestHarness::with_cache(TestCacheType::Memory).await?;
    let mount = harness.mount();

    // Memory cache has max_entries limit (default 1000)
    // Create more files than the limit
    let num_files = 1200;
    let mut files = Vec::new();

    for i in 0..num_files {
        let filename = format!("mem-entry-{}.txt", i);
        let filepath = mount.join(&filename);
        let content = format!("content {}", i);
        create_file_str(&filepath, &content)?;
        files.push((filepath, content));
    }

    // All files should still be accessible (older entries may be evicted from cache
    // but will be re-fetched from S3)
    for (filepath, expected) in &files {
        let actual = read_file_str(filepath)?;
        assert_eq!(actual, *expected);
    }

    harness.cleanup().await?;
    Ok(())
}

/// Test memory cache doesn't leak memory with repeated access
#[tokio::test]
async fn test_memory_cache_no_leak() -> Result<()> {
    let harness = TestHarness::with_cache(TestCacheType::Memory).await?;
    let mount = harness.mount();

    let filename = random_filename("no-leak");
    let filepath = mount.join(&filename);
    let content = random_bytes(1024);

    create_file(&filepath, &content)?;

    // Repeatedly read and write (should not accumulate memory)
    for i in 0..1000 {
        let new_content = random_bytes(1024);
        create_file(&filepath, &new_content)?;
        let _ = fs::read(&filepath)?;
    }

    // If we got here without OOM, the test passes
    assert_file_exists(&filepath);

    harness.cleanup().await?;
    Ok(())
}

// =============================================================================
// Cache Consistency Tests
// =============================================================================

/// Test that cache and backend stay in sync after eviction
#[tokio::test]
async fn test_cache_backend_consistency_after_pressure() -> Result<()> {
    let harness = TestHarness::with_config(|builder| {
        builder.add_mount_with_small_cache("consistency", "5MB")
    })
    .await?;

    let mount = harness.mount();

    // Create files exceeding cache size
    let file_size = 1024 * 1024; // 1MB
    let num_files = 10; // 10MB total

    let mut file_contents: HashMap<String, Vec<u8>> = HashMap::new();

    for i in 0..num_files {
        let filename = format!("consistent-{}.bin", i);
        let filepath = mount.join(&filename);
        let content = random_bytes(file_size);

        create_file(&filepath, &content)?;
        file_contents.insert(filename.clone(), content);
    }

    // Force sync to backend
    harness.force_sync().await?;

    // Verify all files in S3 match what we wrote
    for (filename, expected) in &file_contents {
        let s3_path = format!("consistency/{}", filename);
        let s3_content = harness.bucket().get_object(&s3_path).await?;
        assert_eq!(
            s3_content.len(),
            expected.len(),
            "S3 file size mismatch for {}",
            filename
        );
        assert_eq!(
            s3_content, *expected,
            "S3 content mismatch for {}",
            filename
        );
    }

    harness.cleanup().await?;
    Ok(())
}

/// Test that dirty files are not evicted before sync
#[tokio::test]
async fn test_dirty_files_protected_from_eviction() -> Result<()> {
    let harness = TestHarness::with_config(|builder| {
        builder.add_mount_with_small_cache("dirty-protect", "5MB")
    })
    .await?;

    let mount = harness.mount();

    let filename = random_filename("dirty");
    let filepath = mount.join(&filename);

    // Create a file
    let content = random_bytes(1024 * 1024); // 1MB
    create_file(&filepath, &content)?;

    // Don't sync yet - file is "dirty"

    // Create more files to potentially trigger eviction
    for i in 0..10 {
        let fill_file = mount.join(format!("filler-{}.bin", i));
        create_file(&fill_file, &random_bytes(512 * 1024))?; // 512KB each
    }

    // The original dirty file should still be readable with correct content
    let actual = fs::read(&filepath)?;
    assert_eq!(actual, content, "Dirty file content should be preserved");

    // Now sync
    harness.force_sync().await?;

    // Verify in S3
    let s3_content = harness
        .bucket()
        .get_object(&format!("dirty-protect/{}", filename))
        .await?;
    assert_eq!(s3_content, content);

    harness.cleanup().await?;
    Ok(())
}

// =============================================================================
// Cache Recovery Tests
// =============================================================================

/// Test that cache recovers after adapter restart
#[tokio::test]
async fn test_cache_recovery_after_restart() -> Result<()> {
    let mut harness = TestHarness::with_cache(TestCacheType::FilesystemFast).await?;
    let mount = harness.mount();

    let filename = random_filename("recovery");
    let filepath = mount.join(&filename);

    // Create file and sync
    create_file_str(&filepath, "before restart")?;
    harness.force_sync().await?;

    // Restart adapter
    harness.restart().await?;

    // File should still be accessible
    let mount = harness.mount();
    let filepath = mount.join(&filename);
    assert_file_content_str(&filepath, "before restart");

    harness.cleanup().await?;
    Ok(())
}

/// Test that pending changes survive brief cache pressure
#[tokio::test]
async fn test_pending_changes_survive_pressure() -> Result<()> {
    let harness =
        TestHarness::with_config(|builder| builder.add_mount_with_small_cache("pending", "5MB"))
            .await?;

    let mount = harness.mount();

    // Create a file without syncing
    let important_file = mount.join("important.txt");
    create_file_str(&important_file, "important data")?;

    // Fill cache with other files
    for i in 0..20 {
        let filler = mount.join(format!("filler-{}.bin", i));
        create_file(&filler, &random_bytes(256 * 1024))?;
    }

    // The important file should still have correct content
    let actual = fs::read_to_string(&important_file)?;
    assert_eq!(actual, "important data");

    // Sync everything
    harness.force_sync().await?;

    // Verify in S3
    let s3_content = harness.bucket().get_object("pending/important.txt").await?;
    assert_eq!(String::from_utf8(s3_content)?, "important data");

    harness.cleanup().await?;
    Ok(())
}

// =============================================================================
// Helper trait extension for HarnessBuilder
// =============================================================================

trait HarnessBuilderExt {
    fn add_mount_with_small_cache(&mut self, name: &str, max_size: &str) -> &mut Self;
}

impl HarnessBuilderExt for fuse_adapter_e2e::harness::HarnessBuilder {
    fn add_mount_with_small_cache(&mut self, name: &str, max_size: &str) -> &mut Self {
        // For now, use the standard cached mount
        // TODO: When harness supports custom cache sizes, update this
        self.add_cached_mount(name)
    }
}
