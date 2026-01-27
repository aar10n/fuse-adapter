//! Cache concurrency tests
//!
//! Tests concurrent access patterns through the cache

mod common;

use anyhow::Result;
use common::*;
use fuse_adapter_e2e::{
    assert_file_content, assert_file_exists, random_bytes, random_filename, sha256, TestCacheType,
    TestHarness,
};
use std::fs;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

/// Test multiple writers to different files
#[tokio::test]
async fn test_concurrent_different_files() -> Result<()> {
    let harness = TestHarness::with_cache(TestCacheType::Filesystem).await?;
    let mount = harness.mount().to_path_buf();

    let num_threads = 5;
    let handles: Vec<_> = (0..num_threads)
        .map(|i| {
            let mount = mount.clone();
            thread::spawn(move || {
                for j in 0..10 {
                    let filename = format!("concurrent-{}-{}.txt", i, j);
                    let filepath = mount.join(&filename);
                    let content = format!("thread {} iteration {}", i, j);
                    fs::write(&filepath, &content).unwrap();
                    let read = fs::read_to_string(&filepath).unwrap();
                    assert_eq!(read, content);
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    harness.cleanup().await?;
    Ok(())
}

/// Test readers during write
///
/// This test verifies that concurrent reads during writes don't crash or hang.
/// Due to the nature of concurrent filesystem operations, reads may see:
/// - The initial content
/// - Updated content
/// - Empty content (during write transition)
/// - Partial content (during write transition)
///
/// The test validates the system handles this gracefully.
#[tokio::test]
async fn test_reader_during_write() -> Result<()> {
    let harness = TestHarness::with_cache(TestCacheType::Filesystem).await?;
    let mount = harness.mount().to_path_buf();

    let filename = random_filename("rdwr");
    let filepath = mount.join(&filename);

    // Create initial file
    let initial = "initial content";
    fs::write(&filepath, initial)?;

    let filepath_clone = filepath.clone();

    // Reader thread
    let reader = thread::spawn(move || {
        let mut read_count = 0;
        let mut valid_content_count = 0;
        for _ in 0..20 {
            if let Ok(content) = fs::read_to_string(&filepath_clone) {
                read_count += 1;
                // During concurrent writes, we might see:
                // - "initial content"
                // - "updated content N"
                // - Empty string (during write truncation)
                // - Partial content (during write)
                // All of these are acceptable during concurrent access
                if content.starts_with("initial") || content.starts_with("updated") {
                    valid_content_count += 1;
                }
                // Empty or partial content is acceptable during concurrent writes
            }
            thread::sleep(Duration::from_millis(10));
        }
        // We should have been able to read at least some valid content
        assert!(
            valid_content_count > 0 || read_count == 0,
            "Should see some valid content during concurrent access"
        );
    });

    // Writer updates
    for i in 0..5 {
        let content = format!("updated content {}", i);
        fs::write(&filepath, &content)?;
        thread::sleep(Duration::from_millis(50));
    }

    reader.join().unwrap();

    harness.cleanup().await?;
    Ok(())
}

/// Test multiple files concurrently with verification
#[tokio::test]
async fn test_concurrent_with_verification() -> Result<()> {
    let harness = TestHarness::with_cache(TestCacheType::Filesystem).await?;
    let mount = harness.mount().to_path_buf();

    let num_files = 20;
    let file_size = 64 * 1024; // 64KB each

    // Create files with known content
    let files: Vec<_> = (0..num_files)
        .map(|i| {
            let name = random_filename(&format!("verify{}", i));
            let content = random_bytes(file_size);
            let hash = sha256(&content);
            (name, content, hash)
        })
        .collect();

    // Write all files concurrently
    let handles: Vec<_> = files
        .iter()
        .map(|(name, content, _)| {
            let mount = mount.clone();
            let name = name.clone();
            let content = content.clone();
            thread::spawn(move || {
                let filepath = mount.join(&name);
                fs::write(&filepath, &content).unwrap();
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    // Verify all files
    for (name, _, expected_hash) in &files {
        let filepath = mount.join(name);
        let content = fs::read(&filepath)?;
        let actual_hash = sha256(&content);
        assert_eq!(
            *expected_hash, actual_hash,
            "File {} content mismatch",
            name
        );
    }

    harness.cleanup().await?;
    Ok(())
}

/// Test directory operations during file operations
#[tokio::test]
async fn test_dir_and_file_concurrent() -> Result<()> {
    let harness = TestHarness::with_cache(TestCacheType::Filesystem).await?;
    let mount = harness.mount().to_path_buf();

    let base = random_filename("mixed");
    let basepath = mount.join(&base);
    fs::create_dir(&basepath)?;

    let basepath_clone = basepath.clone();

    // Thread creating files
    let file_thread = thread::spawn(move || {
        for i in 0..10 {
            let filepath = basepath_clone.join(format!("file{}.txt", i));
            fs::write(&filepath, format!("content {}", i)).unwrap();
        }
    });

    // Thread creating subdirectories
    let basepath_clone2 = basepath.clone();
    let dir_thread = thread::spawn(move || {
        for i in 0..10 {
            let dirpath = basepath_clone2.join(format!("dir{}", i));
            fs::create_dir(&dirpath).ok(); // May fail if already exists
        }
    });

    file_thread.join().unwrap();
    dir_thread.join().unwrap();

    // Verify some files exist
    assert!(basepath.join("file0.txt").exists());
    assert!(basepath.join("file9.txt").exists());

    harness.cleanup().await?;
    Ok(())
}

/// Test rapid create/delete cycles concurrently
#[tokio::test]
async fn test_concurrent_create_delete() -> Result<()> {
    let harness = TestHarness::with_cache(TestCacheType::Filesystem).await?;
    let mount = harness.mount().to_path_buf();

    let num_threads = 3;
    let handles: Vec<_> = (0..num_threads)
        .map(|i| {
            let mount = mount.clone();
            thread::spawn(move || {
                for j in 0..20 {
                    let filename = format!("creatdel-{}-{}.txt", i, j);
                    let filepath = mount.join(&filename);
                    fs::write(&filepath, "content").unwrap();
                    fs::remove_file(&filepath).ok(); // May already be deleted
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    harness.cleanup().await?;
    Ok(())
}

/// Test concurrent reads of same file
#[tokio::test]
async fn test_concurrent_reads_same_file() -> Result<()> {
    let harness = TestHarness::with_cache(TestCacheType::Filesystem).await?;
    let mount = harness.mount().to_path_buf();

    let filename = random_filename("sameread");
    let filepath = mount.join(&filename);
    let content = random_bytes(100 * 1024); // 100KB
    let expected_hash = sha256(&content);

    fs::write(&filepath, &content)?;

    // Multiple readers
    let handles: Vec<_> = (0..10)
        .map(|_| {
            let filepath = filepath.clone();
            let expected_hash = expected_hash.clone();
            thread::spawn(move || {
                let content = fs::read(&filepath).unwrap();
                let actual_hash = sha256(&content);
                assert_eq!(expected_hash, actual_hash);
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    harness.cleanup().await?;
    Ok(())
}

/// Test concurrent stat operations
#[tokio::test]
async fn test_concurrent_stat() -> Result<()> {
    let harness = TestHarness::with_cache(TestCacheType::Filesystem).await?;
    let mount = harness.mount().to_path_buf();

    // Create some files
    for i in 0..10 {
        let filepath = mount.join(format!("stat{}.txt", i));
        fs::write(&filepath, format!("content {}", i))?;
    }

    // Many concurrent stat operations
    let handles: Vec<_> = (0..10)
        .map(|i| {
            let mount = mount.clone();
            thread::spawn(move || {
                for _ in 0..100 {
                    let filepath = mount.join(format!("stat{}.txt", i));
                    let _ = fs::metadata(&filepath);
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    harness.cleanup().await?;
    Ok(())
}

/// Test concurrent directory listing
#[tokio::test]
async fn test_concurrent_readdir() -> Result<()> {
    let harness = TestHarness::with_cache(TestCacheType::Filesystem).await?;
    let mount = harness.mount().to_path_buf();

    // Create files
    for i in 0..20 {
        let filepath = mount.join(format!("listfile{}.txt", i));
        fs::write(&filepath, format!("content {}", i))?;
    }

    // Concurrent readdir
    let handles: Vec<_> = (0..5)
        .map(|_| {
            let mount = mount.clone();
            thread::spawn(move || {
                for _ in 0..20 {
                    let count = fs::read_dir(&mount).unwrap().count();
                    assert!(count >= 20, "Expected at least 20 files, got {}", count);
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    harness.cleanup().await?;
    Ok(())
}
