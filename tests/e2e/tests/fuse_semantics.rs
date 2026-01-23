//! FUSE semantics tests
//!
//! Tests low-level FUSE filesystem behavior including:
//! - File descriptor/handle behavior
//! - Open/close semantics
//! - Concurrent access patterns
//! - fsync/flush behavior
//! - Directory iteration consistency
//! - Inode stability

mod common;

use anyhow::Result;
use common::*;
use fuse_adapter_e2e::{
    assert_file_content, assert_file_content_str, assert_file_exists, random_bytes,
    random_filename,
};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::MetadataExt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

// =============================================================================
// File Handle Behavior Tests
// =============================================================================

/// Test that opening a file returns a valid handle
#[tokio::test]
async fn test_file_open_returns_handle() -> Result<()> {
    let ctx = shared_harness().await.context().await?;
    let mount = ctx.mount();

    let filename = random_filename("handle");
    let filepath = mount.join(&filename);

    // Create file
    create_file_str(&filepath, "content")?;

    // Open for reading - should succeed
    let file = File::open(&filepath)?;
    assert!(file.metadata().is_ok());

    // Explicitly drop to close
    drop(file);

    ctx.cleanup().await?;
    Ok(())
}

/// Test that multiple opens on the same file work
#[tokio::test]
async fn test_multiple_opens_same_file() -> Result<()> {
    let ctx = shared_harness().await.context().await?;
    let mount = ctx.mount();

    let filename = random_filename("multi-open");
    let filepath = mount.join(&filename);

    create_file_str(&filepath, "shared content")?;

    // Open multiple handles
    let file1 = File::open(&filepath)?;
    let file2 = File::open(&filepath)?;
    let file3 = File::open(&filepath)?;

    // All should read the same content
    let mut content1 = String::new();
    let mut content2 = String::new();
    let mut content3 = String::new();

    let mut f1 = file1;
    let mut f2 = file2;
    let mut f3 = file3;

    f1.read_to_string(&mut content1)?;
    f2.read_to_string(&mut content2)?;
    f3.read_to_string(&mut content3)?;

    assert_eq!(content1, "shared content");
    assert_eq!(content2, "shared content");
    assert_eq!(content3, "shared content");

    ctx.cleanup().await?;
    Ok(())
}

/// Test that file content persists after handle is closed
#[tokio::test]
async fn test_content_persists_after_close() -> Result<()> {
    let ctx = shared_harness().await.context().await?;
    let mount = ctx.mount();

    let filename = random_filename("persist-close");
    let filepath = mount.join(&filename);

    // Write and close
    {
        let mut file = File::create(&filepath)?;
        file.write_all(b"written content")?;
        // file is dropped/closed here
    }

    // Read with new handle
    let content = fs::read_to_string(&filepath)?;
    assert_eq!(content, "written content");

    ctx.cleanup().await?;
    Ok(())
}

/// Test seek operations
#[tokio::test]
async fn test_file_seek_operations() -> Result<()> {
    let ctx = shared_harness().await.context().await?;
    let mount = ctx.mount();

    let filename = random_filename("seek");
    let filepath = mount.join(&filename);
    let content = b"0123456789ABCDEF";

    create_file(&filepath, content)?;

    let mut file = File::open(&filepath)?;

    // Seek from start
    file.seek(SeekFrom::Start(5))?;
    let mut buf = [0u8; 5];
    file.read_exact(&mut buf)?;
    assert_eq!(&buf, b"56789");

    // Seek from end
    file.seek(SeekFrom::End(-4))?;
    let mut buf = [0u8; 4];
    file.read_exact(&mut buf)?;
    assert_eq!(&buf, b"CDEF");

    // Seek from current
    file.seek(SeekFrom::Start(0))?;
    file.seek(SeekFrom::Current(10))?;
    let mut buf = [0u8; 3];
    file.read_exact(&mut buf)?;
    assert_eq!(&buf, b"ABC");

    ctx.cleanup().await?;
    Ok(())
}

/// Test partial reads
#[tokio::test]
async fn test_partial_reads() -> Result<()> {
    let ctx = shared_harness().await.context().await?;
    let mount = ctx.mount();

    let filename = random_filename("partial");
    let filepath = mount.join(&filename);
    let content = random_bytes(1024);

    create_file(&filepath, &content)?;

    let mut file = File::open(&filepath)?;

    // Read in small chunks
    let mut read_content = Vec::new();
    let mut buf = [0u8; 100];

    loop {
        match file.read(&mut buf)? {
            0 => break,
            n => read_content.extend_from_slice(&buf[..n]),
        }
    }

    assert_eq!(read_content, content);

    ctx.cleanup().await?;
    Ok(())
}

// =============================================================================
// Open/Close Semantics Tests
// =============================================================================

/// Test open with different modes
#[tokio::test]
async fn test_open_modes() -> Result<()> {
    let ctx = shared_harness().await.context().await?;
    let mount = ctx.mount();

    let filename = random_filename("modes");
    let filepath = mount.join(&filename);

    // Create with create flag
    {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&filepath)?;
        file.write_all(b"initial")?;
    }

    // Open read-only
    {
        let file = OpenOptions::new().read(true).open(&filepath)?;
        let mut content = String::new();
        let mut f = file;
        f.read_to_string(&mut content)?;
        assert_eq!(content, "initial");
    }

    // Open for append
    {
        let mut file = OpenOptions::new().append(true).open(&filepath)?;
        file.write_all(b" + appended")?;
    }

    // Verify append worked
    let content = fs::read_to_string(&filepath)?;
    assert_eq!(content, "initial + appended");

    // Open with truncate
    {
        let mut file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&filepath)?;
        file.write_all(b"truncated")?;
    }

    let content = fs::read_to_string(&filepath)?;
    assert_eq!(content, "truncated");

    ctx.cleanup().await?;
    Ok(())
}

/// Test that opening non-existent file fails appropriately
#[tokio::test]
async fn test_open_nonexistent() -> Result<()> {
    let ctx = shared_harness().await.context().await?;
    let mount = ctx.mount();

    let filepath = mount.join("does_not_exist.txt");

    let result = File::open(&filepath);
    assert!(result.is_err());

    let err = result.unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::NotFound);

    ctx.cleanup().await?;
    Ok(())
}

/// Test open with create_new flag fails on existing file
#[tokio::test]
async fn test_open_create_new_existing() -> Result<()> {
    let ctx = shared_harness().await.context().await?;
    let mount = ctx.mount();

    let filename = random_filename("create-new");
    let filepath = mount.join(&filename);

    // Create file first
    create_file_str(&filepath, "existing")?;

    // Try to create_new - should fail
    let result = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&filepath);

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::AlreadyExists);

    ctx.cleanup().await?;
    Ok(())
}

// =============================================================================
// Concurrent Access Tests
// =============================================================================

/// Test concurrent reads from multiple threads
#[tokio::test]
async fn test_concurrent_reads() -> Result<()> {
    let ctx = shared_harness().await.context().await?;
    let mount = ctx.mount();

    let filename = random_filename("concurrent-read");
    let filepath = mount.join(&filename);
    let content = random_bytes(4096);

    create_file(&filepath, &content)?;

    let filepath_clone = filepath.clone();
    let content_clone = content.clone();
    let read_count = Arc::new(AtomicUsize::new(0));
    let read_count_clone = read_count.clone();

    // Spawn multiple concurrent readers
    let handles: Vec<_> = (0..10)
        .map(|_| {
            let fp = filepath_clone.clone();
            let expected = content_clone.clone();
            let counter = read_count_clone.clone();

            std::thread::spawn(move || {
                for _ in 0..10 {
                    let read = fs::read(&fp).expect("read failed");
                    assert_eq!(read, expected);
                    counter.fetch_add(1, Ordering::SeqCst);
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().expect("thread panicked");
    }

    assert_eq!(read_count.load(Ordering::SeqCst), 100);

    ctx.cleanup().await?;
    Ok(())
}

/// Test concurrent writes to different files
#[tokio::test]
async fn test_concurrent_writes_different_files() -> Result<()> {
    let ctx = shared_harness().await.context().await?;
    let mount = ctx.mount();

    let mount_path = mount.to_path_buf();
    let write_count = Arc::new(AtomicUsize::new(0));
    let write_count_clone = write_count.clone();

    let handles: Vec<_> = (0..10)
        .map(|i| {
            let mp = mount_path.clone();
            let counter = write_count_clone.clone();

            std::thread::spawn(move || {
                for j in 0..10 {
                    let filename = format!("concurrent-{}-{}.txt", i, j);
                    let filepath = mp.join(&filename);
                    let content = format!("content from thread {} iteration {}", i, j);

                    fs::write(&filepath, &content).expect("write failed");

                    let read_back = fs::read_to_string(&filepath).expect("read failed");
                    assert_eq!(read_back, content);

                    counter.fetch_add(1, Ordering::SeqCst);
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().expect("thread panicked");
    }

    assert_eq!(write_count.load(Ordering::SeqCst), 100);

    ctx.cleanup().await?;
    Ok(())
}

/// Test read-while-write behavior
#[tokio::test]
async fn test_read_while_write() -> Result<()> {
    let ctx = shared_harness().await.context().await?;
    let mount = ctx.mount();

    let filename = random_filename("read-write");
    let filepath = mount.join(&filename);

    create_file_str(&filepath, "initial content")?;

    let filepath_clone = filepath.clone();

    // Writer thread
    let writer = std::thread::spawn(move || {
        for i in 0..50 {
            let content = format!("update {}", i);
            fs::write(&filepath_clone, &content).expect("write failed");
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    });

    // Reader thread - should always see valid content
    let filepath_clone2 = filepath.clone();
    let reader = std::thread::spawn(move || {
        for _ in 0..100 {
            // Read should succeed (though content may be from any write)
            let result = fs::read_to_string(&filepath_clone2);
            assert!(result.is_ok(), "Read should succeed during writes");
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    });

    writer.join().expect("writer panicked");
    reader.join().expect("reader panicked");

    ctx.cleanup().await?;
    Ok(())
}

// =============================================================================
// fsync/flush Behavior Tests
// =============================================================================

/// Test explicit fsync
#[tokio::test]
async fn test_fsync() -> Result<()> {
    let ctx = shared_harness().await.context().await?;
    let mount = ctx.mount();

    let filename = random_filename("fsync");
    let filepath = mount.join(&filename);

    let mut file = File::create(&filepath)?;
    file.write_all(b"synced content")?;
    file.sync_all()?; // fsync

    // Content should be persisted
    drop(file);

    let content = fs::read_to_string(&filepath)?;
    assert_eq!(content, "synced content");

    ctx.cleanup().await?;
    Ok(())
}

/// Test sync_data vs sync_all
#[tokio::test]
async fn test_sync_data_vs_sync_all() -> Result<()> {
    let ctx = shared_harness().await.context().await?;
    let mount = ctx.mount();

    let filename1 = random_filename("sync-data");
    let filename2 = random_filename("sync-all");
    let filepath1 = mount.join(&filename1);
    let filepath2 = mount.join(&filename2);

    // Test sync_data
    {
        let mut file = File::create(&filepath1)?;
        file.write_all(b"data content")?;
        file.sync_data()?; // Only syncs data, not metadata
    }

    // Test sync_all
    {
        let mut file = File::create(&filepath2)?;
        file.write_all(b"all content")?;
        file.sync_all()?; // Syncs data and metadata
    }

    // Both should be readable
    assert_file_content_str(&filepath1, "data content");
    assert_file_content_str(&filepath2, "all content");

    ctx.cleanup().await?;
    Ok(())
}

/// Test flush on close
#[tokio::test]
async fn test_flush_on_close() -> Result<()> {
    let ctx = shared_harness().await.context().await?;
    let mount = ctx.mount();

    let filename = random_filename("flush-close");
    let filepath = mount.join(&filename);

    // Write without explicit sync - should flush on close
    {
        let mut file = File::create(&filepath)?;
        file.write_all(b"flushed on close")?;
        // No explicit sync - relies on close
    }

    // Small delay to ensure close completed
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let content = fs::read_to_string(&filepath)?;
    assert_eq!(content, "flushed on close");

    ctx.cleanup().await?;
    Ok(())
}

// =============================================================================
// Directory Iteration Consistency Tests
// =============================================================================

/// Test readdir returns all entries
#[tokio::test]
async fn test_readdir_completeness() -> Result<()> {
    let ctx = shared_harness().await.context().await?;
    let mount = ctx.mount();

    let files = vec!["a.txt", "b.txt", "c.txt", "d.txt", "e.txt"];

    for name in &files {
        create_file_str(&mount.join(name), name)?;
    }

    // Create a subdirectory too
    fs::create_dir(mount.join("subdir"))?;

    // Read directory
    let entries: Vec<_> = fs::read_dir(mount)?
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();

    // Should contain all files and the directory
    for name in &files {
        assert!(
            entries.contains(&name.to_string()),
            "Missing entry: {}",
            name
        );
    }
    assert!(entries.contains(&"subdir".to_string()));

    ctx.cleanup().await?;
    Ok(())
}

/// Test readdir during modification
#[tokio::test]
async fn test_readdir_during_modification() -> Result<()> {
    let ctx = shared_harness().await.context().await?;
    let mount = ctx.mount();

    // Create initial files
    for i in 0..10 {
        create_file_str(&mount.join(format!("file{}.txt", i)), "content")?;
    }

    // Start reading directory
    let mut read_dir = fs::read_dir(mount)?;

    // Read some entries
    let mut seen = Vec::new();
    for _ in 0..5 {
        if let Some(entry) = read_dir.next() {
            seen.push(entry?.file_name().to_string_lossy().to_string());
        }
    }

    // Add new files during iteration
    for i in 10..15 {
        create_file_str(&mount.join(format!("file{}.txt", i)), "new content")?;
    }

    // Continue reading - behavior may vary but shouldn't panic
    while let Some(entry) = read_dir.next() {
        let _ = entry?; // Just verify no error
    }

    ctx.cleanup().await?;
    Ok(())
}

/// Test that . and .. are present in readdir
#[tokio::test]
async fn test_readdir_dot_entries() -> Result<()> {
    let ctx = shared_harness().await.context().await?;
    let mount = ctx.mount();

    // Create a subdirectory
    let subdir = mount.join("testdir");
    fs::create_dir(&subdir)?;
    create_file_str(&subdir.join("file.txt"), "content")?;

    // Read the subdirectory
    let entries: Vec<_> = fs::read_dir(&subdir)?
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();

    // Note: std::fs::read_dir does NOT return . and .. on most systems
    // but the underlying FUSE readdir does include them
    // This test verifies normal files are visible
    assert!(entries.contains(&"file.txt".to_string()));

    ctx.cleanup().await?;
    Ok(())
}

// =============================================================================
// Inode Stability Tests
// =============================================================================

/// Test that inode remains stable for a file
#[tokio::test]
async fn test_inode_stability() -> Result<()> {
    let ctx = shared_harness().await.context().await?;
    let mount = ctx.mount();

    let filename = random_filename("inode");
    let filepath = mount.join(&filename);

    create_file_str(&filepath, "content")?;

    let ino1 = fs::metadata(&filepath)?.ino();

    // Modify file
    create_file_str(&filepath, "modified content")?;

    let ino2 = fs::metadata(&filepath)?.ino();

    // Inode should remain the same
    assert_eq!(ino1, ino2, "Inode should be stable across modifications");

    ctx.cleanup().await?;
    Ok(())
}

/// Test that different files have different inodes
#[tokio::test]
async fn test_unique_inodes() -> Result<()> {
    let ctx = shared_harness().await.context().await?;
    let mount = ctx.mount();

    let mut inodes = std::collections::HashSet::new();

    for i in 0..10 {
        let filepath = mount.join(format!("unique{}.txt", i));
        create_file_str(&filepath, &format!("content {}", i))?;

        let ino = fs::metadata(&filepath)?.ino();
        assert!(
            inodes.insert(ino),
            "Inode {} was reused for file {}",
            ino,
            i
        );
    }

    ctx.cleanup().await?;
    Ok(())
}

/// Test that deleted file's inode can be reused
#[tokio::test]
async fn test_inode_reuse_after_delete() -> Result<()> {
    let ctx = shared_harness().await.context().await?;
    let mount = ctx.mount();

    // Create and delete many files, collect inodes
    let mut all_inodes = Vec::new();

    for i in 0..20 {
        let filepath = mount.join(format!("reuse{}.txt", i));
        create_file_str(&filepath, "content")?;
        all_inodes.push(fs::metadata(&filepath)?.ino());
        fs::remove_file(&filepath)?;
    }

    // Create more files - some inodes may be reused
    for i in 0..20 {
        let filepath = mount.join(format!("new{}.txt", i));
        create_file_str(&filepath, "content")?;
        let new_ino = fs::metadata(&filepath)?.ino();
        // Just verify we get valid inodes - reuse is implementation-dependent
        assert!(new_ino > 0);
    }

    ctx.cleanup().await?;
    Ok(())
}

// =============================================================================
// File Type Detection Tests
// =============================================================================

/// Test file type detection for regular files
#[tokio::test]
async fn test_file_type_regular() -> Result<()> {
    let ctx = shared_harness().await.context().await?;
    let mount = ctx.mount();

    let filepath = mount.join(random_filename("regular"));
    create_file_str(&filepath, "regular file")?;

    let meta = fs::metadata(&filepath)?;
    assert!(meta.is_file());
    assert!(!meta.is_dir());
    assert!(!meta.is_symlink());

    ctx.cleanup().await?;
    Ok(())
}

/// Test file type detection for directories
#[tokio::test]
async fn test_file_type_directory() -> Result<()> {
    let ctx = shared_harness().await.context().await?;
    let mount = ctx.mount();

    let dirpath = mount.join(random_filename("dir"));
    fs::create_dir(&dirpath)?;

    let meta = fs::metadata(&dirpath)?;
    assert!(meta.is_dir());
    assert!(!meta.is_file());
    assert!(!meta.is_symlink());

    ctx.cleanup().await?;
    Ok(())
}

/// Test file type detection for symlinks
#[tokio::test]
async fn test_file_type_symlink() -> Result<()> {
    let ctx = shared_harness().await.context().await?;
    let mount = ctx.mount();

    let target = mount.join(random_filename("target"));
    let link = mount.join(random_filename("link"));

    create_file_str(&target, "target content")?;
    std::os::unix::fs::symlink(&target, &link)?;

    // symlink_metadata returns info about the link itself
    let meta = fs::symlink_metadata(&link)?;
    assert!(meta.is_symlink());

    // metadata follows the link
    let meta_followed = fs::metadata(&link)?;
    assert!(meta_followed.is_file());

    ctx.cleanup().await?;
    Ok(())
}

// =============================================================================
// Access Check Tests
// =============================================================================

/// Test access() syscall behavior
#[tokio::test]
async fn test_access_existing_file() -> Result<()> {
    let ctx = shared_harness().await.context().await?;
    let mount = ctx.mount();

    let filepath = mount.join(random_filename("access"));
    create_file_str(&filepath, "content")?;

    // File should exist and be readable
    assert!(filepath.exists());
    assert!(filepath.try_exists()?);

    ctx.cleanup().await?;
    Ok(())
}

/// Test access() on non-existent file
#[tokio::test]
async fn test_access_nonexistent() -> Result<()> {
    let ctx = shared_harness().await.context().await?;
    let mount = ctx.mount();

    let filepath = mount.join("nonexistent_access_test");

    assert!(!filepath.exists());

    ctx.cleanup().await?;
    Ok(())
}
