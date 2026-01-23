//! S3 CRUD operation tests
//!
//! Tests basic file operations: create, read, update, delete
//!
//! These tests use the shared harness pattern for parallel execution.
//! Each test gets an isolated directory within the shared mount.

mod common;

use anyhow::Result;
use common::*;
use fuse_adapter_e2e::{
    assert_dir_contains, assert_file_content, assert_file_content_str, assert_file_exists,
    assert_file_size, assert_not_exists, random_bytes, random_filename,
};
use std::fs;

/// Test creating and reading a simple text file
#[tokio::test]
async fn test_create_and_read_file() -> Result<()> {
    let ctx = shared_harness().await.context().await?;
    let mount = ctx.mount();

    let filename = random_filename("test");
    let filepath = mount.join(&filename);
    let content = "Hello, World!";

    // Create file
    create_file_str(&filepath, content)?;
    assert_file_exists(&filepath);

    // Read back
    let read_content = read_file_str(&filepath)?;
    assert_eq!(read_content, content);

    ctx.cleanup().await?;
    Ok(())
}

/// Test overwriting a file
#[tokio::test]
async fn test_overwrite_file() -> Result<()> {
    let ctx = shared_harness().await.context().await?;
    let mount = ctx.mount();

    let filename = random_filename("overwrite");
    let filepath = mount.join(&filename);

    // Create initial file
    create_file_str(&filepath, "Initial content")?;
    assert_file_content_str(&filepath, "Initial content");

    // Overwrite
    create_file_str(&filepath, "Updated content")?;
    assert_file_content_str(&filepath, "Updated content");

    ctx.cleanup().await?;
    Ok(())
}

/// Test deleting a file
#[tokio::test]
async fn test_delete_file() -> Result<()> {
    let ctx = shared_harness().await.context().await?;
    let mount = ctx.mount();

    let filename = random_filename("delete");
    let filepath = mount.join(&filename);

    // Create file
    create_file_str(&filepath, "To be deleted")?;
    assert_file_exists(&filepath);

    // Delete
    fs::remove_file(&filepath)?;
    assert_not_exists(&filepath);

    ctx.cleanup().await?;
    Ok(())
}

/// Test creating an empty file
#[tokio::test]
async fn test_empty_file() -> Result<()> {
    let ctx = shared_harness().await.context().await?;
    let mount = ctx.mount();

    let filename = random_filename("empty");
    let filepath = mount.join(&filename);

    // Create empty file
    create_file(&filepath, &[])?;
    assert_file_exists(&filepath);
    assert_file_size(&filepath, 0);
    assert_file_content(&filepath, &[]);

    ctx.cleanup().await?;
    Ok(())
}

/// Test binary content (non-UTF8)
#[tokio::test]
async fn test_binary_content() -> Result<()> {
    let ctx = shared_harness().await.context().await?;
    let mount = ctx.mount();

    let filename = random_filename("binary");
    let filepath = mount.join(&filename);

    // Create file with binary content
    let content = random_bytes(1024);
    create_file(&filepath, &content)?;
    assert_file_exists(&filepath);
    assert_file_content(&filepath, &content);

    ctx.cleanup().await?;
    Ok(())
}

/// Test multiple files in the same directory
#[tokio::test]
async fn test_multiple_files() -> Result<()> {
    let ctx = shared_harness().await.context().await?;
    let mount = ctx.mount();

    let files = vec![
        (random_filename("file1"), "Content 1"),
        (random_filename("file2"), "Content 2"),
        (random_filename("file3"), "Content 3"),
    ];

    // Create all files
    for (name, content) in &files {
        let filepath = mount.join(name);
        create_file_str(&filepath, content)?;
    }

    // Verify all files
    for (name, content) in &files {
        let filepath = mount.join(name);
        assert_file_exists(&filepath);
        assert_file_content_str(&filepath, content);
    }

    // Verify directory listing contains all files
    let file_names: Vec<&str> = files.iter().map(|(n, _)| n.as_str()).collect();
    assert_dir_contains(mount, &file_names);

    ctx.cleanup().await?;
    Ok(())
}

/// Test file size reporting
#[tokio::test]
async fn test_file_size() -> Result<()> {
    let ctx = shared_harness().await.context().await?;
    let mount = ctx.mount();

    let test_cases = vec![
        (0, vec![]),
        (10, random_bytes(10)),
        (100, random_bytes(100)),
        (1000, random_bytes(1000)),
        (10000, random_bytes(10000)),
    ];

    for (expected_size, content) in test_cases {
        let filename = random_filename(&format!("size-{}", expected_size));
        let filepath = mount.join(&filename);

        create_file(&filepath, &content)?;
        assert_file_size(&filepath, expected_size as u64);

        // Clean up this file
        fs::remove_file(&filepath)?;
    }

    ctx.cleanup().await?;
    Ok(())
}

/// Test reading file that was just written (consistency)
#[tokio::test]
async fn test_read_after_write_consistency() -> Result<()> {
    let ctx = shared_harness().await.context().await?;
    let mount = ctx.mount();

    let filename = random_filename("consistency");
    let filepath = mount.join(&filename);

    // Write and immediately read multiple times
    for i in 0..5 {
        let content = format!("Iteration {}", i);
        create_file_str(&filepath, &content)?;

        // Immediate read should see the new content
        let read_content = read_file_str(&filepath)?;
        assert_eq!(read_content, content, "Iteration {} failed", i);
    }

    ctx.cleanup().await?;
    Ok(())
}

/// Test file persistence after sync
#[tokio::test]
async fn test_file_persistence_after_sync() -> Result<()> {
    let ctx = shared_harness().await.context().await?;
    let mount = ctx.mount();

    let filename = random_filename("persist");
    let filepath = mount.join(&filename);
    let content = "Persistent content";

    // Create file
    create_file_str(&filepath, content)?;

    // Force sync to backend
    ctx.force_sync().await?;

    // Verify file is still readable
    assert_file_exists(&filepath);
    assert_file_content_str(&filepath, content);

    // Verify file exists in S3 backend (using context's prefix-aware method)
    let s3_exists = ctx.object_exists(&filename).await?;
    assert!(s3_exists, "File should exist in S3 after sync");

    ctx.cleanup().await?;
    Ok(())
}

/// Test that deleted files are removed from backend after sync
#[tokio::test]
async fn test_delete_syncs_to_backend() -> Result<()> {
    let ctx = shared_harness().await.context().await?;
    let mount = ctx.mount();

    let filename = random_filename("delete-sync");
    let filepath = mount.join(&filename);

    // Create and sync
    create_file_str(&filepath, "To be deleted")?;
    ctx.force_sync().await?;

    // Verify exists in S3
    assert!(ctx.object_exists(&filename).await?);

    // Delete locally
    fs::remove_file(&filepath)?;
    assert_not_exists(&filepath);

    // Sync deletion
    ctx.force_sync().await?;

    // Verify deleted from S3
    assert!(
        !ctx.object_exists(&filename).await?,
        "File should be deleted from S3 after sync"
    );

    ctx.cleanup().await?;
    Ok(())
}

/// Test file with special characters in name (except /)
#[tokio::test]
async fn test_special_characters_in_filename() -> Result<()> {
    let ctx = shared_harness().await.context().await?;
    let mount = ctx.mount();

    let special_names = vec![
        "file with spaces.txt",
        "file-with-dashes.txt",
        "file_with_underscores.txt",
        "file.multiple.dots.txt",
        "file(with)parens.txt",
        "file[with]brackets.txt",
        "file{with}braces.txt",
    ];

    for name in special_names {
        let filepath = mount.join(name);
        let content = format!("Content for {}", name);

        create_file_str(&filepath, &content)?;
        assert_file_exists(&filepath);
        assert_file_content_str(&filepath, &content);

        fs::remove_file(&filepath)?;
    }

    ctx.cleanup().await?;
    Ok(())
}

/// Test unicode filenames
#[tokio::test]
async fn test_unicode_filenames() -> Result<()> {
    let ctx = shared_harness().await.context().await?;
    let mount = ctx.mount();

    let unicode_names = vec![
        "файл.txt",     // Russian
        "文件.txt",     // Chinese
        "ファイル.txt", // Japanese
        "αρχείο.txt",   // Greek
        "emoji-🎉.txt", // Emoji
    ];

    for name in unicode_names {
        let filepath = mount.join(name);
        let content = format!("Content for {}", name);

        create_file_str(&filepath, &content)?;
        assert_file_exists(&filepath);
        assert_file_content_str(&filepath, &content);

        fs::remove_file(&filepath)?;
    }

    ctx.cleanup().await?;
    Ok(())
}

/// Test appending to file (requires full rewrite in S3)
#[tokio::test]
async fn test_append_to_file() -> Result<()> {
    let ctx = shared_harness().await.context().await?;
    let mount = ctx.mount();

    let filename = random_filename("append");
    let filepath = mount.join(&filename);

    // Create initial file
    create_file_str(&filepath, "Initial")?;

    // Append by reading, appending, and rewriting
    let mut content = read_file_str(&filepath)?;
    content.push_str(" + Appended");
    create_file_str(&filepath, &content)?;

    assert_file_content_str(&filepath, "Initial + Appended");

    ctx.cleanup().await?;
    Ok(())
}

/// Test rapid create/delete cycles
#[tokio::test]
async fn test_rapid_create_delete() -> Result<()> {
    let ctx = shared_harness().await.context().await?;
    let mount = ctx.mount();

    for i in 0..10 {
        let filename = format!("rapid-{}.txt", i);
        let filepath = mount.join(&filename);

        // Create
        create_file_str(&filepath, &format!("Content {}", i))?;
        assert_file_exists(&filepath);

        // Delete immediately
        fs::remove_file(&filepath)?;
        assert_not_exists(&filepath);
    }

    ctx.cleanup().await?;
    Ok(())
}

/// Test recreating a deleted file
#[tokio::test]
async fn test_recreate_deleted_file() -> Result<()> {
    let ctx = shared_harness().await.context().await?;
    let mount = ctx.mount();

    let filename = random_filename("recreate");
    let filepath = mount.join(&filename);

    // Create, delete, recreate cycle
    create_file_str(&filepath, "Version 1")?;
    assert_file_content_str(&filepath, "Version 1");

    fs::remove_file(&filepath)?;
    assert_not_exists(&filepath);

    create_file_str(&filepath, "Version 2")?;
    assert_file_content_str(&filepath, "Version 2");

    ctx.cleanup().await?;
    Ok(())
}

/// Test that stat returns correct metadata
#[tokio::test]
async fn test_file_metadata() -> Result<()> {
    let ctx = shared_harness().await.context().await?;
    let mount = ctx.mount();

    let filename = random_filename("metadata");
    let filepath = mount.join(&filename);
    let content = random_bytes(512);

    create_file(&filepath, &content)?;

    let metadata = fs::metadata(&filepath)?;
    assert!(metadata.is_file());
    assert!(!metadata.is_dir());
    assert_eq!(metadata.len(), 512);

    ctx.cleanup().await?;
    Ok(())
}
