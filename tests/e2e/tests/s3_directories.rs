//! S3 directory operation tests
//!
//! Tests directory creation, listing, and removal

mod common;

use anyhow::Result;
use common::*;
use fuse_adapter_e2e::{
    assert_dir_contains, assert_dir_contains_exactly, assert_dir_empty, assert_dir_exists,
    assert_dir_not_exists, assert_file_content_str, assert_file_exists, assert_not_exists,
    random_filename, TestHarness,
};
use std::fs;

/// Test creating a directory
#[tokio::test]
async fn test_create_directory() -> Result<()> {
    let harness = TestHarness::new().await?;
    let mount = harness.mount();

    let dirname = random_filename("dir");
    let dirpath = mount.join(&dirname);

    fs::create_dir(&dirpath)?;
    assert_dir_exists(&dirpath);

    harness.cleanup().await?;
    Ok(())
}

/// Test creating nested directories
#[tokio::test]
async fn test_create_nested_directories() -> Result<()> {
    let harness = TestHarness::new().await?;
    let mount = harness.mount();

    let base = random_filename("nested");
    let path = mount
        .join(&base)
        .join("level1")
        .join("level2")
        .join("level3");

    fs::create_dir_all(&path)?;
    assert_dir_exists(&path);

    // Verify each level exists
    assert_dir_exists(&mount.join(&base));
    assert_dir_exists(&mount.join(&base).join("level1"));
    assert_dir_exists(&mount.join(&base).join("level1").join("level2"));

    harness.cleanup().await?;
    Ok(())
}

/// Test listing directory contents
#[tokio::test]
async fn test_list_directory() -> Result<()> {
    let harness = TestHarness::new().await?;
    let mount = harness.mount();

    let dirname = random_filename("listdir");
    let dirpath = mount.join(&dirname);
    fs::create_dir(&dirpath)?;

    // Create some files and subdirectories
    create_file_str(&dirpath.join("file1.txt"), "content1")?;
    create_file_str(&dirpath.join("file2.txt"), "content2")?;
    fs::create_dir(&dirpath.join("subdir"))?;

    // List and verify
    assert_dir_contains(&dirpath, &["file1.txt", "file2.txt", "subdir"]);

    harness.cleanup().await?;
    Ok(())
}

/// Test removing an empty directory
#[tokio::test]
async fn test_remove_empty_directory() -> Result<()> {
    let harness = TestHarness::new().await?;
    let mount = harness.mount();

    let dirname = random_filename("rmdir");
    let dirpath = mount.join(&dirname);

    fs::create_dir(&dirpath)?;
    assert_dir_exists(&dirpath);

    fs::remove_dir(&dirpath)?;
    assert_not_exists(&dirpath);

    harness.cleanup().await?;
    Ok(())
}

/// Test that removing non-empty directory fails
#[tokio::test]
async fn test_remove_nonempty_directory_fails() -> Result<()> {
    let harness = TestHarness::new().await?;
    let mount = harness.mount();

    let dirname = random_filename("nonempty");
    let dirpath = mount.join(&dirname);

    fs::create_dir(&dirpath)?;
    create_file_str(&dirpath.join("file.txt"), "content")?;

    // Should fail
    let result = fs::remove_dir(&dirpath);
    assert!(result.is_err(), "Should fail to remove non-empty directory");

    // Directory and file should still exist
    assert_dir_exists(&dirpath);
    assert_file_exists(&dirpath.join("file.txt"));

    harness.cleanup().await?;
    Ok(())
}

/// Test removing directory with contents using remove_dir_all
#[tokio::test]
async fn test_remove_dir_all() -> Result<()> {
    let harness = TestHarness::new().await?;
    let mount = harness.mount();

    let dirname = random_filename("rmdirall");
    let dirpath = mount.join(&dirname);

    // Create nested structure
    fs::create_dir_all(&dirpath.join("sub1").join("sub2"))?;
    create_file_str(&dirpath.join("file1.txt"), "content1")?;
    create_file_str(&dirpath.join("sub1").join("file2.txt"), "content2")?;

    // Remove all
    fs::remove_dir_all(&dirpath)?;
    assert_not_exists(&dirpath);

    harness.cleanup().await?;
    Ok(())
}

/// Test empty directory listing
#[tokio::test]
async fn test_empty_directory_listing() -> Result<()> {
    let harness = TestHarness::new().await?;
    let mount = harness.mount();

    let dirname = random_filename("empty");
    let dirpath = mount.join(&dirname);

    fs::create_dir(&dirpath)?;
    assert_dir_empty(&dirpath);

    harness.cleanup().await?;
    Ok(())
}

/// Test creating file in subdirectory
#[tokio::test]
async fn test_file_in_subdirectory() -> Result<()> {
    let harness = TestHarness::new().await?;
    let mount = harness.mount();

    let dirname = random_filename("subdir");
    let dirpath = mount.join(&dirname);
    let filepath = dirpath.join("nested-file.txt");

    fs::create_dir(&dirpath)?;
    create_file_str(&filepath, "nested content")?;

    assert_file_exists(&filepath);
    assert_file_content_str(&filepath, "nested content");

    harness.cleanup().await?;
    Ok(())
}

/// Test deeply nested file operations
#[tokio::test]
async fn test_deeply_nested_file() -> Result<()> {
    let harness = TestHarness::new().await?;
    let mount = harness.mount();

    let base = random_filename("deep");
    let deep_path = mount
        .join(&base)
        .join("a")
        .join("b")
        .join("c")
        .join("d")
        .join("e");

    fs::create_dir_all(&deep_path)?;

    let file_path = deep_path.join("deep-file.txt");
    create_file_str(&file_path, "deep content")?;

    assert_file_exists(&file_path);
    assert_file_content_str(&file_path, "deep content");

    harness.cleanup().await?;
    Ok(())
}

/// Test directory with special characters in name
#[tokio::test]
async fn test_directory_special_characters() -> Result<()> {
    let harness = TestHarness::new().await?;
    let mount = harness.mount();

    let special_dirs = vec![
        "dir with spaces",
        "dir-with-dashes",
        "dir_with_underscores",
        "dir.with.dots",
    ];

    for name in special_dirs {
        let dirpath = mount.join(name);
        fs::create_dir(&dirpath)?;
        assert_dir_exists(&dirpath);

        // Create a file inside
        create_file_str(&dirpath.join("test.txt"), "content")?;
        assert_file_exists(&dirpath.join("test.txt"));

        // Cleanup
        fs::remove_dir_all(&dirpath)?;
    }

    harness.cleanup().await?;
    Ok(())
}

/// Test directory with unicode name
#[tokio::test]
async fn test_directory_unicode() -> Result<()> {
    let harness = TestHarness::new().await?;
    let mount = harness.mount();

    let unicode_dirs = vec!["目录", "каталог", "ディレクトリ"];

    for name in unicode_dirs {
        let dirpath = mount.join(name);
        fs::create_dir(&dirpath)?;
        assert_dir_exists(&dirpath);

        // Create a file inside
        create_file_str(&dirpath.join("test.txt"), "content")?;

        // Cleanup
        fs::remove_dir_all(&dirpath)?;
    }

    harness.cleanup().await?;
    Ok(())
}

/// Test rename directory
#[tokio::test]
async fn test_rename_directory() -> Result<()> {
    let harness = TestHarness::new().await?;
    let mount = harness.mount();

    let old_name = random_filename("old-dir");
    let new_name = random_filename("new-dir");
    let old_path = mount.join(&old_name);
    let new_path = mount.join(&new_name);

    fs::create_dir(&old_path)?;
    create_file_str(&old_path.join("file.txt"), "content")?;

    fs::rename(&old_path, &new_path)?;

    assert_not_exists(&old_path);
    assert_dir_exists(&new_path);
    assert_file_exists(&new_path.join("file.txt"));
    assert_file_content_str(&new_path.join("file.txt"), "content");

    harness.cleanup().await?;
    Ok(())
}

/// Test directory persistence after sync
#[tokio::test]
async fn test_directory_persistence() -> Result<()> {
    let harness = TestHarness::new().await?;
    let mount = harness.mount();

    let dirname = random_filename("persist-dir");
    let dirpath = mount.join(&dirname);

    fs::create_dir(&dirpath)?;
    create_file_str(&dirpath.join("file.txt"), "content")?;

    harness.force_sync().await?;

    // Verify still exists
    assert_dir_exists(&dirpath);
    assert_file_exists(&dirpath.join("file.txt"));

    harness.cleanup().await?;
    Ok(())
}

/// Test many files in directory (pagination)
#[tokio::test]
async fn test_many_files_in_directory() -> Result<()> {
    let harness = TestHarness::new().await?;
    let mount = harness.mount();

    let dirname = random_filename("many-files");
    let dirpath = mount.join(&dirname);
    fs::create_dir(&dirpath)?;

    let file_count = 50; // Not too many for quick tests

    // Create many files
    for i in 0..file_count {
        let filename = format!("file-{:04}.txt", i);
        create_file_str(&dirpath.join(&filename), &format!("content {}", i))?;
    }

    // Verify count
    let entries: Vec<_> = fs::read_dir(&dirpath)?.collect();
    assert_eq!(entries.len(), file_count);

    harness.cleanup().await?;
    Ok(())
}

/// Test interleaved directory and file operations
#[tokio::test]
async fn test_interleaved_operations() -> Result<()> {
    let harness = TestHarness::new().await?;
    let mount = harness.mount();

    let base = random_filename("interleaved");
    let basepath = mount.join(&base);
    fs::create_dir(&basepath)?;

    // Interleave creates and deletes
    for i in 0..10 {
        let dirname = format!("dir-{}", i);
        let dirpath = basepath.join(&dirname);
        fs::create_dir(&dirpath)?;

        let filename = format!("file-{}.txt", i);
        create_file_str(&basepath.join(&filename), &format!("content {}", i))?;

        if i % 2 == 0 {
            fs::remove_dir(&dirpath)?;
        }
    }

    // Verify final state
    let entries: Vec<_> = fs::read_dir(&basepath)?.filter_map(|e| e.ok()).collect();

    // Should have 10 files + 5 directories (odd-numbered ones)
    assert_eq!(entries.len(), 15);

    harness.cleanup().await?;
    Ok(())
}
