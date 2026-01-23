//! Symlink operation tests
//!
//! Tests symbolic link creation and handling

mod common;

use anyhow::Result;
use common::*;
use fuse_adapter_e2e::{
    assert_file_content_str, assert_file_exists, assert_is_symlink, assert_not_exists,
    assert_symlink_target, random_filename, TestHarness,
};
use std::fs;
use std::os::unix::fs as unix_fs;

/// Test creating a symlink to a file
#[tokio::test]
async fn test_create_symlink_to_file() -> Result<()> {
    let harness = TestHarness::new().await?;
    let mount = harness.mount();

    let filename = random_filename("target");
    let linkname = random_filename("link");
    let filepath = mount.join(&filename);
    let linkpath = mount.join(&linkname);

    // Create target file
    create_file_str(&filepath, "target content")?;

    // Create symlink
    unix_fs::symlink(&filename, &linkpath)?;

    assert_is_symlink(&linkpath);
    assert_symlink_target(&linkpath, &filename);

    harness.cleanup().await?;
    Ok(())
}

/// Test reading through a symlink
#[tokio::test]
async fn test_read_through_symlink() -> Result<()> {
    let harness = TestHarness::new().await?;
    let mount = harness.mount();

    let filename = random_filename("target");
    let linkname = random_filename("link");
    let filepath = mount.join(&filename);
    let linkpath = mount.join(&linkname);

    // Create target file
    create_file_str(&filepath, "target content")?;

    // Create symlink
    unix_fs::symlink(&filename, &linkpath)?;

    // Read through symlink
    let content = read_file_str(&linkpath)?;
    assert_eq!(content, "target content");

    harness.cleanup().await?;
    Ok(())
}

/// Test writing through a symlink
#[tokio::test]
async fn test_write_through_symlink() -> Result<()> {
    let harness = TestHarness::new().await?;
    let mount = harness.mount();

    let filename = random_filename("target");
    let linkname = random_filename("link");
    let filepath = mount.join(&filename);
    let linkpath = mount.join(&linkname);

    // Create target file
    create_file_str(&filepath, "initial")?;

    // Create symlink
    unix_fs::symlink(&filename, &linkpath)?;

    // Write through symlink
    create_file_str(&linkpath, "updated")?;

    // Verify target was updated
    assert_file_content_str(&filepath, "updated");

    harness.cleanup().await?;
    Ok(())
}

/// Test symlink to directory
#[tokio::test]
async fn test_symlink_to_directory() -> Result<()> {
    let harness = TestHarness::new().await?;
    let mount = harness.mount();

    let dirname = random_filename("targetdir");
    let linkname = random_filename("linkdir");
    let dirpath = mount.join(&dirname);
    let linkpath = mount.join(&linkname);

    // Create target directory with a file
    fs::create_dir(&dirpath)?;
    create_file_str(&dirpath.join("file.txt"), "content")?;

    // Create symlink to directory
    unix_fs::symlink(&dirname, &linkpath)?;

    assert_is_symlink(&linkpath);

    // Access file through symlink
    let content = read_file_str(&linkpath.join("file.txt"))?;
    assert_eq!(content, "content");

    harness.cleanup().await?;
    Ok(())
}

/// Test dangling symlink (target doesn't exist)
#[tokio::test]
async fn test_dangling_symlink() -> Result<()> {
    let harness = TestHarness::new().await?;
    let mount = harness.mount();

    let linkname = random_filename("dangling");
    let linkpath = mount.join(&linkname);

    // Create symlink to non-existent target
    unix_fs::symlink("nonexistent", &linkpath)?;

    assert_is_symlink(&linkpath);
    assert_symlink_target(&linkpath, "nonexistent");

    // Reading should fail
    let result = fs::read(&linkpath);
    assert!(result.is_err());

    harness.cleanup().await?;
    Ok(())
}

/// Test deleting a symlink (not the target)
#[tokio::test]
async fn test_delete_symlink() -> Result<()> {
    let harness = TestHarness::new().await?;
    let mount = harness.mount();

    let filename = random_filename("target");
    let linkname = random_filename("link");
    let filepath = mount.join(&filename);
    let linkpath = mount.join(&linkname);

    create_file_str(&filepath, "content")?;
    unix_fs::symlink(&filename, &linkpath)?;

    // Delete the symlink
    fs::remove_file(&linkpath)?;

    // Symlink should be gone
    assert_not_exists(&linkpath);

    // Target should still exist
    assert_file_exists(&filepath);
    assert_file_content_str(&filepath, "content");

    harness.cleanup().await?;
    Ok(())
}

/// Test symlink with absolute path target
#[tokio::test]
async fn test_symlink_absolute_target() -> Result<()> {
    let harness = TestHarness::new().await?;
    let mount = harness.mount();

    let filename = random_filename("target");
    let linkname = random_filename("link");
    let filepath = mount.join(&filename);
    let linkpath = mount.join(&linkname);

    create_file_str(&filepath, "content")?;

    // Create symlink with absolute path
    unix_fs::symlink(&filepath, &linkpath)?;

    assert_is_symlink(&linkpath);

    // Read through symlink
    let content = read_file_str(&linkpath)?;
    assert_eq!(content, "content");

    harness.cleanup().await?;
    Ok(())
}

/// Test symlink persistence after sync
#[tokio::test]
async fn test_symlink_persistence() -> Result<()> {
    let harness = TestHarness::new().await?;
    let mount = harness.mount();

    let filename = random_filename("target");
    let linkname = random_filename("link");
    let filepath = mount.join(&filename);
    let linkpath = mount.join(&linkname);

    create_file_str(&filepath, "content")?;
    unix_fs::symlink(&filename, &linkpath)?;

    // Force sync
    harness.force_sync().await?;

    // Verify symlink still works
    assert_is_symlink(&linkpath);
    assert_symlink_target(&linkpath, &filename);
    let content = read_file_str(&linkpath)?;
    assert_eq!(content, "content");

    harness.cleanup().await?;
    Ok(())
}

/// Test multiple symlinks to same target
#[tokio::test]
async fn test_multiple_symlinks_same_target() -> Result<()> {
    let harness = TestHarness::new().await?;
    let mount = harness.mount();

    let filename = random_filename("target");
    let filepath = mount.join(&filename);
    create_file_str(&filepath, "content")?;

    // Create multiple symlinks
    let links: Vec<_> = (0..3)
        .map(|i| {
            let linkname = random_filename(&format!("link{}", i));
            let linkpath = mount.join(&linkname);
            unix_fs::symlink(&filename, &linkpath).unwrap();
            linkpath
        })
        .collect();

    // All links should work
    for linkpath in &links {
        assert_is_symlink(linkpath);
        let content = read_file_str(linkpath)?;
        assert_eq!(content, "content");
    }

    harness.cleanup().await?;
    Ok(())
}

/// Test symlink chain (symlink to symlink)
#[tokio::test]
async fn test_symlink_chain() -> Result<()> {
    let harness = TestHarness::new().await?;
    let mount = harness.mount();

    let filename = random_filename("target");
    let link1name = random_filename("link1");
    let link2name = random_filename("link2");
    let filepath = mount.join(&filename);
    let link1path = mount.join(&link1name);
    let link2path = mount.join(&link2name);

    create_file_str(&filepath, "content")?;
    unix_fs::symlink(&filename, &link1path)?;
    unix_fs::symlink(&link1name, &link2path)?;

    // Reading through chain should work
    let content = read_file_str(&link2path)?;
    assert_eq!(content, "content");

    harness.cleanup().await?;
    Ok(())
}

/// Test replacing symlink target
#[tokio::test]
async fn test_replace_symlink() -> Result<()> {
    let harness = TestHarness::new().await?;
    let mount = harness.mount();

    let target1 = random_filename("target1");
    let target2 = random_filename("target2");
    let linkname = random_filename("link");

    let target1path = mount.join(&target1);
    let target2path = mount.join(&target2);
    let linkpath = mount.join(&linkname);

    create_file_str(&target1path, "content1")?;
    create_file_str(&target2path, "content2")?;

    // Create symlink to target1
    unix_fs::symlink(&target1, &linkpath)?;
    assert_file_content_str(&linkpath, "content1");

    // Remove and recreate to target2
    fs::remove_file(&linkpath)?;
    unix_fs::symlink(&target2, &linkpath)?;
    assert_file_content_str(&linkpath, "content2");

    harness.cleanup().await?;
    Ok(())
}

/// Test symlink with special characters in name
#[tokio::test]
async fn test_symlink_special_names() -> Result<()> {
    let harness = TestHarness::new().await?;
    let mount = harness.mount();

    let filename = random_filename("target");
    let filepath = mount.join(&filename);
    create_file_str(&filepath, "content")?;

    let special_links = vec![
        "link with spaces",
        "link-with-dashes",
        "link_with_underscores",
    ];

    for link_name in special_links {
        let linkpath = mount.join(link_name);
        unix_fs::symlink(&filename, &linkpath)?;

        assert_is_symlink(&linkpath);
        let content = read_file_str(&linkpath)?;
        assert_eq!(content, "content");

        fs::remove_file(&linkpath)?;
    }

    harness.cleanup().await?;
    Ok(())
}
