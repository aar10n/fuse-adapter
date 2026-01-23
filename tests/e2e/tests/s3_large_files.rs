//! Large file operation tests
//!
//! Tests handling of large files and data integrity

mod common;

use anyhow::Result;
use common::*;
use fuse_adapter_e2e::{
    assert_file_content, assert_file_exists, assert_file_size, random_bytes, random_filename,
    sha256, TestHarness,
};
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom, Write};

/// Test 1MB file
#[tokio::test]
async fn test_1mb_file() -> Result<()> {
    let harness = TestHarness::new().await?;
    let mount = harness.mount();

    let filename = random_filename("1mb");
    let filepath = mount.join(&filename);
    let size = 1024 * 1024; // 1MB
    let content = random_bytes(size);
    let expected_hash = sha256(&content);

    create_file(&filepath, &content)?;
    assert_file_exists(&filepath);
    assert_file_size(&filepath, size as u64);

    // Verify content integrity
    let read_content = read_file(&filepath)?;
    let actual_hash = sha256(&read_content);
    assert_eq!(expected_hash, actual_hash, "1MB file content mismatch");

    harness.cleanup().await?;
    Ok(())
}

/// Test 10MB file
#[tokio::test]
async fn test_10mb_file() -> Result<()> {
    let harness = TestHarness::new().await?;
    let mount = harness.mount();

    let filename = random_filename("10mb");
    let filepath = mount.join(&filename);
    let size = 10 * 1024 * 1024; // 10MB
    let content = random_bytes(size);
    let expected_hash = sha256(&content);

    create_file(&filepath, &content)?;
    assert_file_exists(&filepath);
    assert_file_size(&filepath, size as u64);

    // Verify content integrity
    let read_content = read_file(&filepath)?;
    let actual_hash = sha256(&read_content);
    assert_eq!(expected_hash, actual_hash, "10MB file content mismatch");

    harness.cleanup().await?;
    Ok(())
}

/// Test partial read (range read)
#[tokio::test]
async fn test_partial_read() -> Result<()> {
    let harness = TestHarness::new().await?;
    let mount = harness.mount();

    let filename = random_filename("partial");
    let filepath = mount.join(&filename);
    let size = 1024 * 1024; // 1MB
    let content = random_bytes(size);

    create_file(&filepath, &content)?;

    // Read from the middle
    let mut file = File::open(&filepath)?;
    file.seek(SeekFrom::Start(512 * 1024))?; // Seek to 512KB

    let mut buffer = vec![0u8; 1024];
    file.read_exact(&mut buffer)?;

    // Verify partial read content
    assert_eq!(buffer, &content[512 * 1024..512 * 1024 + 1024]);

    harness.cleanup().await?;
    Ok(())
}

/// Test reading at various offsets
#[tokio::test]
async fn test_read_at_offsets() -> Result<()> {
    let harness = TestHarness::new().await?;
    let mount = harness.mount();

    let filename = random_filename("offsets");
    let filepath = mount.join(&filename);
    let size = 100 * 1024; // 100KB
    let content = random_bytes(size);

    create_file(&filepath, &content)?;

    // Test various offsets
    let offsets = vec![0, 1, 100, 1000, 10000, 50000, 99000];
    let read_size = 1000;

    for offset in offsets {
        let mut file = File::open(&filepath)?;
        file.seek(SeekFrom::Start(offset as u64))?;

        let actual_read_size = std::cmp::min(read_size, size - offset);
        let mut buffer = vec![0u8; actual_read_size];
        let bytes_read = file.read(&mut buffer)?;

        assert_eq!(bytes_read, actual_read_size);
        assert_eq!(buffer, &content[offset..offset + actual_read_size]);
    }

    harness.cleanup().await?;
    Ok(())
}

/// Test sequential write in chunks
#[tokio::test]
async fn test_chunked_write() -> Result<()> {
    let harness = TestHarness::new().await?;
    let mount = harness.mount();

    let filename = random_filename("chunked");
    let filepath = mount.join(&filename);

    // Build content in chunks
    let chunk_size = 64 * 1024; // 64KB chunks
    let num_chunks = 16; // 1MB total
    let mut full_content = Vec::with_capacity(chunk_size * num_chunks);

    let mut file = File::create(&filepath)?;
    for i in 0..num_chunks {
        let chunk = random_bytes(chunk_size);
        file.write_all(&chunk)?;
        full_content.extend_from_slice(&chunk);
    }
    file.flush()?;
    drop(file);

    // Verify
    assert_file_size(&filepath, (chunk_size * num_chunks) as u64);
    let read_content = read_file(&filepath)?;
    assert_eq!(sha256(&read_content), sha256(&full_content));

    harness.cleanup().await?;
    Ok(())
}

/// Test overwriting large file
#[tokio::test]
async fn test_overwrite_large_file() -> Result<()> {
    let harness = TestHarness::new().await?;
    let mount = harness.mount();

    let filename = random_filename("overwrite-large");
    let filepath = mount.join(&filename);

    // Create 2MB file
    let content1 = random_bytes(2 * 1024 * 1024);
    create_file(&filepath, &content1)?;
    assert_file_size(&filepath, 2 * 1024 * 1024);

    // Overwrite with 1MB file
    let content2 = random_bytes(1 * 1024 * 1024);
    create_file(&filepath, &content2)?;
    assert_file_size(&filepath, 1 * 1024 * 1024);
    assert_file_content(&filepath, &content2);

    harness.cleanup().await?;
    Ok(())
}

/// Test file with exact power-of-2 sizes
#[tokio::test]
async fn test_power_of_2_sizes() -> Result<()> {
    let harness = TestHarness::new().await?;
    let mount = harness.mount();

    let sizes = vec![
        1024,         // 1KB
        4096,         // 4KB (page size)
        65536,        // 64KB
        131072,       // 128KB
        262144,       // 256KB
        524288,       // 512KB
    ];

    for size in sizes {
        let filename = random_filename(&format!("pow2-{}", size));
        let filepath = mount.join(&filename);
        let content = random_bytes(size);

        create_file(&filepath, &content)?;
        assert_file_size(&filepath, size as u64);
        assert_file_content(&filepath, &content);

        fs::remove_file(&filepath)?;
    }

    harness.cleanup().await?;
    Ok(())
}

/// Test file with sizes just off power-of-2
#[tokio::test]
async fn test_off_by_one_sizes() -> Result<()> {
    let harness = TestHarness::new().await?;
    let mount = harness.mount();

    let sizes = vec![
        4095,   // 4KB - 1
        4097,   // 4KB + 1
        65535,  // 64KB - 1
        65537,  // 64KB + 1
    ];

    for size in sizes {
        let filename = random_filename(&format!("offby1-{}", size));
        let filepath = mount.join(&filename);
        let content = random_bytes(size);

        create_file(&filepath, &content)?;
        assert_file_size(&filepath, size as u64);
        assert_file_content(&filepath, &content);

        fs::remove_file(&filepath)?;
    }

    harness.cleanup().await?;
    Ok(())
}

/// Test multiple large files concurrently
#[tokio::test]
async fn test_concurrent_large_files() -> Result<()> {
    let harness = TestHarness::new().await?;
    let mount = harness.mount().to_path_buf();

    let file_size = 512 * 1024; // 512KB each
    let file_count = 5;

    // Create files concurrently
    let handles: Vec<_> = (0..file_count)
        .map(|i| {
            let mount = mount.clone();
            std::thread::spawn(move || {
                let filename = format!("concurrent-{}.bin", i);
                let filepath = mount.join(&filename);
                let content = random_bytes(file_size);
                let hash = sha256(&content);
                fs::write(&filepath, &content).unwrap();
                (filepath, hash)
            })
        })
        .collect();

    // Wait for all to complete and verify
    for handle in handles {
        let (filepath, expected_hash) = handle.join().unwrap();
        let content = fs::read(&filepath)?;
        let actual_hash = sha256(&content);
        assert_eq!(expected_hash, actual_hash);
    }

    harness.cleanup().await?;
    Ok(())
}

/// Test large file persistence through sync
#[tokio::test]
async fn test_large_file_persistence() -> Result<()> {
    let harness = TestHarness::new().await?;
    let mount = harness.mount();

    let filename = random_filename("persist-large");
    let filepath = mount.join(&filename);
    let size = 2 * 1024 * 1024; // 2MB
    let content = random_bytes(size);
    let expected_hash = sha256(&content);

    create_file(&filepath, &content)?;

    // Force sync
    harness.force_sync().await?;

    // Verify via S3
    let s3_content = harness.bucket().get_object(&filename).await?;
    let s3_hash = sha256(&s3_content);
    assert_eq!(expected_hash, s3_hash, "S3 content should match");

    harness.cleanup().await?;
    Ok(())
}

/// Test reading entire file vs chunk reading
#[tokio::test]
async fn test_read_strategies() -> Result<()> {
    let harness = TestHarness::new().await?;
    let mount = harness.mount();

    let filename = random_filename("read-strategy");
    let filepath = mount.join(&filename);
    let size = 256 * 1024; // 256KB
    let content = random_bytes(size);

    create_file(&filepath, &content)?;

    // Strategy 1: Read entire file at once
    let full_read = fs::read(&filepath)?;
    assert_eq!(full_read, content);

    // Strategy 2: Read in small chunks
    let mut file = File::open(&filepath)?;
    let mut chunk_read = Vec::with_capacity(size);
    let mut buffer = [0u8; 4096];
    loop {
        let bytes_read = file.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }
        chunk_read.extend_from_slice(&buffer[..bytes_read]);
    }
    assert_eq!(chunk_read, content);

    harness.cleanup().await?;
    Ok(())
}

/// Test sparse-like access pattern (read beginning, middle, end)
#[tokio::test]
async fn test_sparse_access() -> Result<()> {
    let harness = TestHarness::new().await?;
    let mount = harness.mount();

    let filename = random_filename("sparse");
    let filepath = mount.join(&filename);
    let size = 1024 * 1024; // 1MB
    let content = random_bytes(size);

    create_file(&filepath, &content)?;

    let mut file = File::open(&filepath)?;

    // Read beginning
    let mut buf = vec![0u8; 1000];
    file.read_exact(&mut buf)?;
    assert_eq!(buf, &content[0..1000]);

    // Read middle
    file.seek(SeekFrom::Start(500_000))?;
    file.read_exact(&mut buf)?;
    assert_eq!(buf, &content[500_000..501_000]);

    // Read end
    file.seek(SeekFrom::End(-1000))?;
    file.read_exact(&mut buf)?;
    assert_eq!(buf, &content[size - 1000..]);

    harness.cleanup().await?;
    Ok(())
}
