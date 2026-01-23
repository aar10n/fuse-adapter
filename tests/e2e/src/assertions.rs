//! Filesystem assertions for e2e tests
//!
//! Provides assertion functions for verifying filesystem state.

use anyhow::{Context, Result};
use std::fs;
use std::io::Read;
use std::path::Path;
use std::time::Duration;
use tokio::time::{sleep, timeout};

/// Assert that a file exists at the given path
pub fn assert_file_exists(path: &Path) {
    assert!(
        path.exists() && path.is_file(),
        "Expected file to exist at {:?}, but it doesn't or is not a file",
        path
    );
}

/// Assert that a file does not exist
pub fn assert_file_not_exists(path: &Path) {
    assert!(
        !path.exists() || !path.is_file(),
        "Expected file NOT to exist at {:?}, but it does",
        path
    );
}

/// Assert that a path does not exist at all
pub fn assert_not_exists(path: &Path) {
    assert!(
        !path.exists(),
        "Expected path {:?} to not exist, but it does",
        path
    );
}

/// Assert that a directory exists at the given path
pub fn assert_dir_exists(path: &Path) {
    assert!(
        path.exists() && path.is_dir(),
        "Expected directory to exist at {:?}, but it doesn't or is not a directory",
        path
    );
}

/// Assert that a directory does not exist
pub fn assert_dir_not_exists(path: &Path) {
    assert!(
        !path.exists() || !path.is_dir(),
        "Expected directory NOT to exist at {:?}, but it does",
        path
    );
}

/// Assert that a file has the expected content
pub fn assert_file_content(path: &Path, expected: &[u8]) {
    let actual = fs::read(path).expect(&format!("Failed to read file {:?}", path));
    assert_eq!(
        actual,
        expected,
        "File content mismatch at {:?}\nExpected {} bytes, got {} bytes",
        path,
        expected.len(),
        actual.len()
    );
}

/// Assert that a file contains the expected text
pub fn assert_file_content_str(path: &Path, expected: &str) {
    assert_file_content(path, expected.as_bytes());
}

/// Assert that a file has the expected size
pub fn assert_file_size(path: &Path, expected_size: u64) {
    let metadata = fs::metadata(path).expect(&format!("Failed to get metadata for {:?}", path));
    assert_eq!(
        metadata.len(),
        expected_size,
        "File size mismatch at {:?}: expected {} bytes, got {} bytes",
        path,
        expected_size,
        metadata.len()
    );
}

/// Assert that a directory is empty
pub fn assert_dir_empty(path: &Path) {
    let entries: Vec<_> = fs::read_dir(path)
        .expect(&format!("Failed to read directory {:?}", path))
        .collect();
    assert!(
        entries.is_empty(),
        "Expected directory {:?} to be empty, but it contains {} entries: {:?}",
        path,
        entries.len(),
        entries
            .iter()
            .filter_map(|e| e.as_ref().ok().map(|e| e.file_name()))
            .collect::<Vec<_>>()
    );
}

/// Assert that a directory contains the expected entries (exact match)
pub fn assert_dir_contains_exactly(path: &Path, expected: &[&str]) {
    let entries: Vec<String> = fs::read_dir(path)
        .expect(&format!("Failed to read directory {:?}", path))
        .filter_map(|e| e.ok().map(|e| e.file_name().to_string_lossy().to_string()))
        .collect();

    let mut expected_sorted: Vec<&str> = expected.to_vec();
    expected_sorted.sort();
    let mut entries_sorted = entries.clone();
    entries_sorted.sort();

    assert_eq!(
        entries_sorted, expected_sorted,
        "Directory {:?} contents mismatch.\nExpected: {:?}\nGot: {:?}",
        path, expected_sorted, entries_sorted
    );
}

/// Assert that a directory contains at least the expected entries
pub fn assert_dir_contains(path: &Path, expected: &[&str]) {
    let entries: Vec<String> = fs::read_dir(path)
        .expect(&format!("Failed to read directory {:?}", path))
        .filter_map(|e| e.ok().map(|e| e.file_name().to_string_lossy().to_string()))
        .collect();

    for name in expected {
        assert!(
            entries.iter().any(|e| e == *name),
            "Expected directory {:?} to contain {:?}, but it only has: {:?}",
            path,
            name,
            entries
        );
    }
}

/// Assert that a directory has exactly N entries
pub fn assert_dir_entry_count(path: &Path, expected_count: usize) {
    let entries: Vec<_> = fs::read_dir(path)
        .expect(&format!("Failed to read directory {:?}", path))
        .collect();
    assert_eq!(
        entries.len(),
        expected_count,
        "Directory {:?} entry count mismatch: expected {}, got {}",
        path,
        expected_count,
        entries.len()
    );
}

/// Assert that a path is a symlink
pub fn assert_is_symlink(path: &Path) {
    let metadata =
        fs::symlink_metadata(path).expect(&format!("Failed to get metadata for {:?}", path));
    assert!(
        metadata.file_type().is_symlink(),
        "Expected {:?} to be a symlink, but it's not",
        path
    );
}

/// Assert that a symlink points to the expected target
pub fn assert_symlink_target(path: &Path, expected_target: &str) {
    assert_is_symlink(path);
    let target = fs::read_link(path).expect(&format!("Failed to read symlink {:?}", path));
    assert_eq!(
        target.to_string_lossy(),
        expected_target,
        "Symlink {:?} target mismatch: expected {:?}, got {:?}",
        path,
        expected_target,
        target
    );
}

/// Assert file mode (Unix only)
#[cfg(unix)]
pub fn assert_file_mode(path: &Path, expected_mode: u32) {
    use std::os::unix::fs::PermissionsExt;

    let metadata = fs::metadata(path).expect(&format!("Failed to get metadata for {:?}", path));
    let actual_mode = metadata.permissions().mode() & 0o7777; // Mask to get permission bits only
    let expected_masked = expected_mode & 0o7777;
    assert_eq!(
        actual_mode, expected_masked,
        "File mode mismatch at {:?}: expected {:o}, got {:o}",
        path, expected_masked, actual_mode
    );
}

#[cfg(not(unix))]
pub fn assert_file_mode(_path: &Path, _expected_mode: u32) {
    // No-op on non-Unix platforms
}

/// Assert file owner UID (Unix only)
#[cfg(unix)]
pub fn assert_file_uid(path: &Path, expected_uid: u32) {
    use std::os::unix::fs::MetadataExt;

    let metadata = fs::metadata(path).expect(&format!("Failed to get metadata for {:?}", path));
    assert_eq!(
        metadata.uid(),
        expected_uid,
        "File UID mismatch at {:?}: expected {}, got {}",
        path,
        expected_uid,
        metadata.uid()
    );
}

#[cfg(not(unix))]
pub fn assert_file_uid(_path: &Path, _expected_uid: u32) {
    // No-op on non-Unix platforms
}

/// Assert file owner GID (Unix only)
#[cfg(unix)]
pub fn assert_file_gid(path: &Path, expected_gid: u32) {
    use std::os::unix::fs::MetadataExt;

    let metadata = fs::metadata(path).expect(&format!("Failed to get metadata for {:?}", path));
    assert_eq!(
        metadata.gid(),
        expected_gid,
        "File GID mismatch at {:?}: expected {}, got {}",
        path,
        expected_gid,
        metadata.gid()
    );
}

#[cfg(not(unix))]
pub fn assert_file_gid(_path: &Path, _expected_gid: u32) {
    // No-op on non-Unix platforms
}

/// Retry an assertion until it succeeds or times out
///
/// Useful for eventual consistency scenarios like cache sync.
pub async fn assert_eventually<F, E>(f: F, timeout_duration: Duration) -> Result<()>
where
    F: Fn() -> Result<(), E>,
    E: std::fmt::Debug,
{
    let poll_interval = Duration::from_millis(100);
    let mut last_error = None;

    timeout(timeout_duration, async {
        loop {
            match f() {
                Ok(()) => return Ok::<_, anyhow::Error>(()),
                Err(e) => {
                    last_error = Some(format!("{:?}", e));
                    sleep(poll_interval).await;
                }
            }
        }
    })
    .await
    .map_err(|_| {
        anyhow::anyhow!(
            "Assertion did not succeed within {:?}. Last error: {:?}",
            timeout_duration,
            last_error
        )
    })??;

    Ok(())
}

/// Retry a fallible operation until it succeeds or times out
pub async fn retry_until_ok<F, T, E>(f: F, timeout_duration: Duration) -> Result<T>
where
    F: Fn() -> Result<T, E>,
    E: std::fmt::Debug,
{
    let poll_interval = Duration::from_millis(100);
    let mut last_error = None;

    timeout(timeout_duration, async {
        loop {
            match f() {
                Ok(v) => return Ok::<_, anyhow::Error>(v),
                Err(e) => {
                    last_error = Some(format!("{:?}", e));
                    sleep(poll_interval).await;
                }
            }
        }
    })
    .await
    .map_err(|_| {
        anyhow::anyhow!(
            "Operation did not succeed within {:?}. Last error: {:?}",
            timeout_duration,
            last_error
        )
    })?
}

/// Generate random bytes of the specified size
pub fn random_bytes(size: usize) -> Vec<u8> {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    (0..size).map(|_| rng.gen()).collect()
}

/// Generate a random filename with the given prefix
pub fn random_filename(prefix: &str) -> String {
    format!("{}-{}", prefix, uuid::Uuid::new_v4())
}

/// Calculate SHA-256 hash of data
pub fn sha256(data: &[u8]) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    // Simple hash for testing (not cryptographic)
    let mut hasher = DefaultHasher::new();
    data.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

/// Verify file integrity by comparing content hash
pub fn verify_file_integrity(path: &Path, expected_hash: &str) {
    let content = fs::read(path).expect(&format!("Failed to read file {:?}", path));
    let actual_hash = sha256(&content);
    assert_eq!(
        actual_hash, expected_hash,
        "File integrity check failed at {:?}: expected hash {}, got {}",
        path, expected_hash, actual_hash
    );
}
