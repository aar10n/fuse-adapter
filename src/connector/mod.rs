pub mod s3;

use std::ffi::OsString;
use std::path::Path;
use std::pin::Pin;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;

use crate::error::Result;

/// File type enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileType {
    File,
    Directory,
}

/// Metadata for a file or directory
#[derive(Debug, Clone)]
pub struct Metadata {
    pub file_type: FileType,
    pub size: u64,
    pub mtime: SystemTime,
}

impl Metadata {
    pub fn file(size: u64, mtime: SystemTime) -> Self {
        Self {
            file_type: FileType::File,
            size,
            mtime,
        }
    }

    pub fn directory(mtime: SystemTime) -> Self {
        Self {
            file_type: FileType::Directory,
            size: 0,
            mtime,
        }
    }

    pub fn is_file(&self) -> bool {
        matches!(self.file_type, FileType::File)
    }

    pub fn is_dir(&self) -> bool {
        matches!(self.file_type, FileType::Directory)
    }
}

/// Directory entry returned by list_dir
#[derive(Debug, Clone)]
pub struct DirEntry {
    pub name: OsString,
    pub file_type: FileType,
}

impl DirEntry {
    pub fn file(name: impl Into<OsString>) -> Self {
        Self {
            name: name.into(),
            file_type: FileType::File,
        }
    }

    pub fn directory(name: impl Into<OsString>) -> Self {
        Self {
            name: name.into(),
            file_type: FileType::Directory,
        }
    }
}

/// Connector capabilities declaration
#[derive(Debug, Clone, Default)]
pub struct Capabilities {
    /// Can read files
    pub read: bool,
    /// Can write files (false = read-only)
    pub write: bool,
    /// Supports byte-range reads
    pub range_read: bool,
    /// Can write at arbitrary offsets (S3 cannot)
    pub random_write: bool,
    /// Native rename support
    pub rename: bool,
    /// Native truncate support
    pub truncate: bool,
    /// Can update modification time
    pub set_mtime: bool,
    /// Random access is cheap (hint)
    pub seekable: bool,
}

impl Capabilities {
    /// Full read-write capabilities
    pub fn full() -> Self {
        Self {
            read: true,
            write: true,
            range_read: true,
            random_write: true,
            rename: true,
            truncate: true,
            set_mtime: true,
            seekable: true,
        }
    }

    /// Read-only capabilities
    pub fn read_only() -> Self {
        Self {
            read: true,
            write: false,
            range_read: true,
            random_write: false,
            rename: false,
            truncate: false,
            set_mtime: false,
            seekable: true,
        }
    }
}

/// Cache requirement levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CacheRequirement {
    /// No caching needed
    #[default]
    None,
    /// Caching recommended for performance
    Recommended,
    /// Caching required for correct operation
    Required,
}

/// Cache requirements for a connector
#[derive(Debug, Clone, Default)]
pub struct CacheRequirements {
    /// Write buffer requirement (needed for connectors without random_write)
    pub write_buffer: CacheRequirement,
    /// Whether read caching is beneficial
    pub read_cache: bool,
    /// Suggested metadata cache TTL
    pub metadata_cache_ttl: Option<Duration>,
}

/// Stream type for directory listings
pub type DirEntryStream = Pin<Box<dyn Stream<Item = Result<DirEntry>> + Send>>;

/// Core connector trait for storage backends
///
/// Connectors are stateless and path-based. Each operation receives
/// a path and performs the requested action. The framework handles
/// inode mapping and capability checking.
#[async_trait]
pub trait Connector: Send + Sync {
    /// Get connector capabilities
    fn capabilities(&self) -> Capabilities;

    /// Get cache requirements
    fn cache_requirements(&self) -> CacheRequirements {
        CacheRequirements::default()
    }

    /// Get metadata for a path
    async fn stat(&self, path: &Path) -> Result<Metadata>;

    /// Check if a path exists
    ///
    /// Default implementation uses stat()
    async fn exists(&self, path: &Path) -> Result<bool> {
        match self.stat(path).await {
            Ok(_) => Ok(true),
            Err(crate::error::FuseAdapterError::NotFound(_)) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Read bytes from a file
    ///
    /// # Arguments
    /// * `path` - Path to the file
    /// * `offset` - Byte offset to start reading from
    /// * `size` - Number of bytes to read
    async fn read(&self, path: &Path, offset: u64, size: u32) -> Result<Bytes>;

    /// Write bytes to a file
    ///
    /// # Arguments
    /// * `path` - Path to the file
    /// * `offset` - Byte offset to start writing at
    /// * `data` - Data to write
    ///
    /// # Returns
    /// Number of bytes written
    async fn write(&self, path: &Path, offset: u64, data: &[u8]) -> Result<u64>;

    /// Create an empty file
    async fn create_file(&self, path: &Path) -> Result<()>;

    /// Create a directory
    async fn create_dir(&self, path: &Path) -> Result<()>;

    /// Remove a file
    async fn remove_file(&self, path: &Path) -> Result<()>;

    /// Remove a directory
    ///
    /// # Arguments
    /// * `path` - Path to the directory
    /// * `recursive` - If false, directory must be empty
    async fn remove_dir(&self, path: &Path, recursive: bool) -> Result<()>;

    /// List directory contents as a stream
    fn list_dir(&self, path: &Path) -> DirEntryStream;

    /// Rename/move a file or directory
    async fn rename(&self, from: &Path, to: &Path) -> Result<()>;

    /// Truncate a file to the specified size
    async fn truncate(&self, path: &Path, size: u64) -> Result<()>;

    /// Flush pending writes for a file
    async fn flush(&self, path: &Path) -> Result<()>;
}
