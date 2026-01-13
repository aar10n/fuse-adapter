use std::io;
use thiserror::Error;

/// Main error type for fuse-adapter operations
#[derive(Error, Debug)]
pub enum FuseAdapterError {
    #[error("Path not found: {0}")]
    NotFound(String),

    #[error("Path already exists: {0}")]
    AlreadyExists(String),

    #[error("Not a directory: {0}")]
    NotADirectory(String),

    #[error("Is a directory: {0}")]
    IsADirectory(String),

    #[error("Directory not empty: {0}")]
    NotEmpty(String),

    #[error("Invalid path: {0}")]
    InvalidPath(String),

    #[error("Operation not supported: {0}")]
    NotSupported(String),

    #[error("Read-only filesystem")]
    ReadOnly,

    #[error("Permission denied")]
    PermissionDenied,

    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("Backend error: {0}")]
    Backend(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Cache error: {0}")]
    Cache(String),

    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    #[error("File too large")]
    FileTooLarge,

    #[error("No space left")]
    NoSpace,

    #[error("Name too long: {0}")]
    NameTooLong(String),

    #[error("Operation interrupted")]
    Interrupted,
}

impl FuseAdapterError {
    /// Convert error to libc errno for FUSE responses
    pub fn to_errno(&self) -> i32 {
        match self {
            FuseAdapterError::NotFound(_) => libc::ENOENT,
            FuseAdapterError::AlreadyExists(_) => libc::EEXIST,
            FuseAdapterError::NotADirectory(_) => libc::ENOTDIR,
            FuseAdapterError::IsADirectory(_) => libc::EISDIR,
            FuseAdapterError::NotEmpty(_) => libc::ENOTEMPTY,
            FuseAdapterError::InvalidPath(_) => libc::EINVAL,
            FuseAdapterError::NotSupported(_) => libc::ENOSYS,
            FuseAdapterError::ReadOnly => libc::EROFS,
            FuseAdapterError::PermissionDenied => libc::EACCES,
            FuseAdapterError::Io(e) => e.raw_os_error().unwrap_or(libc::EIO),
            FuseAdapterError::Backend(_) => libc::EIO,
            FuseAdapterError::Config(_) => libc::EINVAL,
            FuseAdapterError::Cache(_) => libc::EIO,
            FuseAdapterError::InvalidArgument(_) => libc::EINVAL,
            FuseAdapterError::FileTooLarge => libc::EFBIG,
            FuseAdapterError::NoSpace => libc::ENOSPC,
            FuseAdapterError::NameTooLong(_) => libc::ENAMETOOLONG,
            FuseAdapterError::Interrupted => libc::EINTR,
        }
    }
}

/// Result type alias for fuse-adapter operations
pub type Result<T> = std::result::Result<T, FuseAdapterError>;
