//! fuse-adapter: A FUSE filesystem framework with pluggable connector architecture
//!
//! This library provides a framework for exposing various storage backends
//! as mounted filesystems via FUSE (Filesystem in Userspace).
//!
//! # Architecture
//!
//! - **Connectors**: Storage backends (S3, database, etc.) that implement the
//!   `Connector` trait for path-based file operations.
//! - **Cache Layer**: Optional caching decorator that wraps connectors to provide
//!   write buffering, read caching, and metadata caching.
//! - **FUSE Adapter**: Translates FUSE operations to connector method calls,
//!   managing inode mapping and capability checking.
//! - **Mount Manager**: Handles lifecycle of multiple simultaneous mounts.
//!
//! # Example
//!
//! ```no_run
//! use fuse_adapter::config::Config;
//! use fuse_adapter::mount::MountManager;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Load configuration
//! let config = Config::from_file(&"config.yaml".into())?;
//!
//! // Create mount manager with tokio runtime
//! let handle = tokio::runtime::Handle::current();
//! let manager = MountManager::new(handle);
//!
//! // Mount filesystems based on config
//! // ...
//! # Ok(())
//! # }
//! ```

pub mod auth;
pub mod cache;
pub mod config;
pub mod connector;
pub mod env;
pub mod error;
pub mod fuse;
pub mod mount;
pub mod overlay;

pub use error::{FuseAdapterError, Result};
