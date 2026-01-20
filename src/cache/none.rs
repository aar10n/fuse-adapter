//! Passthrough (no caching) layer
//!
//! This is a simple wrapper that provides no caching - all operations
//! are passed directly to the underlying connector.

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;

use crate::connector::{CacheRequirements, Capabilities, Connector, DirEntryStream, Metadata};
use crate::error::Result;

/// Passthrough connector that provides no caching
pub struct NoCache<C: Connector> {
    inner: Arc<C>,
}

impl<C: Connector> NoCache<C> {
    pub fn new(connector: C) -> Self {
        Self {
            inner: Arc::new(connector),
        }
    }

    pub fn from_arc(connector: Arc<C>) -> Self {
        Self { inner: connector }
    }
}

#[async_trait]
impl<C: Connector + 'static> Connector for NoCache<C> {
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }

    fn cache_requirements(&self) -> CacheRequirements {
        self.inner.cache_requirements()
    }

    async fn stat(&self, path: &Path) -> Result<Metadata> {
        self.inner.stat(path).await
    }

    async fn exists(&self, path: &Path) -> Result<bool> {
        self.inner.exists(path).await
    }

    async fn read(&self, path: &Path, offset: u64, size: u32) -> Result<Bytes> {
        self.inner.read(path, offset, size).await
    }

    async fn write(&self, path: &Path, offset: u64, data: &[u8]) -> Result<u64> {
        self.inner.write(path, offset, data).await
    }

    async fn create_file(&self, path: &Path) -> Result<()> {
        self.inner.create_file(path).await
    }

    async fn create_dir(&self, path: &Path) -> Result<()> {
        self.inner.create_dir(path).await
    }

    async fn remove_file(&self, path: &Path) -> Result<()> {
        self.inner.remove_file(path).await
    }

    async fn remove_dir(&self, path: &Path, recursive: bool) -> Result<()> {
        self.inner.remove_dir(path, recursive).await
    }

    fn list_dir(&self, path: &Path) -> DirEntryStream {
        self.inner.list_dir(path)
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.rename(from, to).await
    }

    async fn truncate(&self, path: &Path, size: u64) -> Result<()> {
        self.inner.truncate(path, size).await
    }

    async fn flush(&self, path: &Path) -> Result<()> {
        self.inner.flush(path).await
    }

    async fn create_file_with_mode(&self, path: &Path, mode: u32) -> Result<()> {
        self.inner.create_file_with_mode(path, mode).await
    }

    async fn create_dir_with_mode(&self, path: &Path, mode: u32) -> Result<()> {
        self.inner.create_dir_with_mode(path, mode).await
    }

    async fn set_mode(&self, path: &Path, mode: u32) -> Result<()> {
        self.inner.set_mode(path, mode).await
    }
}
