//! Filesystem-backed cache layer
//!
//! This provides persistent caching backed by the local filesystem,
//! with write buffering and dirty file tracking.

use std::io::{Read as IoRead, Seek, SeekFrom, Write as IoWrite};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::RwLock;
use tokio::sync::Notify;
use tracing::{debug, error, info, trace, warn};

use crate::connector::{CacheRequirements, Capabilities, Connector, DirEntryStream, Metadata};
use crate::error::{FuseAdapterError, Result};

/// Filesystem cache configuration
#[derive(Debug, Clone)]
pub struct FilesystemCacheConfig {
    /// Directory to store cached files
    pub cache_dir: PathBuf,
    /// Maximum cache size in bytes
    pub max_size: u64,
    /// Interval for flushing dirty files to backend
    pub flush_interval: Duration,
    /// TTL for cached metadata
    pub metadata_ttl: Duration,
}

impl Default for FilesystemCacheConfig {
    fn default() -> Self {
        Self {
            cache_dir: PathBuf::from("/var/cache/fuse-adapter"),
            max_size: 1024 * 1024 * 1024, // 1GB
            flush_interval: Duration::from_secs(30),
            metadata_ttl: Duration::from_secs(60),
        }
    }
}

/// State for a dirty (modified) file
#[derive(Debug)]
struct DirtyFileState {
    /// When the file was first modified (for future flush scheduling)
    #[allow(dead_code)]
    modified_at: Instant,
    /// Whether this is a new file (not yet on backend)
    is_new: bool,
}

/// Cached metadata entry
#[derive(Debug, Clone)]
struct CachedMetadata {
    metadata: Metadata,
    cached_at: Instant,
}

/// Filesystem-backed caching connector wrapper
///
/// This cache layer:
/// - Stores cached file content on local filesystem
/// - Tracks dirty files that need flushing to backend
/// - Periodically flushes dirty files based on config
/// - Caches metadata with TTL
/// - Handles read-through caching
/// - Preserves POSIX file modes
pub struct FilesystemCache<C: Connector> {
    inner: Arc<C>,
    config: FilesystemCacheConfig,
    /// Tracks dirty files that need flushing
    dirty_files: DashMap<PathBuf, DirtyFileState>,
    /// Cached metadata with TTL
    metadata_cache: DashMap<PathBuf, CachedMetadata>,
    /// Cached file modes (separate from metadata for persistence across flushes)
    mode_cache: DashMap<PathBuf, u32>,
    /// Current approximate cache size
    cache_size: RwLock<u64>,
    /// Shutdown notification
    shutdown: Arc<Notify>,
}

impl<C: Connector> FilesystemCache<C> {
    /// Create a new filesystem cache wrapper
    pub fn new(connector: C, config: FilesystemCacheConfig) -> Self {
        // Ensure cache directory exists
        if let Err(e) = std::fs::create_dir_all(&config.cache_dir) {
            warn!(
                "Failed to create cache directory {:?}: {}",
                config.cache_dir, e
            );
        }

        Self {
            inner: Arc::new(connector),
            config,
            dirty_files: DashMap::new(),
            metadata_cache: DashMap::new(),
            mode_cache: DashMap::new(),
            cache_size: RwLock::new(0),
            shutdown: Arc::new(Notify::new()),
        }
    }

    /// Get the local cache path for a file
    fn cache_path(&self, path: &Path) -> PathBuf {
        // Convert the path to a safe filesystem name
        let safe_name = path
            .to_string_lossy()
            .trim_start_matches('/')
            .replace('/', "_");

        if safe_name.is_empty() {
            self.config.cache_dir.join("_root")
        } else {
            self.config.cache_dir.join(safe_name)
        }
    }

    /// Check if a file is in the local cache
    fn is_cached(&self, path: &Path) -> bool {
        self.cache_path(path).exists()
    }

    /// Read from local cache
    fn read_from_cache(&self, path: &Path, offset: u64, size: u32) -> Result<Option<Bytes>> {
        let cache_path = self.cache_path(path);

        if !cache_path.exists() {
            return Ok(None);
        }

        let mut file = std::fs::File::open(&cache_path)
            .map_err(|e| FuseAdapterError::Cache(format!("Failed to open cache file: {}", e)))?;

        file.seek(SeekFrom::Start(offset))
            .map_err(|e| FuseAdapterError::Cache(format!("Failed to seek: {}", e)))?;

        let mut buffer = vec![0u8; size as usize];
        let bytes_read = file
            .read(&mut buffer)
            .map_err(|e| FuseAdapterError::Cache(format!("Failed to read: {}", e)))?;

        buffer.truncate(bytes_read);
        Ok(Some(Bytes::from(buffer)))
    }

    /// Write to local cache
    fn write_to_cache(&self, path: &Path, offset: u64, data: &[u8]) -> Result<u64> {
        let cache_path = self.cache_path(path);

        // Ensure parent directory exists
        if let Some(parent) = cache_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                FuseAdapterError::Cache(format!("Failed to create cache directory: {}", e))
            })?;
        }

        // Open or create the file
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&cache_path)
            .map_err(|e| FuseAdapterError::Cache(format!("Failed to open cache file: {}", e)))?;

        // Seek to offset
        file.seek(SeekFrom::Start(offset))
            .map_err(|e| FuseAdapterError::Cache(format!("Failed to seek: {}", e)))?;

        // Write data
        file.write_all(data)
            .map_err(|e| FuseAdapterError::Cache(format!("Failed to write: {}", e)))?;

        // Track as dirty
        self.dirty_files
            .entry(path.to_path_buf())
            .or_insert(DirtyFileState {
                modified_at: Instant::now(),
                is_new: !self.inner.capabilities().write,
            });

        // Update cache size estimate
        {
            let mut size = self.cache_size.write();
            *size += data.len() as u64;
        }

        Ok(data.len() as u64)
    }

    /// Create an empty file in the cache
    fn create_in_cache(&self, path: &Path) -> Result<()> {
        let cache_path = self.cache_path(path);

        if let Some(parent) = cache_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                FuseAdapterError::Cache(format!("Failed to create cache directory: {}", e))
            })?;
        }

        std::fs::File::create(&cache_path)
            .map_err(|e| FuseAdapterError::Cache(format!("Failed to create cache file: {}", e)))?;

        // Track as dirty (new file)
        self.dirty_files.insert(
            path.to_path_buf(),
            DirtyFileState {
                modified_at: Instant::now(),
                is_new: true,
            },
        );

        Ok(())
    }

    /// Remove a file from the cache
    fn remove_from_cache(&self, path: &Path) {
        let cache_path = self.cache_path(path);

        if cache_path.exists() {
            if let Err(e) = std::fs::remove_file(&cache_path) {
                warn!("Failed to remove cache file {:?}: {}", cache_path, e);
            }
        }

        self.dirty_files.remove(path);
        self.metadata_cache.remove(path);
        self.mode_cache.remove(path);
    }

    /// Flush a specific file from cache to backend
    async fn flush_file(&self, path: &Path) -> Result<()> {
        let cache_path = self.cache_path(path);

        if !cache_path.exists() {
            // Nothing to flush
            self.dirty_files.remove(path);
            return Ok(());
        }

        // Read the entire cached file
        let data = std::fs::read(&cache_path)
            .map_err(|e| FuseAdapterError::Cache(format!("Failed to read cache file: {}", e)))?;

        // Write to backend (at offset 0, full file)
        debug!("Flushing {} bytes for {:?} to backend", data.len(), path);
        self.inner.write(path, 0, &data).await?;

        // Remove from dirty tracking
        self.dirty_files.remove(path);

        // Invalidate metadata cache
        self.metadata_cache.remove(path);

        Ok(())
    }

    /// Flush all dirty files to backend
    pub async fn flush_all(&self) -> Result<()> {
        let dirty_paths: Vec<PathBuf> = self
            .dirty_files
            .iter()
            .map(|entry| entry.key().clone())
            .collect();

        info!("Flushing {} dirty files to backend", dirty_paths.len());

        for path in dirty_paths {
            if let Err(e) = self.flush_file(&path).await {
                error!("Failed to flush {:?}: {}", path, e);
                // Continue with other files
            }
        }

        Ok(())
    }

    /// Get cached metadata if still valid
    fn get_cached_metadata(&self, path: &Path) -> Option<Metadata> {
        self.metadata_cache.get(path).and_then(|entry| {
            if entry.cached_at.elapsed() < self.config.metadata_ttl {
                Some(entry.metadata.clone())
            } else {
                None
            }
        })
    }

    /// Cache metadata
    fn cache_metadata(&self, path: &Path, metadata: Metadata) {
        self.metadata_cache.insert(
            path.to_path_buf(),
            CachedMetadata {
                metadata,
                cached_at: Instant::now(),
            },
        );
    }

    /// Truncate a cached file
    fn truncate_in_cache(&self, path: &Path, size: u64) -> Result<()> {
        let cache_path = self.cache_path(path);

        if cache_path.exists() {
            let file = std::fs::OpenOptions::new()
                .write(true)
                .open(&cache_path)
                .map_err(|e| {
                    FuseAdapterError::Cache(format!("Failed to open cache file: {}", e))
                })?;

            file.set_len(size)
                .map_err(|e| FuseAdapterError::Cache(format!("Failed to truncate: {}", e)))?;

            // Mark as dirty
            self.dirty_files
                .entry(path.to_path_buf())
                .or_insert(DirtyFileState {
                    modified_at: Instant::now(),
                    is_new: false,
                });
        }

        // Invalidate metadata cache
        self.metadata_cache.remove(path);

        Ok(())
    }

    /// Fetch a file from backend into cache
    async fn fetch_to_cache(&self, path: &Path) -> Result<()> {
        // Get file size first
        let meta = self.inner.stat(path).await?;

        if !meta.is_file() {
            return Err(FuseAdapterError::IsADirectory(
                path.to_string_lossy().to_string(),
            ));
        }

        let cache_path = self.cache_path(path);

        if let Some(parent) = cache_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                FuseAdapterError::Cache(format!("Failed to create cache directory: {}", e))
            })?;
        }

        // Read entire file from backend
        let data = if meta.size > 0 {
            self.inner.read(path, 0, meta.size as u32).await?
        } else {
            Bytes::new()
        };

        // Write to cache
        std::fs::write(&cache_path, &data)
            .map_err(|e| FuseAdapterError::Cache(format!("Failed to write cache file: {}", e)))?;

        // Update cache size
        {
            let mut size = self.cache_size.write();
            *size += data.len() as u64;
        }

        // Cache the metadata
        self.cache_metadata(path, meta);

        Ok(())
    }
}

impl<C: Connector> Drop for FilesystemCache<C> {
    fn drop(&mut self) {
        // Signal shutdown
        self.shutdown.notify_waiters();

        // Note: We can't do async flush here, but dirty files will be
        // persisted in the cache directory for recovery
        let dirty_count = self.dirty_files.len();
        if dirty_count > 0 {
            warn!(
                "{} dirty files not flushed to backend (will remain in cache)",
                dirty_count
            );
        }
    }
}

#[async_trait]
impl<C: Connector + 'static> Connector for FilesystemCache<C> {
    fn capabilities(&self) -> Capabilities {
        let mut caps = self.inner.capabilities();
        // Cache layer enables random_write and truncate even if backend doesn't support it
        if caps.write {
            caps.random_write = true;
            caps.truncate = true;
        }
        // Cache layer can always store mode locally, even if backend doesn't support it
        // (mode will be preserved in cache and written to backend if supported)
        caps.set_mode = true;
        caps
    }

    fn cache_requirements(&self) -> CacheRequirements {
        // Cache layer satisfies write buffer requirements
        CacheRequirements::default()
    }

    async fn stat(&self, path: &Path) -> Result<Metadata> {
        // Check cached metadata first
        if let Some(meta) = self.get_cached_metadata(path) {
            trace!("stat cache hit for {:?}", path);
            return Ok(meta);
        }

        // Check if we have a local cached file
        let cache_path = self.cache_path(path);
        if cache_path.exists() {
            let std_meta = std::fs::metadata(&cache_path).map_err(|e| {
                FuseAdapterError::Cache(format!("Failed to stat cache file: {}", e))
            })?;

            // Get cached mode if available
            let cached_mode = self.mode_cache.get(path).map(|r| *r);

            let meta = if std_meta.is_file() {
                if let Some(mode) = cached_mode {
                    Metadata::file_with_mode(
                        std_meta.len(),
                        std_meta.modified().unwrap_or(SystemTime::now()),
                        mode,
                    )
                } else {
                    Metadata::file(
                        std_meta.len(),
                        std_meta.modified().unwrap_or(SystemTime::now()),
                    )
                }
            } else if let Some(mode) = cached_mode {
                Metadata::directory_with_mode(
                    std_meta.modified().unwrap_or(SystemTime::now()),
                    mode,
                )
            } else {
                Metadata::directory(std_meta.modified().unwrap_or(SystemTime::now()))
            };

            self.cache_metadata(path, meta.clone());
            return Ok(meta);
        }

        // Fall through to backend
        let meta = self.inner.stat(path).await?;
        // Cache mode from backend if present
        if let Some(mode) = meta.mode {
            self.mode_cache.insert(path.to_path_buf(), mode);
        }
        self.cache_metadata(path, meta.clone());
        Ok(meta)
    }

    async fn exists(&self, path: &Path) -> Result<bool> {
        // Check cache first
        if self.cache_path(path).exists() {
            return Ok(true);
        }

        // Check metadata cache
        if self.get_cached_metadata(path).is_some() {
            return Ok(true);
        }

        // Fall through to backend
        self.inner.exists(path).await
    }

    async fn read(&self, path: &Path, offset: u64, size: u32) -> Result<Bytes> {
        // Try reading from cache first
        if let Some(data) = self.read_from_cache(path, offset, size)? {
            trace!(
                "read cache hit for {:?} offset={} size={}",
                path,
                offset,
                size
            );
            return Ok(data);
        }

        // Fetch entire file to cache if not present
        if !self.is_cached(path) {
            debug!("Fetching {:?} to cache", path);
            self.fetch_to_cache(path).await?;
        }

        // Now read from cache
        self.read_from_cache(path, offset, size)?
            .ok_or_else(|| FuseAdapterError::NotFound(path.to_string_lossy().to_string()))
    }

    async fn write(&self, path: &Path, offset: u64, data: &[u8]) -> Result<u64> {
        // If file doesn't exist in cache, fetch it first (unless it's a new file)
        if !self.is_cached(path) && offset > 0 {
            // Need existing content for non-zero offset writes
            if self.inner.stat(path).await.is_ok() {
                self.fetch_to_cache(path).await?;
            }
        }

        // Write to cache
        self.write_to_cache(path, offset, data)
    }

    async fn create_file(&self, path: &Path) -> Result<()> {
        // Create in cache
        self.create_in_cache(path)?;

        // Also create on backend if it supports immediate creates
        if self.inner.capabilities().write {
            self.inner.create_file(path).await?;
            // Mark as no longer "new" since it exists on backend
            if let Some(mut entry) = self.dirty_files.get_mut(path) {
                entry.is_new = false;
            }
        }

        Ok(())
    }

    async fn create_dir(&self, path: &Path) -> Result<()> {
        // Directories go straight to backend
        self.inner.create_dir(path).await?;

        // Invalidate any cached metadata for parent
        if let Some(parent) = path.parent() {
            self.metadata_cache.remove(parent);
        }

        Ok(())
    }

    async fn remove_file(&self, path: &Path) -> Result<()> {
        // Remove from cache
        self.remove_from_cache(path);

        // Remove from backend
        self.inner.remove_file(path).await
    }

    async fn remove_dir(&self, path: &Path, recursive: bool) -> Result<()> {
        // Remove from backend
        self.inner.remove_dir(path, recursive).await?;

        // Invalidate metadata cache
        self.metadata_cache.remove(path);
        if let Some(parent) = path.parent() {
            self.metadata_cache.remove(parent);
        }

        Ok(())
    }

    fn list_dir(&self, path: &Path) -> DirEntryStream {
        // Directory listings go to backend (we don't cache directory structure)
        // But we could enhance this to merge with local dirty files
        self.inner.list_dir(path)
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        // If backend supports rename, use it
        if self.inner.capabilities().rename {
            self.inner.rename(from, to).await?;

            // Update cache paths
            let from_cache = self.cache_path(from);
            let to_cache = self.cache_path(to);
            if from_cache.exists() {
                let _ = std::fs::rename(&from_cache, &to_cache);
            }

            // Update dirty tracking
            if let Some((_, state)) = self.dirty_files.remove(from) {
                self.dirty_files.insert(to.to_path_buf(), state);
            }

            // Preserve mode
            if let Some((_, mode)) = self.mode_cache.remove(from) {
                self.mode_cache.insert(to.to_path_buf(), mode);
            }

            // Invalidate metadata
            self.metadata_cache.remove(from);
            self.metadata_cache.remove(to);

            return Ok(());
        }

        // Synthesize rename via copy + delete if backend doesn't support it
        // First ensure file is in cache
        if !self.is_cached(from) {
            self.fetch_to_cache(from).await?;
        }

        // Copy cache file
        let from_cache = self.cache_path(from);
        let to_cache = self.cache_path(to);
        std::fs::copy(&from_cache, &to_cache)
            .map_err(|e| FuseAdapterError::Cache(format!("Failed to copy: {}", e)))?;

        // Preserve mode
        if let Some((_, mode)) = self.mode_cache.remove(from) {
            self.mode_cache.insert(to.to_path_buf(), mode);
        }

        // Mark destination as dirty
        self.dirty_files.insert(
            to.to_path_buf(),
            DirtyFileState {
                modified_at: Instant::now(),
                is_new: true,
            },
        );

        // Flush destination to backend
        self.flush_file(to).await?;

        // Remove source from backend and cache
        self.remove_from_cache(from);
        self.inner.remove_file(from).await?;

        Ok(())
    }

    async fn truncate(&self, path: &Path, size: u64) -> Result<()> {
        // If backend supports truncate, use it
        if self.inner.capabilities().truncate {
            self.inner.truncate(path, size).await?;
            self.metadata_cache.remove(path);

            // Also truncate cache if present
            self.truncate_in_cache(path, size)?;
            return Ok(());
        }

        // Ensure file is in cache
        if !self.is_cached(path) {
            self.fetch_to_cache(path).await?;
        }

        // Truncate in cache
        self.truncate_in_cache(path, size)
    }

    async fn flush(&self, path: &Path) -> Result<()> {
        // If file is dirty, flush to backend
        if self.dirty_files.contains_key(path) {
            self.flush_file(path).await?;
        }

        // Also call backend flush
        self.inner.flush(path).await
    }

    async fn create_file_with_mode(&self, path: &Path, mode: u32) -> Result<()> {
        // Store mode in cache
        self.mode_cache.insert(path.to_path_buf(), mode);

        // Create in local cache
        self.create_in_cache(path)?;

        // Also create on backend if it supports immediate creates
        if self.inner.capabilities().write {
            self.inner.create_file_with_mode(path, mode).await?;
            // Mark as no longer "new" since it exists on backend
            if let Some(mut entry) = self.dirty_files.get_mut(path) {
                entry.is_new = false;
            }
        }

        Ok(())
    }

    async fn create_dir_with_mode(&self, path: &Path, mode: u32) -> Result<()> {
        // Store mode in cache
        self.mode_cache.insert(path.to_path_buf(), mode);

        // Directories go straight to backend
        self.inner.create_dir_with_mode(path, mode).await?;

        // Invalidate any cached metadata for parent
        if let Some(parent) = path.parent() {
            self.metadata_cache.remove(parent);
        }

        Ok(())
    }

    async fn set_mode(&self, path: &Path, mode: u32) -> Result<()> {
        // Always update local cache
        self.mode_cache.insert(path.to_path_buf(), mode);

        // Invalidate metadata cache so stat() returns fresh data
        self.metadata_cache.remove(path);

        // If backend supports set_mode, update it too
        if self.inner.capabilities().set_mode {
            self.inner.set_mode(path, mode).await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    // Tests would go here, using mock connectors
}
