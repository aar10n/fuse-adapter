//! Filesystem-backed write-back cache layer
//!
//! This provides a write-back cache where all operations happen locally first,
//! and changes are synchronized to the backend periodically based on flush_interval.
//! This design makes operations near-disk-speed rather than network-bound.

use std::collections::HashSet;
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

use crate::connector::{
    CacheRequirements, Capabilities, Connector, DirEntry, DirEntryStream, FileType, Metadata,
};
use crate::error::{FuseAdapterError, Result};

/// Filesystem cache configuration
#[derive(Debug, Clone)]
pub struct FilesystemCacheConfig {
    /// Directory to store cached files
    pub cache_dir: PathBuf,
    /// Maximum cache size in bytes
    pub max_size: u64,
    /// Interval for syncing dirty state to backend
    pub flush_interval: Duration,
    /// TTL for cached metadata from backend
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

/// Type of pending change
#[derive(Debug, Clone)]
enum PendingChangeType {
    /// New file created locally
    NewFile,
    /// File content modified
    ModifiedFile,
    /// File deleted locally
    DeletedFile,
    /// New directory created locally
    NewDirectory,
    /// Directory deleted locally
    DeletedDirectory,
    /// New symlink created locally
    NewSymlink { target: PathBuf },
}

/// A pending change that needs to be synced to backend
#[derive(Debug, Clone)]
struct PendingChange {
    change_type: PendingChangeType,
    created_at: Instant,
    /// File mode if applicable
    mode: Option<u32>,
}

/// Cached metadata entry
#[derive(Debug, Clone)]
struct CachedMetadata {
    metadata: Metadata,
    cached_at: Instant,
}

/// Cached directory listing entry (from backend)
#[derive(Debug, Clone)]
struct CachedDirListing {
    entries: Vec<DirEntry>,
    cached_at: Instant,
}

/// Negative cache entry (path known not to exist)
#[derive(Debug, Clone)]
struct NegativeCacheEntry {
    cached_at: Instant,
}

/// Filesystem-backed write-back caching connector wrapper
///
/// This cache layer:
/// - All mutations happen locally first (near-disk-speed)
/// - Background task syncs dirty state to backend periodically
/// - Stores cached file content on local filesystem
/// - Tracks all pending changes (creates, deletes, renames, symlinks)
/// - Caches metadata and directory listings with TTL
/// - Preserves POSIX file modes
pub struct FilesystemCache<C: Connector> {
    inner: Arc<C>,
    config: FilesystemCacheConfig,
    /// Pending changes that need to be synced to backend
    pending_changes: DashMap<PathBuf, PendingChange>,
    /// Cached metadata with TTL (from backend, for paths without pending changes)
    metadata_cache: DashMap<PathBuf, CachedMetadata>,
    /// Cached file modes (separate from metadata for persistence)
    mode_cache: DashMap<PathBuf, u32>,
    /// Cached directory listings from backend (merged with pending changes at read time)
    dir_cache: DashMap<PathBuf, CachedDirListing>,
    /// Negative cache: paths known not to exist on backend
    negative_cache: DashMap<PathBuf, NegativeCacheEntry>,
    /// Current approximate cache size
    cache_size: RwLock<u64>,
    /// Shutdown notification for background sync task
    shutdown: Arc<Notify>,
    /// Flag to track if background sync is running
    sync_running: Arc<RwLock<bool>>,
}

impl<C: Connector + 'static> FilesystemCache<C> {
    /// Create a new filesystem cache wrapper
    pub fn new(connector: C, config: FilesystemCacheConfig) -> Self {
        // Ensure cache directory exists
        if let Err(e) = std::fs::create_dir_all(&config.cache_dir) {
            warn!(
                "Failed to create cache directory {:?}: {}",
                config.cache_dir, e
            );
        }

        let cache = Self {
            inner: Arc::new(connector),
            config,
            pending_changes: DashMap::new(),
            metadata_cache: DashMap::new(),
            mode_cache: DashMap::new(),
            dir_cache: DashMap::new(),
            negative_cache: DashMap::new(),
            cache_size: RwLock::new(0),
            shutdown: Arc::new(Notify::new()),
            sync_running: Arc::new(RwLock::new(false)),
        };

        cache
    }

    /// Start the background sync task
    /// This should be called after the cache is wrapped in an Arc
    pub fn start_background_sync(self: &Arc<Self>) {
        let cache = Arc::clone(self);
        let flush_interval = cache.config.flush_interval;
        let shutdown = Arc::clone(&cache.shutdown);

        tokio::spawn(async move {
            info!(
                "Background sync task started with interval {:?}",
                flush_interval
            );

            loop {
                tokio::select! {
                    _ = tokio::time::sleep(flush_interval) => {
                        if let Err(e) = cache.sync_to_backend().await {
                            error!("Background sync failed: {}", e);
                        }
                    }
                    _ = shutdown.notified() => {
                        info!("Background sync task shutting down");
                        // Final sync before shutdown
                        if let Err(e) = cache.sync_to_backend().await {
                            error!("Final sync failed: {}", e);
                        }
                        break;
                    }
                }
            }
        });
    }

    /// Get the local cache path for a file
    fn cache_path(&self, path: &Path) -> PathBuf {
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

    /// Get the path for storing symlink target
    fn symlink_meta_path(&self, path: &Path) -> PathBuf {
        let safe_name = path
            .to_string_lossy()
            .trim_start_matches('/')
            .replace('/', "_");

        self.config.cache_dir.join(format!("{}.symlink", safe_name))
    }

    /// Check if a file is in the local cache
    fn is_cached(&self, path: &Path) -> bool {
        self.cache_path(path).exists()
    }

    /// Check if path has a pending delete
    fn is_pending_delete(&self, path: &Path) -> bool {
        self.pending_changes.get(path).map_or(false, |change| {
            matches!(
                change.change_type,
                PendingChangeType::DeletedFile | PendingChangeType::DeletedDirectory
            )
        })
    }

    /// Check if path has a pending create (file, dir, or symlink)
    fn is_pending_create(&self, path: &Path) -> bool {
        self.pending_changes.get(path).map_or(false, |change| {
            matches!(
                change.change_type,
                PendingChangeType::NewFile
                    | PendingChangeType::NewDirectory
                    | PendingChangeType::NewSymlink { .. }
            )
        })
    }

    /// Check if any ancestor of path is a pending new directory.
    /// If so, the path cannot exist on the backend.
    fn has_pending_new_ancestor(&self, path: &Path) -> bool {
        let mut current = path.parent();
        while let Some(parent) = current {
            if self.pending_changes.get(parent).map_or(false, |c| {
                matches!(c.change_type, PendingChangeType::NewDirectory)
            }) {
                return true;
            }
            current = parent.parent();
        }
        false
    }

    /// Check if path is in negative cache (known not to exist on backend)
    fn is_negative_cached(&self, path: &Path) -> bool {
        self.negative_cache.get(path).map_or(false, |entry| {
            entry.cached_at.elapsed() < self.config.metadata_ttl
        })
    }

    /// Add path to negative cache
    fn add_to_negative_cache(&self, path: &Path) {
        self.negative_cache.insert(
            path.to_path_buf(),
            NegativeCacheEntry {
                cached_at: Instant::now(),
            },
        );
    }

    /// Remove path from negative cache (e.g., when it's created)
    fn remove_from_negative_cache(&self, path: &Path) {
        self.negative_cache.remove(path);
    }

    /// Read from local cache
    fn read_from_cache(&self, path: &Path, offset: u64, size: u32) -> Result<Option<Bytes>> {
        // Check for pending delete
        if self.is_pending_delete(path) {
            return Err(FuseAdapterError::NotFound(
                path.to_string_lossy().to_string(),
            ));
        }

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

        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&cache_path)
            .map_err(|e| FuseAdapterError::Cache(format!("Failed to open cache file: {}", e)))?;

        file.seek(SeekFrom::Start(offset))
            .map_err(|e| FuseAdapterError::Cache(format!("Failed to seek: {}", e)))?;

        file.write_all(data)
            .map_err(|e| FuseAdapterError::Cache(format!("Failed to write: {}", e)))?;

        // Mark as modified (or keep as new if it was new)
        self.pending_changes
            .entry(path.to_path_buf())
            .and_modify(|change| {
                if !matches!(change.change_type, PendingChangeType::NewFile) {
                    change.change_type = PendingChangeType::ModifiedFile;
                }
            })
            .or_insert(PendingChange {
                change_type: PendingChangeType::ModifiedFile,
                created_at: Instant::now(),
                mode: None,
            });

        // Invalidate metadata cache
        self.metadata_cache.remove(path);

        // Update cache size estimate
        {
            let mut size = self.cache_size.write();
            *size += data.len() as u64;
        }

        Ok(data.len() as u64)
    }

    /// Create an empty file in the cache (local only)
    fn create_in_cache(&self, path: &Path, mode: Option<u32>) -> Result<()> {
        let cache_path = self.cache_path(path);

        if let Some(parent) = cache_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                FuseAdapterError::Cache(format!("Failed to create cache directory: {}", e))
            })?;
        }

        std::fs::File::create(&cache_path)
            .map_err(|e| FuseAdapterError::Cache(format!("Failed to create cache file: {}", e)))?;

        // Track as pending new file
        self.pending_changes.insert(
            path.to_path_buf(),
            PendingChange {
                change_type: PendingChangeType::NewFile,
                created_at: Instant::now(),
                mode,
            },
        );

        // Store mode if provided
        if let Some(m) = mode {
            self.mode_cache.insert(path.to_path_buf(), m);
        }

        // Remove from negative cache (it now exists)
        self.remove_from_negative_cache(path);

        // Invalidate parent directory cache
        if let Some(parent) = path.parent() {
            self.dir_cache.remove(parent);
        }

        Ok(())
    }

    /// Create a directory locally
    fn create_dir_in_cache(&self, path: &Path, mode: Option<u32>) -> Result<()> {
        // We don't create actual directories in cache - we just track the pending change
        // and create a marker file to help with stat()
        let cache_path = self.cache_path(path);

        if let Some(parent) = cache_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                FuseAdapterError::Cache(format!("Failed to create cache directory: {}", e))
            })?;
        }

        // Create a marker directory in the cache
        std::fs::create_dir_all(&cache_path).map_err(|e| {
            FuseAdapterError::Cache(format!("Failed to create cache directory marker: {}", e))
        })?;

        // Track as pending new directory
        self.pending_changes.insert(
            path.to_path_buf(),
            PendingChange {
                change_type: PendingChangeType::NewDirectory,
                created_at: Instant::now(),
                mode,
            },
        );

        // Store mode if provided
        if let Some(m) = mode {
            self.mode_cache.insert(path.to_path_buf(), m);
        }

        // Remove from negative cache (it now exists)
        self.remove_from_negative_cache(path);

        // Invalidate parent directory cache
        if let Some(parent) = path.parent() {
            self.dir_cache.remove(parent);
        }

        Ok(())
    }

    /// Create a symlink locally
    fn create_symlink_in_cache(&self, target: &Path, link_path: &Path) -> Result<()> {
        // Store symlink target in a metadata file
        let meta_path = self.symlink_meta_path(link_path);

        if let Some(parent) = meta_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                FuseAdapterError::Cache(format!("Failed to create cache directory: {}", e))
            })?;
        }

        std::fs::write(&meta_path, target.to_string_lossy().as_bytes()).map_err(|e| {
            FuseAdapterError::Cache(format!("Failed to write symlink metadata: {}", e))
        })?;

        // Track as pending new symlink
        self.pending_changes.insert(
            link_path.to_path_buf(),
            PendingChange {
                change_type: PendingChangeType::NewSymlink {
                    target: target.to_path_buf(),
                },
                created_at: Instant::now(),
                mode: None,
            },
        );

        // Remove from negative cache (it now exists)
        self.remove_from_negative_cache(link_path);

        // Invalidate parent directory cache
        if let Some(parent) = link_path.parent() {
            self.dir_cache.remove(parent);
        }

        Ok(())
    }

    /// Read symlink target from local cache
    fn read_symlink_from_cache(&self, path: &Path) -> Result<Option<PathBuf>> {
        let meta_path = self.symlink_meta_path(path);

        if !meta_path.exists() {
            return Ok(None);
        }

        let target = std::fs::read_to_string(&meta_path).map_err(|e| {
            FuseAdapterError::Cache(format!("Failed to read symlink metadata: {}", e))
        })?;

        Ok(Some(PathBuf::from(target)))
    }

    /// Mark a file as deleted locally
    fn mark_deleted(&self, path: &Path, is_dir: bool) {
        // Remove from local cache
        let cache_path = self.cache_path(path);
        if cache_path.exists() {
            if is_dir {
                let _ = std::fs::remove_dir_all(&cache_path);
            } else {
                let _ = std::fs::remove_file(&cache_path);
            }
        }

        // Remove symlink metadata if present
        let meta_path = self.symlink_meta_path(path);
        if meta_path.exists() {
            let _ = std::fs::remove_file(&meta_path);
        }

        // Check if this was a pending new item - if so, just remove the pending change
        if let Some(change) = self.pending_changes.get(path) {
            if matches!(
                change.change_type,
                PendingChangeType::NewFile
                    | PendingChangeType::NewDirectory
                    | PendingChangeType::NewSymlink { .. }
            ) {
                // It was created locally but never synced - just remove it
                drop(change);
                self.pending_changes.remove(path);
                self.metadata_cache.remove(path);
                self.mode_cache.remove(path);

                // Invalidate parent directory cache
                if let Some(parent) = path.parent() {
                    self.dir_cache.remove(parent);
                }
                return;
            }
        }

        // Mark as pending delete
        let change_type = if is_dir {
            PendingChangeType::DeletedDirectory
        } else {
            PendingChangeType::DeletedFile
        };

        self.pending_changes.insert(
            path.to_path_buf(),
            PendingChange {
                change_type,
                created_at: Instant::now(),
                mode: None,
            },
        );

        self.metadata_cache.remove(path);
        self.mode_cache.remove(path);

        // Invalidate parent directory cache
        if let Some(parent) = path.parent() {
            self.dir_cache.remove(parent);
        }
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

            // Mark as modified
            self.pending_changes
                .entry(path.to_path_buf())
                .and_modify(|change| {
                    if !matches!(change.change_type, PendingChangeType::NewFile) {
                        change.change_type = PendingChangeType::ModifiedFile;
                    }
                })
                .or_insert(PendingChange {
                    change_type: PendingChangeType::ModifiedFile,
                    created_at: Instant::now(),
                    mode: None,
                });
        }

        self.metadata_cache.remove(path);

        Ok(())
    }

    /// Fetch a file from backend into cache
    async fn fetch_to_cache(&self, path: &Path) -> Result<()> {
        // Don't fetch if pending delete
        if self.is_pending_delete(path) {
            return Err(FuseAdapterError::NotFound(
                path.to_string_lossy().to_string(),
            ));
        }

        // Get file metadata first
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

        // Cache the metadata and mode
        if let Some(mode) = meta.mode {
            self.mode_cache.insert(path.to_path_buf(), mode);
        }
        self.cache_metadata(path, meta);

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

    /// Get metadata for a pending change
    fn get_pending_metadata(&self, path: &Path) -> Option<Metadata> {
        let change = self.pending_changes.get(path)?;
        let mode = self.mode_cache.get(path).map(|r| *r);
        let now = SystemTime::now();

        match &change.change_type {
            PendingChangeType::NewFile | PendingChangeType::ModifiedFile => {
                let cache_path = self.cache_path(path);
                let size = std::fs::metadata(&cache_path).map(|m| m.len()).unwrap_or(0);
                if let Some(m) = mode {
                    Some(Metadata::file_with_mode(size, now, m))
                } else {
                    Some(Metadata::file(size, now))
                }
            }
            PendingChangeType::NewDirectory => {
                if let Some(m) = mode {
                    Some(Metadata::directory_with_mode(now, m))
                } else {
                    Some(Metadata::directory(now))
                }
            }
            PendingChangeType::NewSymlink { .. } => Some(Metadata::symlink(now)),
            PendingChangeType::DeletedFile | PendingChangeType::DeletedDirectory => None,
        }
    }

    /// Sync all pending changes to backend
    pub async fn sync_to_backend(&self) -> Result<()> {
        // Prevent concurrent syncs
        {
            let mut running = self.sync_running.write();
            if *running {
                debug!("Sync already in progress, skipping");
                return Ok(());
            }
            *running = true;
        }

        // Ensure we release the lock when done
        let _guard = scopeguard::guard((), |_| {
            *self.sync_running.write() = false;
        });

        let pending: Vec<(PathBuf, PendingChange)> = self
            .pending_changes
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect();

        if pending.is_empty() {
            trace!("No pending changes to sync");
            return Ok(());
        }

        info!("Syncing {} pending changes to backend", pending.len());

        // Sort to process directories before files (for creates) and files before directories (for deletes)
        let mut creates: Vec<_> = pending
            .iter()
            .filter(|(_, c)| {
                matches!(
                    c.change_type,
                    PendingChangeType::NewFile
                        | PendingChangeType::ModifiedFile
                        | PendingChangeType::NewDirectory
                        | PendingChangeType::NewSymlink { .. }
                )
            })
            .collect();
        let mut deletes: Vec<_> = pending
            .iter()
            .filter(|(_, c)| {
                matches!(
                    c.change_type,
                    PendingChangeType::DeletedFile | PendingChangeType::DeletedDirectory
                )
            })
            .collect();

        // Sort creates: directories first, then by path depth
        creates.sort_by(|(a, _), (b, _)| a.components().count().cmp(&b.components().count()));

        // Sort deletes: files first, then directories in reverse depth order
        deletes.sort_by(|(a, ca), (b, cb)| {
            let a_is_dir = matches!(ca.change_type, PendingChangeType::DeletedDirectory);
            let b_is_dir = matches!(cb.change_type, PendingChangeType::DeletedDirectory);
            match (a_is_dir, b_is_dir) {
                (false, true) => std::cmp::Ordering::Less,
                (true, false) => std::cmp::Ordering::Greater,
                _ => b.components().count().cmp(&a.components().count()),
            }
        });

        // Process creates
        for (path, change) in creates {
            match &change.change_type {
                PendingChangeType::NewDirectory => {
                    debug!("Syncing new directory: {:?}", path);
                    if let Some(mode) = change.mode {
                        if let Err(e) = self.inner.create_dir_with_mode(path, mode).await {
                            error!("Failed to sync directory {:?}: {}", path, e);
                            continue;
                        }
                    } else if let Err(e) = self.inner.create_dir(path).await {
                        error!("Failed to sync directory {:?}: {}", path, e);
                        continue;
                    }
                    self.pending_changes.remove(path);
                }
                PendingChangeType::NewSymlink { target } => {
                    debug!("Syncing new symlink: {:?} -> {:?}", path, target);
                    if let Err(e) = self.inner.symlink(target, path).await {
                        error!("Failed to sync symlink {:?}: {}", path, e);
                        continue;
                    }
                    // Remove the local symlink metadata file
                    let meta_path = self.symlink_meta_path(path);
                    let _ = std::fs::remove_file(&meta_path);
                    self.pending_changes.remove(path);
                }
                PendingChangeType::NewFile | PendingChangeType::ModifiedFile => {
                    debug!("Syncing file: {:?}", path);
                    let cache_path = self.cache_path(path);

                    if !cache_path.exists() {
                        warn!("Cache file missing for {:?}, skipping", path);
                        self.pending_changes.remove(path);
                        continue;
                    }

                    // Create file on backend if new
                    if matches!(change.change_type, PendingChangeType::NewFile) {
                        if let Some(mode) = change.mode {
                            if let Err(e) = self.inner.create_file_with_mode(path, mode).await {
                                error!("Failed to create file {:?}: {}", path, e);
                                continue;
                            }
                        } else if let Err(e) = self.inner.create_file(path).await {
                            error!("Failed to create file {:?}: {}", path, e);
                            continue;
                        }
                    }

                    // Read and upload content
                    let data = match std::fs::read(&cache_path) {
                        Ok(d) => d,
                        Err(e) => {
                            error!("Failed to read cache file {:?}: {}", path, e);
                            continue;
                        }
                    };

                    if let Err(e) = self.inner.write(path, 0, &data).await {
                        error!("Failed to write file {:?}: {}", path, e);
                        continue;
                    }

                    self.pending_changes.remove(path);
                }
                _ => {}
            }
        }

        // Process deletes
        for (path, change) in deletes {
            match change.change_type {
                PendingChangeType::DeletedFile => {
                    debug!("Syncing file deletion: {:?}", path);
                    if let Err(e) = self.inner.remove_file(path).await {
                        // Ignore NotFound errors - file might not exist on backend
                        if !matches!(e, FuseAdapterError::NotFound(_)) {
                            error!("Failed to delete file {:?}: {}", path, e);
                            continue;
                        }
                    }
                    self.pending_changes.remove(path);
                }
                PendingChangeType::DeletedDirectory => {
                    debug!("Syncing directory deletion: {:?}", path);
                    if let Err(e) = self.inner.remove_dir(path, false).await {
                        if !matches!(e, FuseAdapterError::NotFound(_)) {
                            error!("Failed to delete directory {:?}: {}", path, e);
                            continue;
                        }
                    }
                    self.pending_changes.remove(path);
                }
                _ => {}
            }
        }

        info!(
            "Sync complete, {} changes remaining",
            self.pending_changes.len()
        );
        Ok(())
    }

    /// Flush all pending changes (sync version for shutdown)
    pub async fn flush_all(&self) -> Result<()> {
        self.sync_to_backend().await
    }

    /// Get list of pending changes for a directory (for merging with backend listing)
    fn get_pending_entries_for_dir(&self, dir: &Path) -> Vec<DirEntry> {
        let mut entries = Vec::new();

        for entry in self.pending_changes.iter() {
            let path = entry.key();

            // Check if this path is a direct child of dir
            if let Some(parent) = path.parent() {
                if parent == dir {
                    let name = path.file_name().unwrap_or_default();
                    let file_type = match &entry.value().change_type {
                        PendingChangeType::NewFile | PendingChangeType::ModifiedFile => {
                            FileType::File
                        }
                        PendingChangeType::NewDirectory => FileType::Directory,
                        PendingChangeType::NewSymlink { .. } => FileType::Symlink,
                        PendingChangeType::DeletedFile | PendingChangeType::DeletedDirectory => {
                            continue; // Skip deletes
                        }
                    };

                    entries.push(DirEntry {
                        name: name.to_os_string(),
                        file_type,
                    });
                }
            }
        }

        entries
    }

    /// Get set of paths that are pending delete in a directory
    fn get_pending_deletes_for_dir(&self, dir: &Path) -> HashSet<PathBuf> {
        let mut deletes = HashSet::new();

        for entry in self.pending_changes.iter() {
            let path = entry.key();

            if let Some(parent) = path.parent() {
                if parent == dir {
                    if matches!(
                        entry.value().change_type,
                        PendingChangeType::DeletedFile | PendingChangeType::DeletedDirectory
                    ) {
                        deletes.insert(path.clone());
                    }
                }
            }
        }

        deletes
    }
}

impl<C: Connector> Drop for FilesystemCache<C> {
    fn drop(&mut self) {
        // Signal shutdown to background task
        self.shutdown.notify_waiters();

        let pending_count = self.pending_changes.len();
        if pending_count > 0 {
            warn!(
                "{} pending changes not synced to backend (cache preserved for recovery)",
                pending_count
            );
        }
    }
}

#[async_trait]
impl<C: Connector + 'static> Connector for FilesystemCache<C> {
    fn capabilities(&self) -> Capabilities {
        let mut caps = self.inner.capabilities();
        // Cache layer enables all write operations via local cache
        if caps.write {
            caps.random_write = true;
            caps.truncate = true;
            caps.rename = true;
        }
        // Cache layer can always store mode locally
        caps.set_mode = true;
        // Symlink capability - we can cache symlinks locally now
        caps.symlink = true;
        caps
    }

    fn cache_requirements(&self) -> CacheRequirements {
        CacheRequirements::default()
    }

    async fn stat(&self, path: &Path) -> Result<Metadata> {
        // Check for pending delete first
        if self.is_pending_delete(path) {
            return Err(FuseAdapterError::NotFound(
                path.to_string_lossy().to_string(),
            ));
        }

        // Check for pending create/modify - use local metadata
        if let Some(meta) = self.get_pending_metadata(path) {
            trace!("stat from pending change: {:?}", path);
            return Ok(meta);
        }

        // Check cached metadata
        if let Some(meta) = self.get_cached_metadata(path) {
            trace!("stat cache hit: {:?}", path);
            return Ok(meta);
        }

        // Check if we have a local cached file (fetched from backend earlier)
        let cache_path = self.cache_path(path);
        if cache_path.exists() {
            let std_meta = std::fs::metadata(&cache_path).map_err(|e| {
                FuseAdapterError::Cache(format!("Failed to stat cache file: {}", e))
            })?;

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
            } else if std_meta.is_dir() {
                if let Some(mode) = cached_mode {
                    Metadata::directory_with_mode(
                        std_meta.modified().unwrap_or(SystemTime::now()),
                        mode,
                    )
                } else {
                    Metadata::directory(std_meta.modified().unwrap_or(SystemTime::now()))
                }
            } else {
                // Could be a symlink metadata file
                return Err(FuseAdapterError::NotFound(
                    path.to_string_lossy().to_string(),
                ));
            };

            self.cache_metadata(path, meta.clone());
            return Ok(meta);
        }

        // Check for local symlink
        if let Ok(Some(_)) = self.read_symlink_from_cache(path) {
            return Ok(Metadata::symlink(SystemTime::now()));
        }

        // OPTIMIZATION: If any ancestor is a pending new directory, path can't exist on backend
        if self.has_pending_new_ancestor(path) {
            trace!(
                "stat: path {:?} has pending new ancestor, skipping backend",
                path
            );
            return Err(FuseAdapterError::NotFound(
                path.to_string_lossy().to_string(),
            ));
        }

        // OPTIMIZATION: Check negative cache (known not to exist on backend)
        if self.is_negative_cached(path) {
            trace!("stat negative cache hit: {:?}", path);
            return Err(FuseAdapterError::NotFound(
                path.to_string_lossy().to_string(),
            ));
        }

        // Fall through to backend
        match self.inner.stat(path).await {
            Ok(meta) => {
                if let Some(mode) = meta.mode {
                    self.mode_cache.insert(path.to_path_buf(), mode);
                }
                self.cache_metadata(path, meta.clone());
                Ok(meta)
            }
            Err(FuseAdapterError::NotFound(_)) => {
                // Add to negative cache so we don't check again
                self.add_to_negative_cache(path);
                Err(FuseAdapterError::NotFound(
                    path.to_string_lossy().to_string(),
                ))
            }
            Err(e) => Err(e),
        }
    }

    async fn exists(&self, path: &Path) -> Result<bool> {
        // Check pending delete
        if self.is_pending_delete(path) {
            return Ok(false);
        }

        // Check pending create
        if self.is_pending_create(path) {
            return Ok(true);
        }

        // Check local cache
        if self.cache_path(path).exists() {
            return Ok(true);
        }

        // Check symlink cache
        if self.symlink_meta_path(path).exists() {
            return Ok(true);
        }

        // Check metadata cache
        if self.get_cached_metadata(path).is_some() {
            return Ok(true);
        }

        // OPTIMIZATION: If any ancestor is a pending new directory, path can't exist on backend
        if self.has_pending_new_ancestor(path) {
            trace!(
                "exists: path {:?} has pending new ancestor, skipping backend",
                path
            );
            return Ok(false);
        }

        // OPTIMIZATION: Check negative cache (known not to exist on backend)
        if self.is_negative_cached(path) {
            trace!("exists negative cache hit: {:?}", path);
            return Ok(false);
        }

        // Fall through to backend
        match self.inner.exists(path).await {
            Ok(true) => Ok(true),
            Ok(false) => {
                // Add to negative cache
                self.add_to_negative_cache(path);
                Ok(false)
            }
            Err(e) => Err(e),
        }
    }

    async fn read(&self, path: &Path, offset: u64, size: u32) -> Result<Bytes> {
        // Check pending delete
        if self.is_pending_delete(path) {
            return Err(FuseAdapterError::NotFound(
                path.to_string_lossy().to_string(),
            ));
        }

        // Try reading from cache first
        if let Some(data) = self.read_from_cache(path, offset, size)? {
            trace!("read cache hit: {:?} offset={} size={}", path, offset, size);
            return Ok(data);
        }

        // Fetch from backend if not in cache
        if !self.is_cached(path) {
            debug!("Fetching {:?} to cache", path);
            self.fetch_to_cache(path).await?;
        }

        // Read from cache
        self.read_from_cache(path, offset, size)?
            .ok_or_else(|| FuseAdapterError::NotFound(path.to_string_lossy().to_string()))
    }

    async fn write(&self, path: &Path, offset: u64, data: &[u8]) -> Result<u64> {
        // If file doesn't exist in cache and we're writing at non-zero offset, fetch first
        if !self.is_cached(path) && offset > 0 && !self.is_pending_create(path) {
            if self.inner.stat(path).await.is_ok() {
                self.fetch_to_cache(path).await?;
            }
        }

        // Write to local cache only
        self.write_to_cache(path, offset, data)
    }

    async fn create_file(&self, path: &Path) -> Result<()> {
        // Create locally only - will be synced later
        self.create_in_cache(path, None)
    }

    async fn create_dir(&self, path: &Path) -> Result<()> {
        // Create locally only - will be synced later
        self.create_dir_in_cache(path, None)
    }

    async fn remove_file(&self, path: &Path) -> Result<()> {
        // Mark as deleted locally - will be synced later
        self.mark_deleted(path, false);
        Ok(())
    }

    async fn remove_dir(&self, path: &Path, _recursive: bool) -> Result<()> {
        // Mark as deleted locally - will be synced later
        self.mark_deleted(path, true);
        Ok(())
    }

    fn list_dir(&self, path: &Path) -> DirEntryStream {
        // Get pending entries for this directory
        let pending_entries = self.get_pending_entries_for_dir(path);
        let pending_deletes = self.get_pending_deletes_for_dir(path);

        // Check if we have pending directory entries (new local dir with entries)
        let is_pending_dir = self.pending_changes.get(path).map_or(false, |c| {
            matches!(c.change_type, PendingChangeType::NewDirectory)
        });

        // If it's a pending new directory, just return pending entries
        if is_pending_dir {
            return Box::pin(futures::stream::iter(pending_entries.into_iter().map(Ok)));
        }

        // Check cache first
        if let Some(cached) = self.dir_cache.get(path) {
            if cached.cached_at.elapsed() < self.config.metadata_ttl {
                trace!("list_dir cache hit: {:?}", path);

                // Merge cached entries with pending changes
                let mut entries: Vec<DirEntry> = cached
                    .entries
                    .iter()
                    .filter(|e| {
                        let entry_path = path.join(&e.name);
                        !pending_deletes.contains(&entry_path)
                    })
                    .cloned()
                    .collect();

                // Add pending creates (avoiding duplicates)
                let existing_names: HashSet<_> = entries.iter().map(|e| e.name.clone()).collect();
                for entry in pending_entries {
                    if !existing_names.contains(&entry.name) {
                        entries.push(entry);
                    }
                }

                return Box::pin(futures::stream::iter(entries.into_iter().map(Ok)));
            }
        }

        // Fetch from backend and merge with pending changes
        let inner = self.inner.clone();
        let path_owned = path.to_path_buf();
        let dir_cache = self.dir_cache.clone();

        Box::pin(async_stream::try_stream! {
            debug!("list_dir fetching from backend: {:?}", path_owned);
            let stream = inner.list_dir(&path_owned);

            use futures::StreamExt;
            let backend_entries: Vec<Result<DirEntry>> = stream.collect().await;

            let mut cached_entries = Vec::new();
            let mut seen_names: HashSet<std::ffi::OsString> = HashSet::new();

            // Yield backend entries (filtered by pending deletes)
            for entry_result in backend_entries {
                match entry_result {
                    Ok(entry) => {
                        let entry_path = path_owned.join(&entry.name);
                        if !pending_deletes.contains(&entry_path) {
                            seen_names.insert(entry.name.clone());
                            cached_entries.push(entry.clone());
                            yield entry;
                        }
                    }
                    Err(e) => {
                        Err(e)?;
                    }
                }
            }

            // Yield pending creates (avoiding duplicates)
            for entry in pending_entries {
                if !seen_names.contains(&entry.name) {
                    yield entry;
                }
            }

            // Cache the backend listing
            dir_cache.insert(path_owned, CachedDirListing {
                entries: cached_entries,
                cached_at: Instant::now(),
            });
        })
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        // Rename locally only

        // Copy content
        let from_cache = self.cache_path(from);
        let to_cache = self.cache_path(to);

        if from_cache.exists() {
            if let Some(parent) = to_cache.parent() {
                std::fs::create_dir_all(parent).map_err(|e| {
                    FuseAdapterError::Cache(format!("Failed to create directory: {}", e))
                })?;
            }
            std::fs::rename(&from_cache, &to_cache)
                .map_err(|e| FuseAdapterError::Cache(format!("Failed to rename: {}", e)))?;
        }

        // Handle symlink metadata
        let from_meta = self.symlink_meta_path(from);
        let to_meta = self.symlink_meta_path(to);
        if from_meta.exists() {
            std::fs::rename(&from_meta, &to_meta)
                .map_err(|e| FuseAdapterError::Cache(format!("Failed to rename symlink: {}", e)))?;
        }

        // Update pending changes
        if let Some((_, change)) = self.pending_changes.remove(from) {
            self.pending_changes.insert(to.to_path_buf(), change);
        } else {
            // File exists on backend - mark source as deleted, destination as new
            self.pending_changes.insert(
                from.to_path_buf(),
                PendingChange {
                    change_type: PendingChangeType::DeletedFile,
                    created_at: Instant::now(),
                    mode: None,
                },
            );
            self.pending_changes.insert(
                to.to_path_buf(),
                PendingChange {
                    change_type: PendingChangeType::NewFile,
                    created_at: Instant::now(),
                    mode: self.mode_cache.get(from).map(|r| *r),
                },
            );
        }

        // Update mode cache
        if let Some((_, mode)) = self.mode_cache.remove(from) {
            self.mode_cache.insert(to.to_path_buf(), mode);
        }

        // Invalidate metadata and directory caches
        self.metadata_cache.remove(from);
        self.metadata_cache.remove(to);
        if let Some(parent) = from.parent() {
            self.dir_cache.remove(parent);
        }
        if let Some(parent) = to.parent() {
            self.dir_cache.remove(parent);
        }

        Ok(())
    }

    async fn truncate(&self, path: &Path, size: u64) -> Result<()> {
        // Ensure file is in cache
        if !self.is_cached(path) && !self.is_pending_create(path) {
            self.fetch_to_cache(path).await?;
        }

        // Truncate locally
        self.truncate_in_cache(path, size)
    }

    async fn flush(&self, path: &Path) -> Result<()> {
        // In write-back mode, flush doesn't immediately sync to backend
        // The background task handles that
        // But we should ensure data is persisted to local cache
        trace!("flush called for {:?} (write-back mode)", path);
        Ok(())
    }

    async fn create_file_with_mode(&self, path: &Path, mode: u32) -> Result<()> {
        self.create_in_cache(path, Some(mode))
    }

    async fn create_dir_with_mode(&self, path: &Path, mode: u32) -> Result<()> {
        self.create_dir_in_cache(path, Some(mode))
    }

    async fn set_mode(&self, path: &Path, mode: u32) -> Result<()> {
        self.mode_cache.insert(path.to_path_buf(), mode);
        self.metadata_cache.remove(path);

        // Mark as modified if it exists
        if self.is_cached(path) || self.is_pending_create(path) {
            self.pending_changes
                .entry(path.to_path_buf())
                .and_modify(|change| {
                    change.mode = Some(mode);
                });
        }

        Ok(())
    }

    async fn readlink(&self, path: &Path) -> Result<PathBuf> {
        // Check pending delete
        if self.is_pending_delete(path) {
            return Err(FuseAdapterError::NotFound(
                path.to_string_lossy().to_string(),
            ));
        }

        // Check local symlink cache first
        if let Some(target) = self.read_symlink_from_cache(path)? {
            return Ok(target);
        }

        // Fall through to backend
        self.inner.readlink(path).await
    }

    async fn symlink(&self, target: &Path, link_path: &Path) -> Result<()> {
        // Create locally only - will be synced later
        // Clear any existing cache at this path
        self.mark_deleted(link_path, false);
        self.pending_changes.remove(link_path); // Remove the delete we just added

        self.create_symlink_in_cache(target, link_path)
    }
}

#[cfg(test)]
mod tests {
    // Tests would go here
}
