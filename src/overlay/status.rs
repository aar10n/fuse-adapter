//! Status overlay that wraps a connector with virtual status files
//!
//! Provides a virtual `/{prefix}/` directory containing:
//! - `status` - "healthy\n" or "error\n"
//! - `error` - Current error message or empty
//! - `error_log` - Timestamped log of errors

use std::collections::VecDeque;
use std::ffi::OsString;
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};
use std::time::SystemTime;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::stream;
use tracing::warn;

use crate::config::StatusOverlayConfig;
use crate::connector::{
    CacheRequirements, Capabilities, Connector, DirEntry, DirEntryStream, FileType, Metadata,
};
use crate::error::{FuseAdapterError, Result};

/// Mount health status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MountStatus {
    Healthy,
    Error,
}

/// A single error log entry
#[derive(Debug, Clone)]
struct ErrorLogEntry {
    timestamp: DateTime<Utc>,
    operation: String,
    path: String,
    error: String,
}

impl ErrorLogEntry {
    fn format(&self) -> String {
        format!(
            "[{}] {} {}: {}\n",
            self.timestamp.format("%Y-%m-%d %H:%M:%S%.3f UTC"),
            self.operation,
            self.path,
            self.error
        )
    }
}

/// Internal state for tracking mount health
struct OverlayState {
    status: MountStatus,
    current_error: Option<String>,
}

/// Status overlay that wraps a connector with virtual status files
pub struct StatusOverlay {
    /// Inner connector (None if connector initialization failed)
    inner: Option<Arc<dyn Connector>>,
    /// Current mount status
    state: RwLock<OverlayState>,
    /// Configuration
    config: StatusOverlayConfig,
    /// Error log (ring buffer)
    error_log: Mutex<VecDeque<ErrorLogEntry>>,
}

impl StatusOverlay {
    /// Create a new status overlay wrapping a working connector
    pub fn new(connector: Arc<dyn Connector>, config: StatusOverlayConfig) -> Self {
        Self {
            inner: Some(connector),
            state: RwLock::new(OverlayState {
                status: MountStatus::Healthy,
                current_error: None,
            }),
            config,
            error_log: Mutex::new(VecDeque::new()),
        }
    }

    /// Create a status overlay for a failed connector
    ///
    /// The mount will still be accessible but all real file operations will return EIO.
    /// The status files will show the initialization error.
    pub fn new_failed(init_error: String, config: StatusOverlayConfig) -> Self {
        let mut error_log = VecDeque::new();
        error_log.push_back(ErrorLogEntry {
            timestamp: Utc::now(),
            operation: "init".to_string(),
            path: "/".to_string(),
            error: init_error.clone(),
        });

        Self {
            inner: None,
            state: RwLock::new(OverlayState {
                status: MountStatus::Error,
                current_error: Some(init_error),
            }),
            config,
            error_log: Mutex::new(error_log),
        }
    }

    /// Check if a path is within the virtual status directory
    fn is_virtual_path(&self, path: &Path) -> bool {
        let prefix = &self.config.prefix;
        if let Some(std::path::Component::Normal(name)) = path.components().next() {
            return name.to_string_lossy() == *prefix;
        }
        false
    }

    /// Get the virtual file name from a path (e.g., ".fuse-adapter/status" -> "status")
    fn virtual_file_name(&self, path: &Path) -> Option<String> {
        let components: Vec<_> = path.components().collect();
        if components.len() == 2 {
            if let std::path::Component::Normal(name) = components[1] {
                return name.to_str().map(|s| s.to_string());
            }
        }
        None
    }

    /// Log an error and update status
    fn log_error(&self, operation: &str, path: &Path, error: &FuseAdapterError) {
        let entry = ErrorLogEntry {
            timestamp: Utc::now(),
            operation: operation.to_string(),
            path: path.display().to_string(),
            error: error.to_string(),
        };

        // Update state
        {
            let mut state = self.state.write().unwrap();
            state.status = MountStatus::Error;
            state.current_error = Some(error.to_string());
        }

        // Add to log
        {
            let mut log = self.error_log.lock().unwrap();
            log.push_back(entry);
            while log.len() > self.config.max_log_entries {
                log.pop_front();
            }
        }

        warn!(
            "StatusOverlay error in {} on {}: {}",
            operation,
            path.display(),
            error
        );
    }

    /// Get the content of a virtual file
    fn get_virtual_content(&self, name: &str) -> Option<String> {
        match name {
            "status" => {
                let state = self.state.read().unwrap();
                let status_str = match state.status {
                    MountStatus::Healthy => "healthy\n",
                    MountStatus::Error => "error\n",
                };
                Some(status_str.to_string())
            }
            "error" => {
                let state = self.state.read().unwrap();
                Some(state.current_error.clone().unwrap_or_default())
            }
            "error_log" => {
                let log = self.error_log.lock().unwrap();
                let content: String = log.iter().map(|e| e.format()).collect();
                Some(content)
            }
            _ => None,
        }
    }

    /// Get metadata for a virtual file
    fn get_virtual_metadata(&self, name: &str) -> Option<Metadata> {
        let content = self.get_virtual_content(name)?;
        Some(Metadata::file_with_mode(
            content.len() as u64,
            SystemTime::now(),
            0o444, // Read-only
        ))
    }

    /// Execute an operation on the inner connector, logging errors
    async fn with_error_logging<T, F, Fut>(&self, operation: &str, path: &Path, f: F) -> Result<T>
    where
        F: FnOnce(Arc<dyn Connector>) -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let inner = match &self.inner {
            Some(c) => c.clone(),
            None => {
                return Err(FuseAdapterError::Backend(
                    "Connector not available".to_string(),
                ))
            }
        };

        match f(inner).await {
            Ok(result) => Ok(result),
            Err(e) => {
                self.log_error(operation, path, &e);
                Err(e)
            }
        }
    }
}

#[async_trait]
impl Connector for StatusOverlay {
    fn capabilities(&self) -> Capabilities {
        match &self.inner {
            Some(c) => c.capabilities(),
            None => Capabilities::read_only(),
        }
    }

    fn cache_requirements(&self) -> CacheRequirements {
        match &self.inner {
            Some(c) => c.cache_requirements(),
            None => CacheRequirements::default(),
        }
    }

    async fn stat(&self, path: &Path) -> Result<Metadata> {
        // Check if this is the virtual directory itself
        let prefix = &self.config.prefix;
        if path == Path::new(prefix) || path == Path::new(&format!("/{}", prefix)) {
            return Ok(Metadata::directory_with_mode(SystemTime::now(), 0o555));
        }

        // Check if this is a virtual file
        if self.is_virtual_path(path) {
            if let Some(name) = self.virtual_file_name(path) {
                if let Some(metadata) = self.get_virtual_metadata(&name) {
                    return Ok(metadata);
                }
            }
            return Err(FuseAdapterError::NotFound(path.display().to_string()));
        }

        // Delegate to inner
        self.with_error_logging("stat", path, |c| async move { c.stat(path).await })
            .await
    }

    async fn exists(&self, path: &Path) -> Result<bool> {
        match self.stat(path).await {
            Ok(_) => Ok(true),
            Err(FuseAdapterError::NotFound(_)) => Ok(false),
            Err(e) => Err(e),
        }
    }

    async fn read(&self, path: &Path, offset: u64, size: u32) -> Result<Bytes> {
        // Check if this is a virtual file
        if self.is_virtual_path(path) {
            if let Some(name) = self.virtual_file_name(path) {
                if let Some(content) = self.get_virtual_content(&name) {
                    let bytes = content.into_bytes();
                    let start = (offset as usize).min(bytes.len());
                    let end = (start + size as usize).min(bytes.len());
                    return Ok(Bytes::copy_from_slice(&bytes[start..end]));
                }
            }
            return Err(FuseAdapterError::NotFound(path.display().to_string()));
        }

        // Delegate to inner
        self.with_error_logging(
            "read",
            path,
            |c| async move { c.read(path, offset, size).await },
        )
        .await
    }

    async fn write(&self, path: &Path, offset: u64, data: &[u8]) -> Result<u64> {
        // Virtual files are read-only
        if self.is_virtual_path(path) {
            return Err(FuseAdapterError::ReadOnly);
        }

        // Delegate to inner
        self.with_error_logging("write", path, |c| async move {
            c.write(path, offset, data).await
        })
        .await
    }

    async fn create_file(&self, path: &Path) -> Result<()> {
        if self.is_virtual_path(path) {
            return Err(FuseAdapterError::ReadOnly);
        }

        self.with_error_logging(
            "create_file",
            path,
            |c| async move { c.create_file(path).await },
        )
        .await
    }

    async fn create_dir(&self, path: &Path) -> Result<()> {
        if self.is_virtual_path(path) {
            return Err(FuseAdapterError::ReadOnly);
        }

        self.with_error_logging(
            "create_dir",
            path,
            |c| async move { c.create_dir(path).await },
        )
        .await
    }

    async fn remove_file(&self, path: &Path) -> Result<()> {
        if self.is_virtual_path(path) {
            return Err(FuseAdapterError::ReadOnly);
        }

        self.with_error_logging(
            "remove_file",
            path,
            |c| async move { c.remove_file(path).await },
        )
        .await
    }

    async fn remove_dir(&self, path: &Path, recursive: bool) -> Result<()> {
        if self.is_virtual_path(path) {
            return Err(FuseAdapterError::ReadOnly);
        }

        self.with_error_logging("remove_dir", path, |c| async move {
            c.remove_dir(path, recursive).await
        })
        .await
    }

    fn list_dir(&self, path: &Path) -> DirEntryStream {
        let prefix = self.config.prefix.clone();
        let path_owned = path.to_path_buf();

        // If listing the virtual directory
        if self.is_virtual_path(path)
            || path == Path::new(&prefix)
            || path == Path::new(&format!("/{}", prefix))
        {
            let entries = vec![
                Ok(DirEntry::file("status")),
                Ok(DirEntry::file("error")),
                Ok(DirEntry::file("error_log")),
            ];
            return Box::pin(stream::iter(entries));
        }

        // If listing root, inject the virtual directory
        if path == Path::new("") || path == Path::new("/") {
            let inner = self.inner.clone();
            let prefix_for_injection = prefix.clone();

            return Box::pin(async_stream::stream! {
                // First yield the virtual directory
                yield Ok(DirEntry {
                    name: OsString::from(&prefix_for_injection),
                    file_type: FileType::Directory,
                });

                // Then yield entries from the inner connector
                if let Some(connector) = inner {
                    use futures::StreamExt;
                    let mut inner_stream = connector.list_dir(&path_owned);
                    while let Some(entry) = inner_stream.next().await {
                        yield entry;
                    }
                }
            });
        }

        // Delegate to inner for other directories
        match &self.inner {
            Some(c) => c.list_dir(path),
            None => Box::pin(stream::empty()),
        }
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        if self.is_virtual_path(from) || self.is_virtual_path(to) {
            return Err(FuseAdapterError::ReadOnly);
        }

        self.with_error_logging("rename", from, |c| async move { c.rename(from, to).await })
            .await
    }

    async fn truncate(&self, path: &Path, size: u64) -> Result<()> {
        if self.is_virtual_path(path) {
            return Err(FuseAdapterError::ReadOnly);
        }

        self.with_error_logging(
            "truncate",
            path,
            |c| async move { c.truncate(path, size).await },
        )
        .await
    }

    async fn flush(&self, path: &Path) -> Result<()> {
        // Virtual files don't need flushing
        if self.is_virtual_path(path) {
            return Ok(());
        }

        self.with_error_logging("flush", path, |c| async move { c.flush(path).await })
            .await
    }

    async fn create_file_with_mode(&self, path: &Path, mode: u32) -> Result<()> {
        if self.is_virtual_path(path) {
            return Err(FuseAdapterError::ReadOnly);
        }

        self.with_error_logging("create_file_with_mode", path, |c| async move {
            c.create_file_with_mode(path, mode).await
        })
        .await
    }

    async fn create_dir_with_mode(&self, path: &Path, mode: u32) -> Result<()> {
        if self.is_virtual_path(path) {
            return Err(FuseAdapterError::ReadOnly);
        }

        self.with_error_logging("create_dir_with_mode", path, |c| async move {
            c.create_dir_with_mode(path, mode).await
        })
        .await
    }

    async fn set_mode(&self, path: &Path, mode: u32) -> Result<()> {
        if self.is_virtual_path(path) {
            return Err(FuseAdapterError::ReadOnly);
        }

        self.with_error_logging(
            "set_mode",
            path,
            |c| async move { c.set_mode(path, mode).await },
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_new_failed_sets_error_state() {
        let config = StatusOverlayConfig::default();
        let overlay = StatusOverlay::new_failed("Connection refused".to_string(), config);

        let state = overlay.state.read().unwrap();
        assert_eq!(state.status, MountStatus::Error);
        assert_eq!(state.current_error, Some("Connection refused".to_string()));

        let log = overlay.error_log.lock().unwrap();
        assert_eq!(log.len(), 1);
        assert_eq!(log[0].operation, "init");
    }

    #[test]
    fn test_is_virtual_path() {
        let config = StatusOverlayConfig::default();
        let overlay = StatusOverlay::new_failed("test".to_string(), config);

        assert!(overlay.is_virtual_path(Path::new(".fuse-adapter/status")));
        assert!(overlay.is_virtual_path(Path::new(".fuse-adapter/error")));
        assert!(overlay.is_virtual_path(Path::new(".fuse-adapter")));
        assert!(!overlay.is_virtual_path(Path::new("real-file.txt")));
        assert!(!overlay.is_virtual_path(Path::new("subdir/file.txt")));
    }

    #[test]
    fn test_virtual_file_name() {
        let config = StatusOverlayConfig::default();
        let overlay = StatusOverlay::new_failed("test".to_string(), config);

        assert_eq!(
            overlay.virtual_file_name(Path::new(".fuse-adapter/status")),
            Some("status".to_string())
        );
        assert_eq!(
            overlay.virtual_file_name(Path::new(".fuse-adapter/error")),
            Some("error".to_string())
        );
        assert_eq!(
            overlay.virtual_file_name(Path::new(".fuse-adapter/error_log")),
            Some("error_log".to_string())
        );
        // Directory itself has no "file name"
        assert_eq!(overlay.virtual_file_name(Path::new(".fuse-adapter")), None);
    }

    #[test]
    fn test_get_virtual_content() {
        let config = StatusOverlayConfig::default();
        let overlay = StatusOverlay::new_failed("Test error".to_string(), config);

        assert_eq!(
            overlay.get_virtual_content("status"),
            Some("error\n".to_string())
        );
        assert_eq!(
            overlay.get_virtual_content("error"),
            Some("Test error".to_string())
        );

        let log_content = overlay.get_virtual_content("error_log").unwrap();
        assert!(log_content.contains("init"));
        assert!(log_content.contains("Test error"));
    }

    #[test]
    fn test_custom_prefix() {
        let config = StatusOverlayConfig {
            prefix: ".status".to_string(),
            max_log_entries: 100,
        };
        let overlay = StatusOverlay::new_failed("test".to_string(), config);

        assert!(overlay.is_virtual_path(Path::new(".status/status")));
        assert!(!overlay.is_virtual_path(Path::new(".fuse-adapter/status")));
    }

    #[test]
    fn test_error_log_max_entries() {
        let config = StatusOverlayConfig {
            prefix: ".fuse-adapter".to_string(),
            max_log_entries: 3,
        };
        let overlay = StatusOverlay::new_failed("initial".to_string(), config);

        // Add more errors (initial error is already 1)
        for i in 0..5 {
            overlay.log_error(
                "test",
                &PathBuf::from(format!("/file{}", i)),
                &FuseAdapterError::Backend(format!("error {}", i)),
            );
        }

        let log = overlay.error_log.lock().unwrap();
        assert_eq!(log.len(), 3); // Max entries enforced
    }
}
