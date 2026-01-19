# Implementing a New Connector for fuse-adapter

This guide explains how to implement a new storage connector for fuse-adapter. Connectors are the backend storage implementations that provide access to various data sources (S3, Google Drive, etc.).

## Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│                    FUSE Interface                       │
│                    (fuser crate)                        │
└─────────────────────────┬───────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────┐
│                    FuseAdapter                          │
│           (inode mapping, capability checking)          │
└─────────────────────────┬───────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────┐
│              Optional Cache Layer                       │
│     (FilesystemCache, MemoryCache, or NoCache)          │
└─────────────────────────┬───────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────┐
│                  Your Connector                         │
│            (implements Connector trait)                 │
└─────────────────────────────────────────────────────────┘
```

Connectors are:
- **Stateless**: No file handles or open/close semantics. Each operation receives a path and data.
- **Path-based**: Work with `&Path` values. The framework handles inode <-> path mapping.
- **Async**: All operations are async, using the `async_trait` crate.
- **Send + Sync**: Must be thread-safe for concurrent access.

## The Connector Trait

Here's the core trait your connector must implement:

```rust
#[async_trait]
pub trait Connector: Send + Sync {
    /// Declare this connector's capabilities
    fn capabilities(&self) -> Capabilities;

    /// Declare cache requirements (optional, has default)
    fn cache_requirements(&self) -> CacheRequirements {
        CacheRequirements::default()
    }

    /// Get metadata for a path
    async fn stat(&self, path: &Path) -> Result<Metadata>;

    /// Check if a path exists (optional, default uses stat)
    async fn exists(&self, path: &Path) -> Result<bool>;

    /// Read bytes from a file at offset
    async fn read(&self, path: &Path, offset: u64, size: u32) -> Result<Bytes>;

    /// Write bytes to a file at offset
    async fn write(&self, path: &Path, offset: u64, data: &[u8]) -> Result<u64>;

    /// Create an empty file
    async fn create_file(&self, path: &Path) -> Result<()>;

    /// Create a directory
    async fn create_dir(&self, path: &Path) -> Result<()>;

    /// Remove a file
    async fn remove_file(&self, path: &Path) -> Result<()>;

    /// Remove a directory (recursive flag controls behavior)
    async fn remove_dir(&self, path: &Path, recursive: bool) -> Result<()>;

    /// List directory contents as an async stream
    fn list_dir(&self, path: &Path) -> DirEntryStream;

    /// Rename/move a file or directory
    async fn rename(&self, from: &Path, to: &Path) -> Result<()>;

    /// Truncate a file to specified size
    async fn truncate(&self, path: &Path, size: u64) -> Result<()>;

    /// Ensure writes are persisted
    async fn flush(&self, path: &Path) -> Result<()>;
}
```

## Step-by-Step Implementation

### Step 1: Create Your Connector Module

Create a new file in `src/connector/`:

```rust
// src/connector/mybackend.rs

use std::path::Path;
use std::time::{Duration, SystemTime};

use async_stream::try_stream;
use async_trait::async_trait;
use bytes::Bytes;

use crate::config::MyBackendConfig;  // You'll define this
use crate::connector::{
    CacheRequirement, CacheRequirements, Capabilities, Connector,
    DirEntry, DirEntryStream, Metadata,
};
use crate::error::{FuseAdapterError, Result};

pub struct MyBackendConnector {
    // Your connection state
    client: MyClient,
    root_path: String,
}

impl MyBackendConnector {
    pub async fn new(config: MyBackendConfig) -> Result<Self> {
        // Initialize your client/connection
        let client = MyClient::connect(&config.connection_string)
            .await
            .map_err(|e| FuseAdapterError::Backend(e.to_string()))?;

        Ok(Self {
            client,
            root_path: config.root_path,
        })
    }
}
```

### Step 2: Define Capabilities

Carefully consider what your backend supports:

```rust
fn capabilities(&self) -> Capabilities {
    Capabilities {
        read: true,           // Can read files?
        write: true,          // Can write files?
        range_read: true,     // Supports reading byte ranges?
        random_write: false,  // Can write at arbitrary offsets?
        rename: true,         // Has native rename operation?
        truncate: false,      // Can truncate files?
        set_mtime: false,     // Can modify timestamps?
        seekable: true,       // Is random access cheap?
    }
}
```

**Important**: If `random_write` is false, the cache layer is required for write support. Set `write_buffer: CacheRequirement::Required` in cache requirements.

### Step 3: Define Cache Requirements

```rust
fn cache_requirements(&self) -> CacheRequirements {
    CacheRequirements {
        // Required: connector cannot function without write buffering
        // Recommended: performance improves with buffering
        // None: no benefit from write buffering
        write_buffer: CacheRequirement::Required,

        // Whether read caching improves performance
        read_cache: true,

        // Suggested metadata cache TTL
        metadata_cache_ttl: Some(Duration::from_secs(60)),
    }
}
```

### Step 4: Implement Core Operations

#### stat (Required)

Return metadata for a path. This is called frequently.

```rust
async fn stat(&self, path: &Path) -> Result<Metadata> {
    let backend_path = self.to_backend_path(path);

    let info = self.client.get_info(&backend_path)
        .await
        .map_err(|e| {
            if e.is_not_found() {
                FuseAdapterError::NotFound(path.to_string_lossy().to_string())
            } else {
                FuseAdapterError::Backend(e.to_string())
            }
        })?;

    if info.is_directory {
        Ok(Metadata::directory(info.modified_at))
    } else {
        Ok(Metadata::file(info.size, info.modified_at))
    }
}
```

#### read (Required)

Read bytes from a file. Support offset and size for range reads.

```rust
async fn read(&self, path: &Path, offset: u64, size: u32) -> Result<Bytes> {
    let backend_path = self.to_backend_path(path);

    let data = self.client.read_range(&backend_path, offset, size as u64)
        .await
        .map_err(|e| {
            if e.is_not_found() {
                FuseAdapterError::NotFound(path.to_string_lossy().to_string())
            } else {
                FuseAdapterError::Backend(e.to_string())
            }
        })?;

    Ok(Bytes::from(data))
}
```

#### write (Required)

Write bytes to a file. Return the number of bytes written.

```rust
async fn write(&self, path: &Path, offset: u64, data: &[u8]) -> Result<u64> {
    // If your backend doesn't support random writes, return NotSupported
    // The cache layer will handle buffering
    if offset != 0 && !self.capabilities().random_write {
        return Err(FuseAdapterError::NotSupported(
            "Backend doesn't support random writes".to_string()
        ));
    }

    let backend_path = self.to_backend_path(path);

    self.client.write(&backend_path, offset, data)
        .await
        .map_err(|e| FuseAdapterError::Backend(e.to_string()))?;

    Ok(data.len() as u64)
}
```

#### list_dir (Required)

Return a stream of directory entries. Use `async_stream` for convenience:

```rust
fn list_dir(&self, path: &Path) -> DirEntryStream {
    let backend_path = self.to_backend_path(path);
    let client = self.client.clone();

    Box::pin(try_stream! {
        let mut cursor = None;

        loop {
            let page = client.list_directory(&backend_path, cursor)
                .await
                .map_err(|e| FuseAdapterError::Backend(e.to_string()))?;

            for item in page.items {
                if item.is_directory {
                    yield DirEntry::directory(item.name);
                } else {
                    yield DirEntry::file(item.name);
                }
            }

            if page.next_cursor.is_none() {
                break;
            }
            cursor = page.next_cursor;
        }
    })
}
```

### Step 5: Add Configuration

Define your configuration struct in `src/config.rs`:

```rust
/// MyBackend connector configuration
#[derive(Debug, Clone, Deserialize)]
pub struct MyBackendConfig {
    pub connection_string: String,
    pub root_path: Option<String>,
    pub timeout_seconds: Option<u64>,
}
```

Add it to the `ConnectorConfig` enum:

```rust
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum ConnectorConfig {
    S3(S3ConnectorConfig),
    GDrive(GDriveConnectorConfig),
    MyBackend(MyBackendConfig),  // Add your config
}
```

### Step 6: Register in main.rs

Update the connector creation in `main.rs`:

```rust
let connector: Arc<dyn Connector> = match &mount_config.connector {
    ConnectorConfig::S3(config) => { /* ... */ }
    ConnectorConfig::MyBackend(config) => {
        let backend = MyBackendConnector::new(config.clone()).await?;
        wrap_with_cache(backend, &mount_config.cache)?
    }
    // ...
};
```

### Step 7: Export from mod.rs

Add your module to `src/connector/mod.rs`:

```rust
pub mod mybackend;
pub mod s3;

// Re-export for convenience
pub use mybackend::MyBackendConnector;
```

## Error Handling

Use the appropriate error variants from `FuseAdapterError`:

```rust
use crate::error::FuseAdapterError;

// File/directory not found
FuseAdapterError::NotFound(path_string)

// Path already exists
FuseAdapterError::AlreadyExists(path_string)

// Expected directory, got file
FuseAdapterError::NotADirectory(path_string)

// Expected file, got directory
FuseAdapterError::IsADirectory(path_string)

// Directory not empty (for non-recursive remove)
FuseAdapterError::NotEmpty(path_string)

// Operation not supported by this connector
FuseAdapterError::NotSupported(description)

// Read-only connector
FuseAdapterError::ReadOnly

// Generic backend error
FuseAdapterError::Backend(error_message)
```

## Capability System Deep Dive

The framework uses capabilities to:
1. **Return appropriate errors**: If `write: false`, write operations return `EROFS`
2. **Synthesize operations**: If `rename: false` but `read` and `write` are true, rename can be emulated via copy+delete
3. **Enforce cache requirements**: If `random_write: false`, the cache layer enables random write support

### Capability Matrix

| Capability | If False... |
|------------|-------------|
| `read` | Read operations return `ENOSYS` |
| `write` | Write/create operations return `EROFS` |
| `range_read` | Full file must be read for any read |
| `random_write` | Write at offset > 0 fails without cache layer |
| `rename` | Rename synthesized via copy+delete |
| `truncate` | Truncate fails without cache layer |
| `set_mtime` | mtime changes are ignored |
| `seekable` | Hint only; affects caching strategy |

## Testing Your Connector

### Unit Tests

Create tests in your connector module:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use tokio;

    #[tokio::test]
    async fn test_stat_file() {
        let connector = create_test_connector().await;

        let meta = connector.stat(Path::new("/test/file.txt")).await.unwrap();
        assert!(meta.is_file());
        assert!(meta.size > 0);
    }

    #[tokio::test]
    async fn test_not_found() {
        let connector = create_test_connector().await;

        let result = connector.stat(Path::new("/nonexistent")).await;
        assert!(matches!(result, Err(FuseAdapterError::NotFound(_))));
    }
}
```

### Integration Tests

For connectors that require external services, use Docker or mock servers:

```rust
// tests/integration/mybackend.rs

#[tokio::test]
#[ignore] // Run with --ignored flag when backend is available
async fn test_full_workflow() {
    let connector = MyBackendConnector::new(test_config()).await.unwrap();

    // Create file
    connector.create_file(Path::new("/test/new.txt")).await.unwrap();

    // Write content
    connector.write(Path::new("/test/new.txt"), 0, b"Hello").await.unwrap();

    // Read back
    let data = connector.read(Path::new("/test/new.txt"), 0, 100).await.unwrap();
    assert_eq!(&data[..], b"Hello");

    // Clean up
    connector.remove_file(Path::new("/test/new.txt")).await.unwrap();
}
```

## Example: Simple In-Memory Connector

Here's a complete example of a simple in-memory connector for reference:

```rust
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use async_stream::try_stream;
use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::RwLock;

use crate::connector::*;
use crate::error::{FuseAdapterError, Result};

/// In-memory filesystem for testing
pub struct MemoryConnector {
    files: Arc<RwLock<HashMap<PathBuf, Vec<u8>>>>,
    dirs: Arc<RwLock<std::collections::HashSet<PathBuf>>>,
}

impl MemoryConnector {
    pub fn new() -> Self {
        let dirs = Arc::new(RwLock::new(std::collections::HashSet::new()));
        dirs.write().insert(PathBuf::from("/"));

        Self {
            files: Arc::new(RwLock::new(HashMap::new())),
            dirs,
        }
    }
}

#[async_trait]
impl Connector for MemoryConnector {
    fn capabilities(&self) -> Capabilities {
        Capabilities::full()  // Full capabilities
    }

    async fn stat(&self, path: &Path) -> Result<Metadata> {
        if self.dirs.read().contains(path) {
            return Ok(Metadata::directory(SystemTime::now()));
        }

        if let Some(data) = self.files.read().get(path) {
            return Ok(Metadata::file(data.len() as u64, SystemTime::now()));
        }

        Err(FuseAdapterError::NotFound(path.to_string_lossy().to_string()))
    }

    async fn read(&self, path: &Path, offset: u64, size: u32) -> Result<Bytes> {
        let files = self.files.read();
        let data = files.get(path)
            .ok_or_else(|| FuseAdapterError::NotFound(path.to_string_lossy().to_string()))?;

        let start = offset as usize;
        let end = std::cmp::min(start + size as usize, data.len());

        if start >= data.len() {
            return Ok(Bytes::new());
        }

        Ok(Bytes::copy_from_slice(&data[start..end]))
    }

    async fn write(&self, path: &Path, offset: u64, data: &[u8]) -> Result<u64> {
        let mut files = self.files.write();
        let file = files.entry(path.to_path_buf()).or_insert_with(Vec::new);

        let offset = offset as usize;
        if offset > file.len() {
            file.resize(offset, 0);
        }

        if offset + data.len() > file.len() {
            file.resize(offset + data.len(), 0);
        }

        file[offset..offset + data.len()].copy_from_slice(data);
        Ok(data.len() as u64)
    }

    async fn create_file(&self, path: &Path) -> Result<()> {
        self.files.write().insert(path.to_path_buf(), Vec::new());
        Ok(())
    }

    async fn create_dir(&self, path: &Path) -> Result<()> {
        self.dirs.write().insert(path.to_path_buf());
        Ok(())
    }

    async fn remove_file(&self, path: &Path) -> Result<()> {
        self.files.write().remove(path)
            .ok_or_else(|| FuseAdapterError::NotFound(path.to_string_lossy().to_string()))?;
        Ok(())
    }

    async fn remove_dir(&self, path: &Path, _recursive: bool) -> Result<()> {
        self.dirs.write().remove(path);
        Ok(())
    }

    fn list_dir(&self, path: &Path) -> DirEntryStream {
        let files = self.files.clone();
        let dirs = self.dirs.clone();
        let path = path.to_path_buf();

        Box::pin(try_stream! {
            let prefix = if path == Path::new("/") {
                PathBuf::from("/")
            } else {
                path.clone()
            };

            // List files
            for file_path in files.read().keys() {
                if let Some(parent) = file_path.parent() {
                    if parent == prefix {
                        if let Some(name) = file_path.file_name() {
                            yield DirEntry::file(name.to_os_string());
                        }
                    }
                }
            }

            // List subdirectories
            for dir_path in dirs.read().iter() {
                if let Some(parent) = dir_path.parent() {
                    if parent == prefix && dir_path != &prefix {
                        if let Some(name) = dir_path.file_name() {
                            yield DirEntry::directory(name.to_os_string());
                        }
                    }
                }
            }
        })
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        let mut files = self.files.write();
        if let Some(data) = files.remove(from) {
            files.insert(to.to_path_buf(), data);
            Ok(())
        } else {
            Err(FuseAdapterError::NotFound(from.to_string_lossy().to_string()))
        }
    }

    async fn truncate(&self, path: &Path, size: u64) -> Result<()> {
        let mut files = self.files.write();
        let file = files.get_mut(path)
            .ok_or_else(|| FuseAdapterError::NotFound(path.to_string_lossy().to_string()))?;
        file.resize(size as usize, 0);
        Ok(())
    }

    async fn flush(&self, _path: &Path) -> Result<()> {
        Ok(())  // In-memory, nothing to flush
    }
}
```

## Best Practices

1. **Handle errors gracefully**: Convert backend errors to appropriate `FuseAdapterError` variants
2. **Be honest about capabilities**: Don't claim capabilities your backend doesn't support
3. **Support cancellation**: Use async properly so operations can be cancelled
4. **Log appropriately**: Use `tracing` macros at appropriate levels
5. **Document limitations**: Note any backend-specific limitations in comments
6. **Test edge cases**: Empty files, large files, special characters in names, etc.

## Checklist

- [ ] Implement all required trait methods
- [ ] Define accurate capabilities
- [ ] Set appropriate cache requirements
- [ ] Add configuration struct
- [ ] Register in `ConnectorConfig` enum
- [ ] Add connector creation in `main.rs`
- [ ] Export from `connector/mod.rs`
- [ ] Write unit tests
- [ ] Write integration tests (if applicable)
- [ ] Document any limitations
