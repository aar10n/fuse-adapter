//! Google Drive connector implementation
//!
//! This connector provides access to Google Drive using a service account.
//! Files and folders are accessed via path resolution that maps paths to
//! Google Drive file IDs.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_stream::try_stream;
use async_trait::async_trait;
use bytes::Bytes;
use google_drive3::api::{File, Scope};
use google_drive3::yup_oauth2::{read_service_account_key, ServiceAccountAuthenticator};
use google_drive3::DriveHub;
use http_body_util::BodyExt;
use hyper_util::client::legacy::connect::HttpConnector;
use parking_lot::RwLock;
use tracing::{debug, trace};

use crate::config::GDriveConnectorConfig;
use crate::connector::{
    CacheRequirement, CacheRequirements, Capabilities, Connector, DirEntry, DirEntryStream,
    Metadata,
};
use crate::error::{FuseAdapterError, Result};

/// MIME type for Google Drive folders
const FOLDER_MIME_TYPE: &str = "application/vnd.google-apps.folder";

/// Fields to request for file metadata
const FILE_FIELDS: &str = "id, name, mimeType, size, modifiedTime, parents";

/// Fields to request for file list
const LIST_FIELDS: &str = "nextPageToken, files(id, name, mimeType, size, modifiedTime)";

type DriveClient = DriveHub<hyper_rustls::HttpsConnector<HttpConnector>>;

/// Google Drive connector
pub struct GDriveConnector {
    hub: Arc<DriveClient>,
    root_folder_id: String,
    /// Cache mapping paths to file IDs
    path_cache: RwLock<HashMap<String, String>>,
}

impl GDriveConnector {
    /// Create a new Google Drive connector from configuration
    pub async fn new(config: GDriveConnectorConfig) -> Result<Self> {
        // Load service account credentials
        let creds = read_service_account_key(&config.credentials_path)
            .await
            .map_err(|e| {
                FuseAdapterError::Backend(format!("Failed to read credentials: {}", e))
            })?;

        // Create authenticator
        let auth = ServiceAccountAuthenticator::builder(creds)
            .build()
            .await
            .map_err(|e| {
                FuseAdapterError::Backend(format!("Failed to create authenticator: {}", e))
            })?;

        // Create HTTPS connector
        let https = hyper_rustls::HttpsConnectorBuilder::new()
            .with_native_roots()
            .map_err(|e| FuseAdapterError::Backend(format!("Failed to load TLS roots: {}", e)))?
            .https_or_http()
            .enable_http1()
            .enable_http2()
            .build();

        // Create HTTP client
        let client = hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
            .build(https);

        // Create Drive hub
        let hub = DriveHub::new(client, auth);

        // Initialize path cache with root
        let mut path_cache = HashMap::new();
        path_cache.insert("/".to_string(), config.root_folder_id.clone());
        path_cache.insert("".to_string(), config.root_folder_id.clone());

        Ok(Self {
            hub: Arc::new(hub),
            root_folder_id: config.root_folder_id,
            path_cache: RwLock::new(path_cache),
        })
    }

    /// Normalize a path to a consistent format
    fn normalize_path(path: &Path) -> String {
        let path_str = path.to_string_lossy();
        let path_str = path_str.trim_start_matches('/');
        if path_str.is_empty() {
            "/".to_string()
        } else {
            format!("/{}", path_str)
        }
    }

    /// Resolve a path to a Google Drive file ID
    async fn resolve_path(&self, path: &Path) -> Result<String> {
        let normalized = Self::normalize_path(path);
        trace!("resolve_path: {:?} -> {}", path, normalized);

        // Check cache first
        if let Some(id) = self.path_cache.read().get(&normalized) {
            return Ok(id.clone());
        }

        // Walk the path from root
        let components: Vec<&str> = normalized
            .trim_start_matches('/')
            .split('/')
            .filter(|s| !s.is_empty())
            .collect();

        if components.is_empty() {
            return Ok(self.root_folder_id.clone());
        }

        let mut current_id = self.root_folder_id.clone();
        let mut current_path = String::new();

        for component in components {
            current_path = format!("{}/{}", current_path, component);

            // Check cache for this intermediate path
            if let Some(id) = self.path_cache.read().get(&current_path) {
                current_id = id.clone();
                continue;
            }

            // Query for the child with this name
            let query = format!(
                "'{}' in parents and name = '{}' and trashed = false",
                current_id, component
            );

            let result = self
                .hub
                .files()
                .list()
                .q(&query)
                .add_scope(Scope::Full)
                .param("fields", LIST_FIELDS)
                .page_size(1)
                .doit()
                .await
                .map_err(|e| {
                    FuseAdapterError::Backend(format!("Drive API error: {}", e))
                })?;

            let files = result.1.files.unwrap_or_default();
            if files.is_empty() {
                return Err(FuseAdapterError::NotFound(format!(
                    "Path not found: {}",
                    current_path
                )));
            }

            let file = &files[0];
            let file_id = file.id.clone().ok_or_else(|| {
                FuseAdapterError::Backend("File has no ID".to_string())
            })?;

            // Cache this path
            self.path_cache
                .write()
                .insert(current_path.clone(), file_id.clone());

            current_id = file_id;
        }

        Ok(current_id)
    }

    /// Get the parent folder ID and filename from a path
    async fn resolve_parent(&self, path: &Path) -> Result<(String, String)> {
        let normalized = Self::normalize_path(path);
        let parent_path = Path::new(&normalized)
            .parent()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_else(|| "/".to_string());

        let parent_path = if parent_path.is_empty() {
            "/".to_string()
        } else {
            parent_path
        };

        let file_name = Path::new(&normalized)
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .ok_or_else(|| FuseAdapterError::InvalidPath("No filename".to_string()))?;

        let parent_id = self.resolve_path(Path::new(&parent_path)).await?;
        Ok((parent_id, file_name))
    }

    /// Get file metadata by ID
    async fn get_file_metadata(&self, file_id: &str) -> Result<File> {
        let result = self
            .hub
            .files()
            .get(file_id)
            .add_scope(Scope::Full)
            .param("fields", FILE_FIELDS)
            .doit()
            .await
            .map_err(|e| {
                if e.to_string().contains("404") || e.to_string().contains("notFound") {
                    FuseAdapterError::NotFound(format!("File not found: {}", file_id))
                } else {
                    FuseAdapterError::Backend(format!("Drive API error: {}", e))
                }
            })?;

        Ok(result.1)
    }

    /// Convert a Google Drive File to our Metadata
    fn file_to_metadata(file: &File) -> Result<Metadata> {
        let is_folder = file
            .mime_type
            .as_ref()
            .map(|m| m == FOLDER_MIME_TYPE)
            .unwrap_or(false);

        // modified_time is already chrono::DateTime<Utc>
        let mtime = file
            .modified_time
            .as_ref()
            .map(|dt| {
                let timestamp = dt.timestamp();
                if timestamp >= 0 {
                    SystemTime::UNIX_EPOCH + Duration::from_secs(timestamp as u64)
                } else {
                    SystemTime::now()
                }
            })
            .unwrap_or(SystemTime::now());

        if is_folder {
            Ok(Metadata::directory(mtime))
        } else {
            // size is Option<i64> in the API
            let size = file.size.unwrap_or(0) as u64;
            Ok(Metadata::file(size, mtime))
        }
    }

    /// Invalidate a path from the cache
    fn invalidate_path(&self, path: &Path) {
        let normalized = Self::normalize_path(path);
        self.path_cache.write().remove(&normalized);
    }

    /// Invalidate a path and all its children from the cache
    fn invalidate_path_recursive(&self, path: &Path) {
        let normalized = Self::normalize_path(path);
        let mut cache = self.path_cache.write();
        cache.retain(|k, _| !k.starts_with(&normalized) || k == "/");
    }
}

#[async_trait]
impl Connector for GDriveConnector {
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            read: true,
            write: true,
            range_read: false, // Drive supports it but it's complex
            random_write: false,
            rename: true,
            truncate: false,
            set_mtime: false,
            seekable: false,
            set_mode: false, // Drive doesn't support POSIX permissions
        }
    }

    fn cache_requirements(&self) -> CacheRequirements {
        CacheRequirements {
            write_buffer: CacheRequirement::Required,
            read_cache: true,
            metadata_cache_ttl: Some(Duration::from_secs(60)),
        }
    }

    async fn stat(&self, path: &Path) -> Result<Metadata> {
        trace!("stat: {:?}", path);

        let file_id = self.resolve_path(path).await?;
        let file = self.get_file_metadata(&file_id).await?;
        Self::file_to_metadata(&file)
    }

    async fn read(&self, path: &Path, offset: u64, size: u32) -> Result<Bytes> {
        trace!("read: {:?} offset={} size={}", path, offset, size);

        let file_id = self.resolve_path(path).await?;

        // Download the file content
        let response = self
            .hub
            .files()
            .get(&file_id)
            .add_scope(Scope::Full)
            .param("alt", "media")
            .doit()
            .await
            .map_err(|e| {
                if e.to_string().contains("404") || e.to_string().contains("notFound") {
                    FuseAdapterError::NotFound(format!("File not found: {:?}", path))
                } else {
                    FuseAdapterError::Backend(format!("Drive API error: {}", e))
                }
            })?;

        // Read the body
        let body = response.0.into_body();
        let collected = body.collect().await.map_err(|e| {
            FuseAdapterError::Backend(format!("Failed to read response body: {}", e))
        })?;
        let bytes = collected.to_bytes();

        // Handle offset and size
        let start = offset as usize;
        let end = std::cmp::min(start + size as usize, bytes.len());

        if start >= bytes.len() {
            return Ok(Bytes::new());
        }

        Ok(bytes.slice(start..end))
    }

    async fn write(&self, path: &Path, offset: u64, data: &[u8]) -> Result<u64> {
        // Google Drive doesn't support partial writes
        if offset != 0 {
            return Err(FuseAdapterError::NotSupported(
                "Google Drive doesn't support partial writes; use cache layer".to_string(),
            ));
        }

        debug!("write: {:?} size={}", path, data.len());

        let file_id = self.resolve_path(path).await?;

        // Upload using media upload
        let cursor = std::io::Cursor::new(data.to_vec());

        self.hub
            .files()
            .update(File::default(), &file_id)
            .add_scope(Scope::Full)
            .upload(cursor, "application/octet-stream".parse().unwrap())
            .await
            .map_err(|e| {
                FuseAdapterError::Backend(format!("Drive upload error: {}", e))
            })?;

        Ok(data.len() as u64)
    }

    async fn create_file(&self, path: &Path) -> Result<()> {
        debug!("create_file: {:?}", path);

        let (parent_id, file_name) = self.resolve_parent(path).await?;

        let file_metadata = File {
            name: Some(file_name),
            parents: Some(vec![parent_id]),
            ..Default::default()
        };

        let cursor = std::io::Cursor::new(Vec::new());

        let result = self
            .hub
            .files()
            .create(file_metadata)
            .add_scope(Scope::Full)
            .upload(cursor, "application/octet-stream".parse().unwrap())
            .await
            .map_err(|e| {
                FuseAdapterError::Backend(format!("Drive create error: {}", e))
            })?;

        // Cache the new file's ID
        if let Some(id) = result.1.id {
            let normalized = Self::normalize_path(path);
            self.path_cache.write().insert(normalized, id);
        }

        Ok(())
    }

    async fn create_dir(&self, path: &Path) -> Result<()> {
        debug!("create_dir: {:?}", path);

        let (parent_id, folder_name) = self.resolve_parent(path).await?;

        let folder_metadata = File {
            name: Some(folder_name),
            mime_type: Some(FOLDER_MIME_TYPE.to_string()),
            parents: Some(vec![parent_id]),
            ..Default::default()
        };

        // For folders, we still use upload but with empty content
        // The folder MIME type makes it a folder
        let cursor = std::io::Cursor::new(Vec::<u8>::new());

        let result = self
            .hub
            .files()
            .create(folder_metadata)
            .add_scope(Scope::Full)
            .upload(cursor, FOLDER_MIME_TYPE.parse().unwrap())
            .await
            .map_err(|e| {
                FuseAdapterError::Backend(format!("Drive create folder error: {}", e))
            })?;

        // Cache the new folder's ID
        if let Some(id) = result.1.id {
            let normalized = Self::normalize_path(path);
            self.path_cache.write().insert(normalized, id);
        }

        Ok(())
    }

    async fn remove_file(&self, path: &Path) -> Result<()> {
        debug!("remove_file: {:?}", path);

        let file_id = self.resolve_path(path).await?;

        self.hub
            .files()
            .delete(&file_id)
            .add_scope(Scope::Full)
            .doit()
            .await
            .map_err(|e| {
                FuseAdapterError::Backend(format!("Drive delete error: {}", e))
            })?;

        self.invalidate_path(path);
        Ok(())
    }

    async fn remove_dir(&self, path: &Path, recursive: bool) -> Result<()> {
        debug!("remove_dir: {:?} recursive={}", path, recursive);

        let file_id = self.resolve_path(path).await?;

        if !recursive {
            // Check if folder is empty
            let query = format!("'{}' in parents and trashed = false", file_id);
            let result = self
                .hub
                .files()
                .list()
                .q(&query)
                .add_scope(Scope::Full)
                .page_size(1)
                .doit()
                .await
                .map_err(|e| {
                    FuseAdapterError::Backend(format!("Drive API error: {}", e))
                })?;

            let files = result.1.files.unwrap_or_default();
            if !files.is_empty() {
                return Err(FuseAdapterError::NotEmpty(format!(
                    "Directory not empty: {:?}",
                    path
                )));
            }
        }

        // Google Drive delete is recursive by default
        self.hub
            .files()
            .delete(&file_id)
            .add_scope(Scope::Full)
            .doit()
            .await
            .map_err(|e| {
                FuseAdapterError::Backend(format!("Drive delete error: {}", e))
            })?;

        self.invalidate_path_recursive(path);
        Ok(())
    }

    fn list_dir(&self, path: &Path) -> DirEntryStream {
        let hub = self.hub.clone();
        let path_owned = path.to_path_buf();
        let connector = self.clone_for_stream();

        Box::pin(try_stream! {
            let folder_id = connector.resolve_path(&path_owned).await?;

            let query = format!("'{}' in parents and trashed = false", folder_id);
            let mut page_token: Option<String> = None;

            loop {
                let mut request = hub
                    .files()
                    .list()
                    .q(&query)
                    .add_scope(Scope::Full)
                    .param("fields", LIST_FIELDS)
                    .page_size(100);

                if let Some(token) = page_token.take() {
                    request = request.page_token(&token);
                }

                let result = request.doit().await.map_err(|e| {
                    FuseAdapterError::Backend(format!("Drive list error: {}", e))
                })?;

                let files = result.1.files.unwrap_or_default();

                for file in files {
                    let name = match file.name {
                        Some(n) => n,
                        None => continue,
                    };

                    let is_folder = file
                        .mime_type
                        .as_ref()
                        .map(|m| m == FOLDER_MIME_TYPE)
                        .unwrap_or(false);

                    if is_folder {
                        yield DirEntry::directory(name);
                    } else {
                        yield DirEntry::file(name);
                    }
                }

                page_token = result.1.next_page_token;
                if page_token.is_none() {
                    break;
                }
            }
        })
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        debug!("rename: {:?} -> {:?}", from, to);

        let file_id = self.resolve_path(from).await?;
        let (new_parent_id, new_name) = self.resolve_parent(to).await?;

        // Get current parents
        let file = self.get_file_metadata(&file_id).await?;
        let current_parents = file.parents.unwrap_or_default().join(",");

        // Update file with new name and parent using PATCH (param method)
        let update = File {
            name: Some(new_name),
            ..Default::default()
        };

        // For metadata-only update, use upload with empty content
        let cursor = std::io::Cursor::new(Vec::<u8>::new());

        self.hub
            .files()
            .update(update, &file_id)
            .add_parents(&new_parent_id)
            .remove_parents(&current_parents)
            .add_scope(Scope::Full)
            .upload(cursor, "application/octet-stream".parse().unwrap())
            .await
            .map_err(|e| {
                FuseAdapterError::Backend(format!("Drive rename error: {}", e))
            })?;

        // Invalidate cache for both paths
        self.invalidate_path_recursive(from);
        self.invalidate_path(to);

        // Cache the new path
        let normalized = Self::normalize_path(to);
        self.path_cache.write().insert(normalized, file_id);

        Ok(())
    }

    async fn truncate(&self, _path: &Path, _size: u64) -> Result<()> {
        Err(FuseAdapterError::NotSupported(
            "Google Drive doesn't support truncate; use cache layer".to_string(),
        ))
    }

    async fn flush(&self, _path: &Path) -> Result<()> {
        // Google Drive writes are immediately durable
        Ok(())
    }
}

impl GDriveConnector {
    /// Clone the connector for use in async streams
    fn clone_for_stream(&self) -> GDriveConnectorInner {
        GDriveConnectorInner {
            hub: self.hub.clone(),
            root_folder_id: self.root_folder_id.clone(),
            path_cache: self.path_cache.read().clone(),
        }
    }
}

/// Inner struct for use in async streams (without RwLock)
struct GDriveConnectorInner {
    hub: Arc<DriveClient>,
    root_folder_id: String,
    path_cache: HashMap<String, String>,
}

impl GDriveConnectorInner {
    async fn resolve_path(&self, path: &Path) -> Result<String> {
        let normalized = GDriveConnector::normalize_path(path);

        // Check cache first
        if let Some(id) = self.path_cache.get(&normalized) {
            return Ok(id.clone());
        }

        // Walk the path from root
        let components: Vec<&str> = normalized
            .trim_start_matches('/')
            .split('/')
            .filter(|s| !s.is_empty())
            .collect();

        if components.is_empty() {
            return Ok(self.root_folder_id.clone());
        }

        let mut current_id = self.root_folder_id.clone();

        for component in components {
            // Query for the child with this name
            let query = format!(
                "'{}' in parents and name = '{}' and trashed = false",
                current_id, component
            );

            let result = self
                .hub
                .files()
                .list()
                .q(&query)
                .add_scope(Scope::Full)
                .param("fields", LIST_FIELDS)
                .page_size(1)
                .doit()
                .await
                .map_err(|e| {
                    FuseAdapterError::Backend(format!("Drive API error: {}", e))
                })?;

            let files = result.1.files.unwrap_or_default();
            if files.is_empty() {
                return Err(FuseAdapterError::NotFound(format!(
                    "Path not found: {:?}",
                    path
                )));
            }

            let file = &files[0];
            current_id = file.id.clone().ok_or_else(|| {
                FuseAdapterError::Backend("File has no ID".to_string())
            })?;
        }

        Ok(current_id)
    }
}
