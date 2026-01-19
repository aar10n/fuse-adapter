//! S3 connector implementation
//!
//! This connector provides access to Amazon S3 or S3-compatible storage
//! backends (MinIO, LocalStack, etc.).

use std::collections::HashMap;
use std::path::Path;
use std::time::{Duration, SystemTime};

/// S3 metadata key for storing POSIX file mode
const S3_MODE_METADATA_KEY: &str = "posix-mode";

use async_stream::try_stream;
use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use bytes::Bytes;
use tracing::{debug, trace};

use crate::config::S3ConnectorConfig;
use crate::connector::{
    CacheRequirement, CacheRequirements, Capabilities, Connector, DirEntry,
    DirEntryStream, Metadata,
};
use crate::error::{FuseAdapterError, Result};

/// S3 connector for Amazon S3 and S3-compatible storage
pub struct S3Connector {
    client: Client,
    bucket: String,
    prefix: String,
    read_only: bool,
}

impl S3Connector {
    /// Create a new S3 connector from configuration
    pub async fn new(config: S3ConnectorConfig) -> Result<Self> {
        let mut sdk_config_builder = aws_config::defaults(BehaviorVersion::latest());

        if let Some(region) = &config.region {
            sdk_config_builder = sdk_config_builder.region(Region::new(region.clone()));
        }

        let sdk_config = sdk_config_builder.load().await;

        let mut s3_config_builder = aws_sdk_s3::config::Builder::from(&sdk_config);

        if let Some(endpoint) = &config.endpoint {
            s3_config_builder = s3_config_builder.endpoint_url(endpoint);
        }

        if config.force_path_style {
            s3_config_builder = s3_config_builder.force_path_style(true);
        }

        let client = Client::from_conf(s3_config_builder.build());

        let prefix = config.prefix.unwrap_or_default();

        Ok(Self {
            client,
            bucket: config.bucket,
            prefix,
            read_only: config.read_only,
        })
    }

    /// Convert a filesystem path to an S3 key
    fn path_to_key(&self, path: &Path) -> String {
        let path_str = path.to_string_lossy();
        let path_str = path_str.trim_start_matches('/');

        if path_str.is_empty() {
            self.prefix.clone()
        } else if self.prefix.is_empty() {
            path_str.to_string()
        } else {
            // Ensure proper separator between prefix and path
            if self.prefix.ends_with('/') {
                format!("{}{}", self.prefix, path_str)
            } else {
                format!("{}/{}", self.prefix, path_str)
            }
        }
    }

    /// Convert an S3 key to a relative path (removing prefix)
    #[allow(dead_code)]
    fn key_to_path(&self, key: &str) -> std::path::PathBuf {
        let key = key.strip_prefix(&self.prefix).unwrap_or(key);
        let key = key.trim_start_matches('/').trim_end_matches('/');
        std::path::PathBuf::from(format!("/{}", key))
    }

    /// Extract the filename from an S3 key
    #[allow(dead_code)]
    fn key_to_name(&self, key: &str) -> String {
        // Remove prefix
        let key = key.strip_prefix(&self.prefix).unwrap_or(key);
        // Get the last component
        key.trim_end_matches('/')
            .rsplit('/')
            .next()
            .unwrap_or(key)
            .to_string()
    }

    /// Create S3 metadata HashMap with mode
    fn mode_to_metadata(mode: u32) -> HashMap<String, String> {
        let mut metadata = HashMap::new();
        metadata.insert(S3_MODE_METADATA_KEY.to_string(), format!("{:o}", mode));
        metadata
    }
}

#[async_trait]
impl Connector for S3Connector {
    fn capabilities(&self) -> Capabilities {
        if self.read_only {
            Capabilities {
                read: true,
                write: false,
                range_read: true,
                random_write: false,
                rename: false,
                truncate: false,
                set_mtime: false,
                seekable: false,
                set_mode: false,
            }
        } else {
            Capabilities {
                read: true,
                write: true,
                range_read: true,
                random_write: false, // S3 doesn't support partial writes
                rename: false,       // S3 has no native rename
                truncate: false,     // Can't truncate in S3
                set_mtime: false,
                seekable: false, // Range requests work but aren't cheap
                set_mode: true,  // Stored in S3 user metadata
            }
        }
    }

    fn cache_requirements(&self) -> CacheRequirements {
        CacheRequirements {
            write_buffer: CacheRequirement::Required, // Must buffer writes
            read_cache: true,
            metadata_cache_ttl: Some(Duration::from_secs(60)),
        }
    }

    async fn stat(&self, path: &Path) -> Result<Metadata> {
        let key = self.path_to_key(path);
        trace!("stat: path={:?} key={}", path, key);

        // Root directory always exists
        if key.is_empty() || key == self.prefix {
            return Ok(Metadata::directory(SystemTime::now()));
        }

        // First try as a file (HeadObject)
        let head_result = self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await;

        match head_result {
            Ok(output) => {
                let size = output.content_length().unwrap_or(0) as u64;
                let mtime = output
                    .last_modified()
                    .and_then(|dt| {
                        SystemTime::UNIX_EPOCH.checked_add(Duration::from_secs(
                            dt.secs() as u64
                        ))
                    })
                    .unwrap_or(SystemTime::now());

                // Read mode from S3 user metadata
                let mode = output
                    .metadata()
                    .and_then(|m| m.get(S3_MODE_METADATA_KEY))
                    .and_then(|v| u32::from_str_radix(v, 8).ok());

                return Ok(if let Some(mode) = mode {
                    Metadata::file_with_mode(size, mtime, mode)
                } else {
                    Metadata::file(size, mtime)
                });
            }
            Err(e) => {
                // Check if it's a "not found" error
                let service_error = e.into_service_error();
                if !service_error.is_not_found() {
                    // Some other error
                    return Err(FuseAdapterError::Backend(format!(
                        "S3 HeadObject error: {}",
                        service_error
                    )));
                }
            }
        }

        // Try as a directory (check if any objects exist with this prefix)
        let dir_key = if key.ends_with('/') {
            key.clone()
        } else {
            format!("{}/", key)
        };

        let list_result = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(&dir_key)
            .max_keys(1)
            .send()
            .await
            .map_err(|e| {
                let service_error = e.into_service_error();
                FuseAdapterError::Backend(format!(
                    "S3 ListObjectsV2 error for prefix '{}': {:?}",
                    dir_key, service_error
                ))
            })?;

        if list_result.key_count().unwrap_or(0) > 0 {
            return Ok(Metadata::directory(SystemTime::now()));
        }

        // Also check for common prefixes
        if list_result.common_prefixes().len() > 0 {
            return Ok(Metadata::directory(SystemTime::now()));
        }

        Err(FuseAdapterError::NotFound(format!("Path not found: {:?}", path)))
    }

    async fn exists(&self, path: &Path) -> Result<bool> {
        match self.stat(path).await {
            Ok(_) => Ok(true),
            Err(FuseAdapterError::NotFound(_)) => Ok(false),
            Err(e) => Err(e),
        }
    }

    async fn read(&self, path: &Path, offset: u64, size: u32) -> Result<Bytes> {
        let key = self.path_to_key(path);
        trace!("read: path={:?} key={} offset={} size={}", path, key, offset, size);

        let range = format!("bytes={}-{}", offset, offset + size as u64 - 1);

        let result = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&key)
            .range(range)
            .send()
            .await
            .map_err(|e| {
                let service_error = e.into_service_error();
                if service_error.is_no_such_key() {
                    FuseAdapterError::NotFound(format!("File not found: {:?}", path))
                } else {
                    FuseAdapterError::Backend(format!("S3 GetObject error: {}", service_error))
                }
            })?;

        let body = result
            .body
            .collect()
            .await
            .map_err(|e| FuseAdapterError::Backend(format!("S3 read body error: {}", e)))?;

        Ok(body.into_bytes())
    }

    async fn write(&self, path: &Path, offset: u64, data: &[u8]) -> Result<u64> {
        // S3 doesn't support partial writes, so this requires the cache layer
        // to buffer the entire file and upload on flush.
        //
        // For now, we only support writing at offset 0 (full file replacement)
        if offset != 0 {
            return Err(FuseAdapterError::NotSupported(
                "S3 doesn't support partial writes; use cache layer".to_string(),
            ));
        }

        let key = self.path_to_key(path);
        debug!("write: path={:?} key={} size={}", path, key, data.len());

        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(ByteStream::from(data.to_vec()))
            .send()
            .await
            .map_err(|e| {
                FuseAdapterError::Backend(format!("S3 PutObject error: {}", e))
            })?;

        Ok(data.len() as u64)
    }

    async fn create_file(&self, path: &Path) -> Result<()> {
        let key = self.path_to_key(path);
        debug!("create_file: path={:?} key={}", path, key);

        // Create empty file
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(ByteStream::from(Vec::new()))
            .send()
            .await
            .map_err(|e| {
                FuseAdapterError::Backend(format!("S3 PutObject error: {}", e))
            })?;

        Ok(())
    }

    async fn create_dir(&self, path: &Path) -> Result<()> {
        // Directories in S3 are virtual - they exist if there are objects
        // with that prefix. We can create a placeholder object.
        let mut key = self.path_to_key(path);
        if !key.ends_with('/') {
            key.push('/');
        }

        debug!("create_dir: path={:?} key={}", path, key);

        // Create a zero-byte object with trailing slash to represent directory
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(ByteStream::from(Vec::new()))
            .send()
            .await
            .map_err(|e| {
                FuseAdapterError::Backend(format!("S3 PutObject error: {}", e))
            })?;

        Ok(())
    }

    async fn remove_file(&self, path: &Path) -> Result<()> {
        let key = self.path_to_key(path);
        debug!("remove_file: path={:?} key={}", path, key);

        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| {
                FuseAdapterError::Backend(format!("S3 DeleteObject error: {}", e))
            })?;

        Ok(())
    }

    async fn remove_dir(&self, path: &Path, recursive: bool) -> Result<()> {
        let mut key = self.path_to_key(path);
        if !key.ends_with('/') {
            key.push('/');
        }

        debug!("remove_dir: path={:?} key={} recursive={}", path, key, recursive);

        if !recursive {
            // Check if directory is empty
            let list_result = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&key)
                .max_keys(2) // 1 for dir placeholder + 1 for any content
                .send()
                .await
                .map_err(|e| {
                    let service_error = e.into_service_error();
                    FuseAdapterError::Backend(format!(
                        "S3 ListObjectsV2 remove_dir check error: {:?}",
                        service_error
                    ))
                })?;

            let contents = list_result.contents();
            let non_dir_objects: Vec<_> = contents
                .iter()
                .filter(|obj| obj.key().map(|k| k != &key).unwrap_or(true))
                .collect();

            if !non_dir_objects.is_empty() {
                return Err(FuseAdapterError::NotEmpty(format!(
                    "Directory not empty: {:?}",
                    path
                )));
            }
        }

        // Delete all objects with this prefix
        let mut continuation_token: Option<String> = None;

        loop {
            let mut request = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&key);

            if let Some(token) = continuation_token {
                request = request.continuation_token(token);
            }

            let list_result = request.send().await.map_err(|e| {
                let service_error = e.into_service_error();
                FuseAdapterError::Backend(format!(
                    "S3 ListObjectsV2 remove_dir delete error: {:?}",
                    service_error
                ))
            })?;

            let contents = list_result.contents();

            if !contents.is_empty() {
                // Batch delete
                let objects_to_delete: Vec<_> = contents
                    .iter()
                    .filter_map(|obj| obj.key())
                    .map(|k| {
                        aws_sdk_s3::types::ObjectIdentifier::builder()
                            .key(k)
                            .build()
                            .unwrap()
                    })
                    .collect();

                let delete = aws_sdk_s3::types::Delete::builder()
                    .set_objects(Some(objects_to_delete))
                    .build()
                    .map_err(|e| {
                        FuseAdapterError::Backend(format!("Failed to build delete: {}", e))
                    })?;

                self.client
                    .delete_objects()
                    .bucket(&self.bucket)
                    .delete(delete)
                    .send()
                    .await
                    .map_err(|e| {
                        FuseAdapterError::Backend(format!("S3 DeleteObjects error: {}", e))
                    })?;
            }

            if list_result.is_truncated().unwrap_or(false) {
                continuation_token = list_result.next_continuation_token().map(|s| s.to_string());
            } else {
                break;
            }
        }

        Ok(())
    }

    fn list_dir(&self, path: &Path) -> DirEntryStream {
        let mut prefix = self.path_to_key(path);
        if !prefix.is_empty() && !prefix.ends_with('/') {
            prefix.push('/');
        }

        let client = self.client.clone();
        let bucket = self.bucket.clone();

        Box::pin(try_stream! {
            let mut continuation_token: Option<String> = None;

            loop {
                let mut request = client
                    .list_objects_v2()
                    .bucket(&bucket)
                    .prefix(&prefix)
                    .delimiter("/");

                if let Some(token) = continuation_token.take() {
                    request = request.continuation_token(token);
                }

                let result = request.send().await.map_err(|e| {
                    let service_error = e.into_service_error();
                    FuseAdapterError::Backend(format!(
                        "S3 ListObjectsV2 list_dir error: {:?}",
                        service_error
                    ))
                })?;

                // Yield files (objects that aren't the directory marker)
                for obj in result.contents() {
                    if let Some(key) = obj.key() {
                        // Skip directory marker objects
                        if key.ends_with('/') || key == prefix {
                            continue;
                        }

                        // Skip if this is a nested path (shouldn't happen with delimiter)
                        let rel_key = key.strip_prefix(&prefix).unwrap_or(key);
                        if rel_key.contains('/') {
                            continue;
                        }

                        yield DirEntry::file(rel_key.to_string());
                    }
                }

                // Yield directories (common prefixes)
                for common_prefix in result.common_prefixes() {
                    if let Some(p) = common_prefix.prefix() {
                        // Extract directory name
                        let rel_prefix = p.strip_prefix(&prefix).unwrap_or(p);
                        let name = rel_prefix.trim_end_matches('/');
                        if !name.is_empty() {
                            yield DirEntry::directory(name.to_string());
                        }
                    }
                }

                if result.is_truncated().unwrap_or(false) {
                    continuation_token = result.next_continuation_token().map(|s| s.to_string());
                } else {
                    break;
                }
            }
        })
    }

    async fn rename(&self, _from: &Path, _to: &Path) -> Result<()> {
        // S3 doesn't have native rename - this should be handled by the cache
        // layer or synthesized via copy+delete
        Err(FuseAdapterError::NotSupported(
            "S3 doesn't support native rename".to_string(),
        ))
    }

    async fn truncate(&self, _path: &Path, _size: u64) -> Result<()> {
        // S3 doesn't support truncate - this should be handled by the cache layer
        Err(FuseAdapterError::NotSupported(
            "S3 doesn't support truncate".to_string(),
        ))
    }

    async fn flush(&self, _path: &Path) -> Result<()> {
        // S3 writes are immediately durable
        Ok(())
    }

    async fn create_file_with_mode(&self, path: &Path, mode: u32) -> Result<()> {
        let key = self.path_to_key(path);
        debug!("create_file_with_mode: path={:?} key={} mode={:o}", path, key, mode);

        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(ByteStream::from(Vec::new()))
            .set_metadata(Some(Self::mode_to_metadata(mode)))
            .send()
            .await
            .map_err(|e| {
                FuseAdapterError::Backend(format!("S3 PutObject error: {}", e))
            })?;

        Ok(())
    }

    async fn create_dir_with_mode(&self, path: &Path, mode: u32) -> Result<()> {
        let mut key = self.path_to_key(path);
        if !key.ends_with('/') {
            key.push('/');
        }

        debug!("create_dir_with_mode: path={:?} key={} mode={:o}", path, key, mode);

        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(ByteStream::from(Vec::new()))
            .set_metadata(Some(Self::mode_to_metadata(mode)))
            .send()
            .await
            .map_err(|e| {
                FuseAdapterError::Backend(format!("S3 PutObject error: {}", e))
            })?;

        Ok(())
    }

    async fn set_mode(&self, path: &Path, mode: u32) -> Result<()> {
        let key = self.path_to_key(path);
        debug!("set_mode: path={:?} key={} mode={:o}", path, key, mode);

        // S3 doesn't allow updating metadata in place, so we need to copy the object
        // to itself with new metadata
        let copy_source = format!("{}/{}", self.bucket, key);

        self.client
            .copy_object()
            .bucket(&self.bucket)
            .key(&key)
            .copy_source(&copy_source)
            .metadata_directive(aws_sdk_s3::types::MetadataDirective::Replace)
            .set_metadata(Some(Self::mode_to_metadata(mode)))
            .send()
            .await
            .map_err(|e| {
                FuseAdapterError::Backend(format!("S3 CopyObject error: {}", e))
            })?;

        Ok(())
    }
}
