//! MinIO container management for e2e tests

use anyhow::{Context, Result};
use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::Client as S3Client;
use bollard::container::{Config, CreateContainerOptions, StartContainerOptions};
use bollard::image::CreateImageOptions;
use bollard::models::{HostConfig, PortBinding};
use bollard::Docker;
use futures::StreamExt;
use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, info, warn};
use uuid::Uuid;

const MINIO_IMAGE: &str = "minio/minio:latest";
const MINIO_CONTAINER_NAME: &str = "fuse-adapter-e2e-minio";
const DEFAULT_ACCESS_KEY: &str = "minioadmin";
const DEFAULT_SECRET_KEY: &str = "minioadmin";
const DEFAULT_PORT: u16 = 9000;

/// Manages a MinIO container for testing
pub struct MinioContainer {
    docker: Option<Docker>,
    container_id: Option<String>,
    endpoint: String,
    access_key: String,
    secret_key: String,
    s3_client: S3Client,
    keep_alive: bool,
}

impl MinioContainer {
    /// Start or connect to a MinIO container
    ///
    /// Environment variables:
    /// - `MINIO_ENDPOINT`: Use existing MinIO at this endpoint (for CI)
    /// - `MINIO_ACCESS_KEY`: Access key (default: minioadmin)
    /// - `MINIO_SECRET_KEY`: Secret key (default: minioadmin)
    /// - `KEEP_MINIO`: If set, don't stop container on drop
    pub async fn start() -> Result<Self> {
        let access_key = env::var("MINIO_ACCESS_KEY").unwrap_or_else(|_| DEFAULT_ACCESS_KEY.to_string());
        let secret_key = env::var("MINIO_SECRET_KEY").unwrap_or_else(|_| DEFAULT_SECRET_KEY.to_string());
        let keep_alive = env::var("KEEP_MINIO").is_ok();

        // Check if we should use an existing endpoint
        if let Ok(endpoint) = env::var("MINIO_ENDPOINT") {
            info!("Using existing MinIO at {}", endpoint);
            let s3_client = Self::create_s3_client(&endpoint, &access_key, &secret_key).await?;

            // Verify connection
            Self::wait_for_minio(&s3_client).await?;

            return Ok(Self {
                docker: None,
                container_id: None,
                endpoint,
                access_key,
                secret_key,
                s3_client,
                keep_alive: true, // Never stop external MinIO
            });
        }

        // Connect to Docker
        let docker = Docker::connect_with_local_defaults()
            .context("Failed to connect to Docker. Is Docker running?")?;

        // Check for existing container
        let containers = docker
            .list_containers::<String>(None)
            .await
            .context("Failed to list containers")?;

        let existing = containers.iter().find(|c| {
            c.names
                .as_ref()
                .map(|names| names.iter().any(|n| n.contains(MINIO_CONTAINER_NAME)))
                .unwrap_or(false)
        });

        let (container_id, endpoint) = if let Some(container) = existing {
            let id = container.id.clone().unwrap_or_default();
            info!("Found existing MinIO container: {}", id);

            // Ensure it's running
            let inspect = docker.inspect_container(&id, None).await?;
            if !inspect.state.as_ref().map(|s| s.running.unwrap_or(false)).unwrap_or(false) {
                info!("Starting stopped MinIO container");
                docker.start_container(&id, None::<StartContainerOptions<String>>).await?;
            }

            let endpoint = format!("http://localhost:{}", DEFAULT_PORT);
            (id, endpoint)
        } else {
            // Pull image if needed
            info!("Pulling MinIO image...");
            let mut stream = docker.create_image(
                Some(CreateImageOptions {
                    from_image: MINIO_IMAGE,
                    ..Default::default()
                }),
                None,
                None,
            );
            while let Some(result) = stream.next().await {
                if let Err(e) = result {
                    warn!("Image pull warning: {}", e);
                }
            }

            // Create container
            info!("Creating MinIO container...");
            let mut port_bindings = HashMap::new();
            port_bindings.insert(
                "9000/tcp".to_string(),
                Some(vec![PortBinding {
                    host_ip: Some("0.0.0.0".to_string()),
                    host_port: Some(DEFAULT_PORT.to_string()),
                }]),
            );

            let host_config = HostConfig {
                port_bindings: Some(port_bindings),
                ..Default::default()
            };

            let env_user = format!("MINIO_ROOT_USER={}", access_key);
            let env_pass = format!("MINIO_ROOT_PASSWORD={}", secret_key);
            let config = Config {
                image: Some(MINIO_IMAGE),
                env: Some(vec![&env_user, &env_pass]),
                cmd: Some(vec!["server", "/data"]),
                host_config: Some(host_config),
                ..Default::default()
            };

            let container = docker
                .create_container(
                    Some(CreateContainerOptions {
                        name: MINIO_CONTAINER_NAME,
                        platform: None,
                    }),
                    config,
                )
                .await
                .context("Failed to create MinIO container")?;

            // Start container
            docker
                .start_container(&container.id, None::<StartContainerOptions<String>>)
                .await
                .context("Failed to start MinIO container")?;

            info!("MinIO container started: {}", container.id);
            let endpoint = format!("http://localhost:{}", DEFAULT_PORT);
            (container.id, endpoint)
        };

        let s3_client = Self::create_s3_client(&endpoint, &access_key, &secret_key).await?;

        // Wait for MinIO to be ready
        Self::wait_for_minio(&s3_client).await?;

        Ok(Self {
            docker: Some(docker),
            container_id: Some(container_id),
            endpoint,
            access_key,
            secret_key,
            s3_client,
            keep_alive,
        })
    }

    async fn create_s3_client(endpoint: &str, access_key: &str, secret_key: &str) -> Result<S3Client> {
        let credentials = Credentials::new(access_key, secret_key, None, None, "test");

        let config = aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .endpoint_url(endpoint)
            .credentials_provider(credentials)
            .force_path_style(true)
            .build();

        Ok(S3Client::from_conf(config))
    }

    async fn wait_for_minio(client: &S3Client) -> Result<()> {
        info!("Waiting for MinIO to be ready...");
        let mut attempts = 0;
        let max_attempts = 30;

        loop {
            match client.list_buckets().send().await {
                Ok(_) => {
                    info!("MinIO is ready");
                    return Ok(());
                }
                Err(e) => {
                    attempts += 1;
                    if attempts >= max_attempts {
                        return Err(anyhow::anyhow!(
                            "MinIO failed to become ready after {} attempts: {}",
                            max_attempts,
                            e
                        ));
                    }
                    debug!("MinIO not ready yet (attempt {}): {}", attempts, e);
                    sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    /// Create a test bucket with a unique prefix
    pub async fn create_test_bucket(&self) -> Result<TestBucket> {
        let bucket_name = format!("e2e-test-{}", Uuid::new_v4());

        self.s3_client
            .create_bucket()
            .bucket(&bucket_name)
            .send()
            .await
            .context("Failed to create test bucket")?;

        info!("Created test bucket: {}", bucket_name);

        Ok(TestBucket {
            name: bucket_name,
            s3_client: self.s3_client.clone(),
        })
    }

    /// Get the MinIO endpoint URL
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Get the access key
    pub fn access_key(&self) -> &str {
        &self.access_key
    }

    /// Get the secret key
    pub fn secret_key(&self) -> &str {
        &self.secret_key
    }

    /// Get the S3 client for direct operations
    pub fn s3_client(&self) -> &S3Client {
        &self.s3_client
    }

    /// Stop and remove the container
    pub async fn stop(self) -> Result<()> {
        if self.keep_alive {
            info!("Keeping MinIO container alive (KEEP_MINIO is set)");
            return Ok(());
        }

        if let (Some(docker), Some(container_id)) = (&self.docker, &self.container_id) {
            info!("Stopping MinIO container: {}", container_id);
            docker
                .stop_container(container_id, None)
                .await
                .context("Failed to stop MinIO container")?;

            docker
                .remove_container(container_id, None)
                .await
                .context("Failed to remove MinIO container")?;
        }

        Ok(())
    }
}

/// A test bucket that cleans up on drop
pub struct TestBucket {
    pub name: String,
    s3_client: S3Client,
}

impl TestBucket {
    /// Get the bucket name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// List all objects in the bucket
    pub async fn list_objects(&self, prefix: Option<&str>) -> Result<Vec<String>> {
        let mut objects = Vec::new();
        let mut continuation_token = None;

        loop {
            let mut request = self.s3_client.list_objects_v2().bucket(&self.name);

            if let Some(p) = prefix {
                request = request.prefix(p);
            }
            if let Some(token) = continuation_token.take() {
                request = request.continuation_token(token);
            }

            let response = request.send().await?;

            if let Some(contents) = response.contents {
                for obj in contents {
                    if let Some(key) = obj.key {
                        objects.push(key);
                    }
                }
            }

            if response.is_truncated.unwrap_or(false) {
                continuation_token = response.next_continuation_token;
            } else {
                break;
            }
        }

        Ok(objects)
    }

    /// Get object content directly from S3
    pub async fn get_object(&self, key: &str) -> Result<Vec<u8>> {
        let response = self
            .s3_client
            .get_object()
            .bucket(&self.name)
            .key(key)
            .send()
            .await
            .context("Failed to get object")?;

        let data = response.body.collect().await?.into_bytes().to_vec();
        Ok(data)
    }

    /// Check if an object exists
    pub async fn object_exists(&self, key: &str) -> Result<bool> {
        match self.s3_client.head_object().bucket(&self.name).key(key).send().await {
            Ok(_) => Ok(true),
            Err(e) => {
                // Use debug format to get full error chain including causes
                let err_debug = format!("{:?}", e);
                if err_debug.contains("NotFound") || err_debug.contains("404") {
                    Ok(false)
                } else {
                    Err(e.into())
                }
            }
        }
    }

    /// Put an object directly into S3 (useful for test setup)
    pub async fn put_object(&self, key: &str, data: &[u8]) -> Result<()> {
        use aws_sdk_s3::primitives::ByteStream;

        self.s3_client
            .put_object()
            .bucket(&self.name)
            .key(key)
            .body(ByteStream::from(data.to_vec()))
            .send()
            .await
            .context("Failed to put object")?;

        Ok(())
    }

    /// Delete a specific object
    pub async fn delete_object(&self, key: &str) -> Result<()> {
        self.s3_client
            .delete_object()
            .bucket(&self.name)
            .key(key)
            .send()
            .await
            .context("Failed to delete object")?;

        Ok(())
    }

    /// Delete all objects and the bucket
    pub async fn cleanup(self) -> Result<()> {
        info!("Cleaning up test bucket: {}", self.name);

        // List and delete all objects
        let objects = self.list_objects(None).await?;
        for key in objects {
            self.s3_client
                .delete_object()
                .bucket(&self.name)
                .key(&key)
                .send()
                .await
                .context("Failed to delete object")?;
        }

        // Delete the bucket
        self.s3_client
            .delete_bucket()
            .bucket(&self.name)
            .send()
            .await
            .context("Failed to delete bucket")?;

        Ok(())
    }
}

/// Wrapper for shared MinIO container across tests
pub struct SharedMinio {
    inner: Arc<MinioContainer>,
}

impl SharedMinio {
    pub async fn get() -> Result<Arc<MinioContainer>> {
        // For now, just create a new one. In future, could use lazy_static or OnceCell
        // to share across tests in the same process
        Ok(Arc::new(MinioContainer::start().await?))
    }
}
