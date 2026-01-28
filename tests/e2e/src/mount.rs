//! Mount management for e2e tests
//!
//! Handles starting/stopping the fuse-adapter process and waiting for mounts.

use crate::config::TestConfig;
use anyhow::{Context, Result};
use nix::sys::signal::{self, Signal};
use nix::unistd::Pid;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;
use tokio::time::{sleep, timeout};
use tracing::{debug, error, info, warn};

/// Default timeout for waiting for mounts to become ready
const DEFAULT_MOUNT_TIMEOUT: Duration = Duration::from_secs(30);

/// How often to poll for mount readiness
const POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Manages a running fuse-adapter process
pub struct MountedAdapter {
    process: Child,
    config_path: PathBuf,
    mount_points: Vec<PathBuf>,
    stopped: bool,
}

impl MountedAdapter {
    /// Start fuse-adapter with the given configuration
    pub async fn start(config: &TestConfig, config_path: &Path) -> Result<Self> {
        // Write config to file
        config.write_to_file(config_path)?;

        // In CI, log the config for debugging
        if std::env::var("CI").is_ok() {
            if let Ok(config_content) = std::fs::read_to_string(config_path) {
                debug!("Config file content:\n{}", config_content);
            }
        }

        // Collect mount points
        let mount_points: Vec<PathBuf> = config.mounts.iter().map(|m| m.path.clone()).collect();

        // Create mount directories
        for mount_point in &mount_points {
            if !mount_point.exists() {
                std::fs::create_dir_all(mount_point)
                    .with_context(|| format!("Failed to create mount point: {:?}", mount_point))?;
            }
        }

        // Find the fuse-adapter binary
        let binary = find_fuse_adapter_binary()?;
        info!("Using fuse-adapter binary: {:?}", binary);

        // Start the process with MinIO credentials from environment
        let access_key =
            std::env::var("MINIO_ACCESS_KEY").unwrap_or_else(|_| "minioadmin".to_string());
        let secret_key =
            std::env::var("MINIO_SECRET_KEY").unwrap_or_else(|_| "minioadmin".to_string());

        // In CI, capture output for debugging; locally, discard for cleaner output
        let is_ci = std::env::var("CI").is_ok();
        let (stdout, stderr) = if is_ci {
            (Stdio::inherit(), Stdio::inherit())
        } else {
            (Stdio::null(), Stdio::null())
        };

        let mut process = Command::new(&binary)
            .arg(config_path)
            .env("AWS_ACCESS_KEY_ID", &access_key)
            .env("AWS_SECRET_ACCESS_KEY", &secret_key)
            .stdout(stdout)
            .stderr(stderr)
            .spawn()
            .with_context(|| format!("Failed to start fuse-adapter: {:?}", binary))?;

        let pid = process.id();
        info!("Started fuse-adapter with PID {}", pid);

        // In CI, wait a moment and check if process is still running
        if std::env::var("CI").is_ok() {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            match process.try_wait() {
                Ok(Some(status)) => {
                    error!("fuse-adapter exited immediately with status: {:?}", status);
                }
                Ok(None) => {
                    debug!("fuse-adapter is running after 500ms");
                }
                Err(e) => {
                    error!("Failed to check process status: {}", e);
                }
            }
        }

        let adapter = Self {
            process,
            config_path: config_path.to_path_buf(),
            mount_points,
            stopped: false,
        };

        // Wait for mounts to be ready
        adapter.wait_ready(DEFAULT_MOUNT_TIMEOUT).await?;

        Ok(adapter)
    }

    /// Wait for all mount points to become ready
    pub async fn wait_ready(&self, max_wait: Duration) -> Result<()> {
        info!(
            "Waiting for {} mount(s) to become ready...",
            self.mount_points.len()
        );

        timeout(max_wait, async {
            for mount_point in &self.mount_points {
                self.wait_mount_ready(mount_point).await?;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await
        .map_err(|_| anyhow::anyhow!("Timeout waiting for mounts after {:?}", max_wait))??;

        info!("All mounts are ready");
        Ok(())
    }

    /// Wait for a single mount point to be ready
    async fn wait_mount_ready(&self, mount_point: &Path) -> Result<()> {
        loop {
            if is_mount_ready(mount_point) {
                debug!("Mount {:?} is ready", mount_point);
                return Ok(());
            }

            // Check if process is still running
            if !self.is_running() {
                return Err(anyhow::anyhow!(
                    "fuse-adapter process exited before mount {:?} was ready",
                    mount_point
                ));
            }

            sleep(POLL_INTERVAL).await;
        }
    }

    /// Check if the adapter process is still running
    pub fn is_running(&self) -> bool {
        // Try to get process status without blocking
        match Command::new("kill")
            .arg("-0")
            .arg(self.process.id().to_string())
            .output()
        {
            Ok(output) => output.status.success(),
            Err(_) => false,
        }
    }

    /// Get the path to a mount point by index
    pub fn mount_path(&self, index: usize) -> Option<&Path> {
        self.mount_points.get(index).map(|p| p.as_path())
    }

    /// Get the first (or only) mount point
    pub fn mount(&self) -> &Path {
        &self.mount_points[0]
    }

    /// Get all mount points
    pub fn mount_points(&self) -> &[PathBuf] {
        &self.mount_points
    }

    /// Get the PID of the adapter process
    pub fn pid(&self) -> u32 {
        self.process.id()
    }

    /// Stop the adapter gracefully
    pub async fn stop(&mut self) -> Result<()> {
        if self.stopped {
            return Ok(());
        }

        info!("Stopping fuse-adapter (PID {})", self.process.id());

        // Send SIGTERM for graceful shutdown
        let pid = Pid::from_raw(self.process.id() as i32);
        if let Err(e) = signal::kill(pid, Signal::SIGTERM) {
            warn!("Failed to send SIGTERM: {}", e);
        }

        // Wait for process to exit (with timeout)
        let wait_result = timeout(Duration::from_secs(10), async {
            loop {
                if !self.is_running() {
                    return;
                }
                sleep(Duration::from_millis(100)).await;
            }
        })
        .await;

        if wait_result.is_err() {
            // Force kill if graceful shutdown failed
            warn!("Graceful shutdown timed out, sending SIGKILL");
            if let Err(e) = signal::kill(pid, Signal::SIGKILL) {
                error!("Failed to send SIGKILL: {}", e);
            }
            let _ = self.process.wait();
        }

        self.stopped = true;

        // Unmount any remaining mounts
        for mount_point in &self.mount_points {
            if is_mount_ready(mount_point) {
                unmount(mount_point).await?;
            }
        }

        info!("fuse-adapter stopped");
        Ok(())
    }

    /// Restart the adapter (useful for persistence tests)
    /// Note: This consumes self and returns a new adapter
    pub async fn restart(mut self, config: &TestConfig) -> Result<Self> {
        self.stop().await?;

        // Small delay to ensure cleanup
        sleep(Duration::from_millis(500)).await;

        // Start fresh adapter with same config
        Self::start(config, &self.config_path).await
    }
}

impl Drop for MountedAdapter {
    fn drop(&mut self) {
        if !self.stopped {
            // Best-effort cleanup in drop
            let pid = Pid::from_raw(self.process.id() as i32);
            let _ = signal::kill(pid, Signal::SIGKILL);
            let _ = self.process.wait();

            // Try to unmount
            for mount_point in &self.mount_points {
                let _ = std::process::Command::new(unmount_command())
                    .arg(mount_point)
                    .output();
            }
        }
    }
}

/// Find the fuse-adapter binary
fn find_fuse_adapter_binary() -> Result<PathBuf> {
    // Check for FUSE_ADAPTER_BINARY env var
    if let Ok(path) = std::env::var("FUSE_ADAPTER_BINARY") {
        let path = PathBuf::from(path);
        if path.exists() {
            return Ok(path);
        }
    }

    // Check target/release first (preferred for e2e tests)
    let release_path = PathBuf::from("target/release/fuse-adapter");
    if release_path.exists() {
        return Ok(release_path);
    }

    // Fall back to target/debug
    let debug_path = PathBuf::from("target/debug/fuse-adapter");
    if debug_path.exists() {
        return Ok(debug_path);
    }

    // Try to find via cargo metadata
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_string());
    let workspace_release = PathBuf::from(&manifest_dir)
        .parent()
        .and_then(|p| p.parent())
        .map(|p| p.join("target/release/fuse-adapter"));

    if let Some(path) = workspace_release {
        if path.exists() {
            return Ok(path);
        }
    }

    let workspace_debug = PathBuf::from(&manifest_dir)
        .parent()
        .and_then(|p| p.parent())
        .map(|p| p.join("target/debug/fuse-adapter"));

    if let Some(path) = workspace_debug {
        if path.exists() {
            return Ok(path);
        }
    }

    Err(anyhow::anyhow!(
        "Could not find fuse-adapter binary. Run 'cargo build --release' first."
    ))
}

/// Check if a mount point is ready
fn is_mount_ready(path: &Path) -> bool {
    // On macOS and Linux, we can check if the path is a mount point
    // by comparing device IDs with the parent
    if !path.exists() {
        debug!("Mount path {:?} does not exist", path);
        return false;
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;

        let path_meta = match std::fs::metadata(path) {
            Ok(m) => m,
            Err(e) => {
                debug!("Failed to stat mount path {:?}: {}", path, e);
                return false;
            }
        };

        let parent = match path.parent() {
            Some(p) => p,
            None => {
                debug!("No parent for path {:?}", path);
                return false;
            }
        };

        let parent_meta = match std::fs::metadata(parent) {
            Ok(m) => m,
            Err(e) => {
                debug!("Failed to stat parent {:?}: {}", parent, e);
                return false;
            }
        };

        let is_ready = path_meta.dev() != parent_meta.dev();
        debug!(
            "Mount check {:?}: path_dev={}, parent_dev={}, ready={}",
            path,
            path_meta.dev(),
            parent_meta.dev(),
            is_ready
        );
        is_ready
    }

    #[cfg(not(unix))]
    {
        // Non-Unix fallback: just check if we can list the directory
        path.read_dir().is_ok()
    }
}

/// Get the unmount command for the current platform
fn unmount_command() -> &'static str {
    #[cfg(target_os = "macos")]
    {
        "umount"
    }
    #[cfg(target_os = "linux")]
    {
        "fusermount3"
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        "umount"
    }
}

/// Unmount a mount point
async fn unmount(path: &Path) -> Result<()> {
    info!("Unmounting {:?}", path);

    #[cfg(target_os = "linux")]
    {
        let output = Command::new("fusermount3")
            .arg("-u")
            .arg(path)
            .output()
            .context("Failed to run fusermount3")?;

        if !output.status.success() {
            // Try lazy unmount
            let output = Command::new("fusermount3")
                .arg("-uz")
                .arg(path)
                .output()
                .context("Failed to run fusermount3 lazy unmount")?;

            if !output.status.success() {
                warn!(
                    "fusermount3 lazy unmount failed: {}",
                    String::from_utf8_lossy(&output.stderr)
                );
            }
        }
    }

    #[cfg(target_os = "macos")]
    {
        let output = Command::new("umount")
            .arg(path)
            .output()
            .context("Failed to run umount")?;

        if !output.status.success() {
            // Try force unmount
            let output = Command::new("umount")
                .arg("-f")
                .arg(path)
                .output()
                .context("Failed to run umount -f")?;

            if !output.status.success() {
                warn!(
                    "Force unmount failed: {}",
                    String::from_utf8_lossy(&output.stderr)
                );
            }
        }
    }

    Ok(())
}

/// Force unmount all paths (for cleanup)
pub async fn force_unmount_all(paths: &[PathBuf]) -> Result<()> {
    for path in paths {
        if path.exists() {
            let _ = unmount(path).await;
        }
    }
    Ok(())
}
