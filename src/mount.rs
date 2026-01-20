//! Mount management and lifecycle

use std::path::PathBuf;
use std::sync::Arc;

use fuser::MountOption;
use parking_lot::Mutex;
use tokio::runtime::Handle;
use tracing::info;

use crate::connector::Connector;
use crate::error::{FuseAdapterError, Result};
use crate::fuse::FuseAdapter;

/// Represents an active mount
pub struct ActiveMount {
    /// Mount path
    pub path: PathBuf,
    /// Session handle (for unmounting)
    session: Option<fuser::BackgroundSession>,
}

impl ActiveMount {
    /// Create a new active mount
    fn new(path: PathBuf, session: fuser::BackgroundSession) -> Self {
        Self {
            path,
            session: Some(session),
        }
    }

    /// Unmount this filesystem
    pub fn unmount(&mut self) {
        if let Some(session) = self.session.take() {
            info!("Unmounting {:?}", self.path);
            drop(session);
        }
    }
}

impl Drop for ActiveMount {
    fn drop(&mut self) {
        self.unmount();
    }
}

/// Mount manager - handles lifecycle of all mounts
pub struct MountManager {
    /// Active mounts
    mounts: Mutex<Vec<ActiveMount>>,
    /// Tokio runtime handle
    handle: Handle,
}

impl MountManager {
    /// Create a new mount manager
    pub fn new(handle: Handle) -> Self {
        Self {
            mounts: Mutex::new(Vec::new()),
            handle,
        }
    }

    /// Mount a connector at the specified path
    ///
    /// If `read_only` is true, the mount will be read-only at the FUSE level,
    /// preventing any write operations regardless of connector capabilities.
    pub fn mount(
        &self,
        path: PathBuf,
        connector: Arc<dyn Connector>,
        read_only: bool,
    ) -> Result<()> {
        info!("Mounting at {:?}", path);

        // Ensure mount point exists
        if !path.exists() {
            return Err(FuseAdapterError::NotFound(format!(
                "Mount point does not exist: {:?}",
                path
            )));
        }

        if !path.is_dir() {
            return Err(FuseAdapterError::NotADirectory(format!(
                "Mount point is not a directory: {:?}",
                path
            )));
        }

        // Create the FUSE adapter
        let adapter = FuseAdapter::new(connector, self.handle.clone());

        // Configure mount options
        let mut options = vec![
            MountOption::FSName("fuse-adapter".to_string()),
            MountOption::AutoUnmount,
            MountOption::AllowOther,
            MountOption::DefaultPermissions,
        ];

        // Add read-only mount option if configured
        if read_only {
            info!("Mounting {:?} as read-only", path);
            options.push(MountOption::RO);
        }

        // Mount in background
        let session =
            fuser::spawn_mount2(adapter, &path, &options).map_err(FuseAdapterError::Io)?;

        // Track the mount
        let active = ActiveMount::new(path.clone(), session);
        self.mounts.lock().push(active);

        info!("Successfully mounted at {:?}", path);
        Ok(())
    }

    /// Unmount a specific path
    pub fn unmount(&self, path: &PathBuf) -> Result<()> {
        let mut mounts = self.mounts.lock();
        if let Some(pos) = mounts.iter().position(|m| &m.path == path) {
            let mut mount = mounts.remove(pos);
            mount.unmount();
            Ok(())
        } else {
            Err(FuseAdapterError::NotFound(format!(
                "No mount at {:?}",
                path
            )))
        }
    }

    /// Unmount all filesystems
    pub fn unmount_all(&self) {
        info!("Unmounting all filesystems");
        let mut mounts = self.mounts.lock();
        for mut mount in mounts.drain(..) {
            mount.unmount();
        }
    }

    /// Get list of active mount paths
    pub fn list_mounts(&self) -> Vec<PathBuf> {
        self.mounts.lock().iter().map(|m| m.path.clone()).collect()
    }

    /// Number of active mounts
    pub fn count(&self) -> usize {
        self.mounts.lock().len()
    }
}

impl Drop for MountManager {
    fn drop(&mut self) {
        self.unmount_all();
    }
}
