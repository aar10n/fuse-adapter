pub mod inode;

use std::ffi::OsStr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use fuser::{
    FileAttr, FileType as FuseFileType, Filesystem, ReplyAttr, ReplyCreate, ReplyData,
    ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyWrite, Request, TimeOrNow,
};
use tokio::runtime::Handle;
use tracing::{debug, error, trace, warn};

use crate::connector::{Connector, FileType, Metadata};
use crate::error::FuseAdapterError;

use self::inode::{InodeTable, ROOT_INODE};

/// Default TTL for attribute caching (1 second)
const ATTR_TTL: Duration = Duration::from_secs(1);

/// Generation number (not used, always 0)
const GENERATION: u64 = 0;

/// Block size for reporting
const BLOCK_SIZE: u32 = 4096;

/// Convert our FileType to FUSE FileType
fn to_fuse_file_type(ft: FileType) -> FuseFileType {
    match ft {
        FileType::File => FuseFileType::RegularFile,
        FileType::Directory => FuseFileType::Directory,
    }
}

/// Convert Metadata to FileAttr
fn metadata_to_attr(ino: u64, meta: &Metadata, uid: u32, gid: u32) -> FileAttr {
    let kind = to_fuse_file_type(meta.file_type);
    let perm = meta.mode_or_default() as u16;
    let nlink = if meta.is_dir() { 2 } else { 1 };
    let blocks = meta.size.div_ceil(BLOCK_SIZE as u64);

    FileAttr {
        ino,
        size: meta.size,
        blocks,
        atime: meta.mtime,
        mtime: meta.mtime,
        ctime: meta.mtime,
        crtime: meta.mtime,
        kind,
        perm,
        nlink,
        uid,
        gid,
        rdev: 0,
        blksize: BLOCK_SIZE,
        flags: 0,
    }
}

/// FUSE filesystem implementation that delegates to a Connector
pub struct FuseAdapter {
    connector: Arc<dyn Connector>,
    inodes: InodeTable,
    /// Dedicated runtime for FUSE async operations
    runtime: tokio::runtime::Runtime,
    /// User ID to report for all files (defaults to process uid)
    uid: u32,
    /// Group ID to report for all files (defaults to process gid)
    gid: u32,
}

impl FuseAdapter {
    /// Create a new FuseAdapter wrapping the given connector
    ///
    /// # Arguments
    /// * `connector` - The connector to delegate operations to
    /// * `_handle` - Tokio runtime handle (unused, kept for API compatibility)
    /// * `uid` - Optional user ID to report for all files (defaults to process uid)
    /// * `gid` - Optional group ID to report for all files (defaults to process gid)
    pub fn new(
        connector: Arc<dyn Connector>,
        _handle: Handle,
        uid: Option<u32>,
        gid: Option<u32>,
    ) -> Self {
        // Create a dedicated multi-threaded runtime for FUSE operations
        // This ensures async I/O is properly driven without interference
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .expect("Failed to create FUSE runtime");

        // Use configured uid/gid or fall back to process owner
        let uid = uid.unwrap_or_else(|| unsafe { libc::getuid() });
        let gid = gid.unwrap_or_else(|| unsafe { libc::getgid() });

        Self {
            connector,
            inodes: InodeTable::new(),
            runtime,
            uid,
            gid,
        }
    }

    /// Get path for inode, returning ENOENT if not found
    fn inode_to_path(&self, ino: u64) -> Result<PathBuf, i32> {
        self.inodes.get_path(ino).ok_or(libc::ENOENT)
    }

    /// Check if operation is supported, returning appropriate error
    fn check_write_capability(&self) -> Result<(), i32> {
        if !self.connector.capabilities().write {
            return Err(libc::EROFS);
        }
        Ok(())
    }

    fn check_rename_capability(&self) -> Result<(), i32> {
        if !self.connector.capabilities().rename {
            return Err(libc::ENOSYS);
        }
        Ok(())
    }

    fn check_truncate_capability(&self) -> Result<(), i32> {
        if !self.connector.capabilities().truncate {
            return Err(libc::ENOSYS);
        }
        Ok(())
    }

    fn check_set_mode_capability(&self) -> Result<(), i32> {
        if !self.connector.capabilities().set_mode {
            return Err(libc::ENOSYS);
        }
        Ok(())
    }

    /// Run an async operation on the dedicated FUSE runtime and wait for the result.
    /// Uses block_on which properly drives the runtime's I/O driver.
    fn run_async<F, T>(&self, future: F) -> T
    where
        F: std::future::Future<Output = T>,
    {
        self.runtime.block_on(future)
    }
}

impl Filesystem for FuseAdapter {
    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let parent_path = match self.inode_to_path(parent) {
            Ok(p) => p,
            Err(e) => {
                reply.error(e);
                return;
            }
        };

        let path = parent_path.join(name);
        trace!("lookup: {:?}", path);

        let connector = self.connector.clone();
        let path_for_async = path.clone();
        match self.run_async(async move { connector.stat(&path_for_async).await }) {
            Ok(meta) => {
                let ino = self.inodes.get_or_create_inode(&path);
                let attr = metadata_to_attr(ino, &meta, self.uid, self.gid);
                reply.entry(&ATTR_TTL, &attr, GENERATION);
            }
            Err(FuseAdapterError::NotFound(_)) => {
                reply.error(libc::ENOENT);
            }
            Err(e) => {
                error!("lookup error for {:?}: {}", path, e);
                reply.error(e.to_errno());
            }
        }
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyAttr) {
        let path = match self.inode_to_path(ino) {
            Ok(p) => p,
            Err(e) => {
                reply.error(e);
                return;
            }
        };

        trace!("getattr: {:?} (ino={})", path, ino);

        let connector = self.connector.clone();
        let path_for_async = path.clone();
        match self.run_async(async move { connector.stat(&path_for_async).await }) {
            Ok(meta) => {
                let attr = metadata_to_attr(ino, &meta, self.uid, self.gid);
                reply.attr(&ATTR_TTL, &attr);
            }
            Err(e) => {
                debug!("getattr error for {:?}: {}", path, e);
                reply.error(e.to_errno());
            }
        }
    }

    fn setattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<TimeOrNow>,
        _mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        let path = match self.inode_to_path(ino) {
            Ok(p) => p,
            Err(e) => {
                reply.error(e);
                return;
            }
        };

        // Handle mode change (chmod)
        if let Some(new_mode) = mode {
            if let Err(e) = self.check_set_mode_capability() {
                reply.error(e);
                return;
            }

            trace!("setattr chmod: {:?} to {:o}", path, new_mode);

            let connector = self.connector.clone();
            let path_for_async = path.clone();
            // Extract just the permission bits (lower 12 bits)
            let perm_bits = new_mode & 0o7777;
            match self.run_async(async move {
                connector.set_mode(&path_for_async, perm_bits).await?;
                connector.stat(&path_for_async).await
            }) {
                Ok(meta) => {
                    let attr = metadata_to_attr(ino, &meta, self.uid, self.gid);
                    reply.attr(&ATTR_TTL, &attr);
                }
                Err(e) => {
                    error!("setattr chmod error for ino {}: {}", ino, e);
                    reply.error(e.to_errno());
                }
            }
            return;
        }

        // Handle truncate (size change)
        if let Some(new_size) = size {
            if let Err(e) = self.check_truncate_capability() {
                reply.error(e);
                return;
            }

            trace!("setattr truncate: {:?} to {} bytes", path, new_size);

            let connector = self.connector.clone();
            match self.run_async(async move {
                connector.truncate(&path, new_size).await?;
                connector.stat(&path).await
            }) {
                Ok(meta) => {
                    let attr = metadata_to_attr(ino, &meta, self.uid, self.gid);
                    reply.attr(&ATTR_TTL, &attr);
                }
                Err(e) => {
                    error!("setattr error for ino {}: {}", ino, e);
                    reply.error(e.to_errno());
                }
            }
            return;
        }

        // No changes requested, just return current attributes
        self.getattr(_req, ino, reply);
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        let path = match self.inode_to_path(ino) {
            Ok(p) => p,
            Err(e) => {
                reply.error(e);
                return;
            }
        };

        trace!("read: {:?} offset={} size={}", path, offset, size);

        let connector = self.connector.clone();
        let path_for_async = path.clone();
        match self
            .run_async(async move { connector.read(&path_for_async, offset as u64, size).await })
        {
            Ok(data) => {
                reply.data(&data);
            }
            Err(e) => {
                error!("read error for {:?}: {}", path, e);
                reply.error(e.to_errno());
            }
        }
    }

    fn write(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        if let Err(e) = self.check_write_capability() {
            reply.error(e);
            return;
        }

        let path = match self.inode_to_path(ino) {
            Ok(p) => p,
            Err(e) => {
                reply.error(e);
                return;
            }
        };

        trace!("write: {:?} offset={} size={}", path, offset, data.len());

        let connector = self.connector.clone();
        let data = data.to_vec();
        let path_for_async = path.clone();
        match self
            .run_async(async move { connector.write(&path_for_async, offset as u64, &data).await })
        {
            Ok(written) => {
                reply.written(written as u32);
            }
            Err(e) => {
                error!("write error for {:?}: {}", path, e);
                reply.error(e.to_errno());
            }
        }
    }

    fn create(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        if let Err(e) = self.check_write_capability() {
            reply.error(e);
            return;
        }

        let parent_path = match self.inode_to_path(parent) {
            Ok(p) => p,
            Err(e) => {
                reply.error(e);
                return;
            }
        };

        let path = parent_path.join(name);
        // Apply umask to get effective mode (permission bits only)
        let effective_mode = (mode & !umask) & 0o7777;
        debug!("create: {:?} mode={:o}", path, effective_mode);

        let connector = self.connector.clone();
        let path_for_async = path.clone();
        match self.run_async(async move {
            connector
                .create_file_with_mode(&path_for_async, effective_mode)
                .await?;
            connector.stat(&path_for_async).await
        }) {
            Ok(meta) => {
                let ino = self.inodes.get_or_create_inode(&path);
                let attr = metadata_to_attr(ino, &meta, self.uid, self.gid);
                reply.created(&ATTR_TTL, &attr, GENERATION, 0, 0);
            }
            Err(e) => {
                error!("create error for {:?}: {}", path, e);
                reply.error(e.to_errno());
            }
        }
    }

    fn mkdir(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        reply: ReplyEntry,
    ) {
        if let Err(e) = self.check_write_capability() {
            reply.error(e);
            return;
        }

        let parent_path = match self.inode_to_path(parent) {
            Ok(p) => p,
            Err(e) => {
                reply.error(e);
                return;
            }
        };

        let path = parent_path.join(name);
        // Apply umask to get effective mode (permission bits only)
        let effective_mode = (mode & !umask) & 0o7777;
        debug!("mkdir: {:?} mode={:o}", path, effective_mode);

        let connector = self.connector.clone();
        let path_for_async = path.clone();
        match self.run_async(async move {
            connector
                .create_dir_with_mode(&path_for_async, effective_mode)
                .await?;
            connector.stat(&path_for_async).await
        }) {
            Ok(meta) => {
                let ino = self.inodes.get_or_create_inode(&path);
                let attr = metadata_to_attr(ino, &meta, self.uid, self.gid);
                reply.entry(&ATTR_TTL, &attr, GENERATION);
            }
            Err(e) => {
                error!("mkdir error for {:?}: {}", path, e);
                reply.error(e.to_errno());
            }
        }
    }

    fn unlink(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        if let Err(e) = self.check_write_capability() {
            reply.error(e);
            return;
        }

        let parent_path = match self.inode_to_path(parent) {
            Ok(p) => p,
            Err(e) => {
                reply.error(e);
                return;
            }
        };

        let path = parent_path.join(name);
        debug!("unlink: {:?}", path);

        let connector = self.connector.clone();
        let path_for_async = path.clone();
        match self.run_async(async move { connector.remove_file(&path_for_async).await }) {
            Ok(()) => {
                self.inodes.remove_path(&path);
                reply.ok();
            }
            Err(e) => {
                error!("unlink error for {:?}: {}", path, e);
                reply.error(e.to_errno());
            }
        }
    }

    fn rmdir(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        if let Err(e) = self.check_write_capability() {
            reply.error(e);
            return;
        }

        let parent_path = match self.inode_to_path(parent) {
            Ok(p) => p,
            Err(e) => {
                reply.error(e);
                return;
            }
        };

        let path = parent_path.join(name);
        debug!("rmdir: {:?}", path);

        let connector = self.connector.clone();
        let path_for_async = path.clone();
        match self.run_async(async move { connector.remove_dir(&path_for_async, false).await }) {
            Ok(()) => {
                self.inodes.remove_path(&path);
                reply.ok();
            }
            Err(e) => {
                error!("rmdir error for {:?}: {}", path, e);
                reply.error(e.to_errno());
            }
        }
    }

    fn rename(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        _flags: u32,
        reply: ReplyEmpty,
    ) {
        if let Err(e) = self.check_rename_capability() {
            reply.error(e);
            return;
        }

        let parent_path = match self.inode_to_path(parent) {
            Ok(p) => p,
            Err(e) => {
                reply.error(e);
                return;
            }
        };

        let new_parent_path = match self.inode_to_path(newparent) {
            Ok(p) => p,
            Err(e) => {
                reply.error(e);
                return;
            }
        };

        let old_path = parent_path.join(name);
        let new_path = new_parent_path.join(newname);
        debug!("rename: {:?} -> {:?}", old_path, new_path);

        let connector = self.connector.clone();
        let old_path_for_async = old_path.clone();
        let new_path_for_async = new_path.clone();
        match self.run_async(async move {
            connector
                .rename(&old_path_for_async, &new_path_for_async)
                .await
        }) {
            Ok(()) => {
                self.inodes.rename_path(&old_path, &new_path);
                reply.ok();
            }
            Err(e) => {
                error!("rename error {:?} -> {:?}: {}", old_path, new_path, e);
                reply.error(e.to_errno());
            }
        }
    }

    fn open(&mut self, _req: &Request<'_>, _ino: u64, _flags: i32, reply: ReplyOpen) {
        // Stateless - just return success with a dummy file handle
        reply.opened(0, 0);
    }

    fn release(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        // Stateless - nothing to do
        reply.ok();
    }

    fn opendir(&mut self, _req: &Request<'_>, _ino: u64, _flags: i32, reply: ReplyOpen) {
        // Stateless - just return success
        reply.opened(0, 0);
    }

    fn releasedir(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _flags: i32,
        reply: ReplyEmpty,
    ) {
        // Stateless - nothing to do
        reply.ok();
    }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let path = match self.inode_to_path(ino) {
            Ok(p) => p,
            Err(e) => {
                reply.error(e);
                return;
            }
        };

        trace!("readdir: {:?} offset={}", path, offset);

        let connector = self.connector.clone();
        let path_for_async = path.clone();

        use futures::StreamExt;

        let entries: Vec<_> = self.run_async(async move {
            let stream = connector.list_dir(&path_for_async);
            stream.collect().await
        });

        // Add . and ..
        let mut idx = 0i64;

        if offset <= idx && reply.add(ino, idx + 1, FuseFileType::Directory, ".") {
            reply.ok();
            return;
        }
        idx += 1;

        if offset <= idx {
            let parent_ino = if ino == ROOT_INODE {
                ROOT_INODE
            } else {
                // Get parent inode
                path.parent()
                    .and_then(|p| self.inodes.get_inode(p))
                    .unwrap_or(ROOT_INODE)
            };
            if reply.add(parent_ino, idx + 1, FuseFileType::Directory, "..") {
                reply.ok();
                return;
            }
        }
        idx += 1;

        for entry_result in entries {
            match entry_result {
                Ok(entry) => {
                    if offset <= idx {
                        let entry_path = path.join(&entry.name);
                        let entry_ino = self.inodes.get_or_create_inode(&entry_path);
                        let ft = to_fuse_file_type(entry.file_type);

                        if reply.add(entry_ino, idx + 1, ft, &entry.name) {
                            // Buffer full
                            reply.ok();
                            return;
                        }
                    }
                    idx += 1;
                }
                Err(e) => {
                    warn!("readdir entry error: {}", e);
                    // Continue with other entries
                }
            }
        }

        reply.ok();
    }

    fn fsync(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        let path = match self.inode_to_path(ino) {
            Ok(p) => p,
            Err(e) => {
                reply.error(e);
                return;
            }
        };

        trace!("fsync: {:?}", path);

        let connector = self.connector.clone();
        let path_for_async = path.clone();
        match self.run_async(async move { connector.flush(&path_for_async).await }) {
            Ok(()) => reply.ok(),
            Err(e) => {
                error!("fsync error for {:?}: {}", path, e);
                reply.error(e.to_errno());
            }
        }
    }

    fn flush(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        _lock_owner: u64,
        reply: ReplyEmpty,
    ) {
        let path = match self.inode_to_path(ino) {
            Ok(p) => p,
            Err(e) => {
                reply.error(e);
                return;
            }
        };

        trace!("flush: {:?}", path);

        let connector = self.connector.clone();
        let path_for_async = path.clone();
        match self.run_async(async move { connector.flush(&path_for_async).await }) {
            Ok(()) => reply.ok(),
            Err(e) => {
                error!("flush error for {:?}: {}", path, e);
                reply.error(e.to_errno());
            }
        }
    }

    fn access(&mut self, _req: &Request<'_>, ino: u64, _mask: i32, reply: ReplyEmpty) {
        // Check if file exists
        let path = match self.inode_to_path(ino) {
            Ok(p) => p,
            Err(e) => {
                reply.error(e);
                return;
            }
        };

        let connector = self.connector.clone();
        match self.run_async(async move { connector.exists(&path).await }) {
            Ok(true) => reply.ok(),
            Ok(false) => reply.error(libc::ENOENT),
            Err(e) => reply.error(e.to_errno()),
        }
    }

    fn statfs(&mut self, _req: &Request<'_>, _ino: u64, reply: fuser::ReplyStatfs) {
        // Return dummy filesystem stats
        reply.statfs(
            u64::MAX,   // blocks
            u64::MAX,   // bfree
            u64::MAX,   // bavail
            u64::MAX,   // files
            u64::MAX,   // ffree
            BLOCK_SIZE, // bsize
            255,        // namelen
            BLOCK_SIZE, // frsize
        );
    }
}

/// Get current time or UNIX epoch as fallback
#[allow(dead_code)]
pub fn current_time() -> SystemTime {
    SystemTime::now()
}
