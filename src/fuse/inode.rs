//! Inode management for path <-> inode mapping
//!
//! The FUSE interface works with inodes (numeric identifiers) while
//! connectors work with paths. This module provides bidirectional
//! mapping between the two.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;

/// Root directory inode (always 1 in FUSE)
pub const ROOT_INODE: u64 = 1;

/// Manages bidirectional inode <-> path mapping
pub struct InodeTable {
    /// Maps inode -> path
    inode_to_path: DashMap<u64, PathBuf>,
    /// Maps path -> inode
    path_to_inode: DashMap<PathBuf, u64>,
    /// Next inode number to allocate
    next_inode: AtomicU64,
}

impl InodeTable {
    /// Create a new inode table with root directory pre-registered
    pub fn new() -> Self {
        let table = Self {
            inode_to_path: DashMap::new(),
            path_to_inode: DashMap::new(),
            next_inode: AtomicU64::new(ROOT_INODE + 1),
        };

        // Register root directory
        let root_path = PathBuf::from("/");
        table.inode_to_path.insert(ROOT_INODE, root_path.clone());
        table.path_to_inode.insert(root_path, ROOT_INODE);

        table
    }

    /// Get inode for a path, creating one if it doesn't exist
    pub fn get_or_create_inode(&self, path: &Path) -> u64 {
        let normalized = normalize_path(path);

        // Check if already exists
        if let Some(inode) = self.path_to_inode.get(&normalized) {
            return *inode;
        }

        // Allocate new inode
        let inode = self.next_inode.fetch_add(1, Ordering::SeqCst);

        // Insert mappings (handle race condition)
        self.path_to_inode
            .entry(normalized.clone())
            .or_insert_with(|| {
                self.inode_to_path.insert(inode, normalized.clone());
                inode
            });

        // Return the actual inode (might be different if another thread won)
        *self.path_to_inode.get(&normalized).unwrap()
    }

    /// Get path for an inode
    pub fn get_path(&self, inode: u64) -> Option<PathBuf> {
        self.inode_to_path.get(&inode).map(|p| p.clone())
    }

    /// Get inode for a path (without creating)
    pub fn get_inode(&self, path: &Path) -> Option<u64> {
        let normalized = normalize_path(path);
        self.path_to_inode.get(&normalized).map(|i| *i)
    }

    /// Remove inode mapping for a path
    pub fn remove_path(&self, path: &Path) {
        let normalized = normalize_path(path);
        if let Some((_, inode)) = self.path_to_inode.remove(&normalized) {
            self.inode_to_path.remove(&inode);
        }
    }

    /// Rename a path, updating the inode mapping atomically
    pub fn rename_path(&self, old: &Path, new: &Path) {
        let old_normalized = normalize_path(old);
        let new_normalized = normalize_path(new);

        if let Some((_, inode)) = self.path_to_inode.remove(&old_normalized) {
            self.inode_to_path.insert(inode, new_normalized.clone());
            self.path_to_inode.insert(new_normalized, inode);
        }
    }

    /// Get the number of tracked inodes
    pub fn len(&self) -> usize {
        self.inode_to_path.len()
    }

    /// Check if the table is empty
    pub fn is_empty(&self) -> bool {
        self.inode_to_path.is_empty()
    }

    /// Clear all entries except root
    pub fn clear(&self) {
        self.inode_to_path.retain(|k, _| *k == ROOT_INODE);
        self.path_to_inode.retain(|_, v| *v == ROOT_INODE);
    }
}

impl Default for InodeTable {
    fn default() -> Self {
        Self::new()
    }
}

/// Normalize a path for consistent mapping
fn normalize_path(path: &Path) -> PathBuf {
    // Ensure path starts with /
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        PathBuf::from("/").join(path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_root_inode() {
        let table = InodeTable::new();
        assert_eq!(table.get_inode(Path::new("/")), Some(ROOT_INODE));
        assert_eq!(table.get_path(ROOT_INODE), Some(PathBuf::from("/")));
    }

    #[test]
    fn test_get_or_create() {
        let table = InodeTable::new();

        let inode1 = table.get_or_create_inode(Path::new("/foo"));
        let inode2 = table.get_or_create_inode(Path::new("/foo"));

        assert_eq!(inode1, inode2);
        assert_ne!(inode1, ROOT_INODE);
        assert_eq!(table.get_path(inode1), Some(PathBuf::from("/foo")));
    }

    #[test]
    fn test_remove() {
        let table = InodeTable::new();

        let inode = table.get_or_create_inode(Path::new("/foo"));
        assert!(table.get_path(inode).is_some());

        table.remove_path(Path::new("/foo"));
        assert!(table.get_path(inode).is_none());
        assert!(table.get_inode(Path::new("/foo")).is_none());
    }

    #[test]
    fn test_rename() {
        let table = InodeTable::new();

        let inode = table.get_or_create_inode(Path::new("/foo"));
        table.rename_path(Path::new("/foo"), Path::new("/bar"));

        assert!(table.get_inode(Path::new("/foo")).is_none());
        assert_eq!(table.get_inode(Path::new("/bar")), Some(inode));
        assert_eq!(table.get_path(inode), Some(PathBuf::from("/bar")));
    }
}
