pub mod filesystem;
pub mod memory;
pub mod none;

use std::time::Duration;

use serde::Deserialize;

/// Cache configuration
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
#[derive(Default)]
pub enum CacheConfig {
    /// No caching
    #[default]
    None,
    /// In-memory cache with write-back support
    Memory {
        /// Maximum number of cached entries
        max_entries: Option<usize>,
        /// Max cache size (e.g., "100MB", "500MB")
        max_size: Option<String>,
        /// Flush interval for syncing dirty data to backend (e.g., "30s", "1m")
        #[serde(default)]
        #[serde(with = "humantime_serde")]
        flush_interval: Option<Duration>,
        /// Glob patterns for files to exclude from syncing to backend
        #[serde(default)]
        exclude_from_sync: Option<Vec<String>>,
    },
    /// Filesystem-backed cache
    Filesystem {
        path: String,
        /// Max cache size (e.g., "1GB", "500MB")
        max_size: Option<String>,
        /// Flush interval (e.g., "30s", "1m")
        #[serde(default)]
        #[serde(with = "humantime_serde")]
        flush_interval: Option<Duration>,
        /// Glob patterns for files to exclude from syncing to backend
        #[serde(default)]
        exclude_from_sync: Option<Vec<String>>,
    },
}

/// Parse size string like "1GB" to bytes
pub fn parse_size(s: &str) -> Option<u64> {
    let s = s.trim().to_uppercase();
    let (num_part, suffix) = if s.ends_with("GB") {
        (&s[..s.len() - 2], 1024 * 1024 * 1024)
    } else if s.ends_with("MB") {
        (&s[..s.len() - 2], 1024 * 1024)
    } else if s.ends_with("KB") {
        (&s[..s.len() - 2], 1024)
    } else if s.ends_with('B') {
        (&s[..s.len() - 1], 1)
    } else {
        (s.as_str(), 1)
    };

    num_part.trim().parse::<u64>().ok().map(|n| n * suffix)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_size() {
        assert_eq!(parse_size("1GB"), Some(1024 * 1024 * 1024));
        assert_eq!(parse_size("500MB"), Some(500 * 1024 * 1024));
        assert_eq!(parse_size("100KB"), Some(100 * 1024));
        assert_eq!(parse_size("1024B"), Some(1024));
        assert_eq!(parse_size("1024"), Some(1024));
    }
}
