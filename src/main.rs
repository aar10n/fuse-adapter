//! fuse-adapter daemon entry point

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tracing::{debug, error, info};
use tracing_subscriber::EnvFilter;

use fuse_adapter::cache::filesystem::{FilesystemCache, FilesystemCacheConfig};
use fuse_adapter::cache::memory::{MemoryCache, MemoryCacheConfig};
use fuse_adapter::cache::none::NoCache;
use fuse_adapter::cache::CacheConfig;
use fuse_adapter::config::{Config, ConnectorConfig, ErrorMode};
use fuse_adapter::connector::gdrive::GDriveConnector;
use fuse_adapter::connector::s3::S3Connector;
use fuse_adapter::connector::Connector;
use fuse_adapter::mount::MountManager;
use fuse_adapter::overlay::StatusOverlay;

/// Print usage information
fn print_usage() {
    eprintln!("Usage: fuse-adapter <config.yaml>");
    eprintln!();
    eprintln!("fuse-adapter - A FUSE filesystem framework with pluggable connectors");
    eprintln!();
    eprintln!("Arguments:");
    eprintln!("  config.yaml    Path to configuration file");
    eprintln!();
    eprintln!("Example:");
    eprintln!("  fuse-adapter /etc/fuse-adapter/config.yaml");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Install the default crypto provider for rustls
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    // Parse arguments
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 2 {
        print_usage();
        std::process::exit(1);
    }

    let config_path = PathBuf::from(&args[1]);

    // Load configuration
    let config = match Config::from_file(&config_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Failed to load config: {}", e);
            std::process::exit(1);
        }
    };

    // Validate configuration
    if let Err(e) = config.validate() {
        eprintln!("Configuration error: {}", e);
        std::process::exit(1);
    }

    // Initialize logging
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&config.logging.level));

    tracing_subscriber::fmt().with_env_filter(filter).init();

    info!("fuse-adapter starting");
    info!("Loaded configuration from {:?}", config_path);

    // Create mount manager
    let handle = tokio::runtime::Handle::current();
    let manager = Arc::new(MountManager::new(handle.clone()));

    // Set up signal handling for graceful shutdown
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    let m = manager.clone();

    ctrlc::set_handler(move || {
        info!("Received shutdown signal");
        r.store(false, Ordering::SeqCst);
        m.unmount_all();
    })?;

    // Mount all configured filesystems
    for mount_config in &config.mounts {
        info!("Setting up mount at {:?}", mount_config.path);

        // Use per-mount error_mode (already resolved from global default)
        let error_mode = mount_config.error_mode;
        let has_status_overlay = mount_config.status_overlay.is_some();

        // Try to create connector + cache
        let connector_result: Result<Arc<dyn Connector>, String> = match &mount_config.connector {
            ConnectorConfig::S3(s3_config) => match S3Connector::new(s3_config.clone()).await {
                Ok(s3) => match wrap_with_cache(s3, &mount_config.cache) {
                    Ok(c) => Ok(c),
                    Err(e) => Err(format!("Failed to create cache: {}", e)),
                },
                Err(e) => Err(format!("Failed to create S3 connector: {}", e)),
            },
            ConnectorConfig::GDrive(gdrive_config) => {
                match GDriveConnector::new(gdrive_config.clone()).await {
                    Ok(gdrive) => match wrap_with_cache(gdrive, &mount_config.cache) {
                        Ok(c) => Ok(c),
                        Err(e) => Err(format!("Failed to create cache: {}", e)),
                    },
                    Err(e) => Err(format!("Failed to create GDrive connector: {}", e)),
                }
            }
        };

        // Handle connector creation result
        let connector: Arc<dyn Connector> = match connector_result {
            Ok(c) => {
                // Wrap with status overlay if configured
                if let Some(ref overlay_config) = mount_config.status_overlay {
                    Arc::new(StatusOverlay::new(c, overlay_config.clone()))
                } else {
                    c
                }
            }
            Err(init_error) => {
                error!(
                    "Connector failed for {:?}: {}",
                    mount_config.path, init_error
                );

                // Can we mount with failed connector? Only if status_overlay is enabled and error_mode is Continue
                if has_status_overlay && error_mode == ErrorMode::Continue {
                    let overlay_config = mount_config.status_overlay.as_ref().unwrap();
                    Arc::new(StatusOverlay::new_failed(
                        init_error,
                        overlay_config.clone(),
                    ))
                } else {
                    if error_mode == ErrorMode::Exit {
                        std::process::exit(1);
                    }
                    continue; // Skip mount
                }
            }
        };

        // Create mount point directory if it doesn't exist
        if !mount_config.path.exists() {
            debug!("Creating mount point directory {:?}", mount_config.path);
            if let Err(e) = std::fs::create_dir_all(&mount_config.path) {
                error!(
                    "Failed to create mount point {:?}: {}",
                    mount_config.path, e
                );
                if error_mode == ErrorMode::Exit {
                    std::process::exit(1);
                }
                continue;
            }
        }

        // Mount the filesystem
        if let Err(e) = manager.mount(mount_config.path.clone(), connector, mount_config.read_only)
        {
            error!("Failed to mount {:?}: {}", mount_config.path, e);
            if error_mode == ErrorMode::Exit {
                std::process::exit(1);
            }
            continue;
        }
    }

    if manager.count() == 0 {
        error!("No filesystems were mounted successfully");
        std::process::exit(1);
    }

    info!("{} filesystem(s) mounted successfully", manager.count());
    info!("Press Ctrl+C to unmount and exit");

    // Wait for shutdown signal
    while running.load(Ordering::SeqCst) {
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }

    info!("Shutting down");
    manager.unmount_all();
    info!("All filesystems unmounted, exiting");

    Ok(())
}

/// Wrap a connector with the appropriate cache layer based on configuration
fn wrap_with_cache<C: Connector + 'static>(
    connector: C,
    cache_config: &CacheConfig,
) -> Result<Arc<dyn Connector>, Box<dyn std::error::Error>> {
    match cache_config {
        CacheConfig::None => Ok(Arc::new(NoCache::new(connector))),
        CacheConfig::Memory { max_entries } => {
            let config = MemoryCacheConfig {
                max_entries: max_entries.unwrap_or(1000),
            };
            Ok(Arc::new(MemoryCache::new(connector, config)))
        }
        CacheConfig::Filesystem {
            path,
            max_size,
            flush_interval,
        } => {
            let config = FilesystemCacheConfig {
                cache_dir: PathBuf::from(path),
                max_size: max_size
                    .as_ref()
                    .and_then(|s| fuse_adapter::cache::parse_size(s))
                    .unwrap_or(1024 * 1024 * 1024),
                flush_interval: flush_interval.unwrap_or(std::time::Duration::from_secs(30)),
                metadata_ttl: std::time::Duration::from_secs(60),
            };
            Ok(Arc::new(FilesystemCache::new(connector, config)))
        }
    }
}
