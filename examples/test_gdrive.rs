//! Test GDrive connector with HTTP auth - no FUSE needed
//!
//! Usage: cargo run --example test_gdrive [path]
//!
//! Examples:
//!   cargo run --example test_gdrive          # list root
//!   cargo run --example test_gdrive /Documents
//!   cargo run --example test_gdrive /Documents/test.txt

use std::collections::HashMap;
use std::path::Path;
use std::pin::Pin;

use futures::StreamExt;

use fuse_adapter::config::{GDriveAuthConfig, GDriveConnectorConfig};
use fuse_adapter::connector::gdrive::GDriveConnector;
use fuse_adapter::connector::{Connector, FileType};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    // Initialize logging
    tracing_subscriber::fmt().with_env_filter("debug").init();

    let test_path = std::env::args().nth(1).unwrap_or_else(|| "/".to_string());

    println!("=== GDrive Connector Test ===");
    println!();

    // Build config matching test.yaml
    let mut headers = HashMap::new();
    headers.insert(
        "X-Service-Auth".to_string(),
        "sandbox-helper:sandbox-helper-secret".to_string(),
    );

    let config = GDriveConnectorConfig {
        auth: GDriveAuthConfig::Http {
            endpoint: "http://localhost:8000/internal/v1/service/tool-auth/google_drive/token?user_id=f72b100a-1eb9-436b-b285-d46b329736df".to_string(),
            method: "GET".to_string(),
            headers,
        },
        root_folder_id: "root".to_string(),
        read_only: false,
    };

    println!("Creating GDrive connector...");
    println!("  Auth endpoint: http://localhost:8000/...");
    let connector = GDriveConnector::new(config).await?;
    println!("Connector created successfully!");
    println!();

    let path = Path::new(&test_path);

    // Test stat
    println!("=== stat(\"{}\") ===", test_path);
    match connector.stat(path).await {
        Ok(meta) => {
            println!("  is_dir: {}", meta.is_dir());
            println!("  size: {} bytes", meta.size);
            println!("  mtime: {:?}", meta.mtime);
        }
        Err(e) => println!("  Error: {}", e),
    }
    println!();

    // Test list_dir
    println!("=== list_dir(\"{}\") ===", test_path);
    let mut stream = Pin::from(connector.list_dir(path));
    let mut count = 0;
    while let Some(result) = stream.next().await {
        match result {
            Ok(entry) => {
                let t = if entry.file_type == FileType::Directory {
                    "dir "
                } else {
                    "file"
                };
                println!("  [{t}] {:?}", entry.name);
                count += 1;
                if count >= 25 {
                    println!("  ... (truncated)");
                    break;
                }
            }
            Err(e) => {
                println!("  Error: {}", e);
                break;
            }
        }
    }
    if count == 0 {
        println!("  (empty or not a directory)");
    }
    println!();

    // If path looks like a file, try reading it
    if !test_path.ends_with('/') && test_path != "/" {
        println!("=== read(\"{}\", 0, 1024) ===", test_path);
        match connector.read(path, 0, 1024).await {
            Ok(data) => {
                println!("  Read {} bytes", data.len());
                if let Ok(text) = std::str::from_utf8(&data) {
                    let preview: String = text.chars().take(200).collect();
                    println!("  Content preview: {:?}", preview);
                } else {
                    println!("  (binary data)");
                }
            }
            Err(e) => println!("  Error: {}", e),
        }
    }

    println!("=== Done ===");
    Ok(())
}
