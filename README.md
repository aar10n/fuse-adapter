# fuse-adapter

A FUSE filesystem framework in Rust with a pluggable connector architecture. Mount various storage backends (S3, Google Drive, etc.) as local filesystems.

## Features

- **Pluggable Connectors**: Implement the `Connector` trait to add new storage backends
- **Flexible Caching**: Optional cache layers for write buffering, read caching, and metadata caching
- **Multiple Mounts**: Single daemon manages multiple mount points
- **Async Architecture**: Built on Tokio for efficient async I/O
- **Capability System**: Connectors declare their capabilities; framework handles unsupported operations

## Architecture

```
┌─────────────────────────────────────────────────┐
│                fuse-adapter daemon              │
│  ┌───────────┐  ┌───────────┐  ┌───────────┐    │
│  │  Mount 1  │  │  Mount 2  │  │  Mount 3  │    │
│  │  /data    │  │  /import  │  │  /mydrive │    │
│  │  S3Conn   │  │  S3Conn   │  │  GDriveC  │    │
│  │  +Cache   │  │  (ro)     │  │  +Cache   │    │
│  └───────────┘  └───────────┘  └───────────┘    │
│                                                 │
│  ┌─────────────────────────────────────────┐    │
│  │           Shared Tokio Runtime          │    │
│  └─────────────────────────────────────────┘    │
└─────────────────────────────────────────────────┘
```

## Installation

### Prerequisites

- Rust 1.70+
- FUSE libraries (libfuse3-dev on Debian/Ubuntu, macfuse on macOS)
- For S3: AWS credentials configured

### Building

```bash
cargo build --release
```

## Quick Start (Local Testing)

The easiest way to test locally is with MinIO (S3-compatible storage):

```bash
# One command to start everything
make quickstart
```

This will:
1. Start a MinIO container
2. Create a test bucket
3. Set up mount directories
4. Run fuse-adapter

In another terminal, test it:
```bash
make test-s3
```

See all available commands:
```bash
make help
```

## Usage

1. Create a configuration file (see `config.example.yaml`):

```yaml
logging:
  level: info

mounts:
  - path: /mnt/s3-data
    connector:
      type: s3
      bucket: my-bucket
      region: us-east-1
    cache:
      type: filesystem
      path: /var/cache/fuse-adapter/s3
```

2. Create the mount point directory:

```bash
sudo mkdir -p /mnt/s3-data
```

3. Run the daemon:

```bash
./target/release/fuse-adapter config.yaml
```

4. Access your files at the mount point:

```bash
ls /mnt/s3-data
```

5. Press Ctrl+C to unmount and exit.

## Connectors

### S3 Connector

Mount Amazon S3 buckets or S3-compatible storage (MinIO, LocalStack).

**Capabilities:**
- Read: ✓
- Write: ✓ (requires cache layer for random writes)
- Range reads: ✓
- Random write: ✗ (handled by cache)
- Rename: ✗ (can be synthesized)
- Truncate: ✗ (handled by cache)

**Configuration:**
```yaml
connector:
  type: s3
  bucket: my-bucket
  region: us-east-1
  prefix: "optional/path/prefix/"
  endpoint: "http://localhost:9000"  # For S3-compatible stores
  force_path_style: true             # For MinIO, LocalStack
```

### Google Drive Connector

Mount Google Drive folders as local filesystems using service account authentication.

**Capabilities:**
- Read: ✓
- Write: ✓ (requires cache layer for random writes)
- Range reads: ✗
- Random write: ✗ (handled by cache)
- Rename: ✓
- Truncate: ✗ (handled by cache)

**Configuration:**
```yaml
connectors:
  gdrive:
    credentials_path: /etc/fuse-adapter/gdrive-service-account.json
    root_folder_id: "1ABC123DEF456_folder_id"

mounts:
  - path: /mnt/gdrive
    connector:
      type: gdrive
    cache:
      type: filesystem
      path: /var/cache/fuse-adapter/gdrive
```

**Setup:**
1. Create a service account in Google Cloud Console
2. Enable the Google Drive API
3. Download the credentials JSON file
4. Share the target Drive folder with the service account email

## Cache Layers

### No Cache

Direct passthrough to the connector. Not recommended for connectors without random write support.

```yaml
cache:
  type: none
```

### Memory Cache

In-memory LRU cache for metadata and small files.

```yaml
cache:
  type: memory
  max_entries: 1000
```

### Filesystem Cache

Persistent cache backed by local filesystem. Supports write buffering for connectors that don't support random writes.

```yaml
cache:
  type: filesystem
  path: /var/cache/fuse-adapter/mount-name
```

## Implementing a New Connector

See [docs/CONNECTOR_SKILL.md](docs/CONNECTOR_SKILL.md) for a comprehensive guide.

Quick overview:

1. Create a new file in `src/connector/`
2. Implement the `Connector` trait
3. Define capabilities and cache requirements
4. Add configuration struct
5. Register in `src/config.rs` and `src/main.rs`

## Development

### Makefile Commands

```bash
make help              # Show all commands

# Build
make build             # Debug build
make release           # Release build
make test              # Run tests

# MinIO (S3-compatible)
make minio-start       # Start MinIO container
make minio-stop        # Stop MinIO
make minio-setup       # Create test bucket
make minio-logs        # View logs

# LocalStack (AWS emulator)
make localstack-start  # Start LocalStack
make localstack-stop   # Stop LocalStack
make localstack-setup  # Create test bucket

# Run
make run-s3            # Run with MinIO
make run-s3-localstack # Run with LocalStack

# Test
make test-s3           # Basic integration tests
make test-write        # Test write operations
make test-stress       # Stress test with many files

# Cleanup
make unmount           # Unmount filesystem
make stop-all          # Stop all services
```

### Project Structure

```
fuse-adapter/
├── config/              # Configuration files
│   ├── s3.yaml          # MinIO config
│   └── s3-localstack.yaml
├── docs/
│   └── CONNECTOR_SKILL.md
├── src/
│   ├── main.rs
│   ├── lib.rs
│   ├── config.rs
│   ├── error.rs
│   ├── mount.rs
│   ├── connector/
│   │   ├── mod.rs
│   │   ├── s3.rs
│   │   └── gdrive.rs
│   ├── cache/
│   │   ├── mod.rs
│   │   ├── none.rs
│   │   ├── memory.rs
│   │   └── filesystem.rs
│   └── fuse/
│       ├── mod.rs
│       └── inode.rs
├── Cargo.toml
├── Makefile
└── README.md
```

## License

MIT
