//! Read throughput benchmarks

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::time::Duration;

// Note: These benchmarks require a pre-configured mount point
// Set FUSE_MOUNT_PATH environment variable to point to a mounted fuse-adapter

fn get_mount_path() -> PathBuf {
    std::env::var("FUSE_MOUNT_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            eprintln!("FUSE_MOUNT_PATH not set, using /tmp/fuse-bench");
            PathBuf::from("/tmp/fuse-bench")
        })
}

fn setup_test_file(mount: &PathBuf, name: &str, size: usize) -> PathBuf {
    let path = mount.join(name);
    if !path.exists() || fs::metadata(&path).map(|m| m.len() as usize).unwrap_or(0) != size {
        let data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
        fs::write(&path, &data).expect("Failed to create test file");
    }
    path
}

fn bench_sequential_read(c: &mut Criterion) {
    let mount = get_mount_path();
    if !mount.exists() {
        eprintln!("Mount path does not exist, skipping benchmarks");
        return;
    }

    let mut group = c.benchmark_group("sequential_read");
    group.measurement_time(Duration::from_secs(10));

    let sizes = vec![
        ("1KB", 1024),
        ("64KB", 64 * 1024),
        ("1MB", 1024 * 1024),
        ("10MB", 10 * 1024 * 1024),
    ];

    for (name, size) in sizes {
        group.throughput(Throughput::Bytes(size as u64));

        let path = setup_test_file(&mount, &format!("bench_read_{}.bin", name), size);

        group.bench_with_input(BenchmarkId::new("full_read", name), &size, |b, _| {
            b.iter(|| {
                let data = fs::read(&path).unwrap();
                black_box(data.len());
            });
        });
    }

    group.finish();
}

fn bench_random_read(c: &mut Criterion) {
    let mount = get_mount_path();
    if !mount.exists() {
        return;
    }

    let mut group = c.benchmark_group("random_read");
    group.measurement_time(Duration::from_secs(10));

    let file_size = 10 * 1024 * 1024; // 10MB file
    let path = setup_test_file(&mount, "bench_random_read.bin", file_size);

    let read_sizes = vec![("4KB", 4096), ("64KB", 65536)];

    for (name, read_size) in read_sizes {
        group.throughput(Throughput::Bytes(read_size as u64));

        group.bench_with_input(BenchmarkId::new("random_offset", name), &read_size, |b, &size| {
            b.iter(|| {
                let mut file = File::open(&path).unwrap();
                let offset = (rand::random::<u64>() % (file_size as u64 - size as u64)) as u64;
                file.seek(SeekFrom::Start(offset)).unwrap();
                let mut buf = vec![0u8; size];
                file.read_exact(&mut buf).unwrap();
                black_box(buf.len());
            });
        });
    }

    group.finish();
}

fn bench_chunked_read(c: &mut Criterion) {
    let mount = get_mount_path();
    if !mount.exists() {
        return;
    }

    let mut group = c.benchmark_group("chunked_read");
    group.measurement_time(Duration::from_secs(10));

    let file_size = 1024 * 1024; // 1MB file
    let path = setup_test_file(&mount, "bench_chunked_read.bin", file_size);

    let chunk_sizes = vec![("4KB", 4096), ("64KB", 65536), ("256KB", 256 * 1024)];

    for (name, chunk_size) in chunk_sizes {
        group.throughput(Throughput::Bytes(file_size as u64));

        group.bench_with_input(BenchmarkId::new("chunk", name), &chunk_size, |b, &size| {
            b.iter(|| {
                let mut file = File::open(&path).unwrap();
                let mut buf = vec![0u8; size];
                let mut total = 0;
                loop {
                    let n = file.read(&mut buf).unwrap();
                    if n == 0 {
                        break;
                    }
                    total += n;
                }
                black_box(total);
            });
        });
    }

    group.finish();
}

fn bench_repeated_read(c: &mut Criterion) {
    let mount = get_mount_path();
    if !mount.exists() {
        return;
    }

    let mut group = c.benchmark_group("repeated_read");
    group.measurement_time(Duration::from_secs(10));

    let file_size = 64 * 1024; // 64KB file
    let path = setup_test_file(&mount, "bench_repeated_read.bin", file_size);

    group.throughput(Throughput::Bytes(file_size as u64));

    group.bench_function("same_file_10x", |b| {
        b.iter(|| {
            for _ in 0..10 {
                let data = fs::read(&path).unwrap();
                black_box(data.len());
            }
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_sequential_read,
    bench_random_read,
    bench_chunked_read,
    bench_repeated_read,
);
criterion_main!(benches);
