//! Write throughput benchmarks

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::fs::{self, File};
use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;

fn get_mount_path() -> PathBuf {
    std::env::var("FUSE_MOUNT_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("/tmp/fuse-bench"))
}

fn random_data(size: usize) -> Vec<u8> {
    (0..size).map(|i| (i % 256) as u8).collect()
}

fn bench_sequential_write(c: &mut Criterion) {
    let mount = get_mount_path();
    if !mount.exists() {
        eprintln!("Mount path does not exist, skipping benchmarks");
        return;
    }

    let mut group = c.benchmark_group("sequential_write");
    group.measurement_time(Duration::from_secs(10));

    let sizes = vec![
        ("1KB", 1024),
        ("64KB", 64 * 1024),
        ("1MB", 1024 * 1024),
        ("10MB", 10 * 1024 * 1024),
    ];

    for (name, size) in sizes {
        let data = random_data(size);
        group.throughput(Throughput::Bytes(size as u64));

        group.bench_with_input(BenchmarkId::new("full_write", name), &data, |b, data| {
            let path = mount.join(format!("bench_write_{}.bin", name));
            b.iter(|| {
                fs::write(&path, data).unwrap();
                black_box(());
            });
            let _ = fs::remove_file(&path);
        });
    }

    group.finish();
}

fn bench_small_files(c: &mut Criterion) {
    let mount = get_mount_path();
    if !mount.exists() {
        return;
    }

    let mut group = c.benchmark_group("small_files");
    group.measurement_time(Duration::from_secs(10));

    let file_size = 1024; // 1KB files
    let data = random_data(file_size);

    group.throughput(Throughput::Elements(1));

    group.bench_function("create_1kb_file", |b| {
        let mut counter = 0u64;
        b.iter(|| {
            let path = mount.join(format!("bench_small_{}.bin", counter));
            fs::write(&path, &data).unwrap();
            counter += 1;
            black_box(());
        });

        // Cleanup
        for i in 0..counter {
            let _ = fs::remove_file(mount.join(format!("bench_small_{}.bin", i)));
        }
    });

    group.finish();
}

fn bench_chunked_write(c: &mut Criterion) {
    let mount = get_mount_path();
    if !mount.exists() {
        return;
    }

    let mut group = c.benchmark_group("chunked_write");
    group.measurement_time(Duration::from_secs(10));

    let total_size = 1024 * 1024; // 1MB total
    group.throughput(Throughput::Bytes(total_size as u64));

    let chunk_sizes = vec![("4KB", 4096), ("64KB", 65536), ("256KB", 256 * 1024)];

    for (name, chunk_size) in chunk_sizes {
        let chunk = random_data(chunk_size);

        group.bench_with_input(BenchmarkId::new("chunk", name), &chunk, |b, chunk| {
            let path = mount.join(format!("bench_chunked_write_{}.bin", name));
            b.iter(|| {
                let mut file = File::create(&path).unwrap();
                let mut written = 0;
                while written < total_size {
                    let to_write = std::cmp::min(chunk.len(), total_size - written);
                    file.write_all(&chunk[..to_write]).unwrap();
                    written += to_write;
                }
                file.flush().unwrap();
                black_box(());
            });
            let _ = fs::remove_file(&path);
        });
    }

    group.finish();
}

fn bench_overwrite(c: &mut Criterion) {
    let mount = get_mount_path();
    if !mount.exists() {
        return;
    }

    let mut group = c.benchmark_group("overwrite");
    group.measurement_time(Duration::from_secs(10));

    let size = 64 * 1024; // 64KB
    let data = random_data(size);
    let path = mount.join("bench_overwrite.bin");

    // Create initial file
    fs::write(&path, &data).unwrap();

    group.throughput(Throughput::Bytes(size as u64));

    group.bench_function("overwrite_64kb", |b| {
        b.iter(|| {
            fs::write(&path, &data).unwrap();
            black_box(());
        });
    });

    let _ = fs::remove_file(&path);
    group.finish();
}

fn bench_create_delete(c: &mut Criterion) {
    let mount = get_mount_path();
    if !mount.exists() {
        return;
    }

    let mut group = c.benchmark_group("create_delete");
    group.measurement_time(Duration::from_secs(10));

    let size = 1024; // 1KB
    let data = random_data(size);

    group.throughput(Throughput::Elements(1));

    group.bench_function("create_and_delete", |b| {
        let mut counter = 0u64;
        b.iter(|| {
            let path = mount.join(format!("bench_cd_{}.bin", counter));
            fs::write(&path, &data).unwrap();
            fs::remove_file(&path).unwrap();
            counter += 1;
            black_box(());
        });
    });

    group.finish();
}

fn bench_many_files_in_dir(c: &mut Criterion) {
    let mount = get_mount_path();
    if !mount.exists() {
        return;
    }

    let mut group = c.benchmark_group("many_files");
    group.measurement_time(Duration::from_secs(15));
    group.sample_size(20); // Fewer samples for this expensive benchmark

    let data = random_data(512); // 512 bytes each
    let dir = mount.join("bench_many_files");

    group.throughput(Throughput::Elements(100));

    group.bench_function("create_100_files", |b| {
        b.iter(|| {
            let _ = fs::remove_dir_all(&dir);
            fs::create_dir(&dir).unwrap();

            for i in 0..100 {
                let path = dir.join(format!("file_{:04}.bin", i));
                fs::write(&path, &data).unwrap();
            }
            black_box(());
        });

        let _ = fs::remove_dir_all(&dir);
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_sequential_write,
    bench_small_files,
    bench_chunked_write,
    bench_overwrite,
    bench_create_delete,
    bench_many_files_in_dir,
);
criterion_main!(benches);
