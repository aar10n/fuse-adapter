//! Metadata operation benchmarks

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::fs::{self, File, Permissions};
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::time::Duration;

fn get_mount_path() -> PathBuf {
    std::env::var("FUSE_MOUNT_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("/tmp/fuse-bench"))
}

fn setup_files(mount: &PathBuf, prefix: &str, count: usize) {
    for i in 0..count {
        let path = mount.join(format!("{}_{:04}.txt", prefix, i));
        if !path.exists() {
            fs::write(&path, format!("content {}", i)).unwrap();
        }
    }
}

fn bench_stat(c: &mut Criterion) {
    let mount = get_mount_path();
    if !mount.exists() {
        eprintln!("Mount path does not exist, skipping benchmarks");
        return;
    }

    let mut group = c.benchmark_group("stat");
    group.measurement_time(Duration::from_secs(10));

    // Create test file
    let path = mount.join("bench_stat.txt");
    fs::write(&path, "test content").unwrap();

    group.throughput(Throughput::Elements(1));

    group.bench_function("single_stat", |b| {
        b.iter(|| {
            let meta = fs::metadata(&path).unwrap();
            black_box(meta.len());
        });
    });

    group.bench_function("repeated_stat_10x", |b| {
        b.iter(|| {
            for _ in 0..10 {
                let meta = fs::metadata(&path).unwrap();
                black_box(meta.len());
            }
        });
    });

    let _ = fs::remove_file(&path);
    group.finish();
}

fn bench_readdir(c: &mut Criterion) {
    let mount = get_mount_path();
    if !mount.exists() {
        return;
    }

    let mut group = c.benchmark_group("readdir");
    group.measurement_time(Duration::from_secs(10));

    let dir_sizes = vec![("10", 10), ("100", 100), ("1000", 1000)];

    for (name, count) in dir_sizes {
        let dir = mount.join(format!("bench_readdir_{}", name));
        let _ = fs::create_dir(&dir);
        setup_files(&dir, "file", count);

        group.throughput(Throughput::Elements(count as u64));

        group.bench_with_input(BenchmarkId::new("list", name), &dir, |b, dir| {
            b.iter(|| {
                let count = fs::read_dir(dir).unwrap().count();
                black_box(count);
            });
        });
    }

    group.finish();
}

fn bench_exists_check(c: &mut Criterion) {
    let mount = get_mount_path();
    if !mount.exists() {
        return;
    }

    let mut group = c.benchmark_group("exists");
    group.measurement_time(Duration::from_secs(10));

    // Create existing file
    let existing = mount.join("bench_exists.txt");
    fs::write(&existing, "content").unwrap();

    let nonexistent = mount.join("bench_nonexistent_12345.txt");

    group.throughput(Throughput::Elements(1));

    group.bench_function("existing_file", |b| {
        b.iter(|| {
            black_box(existing.exists());
        });
    });

    group.bench_function("nonexistent_file", |b| {
        b.iter(|| {
            black_box(nonexistent.exists());
        });
    });

    let _ = fs::remove_file(&existing);
    group.finish();
}

fn bench_chmod(c: &mut Criterion) {
    let mount = get_mount_path();
    if !mount.exists() {
        return;
    }

    let mut group = c.benchmark_group("chmod");
    group.measurement_time(Duration::from_secs(10));

    let path = mount.join("bench_chmod.txt");
    fs::write(&path, "content").unwrap();

    group.throughput(Throughput::Elements(1));

    group.bench_function("toggle_permissions", |b| {
        let mut mode = 0o644;
        b.iter(|| {
            fs::set_permissions(&path, Permissions::from_mode(mode)).unwrap();
            mode = if mode == 0o644 { 0o755 } else { 0o644 };
            black_box(());
        });
    });

    let _ = fs::remove_file(&path);
    group.finish();
}

fn bench_mkdir_rmdir(c: &mut Criterion) {
    let mount = get_mount_path();
    if !mount.exists() {
        return;
    }

    let mut group = c.benchmark_group("mkdir_rmdir");
    group.measurement_time(Duration::from_secs(10));

    group.throughput(Throughput::Elements(1));

    group.bench_function("create_remove_dir", |b| {
        let mut counter = 0u64;
        b.iter(|| {
            let path = mount.join(format!("bench_dir_{}", counter));
            fs::create_dir(&path).unwrap();
            fs::remove_dir(&path).unwrap();
            counter += 1;
            black_box(());
        });
    });

    group.finish();
}

fn bench_rename(c: &mut Criterion) {
    let mount = get_mount_path();
    if !mount.exists() {
        return;
    }

    let mut group = c.benchmark_group("rename");
    group.measurement_time(Duration::from_secs(10));

    let path_a = mount.join("bench_rename_a.txt");
    let path_b = mount.join("bench_rename_b.txt");
    fs::write(&path_a, "content").unwrap();

    group.throughput(Throughput::Elements(1));

    group.bench_function("rename_file", |b| {
        let mut use_a = true;
        b.iter(|| {
            if use_a {
                fs::rename(&path_a, &path_b).unwrap();
            } else {
                fs::rename(&path_b, &path_a).unwrap();
            }
            use_a = !use_a;
            black_box(());
        });
    });

    let _ = fs::remove_file(&path_a);
    let _ = fs::remove_file(&path_b);
    group.finish();
}

fn bench_symlink(c: &mut Criterion) {
    let mount = get_mount_path();
    if !mount.exists() {
        return;
    }

    let mut group = c.benchmark_group("symlink");
    group.measurement_time(Duration::from_secs(10));

    let target = mount.join("bench_symlink_target.txt");
    fs::write(&target, "content").unwrap();

    group.throughput(Throughput::Elements(1));

    group.bench_function("readlink", |b| {
        let link = mount.join("bench_symlink_link.txt");
        std::os::unix::fs::symlink(&target, &link).ok();

        b.iter(|| {
            if let Ok(t) = fs::read_link(&link) {
                black_box(t);
            }
        });

        let _ = fs::remove_file(&link);
    });

    let _ = fs::remove_file(&target);
    group.finish();
}

fn bench_open_close(c: &mut Criterion) {
    let mount = get_mount_path();
    if !mount.exists() {
        return;
    }

    let mut group = c.benchmark_group("open_close");
    group.measurement_time(Duration::from_secs(10));

    let path = mount.join("bench_open_close.txt");
    fs::write(&path, "content").unwrap();

    group.throughput(Throughput::Elements(1));

    group.bench_function("open_read_close", |b| {
        b.iter(|| {
            let file = File::open(&path).unwrap();
            black_box(file);
        });
    });

    group.bench_function("open_write_close", |b| {
        b.iter(|| {
            let file = File::create(&path).unwrap();
            black_box(file);
        });
    });

    let _ = fs::remove_file(&path);
    group.finish();
}

criterion_group!(
    benches,
    bench_stat,
    bench_readdir,
    bench_exists_check,
    bench_chmod,
    bench_mkdir_rmdir,
    bench_rename,
    bench_symlink,
    bench_open_close,
);
criterion_main!(benches);
