#!/usr/bin/env bash
#
# Persistence tests - verify data survives in the FUSE mount
#

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

echo "=== Persistence ==="

SUFFIX=$(random_suffix)

# Test: Create file at root
ROOT_FILE="${MOUNT_PATH}/persist_root_${SUFFIX}.txt"
printf "root content\n" > "${ROOT_FILE}"
run_test "file at root" "[[ -f '${ROOT_FILE}' ]]"
run_test "root content" "[[ \$(cat '${ROOT_FILE}') == 'root content' ]]"
rm -f "${ROOT_FILE}"

# Test: Create file in subdirectory
SUBDIR="${MOUNT_PATH}/persist_subdir_${SUFFIX}"
mkdir -p "${SUBDIR}"
printf "sub content\n" > "${SUBDIR}/file.txt"
run_test "file in subdir" "[[ -f '${SUBDIR}/file.txt' ]]"
run_test "subdir content" "[[ \$(cat '${SUBDIR}/file.txt') == 'sub content' ]]"
rm -rf "${SUBDIR}"

# Test: Large file data integrity
LARGE_FILE="${MOUNT_PATH}/large_persist_${SUFFIX}.bin"
dd if=/dev/urandom of="${LARGE_FILE}" bs=1M count=2 2>/dev/null

# Get checksum (macOS vs Linux compatible)
if command -v md5sum &>/dev/null; then
    CHECKSUM1=$(md5sum "${LARGE_FILE}" | cut -d' ' -f1)
else
    CHECKSUM1=$(md5 -q "${LARGE_FILE}")
fi

# Read again and compare
if command -v md5sum &>/dev/null; then
    CHECKSUM2=$(md5sum "${LARGE_FILE}" | cut -d' ' -f1)
else
    CHECKSUM2=$(md5 -q "${LARGE_FILE}")
fi

run_test "2MB data integrity" "[[ '${CHECKSUM1}' == '${CHECKSUM2}' ]]"
rm -f "${LARGE_FILE}"

# Test: Multiple files in batch
BATCH_DIR="${MOUNT_PATH}/batch_${SUFFIX}"
mkdir -p "${BATCH_DIR}"
for i in {1..5}; do
    printf "batch file %d\n" "$i" > "${BATCH_DIR}/file_${i}.txt"
done
run_test "batch 5 files" "[[ \$(ls '${BATCH_DIR}' | wc -l | tr -d ' ') -eq 5 ]]"
rm -rf "${BATCH_DIR}"

# Test: Nested directory structure
DEEP_DIR="${MOUNT_PATH}/deep_${SUFFIX}/a/b/c"
mkdir -p "${DEEP_DIR}"
printf "deep content\n" > "${DEEP_DIR}/file.txt"
run_test "deep nested file" "[[ -f '${DEEP_DIR}/file.txt' ]]"
run_test "deep content" "[[ \$(cat '${DEEP_DIR}/file.txt') == 'deep content' ]]"
rm -rf "${MOUNT_PATH}/deep_${SUFFIX}"

echo ""
