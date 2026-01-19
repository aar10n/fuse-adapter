#!/usr/bin/env bash
#
# Directory operation tests
#

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

echo "=== Directory Operations ==="

SUFFIX=$(random_suffix)
TEST_DIR="${MOUNT_PATH}/dir_test_${SUFFIX}"

# Test: Create directory
run_test "mkdir" "mkdir '${TEST_DIR}'"

# Test: Directory exists
run_test "dir exists" "assert_is_dir '${TEST_DIR}'"

# Test: List empty directory
run_test "list empty dir" "ls '${TEST_DIR}'"

# Test: Create file in directory
run_test "create file in dir" "echo 'content' > '${TEST_DIR}/file.txt'"

# Test: List directory with file
run_test "list dir with file" "[[ \$(ls '${TEST_DIR}' | wc -l) -eq 1 ]]"

# Test: Delete directory with content fails (rmdir)
run_test_fails "rmdir non-empty fails" "rmdir '${TEST_DIR}'"

# Test: Delete file in directory
run_test "delete file in dir" "rm '${TEST_DIR}/file.txt'"

# Test: Delete empty directory
run_test "rmdir empty" "rmdir '${TEST_DIR}'"

# Test: Directory gone
run_test "dir gone" "assert_not_exists '${TEST_DIR}'"

# Test: Nested directory creation
NESTED="${MOUNT_PATH}/nested_${SUFFIX}/a/b/c"
run_test "mkdir -p nested" "mkdir -p '${NESTED}'"
run_test "nested exists" "assert_is_dir '${NESTED}'"

# Test: Create files at different depths
run_test "file at depth 1" "echo '1' > '${MOUNT_PATH}/nested_${SUFFIX}/file1.txt'"
run_test "file at depth 2" "echo '2' > '${MOUNT_PATH}/nested_${SUFFIX}/a/file2.txt'"
run_test "file at depth 4" "echo '4' > '${NESTED}/file4.txt'"

# Test: List nested structure
run_test "list nested root" "[[ \$(ls '${MOUNT_PATH}/nested_${SUFFIX}' | wc -l) -eq 2 ]]"

# Test: Recursive delete
run_test "rm -rf nested" "rm -rf '${MOUNT_PATH}/nested_${SUFFIX}'"
run_test "nested gone" "assert_not_exists '${MOUNT_PATH}/nested_${SUFFIX}'"

# Test: Multiple directories at same level
MULTI_BASE="${MOUNT_PATH}/multi_${SUFFIX}"
run_test "mkdir base" "mkdir '${MULTI_BASE}'"
run_test "mkdir sub1" "mkdir '${MULTI_BASE}/sub1'"
run_test "mkdir sub2" "mkdir '${MULTI_BASE}/sub2'"
run_test "mkdir sub3" "mkdir '${MULTI_BASE}/sub3'"

# Test: List multiple subdirs
run_test "list 3 subdirs" "[[ \$(ls '${MULTI_BASE}' | wc -l) -eq 3 ]]"

# Clean up
run_test "cleanup multi" "rm -rf '${MULTI_BASE}'"

# Test: Rename/move directory (if supported)
RENAME_SRC="${MOUNT_PATH}/rename_src_${SUFFIX}"
RENAME_DST="${MOUNT_PATH}/rename_dst_${SUFFIX}"
mkdir "${RENAME_SRC}" 2>/dev/null || true
echo "test" > "${RENAME_SRC}/file.txt" 2>/dev/null || true
if mv "${RENAME_SRC}" "${RENAME_DST}" 2>/dev/null; then
    run_test "dir rename" "assert_is_dir '${RENAME_DST}'"
    run_test "file preserved after rename" "assert_is_file '${RENAME_DST}/file.txt'"
    rm -rf "${RENAME_DST}"
else
    echo "  dir rename... SKIP (not supported)"
fi

echo ""
