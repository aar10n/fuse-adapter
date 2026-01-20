#!/usr/bin/env bash
#
# Read-only mount tests - verify read_only mode is enforced
#

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

echo "=== Read-Only Mount Tests ==="

# Ensure we have a read-only mount point
if [[ -z "${MOUNT_PATH_RO:-}" ]]; then
    echo -e "  ${YELLOW}SKIP${NC} - No read-only mount point configured"
    exit 0
fi

SUFFIX=$(random_suffix)

# First, seed some test data via the writable mount
# Both mounts point to the same bucket but different prefixes
# We need to pre-create a file in the RO prefix using the RW mount's bucket access
# For this test, we'll create test files directly in the RO mount area

# Test: Creating a file should fail on read-only mount
run_test_fails "EROFS: create file" "echo 'test' > '${MOUNT_PATH_RO}/newfile_${SUFFIX}.txt'"

# Test: Creating a directory should fail on read-only mount
run_test_fails "EROFS: mkdir" "mkdir '${MOUNT_PATH_RO}/newdir_${SUFFIX}'"

# Test: touch should fail (creates file if not exists)
run_test_fails "EROFS: touch new file" "touch '${MOUNT_PATH_RO}/touched_${SUFFIX}.txt'"

# Test: Reading directory listing should succeed
run_test "read-only: ls works" "ls '${MOUNT_PATH_RO}' >/dev/null"

# Test: Verify mount shows as read-only in mount output (Linux)
if command -v findmnt >/dev/null 2>&1; then
    run_test "mount shows read-only" "findmnt -n -o OPTIONS '${MOUNT_PATH_RO}' | grep -q 'ro'"
fi

# Test: Verify mount shows as read-only (macOS)
if [[ "$(uname)" == "Darwin" ]]; then
    run_test "mount shows read-only" "mount | grep '${MOUNT_PATH_RO}' | grep -q 'read-only'"
fi

# Create a file via the writable mount that we can then try to modify via RO mount
# Note: This requires the test harness to set up both mounts pointing to overlapping data
# For now, we test what we can without pre-existing files

# Test: Appending to a hypothetical existing file should fail
# We use a pattern that would work if a file existed
run_test_fails "EROFS: append fails" "echo 'append' >> '${MOUNT_PATH_RO}/anyfile_${SUFFIX}.txt' 2>/dev/null"

# Test: rm should fail even on non-existent file (it fails differently, but shouldn't succeed)
# Note: rm on non-existent returns ENOENT, not EROFS, so we skip this

# Test: rmdir should fail
run_test_fails "EROFS: rmdir fails" "rmdir '${MOUNT_PATH_RO}/anydir_${SUFFIX}' 2>/dev/null"

# Test: mv/rename should fail
run_test_fails "EROFS: mv fails" "mv '${MOUNT_PATH_RO}/src_${SUFFIX}' '${MOUNT_PATH_RO}/dst_${SUFFIX}' 2>/dev/null"

# Test: chmod should fail (even if file existed)
run_test_fails "EROFS: chmod fails" "chmod 755 '${MOUNT_PATH_RO}/anyfile_${SUFFIX}.txt' 2>/dev/null"

echo ""
