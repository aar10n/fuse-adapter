#!/usr/bin/env bash
#
# UID/GID mount configuration tests - verify files report configured owner
#

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

echo "=== UID/GID Configuration Tests ==="

# Ensure we have a uid/gid-configured mount point
if [[ -z "${MOUNT_PATH_UIDGID:-}" ]]; then
    echo -e "  ${YELLOW}SKIP${NC} - No uid/gid mount point configured"
    exit 0
fi

# Expected uid/gid (set by harness)
EXPECTED_UID="${EXPECTED_UID:-1000}"
EXPECTED_GID="${EXPECTED_GID:-1000}"

SUFFIX=$(random_suffix)

# Get file uid (cross-platform)
get_uid() {
    local file="$1"
    if $IS_MACOS; then
        stat -f "%u" "$file"
    else
        stat -c "%u" "$file"
    fi
}

# Get file gid (cross-platform)
get_gid() {
    local file="$1"
    if $IS_MACOS; then
        stat -f "%g" "$file"
    else
        stat -c "%g" "$file"
    fi
}

# Test: Create a file and verify uid/gid
test_file="test_uidgid_${SUFFIX}.txt"
echo "hello" > "${MOUNT_PATH_UIDGID}/${test_file}"

run_test "file reports configured UID" '
    actual_uid=$(get_uid "${MOUNT_PATH_UIDGID}/${test_file}")
    [[ "$actual_uid" == "${EXPECTED_UID}" ]]
'

run_test "file reports configured GID" '
    actual_gid=$(get_gid "${MOUNT_PATH_UIDGID}/${test_file}")
    [[ "$actual_gid" == "${EXPECTED_GID}" ]]
'

# Test: Create a directory and verify uid/gid
test_dir="test_uidgid_dir_${SUFFIX}"
mkdir "${MOUNT_PATH_UIDGID}/${test_dir}"

run_test "directory reports configured UID" '
    actual_uid=$(get_uid "${MOUNT_PATH_UIDGID}/${test_dir}")
    [[ "$actual_uid" == "${EXPECTED_UID}" ]]
'

run_test "directory reports configured GID" '
    actual_gid=$(get_gid "${MOUNT_PATH_UIDGID}/${test_dir}")
    [[ "$actual_gid" == "${EXPECTED_GID}" ]]
'

# Test: Compare with default mount (should be different if running as root)
if [[ -n "${MOUNT_PATH:-}" ]]; then
    default_test_file="test_compare_${SUFFIX}.txt"
    echo "compare" > "${MOUNT_PATH}/${default_test_file}"

    default_uid=$(get_uid "${MOUNT_PATH}/${default_test_file}")
    uidgid_uid=$(get_uid "${MOUNT_PATH_UIDGID}/${test_file}")

    run_test "uid differs from default mount (or matches if same)" '
        # This test documents that uid/gid config works
        # The configured mount should report the configured uid
        [[ "$uidgid_uid" == "${EXPECTED_UID}" ]]
    '

    # Cleanup comparison file
    rm -f "${MOUNT_PATH}/${default_test_file}"
fi

# Cleanup test files
rm -f "${MOUNT_PATH_UIDGID}/${test_file}"
rmdir "${MOUNT_PATH_UIDGID}/${test_dir}" 2>/dev/null || true

echo ""
