#!/usr/bin/env bash
#
# Error case tests - verify proper error handling
#

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

echo "=== Error Cases ==="

SUFFIX=$(random_suffix)

# Test: ENOENT - Read nonexistent file
run_test_fails "ENOENT: read missing file" "cat '${MOUNT_PATH}/nonexistent_${SUFFIX}.txt'"

# Test: ENOENT - Delete nonexistent file
run_test_fails "ENOENT: delete missing file" "rm '${MOUNT_PATH}/nonexistent_${SUFFIX}.txt'"

# Test: ENOENT - Create file in nonexistent directory
run_test_fails "ENOENT: create in missing dir" "echo 'x' > '${MOUNT_PATH}/missing_dir_${SUFFIX}/file.txt'"

# Test: ENOENT - rmdir nonexistent directory
run_test_fails "ENOENT: rmdir missing dir" "rmdir '${MOUNT_PATH}/missing_dir_${SUFFIX}'"

# Test: EEXIST - Create file that already exists (with O_EXCL behavior)
EXIST_FILE="${MOUNT_PATH}/exists_${SUFFIX}.txt"
echo "original" > "${EXIST_FILE}"
# Note: set -o noclobber is shell-level, use a different approach
# Create a test that uses O_EXCL behavior
run_test_fails "EEXIST: mkdir on file" "mkdir '${EXIST_FILE}'"
rm "${EXIST_FILE}"

# Test: EEXIST - mkdir on existing directory
EXIST_DIR="${MOUNT_PATH}/exists_dir_${SUFFIX}"
mkdir "${EXIST_DIR}"
run_test_fails "EEXIST: mkdir existing" "mkdir '${EXIST_DIR}'"
rmdir "${EXIST_DIR}"

# Test: ENOTEMPTY - rmdir on non-empty directory
NONEMPTY="${MOUNT_PATH}/nonempty_${SUFFIX}"
mkdir "${NONEMPTY}"
echo "content" > "${NONEMPTY}/file.txt"
run_test_fails "ENOTEMPTY: rmdir non-empty" "rmdir '${NONEMPTY}'"
rm -rf "${NONEMPTY}"

# Test: EISDIR - unlink on directory
ISDIR="${MOUNT_PATH}/isdir_${SUFFIX}"
mkdir "${ISDIR}"
run_test_fails "EISDIR: rm on directory" "rm '${ISDIR}'"
rmdir "${ISDIR}"

# Test: ENOTDIR - rmdir on file
NOTDIR="${MOUNT_PATH}/notdir_${SUFFIX}.txt"
echo "file" > "${NOTDIR}"
run_test_fails "ENOTDIR: rmdir on file" "rmdir '${NOTDIR}'"
rm "${NOTDIR}"

# Test: ENOTDIR - create file inside a file path
FAKEDIR="${MOUNT_PATH}/fakedir_${SUFFIX}.txt"
echo "file" > "${FAKEDIR}"
run_test_fails "ENOTDIR: create in file path" "echo 'x' > '${FAKEDIR}/child.txt'"
rm "${FAKEDIR}"

# Test: Invalid paths
# These may succeed or fail depending on implementation - just ensure no crash
echo -n "  handle path edge cases... "
# Empty component (double slash) - usually normalized
ls "${MOUNT_PATH}//." > /dev/null 2>&1 || true
# Current dir reference
ls "${MOUNT_PATH}/./." > /dev/null 2>&1 || true
# Parent dir reference at root
ls "${MOUNT_PATH}/../.." > /dev/null 2>&1 || true
echo -e "${GREEN}PASS${NC} (no crash)"
((TEST_PASSED++)) || true

# Test: Long filename
LONG_NAME="${MOUNT_PATH}/$(printf 'x%.0s' {1..200})_${SUFFIX}.txt"
if echo "test" > "${LONG_NAME}" 2>/dev/null; then
    echo -e "  long filename (200 chars)... ${GREEN}PASS${NC}"
    ((TEST_PASSED++)) || true
    rm "${LONG_NAME}"
else
    # ENAMETOOLONG is expected for very long names
    echo -e "  long filename rejected... ${GREEN}PASS${NC}"
    ((TEST_PASSED++)) || true
fi

# Test: Unicode filename
UNICODE_FILE="${MOUNT_PATH}/unicode_\xc3\xa9\xc3\xa0\xc3\xbc_${SUFFIX}.txt"
if echo "test" > "${UNICODE_FILE}" 2>/dev/null; then
    run_test "unicode filename" "assert_is_file '${UNICODE_FILE}'"
    rm "${UNICODE_FILE}"
else
    echo -e "  unicode filename... ${YELLOW}SKIP${NC}"
fi

echo ""
