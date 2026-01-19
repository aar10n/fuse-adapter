#!/usr/bin/env bash
#
# Basic file CRUD tests
#

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

echo "=== Basic File CRUD ==="

SUFFIX=$(random_suffix)
TEST_FILE="${MOUNT_PATH}/crud_test_${SUFFIX}.txt"
TEST_CONTENT="Hello, FUSE adapter!"
UPDATED_CONTENT="Updated content here"

# Test: Create file
run_test "create file" "echo '${TEST_CONTENT}' > '${TEST_FILE}'"

# Test: File exists after create
run_test "file exists" "assert_is_file '${TEST_FILE}'"

# Test: Read file content
run_test "read file" "[[ \$(cat '${TEST_FILE}') == '${TEST_CONTENT}' ]]"

# Test: Update file (overwrite)
run_test "update file" "echo '${UPDATED_CONTENT}' > '${TEST_FILE}'"

# Test: Read updated content
run_test "read updated" "[[ \$(cat '${TEST_FILE}') == '${UPDATED_CONTENT}' ]]"

# Test: Append to file
run_test "append to file" "echo 'appended line' >> '${TEST_FILE}'"

# Test: Verify append
run_test "verify append" "grep -q 'appended line' '${TEST_FILE}'"

# Test: Delete file
run_test "delete file" "rm '${TEST_FILE}'"

# Test: File no longer exists
run_test "file gone" "assert_not_exists '${TEST_FILE}'"

# Test: Read nonexistent file fails
NONEXISTENT="${MOUNT_PATH}/does_not_exist_${SUFFIX}.txt"
run_test_fails "read nonexistent fails" "cat '${NONEXISTENT}'"

# Test: Create file with spaces in name
SPACE_FILE="${MOUNT_PATH}/file with spaces ${SUFFIX}.txt"
run_test "create file with spaces" "echo 'content' > '${SPACE_FILE}'"
run_test "read file with spaces" "[[ \$(cat '${SPACE_FILE}') == 'content' ]]"
run_test "delete file with spaces" "rm '${SPACE_FILE}'"

# Test: Create file with special chars
SPECIAL_FILE="${MOUNT_PATH}/file-with_special.chars-${SUFFIX}.txt"
run_test "create special name file" "echo 'content' > '${SPECIAL_FILE}'"
run_test "delete special name file" "rm '${SPECIAL_FILE}'"

# Test: Empty file
EMPTY_FILE="${MOUNT_PATH}/empty_${SUFFIX}.txt"
run_test "create empty file" "touch '${EMPTY_FILE}'"
run_test "empty file exists" "assert_is_file '${EMPTY_FILE}'"
run_test "empty file has zero size" "[[ \$(get_size '${EMPTY_FILE}') -eq 0 ]]"
run_test "delete empty file" "rm '${EMPTY_FILE}'"

echo ""
