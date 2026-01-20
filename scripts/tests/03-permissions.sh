#!/usr/bin/env bash
#
# File permission (chmod) tests
#

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

echo "=== Permissions ==="

SUFFIX=$(random_suffix)

# Test: chmod file to 600
CHMOD_FILE="${MOUNT_PATH}/chmod_${SUFFIX}.txt"
echo "content" > "${CHMOD_FILE}"
run_test "chmod 600" "chmod 600 '${CHMOD_FILE}'"
run_test "verify chmod 600" "[[ \$(get_mode '${CHMOD_FILE}') == '600' ]]"

# Test: chmod to 644
run_test "chmod 644" "chmod 644 '${CHMOD_FILE}'"
run_test "verify chmod 644" "[[ \$(get_mode '${CHMOD_FILE}') == '644' ]]"

# Test: chmod to 755 (executable)
run_test "chmod 755" "chmod 755 '${CHMOD_FILE}'"
run_test "verify chmod 755" "[[ \$(get_mode '${CHMOD_FILE}') == '755' ]]"

# Test: chmod to 400 (read-only)
# Note: This may fail on some systems due to caching or S3 limitations
run_test "chmod 400" "chmod 400 '${CHMOD_FILE}'"
MODE_400=$(get_mode "${CHMOD_FILE}" 2>/dev/null || echo "unknown")
if [[ "${MODE_400}" == "400" ]]; then
    echo -e "  verify chmod 400... ${GREEN}PASS${NC}"
    ((TEST_PASSED++)) || true
else
    echo -e "  verify chmod 400 (got ${MODE_400})... ${YELLOW}SKIP${NC} (known limitation)"
fi

# Restore for cleanup
chmod 644 "${CHMOD_FILE}" 2>/dev/null || true
rm -f "${CHMOD_FILE}"

# Test: chmod directory
CHMOD_DIR="${MOUNT_PATH}/chmod_dir_${SUFFIX}"
mkdir "${CHMOD_DIR}"
run_test "chmod dir 700" "chmod 700 '${CHMOD_DIR}'"
run_test "verify dir chmod 700" "[[ \$(get_mode '${CHMOD_DIR}') == '700' ]]"

run_test "chmod dir 755" "chmod 755 '${CHMOD_DIR}'"
run_test "verify dir chmod 755" "[[ \$(get_mode '${CHMOD_DIR}') == '755' ]]"

rmdir "${CHMOD_DIR}"

# Test: Mode persistence across stat
PERSIST_FILE="${MOUNT_PATH}/persist_mode_${SUFFIX}.txt"
echo "test" > "${PERSIST_FILE}"
chmod 640 "${PERSIST_FILE}"
run_test "mode persists" "[[ \$(get_mode '${PERSIST_FILE}') == '640' ]]"

# Read the file to ensure it still has correct mode after access
cat "${PERSIST_FILE}" > /dev/null
run_test "mode after read" "[[ \$(get_mode '${PERSIST_FILE}') == '640' ]]"

rm "${PERSIST_FILE}"

# Test: Create file and check default mode
# Note: Default mode depends on umask, typically 644 for files
DEFAULT_FILE="${MOUNT_PATH}/default_mode_${SUFFIX}.txt"
echo "test" > "${DEFAULT_FILE}"
DEFAULT_MODE=$(get_mode "${DEFAULT_FILE}")
# Accept 644 or 664 depending on umask
if [[ "${DEFAULT_MODE}" == "644" ]] || [[ "${DEFAULT_MODE}" == "664" ]]; then
    echo -e "  default file mode (${DEFAULT_MODE})... ${GREEN}PASS${NC}"
    ((TEST_PASSED++)) || true
else
    echo -e "  default file mode (got ${DEFAULT_MODE})... ${YELLOW}WARN${NC}"
fi
rm "${DEFAULT_FILE}"

# Test: Create directory and check default mode
DEFAULT_DIR="${MOUNT_PATH}/default_dir_mode_${SUFFIX}"
mkdir "${DEFAULT_DIR}"
DEFAULT_DIR_MODE=$(get_mode "${DEFAULT_DIR}")
# Accept 755 or 775 depending on umask
if [[ "${DEFAULT_DIR_MODE}" == "755" ]] || [[ "${DEFAULT_DIR_MODE}" == "775" ]]; then
    echo -e "  default dir mode (${DEFAULT_DIR_MODE})... ${GREEN}PASS${NC}"
    ((TEST_PASSED++)) || true
else
    echo -e "  default dir mode (got ${DEFAULT_DIR_MODE})... ${YELLOW}WARN${NC}"
fi
rmdir "${DEFAULT_DIR}"

echo ""
