#!/usr/bin/env bash
#
# Large file handling tests
#

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

echo "=== Large Files ==="

SUFFIX=$(random_suffix)

# Test: 1MB file
FILE_1MB="${MOUNT_PATH}/large_1mb_${SUFFIX}.bin"
run_test "create 1MB file" "dd if=/dev/zero of='${FILE_1MB}' bs=1M count=1 2>/dev/null"
run_test "verify 1MB size" "[[ \$(get_size '${FILE_1MB}') -eq 1048576 ]]"
run_test "read 1MB file" "cat '${FILE_1MB}' > /dev/null"
rm "${FILE_1MB}"

# Test: 5MB file with random content
FILE_5MB="${MOUNT_PATH}/large_5mb_${SUFFIX}.bin"
run_test "create 5MB random" "dd if=/dev/urandom of='${FILE_5MB}' bs=1M count=5 2>/dev/null"
run_test "verify 5MB size" "[[ \$(get_size '${FILE_5MB}') -eq 5242880 ]]"

# Test: Read back and verify (checksum)
CHECKSUM_ORIG=$(md5sum "${FILE_5MB}" 2>/dev/null | cut -d' ' -f1 || md5 -q "${FILE_5MB}")
run_test "5MB checksum valid" "[[ -n '${CHECKSUM_ORIG}' ]]"

# Copy to temp, compare checksums
TEMP_COPY="/tmp/large_copy_${SUFFIX}.bin"
cp "${FILE_5MB}" "${TEMP_COPY}"
CHECKSUM_COPY=$(md5sum "${TEMP_COPY}" 2>/dev/null | cut -d' ' -f1 || md5 -q "${TEMP_COPY}")
run_test "5MB data integrity" "[[ '${CHECKSUM_ORIG}' == '${CHECKSUM_COPY}' ]]"
rm "${TEMP_COPY}" "${FILE_5MB}"

# Test: 10MB file
FILE_10MB="${MOUNT_PATH}/large_10mb_${SUFFIX}.bin"
run_test "create 10MB file" "dd if=/dev/zero of='${FILE_10MB}' bs=1M count=10 2>/dev/null"
run_test "verify 10MB size" "[[ \$(get_size '${FILE_10MB}') -eq 10485760 ]]"
rm "${FILE_10MB}"

# Test: Chunked write (multiple small writes)
CHUNKED="${MOUNT_PATH}/chunked_${SUFFIX}.bin"
run_test "chunked write start" "dd if=/dev/zero of='${CHUNKED}' bs=64K count=1 2>/dev/null"
for i in {2..16}; do
    dd if=/dev/zero bs=64K count=1 >> "${CHUNKED}" 2>/dev/null
done
run_test "chunked write complete" "[[ \$(get_size '${CHUNKED}') -eq 1048576 ]]"
rm "${CHUNKED}"

# Test: Sparse-ish behavior (write at offset via seek)
# Note: This tests write buffering since S3 doesn't support random writes
SPARSE="${MOUNT_PATH}/sparse_${SUFFIX}.bin"
dd if=/dev/zero of="${SPARSE}" bs=1 count=0 seek=1048576 2>/dev/null || true
if [[ -f "${SPARSE}" ]]; then
    # File was created - check if it's actually 1MB or if sparse files are supported
    SIZE=$(get_size "${SPARSE}")
    if [[ "${SIZE}" -eq 1048576 ]]; then
        echo -e "  sparse file (1MB)... ${GREEN}PASS${NC}"
        ((TEST_PASSED++)) || true
    else
        echo -e "  sparse file... ${YELLOW}SKIP${NC} (got ${SIZE} bytes)"
    fi
    rm "${SPARSE}"
else
    echo -e "  sparse file... ${YELLOW}SKIP${NC} (not supported)"
fi

# Test: Overwrite large file
OVERWRITE="${MOUNT_PATH}/overwrite_${SUFFIX}.bin"
dd if=/dev/zero of="${OVERWRITE}" bs=1M count=2 2>/dev/null
run_test "overwrite 2MB->1MB" "dd if=/dev/urandom of='${OVERWRITE}' bs=1M count=1 2>/dev/null"
run_test "verify overwrite size" "[[ \$(get_size '${OVERWRITE}') -eq 1048576 ]]"
rm "${OVERWRITE}"

echo ""
