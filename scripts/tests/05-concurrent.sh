#!/usr/bin/env bash
#
# Concurrent operation tests
#

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

echo "=== Concurrent Operations ==="

SUFFIX=$(random_suffix)
CONC_DIR="${MOUNT_PATH}/concurrent_${SUFFIX}"
mkdir -p "${CONC_DIR}"

# Test: Concurrent file creation (10 files in parallel)
echo -n "  concurrent creates (10)... "
PIDS=""
for i in {1..10}; do
    echo "content $i" > "${CONC_DIR}/file_${i}.txt" &
    PIDS="$PIDS $!"
done
# Wait for all
FAILED=0
for pid in $PIDS; do
    wait "$pid" || ((FAILED++))
done
if [[ $FAILED -eq 0 ]]; then
    echo -e "${GREEN}PASS${NC}"
    ((TEST_PASSED++)) || true
else
    echo -e "${RED}FAIL${NC} ($FAILED failed)"
    ((TEST_FAILED++)) || true
fi

# Verify all files exist
run_test "all 10 files exist" "[[ \$(ls '${CONC_DIR}' | wc -l) -eq 10 ]]"

# Test: Concurrent reads (10 parallel readers of same file)
SHARED_FILE="${CONC_DIR}/shared.txt"
echo "shared content for concurrent reads" > "${SHARED_FILE}"

echo -n "  concurrent reads (10)... "
PIDS=""
for i in {1..10}; do
    cat "${SHARED_FILE}" > /dev/null &
    PIDS="$PIDS $!"
done
FAILED=0
for pid in $PIDS; do
    wait "$pid" || ((FAILED++))
done
if [[ $FAILED -eq 0 ]]; then
    echo -e "${GREEN}PASS${NC}"
    ((TEST_PASSED++)) || true
else
    echo -e "${RED}FAIL${NC} ($FAILED failed)"
    ((TEST_FAILED++)) || true
fi

# Test: Concurrent mixed operations (read while writing different files)
echo -n "  mixed concurrent ops... "
PIDS=""
for i in {1..5}; do
    # Writers
    echo "write $i" > "${CONC_DIR}/mixed_write_${i}.txt" &
    PIDS="$PIDS $!"
    # Readers (of previously created files)
    cat "${CONC_DIR}/file_${i}.txt" > /dev/null &
    PIDS="$PIDS $!"
done
FAILED=0
for pid in $PIDS; do
    wait "$pid" || ((FAILED++))
done
if [[ $FAILED -eq 0 ]]; then
    echo -e "${GREEN}PASS${NC}"
    ((TEST_PASSED++)) || true
else
    echo -e "${RED}FAIL${NC} ($FAILED failed)"
    ((TEST_FAILED++)) || true
fi

# Test: Concurrent directory listings
echo -n "  concurrent listings (5)... "
PIDS=""
for i in {1..5}; do
    ls -la "${CONC_DIR}" > /dev/null &
    PIDS="$PIDS $!"
done
FAILED=0
for pid in $PIDS; do
    wait "$pid" || ((FAILED++))
done
if [[ $FAILED -eq 0 ]]; then
    echo -e "${GREEN}PASS${NC}"
    ((TEST_PASSED++)) || true
else
    echo -e "${RED}FAIL${NC} ($FAILED failed)"
    ((TEST_FAILED++)) || true
fi

# Test: Concurrent file creation in nested directories
NESTED_CONC="${CONC_DIR}/nested"
mkdir -p "${NESTED_CONC}/a" "${NESTED_CONC}/b" "${NESTED_CONC}/c"

echo -n "  concurrent nested writes... "
PIDS=""
for dir in a b c; do
    for i in {1..3}; do
        echo "nested $dir $i" > "${NESTED_CONC}/${dir}/file_${i}.txt" &
        PIDS="$PIDS $!"
    done
done
FAILED=0
for pid in $PIDS; do
    wait "$pid" || ((FAILED++))
done
if [[ $FAILED -eq 0 ]]; then
    echo -e "${GREEN}PASS${NC}"
    ((TEST_PASSED++)) || true
else
    echo -e "${RED}FAIL${NC} ($FAILED failed)"
    ((TEST_FAILED++)) || true
fi

# Verify nested file count
run_test "nested files (9 total)" "[[ \$(find '${NESTED_CONC}' -type f | wc -l) -eq 9 ]]"

# Cleanup
rm -rf "${CONC_DIR}"

echo ""
