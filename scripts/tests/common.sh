#!/usr/bin/env bash
#
# Common test utilities
# Source this file from test scripts: source "$(dirname "$0")/common.sh"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m'

# Test counters (initialized by harness, but provide defaults)
: "${TEST_PASSED:=0}"
: "${TEST_FAILED:=0}"

# Mount point (set by harness)
: "${MOUNT_PATH:=/tmp/fuse-adapter-test/mnt}"

# Detect OS for stat command differences
if [[ "$(uname)" == "Darwin" ]]; then
    IS_MACOS=true
else
    IS_MACOS=false
fi

# Get file permissions (cross-platform)
get_mode() {
    local file="$1"
    if $IS_MACOS; then
        stat -f "%Lp" "$file"
    else
        stat -c "%a" "$file"
    fi
}

# Get file size in bytes (cross-platform)
get_size() {
    local file="$1"
    if $IS_MACOS; then
        stat -f "%z" "$file"
    else
        stat -c "%s" "$file"
    fi
}

# Run a test and report result
# Usage: run_test "test name" command_to_run
# Note: Always returns 0 to avoid triggering set -e exits
run_test() {
    local name="$1"
    shift
    local cmd="$*"

    echo -n "  ${name}... "

    if eval "$cmd" >/dev/null 2>&1; then
        echo -e "${GREEN}PASS${NC}"
        ((TEST_PASSED++)) || true
    else
        echo -e "${RED}FAIL${NC}"
        ((TEST_FAILED++)) || true
    fi
    return 0  # Always return success to continue test suite
}

# Run a test that should fail
# Usage: run_test_fails "test name" command_that_should_fail
# Note: Always returns 0 to avoid triggering set -e exits
run_test_fails() {
    local name="$1"
    shift
    local cmd="$*"

    echo -n "  ${name}... "

    if eval "$cmd" >/dev/null 2>&1; then
        echo -e "${RED}FAIL${NC} (expected failure but succeeded)"
        ((TEST_FAILED++)) || true
    else
        echo -e "${GREEN}PASS${NC}"
        ((TEST_PASSED++)) || true
    fi
    return 0  # Always return success to continue test suite
}

# Assert two values are equal
# Usage: assert_eq "expected" "actual" "message"
assert_eq() {
    local expected="$1"
    local actual="$2"
    local msg="${3:-values should be equal}"

    if [[ "$expected" == "$actual" ]]; then
        return 0
    else
        echo "  ASSERT FAILED: $msg"
        echo "    Expected: $expected"
        echo "    Actual:   $actual"
        return 1
    fi
}

# Assert file exists
assert_exists() {
    local path="$1"
    [[ -e "$path" ]]
}

# Assert file does not exist
assert_not_exists() {
    local path="$1"
    [[ ! -e "$path" ]]
}

# Assert is a file
assert_is_file() {
    local path="$1"
    [[ -f "$path" ]]
}

# Assert is a directory
assert_is_dir() {
    local path="$1"
    [[ -d "$path" ]]
}

# Clean up test artifacts in mount point
# Usage: cleanup_test_files pattern...
cleanup_test_files() {
    for pattern in "$@"; do
        rm -rf "${MOUNT_PATH:?}/${pattern}" 2>/dev/null || true
    done
}

# Generate random string for unique test names
random_suffix() {
    date +%s%N | sha256sum | head -c 8
}

# Export counters so they can be read by harness
export_results() {
    echo "TEST_PASSED=$TEST_PASSED"
    echo "TEST_FAILED=$TEST_FAILED"
}
