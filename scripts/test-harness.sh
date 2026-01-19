#!/usr/bin/env bash
#
# Integration test harness for fuse-adapter
#
# Usage:
#   ./scripts/test-harness.sh             # Run all tests
#   ./scripts/test-harness.sh --quick     # Run quick smoke tests only
#   ./scripts/test-harness.sh --ci        # CI mode (MinIO already running)
#   ./scripts/test-harness.sh --skip-cleanup  # Leave mount up for debugging

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
TESTS_DIR="${SCRIPT_DIR}/tests"

# Configuration
MOUNT_BASE="${TEST_MOUNT_BASE:-/tmp/fuse-adapter-test}"
MOUNT_PATH="${MOUNT_BASE}/mnt"
CACHE_PATH="${MOUNT_BASE}/cache"
CONFIG_PATH="${MOUNT_BASE}/config.yaml"
PID_FILE="${MOUNT_BASE}/adapter.pid"
LOG_FILE="${MOUNT_BASE}/adapter.log"

MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://localhost:9000}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin}"
TEST_BUCKET="${TEST_BUCKET:-integration-test-$(date +%s)}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Options
CI_MODE=false
QUICK_MODE=false
SKIP_CLEANUP=false
VERBOSE=false

# Test counters
export TEST_PASSED=0
export TEST_FAILED=0

log_info()  { echo -e "${GREEN}[INFO]${NC} $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*"; }
log_debug() { $VERBOSE && echo -e "${BLUE}[DEBUG]${NC} $*" || true; }

usage() {
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo "  --ci            CI mode - assume MinIO is already running"
    echo "  --quick         Run only basic CRUD tests"
    echo "  --skip-cleanup  Don't clean up after tests (for debugging)"
    echo "  --verbose       Show debug output"
    echo "  --help          Show this help"
    exit 0
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --ci) CI_MODE=true; shift ;;
        --quick) QUICK_MODE=true; shift ;;
        --skip-cleanup) SKIP_CLEANUP=true; shift ;;
        --verbose) VERBOSE=true; shift ;;
        --help) usage ;;
        *) log_error "Unknown option: $1"; exit 1 ;;
    esac
done

# Cleanup function
cleanup() {
    if $SKIP_CLEANUP; then
        log_warn "Skipping cleanup (--skip-cleanup)"
        log_info "Mount path: ${MOUNT_PATH}"
        log_info "To unmount: umount ${MOUNT_PATH} || fusermount -u ${MOUNT_PATH}"
        return
    fi

    log_info "Cleaning up..."

    # Stop adapter process
    if [[ -f "${PID_FILE}" ]]; then
        PID=$(cat "${PID_FILE}")
        if kill -0 "${PID}" 2>/dev/null; then
            log_debug "Stopping fuse-adapter (PID ${PID})..."
            kill -TERM "${PID}" 2>/dev/null || true
            sleep 2
            kill -9 "${PID}" 2>/dev/null || true
        fi
        rm -f "${PID_FILE}"
    fi

    # Force unmount
    if mountpoint -q "${MOUNT_PATH}" 2>/dev/null; then
        log_debug "Unmounting ${MOUNT_PATH}..."
        umount "${MOUNT_PATH}" 2>/dev/null || \
        fusermount -u "${MOUNT_PATH}" 2>/dev/null || \
        diskutil unmount "${MOUNT_PATH}" 2>/dev/null || true
    fi

    # Clean bucket (only if we created it)
    if ! $CI_MODE; then
        log_debug "Cleaning bucket ${TEST_BUCKET}..."
        docker exec fuse-adapter-minio mc rm --recursive --force "local/${TEST_BUCKET}/" 2>/dev/null || true
        docker exec fuse-adapter-minio mc rb "local/${TEST_BUCKET}" 2>/dev/null || true
    fi

    # Remove temp directories
    rm -rf "${MOUNT_BASE}"

    log_info "Cleanup complete"
}

trap cleanup EXIT

# Setup MinIO (if not CI mode)
setup_minio() {
    if $CI_MODE; then
        log_info "CI mode - assuming MinIO is running"
        return
    fi

    log_info "Checking MinIO..."

    # Check if MinIO is running
    if ! curl -sf "${MINIO_ENDPOINT}/minio/health/live" >/dev/null 2>&1; then
        log_info "Starting MinIO..."
        cd "${PROJECT_DIR}"
        make minio-start >/dev/null 2>&1 || true
    fi

    # Wait for MinIO
    log_info "Waiting for MinIO..."
    "${SCRIPT_DIR}/wait-for-minio.sh" "${MINIO_ENDPOINT}" 30

    # Create test bucket
    log_info "Creating test bucket: ${TEST_BUCKET}"
    docker exec fuse-adapter-minio mc alias set local http://localhost:9000 "${MINIO_ACCESS_KEY}" "${MINIO_SECRET_KEY}" >/dev/null 2>&1 || true
    docker exec fuse-adapter-minio mc mb --ignore-existing "local/${TEST_BUCKET}" >/dev/null 2>&1 || true
}

# Generate test configuration
generate_config() {
    log_info "Generating test configuration..."

    mkdir -p "${MOUNT_PATH}" "${CACHE_PATH}"

    cat > "${CONFIG_PATH}" <<EOF
logging:
  level: debug

connectors:
  s3:
    bucket: ${TEST_BUCKET}
    region: us-east-1
    endpoint: "${MINIO_ENDPOINT}"
    force_path_style: true
    cache:
      type: filesystem
      path: ${CACHE_PATH}
      max_size: "256MB"
      flush_interval: 5s

mounts:
  - path: ${MOUNT_PATH}
    connector:
      type: s3
EOF

    log_debug "Config written to ${CONFIG_PATH}"
}

# Build the project
build_project() {
    log_info "Building fuse-adapter..."
    cd "${PROJECT_DIR}"
    cargo build 2>&1 | grep -v "Compiling\|Downloading\|Downloaded" || true
}

# Start fuse-adapter
start_adapter() {
    log_info "Starting fuse-adapter..."

    local binary="${PROJECT_DIR}/target/debug/fuse-adapter"

    if [[ ! -x "${binary}" ]]; then
        log_error "Binary not found: ${binary}"
        exit 1
    fi

    AWS_ACCESS_KEY_ID="${MINIO_ACCESS_KEY}" \
    AWS_SECRET_ACCESS_KEY="${MINIO_SECRET_KEY}" \
    "${binary}" "${CONFIG_PATH}" > "${LOG_FILE}" 2>&1 &

    echo $! > "${PID_FILE}"
    log_debug "fuse-adapter started (PID $(cat "${PID_FILE}"))"
}

# Wait for mount to be ready
wait_for_mount() {
    log_info "Waiting for mount..."

    for i in {1..30}; do
        # Check if mount point is accessible
        if ls "${MOUNT_PATH}" >/dev/null 2>&1; then
            log_info "Mount ready at ${MOUNT_PATH}"
            return 0
        fi

        # Check if process is still running
        if [[ -f "${PID_FILE}" ]]; then
            PID=$(cat "${PID_FILE}")
            if ! kill -0 "${PID}" 2>/dev/null; then
                log_error "fuse-adapter process died"
                cat "${LOG_FILE}"
                exit 1
            fi
        fi

        sleep 1
    done

    log_error "Mount did not become ready within 30 seconds"
    cat "${LOG_FILE}"
    exit 1
}

# Run test scripts
run_tests() {
    log_info "Running tests..."
    echo ""

    # Export mount path for test scripts
    export MOUNT_PATH
    export TEST_PASSED
    export TEST_FAILED

    if $QUICK_MODE; then
        # Quick mode: only basic CRUD
        source "${TESTS_DIR}/01-basic-crud.sh"
    else
        # Full test suite
        for test_script in "${TESTS_DIR}"/*.sh; do
            if [[ "$(basename "${test_script}")" == "common.sh" ]]; then
                continue
            fi
            source "${test_script}"
        done
    fi
}

# Print summary
print_summary() {
    echo ""
    echo "========================================"
    echo -e "Results: ${GREEN}${TEST_PASSED} passed${NC}, ${RED}${TEST_FAILED} failed${NC}"
    echo "========================================"

    if [[ ${TEST_FAILED} -gt 0 ]]; then
        return 1
    fi
    return 0
}

# Main
main() {
    echo -e "${BLUE}"
    echo "========================================"
    echo "  fuse-adapter Integration Tests"
    echo "========================================"
    echo -e "${NC}"

    setup_minio
    generate_config
    build_project
    start_adapter
    wait_for_mount
    run_tests

    if print_summary; then
        exit 0
    else
        exit 1
    fi
}

main
