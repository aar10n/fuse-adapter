#!/usr/bin/env bash
#
# Wait for MinIO to be ready
#
# Usage: ./wait-for-minio.sh [endpoint] [timeout_seconds]

set -euo pipefail

ENDPOINT="${1:-http://localhost:9000}"
TIMEOUT="${2:-30}"

echo "Waiting for MinIO at ${ENDPOINT}..."

for i in $(seq 1 "${TIMEOUT}"); do
    if curl -sf "${ENDPOINT}/minio/health/live" >/dev/null 2>&1; then
        echo "MinIO is ready!"
        exit 0
    fi
    echo "  Attempt ${i}/${TIMEOUT}: waiting..."
    sleep 1
done

echo "ERROR: MinIO did not become ready within ${TIMEOUT} seconds"
exit 1
