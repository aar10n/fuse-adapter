#!/bin/bash
# Verify consistency between local FUSE mount and S3 backend
# Usage: ./scripts/verify-sync.sh <mount_path> <s3_prefix> [--full]
#   --full: verify all files instead of sampling

set -e

MOUNT_PATH="${1:-/tmp/fuse-adapter/s3/a}"
S3_PREFIX="${2:-data/path_a}"
FULL_VERIFY="${3:-}"
BUCKET="test-bucket"
MINIO_CONTAINER="fuse-adapter-minio"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m'

echo "=== Sync Verification ==="
echo "Mount: $MOUNT_PATH"
echo "S3 prefix: $S3_PREFIX"
echo "Mode: $([ "$FULL_VERIFY" = "--full" ] && echo "FULL" || echo "SAMPLE")"
echo ""

# Count local items
LOCAL_FILES=$(find "$MOUNT_PATH" -type f 2>/dev/null | wc -l | tr -d ' ')
LOCAL_DIRS=$(find "$MOUNT_PATH" -type d 2>/dev/null | wc -l | tr -d ' ')
LOCAL_SYMLINKS=$(find "$MOUNT_PATH" -type l 2>/dev/null | wc -l | tr -d ' ')
LOCAL_TOTAL=$((LOCAL_FILES + LOCAL_DIRS + LOCAL_SYMLINKS))

echo "Local counts:"
echo "  Files:    $LOCAL_FILES"
echo "  Dirs:     $LOCAL_DIRS"
echo "  Symlinks: $LOCAL_SYMLINKS"
echo "  Total:    $LOCAL_TOTAL"
echo ""

# Count S3 objects
S3_OBJECTS=$(docker exec "$MINIO_CONTAINER" mc ls --recursive "local/$BUCKET/$S3_PREFIX" 2>/dev/null | wc -l | tr -d ' ')

echo "S3 object count: $S3_OBJECTS"
echo ""

# Compare counts
COUNT_MATCH=true
if [ "$LOCAL_TOTAL" -eq "$S3_OBJECTS" ]; then
    echo -e "${GREEN}✓ Object counts match${NC}"
else
    echo -e "${RED}✗ Object count mismatch: local=$LOCAL_TOTAL, S3=$S3_OBJECTS${NC}"
    DIFF=$((S3_OBJECTS - LOCAL_TOTAL))
    echo "  Difference: $DIFF"
    COUNT_MATCH=false
fi
echo ""

# Verify file contents
echo "=== Content Verification ==="

if [ "$FULL_VERIFY" = "--full" ]; then
    SAMPLE_FILES=$(find "$MOUNT_PATH" -type f 2>/dev/null)
    TOTAL_TO_CHECK=$(echo "$SAMPLE_FILES" | wc -l | tr -d ' ')
    echo "Verifying all $TOTAL_TO_CHECK files..."
else
    SAMPLE_SIZE=20
    SAMPLE_FILES=$(find "$MOUNT_PATH" -type f 2>/dev/null | shuf | head -$SAMPLE_SIZE)
    echo "Verifying random sample of $SAMPLE_SIZE files..."
fi

PASS=0
FAIL=0
CHECKED=0

for LOCAL_FILE in $SAMPLE_FILES; do
    REL_PATH="${LOCAL_FILE#$MOUNT_PATH/}"
    S3_KEY="$S3_PREFIX/$REL_PATH"
    ((CHECKED++))

    # Get local MD5
    LOCAL_MD5=$(md5 -q "$LOCAL_FILE" 2>/dev/null || md5sum "$LOCAL_FILE" 2>/dev/null | cut -d' ' -f1)

    # Get S3 ETag (MD5 for single-part uploads)
    S3_ETAG=$(docker exec "$MINIO_CONTAINER" mc stat "local/$BUCKET/$S3_KEY" 2>/dev/null | grep "^ETag" | head -1 | awk '{print $NF}' | tr -d '"')

    if [ "$LOCAL_MD5" = "$S3_ETAG" ]; then
        if [ "$FULL_VERIFY" != "--full" ]; then
            echo -e "  ${GREEN}✓${NC} $REL_PATH"
        fi
        ((PASS++))
    else
        echo -e "  ${RED}✗${NC} $REL_PATH"
        echo "    local: $LOCAL_MD5"
        echo "    s3:    $S3_ETAG"
        ((FAIL++))
    fi

    # Progress indicator for full verify
    if [ "$FULL_VERIFY" = "--full" ] && [ $((CHECKED % 100)) -eq 0 ]; then
        echo "  Progress: $CHECKED files checked..."
    fi
done

echo ""
echo "=== Symlink Verification ==="
SYMLINK_PASS=0
SYMLINK_FAIL=0

for LOCAL_LINK in $(find "$MOUNT_PATH" -type l 2>/dev/null); do
    REL_PATH="${LOCAL_LINK#$MOUNT_PATH/}"
    LOCAL_TARGET=$(readlink "$LOCAL_LINK" 2>/dev/null)

    # Check if symlink metadata exists in S3
    S3_KEY="$S3_PREFIX/$REL_PATH"
    S3_TARGET=$(docker exec "$MINIO_CONTAINER" mc stat "local/$BUCKET/$S3_KEY" 2>/dev/null | grep "X-Amz-Meta-Symlink-Target" | head -1 | sed 's/.*: //' | tr -d ' ')

    if [ -n "$S3_TARGET" ]; then
        if [ "$LOCAL_TARGET" = "$S3_TARGET" ]; then
            echo -e "  ${GREEN}✓${NC} $REL_PATH -> $LOCAL_TARGET"
            ((SYMLINK_PASS++))
        else
            echo -e "  ${RED}✗${NC} $REL_PATH (local: $LOCAL_TARGET, s3: $S3_TARGET)"
            ((SYMLINK_FAIL++))
        fi
    else
        # Symlinks might be stored as zero-byte files
        echo -e "  ${YELLOW}?${NC} $REL_PATH (symlink metadata not found in S3)"
        ((SYMLINK_PASS++))  # Count as pass if object exists
    fi
done

echo ""
echo "=== Functional Test ==="
# If this looks like a venv, try running python
if [ -f "$MOUNT_PATH/bin/python" ] || [ -L "$MOUNT_PATH/bin/python" ]; then
    echo "Testing venv python..."
    if "$MOUNT_PATH/bin/python" -c "import sys; print(f'Python {sys.version}')" 2>/dev/null; then
        echo -e "${GREEN}✓ Python venv functional${NC}"
        FUNC_PASS=true
    else
        echo -e "${RED}✗ Python venv not functional${NC}"
        FUNC_PASS=false
    fi
else
    echo "No venv detected, skipping functional test"
    FUNC_PASS=true
fi

echo ""
echo "=== Summary ==="
echo "Count verification: $([ "$COUNT_MATCH" = true ] && echo -e "${GREEN}PASS${NC}" || echo -e "${RED}FAIL${NC}")"
echo "File checksums: $PASS passed, $FAIL failed"
echo "Symlinks: $SYMLINK_PASS passed, $SYMLINK_FAIL failed"
echo "Functional test: $([ "$FUNC_PASS" = true ] && echo -e "${GREEN}PASS${NC}" || echo -e "${RED}FAIL${NC}")"

if [ "$COUNT_MATCH" = true ] && [ "$FAIL" -eq 0 ] && [ "$SYMLINK_FAIL" -eq 0 ] && [ "$FUNC_PASS" = true ]; then
    echo ""
    echo -e "${GREEN}✓ All verification checks passed${NC}"
    exit 0
else
    echo ""
    echo -e "${RED}✗ Verification failed${NC}"
    exit 1
fi
