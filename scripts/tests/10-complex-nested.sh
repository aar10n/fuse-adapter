#!/usr/bin/env bash
#
# Complex nested directory structure tests
# Tests deep nesting, wide directories, and mixed file/directory structures
#

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

echo "=== Complex Nested Directory Structures ==="

SUFFIX=$(random_suffix)
BASE="${MOUNT_PATH}/complex_${SUFFIX}"

# Test 1: Deep nesting (10 levels)
echo ""
echo "--- Deep Nesting Tests ---"
DEEP_PATH="${BASE}/deep/level1/level2/level3/level4/level5/level6/level7/level8/level9/level10"
run_test "mkdir -p 10 levels" "mkdir -p '${DEEP_PATH}'"
run_test "deep path exists" "assert_is_dir '${DEEP_PATH}'"
run_test "file at depth 10" "echo 'deep content' > '${DEEP_PATH}/deep_file.txt'"
run_test "read file at depth 10" "[[ \$(cat '${DEEP_PATH}/deep_file.txt') == 'deep content' ]]"

# Verify intermediate directories exist
run_test "level 5 exists" "assert_is_dir '${BASE}/deep/level1/level2/level3/level4/level5'"
run_test "level 3 exists" "assert_is_dir '${BASE}/deep/level1/level2/level3'"

# Test 2: Wide directory (many siblings at same level)
echo ""
echo "--- Wide Directory Tests ---"
WIDE="${BASE}/wide"
run_test "create wide base" "mkdir -p '${WIDE}'"

# Create 20 subdirectories
for i in $(seq 1 20); do
    run_test "mkdir subdir_${i}" "mkdir '${WIDE}/subdir_${i}'"
done

run_test "count 20 subdirs" "[[ \$(ls '${WIDE}' | wc -l | tr -d ' ') -eq 20 ]]"

# Create file in each subdirectory
for i in $(seq 1 20); do
    run_test "file in subdir_${i}" "echo 'content_${i}' > '${WIDE}/subdir_${i}/file.txt'"
done

# Verify random sample of files
run_test "verify file in subdir_5" "[[ \$(cat '${WIDE}/subdir_5/file.txt') == 'content_5' ]]"
run_test "verify file in subdir_15" "[[ \$(cat '${WIDE}/subdir_15/file.txt') == 'content_15' ]]"

# Test 3: Sequential mkdir (not using -p)
echo ""
echo "--- Sequential mkdir Tests ---"
SEQ="${BASE}/sequential"
run_test "mkdir seq base" "mkdir '${SEQ}'"
run_test "mkdir seq level1" "mkdir '${SEQ}/level1'"
run_test "mkdir seq level2" "mkdir '${SEQ}/level1/level2'"
run_test "mkdir seq level3" "mkdir '${SEQ}/level1/level2/level3'"
run_test "seq level3 exists" "assert_is_dir '${SEQ}/level1/level2/level3'"

# Test 4: Mixed structure (files and directories at each level)
echo ""
echo "--- Mixed Structure Tests ---"
MIXED="${BASE}/mixed"
run_test "create mixed base" "mkdir -p '${MIXED}'"

# Level 1: 3 dirs + 2 files
run_test "mixed: mkdir dir_a" "mkdir '${MIXED}/dir_a'"
run_test "mixed: mkdir dir_b" "mkdir '${MIXED}/dir_b'"
run_test "mixed: mkdir dir_c" "mkdir '${MIXED}/dir_c'"
run_test "mixed: create file1.txt" "echo 'file1' > '${MIXED}/file1.txt'"
run_test "mixed: create file2.txt" "echo 'file2' > '${MIXED}/file2.txt'"

# Level 2 in dir_a: 2 dirs + 1 file
run_test "mixed: mkdir dir_a/sub1" "mkdir '${MIXED}/dir_a/sub1'"
run_test "mixed: mkdir dir_a/sub2" "mkdir '${MIXED}/dir_a/sub2'"
run_test "mixed: create dir_a/data.txt" "echo 'data' > '${MIXED}/dir_a/data.txt'"

# Level 3 in dir_a/sub1: 1 dir + 2 files
run_test "mixed: mkdir dir_a/sub1/deep" "mkdir '${MIXED}/dir_a/sub1/deep'"
run_test "mixed: create dir_a/sub1/a.txt" "echo 'a' > '${MIXED}/dir_a/sub1/a.txt'"
run_test "mixed: create dir_a/sub1/b.txt" "echo 'b' > '${MIXED}/dir_a/sub1/b.txt'"

# Verify structure
run_test "mixed: count level1" "[[ \$(ls '${MIXED}' | wc -l | tr -d ' ') -eq 5 ]]"
run_test "mixed: count dir_a" "[[ \$(ls '${MIXED}/dir_a' | wc -l | tr -d ' ') -eq 3 ]]"
run_test "mixed: count dir_a/sub1" "[[ \$(ls '${MIXED}/dir_a/sub1' | wc -l | tr -d ' ') -eq 3 ]]"

# Test 5: Create nested structure then delete middle and recreate
echo ""
echo "--- Delete and Recreate Tests ---"
RECREATE="${BASE}/recreate/a/b/c"
run_test "create for recreate" "mkdir -p '${RECREATE}'"
run_test "file in recreate" "echo 'original' > '${RECREATE}/file.txt'"
run_test "delete middle (rm -rf b)" "rm -rf '${BASE}/recreate/a/b'"
run_test "middle deleted" "assert_not_exists '${BASE}/recreate/a/b'"
run_test "recreate path" "mkdir -p '${RECREATE}'"
run_test "new file in recreated" "echo 'new' > '${RECREATE}/file.txt'"
run_test "verify new content" "[[ \$(cat '${RECREATE}/file.txt') == 'new' ]]"

# Test 6: Parallel directory creation (multiple mkdir -p calls)
echo ""
echo "--- Parallel Structure Creation ---"
PARALLEL="${BASE}/parallel"
# Create multiple deep paths that share common prefixes
run_test "parallel path 1" "mkdir -p '${PARALLEL}/shared/branch1/deep1/deeper1'"
run_test "parallel path 2" "mkdir -p '${PARALLEL}/shared/branch1/deep2/deeper2'"
run_test "parallel path 3" "mkdir -p '${PARALLEL}/shared/branch2/deep1/deeper1'"
run_test "parallel path 4" "mkdir -p '${PARALLEL}/shared/branch2/deep2/deeper2'"

# Verify shared parent exists
run_test "shared parent exists" "assert_is_dir '${PARALLEL}/shared'"
run_test "branch1 exists" "assert_is_dir '${PARALLEL}/shared/branch1'"
run_test "branch2 exists" "assert_is_dir '${PARALLEL}/shared/branch2'"

# Count items at branch level
run_test "2 branches exist" "[[ \$(ls '${PARALLEL}/shared' | wc -l | tr -d ' ') -eq 2 ]]"

# Create files in parallel structure
run_test "file in branch1/deep1" "echo '1-1' > '${PARALLEL}/shared/branch1/deep1/file.txt'"
run_test "file in branch1/deep2" "echo '1-2' > '${PARALLEL}/shared/branch1/deep2/file.txt'"
run_test "file in branch2/deep1" "echo '2-1' > '${PARALLEL}/shared/branch2/deep1/file.txt'"
run_test "file in branch2/deep2" "echo '2-2' > '${PARALLEL}/shared/branch2/deep2/file.txt'"

# Verify all files
run_test "verify branch1/deep1" "[[ \$(cat '${PARALLEL}/shared/branch1/deep1/file.txt') == '1-1' ]]"
run_test "verify branch2/deep2" "[[ \$(cat '${PARALLEL}/shared/branch2/deep2/file.txt') == '2-2' ]]"

# Test 7: Special characters in nested paths
echo ""
echo "--- Special Characters in Paths ---"
SPECIAL="${BASE}/special"
run_test "create special base" "mkdir -p '${SPECIAL}'"
run_test "dir with spaces" "mkdir '${SPECIAL}/dir with spaces'"
run_test "nested in spaces dir" "mkdir '${SPECIAL}/dir with spaces/nested'"
run_test "file in spaces path" "echo 'spaced' > '${SPECIAL}/dir with spaces/nested/file.txt'"
run_test "read from spaces path" "[[ \$(cat '${SPECIAL}/dir with spaces/nested/file.txt') == 'spaced' ]]"

# Underscores and dashes
run_test "dir with dash-and_underscore" "mkdir '${SPECIAL}/my-dir_name'"
run_test "nested with special chars" "mkdir -p '${SPECIAL}/my-dir_name/sub-1_a/sub-2_b'"
run_test "file in special path" "echo 'special' > '${SPECIAL}/my-dir_name/sub-1_a/sub-2_b/data.txt'"

# Test 8: Very long path components
echo ""
echo "--- Long Path Component Names ---"
LONG="${BASE}/long_names"
LONG_NAME="this_is_a_very_long_directory_name_that_tests_filesystem_limits"
run_test "create long names base" "mkdir -p '${LONG}'"
run_test "mkdir with long name" "mkdir '${LONG}/${LONG_NAME}'"
run_test "nested long names" "mkdir -p '${LONG}/${LONG_NAME}/${LONG_NAME}_2/${LONG_NAME}_3'"
run_test "file in long path" "echo 'long' > '${LONG}/${LONG_NAME}/${LONG_NAME}_2/${LONG_NAME}_3/file.txt'"
run_test "verify long path file" "[[ \$(cat '${LONG}/${LONG_NAME}/${LONG_NAME}_2/${LONG_NAME}_3/file.txt') == 'long' ]]"

# Test 9: Rapid create/check cycle
echo ""
echo "--- Rapid Create/Check Cycle ---"
RAPID="${BASE}/rapid"
run_test "rapid base" "mkdir -p '${RAPID}'"
for i in $(seq 1 10); do
    run_test "rapid cycle ${i}: create" "mkdir '${RAPID}/dir_${i}'"
    run_test "rapid cycle ${i}: verify" "assert_is_dir '${RAPID}/dir_${i}'"
    run_test "rapid cycle ${i}: file" "echo 'rapid_${i}' > '${RAPID}/dir_${i}/file.txt'"
    run_test "rapid cycle ${i}: read" "[[ \$(cat '${RAPID}/dir_${i}/file.txt') == 'rapid_${i}' ]]"
done

# Cleanup
echo ""
echo "--- Cleanup ---"
run_test "cleanup all" "rm -rf '${BASE}'"
run_test "base removed" "assert_not_exists '${BASE}'"

echo ""
