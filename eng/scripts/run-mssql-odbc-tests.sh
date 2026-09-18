#!/bin/bash
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

# Run every mssql-python test file in an isolated process with mssql-odbc
# selected. Functional failures are advisory, but crashes and timeouts still
# produce JUnit entries so the pipeline reports the complete result.

set -uo pipefail

RESULTS_DIR="${TEST_RESULTS_DIR:-test-results/mssql-odbc}"
FILE_TIMEOUT="${PYTEST_FILE_TIMEOUT:-10m}"
TOTAL_BUDGET="${PYTEST_TOTAL_BUDGET:-110m}"
STATUS_FILE="${TEST_STATUS_FILE:-$RESULTS_DIR/runner.status}"
KILL_GRACE_SECONDS=10

if ! mkdir -p "$RESULTS_DIR"; then
    echo "##[error]Could not create test results directory: $RESULTS_DIR"
    exit 2
fi
rm -f "$RESULTS_DIR"/*.xml
rm -f "$STATUS_FILE"

finish() {
    local code="$1" status="$2"
    printf '%s\n' "$status" > "$STATUS_FILE" || exit 2
    exit "$code"
}

on_exit() {
    local code=$?
    if [ ! -s "$STATUS_FILE" ]; then
        printf 'harness\n' > "$STATUS_FILE" 2>/dev/null || true
    fi
    return "$code"
}
trap on_exit EXIT

xml_escape() {
    printf '%s' "$1" | sed -e 's/&/\&amp;/g' -e 's/</\&lt;/g' -e 's/>/\&gt;/g' -e 's/"/\&quot;/g'
}

write_stub() {
    local name="$1" kind="$2" reason="$3" report="$4"
    local safe_name safe_reason
    safe_name="$(xml_escape "$name")"
    safe_reason="$(xml_escape "$reason")"
    if [ "$kind" = "skipped" ]; then
        cat > "$report" <<XML
<?xml version="1.0" encoding="utf-8"?>
<testsuites>
  <testsuite name="$safe_name" tests="1" errors="0" failures="0" skipped="1" time="0">
    <testcase classname="mssql_odbc.$safe_name" name="pytest_process" time="0">
      <skipped type="pytest.skip" message="$safe_reason"/>
    </testcase>
  </testsuite>
</testsuites>
XML
    else
        cat > "$report" <<XML
<?xml version="1.0" encoding="utf-8"?>
<testsuites>
  <testsuite name="$safe_name" tests="1" errors="1" failures="0" skipped="0" time="0">
    <testcase classname="mssql_odbc.$safe_name" name="pytest_process" time="0">
      <error type="ProcessTerminated" message="$safe_reason">The pytest process terminated before producing a report.</error>
    </testcase>
  </testsuite>
</testsuites>
XML
    fi
}

to_seconds() {
    local value="$1" number="${1%[smh]}"
    case "$value" in
        *h) echo $((number * 3600)) ;;
        *m) echo $((number * 60)) ;;
        *s) echo "$number" ;;
        *)  echo "$value" ;;
    esac
}

report_is_valid() {
    python - "$1" <<'PY'
import sys
from xml.etree import ElementTree

ElementTree.parse(sys.argv[1])
PY
}

mapfile -t TEST_FILES < <(find tests -name 'test_*.py' -type f | sort)
if [ "${#TEST_FILES[@]}" -eq 0 ] || ! python -m pytest --version >/dev/null 2>&1; then
    echo "##[error]The pytest harness is not usable"
    finish 2 harness
fi

FILE_BUDGET_SECONDS="$(to_seconds "$FILE_TIMEOUT")"
TOTAL_BUDGET_SECONDS="$(to_seconds "$TOTAL_BUDGET")"
passed=0
failed=0
crashed=0
timed_out=0
skipped=0

echo "Running ${#TEST_FILES[@]} test files with MSSQL_PYTHON_NATIVE_PROVIDER=${MSSQL_PYTHON_NATIVE_PROVIDER:-unset}"

for index in "${!TEST_FILES[@]}"; do
    test_file="${TEST_FILES[$index]}"
    name="${test_file#tests/}"
    name="${name%.py}"
    name="${name//\//_}"
    report="$RESULTS_DIR/results-$name.xml"
    remaining=$((TOTAL_BUDGET_SECONDS - SECONDS))

    if [ "$remaining" -le "$KILL_GRACE_SECONDS" ]; then
        remaining_files=$((${#TEST_FILES[@]} - index))
        for rest_index in $(seq "$index" $((${#TEST_FILES[@]} - 1))); do
            rest_file="${TEST_FILES[$rest_index]}"
            rest_name="${rest_file#tests/}"
            rest_name="${rest_name%.py}"
            rest_name="${rest_name//\//_}"
            write_stub "$rest_name" error "Total test budget of $TOTAL_BUDGET exhausted before this file ran" \
                "$RESULTS_DIR/results-$rest_name.xml"
        done
        timed_out=$((timed_out + remaining_files))
        break
    fi

    slice="$FILE_BUDGET_SECONDS"
    if [ "$slice" -gt "$((remaining - KILL_GRACE_SECONDS))" ]; then
        slice=$((remaining - KILL_GRACE_SECONDS))
    fi

    echo "##[group]$test_file"
    timeout --kill-after="${KILL_GRACE_SECONDS}s" "${slice}s" \
        python -m pytest "$test_file" -v --junitxml="$report" \
        --capture=tee-sys --cache-clear
    rc=$?
    echo "##[endgroup]"

    case "$rc" in
        0)
            passed=$((passed + 1))
            ;;
        1)
            failed=$((failed + 1))
            ;;
        2|3|4)
            echo "##[error]The pytest harness failed on $test_file (exit $rc)"
            finish 2 harness
            ;;
        5)
            write_stub "$name" skipped "No tests collected" "$report"
            skipped=$((skipped + 1))
            ;;
        124|137)
            timed_out=$((timed_out + 1))
            ;;
        125|126|127)
            echo "##[error]The pytest harness could not execute $test_file (exit $rc)"
            finish 2 harness
            ;;
        *)
            crashed=$((crashed + 1))
            ;;
    esac

    if [ ! -s "$report" ] || ! report_is_valid "$report" >/dev/null 2>&1; then
        if [ "$rc" -eq 0 ]; then
            echo "##[error]Pytest reported success for $test_file but produced no valid JUnit results (harness failure)"
            finish 2 harness
        fi
        write_stub "$name" error "Pytest exited $rc without producing valid JUnit" "$report"
    fi
    if [ "$rc" -eq 124 ] || [ "$rc" -eq 137 ] || [ "$rc" -gt 127 ]; then
        write_stub "${name}_process" error "Pytest process exited $rc after producing JUnit" \
            "$RESULTS_DIR/results-$name-process.xml"
    fi
done

echo "files: ${#TEST_FILES[@]} | passed: $passed | failed: $failed | crashed: $crashed | timed out: $timed_out | skipped: $skipped"

if [ "$((passed + failed + crashed + timed_out))" -eq 0 ]; then
    echo "##[error]No test file executed any tests"
    finish 2 harness
fi

# Intentional no-test files (pytest exit 5, e.g. stress files excluded by the
# "not stress" marker) count as skipped and must not mark the run SucceededWithIssues.
if [ "$failed" -gt 0 ] || [ "$crashed" -gt 0 ] || [ "$timed_out" -gt 0 ]; then
    finish 1 advisory
fi
finish 0 success
