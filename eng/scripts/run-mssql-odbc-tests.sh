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

mkdir -p "$RESULTS_DIR"
rm -f "$RESULTS_DIR"/*.xml

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

mapfile -t TEST_FILES < <(find tests -name 'test_*.py' -type f | sort)
if [ "${#TEST_FILES[@]}" -eq 0 ] || ! python -m pytest --version >/dev/null 2>&1; then
    echo "##[error]The pytest harness is not usable"
    exit 2
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

    if [ "$remaining" -le 30 ]; then
        for rest_index in $(seq "$index" $((${#TEST_FILES[@]} - 1))); do
            rest_file="${TEST_FILES[$rest_index]}"
            rest_name="${rest_file#tests/}"
            rest_name="${rest_name%.py}"
            rest_name="${rest_name//\//_}"
            write_stub "$rest_name" skipped "Total test budget of $TOTAL_BUDGET exhausted" \
                "$RESULTS_DIR/results-$rest_name.xml"
            skipped=$((skipped + 1))
        done
        break
    fi

    slice="$FILE_BUDGET_SECONDS"
    if [ "$slice" -gt "$remaining" ]; then
        slice="$remaining"
    fi

    echo "##[group]$test_file"
    timeout --kill-after=60s "${slice}s" \
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
            exit 2
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
            exit 2
            ;;
        *)
            crashed=$((crashed + 1))
            ;;
    esac

    if [ ! -s "$report" ]; then
        if [ "$rc" -eq 0 ]; then
            echo "##[error]Pytest reported success for $test_file but produced no JUnit results (harness failure)"
            exit 2
        fi
        write_stub "$name" error "Pytest exited $rc without producing JUnit" "$report"
    fi
done

echo "files: ${#TEST_FILES[@]} | passed: $passed | failed: $failed | crashed: $crashed | timed out: $timed_out | skipped: $skipped"

if [ "$((passed + failed + crashed + timed_out))" -eq 0 ]; then
    echo "##[error]No test file executed any tests"
    exit 2
fi

# Intentional no-test files (pytest exit 5, e.g. stress files excluded by the
# "not stress" marker) count as skipped and must not mark the run SucceededWithIssues.
if [ "$failed" -gt 0 ] || [ "$crashed" -gt 0 ] || [ "$timed_out" -gt 0 ]; then
    exit 1
fi
