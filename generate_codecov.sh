#!/bin/bash
set -euo pipefail

echo "==================================="
echo "[STEP 1] Installing dependencies"
echo "==================================="

# Update package list
sudo apt-get update

# Install LLVM (for llvm-profdata, llvm-cov)
if ! command -v llvm-profdata &>/dev/null; then
    echo "[ACTION] Installing LLVM via apt"
    sudo apt-get install -y llvm
fi

# Install lcov (provides lcov + genhtml)
if ! command -v genhtml &>/dev/null; then
    echo "[ACTION] Installing lcov via apt"
    sudo apt-get install -y lcov
fi

# Install Python plugin for LCOV export
if ! python -m pip show coverage-lcov &>/dev/null; then
    echo "[ACTION] Installing coverage-lcov via pip"
    python -m pip install coverage-lcov
fi

# Install LCOV → Cobertura converter (for ADO)
if ! python -m pip show lcov-cobertura &>/dev/null; then
    echo "[ACTION] Installing lcov-cobertura via pip"
    python -m pip install lcov-cobertura
fi

echo "==================================="
echo "[STEP 2] Running pytest with Python coverage"
echo "==================================="

# Cleanup old coverage
rm -f .coverage coverage.xml python-coverage.info cpp-coverage.info total.info
rm -f default.profraw default.profdata
rm -rf htmlcov unified-coverage profraw

# Capture one raw profile *per process* so subprocess-based tests count too.
# Several pooling tests (idle eviction, pool-full, orphan return) must run in a
# fresh interpreter because the C++ pool config is locked in via std::call_once;
# they spawn `python -c` workers. With a fixed LLVM_PROFILE_FILE every process
# writes the same default.profraw and the last writer (the main pytest process)
# clobbers the workers, dropping their C++ coverage entirely. Using %p (PID) and
# %m (binary signature) gives each instrumented process its own file, which we
# merge below. Subprocess workers inherit this env var (os.environ.copy()).
mkdir -p "$(pwd)/profraw"
export LLVM_PROFILE_FILE="$(pwd)/profraw/default-%p-%m.profraw"

# Run pytest with Python coverage (XML + HTML output)
python -m pytest -v \
  --junitxml=test-results.xml \
  --cov=mssql_python \
  --cov-report=xml:coverage.xml \
  --cov-report=html \
  --capture=tee-sys \
  --cache-clear

# Convert Python coverage to LCOV format (restrict to repo only)
echo "[ACTION] Converting Python coverage to LCOV"
coverage lcov -o python-coverage.info --include="mssql_python/*"

echo "==================================="
echo "[STEP 3] Processing C++ coverage (Clang/LLVM)"
echo "==================================="

# Merge raw profile data from every instrumented process (main pytest run plus
# any `python -c` subprocess workers). Each wrote its own profraw/*.profraw via
# the LLVM_PROFILE_FILE pattern set in STEP 2.
shopt -s nullglob
PROFRAW_FILES=(profraw/*.profraw)
# Fallback: pick up a legacy single default.profraw if one exists in CWD.
if [ -f default.profraw ]; then
    PROFRAW_FILES+=(default.profraw)
fi
if [ ${#PROFRAW_FILES[@]} -eq 0 ]; then
    echo "[ERROR] No .profraw files found. Did you build with -fprofile-instr-generate?"
    exit 1
fi

echo "[INFO] Merging ${#PROFRAW_FILES[@]} raw profile file(s)"
llvm-profdata merge -sparse "${PROFRAW_FILES[@]}" -o default.profdata

# Find the pybind .so file (Linux build)
PYBIND_SO=$(find mssql_python -name "*.so" | head -n 1)
if [ -z "$PYBIND_SO" ]; then
    echo "[ERROR] Could not find pybind .so"
    exit 1
fi

echo "[INFO] Using pybind module: $PYBIND_SO"

# Export C++ coverage, excluding Python headers, pybind11, system includes, and vendored deps
llvm-cov export "$PYBIND_SO" \
  -instr-profile=default.profdata \
  -ignore-filename-regex='(python3\.[0-9]+|cpython|pybind11|/usr/include/|/usr/lib/|build/_deps/)' \
  --skip-functions \
  -format=lcov > cpp-coverage.info

# Note: LCOV exclusion markers (LCOV_EXCL_LINE) are processed below

echo "==================================="
echo "[STEP 4] Merging Python + C++ coverage"
echo "==================================="

# Merge LCOV reports and filter LOG statements using --omit-lines
# The --omit-lines option excludes lines matching the regex from coverage
# Since we joined multi-line LOGs during build, they're now on single lines
echo "[ACTION] Merging Python and C++ coverage with LOG exclusion"
lcov -a python-coverage.info -a cpp-coverage.info -o total-unfiltered.info \
  --omit-lines '\bLOG[A-Z_]*\s*\(' \
  --ignore-errors inconsistent,corrupt

echo "[INFO] Coverage merged with LOG statements excluded"

# Defense-in-depth: drop any vendored third-party sources pulled in via CMake
# FetchContent (e.g. simdutf). The llvm-cov ignore-filename-regex above is the
# primary filter; this catches anything that slips through future deps.
echo "[ACTION] Removing vendored third-party sources from merged coverage"
lcov --remove total-unfiltered.info '*/build/_deps/*' -o total.info \
  --ignore-errors inconsistent,unused

# Normalize paths so everything starts from mssql_python/
echo "[ACTION] Normalizing paths in LCOV report"
sed -i "s|$(pwd)/||g" total.info

# Generate full HTML report
echo "[ACTION] Generating HTML coverage report"
genhtml total.info \
  --output-directory unified-coverage \
  --quiet \
  --title "Unified Coverage Report"

# Generate Cobertura XML (for Azure DevOps Code Coverage tab)
lcov_cobertura total.info --output coverage.xml

echo "==================================="
echo "[STEP 5] Cleanup"
echo "==================================="

# Restore original source files if they were backed up during coverage build
BACKUP_FILE="mssql_python/pybind/.source_backup_coverage.tar.gz"
if [ -f "$BACKUP_FILE" ]; then
    echo "[ACTION] Restoring original source files from backup"
    (cd mssql_python/pybind && tar -xzf .source_backup_coverage.tar.gz)
    rm -f "$BACKUP_FILE"
    echo "[INFO] Original source files restored"
fi

echo "[INFO] Coverage report generation complete"
