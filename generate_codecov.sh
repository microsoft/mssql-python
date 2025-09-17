#!/bin/bash
set -euo pipefail

echo "==================================="
echo "[STEP 1] Installing dependencies"
echo "==================================="

# Ensure Homebrew exists
if ! command -v brew &>/dev/null; then
    echo "[ERROR] Homebrew is required. Install from https://brew.sh/"
    exit 1
fi

# Install LLVM (for llvm-profdata, llvm-cov)
if ! command -v llvm-profdata &>/dev/null; then
    echo "[ACTION] Installing LLVM via Homebrew"
    brew install llvm
fi
export PATH="/opt/homebrew/opt/llvm/bin:$PATH"

# Install lcov (provides lcov + genhtml)
if ! command -v genhtml &>/dev/null; then
    echo "[ACTION] Installing lcov via Homebrew"
    brew install lcov
fi

# Install Python plugin for LCOV export
if ! python -m pip show coverage-lcov &>/dev/null; then
    echo "[ACTION] Installing coverage-lcov via pip"
    python -m pip install coverage-lcov
fi

echo "==================================="
echo "[STEP 2] Running pytest with Python coverage"
echo "==================================="

# Cleanup old coverage
rm -f .coverage coverage.xml python-coverage.info cpp-coverage.info total.info
rm -rf htmlcov unified-coverage

# Run pytest with Python coverage (XML + HTML output)
python -m pytest -v \
  --junitxml=test-results.xml \
  --cov=mssql_python \
  --cov-report=xml \
  --cov-report=html \
  --capture=tee-sys \
  --cache-clear

# Convert Python coverage to LCOV format (restrict to your repo only)
echo "[ACTION] Converting Python coverage to LCOV"
coverage lcov -o python-coverage.info --include="mssql_python/*"

echo "==================================="
echo "[STEP 3] Processing C++ coverage (Clang/LLVM)"
echo "==================================="

# Merge raw profile data from pybind runs
if [ ! -f default.profraw ]; then
    echo "[ERROR] default.profraw not found. Did you build with -fprofile-instr-generate?"
    exit 1
fi

llvm-profdata merge -sparse default.profraw -o default.profdata

# Find the pybind .so (assuming universal2 build)
PYBIND_SO=$(find mssql_python -name "*.so" | grep "universal2" | head -n 1)
if [ -z "$PYBIND_SO" ]; then
    echo "[ERROR] Could not find pybind .so (universal2 build)."
    exit 1
fi

echo "[INFO] Using pybind module: $PYBIND_SO"

# Export C++ coverage, excluding Python headers, pybind11, and Homebrew includes
llvm-cov export "$PYBIND_SO" \
  -instr-profile=default.profdata \
  -arch arm64 \
  -ignore-filename-regex='(python3\.13|cpython|pybind11|homebrew)' \
  -format=lcov > cpp-coverage.info

echo "==================================="
echo "[STEP 4] Merging Python + C++ coverage"
echo "==================================="

# Merge LCOV reports (ignore minor inconsistencies in Python LCOV export)
lcov -a python-coverage.info -a cpp-coverage.info -o total.info \
  --ignore-errors inconsistent,corrupt

# Generate unified HTML report
genhtml total.info --output-directory unified-coverage

echo "[SUCCESS] Unified coverage report generated at unified-coverage/index.html"