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

# Find the pybind .so file (Linux build)
PYBIND_SO=$(find mssql_python -name "*.so" | head -n 1)
if [ -z "$PYBIND_SO" ]; then
    echo "[ERROR] Could not find pybind .so"
    exit 1
fi

echo "[INFO] Using pybind module: $PYBIND_SO"

# Export C++ coverage, excluding Python headers, pybind11, and system includes
llvm-cov export "$PYBIND_SO" \
  -instr-profile=default.profdata \
  -ignore-filename-regex='(python3\.[0-9]+|cpython|pybind11|/usr/include/|/usr/lib/)' \
  -format=lcov > cpp-coverage.info

echo "==================================="
echo "[STEP 4] Merging Python + C++ coverage"
echo "==================================="

# Merge LCOV reports (ignore minor inconsistencies in Python LCOV export)
lcov -a python-coverage.info -a cpp-coverage.info -o total.info \
  --ignore-errors inconsistent,corrupt

# Generate full HTML report
genhtml total.info --output-directory unified-coverage --quiet --title "Unified Coverage Report"

# Generate plain-text summary for PR comment
lcov --summary total.info > unified-coverage/summary.txt

# Append link to artifact
echo "" >> unified-coverage/summary.txt
echo "👉 [Download full HTML report](https://github.com/${GITHUB_REPOSITORY}/actions/runs/${GITHUB_RUN_ID}#artifacts)" >> unified-coverage/summary.txt

echo "[SUCCESS] Unified coverage report generated at unified-coverage/index.html"
