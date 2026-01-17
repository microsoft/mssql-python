#!/bin/bash
# Build mssql-py-core from mssql-tds repo and install into mssql-python
#
# Prerequisites:
#   - Rust toolchain installed (https://rustup.rs/)
#   - maturin installed: pip install maturin
#   - Both mssql-python and mssql-tds repos cloned as siblings:
#       parent-folder/
#       ├── mssql-python/   (this repo)
#       └── mssql-tds/      (Rust TDS library)
#
# Usage:
#   ./scripts/build-py-core.sh          # Build and install .so to mssql_python/
#   ./scripts/build-py-core.sh --dev    # Use maturin develop (faster, editable)
#   ./scripts/build-py-core.sh --clean  # Clean build artifacts first
#
# This script builds mssql_py_core (Rust-based Python bindings) and places 
# the resulting .so file in the mssql_python/ directory alongside ddbc_bindings.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MSSQL_PYTHON_DIR="$(dirname "$SCRIPT_DIR")"
MSSQL_TDS_DIR="$(dirname "$MSSQL_PYTHON_DIR")/mssql-tds"
PY_CORE_DIR="$MSSQL_TDS_DIR/mssql-py-core"
TARGET_DIR="$MSSQL_PYTHON_DIR/mssql_python"

# Parse arguments
USE_DEV_MODE=false
CLEAN_FIRST=false

for arg in "$@"; do
    case $arg in
        --dev)
            USE_DEV_MODE=true
            ;;
        --clean)
            CLEAN_FIRST=true
            ;;
        --help|-h)
            echo "Usage: $0 [--dev] [--clean]"
            echo ""
            echo "Options:"
            echo "  --dev    Use 'maturin develop' for faster builds (requires venv)"
            echo "  --clean  Clean build artifacts before building"
            echo ""
            exit 0
            ;;
    esac
done

echo "=========================================="
echo "Building mssql-py-core for mssql-python"
echo "=========================================="

# Check if mssql-tds repo exists
if [ ! -d "$MSSQL_TDS_DIR" ]; then
    echo "ERROR: mssql-tds repo not found at: $MSSQL_TDS_DIR"
    echo ""
    echo "Please clone mssql-tds as a sibling to mssql-python:"
    echo "  cd $(dirname "$MSSQL_PYTHON_DIR")"
    echo "  git clone <mssql-tds-repo-url> mssql-tds"
    exit 1
fi

if [ ! -d "$PY_CORE_DIR" ]; then
    echo "ERROR: mssql-py-core directory not found at: $PY_CORE_DIR"
    exit 1
fi

# Check for Rust toolchain
if ! command -v cargo &> /dev/null; then
    echo "ERROR: Rust toolchain not found. Please install Rust:"
    echo "  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh"
    exit 1
fi

# Check for maturin
if ! command -v maturin &> /dev/null; then
    echo "ERROR: maturin not found. Please install:"
    echo "  pip install maturin"
    exit 1
fi

# Clean if requested
if [ "$CLEAN_FIRST" = true ]; then
    echo "[ACTION] Cleaning build artifacts..."
    rm -rf "$PY_CORE_DIR/target"
    rm -f "$TARGET_DIR"/mssql_py_core.*.so
fi

# Get Python version info
PYTHON_VERSION=$(python3 -c "import sys; print(f'{sys.version_info.major}{sys.version_info.minor}')")
echo "[INFO] Python version: $PYTHON_VERSION"
echo "[INFO] mssql-tds path: $MSSQL_TDS_DIR"
echo "[INFO] Target directory: $TARGET_DIR"

cd "$PY_CORE_DIR"

if [ "$USE_DEV_MODE" = true ]; then
    echo ""
    echo "[ACTION] Running maturin develop (editable install)..."
    maturin develop --release
    
    # Find and copy the .so file from the venv to mssql_python/
    echo "[ACTION] Copying .so to mssql_python/..."
    
    # Find the .so file in site-packages
    SITE_PACKAGES=$(python3 -c "import site; print(site.getsitepackages()[0])")
    SO_FILE=$(find "$SITE_PACKAGES" -name "mssql_py_core*.so" 2>/dev/null | head -1)
    
    if [ -n "$SO_FILE" ] && [ -f "$SO_FILE" ]; then
        cp -f "$SO_FILE" "$TARGET_DIR/"
        echo "[SUCCESS] Copied: $(basename "$SO_FILE")"
    else
        echo "[WARNING] .so file not found in site-packages, trying target directory..."
    fi
else
    echo ""
    echo "[ACTION] Running maturin build --release..."
    maturin build --release --out "$MSSQL_PYTHON_DIR/target/wheels"
    
    echo ""
    echo "[ACTION] Extracting .so from wheel..."
    
    WHEEL_DIR="$MSSQL_PYTHON_DIR/target/wheels"
    WHEEL_FILE=$(ls -1 "$WHEEL_DIR"/mssql_py_core-*.whl 2>/dev/null | head -1)
    
    if [ -n "$WHEEL_FILE" ] && [ -f "$WHEEL_FILE" ]; then
        # Extract .so from wheel
        unzip -o "$WHEEL_FILE" "*.so" -d "$TARGET_DIR/" 2>/dev/null || true
        
        # Move .so file if extracted to subdirectory
        find "$TARGET_DIR" -name "mssql_py_core*.so" -exec mv {} "$TARGET_DIR/" \; 2>/dev/null || true
        
        echo "[SUCCESS] Wheel built: $(basename "$WHEEL_FILE")"
    else
        echo "[ERROR] No wheel file found in $WHEEL_DIR"
        exit 1
    fi
fi

# Verify the result
echo ""
echo "[RESULT] mssql_python directory contents:"
ls -la "$TARGET_DIR"/*.so 2>/dev/null || echo "No .so files found"

# Check specifically for mssql_py_core
if ls "$TARGET_DIR"/mssql_py_core*.so 1>/dev/null 2>&1; then
    echo ""
    echo "=========================================="
    echo "[SUCCESS] mssql-py-core built and installed!"
    echo "=========================================="
    echo ""
    echo "You can now import mssql_py_core in Python:"
    echo "  from mssql_python import mssql_py_core"
else
    echo ""
    echo "[WARNING] mssql_py_core .so file not found in $TARGET_DIR"
    echo "Check the build output above for errors."
fi
