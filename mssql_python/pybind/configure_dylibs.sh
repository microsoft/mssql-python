#!/bin/bash
# Script to configure dylib paths for macOS
# This script fixes the library paths in the ODBC driver dylibs and codesigns them
set -euo pipefail

# Directory structure setup
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

ODBC_PACKAGE_DIR="${ODBC_PACKAGE_DIR:-$PROJECT_DIR/../mssql_python_odbc}"
ODBC_VERSION_FILE="$ODBC_PACKAGE_DIR/__init__.py"
if [ ! -f "$ODBC_VERSION_FILE" ]; then
  echo "Error: SSOT version file not found: $ODBC_VERSION_FILE"
  exit 1
fi

DRIVER_MAJOR=$(sed -nE "s/^__version__[[:space:]]*=[[:space:]]*['\"]([0-9]+)\.[0-9]+\.[0-9]+(\.[0-9]+)?['\"].*/\1/p" "$ODBC_VERSION_FILE" | head -1)
if [ -z "$DRIVER_MAJOR" ]; then
  echo "Error: failed to parse __version__ from $ODBC_VERSION_FILE"
  exit 1
fi

DRIVER_DYLIB_NAME="libmsodbcsql.${DRIVER_MAJOR}.dylib"

# The universal2 wheel bundles a per-arch copy of the driver dylibs, so every
# bundled arch must be configured, not just the build host's ($(uname -m)).
# install_name_tool and codesign both work cross-arch. Fixing only the host arch
# is what shipped the broken arm64 driver in issue #656.
for ARCH in arm64 x86_64; do
# Post-split the driver dylibs ship in the standalone mssql_python_odbc package
# (a pure-data sibling of mssql_python), so configure them there. Mirrors the
# ODBC_VERSION_FILE path above.
LIB_DIR="$ODBC_PACKAGE_DIR/libs/macos/$ARCH/lib"
LIBMSODBCSQL_PATH="$LIB_DIR/$DRIVER_DYLIB_NAME"
LIBMSODBCSQL_NAME="$DRIVER_DYLIB_NAME"
LIBODBCINST_PATH="$LIB_DIR/libodbcinst.2.dylib"
LIBLTDL_PATH="$LIB_DIR/libltdl.7.dylib"
MANIFEST="$LIB_DIR/../odbc-187-driver.json"

# Check if the directories and files exist
if [ ! -d "$LIB_DIR" ]; then
  echo "Note: library directory doesn't exist, skipping: $LIB_DIR"
  continue
fi

if [ ! -f "$LIBMSODBCSQL_PATH" ]; then
  echo "Error: $DRIVER_DYLIB_NAME not found at: $LIBMSODBCSQL_PATH"
  exit 1
fi

if [ ! -f "$LIBODBCINST_PATH" ]; then
  echo "Error: libodbcinst.2.dylib not found at: $LIBODBCINST_PATH"
  exit 1
fi

echo "Initial configuration:"
otool -L "$LIBMSODBCSQL_PATH"
otool -L "$LIBODBCINST_PATH"
if [ -f "$LIBLTDL_PATH" ]; then
  otool -L "$LIBLTDL_PATH"
fi

echo "Configuring dylibs in: $LIB_DIR"

if [ -f "$MANIFEST" ]; then
  python - "$MANIFEST" "$LIBMSODBCSQL_PATH" <<'PY'
import hashlib
import json
from pathlib import Path
import sys

manifest = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
driver = manifest["driver"]
expected = driver.get("prepared_sha256", driver["sha256"])
actual = hashlib.sha256(Path(sys.argv[2]).read_bytes()).hexdigest()
if manifest["version"] != "18.7.1.1" or actual != expected:
    raise RuntimeError("ODBC candidate changed before macOS relocation")
PY
fi

# Get the existing library paths which are linked to the dylibs
echo "Reading dependencies from $LIBMSODBCSQL_NAME..."
OTOOL_LIST=$(otool -L "$LIBMSODBCSQL_PATH")
OLD_LIBODBCINST_PATH=""

# In the otool list of libmsodbcsql_path, get the library path for libodbcinst
while IFS= read -r line; do
  if [[ "$line" == *"libodbcinst"* ]]; then
    OLD_LIBODBCINST_PATH=$(echo "$line" | awk '{print $1}')
    break
  fi
done <<< "$OTOOL_LIST"

echo "Reading dependencies from libodbcinst.2.dylib..."
OTOOL_LIST=$(otool -L "$LIBODBCINST_PATH")
OLD_LIBLTDL_PATH=""

# In the otool list of libodbcinst_path, get the library path for libltdl
while IFS= read -r line; do
  if [[ "$line" == *"libltdl"* ]]; then
    OLD_LIBLTDL_PATH=$(echo "$line" | awk '{print $1}')
    break
  fi
done <<< "$OTOOL_LIST"

# Configure the library paths if dependencies were found
if [ -n "$OLD_LIBODBCINST_PATH" ]; then
  echo "Fixing $LIBMSODBCSQL_NAME dependency on libodbcinst.2.dylib..."
  echo "  Changing: $OLD_LIBODBCINST_PATH"
  echo "  To: @loader_path/libodbcinst.2.dylib"
  install_name_tool -change "$OLD_LIBODBCINST_PATH" "@loader_path/libodbcinst.2.dylib" "$LIBMSODBCSQL_PATH"
else
  echo "Warning: libodbcinst dependency not found in $LIBMSODBCSQL_NAME"
fi

if [ -n "$OLD_LIBLTDL_PATH" ] && [ -f "$LIBLTDL_PATH" ]; then
  echo "Fixing libodbcinst.2.dylib dependency on libltdl.7.dylib..."
  echo "  Changing: $OLD_LIBLTDL_PATH"
  echo "  To: @loader_path/libltdl.7.dylib"
  install_name_tool -change "$OLD_LIBLTDL_PATH" "@loader_path/libltdl.7.dylib" "$LIBODBCINST_PATH"
else
  echo "Note: libltdl dependency not found or not needed"
fi

# Force codesign the dylibs

# First set the IDs of the libraries using @loader_path
echo "Setting library IDs with @loader_path..."
echo "Setting ID for $LIBMSODBCSQL_NAME..."
install_name_tool -id "@loader_path/$LIBMSODBCSQL_NAME" "$LIBMSODBCSQL_PATH"

echo "Setting ID for libodbcinst.2.dylib..."
install_name_tool -id "@loader_path/libodbcinst.2.dylib" "$LIBODBCINST_PATH"

if [ -f "$LIBLTDL_PATH" ]; then
  echo "Setting ID for libltdl.7.dylib..."
  install_name_tool -id "@loader_path/libltdl.7.dylib" "$LIBLTDL_PATH"
fi

echo "Codesigning $LIBMSODBCSQL_NAME..."
codesign -s - -f "$LIBMSODBCSQL_PATH"

echo "Codesigning libodbcinst.2.dylib..."
codesign -s - -f "$LIBODBCINST_PATH"

if [ -f "$LIBLTDL_PATH" ]; then
  echo "Codesigning libltdl.7.dylib..."
  codesign -s - -f "$LIBLTDL_PATH"
fi

if [ -f "$MANIFEST" ]; then
  python - "$MANIFEST" "$LIBMSODBCSQL_PATH" <<'PY'
import hashlib
import json
from pathlib import Path
import sys

path = Path(sys.argv[1])
manifest = json.loads(path.read_text(encoding="utf-8"))
manifest["driver"]["prepared_sha256"] = hashlib.sha256(Path(sys.argv[2]).read_bytes()).hexdigest()
path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
PY
fi

echo "Library configuration complete for $ARCH!"
echo "Final configuration:"
otool -L "$LIBMSODBCSQL_PATH"
otool -L "$LIBODBCINST_PATH"
if [ -f "$LIBLTDL_PATH" ]; then
  otool -L "$LIBLTDL_PATH"
fi
done
