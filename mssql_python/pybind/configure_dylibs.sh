#!/bin/bash
# Script to configure dylib paths for macOS
# This script fixes the library paths in the ODBC driver dylibs and codesigns them

# Function to get the current architecture
get_mac_platform_architecture() {
  arch=$(uname -m)
  echo "$arch"
}

# Directory structure setup
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Get platform and configure paths
ARCH=$(get_mac_platform_architecture)
LIB_DIR="$PROJECT_DIR/libs/macos/lib"
LIBMSODBCSQL_PATH="$LIB_DIR/libmsodbcsql.18.dylib"
LIBODBCINST_PATH="$LIB_DIR/libodbcinst.2.dylib"
LIBLTDL_PATH="$LIB_DIR/libltdl.7.dylib"

echo "Configuring dylibs in: $LIB_DIR"

# Check if the directories and files exist
if [ ! -d "$LIB_DIR" ]; then
  echo "Error: Library directory doesn't exist: $LIB_DIR"
  exit 1
fi

if [ ! -f "$LIBMSODBCSQL_PATH" ]; then
  echo "Error: libmsodbcsql.18.dylib not found at: $LIBMSODBCSQL_PATH"
  exit 1
fi

if [ ! -f "$LIBODBCINST_PATH" ]; then
  echo "Error: libodbcinst.2.dylib not found at: $LIBODBCINST_PATH"
  exit 1
fi

# Get the existing library paths which are linked to the dylibs
echo "Reading dependencies from libmsodbcsql.18.dylib..."
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
  echo "Fixing libmsodbcsql.18.dylib dependency on libodbcinst.2.dylib..."
  echo "  Changing: $OLD_LIBODBCINST_PATH"
  echo "  To: $LIBODBCINST_PATH"
  install_name_tool -change "$OLD_LIBODBCINST_PATH" "$LIBODBCINST_PATH" "$LIBMSODBCSQL_PATH"
else
  echo "Warning: libodbcinst dependency not found in libmsodbcsql.18.dylib"
fi

if [ -n "$OLD_LIBLTDL_PATH" ] && [ -f "$LIBLTDL_PATH" ]; then
  echo "Fixing libodbcinst.2.dylib dependency on libltdl.7.dylib..."
  echo "  Changing: $OLD_LIBLTDL_PATH"
  echo "  To: $LIBLTDL_PATH"
  install_name_tool -change "$OLD_LIBLTDL_PATH" "$LIBLTDL_PATH" "$LIBODBCINST_PATH"
else
  echo "Note: libltdl dependency not found or not needed"
fi

# Force codesign the dylibs
echo "Codesigning libmsodbcsql.18.dylib..."
codesign -s - -f "$LIBMSODBCSQL_PATH" 2>/dev/null

echo "Codesigning libodbcinst.2.dylib..."
codesign -s - -f "$LIBODBCINST_PATH" 2>/dev/null

if [ -f "$LIBLTDL_PATH" ]; then
  echo "Codesigning libltdl.7.dylib..."
  codesign -s - -f "$LIBLTDL_PATH" 2>/dev/null
fi

echo "Library configuration complete!"
