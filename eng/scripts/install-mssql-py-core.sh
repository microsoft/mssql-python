#!/usr/bin/env bash
# Downloads the mssql-py-core-wheels NuGet package from a public Azure Artifacts
# feed and installs the appropriate wheel for the current platform so that
# 'import mssql_py_core' works. No authentication required (public feed).
#
# The package version is read from eng/versions/mssql-py-core.version (required).
#
# Usage:
#   ./install-mssql-py-core.sh [--feed-url URL]

set -euo pipefail

FEED_URL="${FEED_URL:-https://pkgs.dev.azure.com/sqlclientdrivers/public/_packaging/mssql-rs_Public/nuget/v3/index.json}"
OUTPUT_DIR="${TMPDIR:-/tmp}/mssql-py-core-wheels"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --feed-url) FEED_URL="$2"; shift 2 ;;
        *) echo "Unknown argument: $1"; exit 1 ;;
    esac
done

echo "=== Install mssql_py_core from NuGet wheel package ==="

# Read version from pinned version file (required)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VERSION_FILE="$SCRIPT_DIR/../../eng/versions/mssql-py-core.version"
if [ ! -f "$VERSION_FILE" ]; then
    echo "ERROR: Version file not found: $VERSION_FILE"
    echo "This file must exist and contain the mssql-py-core-wheels NuGet package version."
    exit 1
fi
PACKAGE_VERSION=$(tr -d '[:space:]' < "$VERSION_FILE")
if [ -z "$PACKAGE_VERSION" ]; then
    echo "ERROR: Version file is empty: $VERSION_FILE"
    exit 1
fi
echo "Using version from $VERSION_FILE: $PACKAGE_VERSION"

# Determine platform info
PY_VERSION=$(python3 -c "import sys; print(f'cp{sys.version_info.major}{sys.version_info.minor}')")
PLATFORM=$(python3 -c "import platform; print(platform.system().lower())")
ARCH=$(python3 -c "import platform; print(platform.machine().lower())")

echo "Python: $PY_VERSION | Platform: $PLATFORM | Arch: $ARCH"

# Map to wheel platform tags
case "$PLATFORM" in
    linux)
        case "$ARCH" in
            x86_64|amd64) WHEEL_PLATFORM="manylinux_2_28_x86_64" ;;
            aarch64|arm64) WHEEL_PLATFORM="manylinux_2_28_aarch64" ;;
            *) echo "Unsupported Linux architecture: $ARCH"; exit 1 ;;
        esac
        ;;
    darwin)
        WHEEL_PLATFORM="macosx_15_0_universal2"
        ;;
    *)
        echo "Unsupported platform: $PLATFORM"
        exit 1
        ;;
esac

WHEEL_PATTERN="mssql_py_core-*-${PY_VERSION}-${PY_VERSION}-${WHEEL_PLATFORM}.whl"
echo "Looking for wheel matching: $WHEEL_PATTERN"

# Setup temp directory
rm -rf "$OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR"

# Resolve NuGet v3 feed
echo "Resolving feed: $FEED_URL"
FEED_INDEX=$(curl -sS "$FEED_URL")

PACKAGE_BASE_URL=$(echo "$FEED_INDEX" | python3 -c "
import json, sys
data = json.load(sys.stdin)
for r in data['resources']:
    if 'PackageBaseAddress' in r.get('@type', ''):
        print(r['@id'])
        break
")

if [ -z "$PACKAGE_BASE_URL" ]; then
    echo "Could not resolve PackageBaseAddress from feed"
    exit 1
fi
echo "Package base: $PACKAGE_BASE_URL"

PACKAGE_ID="mssql-py-core-wheels"

VERSION_LOWER=$(echo "$PACKAGE_VERSION" | tr '[:upper:]' '[:lower:]')
NUPKG_URL="${PACKAGE_BASE_URL}${PACKAGE_ID}/${VERSION_LOWER}/${PACKAGE_ID}.${VERSION_LOWER}.nupkg"
NUPKG_PATH="$OUTPUT_DIR/${PACKAGE_ID}.${VERSION_LOWER}.nupkg"

echo "Downloading: $NUPKG_URL"
curl -sSL -o "$NUPKG_PATH" "$NUPKG_URL"
FILESIZE=$(stat -c%s "$NUPKG_PATH" 2>/dev/null || stat -f%z "$NUPKG_PATH" 2>/dev/null || echo "unknown")
echo "Downloaded: $NUPKG_PATH ($FILESIZE bytes)"

if [ "$FILESIZE" = "0" ] || [ "$FILESIZE" = "unknown" ]; then
    echo "ERROR: Downloaded file is empty or could not determine size"
    exit 1
fi

# Extract NuGet (ZIP format) — use python if unzip is not available
EXTRACT_DIR="$OUTPUT_DIR/extracted"
mkdir -p "$EXTRACT_DIR"
if command -v unzip &>/dev/null; then
    unzip -q "$NUPKG_PATH" -d "$EXTRACT_DIR"
else
    python3 -c "import zipfile; zipfile.ZipFile('$NUPKG_PATH').extractall('$EXTRACT_DIR')"
fi

# Find matching wheel
WHEELS_DIR="$EXTRACT_DIR/wheels"
if [ ! -d "$WHEELS_DIR" ]; then
    echo "No 'wheels' directory found in NuGet package"
    ls -la "$EXTRACT_DIR"
    exit 1
fi

MATCHING_WHEEL=$(find "$WHEELS_DIR" -name "$WHEEL_PATTERN" | head -1)
if [ -z "$MATCHING_WHEEL" ]; then
    echo "Available wheels:"
    ls "$WHEELS_DIR"/*.whl 2>/dev/null || echo "  (none)"
    echo "No wheel found matching pattern: $WHEEL_PATTERN"
    exit 1
fi

echo "Found matching wheel: $(basename "$MATCHING_WHEEL")"

# Install with pip
echo "Installing wheel with pip..."
python3 -m pip install "$MATCHING_WHEEL" --force-reinstall --no-deps

# Verify import
echo "Verifying mssql_py_core import..."
python3 -c "import mssql_py_core; print(f'mssql_py_core loaded successfully: {dir(mssql_py_core)}')"

# Cleanup
rm -rf "$OUTPUT_DIR"

echo "=== mssql_py_core installed successfully ==="
