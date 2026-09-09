#!/usr/bin/env bash
# Downloads the mssql-python-rs-wheels NuGet package from a public Azure
# Artifacts feed and installs the matching mssql-python-rs wheel with pip.
#
# This script is used identically for:
#   - Local development (dev runs it after build.sh)
#   - PR validation pipelines
#   - Official build pipelines and tests
#
# The package version is read from eng/versions/mssql-python-rs.version (required).
#
# Usage:
#   ./install-mssql-py-core.sh [--feed-url URL]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
PYTHON="${PYTHON:-$(command -v python || command -v python3)}"

read_version() {
    local version_file="$REPO_ROOT/eng/versions/mssql-python-rs.version"
    if [ ! -f "$version_file" ]; then
        echo "ERROR: Version file not found: $version_file"
        exit 1
    fi
    PACKAGE_VERSION=$(tr -d '[:space:]' < "$version_file")
    if [ -z "$PACKAGE_VERSION" ]; then
        echo "ERROR: Version file is empty: $version_file"
        exit 1
    fi
    echo "Version: $PACKAGE_VERSION"
}

detect_platform() {
    read -r PY_VERSION PLATFORM ARCH <<< "$("$PYTHON" -c "
import sys, platform
v = sys.version_info
print(f'cp{v.major}{v.minor} {platform.system().lower()} {platform.machine().lower()}')"
    )"

    echo "Python: $PY_VERSION | Platform: $PLATFORM | Arch: $ARCH"

    case "$PLATFORM" in
        linux)
            case "$ARCH" in
                x86_64|amd64) ARCH_TAG="x86_64" ;;
                aarch64|arm64) ARCH_TAG="aarch64" ;;
                *) echo "Unsupported Linux architecture: $ARCH"; exit 1 ;;
            esac

            # Detect musl libc (Alpine) vs glibc.
            # ldd --version exits 1 on musl, so capture output instead of piping.
            local ldd_output
            ldd_output=$(ldd --version 2>&1 || true)
            if echo "$ldd_output" | grep -qi musl || [ -f /etc/alpine-release ]; then
                WHEEL_PLATFORM="musllinux_1_2_${ARCH_TAG}"
            else
                # auditwheel=skip: wheels are tagged linux_* not manylinux_2_34_*
                WHEEL_PLATFORM="linux_${ARCH_TAG}"
            fi
            ;;
        darwin)
            WHEEL_PLATFORM="macosx_15_0_universal2"
            ;;
        *)
            echo "Unsupported platform: $PLATFORM"
            exit 1
            ;;
    esac

    WHEEL_PATTERN="mssql_python_rs-*-${PY_VERSION}-${PY_VERSION}-${WHEEL_PLATFORM}.whl"
    echo "Wheel pattern: $WHEEL_PATTERN"
}

download_nupkg() {
    local feed_url="$1"
    local output_dir="$2"

    rm -rf "$output_dir"
    mkdir -p "$output_dir"

    echo "Resolving feed: $feed_url"
    PACKAGE_BASE_URL=$("$PYTHON" "$SCRIPT_DIR/resolve_nuget_feed.py" "$feed_url")
    if [ -z "$PACKAGE_BASE_URL" ]; then
        echo "ERROR: Could not resolve PackageBaseAddress from feed"
        exit 1
    fi

    local version_lower
    version_lower=$(echo "$PACKAGE_VERSION" | tr '[:upper:]' '[:lower:]')
    local package_id="mssql-python-rs-wheels"
    NUPKG_URL="${PACKAGE_BASE_URL}${package_id}/${version_lower}/${package_id}.${version_lower}.nupkg"
    NUPKG_PATH="$output_dir/${package_id}.${version_lower}.nupkg"
    local http_status
    echo "Downloading: $NUPKG_URL"
    if ! http_status=$(curl -sSL -o "$NUPKG_PATH" -w '%{http_code}' "$NUPKG_URL"); then
        rm -f "$NUPKG_PATH"
        echo "ERROR: Failed to download NuGet package: $package_id $PACKAGE_VERSION" >&2
        exit 1
    fi
    if [ "$http_status" != "200" ]; then
        rm -f "$NUPKG_PATH"
        echo "ERROR: Failed to download NuGet package: $package_id $PACKAGE_VERSION (HTTP $http_status)" >&2
        exit 1
    fi

    local filesize
    filesize=$(wc -c < "$NUPKG_PATH")
    echo "Downloaded: $NUPKG_PATH ($filesize bytes)"

    if [ "$filesize" -eq 0 ]; then
        echo "ERROR: Downloaded file is empty"
        exit 1
    fi
}

find_matching_wheel() {
    local output_dir="$1"
    local extract_dir="$output_dir/extracted"

    mkdir -p "$extract_dir"
    if command -v unzip &>/dev/null; then
        unzip -q "$NUPKG_PATH" -d "$extract_dir"
    else
        "$PYTHON" -c "import zipfile, sys; zipfile.ZipFile(sys.argv[1]).extractall(sys.argv[2])" "$NUPKG_PATH" "$extract_dir"
    fi

    local wheels_dir="$extract_dir/wheels"
    if [ ! -d "$wheels_dir" ]; then
        echo "ERROR: No 'wheels' directory found in NuGet package"
        ls -la "$extract_dir"
        exit 1
    fi

    MATCHING_WHEEL=""
    MATCHING_WHEEL=$(find "$wheels_dir" -name "$WHEEL_PATTERN" -print -quit)
    if [ -z "$MATCHING_WHEEL" ]; then
        echo "Available wheels:"
        ls "$wheels_dir"/*.whl 2>/dev/null || echo "  (none)"
        echo "ERROR: No wheel found matching: $WHEEL_PATTERN"
        exit 1
    fi

    echo "Found: $(basename "$MATCHING_WHEEL")"
}

# Returns 0 (true) if the runtime glibc is new enough to load the .so.
# The mssql_py_core native extension is built on manylinux_2_34 (glibc 2.34).
# Build containers running manylinux_2_34 have glibc 2.34 — sufficient to dlopen it.
# On musl (Alpine) or macOS we always attempt the import.
can_verify_import() {
    case "$PLATFORM" in
        linux)
            # musl doesn't use glibc versioning — always try
            if echo "$WHEEL_PLATFORM" | grep -q musl; then
                return 0
            fi
            local glibc_version
            glibc_version=$(ldd --version 2>&1 | head -1 | grep -oP '[0-9]+\.[0-9]+$' || echo "0.0")
            local major minor
            major=$(echo "$glibc_version" | cut -d. -f1)
            minor=$(echo "$glibc_version" | cut -d. -f2)
            # Require glibc >= 2.34
            if [ "$major" -gt 2 ] 2>/dev/null || { [ "$major" -eq 2 ] && [ "$minor" -ge 34 ]; } 2>/dev/null; then
                return 0
            fi
            return 1
            ;;
        *)
            return 0
            ;;
    esac
}

install_and_verify() {
    local core_dir="$REPO_ROOT/mssql_py_core"

    if [ -d "$core_dir" ]; then
        rm -rf "$core_dir"
        echo "Cleaned previous mssql_py_core/"
    fi

    "$PYTHON" -m pip install --force-reinstall --no-deps "$MATCHING_WHEEL"
    "$PYTHON" -c "import importlib.metadata as m; assert m.version('mssql-python-rs') == '$PACKAGE_VERSION'"

    # Skip import verification when glibc is older than what the .so requires
    # (e.g. manylinux_2_34 build containers with glibc 2.34, matching .so requirements).
    if can_verify_import; then
        echo "Verifying import..."
        "$PYTHON" -c "import mssql_py_core; print(f'mssql_py_core loaded from {mssql_py_core.__file__}')"
    else
        echo "Skipping import verification (glibc too old for runtime load)"
    fi
}

# --- main ---

FEED_URL="${FEED_URL:-https://pkgs.dev.azure.com/sqlclientdrivers/public/_packaging/mssql-rs_Public/nuget/v3/index.json}"
OUTPUT_DIR="${TMPDIR:-/tmp}/mssql-python-rs-wheels"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --feed-url) FEED_URL="$2"; shift 2 ;;
        *) echo "Unknown argument: $1"; exit 1 ;;
    esac
done

echo "=== Install mssql-python-rs from NuGet wheel package ==="

read_version
detect_platform
download_nupkg "$FEED_URL" "$OUTPUT_DIR"
find_matching_wheel "$OUTPUT_DIR"
install_and_verify

rm -rf "$OUTPUT_DIR"
echo "=== mssql-python-rs installed successfully ==="
