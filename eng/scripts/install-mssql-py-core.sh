#!/usr/bin/env bash
# Installs mssql-python-rs from the pinned internal NuGet transport package.
#
# This script is used identically for:
#   - Local development (dev runs it after build.sh)
#   - PR validation pipelines
#   - Official build pipelines and tests
#
# The Python distribution and NuGet transport versions are pinned separately.
#
# Usage:
#   ./install-mssql-py-core.sh [--feed-url URL]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
PYTHON="${PYTHON:-$(command -v python || command -v python3)}"

read_version() {
    local distribution_file="$REPO_ROOT/eng/versions/mssql-python-rs.version"
    local transport_file="$REPO_ROOT/eng/versions/mssql-python-rs-nuget.version"
    for version_file in "$distribution_file" "$transport_file"; do
        if [ ! -f "$version_file" ]; then
            echo "ERROR: Version file not found: $version_file"
            exit 1
        fi
    done
    DISTRIBUTION_VERSION=$(tr -d '[:space:]' < "$distribution_file")
    TRANSPORT_VERSION=$(tr -d '[:space:]' < "$transport_file")
    if [ -z "$DISTRIBUTION_VERSION" ] || [ -z "$TRANSPORT_VERSION" ]; then
        echo "ERROR: mssql-python-rs version files must not be empty"
        exit 1
    fi
    echo "Distribution version: $DISTRIBUTION_VERSION"
    echo "NuGet transport version: $TRANSPORT_VERSION"
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
                local glibc_version major minor
                glibc_version=$(printf '%s\n' "$ldd_output" | head -1 | grep -Eo '[0-9]+\.[0-9]+' | tail -1)
                major=${glibc_version%%.*}
                minor=${glibc_version#*.}
                if [ -z "$glibc_version" ] || ! [ "$major" -eq "$major" ] 2>/dev/null || ! [ "$minor" -eq "$minor" ] 2>/dev/null; then
                    echo "ERROR: Could not determine glibc version from: $ldd_output"
                    exit 1
                fi
                if [ "$major" -gt 2 ] || { [ "$major" -eq 2 ] && [ "$minor" -ge 34 ]; }; then
                    # glibc 2.34+ systems carry OpenSSL 3.
                    WHEEL_PLATFORM="manylinux_2_34_${ARCH_TAG}"
                elif [ "$major" -eq 2 ] && [ "$minor" -ge 28 ]; then
                    # glibc 2.28-2.33 systems carry OpenSSL 1.1.
                    WHEEL_PLATFORM="manylinux_2_28_${ARCH_TAG}"
                else
                    echo "ERROR: mssql-python-rs requires glibc 2.28 or newer; found $glibc_version"
                    exit 1
                fi
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

    WHEEL_PATTERN="mssql_python_rs-${DISTRIBUTION_VERSION}-cp310-abi3-${WHEEL_PLATFORM}.whl"
    echo "Wheel pattern: $WHEEL_PATTERN"
}

download_nupkg() {
    local feed_url="$1"
    local output_dir="$2"

    if ! output_dir=$("$PYTHON" "$SCRIPT_DIR/mssql_python_build_safety.py" "$output_dir"); then
        exit 1
    fi
    rm -rf "$output_dir"
    mkdir -p "$output_dir"
    RESOLVED_OUTPUT_DIR="$output_dir"

    echo "Resolving feed: $feed_url"
    PACKAGE_BASE_URL=$("$PYTHON" "$SCRIPT_DIR/resolve_nuget_feed.py" "$feed_url")
    if [ -z "$PACKAGE_BASE_URL" ]; then
        echo "ERROR: Could not resolve PackageBaseAddress from feed"
        exit 1
    fi
    PACKAGE_BASE_URL="${PACKAGE_BASE_URL%/}/"

    local version_lower
    version_lower=$(echo "$TRANSPORT_VERSION" | tr '[:upper:]' '[:lower:]')
    local package_id="mssql-python-rs-wheels"
    NUPKG_URL="${PACKAGE_BASE_URL}${package_id}/${version_lower}/${package_id}.${version_lower}.nupkg"
    NUPKG_PATH="$output_dir/${package_id}.${version_lower}.nupkg"
    local http_status
    echo "Downloading: $NUPKG_URL"
    if ! http_status=$(curl -sSL -o "$NUPKG_PATH" -w '%{http_code}' "$NUPKG_URL"); then
        rm -f "$NUPKG_PATH"
        echo "ERROR: Failed to download NuGet package: $package_id $TRANSPORT_VERSION" >&2
        exit 1
    fi
    if [ "$http_status" != "200" ]; then
        rm -f "$NUPKG_PATH"
        echo "ERROR: Failed to download NuGet package: $package_id $TRANSPORT_VERSION (HTTP $http_status)" >&2
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

    MATCHING_WHEEL=$(find "$wheels_dir" -name "$WHEEL_PATTERN" -print -quit)
    if [ -z "$MATCHING_WHEEL" ]; then
        echo "Available wheels:"
        ls "$wheels_dir"/*.whl 2>/dev/null || echo "  (none)"
        echo "ERROR: No wheel found matching: $WHEEL_PATTERN"
        exit 1
    fi

    echo "Found: $(basename "$MATCHING_WHEEL")"
}

install_and_verify() {
    local core_dir="$REPO_ROOT/mssql_py_core"

    if [ -d "$core_dir" ]; then
        rm -rf "$core_dir"
        echo "Cleaned previous mssql_py_core/"
    fi

    "$PYTHON" -m pip install --force-reinstall --no-deps "$MATCHING_WHEEL"
    "$PYTHON" -c "import importlib.metadata as m, mssql_py_core; assert m.version('mssql-python-rs') == '$DISTRIBUTION_VERSION'; print('mssql-python-rs', m.version('mssql-python-rs'), 'loaded from', mssql_py_core.__file__)"
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

echo "=== Install mssql-python-rs from NuGet transport ==="

RESOLVED_OUTPUT_DIR=""
trap 'if [ -n "$RESOLVED_OUTPUT_DIR" ]; then rm -rf "$RESOLVED_OUTPUT_DIR"; fi' EXIT

read_version
detect_platform
download_nupkg "$FEED_URL" "$OUTPUT_DIR"
find_matching_wheel "$RESOLVED_OUTPUT_DIR"
install_and_verify
echo "=== mssql-python-rs installed successfully ==="
