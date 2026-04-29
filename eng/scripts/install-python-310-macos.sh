#!/usr/bin/env bash
# Downloads the python-310-macos-installer NuGet package from an Azure Artifacts
# feed and extracts the Python 3.10.11 universal2 macOS .pkg installer.
#
# This avoids a direct download from python.org in the pipeline, addressing
# supply-chain concerns (URL availability, content integrity). The .pkg is
# hosted in our own NuGet feed and its Apple code signature is verified after
# extraction.
#
# The package version is read from eng/versions/python-310-macos.version.
#
# Usage:
#   ./install-python-310-macos.sh [--feed-url URL]
#
# Outputs:
#   Installs Python 3.10 universal2 to /Library/Frameworks/Python.framework/Versions/3.10
#   Prepends the bin directory to PATH via ##vso[task.prependpath]
#   Creates python/pip symlinks for build script compatibility

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Use system Python (pre-installed on macOS) for resolve_nuget_feed.py
# since we're installing Python 3.10 and it may not be available yet.
PYTHON="${PYTHON:-$(command -v python3 || command -v python)}"

FEED_URL="${FEED_URL:-https://pkgs.dev.azure.com/sqlclientdrivers/public/_packaging/mssql-rs_Public/nuget/v3/index.json}"
PACKAGE_ID="python-310-macos-installer"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --feed-url) FEED_URL="$2"; shift 2 ;;
        *) echo "Unknown argument: $1"; exit 1 ;;
    esac
done

# ---------------------------------------------------------------------------
# Read package version
# ---------------------------------------------------------------------------
VERSION_FILE="$REPO_ROOT/eng/versions/python-310-macos.version"
if [ ! -f "$VERSION_FILE" ]; then
    echo "ERROR: Version file not found: $VERSION_FILE"
    exit 1
fi
PACKAGE_VERSION=$(tr -d '[:space:]' < "$VERSION_FILE")
if [ -z "$PACKAGE_VERSION" ]; then
    echo "ERROR: Version file is empty: $VERSION_FILE"
    exit 1
fi
echo "Package version: $PACKAGE_VERSION"

# ---------------------------------------------------------------------------
# Download .nupkg from Azure Artifacts feed
# ---------------------------------------------------------------------------
echo "Resolving feed: $FEED_URL"
PACKAGE_BASE_URL=$("$PYTHON" "$SCRIPT_DIR/resolve_nuget_feed.py" "$FEED_URL")
if [ -z "$PACKAGE_BASE_URL" ]; then
    echo "ERROR: Could not resolve PackageBaseAddress from feed"
    exit 1
fi

VERSION_LOWER=$(echo "$PACKAGE_VERSION" | tr '[:upper:]' '[:lower:]')
NUPKG_URL="${PACKAGE_BASE_URL}${PACKAGE_ID}/${VERSION_LOWER}/${PACKAGE_ID}.${VERSION_LOWER}.nupkg"
WORK_DIR="${TMPDIR:-/tmp}/python-310-installer"
NUPKG_PATH="$WORK_DIR/${PACKAGE_ID}.${VERSION_LOWER}.nupkg"

rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR"

echo "Downloading: $NUPKG_URL"
curl -sSL -o "$NUPKG_PATH" "$NUPKG_URL"

FILESIZE=$(wc -c < "$NUPKG_PATH")
echo "Downloaded: $NUPKG_PATH ($FILESIZE bytes)"
if [ "$FILESIZE" -eq 0 ]; then
    echo "ERROR: Downloaded file is empty"
    exit 1
fi

# ---------------------------------------------------------------------------
# Extract .pkg from .nupkg (which is a ZIP)
# ---------------------------------------------------------------------------
EXTRACT_DIR="$WORK_DIR/extracted"
mkdir -p "$EXTRACT_DIR"
if command -v unzip &>/dev/null; then
    unzip -q "$NUPKG_PATH" -d "$EXTRACT_DIR"
else
    "$PYTHON" -c "import zipfile, sys; zipfile.ZipFile(sys.argv[1]).extractall(sys.argv[2])" "$NUPKG_PATH" "$EXTRACT_DIR"
fi

PKG_PATH=$(find "$EXTRACT_DIR" -name "*.pkg" | head -1)
if [ -z "$PKG_PATH" ]; then
    echo "ERROR: No .pkg file found in NuGet package"
    ls -laR "$EXTRACT_DIR"
    exit 1
fi
echo "Found installer: $(basename "$PKG_PATH")"

# ---------------------------------------------------------------------------
# Verify SHA-256 hash (integrity check)
# ---------------------------------------------------------------------------
# SHA-256 of the official python-3.10.11-macos11.pkg from python.org
EXPECTED_SHA256="767ed35ad688d28ea4494081ae96408a0318d0d5bb9ca0139d74d6247b231cfc"
ACTUAL_SHA256="$(shasum -a 256 "${PKG_PATH}" | awk '{print $1}')"
echo "Expected SHA-256: ${EXPECTED_SHA256}"
echo "Actual   SHA-256: ${ACTUAL_SHA256}"
if [ "${ACTUAL_SHA256}" != "${EXPECTED_SHA256}" ]; then
    echo "ERROR: SHA-256 mismatch — installer may be corrupted or tampered with"
    exit 1
fi
echo "SHA-256 verified."

# ---------------------------------------------------------------------------
# Verify Apple code signature (supply-chain safety)
# ---------------------------------------------------------------------------
# The python.org macOS installers are signed by Ned Deily (DJ3H93M7VJ),
# who is the CPython macOS release manager, using a Developer ID Installer
# certificate issued by Apple.
echo "Verifying installer package signature..."
PKG_SIGNATURE="$(pkgutil --check-signature "${PKG_PATH}")"
echo "${PKG_SIGNATURE}"
echo "${PKG_SIGNATURE}" | grep -F "signed by a developer certificate issued by Apple" >/dev/null || {
    echo "ERROR: Package is not signed by an Apple-issued developer certificate"
    exit 1
}
echo "${PKG_SIGNATURE}" | grep -F "Ned Deily" >/dev/null || {
    echo "ERROR: Package signer did not match expected publisher (Ned Deily - CPython macOS release manager)"
    exit 1
}

# ---------------------------------------------------------------------------
# Install Python 3.10 universal2
# ---------------------------------------------------------------------------
echo "Installing Python 3.10 universal2..."
sudo installer -pkg "${PKG_PATH}" -target /

PY310_BIN="/Library/Frameworks/Python.framework/Versions/3.10/bin"
echo "##vso[task.prependpath]${PY310_BIN}"

# Create 'python' symlink so build scripts find the intended interpreter
# /Library/Frameworks requires sudo for symlink creation
sudo ln -sf "${PY310_BIN}/python3" "${PY310_BIN}/python"

# Ensure pip is available for dependency installation
if [ ! -f "${PY310_BIN}/pip3" ]; then
    "${PY310_BIN}/python3" -m ensurepip --upgrade
fi
if [ ! -f "${PY310_BIN}/pip" ]; then
    sudo ln -sf "${PY310_BIN}/pip3" "${PY310_BIN}/pip"
fi

# Fix SSL certificates — python.org macOS installers don't include root CA
# certificates. Without this, urllib/requests fail with SSL: CERTIFICATE_VERIFY_FAILED.
# The "Install Certificates.command" ships with the installer and installs
# certifi's CA bundle into Python's OpenSSL directory.
echo "Installing SSL root certificates..."
"${PY310_BIN}/python3" -m pip install --upgrade certifi
CERT_SCRIPT="/Applications/Python 3.10/Install Certificates.command"
if [ -f "$CERT_SCRIPT" ]; then
    bash "$CERT_SCRIPT"
else
    # Fallback: manually link certifi's CA bundle
    CERT_PATH=$("${PY310_BIN}/python3" -c "import certifi; print(certifi.where())")
    SSL_DIR="/Library/Frameworks/Python.framework/Versions/3.10/etc/openssl"
    sudo mkdir -p "$SSL_DIR"
    sudo ln -sf "$CERT_PATH" "$SSL_DIR/cert.pem"
    echo "Linked certifi CA bundle to $SSL_DIR/cert.pem"
fi

echo "Python version:"
"${PY310_BIN}/python3" --version
echo "pip version:"
"${PY310_BIN}/python3" -m pip --version
echo "Interpreter architectures:"
file "${PY310_BIN}/python3"

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------
rm -rf "$WORK_DIR"
echo "=== Python 3.10 universal2 installed successfully ==="
