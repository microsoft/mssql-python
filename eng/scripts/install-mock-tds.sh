#!/usr/bin/env bash
# Installs the mssql-mock-tds in-process TDS server (plus cryptography, used to
# mint the throwaway TLS identity it needs) from the public mssql-rs_Public
# Azure Artifacts PyPI feed so that tests/test_024_mock_tds_fedauth.py runs in CI
# instead of skipping.
#
# The package is currently published only as a dev pre-release on a sandbox feed
# and ships wheels for a limited platform matrix (linux glibc x86_64/aarch64,
# macOS universal2, Windows amd64/arm64 -- but NOT musllinux/Alpine). To keep
# pipeline legs green where no compatible wheel exists, a failed install is
# reported as a pipeline *warning* (not an error): the test then skips cleanly.
#
# The package version is read from eng/versions/mssql-mock-tds.version (required).
#
# Usage:
#   ./install-mock-tds.sh [--feed-url URL]

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
PYTHON="${PYTHON:-$(command -v python || command -v python3)}"

# Public PyPI-format index for the mssql-rs_Public feed (no auth required).
FEED_URL="${FEED_URL:-https://pkgs.dev.azure.com/sqlclientdrivers/public/_packaging/mssql-rs_Public/pypi/simple/}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --feed-url) FEED_URL="$2"; shift 2 ;;
        *) echo "Unknown argument: $1"; exit 1 ;;
    esac
done

warn() {
    # Surface as an Azure DevOps pipeline warning while keeping the leg green.
    echo "##vso[task.logissue type=warning]$1"
    echo "WARNING: $1"
}

version_file="$REPO_ROOT/eng/versions/mssql-mock-tds.version"
if [ ! -f "$version_file" ]; then
    warn "Version file not found: $version_file -- skipping mssql-mock-tds install."
    exit 0
fi
PACKAGE_VERSION=$(tr -d '[:space:]' < "$version_file")
if [ -z "$PACKAGE_VERSION" ]; then
    warn "Version file is empty: $version_file -- skipping mssql-mock-tds install."
    exit 0
fi

echo "=== Install mssql-mock-tds ($PACKAGE_VERSION) from $FEED_URL ==="

if "$PYTHON" -m pip install \
        --extra-index-url "$FEED_URL" \
        "mssql-mock-tds==$PACKAGE_VERSION" \
        cryptography; then
    echo "Verifying import..."
    if "$PYTHON" -c "import mssql_mock_tds; print('mssql_mock_tds import OK')"; then
        echo "=== mssql-mock-tds installed successfully ==="
        exit 0
    fi
    warn "mssql-mock-tds installed but failed to import -- the mock TDS test will skip."
    exit 0
fi

warn "Could not install mssql-mock-tds==$PACKAGE_VERSION (no compatible wheel on this platform, e.g. Alpine/musl, or feed unavailable) -- the mock TDS test will skip."
exit 0
