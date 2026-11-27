#!/usr/bin/env bash
# Install the release pin, or an explicit MSSQL_PYTHON_RS_VERSION feature-test override.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON="${PYTHON:-$(command -v python || command -v python3)}"
exec "$PYTHON" "$SCRIPT_DIR/install_mssql_python_rs.py" "$@"
