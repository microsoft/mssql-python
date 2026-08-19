#!/bin/bash
# Repackage the prebuilt, ESRP-signed wheel into a conda package (offline).
# PKG_NAME / PKG_VERSION are exported by conda-build; WHEELS_DIR by the pipeline/harness.
set -euo pipefail
# Cross-arch (emulated) build: when repackaging the aarch64 wheel on an x86_64 host,
# $PYTHON is the target-arch interpreter and runs under qemu-user. Point qemu at the
# aarch64 glibc loader/libs (installed via libc6-arm64-cross) so it can find
# /lib/ld-linux-aarch64.so.1 instead of aborting with "Could not open". The dir only
# exists on the emulated aarch64 leg; setting the var elsewhere is a harmless no-op.
[ -d /usr/aarch64-linux-gnu ] && export QEMU_LD_PREFIX="${QEMU_LD_PREFIX:-/usr/aarch64-linux-gnu}"

# This package is SELF-CONTAINED (v1.11.0 model): the ODBC Driver 18 payload ships
# INSIDE it, so there is NO separate mssql-python-odbc conda package. We land BOTH
# the code wheel AND the python-agnostic py3-none-<plat> odbc wheel in the SAME
# site-packages, so mssql_python_odbc/libs/ sits beside mssql_python/ and the C++
# loader resolves the driver there. WHEELS_DIR is staged per-target by the pipeline,
# so exactly one matching odbc wheel is present.
odbc_ver="${MSSQL_ODBC_VERSION:?MSSQL_ODBC_VERSION not set}"

# The normal path installs with the host-env Python -- native builds, and the
# QEMU-emulated linux-aarch64 leg where the aarch64 Python runs under binfmt. pip
# resolves the correct site-packages for BOTH wheels, so no unzip is needed there.
# The osx-arm64 conda package is CROSS-built on an Intel macOS agent (no reverse
# Rosetta): the arm64 host Python CANNOT execute and pip would abort, so extract
# both wheels (zips) WITHOUT Python -- the same approach the Windows bld.bat uses
# with `tar`. macOS ships `unzip`. The arm64 slice comes from the universal2 wheel;
# conda-build still stamps osx-arm64.
if "$PYTHON" -c "import sys" >/dev/null 2>&1; then
  "$PYTHON" -m pip install --no-deps --no-index --find-links "$WHEELS_DIR" "$PKG_NAME==$PKG_VERSION" -vv
  "$PYTHON" -m pip install --no-deps --no-index --find-links "$WHEELS_DIR" "mssql-python-odbc==$odbc_ver" -vv
else
  echo "Host Python '$PYTHON' is not executable on this agent (non-emulated cross-build);"
  echo "extracting both wheels into \$SP_DIR without running Python."
  mkdir -p "$SP_DIR"
  pkg_underscore="${PKG_NAME//-/_}"
  code_whl=""
  for w in "$WHEELS_DIR/${pkg_underscore}-${PKG_VERSION}-"*.whl; do
    [ -e "$w" ] && { code_whl="$w"; break; }
  done
  [ -n "$code_whl" ] || { echo "ERROR: no ${PKG_NAME}==${PKG_VERSION} wheel in '$WHEELS_DIR'" >&2; exit 1; }
  odbc_whl=""
  for w in "$WHEELS_DIR"/mssql_python_odbc-"$odbc_ver"-py3-none-*.whl; do
    [ -e "$w" ] && { odbc_whl="$w"; break; }
  done
  [ -n "$odbc_whl" ] || { echo "ERROR: no mssql_python_odbc==$odbc_ver py3-none wheel in '$WHEELS_DIR'" >&2; exit 1; }
  echo "Extracting '$code_whl' -> '$SP_DIR'"
  unzip -oq "$code_whl" -d "$SP_DIR"
  echo "Extracting '$odbc_whl' -> '$SP_DIR'"
  unzip -oq "$odbc_whl" -d "$SP_DIR"
fi
