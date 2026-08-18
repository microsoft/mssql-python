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

# The normal path installs the wheel with the host-env Python -- native builds, and
# the QEMU-emulated linux-aarch64 leg where the aarch64 Python runs under binfmt.
# The osx-arm64 conda package is CROSS-built on an Intel macOS agent (there is no
# reverse Rosetta), so the arm64 host Python CANNOT execute here and pip would abort.
# Detect that and extract the wheel WITHOUT running Python -- the same "unpack the
# wheel zip into site-packages" approach the Windows bld.bat uses with `tar`. The
# arm64 slice comes from the universal2 wheel; conda-build still stamps osx-arm64.
if "$PYTHON" -c "import sys" >/dev/null 2>&1; then
  "$PYTHON" -m pip install --no-deps --no-index --find-links "$WHEELS_DIR" "$PKG_NAME==$PKG_VERSION" -vv
else
  echo "Host Python '$PYTHON' is not executable on this agent (non-emulated cross-build);"
  echo "extracting the wheel into \$SP_DIR without running Python."
  pkg_underscore="${PKG_NAME//-/_}"
  whl=""
  for w in "$WHEELS_DIR/${pkg_underscore}-${PKG_VERSION}-"*.whl; do
    [ -e "$w" ] && { whl="$w"; break; }
  done
  [ -n "$whl" ] || { echo "ERROR: no ${PKG_NAME}==${PKG_VERSION} wheel in '$WHEELS_DIR'" >&2; exit 1; }
  echo "Extracting '$whl' -> '$SP_DIR'"
  mkdir -p "$SP_DIR"
  unzip -oq "$whl" -d "$SP_DIR"
fi
