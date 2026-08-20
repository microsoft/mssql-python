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

# ---------------------------------------------------------------------------
# Linux driver reachability (#563) -- the core fix.
# ---------------------------------------------------------------------------
# Declaring krb5/openssl/libltdl as conda deps drops one consistent copy of each
# into $PREFIX/lib, but that is INERT on its own: the vendored ODBC binaries ship
# with a bare DT_RUNPATH=$ORIGIN (no climb), so on a minimal conda base the loader
# never looks in $PREFIX/lib -- it falls through to SYSTEM krb5 (the #563 mixing
# crash) and cannot find libltdl.so.7 at all. Reachability, not declaration, is the
# lever: stamp a PURELY RELATIVE $ORIGIN climb (the ELF twin of the macOS
# @loader_path flow) onto libmsodbcsql* and libodbcinst.so.2 so they resolve THIS
# env's own $PREFIX/lib, location-independently.
#
# SIGNATURE SAFETY:
#   The Linux ODBC .so are NOT ESRP code-signed -- the mssql-python-odbc pipeline only
#   MALWARE-SCANS them (there is no CodeSign task). Only Windows .dll (Authenticode)
#   and macOS .dylib (codesign) are code-signed, and this recipe never patches those
#   (the climb is Linux-only). So stamping the relative $ORIGIN climb here breaks no
#   signature. If the binaries already carry the EXACT climb (e.g. a future odbc-side
#   pre-bake before signing), the patch is a byte-for-byte no-op.
#
# The canonical RUNPATH is exactly "$ORIGIN:$ORIGIN/<climb>" -- the patch below emits
# that literal form, and the static audit (eng/scripts/audit_bundled_binaries.py)
# requires the same exact climb entry.
#
# Linux-only by construction: the glob matches nothing in a macOS payload
# (libs/macos/...), so this whole block is a natural no-op on the osx legs.
prefix_lib="$PREFIX/lib"
shopt -s nullglob
have_linux_payload=0
[ -d "$SP_DIR/mssql_python_odbc/libs/linux" ] && have_linux_payload=1
drivers_seen=0
for libdir in "$SP_DIR"/mssql_python_odbc/libs/linux/*/*/lib; do
  # Compute the EXACT expected climb from THIS driver dir up to $PREFIX/lib (derived
  # from the real install layout, never a hard-coded ../ count).
  climb="$("$PYTHON" -c 'import os,sys; print(os.path.relpath(sys.argv[1], sys.argv[2]))' "$prefix_lib" "$libdir")"
  want="\$ORIGIN:\$ORIGIN/$climb"
  for so in "$libdir"/libmsodbcsql-*.so.* "$libdir"/libodbcinst.so.2; do
    [ -e "$so" ] || continue
    drivers_seen=$((drivers_seen + 1))
    got="$(patchelf --print-rpath "$so" 2>/dev/null || true)"
    if [ "$got" = "$want" ]; then
      echo "RPATH-OK (already baked) $(basename "$so") -> $got"
      continue
    fi
    # Stamp the exact $ORIGIN climb (safe -- these Linux .so are not code-signed).
    patchelf --set-rpath "$want" "$so"
    got="$(patchelf --print-rpath "$so")"
    # H2: compare EXACTLY to the intended value, not just "no absolute entry".
    if [ "$got" != "$want" ]; then
      echo "ERROR: patch did not yield the exact expected RUNPATH ('$got' != '$want')." >&2
      exit 1
    fi
    echo "RPATH-PATCHED $(basename "$so") -> $got"
  done
done
shopt -u nullglob
# H2: a Linux payload with NO driver found is a bypass hole -- a bare `conda build`
# skipping the orchestrator audit would then ship un-asserted drivers. Fail loudly.
if [ "$have_linux_payload" = "1" ] && [ "$drivers_seen" = "0" ]; then
  echo "ERROR: Linux ODBC payload present but no libmsodbcsql*/libodbcinst.so.2 found to assert the #563 climb." >&2
  exit 1
fi
[ "$drivers_seen" -gt 0 ] && echo "LINUX_RPATH_CLIMB_OK" || true
