#!/bin/bash
# Repackage the prebuilt, signed mssql-python wheel into a conda package (offline) and
# vendor the ODBC Driver 18 payload inside it -- no separate mssql-python-odbc conda
# package. Both the code wheel and the py3-none-<plat> odbc wheel land in the SAME
# site-packages, so mssql_python_odbc/libs/ sits beside mssql_python/ and the C++
# loader finds the driver. conda-build exports PKG_NAME / PKG_VERSION / CONDA_PY;
# WHEELS_DIR (one wheel per target) + MSSQL_ODBC_VERSION come from the pipeline.
set -euo pipefail
# Emulated aarch64 leg: $PYTHON is the aarch64 interpreter under qemu-user; point it at
# the aarch64 glibc loader so it doesn't abort. Harmless no-op on every other leg.
[ -d /usr/aarch64-linux-gnu ] && export QEMU_LD_PREFIX="${QEMU_LD_PREFIX:-/usr/aarch64-linux-gnu}"

odbc_ver="${MSSQL_ODBC_VERSION:?MSSQL_ODBC_VERSION not set}"

# Native / QEMU-emulated legs: the host Python runs, so pip installs both wheels. Cross
# osx-arm64 (built on Intel): the arm64 Python can't execute, so extract both wheels
# (zips) with unzip instead -- same approach as the Windows bld.bat.
if "$PYTHON" -c "import sys" >/dev/null 2>&1; then
  "$PYTHON" -m pip install --no-deps --no-index --find-links "$WHEELS_DIR" "$PKG_NAME==$PKG_VERSION" -vv
  "$PYTHON" -m pip install --no-deps --no-index --find-links "$WHEELS_DIR" "mssql-python-odbc==$odbc_ver" -vv
else
  echo "Host Python '$PYTHON' is not executable on this agent (non-emulated cross-build);"
  echo "extracting both wheels into \$SP_DIR without running Python."
  mkdir -p "$SP_DIR"
  pkg_underscore="${PKG_NAME//-/_}"
  # universal2 wheels are cpXY-specific (compiled ddbc_bindings), so filter on the
  # target CONDA_PY to never grab another interpreter's wheel (mirrors bld.bat).
  code_whl=""
  for w in "$WHEELS_DIR/${pkg_underscore}-${PKG_VERSION}-cp${CONDA_PY}-"*.whl; do
    [ -e "$w" ] && { code_whl="$w"; break; }
  done
  [ -n "$code_whl" ] || { echo "ERROR: no ${PKG_NAME}==${PKG_VERSION} cp${CONDA_PY} wheel in '$WHEELS_DIR'" >&2; exit 1; }
  odbc_whl=""
  # This cross branch only runs on macOS; the odbc payload is a single universal2 wheel
  # (both arches in one), so match macosx explicitly rather than py3-none-* which would
  # also match a Linux odbc wheel if one were ever staged in the same dir.
  for w in "$WHEELS_DIR"/mssql_python_odbc-"$odbc_ver"-py3-none-macosx*.whl; do
    [ -e "$w" ] && { odbc_whl="$w"; break; }
  done
  [ -n "$odbc_whl" ] || { echo "ERROR: no mssql_python_odbc==$odbc_ver py3-none-macosx wheel in '$WHEELS_DIR'" >&2; exit 1; }
  echo "Extracting '$code_whl' -> '$SP_DIR'"
  unzip -oq "$code_whl" -d "$SP_DIR"
  # osx-arm64 cross can't run the arm64 Python, so statically prove the extracted
  # binding is for THIS interpreter -- a cpXY ddbc_bindings for another Python (the bug
  # where every osx-arm64 build shipped the cp310 .so) would only fail at the user's
  # import. The python-tag twin of the win-arm64 PE-arch assert.
  ls "$SP_DIR"/mssql_python/ddbc_bindings.cp${CONDA_PY}-*.so >/dev/null 2>&1 || {
    echo "ERROR: '$code_whl' has no mssql_python/ddbc_bindings.cp${CONDA_PY}-*.so (wrong-Python binding)." >&2
    exit 1
  }
  echo "Extracting '$odbc_whl' -> '$SP_DIR'"
  unzip -oq "$odbc_whl" -d "$SP_DIR"
fi

# ---------------------------------------------------------------------------
# Linux driver reachability (#563) -- the core fix.
# ---------------------------------------------------------------------------
# Declaring krb5/openssl/libltdl as conda deps drops them in $PREFIX/lib, but that is
# INERT: the vendored ODBC .so ship with a bare DT_RUNPATH=$ORIGIN (no climb), so the
# loader never looks in $PREFIX/lib and falls through to SYSTEM krb5 (#563 crash) or
# can't find libltdl.so.7. Fix: stamp a relative "$ORIGIN:$ORIGIN/<climb>" onto
# libmsodbcsql* + libodbcinst.so.2 so they resolve THIS env's $PREFIX/lib. Safe to
# patch -- the Linux .so are malware-scanned, not code-signed (only Windows .dll /
# macOS .dylib are, and those are never touched). Linux-only: the glob is a no-op on
# macOS. audit_bundled_binaries.py asserts the same exact climb.
prefix_lib="$PREFIX/lib"
shopt -s nullglob
have_linux_payload=0
[ -d "$SP_DIR/mssql_python_odbc/libs/linux" ] && have_linux_payload=1
# Count the driver (libmsodbcsql) and the driver manager (libodbcinst) separately so a
# payload missing EITHER fails loudly -- a lone libodbcinst would ship no SQL driver.
msodbc_seen=0
odbcinst_seen=0
for libdir in "$SP_DIR"/mssql_python_odbc/libs/linux/*/*/lib; do
  # Exact climb from this driver dir up to $PREFIX/lib (from the real layout, never a
  # hard-coded ../ count).
  climb="$("$PYTHON" -c 'import os,sys; print(os.path.relpath(sys.argv[1], sys.argv[2]))' "$prefix_lib" "$libdir")"
  want="\$ORIGIN:\$ORIGIN/$climb"
  for so in "$libdir"/libmsodbcsql-*.so.* "$libdir"/libodbcinst.so.2; do
    [ -e "$so" ] || continue
    case "$(basename "$so")" in
      libmsodbcsql-*.so.*) msodbc_seen=$((msodbc_seen + 1)) ;;
      libodbcinst.so.2)    odbcinst_seen=$((odbcinst_seen + 1)) ;;
    esac
    got="$(patchelf --print-rpath "$so" 2>/dev/null || true)"
    if [ "$got" = "$want" ]; then
      echo "RPATH-OK (already baked) $(basename "$so") -> $got"
      continue
    fi
    patchelf --set-rpath "$want" "$so"
    got="$(patchelf --print-rpath "$so")"
    # Assert the EXACT intended RUNPATH, not just "no absolute entry".
    if [ "$got" != "$want" ]; then
      echo "ERROR: patch did not yield the exact expected RUNPATH ('$got' != '$want')." >&2
      exit 1
    fi
    echo "RPATH-PATCHED $(basename "$so") -> $got"
  done
done
shopt -u nullglob
# A Linux payload missing the driver OR the driver manager is a bypass hole (a bare
# `conda build` skipping the orchestrator audit would ship un-asserted binaries).
if [ "$have_linux_payload" = "1" ] && { [ "$msodbc_seen" = "0" ] || [ "$odbcinst_seen" = "0" ]; }; then
  echo "ERROR: Linux payload present but incomplete (libmsodbcsql=$msodbc_seen, libodbcinst.so.2=$odbcinst_seen)." >&2
  exit 1
fi
[ "$msodbc_seen" -gt 0 ] && echo "LINUX_RPATH_CLIMB_OK" || true
