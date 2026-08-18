#!/usr/bin/env bash
# ============================================================================
# Complete the self-contained Linux ODBC driver payload: bundle libltdl.so.7
# and fix the driver-manager library's runpath.
# ----------------------------------------------------------------------------
# WHY: the vendored Linux payload ships libmsodbcsql-18.*.so + libodbcinst.so.2
# but NOT libltdl.so.7. Measured with readelf/pyelftools:
#   * libmsodbcsql-18.*.so : RUNPATH=$ORIGIN, NEEDED libodbcinst.so.2 (co-located,
#                            resolves) + libkrb5/libgssapi_krb5 (system, declared).
#   * libodbcinst.so.2     : RUNPATH=<none>, NEEDED libltdl.so.7 (NOT bundled).
# So on a minimal Linux base the driver import throws
#   OSError: libltdl.so.7: cannot open shared object file
# It only "works" where the host happens to have a system libltdl on the loader
# path. macOS ALREADY bundles libltdl.7.dylib co-located with libodbcinst.2.dylib;
# Linux is simply the outlier that forgot to vendor its libltdl. This script makes
# Linux match that established, shipped macOS practice IN THE SAME wheel payload,
# so BOTH the PyPI wheel and the conda repackage become self-contained.
#
# WHAT (per glibc distro subtree x arch -- Alpine/musl is intentionally skipped;
# its libltdl differs and the musl slice is handled separately):
#   1. copy a glibc-built libltdl.so.7 next to libodbcinst.so.2, and
#   2. patchelf --set-rpath '$ORIGIN' libodbcinst.so.2 so its co-located
#      libltdl.so.7 NEEDED resolves without LD_LIBRARY_PATH.
# libltdl links only libc/libdl (symbols from glibc <=2.4), so a manylinux_2_28
# (AlmaLinux 8 / glibc 2.28) libltdl is portable across every glibc target we ship.
#
# WHERE: run this INSIDE a manylinux_2_28 container (has dnf + patchelf; glibc 2.28
# so the sourced libltdl stays manylinux-clean). patchelf edits ELF structure only,
# so an x86_64 patchelf can rewrite the aarch64 libodbcinst.so.2 too -- one x86_64
# container patches both arches. It is also runnable by a maintainer to refresh the
# committed mssql_python_odbc/libs/ tree.
#
# USAGE: patch-linux-odbc-libs.sh <path-to>/mssql_python_odbc/libs/linux
# ============================================================================
set -euo pipefail

LIBS_LINUX_ROOT="${1:?usage: patch-linux-odbc-libs.sh <path-to>/mssql_python_odbc/libs/linux}"
[ -d "$LIBS_LINUX_ROOT" ] || { echo "ERROR: not a directory: $LIBS_LINUX_ROOT" >&2; exit 1; }
command -v patchelf >/dev/null 2>&1 || { echo "ERROR: patchelf not found (run inside a manylinux image)" >&2; exit 1; }

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

# --- 1. source a glibc libltdl.so.7 for each shipped arch -------------------
# x86_64: install natively (this script runs in an x86_64 manylinux container) and
# resolve the REAL file behind the libltdl.so.7 SONAME symlink.
echo "== sourcing x86_64 libltdl.so.7 (native install) =="
dnf -y install libtool-ltdl dnf-plugins-core >/dev/null 2>&1 \
  || yum -y install libtool-ltdl yum-utils >/dev/null 2>&1
src_x86_64=""
for cand in /usr/lib64/libltdl.so.7 /usr/lib/libltdl.so.7 /lib64/libltdl.so.7; do
  real="$(readlink -f "$cand" 2>/dev/null || true)"
  if [ -n "$real" ] && [ -f "$real" ]; then
    cp -f "$real" "$work/libltdl.so.7.x86_64"
    src_x86_64="$work/libltdl.so.7.x86_64"
    break
  fi
done
[ -n "$src_x86_64" ] || { echo "ERROR: could not source an x86_64 libltdl.so.7" >&2; exit 1; }

# aarch64: DOWNLOAD-ONLY the aarch64 rpm (reliable -- no install/exec under emulation,
# see the QEMU dnf hazard) and extract libltdl.so.7 from it.
echo "== sourcing aarch64 libltdl.so.7 (download-only + extract) =="
dl="$work/aa"; mkdir -p "$dl"
( cd "$dl"
  dnf download --forcearch aarch64 libtool-ltdl >/dev/null 2>&1 \
    || dnf --releasever=8 download --forcearch aarch64 libtool-ltdl >/dev/null 2>&1 \
    || yumdownloader --forcearch aarch64 libtool-ltdl >/dev/null 2>&1 || true )
src_aarch64=""
rpm="$(find "$dl" -maxdepth 1 -name 'libtool-ltdl*aarch64.rpm' | head -1 || true)"
if [ -n "$rpm" ]; then
  ( cd "$dl" && rpm2cpio "$rpm" | cpio -idm >/dev/null 2>&1 )
  aa="$(find "$dl" -name 'libltdl.so.7*' -type f | head -1 || true)"
  if [ -n "$aa" ]; then
    cp -f "$aa" "$work/libltdl.so.7.aarch64"
    src_aarch64="$work/libltdl.so.7.aarch64"
  fi
fi
[ -n "$src_aarch64" ] || { echo "ERROR: could not source an aarch64 libltdl.so.7" >&2; exit 1; }

# --- 2. install into each glibc distro subtree + set $ORIGIN runpath --------
patched=0
for distro in debian_ubuntu rhel suse; do
  for arch in x86_64 arm64; do
    d="$LIBS_LINUX_ROOT/$distro/$arch/lib"
    [ -d "$d" ] || continue
    inst="$d/libodbcinst.so.2"
    if [ ! -f "$inst" ]; then
      echo "WARN: no libodbcinst.so.2 in $d; skipping"
      continue
    fi
    case "$arch" in
      x86_64) src="$src_x86_64" ;;
      arm64)  src="$src_aarch64" ;;
      *) echo "WARN: unexpected arch '$arch'; skipping"; continue ;;
    esac
    cp -f "$src" "$d/libltdl.so.7"
    chmod 0755 "$d/libltdl.so.7"
    # libodbcinst.so.2 has NO runpath today and NEEDs libltdl.so.7 -> add $ORIGIN so
    # its co-located sibling resolves. (libmsodbcsql already has RUNPATH=$ORIGIN and
    # finds libodbcinst.so.2 the same way.)
    patchelf --set-rpath '$ORIGIN' "$inst"
    rp="$(patchelf --print-rpath "$inst")"
    [ "$rp" = '$ORIGIN' ] || { echo "ERROR: rpath not applied to $inst (got '$rp')" >&2; exit 1; }
    echo "OK: $distro/$arch -> bundled libltdl.so.7 + RUNPATH=\$ORIGIN on libodbcinst.so.2"
    patched=$((patched + 1))
  done
done
[ "$patched" -gt 0 ] || { echo "ERROR: patched nothing under $LIBS_LINUX_ROOT (no glibc distro lib dirs?)" >&2; exit 1; }
echo "patch-linux-odbc-libs: completed ($patched glibc subtree(s) patched)."
