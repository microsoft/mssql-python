#!/usr/bin/env bash
# ============================================================================
# Fix the Linux ODBC driver-manager + driver runpaths so the DECLARED (conda,
# not vendored) libltdl / krb5 / openssl resolve -- for BOTH glibc and musl.
# ----------------------------------------------------------------------------
# WHY: the vendored Linux payload ships libmsodbcsql-18.*.so + libodbcinst.so.2
# but NOT libltdl.so.7. Measured with readelf/pyelftools:
#   * libmsodbcsql-18.*.so : RUNPATH=$ORIGIN, NEEDED libodbcinst.so.2 (co-located,
#                            resolves) + libkrb5.so.3/libgssapi_krb5.so.2 (declared,
#                            NOT bundled). It ALSO dlopens libssl/libcrypto lazily
#                            on Encrypt=yes: the OpenSSL symbols are present but
#                            there is NO libssl/libcrypto DT_NEEDED or soname string
#                            (the name is built at runtime), so TLS loads OpenSSL
#                            by name at connect time, not at import.
#   * libodbcinst.so.2     : RUNPATH=<none>, NEEDED libltdl.so.7 (NOT bundled).
# So on a minimal Linux base the driver import throws
#   OSError: libltdl.so.7: cannot open shared object file
# and -- the subtler, second bug -- with RUNPATH=$ORIGIN the driver searches only
# its OWN dir for the DECLARED libkrb5/libgssapi_krb5 (load-time NEEDED) and the
# dlopen'd libssl/libcrypto (Encrypt=yes). In a conda env those live in
# <PREFIX>/lib, which is NOT on the loader path (conda sets no LD_LIBRARY_PATH, and
# binary_relocation=false means conda-build injects no rpath), so the
# conda-DECLARED openssl/krb5 are UNREACHABLE -- it only "works" where a SYSTEM
# copy happens to mask it (exactly what a hosted agent's system libs hide).
# It only "works" where the host happens to have a system libltdl on the loader
# path -- e.g. a CI container that installed unixODBC, which is exactly why the
# Alpine test leg MASKS this today. On Linux we do NOT vendor libltdl (macOS keeps
# its co-located libltdl.7.dylib): instead the conda package DECLARES libtool and
# this script gives libodbcinst.so.2 + libmsodbcsql a RUNPATH that climbs to the
# conda <PREFIX>/lib, so BOTH the DECLARED libltdl and the DECLARED openssl/krb5
# resolve there. The PyPI wheel keeps relying on a system libltdl (as before).
#
# WHAT (per distro subtree for the CURRENT arch/libc):
#   1. patchelf --set-rpath '$ORIGIN:$ORIGIN/../..(x8)' libodbcinst.so.2 so it
#      keeps finding co-located siblings (via $ORIGIN) AND reaches the conda
#      <PREFIX>/lib for its DECLARED, NOT-vendored NEEDED libltdl.so.7, and
#   2. patchelf --set-rpath '$ORIGIN:$ORIGIN/../..(x8)' libmsodbcsql-*.so* so the
#      driver keeps finding co-located libodbcinst.so.2 (via $ORIGIN) AND ALSO
#      reaches the conda <PREFIX>/lib for the DECLARED libkrb5/libgssapi_krb5 +
#      the dlopen'd libssl/libcrypto. glibc consults the caller's DT_RUNPATH for
#      BOTH its own direct NEEDED and its own dlopen(soname), so this one relative
#      climb fixes both. It is inert for the PyPI wheel (the venv lib dir holds
#      none of these -> the loader falls through to the system), so a SINGLE
#      committed binary is correct for both the wheel and the conda repackage.
# libltdl is NOT vendored here: it is declared (conda-forge `libtool`) and the
# conda build.sh applies the SAME libodbcinst climb at repackage time, so the two
# paths converge. ONE run per (libc, arch) serves all the distro subtrees for that
# arch.
#
# WHERE: run this INSIDE the matching pipeline image, once per (libc, arch):
#   * glibc: manylinux_2_28_{x86_64,aarch64}  (dnf/yum + patchelf; AlmaLinux 8)
#   * musl : ghcr.io/microsoft/mssql-rs/import/python-build/musllinux_1_2_{x86_64,aarch64}
#            (apk add patchelf)
# The script auto-detects libc + arch and patches only the subtrees it can serve.
# It is run by a maintainer to REFRESH the committed mssql_python_odbc/libs/ tree;
# the produced binaries are then committed (the libs/ payload is already a set of
# checked-in vendored driver binaries, so committing the rpath-patched
# libodbcinst.so.2 + libmsodbcsql beside them is consistent).
#
# USAGE: patch-linux-odbc-libs.sh <path-to>/mssql_python_odbc/libs/linux
# ============================================================================
set -euo pipefail

LIBS_LINUX_ROOT="${1:?usage: patch-linux-odbc-libs.sh <path-to>/mssql_python_odbc/libs/linux}"
[ -d "$LIBS_LINUX_ROOT" ] || { echo "ERROR: not a directory: $LIBS_LINUX_ROOT" >&2; exit 1; }

# --- detect arch (matches the libs/ subtree dir name) -----------------------
case "$(uname -m)" in
  x86_64|amd64)  ARCH=x86_64 ;;
  aarch64|arm64) ARCH=arm64 ;;
  *) echo "ERROR: unsupported arch '$(uname -m)'" >&2; exit 1 ;;
esac

# --- detect libc + ensure patchelf ------------------------------------------
# glibc images (manylinux, AlmaLinux 8) have dnf/yum; musl images (Alpine) have apk.
# Detect by libc to pick the distro subtrees this image serves for THIS arch, and
# make sure patchelf is available. libltdl.so.7 is NO LONGER vendored: it is
# DECLARED (conda-forge `libtool`) and resolved from the conda <PREFIX>/lib via the
# RUNPATH climb set below, so nothing is sourced or copied here.
if command -v apk >/dev/null 2>&1 || (ldd --version 2>&1 | grep -qi musl); then
  LIBC=musl
  DISTROS="alpine"
  apk add --no-cache patchelf >/dev/null 2>&1 || true
else
  LIBC=glibc
  DISTROS="debian_ubuntu rhel suse"
  ( dnf -y install patchelf >/dev/null 2>&1 \
    || yum -y install patchelf >/dev/null 2>&1 ) || true
fi

command -v patchelf >/dev/null 2>&1 || { echo "ERROR: patchelf not found (install it in this image)" >&2; exit 1; }
echo "== $LIBC/$ARCH: patchelf ready =="

# --- install into each distro subtree for THIS arch + set runpaths -----------
# In a pip/conda install the driver lives at
#   <PREFIX>/lib/pythonX.Y/site-packages/mssql_python_odbc/libs/linux/<distro>/<arch>/lib/
# so <PREFIX>/lib is exactly 8 dirs above $ORIGIN. That depth is identical for
# every python version and every distro/arch subtree, so one relative climb serves
# all of them. patchelf writes the literal $ORIGIN; the loader expands it per load.
PREFIX_LIB='$ORIGIN/../../../../../../../..'
patched=0
for distro in $DISTROS; do
  d="$LIBS_LINUX_ROOT/$distro/$ARCH/lib"
  [ -d "$d" ] || { echo "-- $distro/$ARCH: no lib dir, skipping"; continue; }
  inst="$d/libodbcinst.so.2"
  if [ ! -f "$inst" ]; then
    echo "WARN: no libodbcinst.so.2 in $d; skipping"
    continue
  fi
  # libodbcinst.so.2 NEEDs libltdl.so.7, which we DECLARE (conda-forge `libtool`)
  # rather than vendor. Give it the same climb the driver uses so the declared
  # libltdl.so.7 resolves from the conda <PREFIX>/lib. (A bare $ORIGIN would only
  # find a co-located copy, which no longer exists.)
  patchelf --set-rpath "\$ORIGIN:$PREFIX_LIB" "$inst"
  rp="$(patchelf --print-rpath "$inst")"
  [ "$rp" = "\$ORIGIN:$PREFIX_LIB" ] || { echo "ERROR: rpath not applied to $inst (got '$rp')" >&2; exit 1; }
  echo "OK: $distro/$ARCH -> RUNPATH='\$ORIGIN:$PREFIX_LIB' on libodbcinst.so.2 (declared libltdl)"
  # The driver: keep $ORIGIN (co-located libodbcinst.so.2) AND add the climb to
  # <PREFIX>/lib so its DECLARED, NOT-bundled deps resolve from a conda env -- the
  # load-time NEEDED libkrb5.so.3/libgssapi_krb5.so.2 AND the libssl/libcrypto it
  # dlopens at Encrypt=yes. Without the climb those resolve only off a SYSTEM copy.
  drv_patched=0
  for drv in "$d"/libmsodbcsql-*.so*; do
    [ -f "$drv" ] || continue
    patchelf --set-rpath "\$ORIGIN:$PREFIX_LIB" "$drv"
    drp="$(patchelf --print-rpath "$drv")"
    [ "$drp" = "\$ORIGIN:$PREFIX_LIB" ] || { echo "ERROR: driver rpath not applied to $drv (got '$drp')" >&2; exit 1; }
    echo "OK: $distro/$ARCH -> RUNPATH='\$ORIGIN:$PREFIX_LIB' on $(basename "$drv")"
    drv_patched=$((drv_patched + 1))
  done
  [ "$drv_patched" -gt 0 ] || { echo "ERROR: no libmsodbcsql-*.so* found in $d" >&2; exit 1; }
  patched=$((patched + 1))
done
[ "$patched" -gt 0 ] || { echo "ERROR: patched nothing under $LIBS_LINUX_ROOT for $LIBC/$ARCH" >&2; exit 1; }
echo "patch-linux-odbc-libs: completed ($patched $LIBC/$ARCH subtree(s) patched)."
