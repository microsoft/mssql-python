#!/usr/bin/env bash
# ============================================================================
# Complete the self-contained Linux ODBC driver payload: bundle libltdl.so.7
# and fix the driver-manager library's runpath -- for BOTH glibc and musl.
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
# Alpine test leg MASKS this today. macOS ALREADY bundles libltdl.7.dylib
# co-located with libodbcinst.2.dylib; Linux -- glibc AND musl -- is the outlier
# that forgot to vendor its libltdl. This script makes every Linux payload match
# that established, shipped macOS practice IN THE SAME wheel, so BOTH the PyPI
# wheel and the conda repackage become self-contained on minimal bases.
#
# WHAT (per distro subtree for the CURRENT arch/libc):
#   1. copy a matching-libc libltdl.so.7 next to libodbcinst.so.2,
#   2. drop the libltdl LGPL license notice alongside it (compliance),
#   3. patchelf --set-rpath '$ORIGIN' libodbcinst.so.2 so its co-located
#      libltdl.so.7 NEEDED resolves without LD_LIBRARY_PATH, and
#   4. patchelf --set-rpath '$ORIGIN:$ORIGIN/../..(x8)' libmsodbcsql-*.so* so the
#      driver keeps finding co-located libodbcinst.so.2 (via $ORIGIN) AND ALSO
#      reaches the conda <PREFIX>/lib for the DECLARED libkrb5/libgssapi_krb5 +
#      the dlopen'd libssl/libcrypto. glibc consults the caller's DT_RUNPATH for
#      BOTH its own direct NEEDED and its own dlopen(soname), so this one relative
#      climb fixes both. It is inert for the PyPI wheel (the venv lib dir holds
#      none of these -> the loader falls through to the system), so a SINGLE
#      committed binary is correct for both the wheel and the conda repackage.
# libltdl links only libc/libdl, so a manylinux_2_28 (glibc 2.28) libltdl is
# portable across every glibc target we ship (debian_ubuntu + rhel + suse), and a
# musllinux_1_2 libltdl covers alpine. ONE build per (libc, arch) serves all the
# distro subtrees for that arch.
#
# WHERE: run this INSIDE the matching pipeline image, once per (libc, arch):
#   * glibc: manylinux_2_28_{x86_64,aarch64}  (dnf/yum + patchelf; AlmaLinux 8)
#   * musl : ghcr.io/microsoft/mssql-rs/import/python-build/musllinux_1_2_{x86_64,aarch64}
#            (apk add libtool patchelf)
# The script auto-detects libc + arch and patches only the subtrees it can serve.
# It is run by a maintainer to REFRESH the committed mssql_python_odbc/libs/ tree;
# the produced binaries are then committed (the libs/ payload is already a set of
# checked-in vendored driver binaries, so committing libltdl.so.7 + the
# rpath-patched libodbcinst.so.2 beside them is consistent).
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

# --- detect libc + source a matching libltdl.so.7 ---------------------------
# glibc images (manylinux, AlmaLinux 8) have dnf/yum; musl images (Alpine) have apk.
# Detect by libc so the sourced libltdl matches the target ABI, then patch only the
# distro subtrees that libc serves for THIS arch.
LIBLTDL=""
if command -v apk >/dev/null 2>&1 || (ldd --version 2>&1 | grep -qi musl); then
  LIBC=musl
  DISTROS="alpine"
  apk add --no-cache libtool patchelf >/dev/null 2>&1 || true
  for cand in /usr/lib/libltdl.so.7 /usr/lib/libltdl.so.7.* /lib/libltdl.so.7; do
    if [ -f "$cand" ]; then LIBLTDL="$(readlink -f "$cand")"; break; fi
  done
else
  LIBC=glibc
  DISTROS="debian_ubuntu rhel suse"
  ( dnf -y install libtool-ltdl patchelf >/dev/null 2>&1 \
    || yum -y install libtool-ltdl patchelf >/dev/null 2>&1 ) || true
  for cand in /usr/lib64/libltdl.so.7 /usr/lib/libltdl.so.7 /lib64/libltdl.so.7; do
    if [ -f "$cand" ]; then LIBLTDL="$(readlink -f "$cand")"; break; fi
  done
fi

command -v patchelf >/dev/null 2>&1 || { echo "ERROR: patchelf not found (install it in this image)" >&2; exit 1; }
[ -n "$LIBLTDL" ] && [ -f "$LIBLTDL" ] || { echo "ERROR: could not source a $LIBC libltdl.so.7 for $ARCH" >&2; exit 1; }
echo "== $LIBC/$ARCH: sourced $LIBLTDL =="

# libltdl is GNU Libtool -> LGPL-2.1-or-later. Ship a notice next to the binary
# (the full text is also aggregated in libs/LICENSING, shipped in every wheel).
read -r -d '' LGPL_NOTICE <<'EOF' || true
GNU Libtool -- libltdl (libltdl.so.7)
=====================================
This directory bundles libltdl (the GNU Libtool ltdl library), used by the
Microsoft ODBC driver manager (libodbcinst.so.2). libltdl is free software
licensed under the GNU Lesser General Public License, version 2.1 or later
(LGPL-2.1-or-later), with the GNU Libtool exception. It is linked DYNAMICALLY.

Corresponding source: https://www.gnu.org/software/libtool/
(release tarballs at https://ftp.gnu.org/gnu/libtool/).
Full LGPL-2.1 text: https://www.gnu.org/licenses/old-licenses/lgpl-2.1.txt
EOF

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
  cp -f "$LIBLTDL" "$d/libltdl.so.7"
  chmod 0755 "$d/libltdl.so.7"
  printf '%s\n' "$LGPL_NOTICE" > "$d/LIBLTDL_LGPL_LICENSE.txt"
  # libodbcinst.so.2 has NO runpath today and NEEDs libltdl.so.7 -> add $ORIGIN so
  # its co-located sibling resolves. (libmsodbcsql already has RUNPATH=$ORIGIN and
  # finds libodbcinst.so.2 the same way.)
  patchelf --set-rpath '$ORIGIN' "$inst"
  rp="$(patchelf --print-rpath "$inst")"
  [ "$rp" = '$ORIGIN' ] || { echo "ERROR: rpath not applied to $inst (got '$rp')" >&2; exit 1; }
  echo "OK: $distro/$ARCH -> bundled libltdl.so.7 + RUNPATH=\$ORIGIN on libodbcinst.so.2"
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
