#!/usr/bin/env bash
# Build + validate the self-contained mssql-python conda package from prebuilt
# (ESRP-signed) wheels, fully offline via a local --find-links dir. The package
# VENDORS the ODBC Driver 18 payload -- the recipe extracts BOTH the code wheel and
# the mssql-python-odbc wheel into one site-packages -- so there is NO separate
# conda package (the v1.11.0 model).
# ============================================================================
# Bash port of build-conda-packages.ps1 for the macOS and Linux build legs.
# conda-build provisions a per-subdir HOST env and installs the matching wheel
# (see conda/*/build.sh). It runs NATIVELY for linux-64 and osx-64, under QEMU
# binfmt for linux-aarch64, and as a CROSS-build for osx-arm64 on the Intel macOS
# agent (there is no reverse Rosetta, so the arm64 Python is never executed --
# conda/*/build.sh extract the universal2 wheel without Python and the section-7
# runtime import is skipped; the pipeline's static arm64-slice audit stands in).
#
# Args:
#   $1 WheelsDir           find-links dir holding the mssql-python + mssql-python-odbc wheels
#   $2 RecipeRoot          repo conda/ dir (mssql-python/ + mssql-python-odbc/)
#   $3 OutputDir           space-free work/output dir (Miniforge + croot + built pkgs)
#   $4 MssqlPythonVersion  version to stamp on mssql-python
#   $5 OdbcVersion         version to stamp on mssql-python-odbc
#   $6 PythonVersions      optional comma-separated (e.g. "3.11,3.12"); empty = auto-detect
#   $7 CondaSubdir         optional target subdir (e.g. osx-64, osx-arm64,
#                          linux-aarch64) to CROSS-target via CONDA_SUBDIR; empty =
#                          build the host's native subdir. The section-7 runtime
#                          import validation requires the host to be able to RUN the
#                          target's Python -- true natively, under Rosetta 2 (osx-64
#                          on Apple Silicon) and under QEMU binfmt (linux-aarch64 on
#                          x86_64). For osx-arm64 on the Intel agent it is NOT, so
#                          that leg auto-skips the import (static arch audit stands in).
set -euo pipefail

WheelsDir="${1:?WheelsDir required}"
RecipeRoot="${2:?RecipeRoot required}"
OutputDir="${3:?OutputDir required}"
MssqlPythonVersion="${4:?MssqlPythonVersion required}"
OdbcVersion="${5:?OdbcVersion required}"
PythonVersions="${6:-}"
CondaSubdir="${7:-}"

echo "==================== conda build inputs ===================="
echo "WheelsDir          : $WheelsDir"
echo "RecipeRoot         : $RecipeRoot"
echo "OutputDir          : $OutputDir"
echo "MssqlPythonVersion : $MssqlPythonVersion"
echo "OdbcVersion        : $OdbcVersion"
echo "PythonVersions     : ${PythonVersions:-(auto-detect)}"
echo "CondaSubdir        : ${CondaSubdir:-(native)}"
echo "============================================================"

mkdir -p "$OutputDir"
bld="$OutputDir/bld"
mkdir -p "$bld"

# ---------------------------------------------------------------------------
# 1. Locate conda, or install Miniforge3 (conda-forge defaults) for THIS platform
# ---------------------------------------------------------------------------
conda="$(command -v conda || true)"
# Reuse an existing Miniforge install if a previous run already created one. On
# macOS the universal2 build invokes this script once per subdir (osx-64 AND
# osx-arm64) on the SAME agent, sharing $OutputDir; each run is a fresh shell so
# `command -v conda` is empty even though miniforge/ already exists. Without this
# guard the second run re-runs the installer into the existing dir and fails with
# "File or directory already exists: .../conda-bld/miniforge".
if [ -z "$conda" ] && [ -x "$OutputDir/miniforge/bin/conda" ]; then
  echo "=== reusing existing Miniforge3 at $OutputDir/miniforge ==="
  conda="$OutputDir/miniforge/bin/conda"
fi
if [ -z "$conda" ]; then
  echo "=== conda not found on PATH; installing Miniforge3 ==="
  os="$(uname -s)"; arch="$(uname -m)"
  case "$os-$arch" in
    Darwin-arm64)  mf="Miniforge3-MacOSX-arm64.sh" ;;
    Darwin-x86_64) mf="Miniforge3-MacOSX-x86_64.sh" ;;
    Linux-x86_64)  mf="Miniforge3-Linux-x86_64.sh" ;;
    Linux-aarch64) mf="Miniforge3-Linux-aarch64.sh" ;;
    *) echo "ERROR: unsupported platform '$os-$arch' for Miniforge" >&2; exit 1 ;;
  esac
  forgeDir="$OutputDir/miniforge"
  installer="$OutputDir/$mf"
  url="https://github.com/conda-forge/miniforge/releases/latest/download/$mf"
  echo "Downloading $url"
  curl -fL "$url" -o "$installer"
  # -u = update/reuse an existing target dir instead of erroring, in case a prior
  # run left a partial miniforge/ behind that failed the reuse check above.
  bash "$installer" -b -u -p "$forgeDir"
  conda="$forgeDir/bin/conda"
fi
if ! "$conda" --version >/dev/null 2>&1; then
  echo "ERROR: conda not available at '$conda' after install attempt." >&2
  exit 1
fi
echo "Using conda: $conda"
"$conda" --version

# ---------------------------------------------------------------------------
# 2. Install conda-build (pinned to the stable pre-26 series)
# ---------------------------------------------------------------------------
# Pin conda-build<26: the bleeding-edge 26.7.0 crashes with an internal
# "An unexpected error has occurred" during the LOCAL packaging phase (right
# after "Fixing permissions"); 26.7.1 is not yet released. The mature 25.x
# series builds these recipes cleanly and supports every key we use.
# NOTE: anaconda-client is intentionally NOT installed here — this script only
# builds + validates (it never runs `anaconda upload`). Publishing installs its
# own anaconda-client in conda-publish-step.yml. Keeping it out of the build env
# also drops the anaconda-auth conda plugin, which the crash report fingered.
echo "=== installing conda-build (<26) ==="
"$conda" install -y -n base "conda-build<26"

# ---------------------------------------------------------------------------
# 3. Determine which Python versions to build (auto-detect from mssql_python wheels)
# ---------------------------------------------------------------------------
if [ -z "$PythonVersions" ]; then
  pyvers="$(ls "$WheelsDir"/mssql_python-*.whl 2>/dev/null \
    | grep -v 'mssql_python_odbc' \
    | sed -nE 's/.*-cp3([0-9]+)-.*/3.\1/p' | sort -u)"
else
  pyvers="$(echo "$PythonVersions" | tr ',' '\n' | sed 's/[[:space:]]//g' | grep -v '^$')"
fi
if [ -z "$pyvers" ]; then
  echo "ERROR: no mssql_python wheels in '$WheelsDir' to determine Python versions." >&2
  exit 1
fi
echo "Building conda packages for Python versions: $(echo "$pyvers" | tr '\n' ' ')"

# ---------------------------------------------------------------------------
# 4. Export the environment consumed by the recipes (jinja + build scripts)
# ---------------------------------------------------------------------------
export WHEELS_DIR="$WheelsDir"
export MSSQL_PYTHON_VERSION="$MssqlPythonVersion"
export MSSQL_ODBC_VERSION="$OdbcVersion"

# Cross-subdir builds: force conda-build AND the verify `conda create` to target the
# requested subdir instead of the host's native one. Both honor CONDA_SUBDIR, so the
# packages are stamped for $CondaSubdir. The section-7 import validation solves that
# subdir and runs the target Python where the host can execute it (natively, under
# Rosetta 2 for osx-64, or under QEMU binfmt for linux-aarch64); on the osx-arm64
# cross-build (Intel agent, no reverse Rosetta) section 7 auto-detects that the target
# Python can't run and skips the import. Left unset for a native build.
if [ -n "$CondaSubdir" ]; then
  export CONDA_SUBDIR="$CondaSubdir"
  echo "Cross-targeting conda subdir: CONDA_SUBDIR=$CONDA_SUBDIR"
  # Emulated aarch64 cross-build: the verify env's target-arch Python (section 7)
  # runs under qemu-user. Point qemu at the aarch64 glibc loader/libs (installed via
  # libc6-arm64-cross on the leg) so it can find /lib/ld-linux-aarch64.so.1. Only the
  # emulated aarch64 leg has this dir; elsewhere the var is a harmless no-op.
  case "$CONDA_SUBDIR" in
    *aarch64)
      if [ -d /usr/aarch64-linux-gnu ]; then
        export QEMU_LD_PREFIX="${QEMU_LD_PREFIX:-/usr/aarch64-linux-gnu}"
        echo "Set QEMU_LD_PREFIX=$QEMU_LD_PREFIX for emulated aarch64 verify"
      fi
      ;;
  esac
fi

# ---------------------------------------------------------------------------
# 5. Build companion FIRST, then the binding, for each Python version
# ---------------------------------------------------------------------------
bindRecipe="$RecipeRoot/mssql-python"
for py in $pyvers; do
  echo "=== [py $py] build mssql-python (self-contained: vendors the ODBC payload) ==="
  "$conda" build "$bindRecipe" --python "$py" --no-test --no-anaconda-upload --output-folder "$bld"
done

# ---------------------------------------------------------------------------
# 6. Index the freshly built local channel
# ---------------------------------------------------------------------------
echo "=== indexing local channel ==="
"$conda" index "$bld"

# ---------------------------------------------------------------------------
# 6b. Masking-immune RUNPATH audit of the freshly built packages (#563).
# ---------------------------------------------------------------------------
# BLOCKING static gate: read the ELF RUNPATH BYTES of the vendored Linux ODBC
# binaries in every built .conda and require the relative $ORIGIN climb (+ no
# vendored krb5/openssl/libltdl). Immune to the system-lib masking that hides an
# unreachable conda copy from a runtime ldd/import on a full agent; win/osx
# packages carry no ELF payload and are skipped. `set -e` makes a violation abort.
auditScript="$(cd "$(dirname "$RecipeRoot")" && pwd)/eng/scripts/audit_bundled_binaries.py"
if [ ! -f "$auditScript" ]; then
  echo "ERROR: RUNPATH audit script not found at $auditScript" >&2
  exit 1
fi
echo "=== RUNPATH self-containment audit (eng/scripts/audit_bundled_binaries.py) ==="
"$conda" run -n base python -m pip install --quiet --disable-pip-version-check zstandard
"$conda" run -n base python "$auditScript" --root "$bld"

# ---------------------------------------------------------------------------
# 7. Validate: solve a fresh env from the local channel and import the package.
#    Proves azure-identity + the folded-in openssl/krb5 deps resolve AND that the
#    repackaged native binding imports with its vendored ODBC payload (driver loads
#    at import).
# ---------------------------------------------------------------------------
for py in $pyvers; do
  envName="verify_${py//./}"
  echo "=== [py $py] create verify env from local channel ==="
  # -c microsoft (ahead of conda-forge) so azure-core/azure-identity/msal resolve from the
  # lean `microsoft` channel, NOT conda-forge whose azure-core recipe over-declares flask/six
  # -> celery/boto3/botocore (~9 MB); see conda-forge/azure-core-feedstock#71.
  # --strict-channel-priority keeps the freshly built local package authoritative.
  "$conda" create -y -n "$envName" -c "$bld" -c microsoft -c conda-forge --strict-channel-priority --override-channels "python=$py" mssql-python
  # Whether the freshly built package's Python can EXECUTE on this host.
  target_runnable=1
  "$conda" run -n "$envName" python -c "import sys" >/dev/null 2>&1 || target_runnable=0
  if [ "$target_runnable" = "0" ]; then
    # The ONLY leg allowed to skip the runtime proof is the osx-arm64 cross-build on
    # an Intel agent (no reverse Rosetta): the arm64 Python genuinely cannot run here,
    # and the pipeline's static arm64-slice audit (lipo/otool/file on the arm64 Mach-O
    # payload) stands in -- the same assurance as the shipping PyPI universal2 arm64
    # slice. Every OTHER target (linux-64/osx-64 native, linux-aarch64 under QEMU
    # binfmt) MUST run its own import; a leg that cannot is a real failure, never a
    # silent pass -- otherwise a broken linux-aarch64 package ships unvalidated.
    if [ "${CONDA_SUBDIR:-}" = "osx-arm64" ] && [ "$(uname -s)" = "Darwin" ]; then
      echo "=== [py $py] osx-arm64 cross on Intel: target Python not executable; skipping runtime import (static arm64-slice audit covers this leg). ==="
      continue
    fi
    echo "ERROR: [py $py] target Python for CONDA_SUBDIR=${CONDA_SUBDIR:-native} is not executable on $(uname -s)/$(uname -m), and this is NOT the osx-arm64 cross-build. Refusing to silently skip validation (linux-aarch64 requires QEMU binfmt to be registered on this leg)." >&2
    exit 1
  fi
  echo "=== [py $py] import mssql_python + prove the vendored ODBC payload is present ==="
  "$conda" run -n "$envName" python -c "import mssql_python; print('BINDING_OK', mssql_python.__version__)"
  "$conda" run -n "$envName" python -c "import mssql_python_odbc; print('ODBC_PAYLOAD_OK', mssql_python_odbc.__version__)"
  echo "=== [py $py] DB-less driver-load proof (real ODBC driver must load, not just the shim) ==="
  "$conda" run -n "$envName" python "$RecipeRoot/driver_load_probe.py"
  # Minimal-base reachability gate (#563): on a Linux leg with NO system
  # krb5/libltdl (set CONDA_ASSERT_PREFIX_REACHABLE=1), prove the vendored driver
  # binds the env's OWN $CONDA_PREFIX/lib copies via the $ORIGIN climb -- not a
  # system fallthrough that would MASK an unreachable conda lib on a full agent.
  # The $ORIGIN climb makes ldd resolve krb5/gssapi/libltdl from $CONDA_PREFIX/lib
  # without LD_LIBRARY_PATH; a system or not-found binding fails the leg.
  if [ "${CONDA_ASSERT_PREFIX_REACHABLE:-}" = "1" ] && [ "$(uname -s)" = "Linux" ]; then
    echo "=== [py $py] minimal-base ldd reachability gate (driver must bind CONDA_PREFIX/lib) ==="
    env_prefix="$("$conda" run -n "$envName" python -c 'import os,sys; print(os.environ.get("CONDA_PREFIX") or sys.prefix)')"
    drv="$("$conda" run -n "$envName" python -c 'import mssql_python,glob,os; b=os.path.dirname(mssql_python.__file__); print(glob.glob(os.path.join(b,"..","mssql_python_odbc","libs","linux","*","*","lib","libmsodbcsql*"))[0])')"
    inst="$(dirname "$drv")/libodbcinst.so.2"
    reach_fail=0
    for lib in "$drv" "$inst"; do
      echo "--- ldd $(basename "$lib") ---"
      ldd_out="$("$conda" run -n "$envName" ldd "$lib" 2>/dev/null || true)"
      echo "$ldd_out"
      while IFS= read -r line; do
        case "$line" in
          *libkrb5.so*|*libgssapi_krb5.so*|*libltdl.so*)
            resolved="$(printf '%s' "$line" | sed -nE 's/.*=> +([^ ]+).*/\1/p')"
            case "$resolved" in
              "$env_prefix"/*) echo "OK   $line" ;;
              "")              echo "MISS $line"; reach_fail=1 ;;
              *)               echo "MASK(system) $line"; reach_fail=1 ;;
            esac
            ;;
        esac
      done <<EOF
$ldd_out
EOF
    done
    [ "$reach_fail" = "0" ] || { echo "ERROR: [py $py] driver bound a system/absent krb5/gssapi/libltdl instead of $env_prefix/lib (reachability broken)." >&2; exit 1; }
    echo "REACHABILITY_OK"
  fi
  # Live Encrypt=yes TLS gate -- forces the driver to dlopen its OpenSSL backend
  # (libssl/libcrypto), which the DB-less Encrypt=no probe above NEVER exercises.
  # Runs (BLOCKING) only when CONDA_TLS_PROBE_CONN points at a reachable server;
  # otherwise it SKIPS loudly (it never silently passes). CAVEAT: this is
  # conclusive ONLY on a minimal base with NO system OpenSSL -- a system libssl
  # lets the driver's dlopen fall through and MASK an unreachable conda
  # <PREFIX>/lib copy (exactly what full CI agents hide). The masking-IMMUNE guard
  # is eng/scripts/audit_bundled_binaries.py, which reads the RUNPATH bytes and
  # requires an $ORIGIN/.. climb regardless of any system libs; this gate is the
  # complementary end-to-end backstop for a minimal-base leg.
  if [ -n "${CONDA_TLS_PROBE_CONN:-}" ]; then
    echo "=== [py $py] live Encrypt=yes TLS gate (OpenSSL backend must be reachable) ==="
    "$conda" run -n "$envName" python "$RecipeRoot/tls_connect_probe.py"
  else
    echo "=== [py $py] Encrypt=yes TLS gate SKIPPED (set CONDA_TLS_PROBE_CONN on a minimal-base leg to enable) ==="
  fi
  echo "=== [py $py] confirm resolved dependencies ==="
  "$conda" list -n "$envName" | grep -E 'azure-identity|mssql-python|openssl|krb5' || true
done

echo "==================== built conda artifacts ===================="
find "$bld" -type f \( -name 'mssql-python*.conda' -o -name 'mssql-python*.tar.bz2' \) -print
echo "CONDA_BUILD_OK"
