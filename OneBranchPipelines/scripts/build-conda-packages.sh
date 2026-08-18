#!/usr/bin/env bash
# Build + validate the mssql-python + mssql-python-odbc conda packages from
# prebuilt (ESRP-signed) wheels, fully offline via a local --find-links dir.
# ============================================================================
# Bash port of build-conda-packages.ps1 for the macOS (osx-arm64) and Linux
# (linux-64) build legs. conda-build provisions a real per-subdir HOST env and
# pip-installs the matching wheel (see conda/*/build.sh), so this MUST run on the
# matching native agent — osx-arm64 on Apple Silicon, linux-64 on the glibc
# x86_64 host — exactly like the wheel matrix. It does NOT cross-build.
#
# Args:
#   $1 WheelsDir           find-links dir holding the mssql-python + mssql-python-odbc wheels
#   $2 RecipeRoot          repo conda/ dir (mssql-python/ + mssql-python-odbc/)
#   $3 OutputDir           space-free work/output dir (Miniforge + croot + built pkgs)
#   $4 MssqlPythonVersion  version to stamp on mssql-python
#   $5 OdbcVersion         version to stamp on mssql-python-odbc
#   $6 PythonVersions      optional comma-separated (e.g. "3.11,3.12"); empty = auto-detect
#   $7 CondaSubdir         optional target subdir (e.g. osx-64, linux-aarch64) to
#                          CROSS-target via CONDA_SUBDIR; empty = build the host's
#                          native subdir. Cross-targeting relies on the host being
#                          able to RUN the target's Python for the import validation
#                          (Rosetta 2 for osx-64 on Apple Silicon; QEMU binfmt for
#                          linux-aarch64 on an x86_64 host).
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
# packages are stamped for $CondaSubdir and the import validation solves that subdir
# (running the target Python under Rosetta 2 / QEMU as noted above). Left unset for a
# native build (osx-arm64 on Apple Silicon, linux-64 on the glibc x86_64 host).
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
odbcRecipe="$RecipeRoot/mssql-python-odbc"
bindRecipe="$RecipeRoot/mssql-python"
for py in $pyvers; do
  echo "=== [py $py] build mssql-python-odbc (companion) ==="
  "$conda" build "$odbcRecipe" --python "$py" --no-test --no-anaconda-upload --output-folder "$bld"
  echo "=== [py $py] build mssql-python (binding) ==="
  "$conda" build "$bindRecipe" --python "$py" --no-test --no-anaconda-upload --output-folder "$bld"
done

# ---------------------------------------------------------------------------
# 6. Index the freshly built local channel
# ---------------------------------------------------------------------------
echo "=== indexing local channel ==="
"$conda" index "$bld"

# ---------------------------------------------------------------------------
# 7. Validate: solve a fresh env from the local channel and import both packages.
#    Proves the version-locked companion + azure-identity deps resolve AND that
#    the repackaged native binding imports (the driver loads at import).
# ---------------------------------------------------------------------------
for py in $pyvers; do
  envName="verify_${py//./}"
  echo "=== [py $py] create verify env from local channel ==="
  # -c microsoft (ahead of conda-forge) so azure-core/azure-identity/msal resolve from the
  # lean `microsoft` channel, NOT conda-forge whose azure-core recipe over-declares flask/six
  # -> celery/boto3/botocore (~9 MB); see conda-forge/azure-core-feedstock#71.
  # --strict-channel-priority keeps the freshly built local companion + binding authoritative.
  "$conda" create -y -n "$envName" -c "$bld" -c microsoft -c conda-forge --strict-channel-priority --override-channels "python=$py" mssql-python
  echo "=== [py $py] import companion + binding ==="
  "$conda" run -n "$envName" python -c "import mssql_python_odbc; print('COMPANION_OK', mssql_python_odbc.__version__)"
  "$conda" run -n "$envName" python -c "import mssql_python; print('BINDING_OK', mssql_python.__version__)"
  echo "=== [py $py] DB-less driver-load proof (real ODBC driver must load, not just the shim) ==="
  "$conda" run -n "$envName" python "$RecipeRoot/driver_load_probe.py"
  echo "=== [py $py] confirm resolved dependencies ==="
  "$conda" list -n "$envName" | grep -E 'azure-identity|mssql-python|mssql-python-odbc' || true
done

echo "==================== built conda artifacts ===================="
find "$bld" -type f \( -name 'mssql-python*.conda' -o -name 'mssql-python*.tar.bz2' \) -print
echo "CONDA_BUILD_OK"
