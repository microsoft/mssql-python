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
# runtime import is skipped -- osx-arm64 arch is NOT independently verified here
# (trusted from the universal2 wheel tag, like the PyPI wheel; no Mach-O arch check)).
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
#                          that leg auto-skips the import. NOTE: osx-arm64 arch is not
#                          independently verified here -- trusted from the universal2
#                          wheel tag; there is no Mach-O arch check in this pipeline.
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
  mfver="${MINIFORGE_VERSION:-26.3.2-3}"
  case "$os-$arch" in
    Darwin-arm64)  mf="Miniforge3-${mfver}-MacOSX-arm64.sh" ;;
    Darwin-x86_64) mf="Miniforge3-${mfver}-MacOSX-x86_64.sh" ;;
    Linux-x86_64)  mf="Miniforge3-${mfver}-Linux-x86_64.sh" ;;
    Linux-aarch64) mf="Miniforge3-${mfver}-Linux-aarch64.sh" ;;
    *) echo "ERROR: unsupported platform '$os-$arch' for Miniforge" >&2; exit 1 ;;
  esac
  forgeDir="$OutputDir/miniforge"
  installer="$OutputDir/$mf"
  # Pin Miniforge to a specific release (never `latest`, which floats) and verify its
  # SHA256 BEFORE executing. The expected hash is NOT hard-coded in source: prefer an
  # explicit MINIFORGE_SHA256 (a pipeline variable = strongest, out-of-source), else
  # verify against the release's OWN published <installer>.sha256 sidecar. The installer
  # is never executed unverified.
  url="https://github.com/conda-forge/miniforge/releases/download/${mfver}/$mf"
  echo "Downloading pinned Miniforge $mfver: $url"
  curl -fL "$url" -o "$installer"
  if [ -n "${MINIFORGE_SHA256:-}" ]; then
    mfsha="$MINIFORGE_SHA256"
  else
    curl -fL "$url.sha256" -o "$installer.sha256"
    mfsha="$(grep -oiE '[0-9a-f]{64}' "$installer.sha256" | head -n1)"
  fi
  if [ -z "$mfsha" ]; then
    echo "ERROR: could not determine the expected SHA256 for $mf." >&2
    exit 1
  fi
  if command -v sha256sum >/dev/null 2>&1; then
    actual="$(sha256sum "$installer" | awk '{print $1}')"
  else
    actual="$(shasum -a 256 "$installer" | awk '{print $1}')"
  fi
  if [ "$actual" != "$mfsha" ]; then
    echo "ERROR: Miniforge installer SHA256 mismatch: expected '$mfsha', got '$actual'." >&2
    exit 1
  fi
  echo "Miniforge installer SHA256 verified ($mfsha)."
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
#
# Use a DEDICATED env instead of `install -n base`: a pre-installed conda whose base
# is pinned to a too-new python (the GitHub-hosted runner's Miniconda pins python
# 3.14, which no conda-build<26 supports) makes a base install UNSOLVABLE. A fresh env
# lets conda pick a python conda-build<26 supports, independent of the base pin.
# conda-forge only (--override-channels) avoids the defaults-channel ToS; zstandard
# rides along so the RUNPATH audit reads .conda metadata from this same env.
condaBuildEnv="conda_builder"
echo "=== creating dedicated conda-build env ($condaBuildEnv: conda-build<26) ==="
# Idempotent: a reused agent/workdir may already have this env, and a pre-existing
# env makes `conda create` fail under `set -e`. Remove it first (best-effort, like
# the verify envs below) so a rerun recreates cleanly.
"$conda" env remove -y -n "$condaBuildEnv" 2>/dev/null || true
"$conda" create -y -n "$condaBuildEnv" -c conda-forge --override-channels "conda-build<26" zstandard

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
  "$conda" run -n "$condaBuildEnv" conda-build "$bindRecipe" --python "$py" --no-test --no-anaconda-upload --output-folder "$bld"
done

# ---------------------------------------------------------------------------
# 6. Make the local output folder a VALID conda channel.
#    conda-build --output-folder already wrote $bld/<subdir>/repodata.json for the
#    platform we built, but a conda channel is only valid if it ALSO carries
#    noarch/repodata.json (even empty) -- otherwise `conda create -c file://$bld`
#    fails with "UnavailableInvalidChannel ... must contain noarch/repodata.json".
#    Create it directly rather than via `conda index`, whose subcommand is absent
#    from miniforge (it moved to the standalone conda-index package).
# ---------------------------------------------------------------------------
mkdir -p "$bld/noarch"
if [ ! -f "$bld/noarch/repodata.json" ]; then
  printf '%s' '{"info":{"subdir":"noarch"},"packages":{},"packages.conda":{}}' > "$bld/noarch/repodata.json"
fi

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
"$conda" run -n "$condaBuildEnv" python "$auditScript" --root "$bld"

# ---------------------------------------------------------------------------
# 7. Validate: solve a fresh env from the local channel and import the package.
#    Proves azure-identity + the folded-in openssl/krb5 deps resolve AND that the
#    repackaged native binding imports with its vendored ODBC payload (driver loads
#    at import).
# ---------------------------------------------------------------------------
# Run from a NEUTRAL dir: `python -c` prepends the cwd to sys.path, and the pipeline
# runs from the repo checkout whose in-tree mssql_python/ (source, no compiled
# extension) would shadow the conda-installed package -> "No ddbc_bindings module
# found". $OutputDir is outside the repo checkout.
cd "$OutputDir"
# conda's channel URL parser treats any path COMPONENT equal to a known conda subdir
# (osx-arm64, osx-64, linux-64, linux-aarch64, win-64, noarch) as the platform subdir
# and STRIPS it from the channel root. The pipeline isolates each leg under a
# subdir-NAMED dir (OutputDir=.../<subdir>), so $bld's path contains that token and
# `conda create -c "$bld"` would resolve to the WRONG, token-stripped path (.../bld,
# which has no repodata) -> "UnavailableInvalidChannel ... must contain noarch/...".
# Copy the freshly built channel (platform subdir + the section-6 noarch stub) into a
# token-FREE, per-leg-unique dir so conda parses the channel path verbatim.
legName="${OutputDir##*/}"                                   # e.g. osx-arm64 / linux-64
localChannel="$(dirname "$OutputDir")/verifychan_${legName//[^A-Za-z0-9]/_}"
rm -rf "$localChannel"; mkdir -p "$localChannel"
cp -a "$bld"/. "$localChannel"/
echo "verify channel (token-free alias of $bld): $localChannel"

# Emulated CROSS leg: the target-arch conda subdir differs from the host arch, so the
# verify Python runs under QEMU binfmt (e.g. linux-aarch64 on an x86_64 agent). The
# Python binding imports fine under qemu-user, but qemu-user CANNOT reliably initialize
# the native unixODBC environment -- SQLAllocEnv does ltdl/pthread/locale init it
# mis-emulates, surfacing as "Failed to allocate environment handle". The SAME driver
# loads under full-arch emulation AND on the native same-arch leg, and the masking-
# immune static RUNPATH audit already proved self-containment. So on an emulated cross
# leg the RUNTIME driver probes (driver-load, ldd reachability, TLS) are BEST-EFFORT;
# build + audit + import stay blocking.
emulated_cross=0
host_machine="$(uname -m)"
case "${CONDA_SUBDIR:-}" in
  *aarch64 | *arm64)
    if [ "$host_machine" != "aarch64" ] && [ "$host_machine" != "arm64" ]; then
      emulated_cross=1
      echo "NOTE: emulated CROSS leg (CONDA_SUBDIR=$CONDA_SUBDIR on $host_machine host); runtime driver probes are best-effort under QEMU binfmt, build/audit/import remain blocking."
    fi
    ;;
esac

for py in $pyvers; do
  # Include the target subdir so the two macOS legs (osx-64 + osx-arm64) that run on
  # the SAME agent never collide on the env name, and recreate cleanly so a re-run
  # or a leftover env can't fail `conda create`.
  sub="${CONDA_SUBDIR:-native}"; sub="${sub//-/_}"
  envName="verify_${sub}_${py//./}"
  "$conda" env remove -y -n "$envName" >/dev/null 2>&1 || true
  echo "=== [py $py] create verify env from local channel ==="
  # -c microsoft (ahead of conda-forge) so azure-core/azure-identity/msal resolve from the
  # lean `microsoft` channel, NOT conda-forge whose azure-core recipe over-declares flask/six
  # -> celery/boto3/botocore (~9 MB); see conda-forge/azure-core-feedstock#71.
  # --strict-channel-priority keeps the freshly built local package authoritative.
  "$conda" create -y -n "$envName" -c "$localChannel" -c microsoft -c conda-forge --strict-channel-priority --override-channels "python=$py" mssql-python
  # Whether the freshly built package's Python can EXECUTE on this host.
  target_runnable=1
  "$conda" run -n "$envName" python -c "import sys" >/dev/null 2>&1 || target_runnable=0
  if [ "$target_runnable" = "0" ]; then
    # The ONLY leg allowed to skip the runtime proof is the osx-arm64 cross-build on
    # an Intel agent (no reverse Rosetta): the arm64 Python genuinely cannot run here.
    # CAVEAT: osx-arm64 arch is NOT independently verified in this pipeline. Unlike
    # Windows (assert_pe_machine.py PE check) and Linux (audit_bundled_binaries.py ELF
    # e_machine check), there is NO Mach-O arch audit, so the arm64 slice is trusted
    # from the universal2 wheel tag -- exactly like the shipping PyPI universal2 wheel.
    # Every OTHER target (linux-64/osx-64 native, linux-aarch64 under QEMU binfmt) MUST
    # run its own import; a leg that cannot is a real failure, never a silent pass --
    # otherwise a broken linux-aarch64 package ships unvalidated.
    if [ "${CONDA_SUBDIR:-}" = "osx-arm64" ] && [ "$(uname -s)" = "Darwin" ]; then
      echo "=== [py $py] osx-arm64 cross on Intel: target Python not executable; skipping runtime import (osx-arm64 arch NOT independently verified -- trusted from the universal2 wheel tag). ==="
      continue
    fi
    echo "ERROR: [py $py] target Python for CONDA_SUBDIR=${CONDA_SUBDIR:-native} is not executable on $(uname -s)/$(uname -m), and this is NOT the osx-arm64 cross-build. Refusing to silently skip validation (linux-aarch64 requires QEMU binfmt to be registered on this leg)." >&2
    exit 1
  fi
  echo "=== [py $py] import mssql_python + prove the vendored ODBC payload is present ==="
  "$conda" run -n "$envName" python -c "import mssql_python; print('BINDING_OK', mssql_python.__version__)"
  "$conda" run -n "$envName" python -c "import mssql_python_odbc; print('ODBC_PAYLOAD_OK', mssql_python_odbc.__version__)"
  echo "=== [py $py] DB-less driver-load proof (real ODBC driver must load, not just the shim) ==="
  if [ "$emulated_cross" = "1" ]; then
    "$conda" run -n "$envName" python "$RecipeRoot/driver_load_probe.py" \
      || echo "SKIP (emulated cross under QEMU binfmt): qemu-user cannot initialize the native ODBC environment (SQLAllocEnv). The masking-immune static RUNPATH audit + the native same-arch leg + full-arch emulation validate the driver; this runtime probe is best-effort on the emulated leg."
  else
    "$conda" run -n "$envName" python "$RecipeRoot/driver_load_probe.py"
  fi
  # Minimal-base reachability gate (#563): on a Linux leg with NO system
  # krb5/libltdl (set CONDA_ASSERT_PREFIX_REACHABLE=1), prove the vendored driver
  # binds the env's OWN $CONDA_PREFIX/lib copies via the $ORIGIN climb -- not a
  # system fallthrough that would MASK an unreachable conda lib on a full agent.
  # The $ORIGIN climb makes ldd resolve krb5/gssapi/libltdl from $CONDA_PREFIX/lib
  # without LD_LIBRARY_PATH; a system or not-found binding fails the leg.
  if [ "${CONDA_ASSERT_PREFIX_REACHABLE:-}" = "1" ] && [ "$(uname -s)" = "Linux" ] && [ "$emulated_cross" = "1" ]; then
    echo "=== [py $py] reachability gate SKIPPED on the emulated cross leg (qemu-user cannot reliably run the aarch64 driver's ldd/env init); the masking-immune static RUNPATH audit is the authoritative \$ORIGIN-climb guard. ==="
  elif [ "${CONDA_ASSERT_PREFIX_REACHABLE:-}" = "1" ] && [ "$(uname -s)" = "Linux" ]; then
    echo "=== [py $py] minimal-base ldd reachability gate (driver MUST bind CONDA_PREFIX/lib) ==="
    env_prefix="$("$conda" run -n "$envName" python -c 'import os,sys; print(os.environ.get("CONDA_PREFIX") or sys.prefix)')"
    # Inspect the SAME driver variant the loader actually binds on THIS host. mssql_python
    # (GetDriverPathCpp in ddbc_bindings.cpp) selects libs/linux/<distro>/<arch> by probing
    # /etc/*-release; a blind glob instead grabs the alphabetically-first 'alpine' (musl)
    # variant, which needs libc.musl (absent on glibc) and whose libodbcinst does NOT link
    # libltdl -> a false "libltdl absent" failure. Mirror that selection exactly.
    drv="$("$conda" run -n "$envName" python -c 'import mssql_python,glob,os,platform; b=os.path.dirname(mssql_python.__file__); d=("alpine" if os.path.exists("/etc/alpine-release") else "rhel" if (os.path.exists("/etc/redhat-release") or os.path.exists("/etc/centos-release")) else "suse" if (os.path.exists("/etc/SuSE-release") or os.path.exists("/etc/SUSE-brand")) else "debian_ubuntu"); a=("arm64" if platform.machine() in ("aarch64","arm64") else "x86_64"); m=glob.glob(os.path.join(b,"..","mssql_python_odbc","libs","linux",d,a,"lib","libmsodbcsql*")); print(m[0] if m else "")')"
    if [ -z "$drv" ]; then
      echo "ERROR: [py $py] no libmsodbcsql driver found in the verify env; cannot prove reachability." >&2
      exit 1
    fi
    inst="$(dirname "$drv")/libodbcinst.so.2"
    # H2: the inspection itself MUST succeed (no `|| true`) -- a failed ldd cannot
    # be read as "reachable". Collect the combined transitive ldd of both binaries.
    ldd_all=""
    for lib in "$drv" "$inst"; do
      echo "--- ldd $(basename "$lib") ---"
      # Clear any inherited LD_LIBRARY_PATH so resolution proves the RUNPATH $ORIGIN climb
      # ALONE reaches $CONDA_PREFIX/lib -- an ambient LD_LIBRARY_PATH could otherwise satisfy
      # the sonames and MASK a broken RUNPATH.
      if ! out="$("$conda" run -n "$envName" env -u LD_LIBRARY_PATH ldd "$lib" 2>&1)"; then
        echo "$out"
        echo "ERROR: [py $py] ldd failed on $(basename "$lib"); cannot verify reachability." >&2
        exit 1
      fi
      echo "$out"
      ldd_all="$ldd_all
$out"
    done
    # H2: each required soname MUST be present AND resolve from $env_prefix -- a
    # missing (not found) or system binding FAILS closed (never passes on no-match).
    reach_fail=0
    for want in libkrb5.so libgssapi_krb5.so libltdl.so; do
      hits="$(printf '%s\n' "$ldd_all" | grep -F "$want" || true)"
      if [ -z "$hits" ]; then
        echo "MISS: required '$want' absent from ldd output (driver stopped resolving it?)." >&2
        reach_fail=1
        continue
      fi
      n_prefix=0
      n_bad=0
      while IFS= read -r line; do
        [ -n "$line" ] || continue
        resolved="$(printf '%s' "$line" | sed -nE 's/.*=> +([^ ]+).*/\1/p')"
        case "$resolved" in
          "$env_prefix"/lib/*) n_prefix=$((n_prefix + 1)); echo "OK       $line" ;;
          "") n_bad=$((n_bad + 1)); echo "NOTFOUND $line" >&2 ;;
          *) n_bad=$((n_bad + 1)); echo "SYSTEM   $line" >&2 ;;
        esac
      done <<EOF
$hits
EOF
      if [ "$n_bad" != "0" ] || [ "$n_prefix" -lt 1 ]; then
        echo "ERROR: '$want' did not resolve cleanly from $env_prefix/lib (prefix=$n_prefix, system/absent=$n_bad)." >&2
        reach_fail=1
      fi
    done
    [ "$reach_fail" = "0" ] || { echo "ERROR: [py $py] reachability gate FAILED -- a required krb5/gssapi/libltdl bound to system or was absent instead of $env_prefix/lib." >&2; exit 1; }
    echo "REACHABILITY_OK (krb5 + gssapi_krb5 + libltdl all bound from $env_prefix/lib)"
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
  # Decide whether to invoke the Encrypt=yes gate. Run it when a connection string is set,
  # OR when CONDA_TLS_PROBE_REQUIRED is anything OTHER than an explicit off/empty value. The
  # probe (tls_connect_probe.py) is the single source of truth for required-mode semantics --
  # it enforces on a truthy value and FAILS LOUD on an unrecognized one -- so the shell and
  # Python agree on truthiness (1/true/yes/on) and an ambiguous typo can't silently skip here.
  _tls_req="$(printf '%s' "${CONDA_TLS_PROBE_REQUIRED:-}" | tr '[:upper:]' '[:lower:]')"
  case "$_tls_req" in
    "" | 0 | false | no | off) _tls_req_active=0 ;;
    *) _tls_req_active=1 ;;
  esac
  if [ -n "${CONDA_TLS_PROBE_CONN:-}" ] || [ "$_tls_req_active" = "1" ]; then
    echo "=== [py $py] live Encrypt=yes TLS gate (OpenSSL backend must be reachable) ==="
    if [ "$emulated_cross" = "1" ]; then
      "$conda" run -n "$envName" python "$RecipeRoot/tls_connect_probe.py" \
        || echo "SKIP (emulated cross under QEMU binfmt): qemu-user cannot run the aarch64 driver's TLS/OpenSSL init; best-effort on the emulated leg (static RUNPATH audit covers OpenSSL layout)."
    else
      # Non-emulated: the probe is fail-closed. With a truthy CONDA_TLS_PROBE_REQUIRED a
      # missing or malformed connection string FAILS here (a typo can't silently no-op it).
      "$conda" run -n "$envName" python "$RecipeRoot/tls_connect_probe.py"
    fi
  else
    echo "=== [py $py] Encrypt=yes TLS gate SKIPPED (set CONDA_TLS_PROBE_CONN on a minimal-base leg to enable) ==="
  fi
  echo "=== [py $py] confirm resolved dependencies ==="
  "$conda" list -n "$envName" | grep -E 'azure-identity|mssql-python|openssl|krb5' || true
done

echo "==================== built conda artifacts ===================="
find "$bld" -type f \( -name 'mssql-python*.conda' -o -name 'mssql-python*.tar.bz2' \) -print
echo "CONDA_BUILD_OK"
