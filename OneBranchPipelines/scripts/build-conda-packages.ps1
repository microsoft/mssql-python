<#
.SYNOPSIS
  Build and validate the self-contained mssql-python conda package (which vendors the
  ODBC Driver 18 payload) from prebuilt (ESRP-signed) wheels, fully offline.

.DESCRIPTION
  Repackages the wheels produced by build definition 2199 into conda packages using
  conda-build, then proves the recipes are correct by solving a fresh environment
  from the freshly built local channel and importing both packages.

  Runs on the OneBranch Windows 1ES pool (or locally). Builds the win_amd64 slice
  for every Python version detected among the mssql_python wheels. Other platforms
  (linux-*, osx-*, win_arm64) must be built on matching agents in a follow-up, the
  same way the wheel build matrix fans out.

.PARAMETER WheelsDir
  Directory containing ALL downloaded wheels (both packages, all platforms/pythons).

.PARAMETER RecipeRoot
  Path to the repo's conda/ directory (contains mssql-python/ and mssql-python-odbc/).

.PARAMETER OutputDir
  Space-free working/output directory (conda croot, Miniforge install, built pkgs).

.PARAMETER MssqlPythonVersion
  Version to stamp on the mssql-python conda package (e.g. 1.13.0).

.PARAMETER OdbcVersion
  Version to stamp on the mssql-python-odbc conda package (e.g. 18.6.2.1).

.PARAMETER PythonVersions
  Optional comma-separated list (e.g. "3.11,3.12"). Empty = auto-detect from wheels.

.PARAMETER CondaSubdir
  Optional target subdir (e.g. win-arm64) to CROSS-target via CONDA_SUBDIR instead of
  the host's native subdir. Empty = build the host's native subdir (win-64). Cross-
  targeting only yields a VALIDATED package when the host can run the target Python for
  the import check, so it is left unset for the native win-64 leg.

#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$WheelsDir,
    [Parameter(Mandatory = $true)][string]$RecipeRoot,
    [Parameter(Mandatory = $true)][string]$OutputDir,
    [Parameter(Mandatory = $true)][string]$MssqlPythonVersion,
    [Parameter(Mandatory = $true)][string]$OdbcVersion,
    [string]$PythonVersions = "",
    [string]$CondaSubdir = ""
)

$ErrorActionPreference = 'Stop'

function Assert-LastExit([string]$Message) {
    if ($LASTEXITCODE -ne 0) {
        Write-Error "FAILED (exit $LASTEXITCODE): $Message"
        exit 1
    }
}

Write-Host "==================== conda build inputs ===================="
Write-Host "WheelsDir          : $WheelsDir"
Write-Host "RecipeRoot         : $RecipeRoot"
Write-Host "OutputDir          : $OutputDir"
Write-Host "MssqlPythonVersion : $MssqlPythonVersion"
Write-Host "OdbcVersion        : $OdbcVersion"
Write-Host "PythonVersions     : $(if ($PythonVersions) { $PythonVersions } else { '(auto-detect)' })"
Write-Host "CondaSubdir        : $(if ($CondaSubdir) { $CondaSubdir } else { '(native)' })"
Write-Host "============================================================"

New-Item -ItemType Directory -Force -Path $OutputDir | Out-Null
$bld = Join-Path $OutputDir 'bld'
New-Item -ItemType Directory -Force -Path $bld | Out-Null

# ---------------------------------------------------------------------------
# 1. Locate conda, or install Miniforge3 (conda-forge defaults, no license issues)
# ---------------------------------------------------------------------------
$conda = (Get-Command conda -ErrorAction SilentlyContinue).Source
if (-not $conda) {
    Write-Host "=== conda not found on PATH; installing Miniforge3 ==="
    $installer = Join-Path $OutputDir 'Miniforge3-Windows-x86_64.exe'
    $forgeDir = Join-Path $OutputDir 'miniforge'
    # Pin Miniforge to a specific release (never 'latest', which floats) and verify its
    # SHA256 BEFORE executing. The expected hash is NOT hard-coded in source: prefer an
    # explicit MINIFORGE_SHA256 (a pipeline variable = strongest, out-of-source), else
    # verify against the release's OWN published <installer>.sha256 sidecar. The installer
    # is never executed unverified.
    $mfver = if ($env:MINIFORGE_VERSION) { $env:MINIFORGE_VERSION } else { '26.3.2-3' }
    $mfName = "Miniforge3-$mfver-Windows-x86_64.exe"
    $url = "https://github.com/conda-forge/miniforge/releases/download/$mfver/$mfName"
    Write-Host "Downloading pinned Miniforge ${mfver}: $url"
    Invoke-WebRequest -Uri $url -OutFile $installer
    if ($env:MINIFORGE_SHA256) {
        $expected = $env:MINIFORGE_SHA256
    }
    else {
        $sumFile = "$installer.sha256"
        Invoke-WebRequest -Uri "$url.sha256" -OutFile $sumFile
        $expected = [regex]::Match((Get-Content -Raw $sumFile), '[0-9a-fA-F]{64}').Value
    }
    if (-not $expected) {
        Write-Error "Could not determine the expected SHA256 for $mfName."
        exit 1
    }
    $actual = (Get-FileHash -Algorithm SHA256 -Path $installer).Hash
    if ($actual -ne $expected) {
        Write-Error "Miniforge installer SHA256 mismatch: expected '$expected', got '$actual'."
        exit 1
    }
    Write-Host "Miniforge installer SHA256 verified."
    # NSIS silent install; /D (target dir) MUST be last and unquoted.
    Start-Process -FilePath $installer -ArgumentList '/S', '/InstallationType=JustMe', '/AddToPath=0', "/D=$forgeDir" -Wait
    $conda = Join-Path $forgeDir 'Scripts\conda.exe'
}
if (-not (Test-Path $conda)) {
    Write-Error "conda not available at '$conda' after install attempt."
    exit 1
}
Write-Host "Using conda: $conda"
& $conda --version
Assert-LastExit "conda --version"

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
Write-Host "=== creating dedicated conda-build env (conda_builder: conda-build<26) ==="
# Use a DEDICATED env instead of `install -n base`: a pre-installed conda whose base is
# pinned to a python that no conda-build<26 supports (e.g. 3.14) makes a base install
# UNSOLVABLE -- a fresh env lets conda pick a python conda-build<26 supports, independent
# of the base pin (the .sh port does the same). zstandard is folded in here so the RUNPATH
# audit (step 6b) reads the .conda payload from this same env, with no pip install into base.
$condaBuildEnv = 'conda_builder'
# Pre-remove so a reused / self-hosted agent (or a system conda on PATH) reruns cleanly.
# `conda env remove` on an ABSENT env writes to stderr, which the script-wide
# ErrorActionPreference='Stop' escalates to a terminating error -- switch to 'Continue' for
# just this best-effort step (the bash port's `|| true`), then restore 'Stop'.
$ErrorActionPreference = 'Continue'
& $conda env remove -y -n $condaBuildEnv 2>$null
$ErrorActionPreference = 'Stop'
& $conda create -y -n $condaBuildEnv -c conda-forge --override-channels "conda-build<26" zstandard
Assert-LastExit "conda create $condaBuildEnv (conda-build<26 + zstandard)"

# ---------------------------------------------------------------------------
# 3. Determine which Python versions to build (win_amd64 mssql_python wheels)
# ---------------------------------------------------------------------------
if ([string]::IsNullOrWhiteSpace($PythonVersions)) {
    $pyvers = Get-ChildItem -Path $WheelsDir -Filter 'mssql_python-*win_amd64.whl' |
    ForEach-Object { if ($_.Name -match 'cp3(\d+)') { "3.$($Matches[1])" } } |
    Sort-Object -Unique
}
else {
    $pyvers = $PythonVersions.Split(',') | ForEach-Object { $_.Trim() } | Where-Object { $_ }
}
if (-not $pyvers) {
    Write-Error "No win_amd64 mssql_python wheels found in '$WheelsDir' to determine Python versions."
    exit 1
}
Write-Host "Building conda packages for Python versions: $($pyvers -join ', ')"

# ---------------------------------------------------------------------------
# 4. Export the environment consumed by the recipes (jinja + build scripts)
# ---------------------------------------------------------------------------
$env:WHEELS_DIR = $WheelsDir
$env:MSSQL_PYTHON_VERSION = $MssqlPythonVersion
$env:MSSQL_ODBC_VERSION = $OdbcVersion

# CROSS-target a non-native subdir when requested: conda-build and the verify env's
# `conda create` both honor CONDA_SUBDIR, so the packages are stamped for $CondaSubdir
# and the import check runs the target Python (via Rosetta 2 / QEMU on an emulating
# host). Empty = build the host's native subdir.
if ($CondaSubdir) {
    $env:CONDA_SUBDIR = $CondaSubdir
    Write-Host "Cross-targeting conda subdir: CONDA_SUBDIR=$($env:CONDA_SUBDIR)"
    if ($CondaSubdir -eq 'win-arm64') {
        # win-arm64 deps (python 3.12-3.14, cryptography, vc14_runtime, pyodbc) live on
        # Anaconda `defaults`, NOT conda-forge (which only ships win-arm64 python 3.14 and
        # no cryptography). Auto-accept the defaults Terms of Service so the unattended
        # conda-build host-env solve and the verify solve never block on a ToS prompt.
        $env:CONDA_PLUGINS_AUTO_ACCEPT_TOS = 'yes'
        Write-Host "win-arm64: CONDA_PLUGINS_AUTO_ACCEPT_TOS=yes (Anaconda defaults supplies the win-arm64 deps)"
    }
}

# ---------------------------------------------------------------------------
# 5. Build the self-contained mssql-python package (per Python). The recipe vendors
#    the ODBC Driver 18 payload by extracting the mssql-python-odbc wheel into its
#    own site-packages, so there is NO separate companion package to build.
# ---------------------------------------------------------------------------
$bindRecipe = Join-Path $RecipeRoot 'mssql-python'

foreach ($py in $pyvers) {
    Write-Host "=== [py $py] build mssql-python (self-contained: vendors the ODBC payload) ==="
    if ($CondaSubdir -eq 'win-arm64') {
        # The win-arm64 host env's python 3.12/3.13 only exists on Anaconda `defaults`
        # (conda-forge ships win-arm64 python 3.14 only), so add defaults ahead of
        # conda-forge for the host-env solve.
        & $conda run -n $condaBuildEnv conda-build $bindRecipe --python $py --no-test --no-anaconda-upload --output-folder $bld -c defaults -c conda-forge
    }
    else {
        & $conda run -n $condaBuildEnv conda-build $bindRecipe --python $py --no-test --no-anaconda-upload --output-folder $bld
    }
    Assert-LastExit "conda build mssql-python (py $py)"
}

# ---------------------------------------------------------------------------
# 6. Make the local output folder a VALID conda channel.
#    conda-build --output-folder already wrote $bld\<subdir>\repodata.json for the
#    platform we built, but a conda channel is only valid if it ALSO carries
#    noarch\repodata.json (even empty) -- otherwise `conda create -c file://$bld`
#    fails with "UnavailableInvalidChannel ... must contain noarch/repodata.json".
#    Create it directly rather than via `conda index`, whose subcommand is absent
#    from miniforge (it moved to the standalone conda-index package).
# ---------------------------------------------------------------------------
$noarchDir = Join-Path $bld 'noarch'
New-Item -ItemType Directory -Force -Path $noarchDir | Out-Null
$noarchRepo = Join-Path $noarchDir 'repodata.json'
if (-not (Test-Path $noarchRepo)) {
    '{"info":{"subdir":"noarch"},"packages":{},"packages.conda":{}}' | Set-Content -NoNewline -Encoding ascii $noarchRepo
}

# ---------------------------------------------------------------------------
# 6b. Masking-immune RUNPATH audit of the freshly built packages (#563).
# ---------------------------------------------------------------------------
# BLOCKING static gate that reads the ELF RUNPATH bytes of the vendored Linux ODBC
# binaries and requires the relative $ORIGIN climb. win-64 packages carry no ELF
# payload so this is a clean no-op here, but it is wired on EVERY leg so a Linux
# package can never reach publish without the #563 self-containment being proven.
$auditScript = Join-Path (Split-Path $RecipeRoot -Parent) 'eng/scripts/audit_bundled_binaries.py'
if (-not (Test-Path $auditScript)) {
    Write-Error "RUNPATH audit script not found at $auditScript"
    exit 1
}
Write-Host "=== RUNPATH self-containment audit (eng/scripts/audit_bundled_binaries.py) ==="
# zstandard already lives in $condaBuildEnv (installed with conda-build above), so the
# audit reads the .conda payload from that env -- no separate pip install into base.
& $conda run -n $condaBuildEnv python $auditScript --root $bld
Assert-LastExit "RUNPATH self-containment audit"

# ---------------------------------------------------------------------------
# 6c. PE machine-type assert for win-arm64 (the Windows twin of the 6b ELF audit).
# ---------------------------------------------------------------------------
# The arm64 runtime import is SKIPPED on the x64 cross host, so without this the
# package's architecture is trusted purely from the wheel filename. Read the PE COFF
# Machine field of every vendored .pyd/.dll in the freshly built win-arm64 package and
# fail if any is not ARM64 -- so a mislabeled/mis-built wheel can never ship x64
# binaries inside a win-arm64 package. No-op on every non-win-arm64 leg.
if ($CondaSubdir -eq 'win-arm64') {
    $peCheck = Join-Path (Split-Path $RecipeRoot -Parent) 'eng/scripts/assert_pe_machine.py'
    if (-not (Test-Path $peCheck)) {
        Write-Error "PE machine-type assert script not found at $peCheck"
        exit 1
    }
    Write-Host "=== win-arm64 PE machine-type assert (vendored .pyd/.dll must be ARM64) ==="
    & $conda run -n $condaBuildEnv python $peCheck --root $bld --subdir win-arm64
    Assert-LastExit "win-arm64 PE machine-type assert"
}

# ---------------------------------------------------------------------------
# 7. Validate: solve a fresh env from the local channel and import the package.
#    Proves azure-identity + the folded-in openssl/krb5 deps resolve AND that the
#    repackaged native binding imports with its vendored ODBC payload (driver loads
#    at import).
# ---------------------------------------------------------------------------
$localChannel = "file:///" + ($bld -replace '\\', '/')
# Run the verify imports from a NEUTRAL dir: `python -c` prepends the cwd to
# sys.path, and the pipeline runs from the repo checkout whose in-tree
# mssql_python\ (source, no compiled .pyd) would shadow the conda-installed
# package -> "No ddbc_bindings module found". $OutputDir is outside the repo.
Set-Location $OutputDir
# The verify loop drives native `conda` commands (env remove / run) that legitimately
# write to stderr on their NON-FATAL paths: `conda env remove` on an absent env (fresh
# agent / first run), and `conda run` when the cross-built arm64 Python can't execute on
# this x64 host. Under the script-wide $ErrorActionPreference='Stop', PowerShell escalates
# ANY native stderr write to a terminating NativeCommandError, aborting the leg BEFORE the
# exit-code checks below (this is what failed both win-64 and win-arm64 at `conda env
# remove`). Switch to 'Continue' for the verify section and gate control flow on
# $LASTEXITCODE / Assert-LastExit instead -- the same intent as the bash port's `|| true`.
$ErrorActionPreference = 'Continue'
# A win-arm64 package is CROSS-built on the x64 agent. The dependency SOLVE runs on the
# x64 host and does NOT need the arm64 interpreter, so it stays BLOCKING on every leg (an
# unsolvable win-arm64 graph must fail the build, never slip through to publish and then
# break the user's `conda install`). ONLY the runtime import is best-effort here, because
# the arm64 Python cannot execute on x64 -- the same contract as the osx-arm64 cross-build
# in build.sh (a static arch audit stands in). Native legs stay fully blocking end-to-end.
$crossBestEffort = ($CondaSubdir -eq 'win-arm64')
foreach ($py in $pyvers) {
    $sub = if ($CondaSubdir) { $CondaSubdir -replace '-', '_' } else { 'native' }
    $envName = "verify_${sub}_" + ($py -replace '\.', '')
    & $conda env remove -y -n $envName 2>$null
    Write-Host "=== [py $py] create verify env from local channel ==="
    if ($CondaSubdir -eq 'win-arm64') {
        # BLOCKING solvability gate: --dry-run resolves the FULL win-arm64 dependency graph
        # on the x64 host (CONDA_SUBDIR=win-arm64 pins the target subdir) WITHOUT linking,
        # post-link scripts, or executing the arm64 interpreter -- a pure "is this
        # installable?" check that runs anywhere. An unsolvable graph fails the build here
        # instead of shipping a package that breaks the user's `conda install`.
        # win-arm64 deps: azure-identity/msal are noarch on `microsoft`; cryptography +
        # python/vc14_runtime/pyodbc are on Anaconda `defaults`, NOT conda-forge -- so no
        # --strict-channel-priority (the graph legitimately splits across microsoft+defaults).
        & $conda create --dry-run -n $envName -c $localChannel -c microsoft -c defaults --override-channels "python=$py" mssql-python
        Assert-LastExit "win-arm64 --dry-run solve (py $py)"
        # Real env is BEST-EFFORT: only a real arm64 host can create+run it. The pipeline
        # cross-builds on x64 (arm64 Python cannot execute); there the PE-machine assert
        # (step 6c) + the static arm64-slice audit enforce arch/correctness, so a real-create
        # failure here is not fatal -- we skip the runtime import.
        & $conda create -y -n $envName -c $localChannel -c microsoft -c defaults --override-channels "python=$py" mssql-python 2>$null
        if ($LASTEXITCODE -ne 0) {
            Write-Host "=== [py $py] win-arm64: SOLVES (dry-run OK); real env not creatable on this x64 host -- arch enforced by the PE assert + static audit, skipping runtime import. ==="
            continue
        }
    }
    else {
        # -c microsoft (ahead of conda-forge) so azure-core/azure-identity/msal resolve from the
        # lean `microsoft` channel, NOT conda-forge whose azure-core recipe over-declares flask/six
        # -> celery/boto3/botocore (~9 MB); see conda-forge/azure-core-feedstock#71.
        # --strict-channel-priority keeps the freshly built local package authoritative.
        & $conda create -y -n $envName -c $localChannel -c microsoft -c conda-forge --strict-channel-priority --override-channels "python=$py" mssql-python
        # The full create IS the solve on a native leg -- keep it BLOCKING.
        Assert-LastExit "conda create verify env (py $py)"
    }

    # Can the freshly built package's Python EXECUTE on this host? On the win-arm64
    # cross leg it cannot (arm64 on x64), so skip the runtime import -- exactly like
    # the osx-arm64 cross-build. Any OTHER non-runnable target is a real failure.
    & $conda run -n $envName python -c "import sys" 2>$null
    if ($LASTEXITCODE -ne 0) {
        if ($crossBestEffort) {
            Write-Host "=== [py $py] win-arm64 cross on x64: target Python not executable; deps SOLVED (blocking) but skipping runtime import (static arm64-slice audit stands in). ==="
            continue
        }
        Write-Error "target Python for CONDA_SUBDIR=$CondaSubdir is not executable on this host, and this is NOT the win-arm64 cross-build. Refusing to silently skip validation."
        exit 1
    }

    Write-Host "=== [py $py] import mssql_python + prove the vendored ODBC payload is present ==="
    & $conda run -n $envName python -c "import mssql_python; print('BINDING_OK', mssql_python.__version__)"
    Assert-LastExit "import mssql_python (py $py)"
    & $conda run -n $envName python -c "import mssql_python_odbc; print('ODBC_PAYLOAD_OK', mssql_python_odbc.__version__)"
    Assert-LastExit "import mssql_python_odbc (py $py)"

    Write-Host "=== [py $py] DB-less driver-load proof (real ODBC driver must load, not just the shim) ==="
    & $conda run -n $envName python (Join-Path $RecipeRoot 'driver_load_probe.py')
    Assert-LastExit "driver-load proof (py $py)"

    Write-Host "=== [py $py] confirm resolved dependencies ==="
    & $conda list -n $envName | Select-String -Pattern 'azure-identity|mssql-python|openssl|krb5'
}

Write-Host "==================== built conda artifacts ===================="
Get-ChildItem -Path $bld -Recurse -Include *.conda, *.tar.bz2 |
Where-Object { $_.Name -like 'mssql-python*' } |
ForEach-Object { Write-Host "  $($_.FullName)" }
Write-Host "CONDA_BUILD_OK"
# Reset the process exit code to 0 ONLY on the win-arm64 cross leg. There, the last native command
# in the verify loop is the runnable-check `conda run` that INTENTIONALLY fails -- the arm64 Python
# cannot launch on the x64 host (exit 216 = ERROR_EXE_MACHINE_TYPE_MISMATCH) -- so we skip the
# import and `continue`, leaving a non-zero $LASTEXITCODE that would otherwise fail the leg even
# though every package built and its deps SOLVED. Scoping the reset to win-arm64 means a future
# post-check on a native leg can never be silently masked. We must NOT use PowerShell `exit 0`: the
# .yml step calls this script with `& ...` in-session, so `exit` would terminate the whole step
# BEFORE it stages the packages. A trailing SUCCESSFUL native command resets $LASTEXITCODE and
# returns control to the caller (so staging runs). Any REAL failure already exited 1 via
# Assert-LastExit / the explicit `exit 1` paths above.
if ($CondaSubdir -eq 'win-arm64') {
    cmd /c "exit 0"
}
