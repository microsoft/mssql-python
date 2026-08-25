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

.PARAMETER Package
  Which package(s) to build:
    'all'     - companion (ONCE) + binding (per-Python)         [default]
    'odbc'    - ONLY the Python-agnostic companion, built ONCE (ODBC_BuildAll stage);
                validated by importing it under each target Python.
    'binding' - ONLY the per-Python binding; the companion is seeded from
                -DriverCondaDir into the local channel so the version-locked
                `mssql-python-odbc ==<ver>` dependency resolves for the solve/import.

.PARAMETER DriverCondaDir
  Folder holding a prebuilt companion .conda (mssql-python-odbc) under a <subdir>/
  layout, to seed into the local channel (binding mode) instead of rebuilding the
  companion per-Python. Empty in 'all'/'odbc' mode.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$WheelsDir,
    [Parameter(Mandatory = $true)][string]$RecipeRoot,
    [Parameter(Mandatory = $true)][string]$OutputDir,
    [Parameter(Mandatory = $true)][string]$MssqlPythonVersion,
    [Parameter(Mandatory = $true)][string]$OdbcVersion,
    [string]$PythonVersions = "",
    [string]$CondaSubdir = "",
    [ValidateSet('all', 'odbc', 'binding')]
    [string]$Package = 'all',
    [string]$DriverCondaDir = ""
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
    # Pin Miniforge to a specific release for reproducible, supply-chain-safe builds
    # (never 'latest', which floats). Override with MINIFORGE_VERSION. When
    # MINIFORGE_SHA256 is set (this installer's checksum for the pinned version) the
    # download is verified BEFORE it is executed -- set it in the pipeline to make
    # integrity checking mandatory.
    $mfver = if ($env:MINIFORGE_VERSION) { $env:MINIFORGE_VERSION } else { '26.3.2-3' }
    $url = "https://github.com/conda-forge/miniforge/releases/download/$mfver/Miniforge3-Windows-x86_64.exe"
    Write-Host "Downloading pinned Miniforge ${mfver}: $url"
    Invoke-WebRequest -Uri $url -OutFile $installer
    if ($env:MINIFORGE_SHA256) {
        $actual = (Get-FileHash -Algorithm SHA256 -Path $installer).Hash
        if ($actual -ne $env:MINIFORGE_SHA256) {
            Write-Error "Miniforge installer SHA256 mismatch: expected '$($env:MINIFORGE_SHA256)', got '$actual'."
            exit 1
        }
        Write-Host "Miniforge installer SHA256 verified."
    }
    else {
        Write-Host "WARNING: MINIFORGE_SHA256 not set -- skipping installer integrity check. Set it in the pipeline to the pinned checksum for $mfver."
    }
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
Write-Host "=== installing conda-build (<26) ==="
& $conda install -y -n base "conda-build<26"
Assert-LastExit "conda install conda-build<26"

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

if ($Package -eq 'odbc') {
    Write-Host "NOTE: -Package odbc is a no-op in the self-contained model (the ODBC payload"
    Write-Host "is vendored INTO mssql-python; there is no separate companion). Nothing to build."
}
else {
    foreach ($py in $pyvers) {
        Write-Host "=== [py $py] build mssql-python (self-contained: vendors the ODBC payload) ==="
        if ($CondaSubdir -eq 'win-arm64') {
            # The win-arm64 host env's python 3.12/3.13 only exists on Anaconda `defaults`
            # (conda-forge ships win-arm64 python 3.14 only), so add defaults ahead of
            # conda-forge for the host-env solve.
            & $conda build $bindRecipe --python $py --no-test --no-anaconda-upload --output-folder $bld -c defaults -c conda-forge
        }
        else {
            & $conda build $bindRecipe --python $py --no-test --no-anaconda-upload --output-folder $bld
        }
        Assert-LastExit "conda build mssql-python (py $py)"
    }
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
& $conda run -n base python -m pip install --quiet --disable-pip-version-check zstandard
& $conda run -n base python $auditScript --root $bld
Assert-LastExit "RUNPATH self-containment audit"

# ---------------------------------------------------------------------------
# 7. Validate: solve a fresh env from the local channel and import the package.
#    Proves azure-identity + the folded-in openssl/krb5 deps resolve AND that the
#    repackaged native binding imports with its vendored ODBC payload (driver loads
#    at import).
# ---------------------------------------------------------------------------
$localChannel = "file:///" + ($bld -replace '\\', '/')
if ($Package -eq 'odbc') {
    Write-Host "NOTE: -Package odbc is a no-op in the self-contained model; nothing to validate."
}
else {
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
    # A win-arm64 package is CROSS-built on the x64 agent: its deps resolve (from Anaconda
    # defaults + the microsoft noarch azure-identity/msal), but the arm64 Python cannot
    # execute here, so the runtime import is best-effort/auto-skipped -- the same contract
    # as the osx-arm64 cross-build in build.sh (a static arch audit stands in). Native legs
    # stay fully blocking.
    $crossBestEffort = ($CondaSubdir -eq 'win-arm64')
    foreach ($py in $pyvers) {
        $sub = if ($CondaSubdir) { $CondaSubdir -replace '-', '_' } else { 'native' }
        $envName = "verify_${sub}_" + ($py -replace '\.', '')
        & $conda env remove -y -n $envName 2>$null
        Write-Host "=== [py $py] create verify env from local channel ==="
        if ($CondaSubdir -eq 'win-arm64') {
            # win-arm64: azure-identity/msal are noarch on `microsoft`; their native dep
            # cryptography (plus python/vc14_runtime/pyodbc) is on Anaconda `defaults`, NOT
            # conda-forge. Drop --strict-channel-priority so a dep split across microsoft +
            # defaults still solves.
            & $conda create -y -n $envName -c $localChannel -c microsoft -c defaults --override-channels "python=$py" mssql-python
        }
        else {
            # -c microsoft (ahead of conda-forge) so azure-core/azure-identity/msal resolve from the
            # lean `microsoft` channel, NOT conda-forge whose azure-core recipe over-declares flask/six
            # -> celery/boto3/botocore (~9 MB); see conda-forge/azure-core-feedstock#71.
            # --strict-channel-priority keeps the freshly built local package authoritative.
            & $conda create -y -n $envName -c $localChannel -c microsoft -c conda-forge --strict-channel-priority --override-channels "python=$py" mssql-python
        }
        if ($LASTEXITCODE -ne 0) {
            if ($crossBestEffort) {
                Write-Warning "[py $py] win-arm64 verify solve failed (best-effort cross leg); the package was already built + staged. Investigate dep availability on defaults/microsoft for python=$py."
                continue
            }
            Assert-LastExit "conda create verify env (py $py)"
        }

        # Can the freshly built package's Python EXECUTE on this host? On the win-arm64
        # cross leg it cannot (arm64 on x64), so skip the runtime import -- exactly like
        # the osx-arm64 cross-build. Any OTHER non-runnable target is a real failure.
        & $conda run -n $envName python -c "import sys" 2>$null
        if ($LASTEXITCODE -ne 0) {
            if ($crossBestEffort) {
                Write-Host "=== [py $py] win-arm64 cross on x64: target Python not executable; deps resolved but skipping runtime import (static arm64-slice audit stands in). ==="
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
}

Write-Host "==================== built conda artifacts ===================="
Get-ChildItem -Path $bld -Recurse -Include *.conda, *.tar.bz2 |
Where-Object { $_.Name -like 'mssql-python*' } |
ForEach-Object { Write-Host "  $($_.FullName)" }
Write-Host "CONDA_BUILD_OK"
