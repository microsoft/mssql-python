# =============================================================================
# Local "test-before-live" gate for the conda packages.
#
# Builds BOTH conda packages (driver + binding) by repackaging the currently-live
# PyPI wheels, assembles a LOCAL conda channel, installs into a clean env, and runs
# an import + (optional) live-connect smoke test. Requires NO channel onboarding/
# permission (but uses -c microsoft / -c conda-forge to resolve dependencies).
#
# Prereq: conda + conda-build + anaconda-client on PATH (Miniforge/Miniconda).
#
# Usage (PowerShell):
#   $env:DB_CONNECTION_STRING = "<conn string>"   # optional; type directly, never commit
#   ./conda/build_and_test_local.ps1
#
# When the Aug 20 release ships, bump `version` in the two meta.yaml files to
# 1.14.0 and re-run; in CI the recipes consume the signed artifacts instead of PyPI.
# =============================================================================

param(
    [string]$PyVer = "3.12",
    [string]$BuildEnv = "mssql-condabuild",
    [string]$TestEnv = "mssql-conda-test"
)

$ErrorActionPreference = "Stop"
function Assert-LastExit($msg) { if ($LASTEXITCODE -ne 0) { throw "FAILED: $msg (exit $LASTEXITCODE)" } }

$RepoRoot = Split-Path -Parent $PSScriptRoot
# Build in a space-free path — conda-build dislikes spaces (repo lives under OneDrive).
$BldDir = Join-Path $env:TEMP "mssql-conda-bld"
$env:CONDA_BLD_PATH = $BldDir
New-Item -ItemType Directory -Force -Path $BldDir | Out-Null

Write-Host "== 1. Create build env ($BuildEnv, python=$PyVer) ==" -ForegroundColor Cyan
conda create -y -n $BuildEnv "python=$PyVer" conda-build anaconda-client
Assert-LastExit "create build env"

Write-Host "== 2. Build driver package FIRST (it is a dependency) ==" -ForegroundColor Cyan
conda run -n $BuildEnv conda build "$RepoRoot\conda\mssql-python-odbc" --no-test --output-folder $BldDir
Assert-LastExit "conda build mssql-python-odbc"

Write-Host "== 3. Build the binding package ==" -ForegroundColor Cyan
conda run -n $BuildEnv conda build "$RepoRoot\conda\mssql-python" --no-test --output-folder $BldDir
Assert-LastExit "conda build mssql-python"

Write-Host "== 4. Index the local channel ==" -ForegroundColor Cyan
conda run -n $BuildEnv conda index $BldDir
Assert-LastExit "conda index"

Write-Host "== 5. Clean-env install from LOCAL + microsoft + conda-forge ==" -ForegroundColor Cyan
# Mirror the real user install path (-c microsoft). The driver companion resolves from
# our LOCAL channel; azure-core/azure-identity/msal resolve from the lean `microsoft`
# channel. We deliberately do NOT let conda-forge own azure-core: its recipe over-declares
# flask/six as runtime deps, dragging in celery/boto3/botocore (~9 MB) -- see
# conda-forge/azure-core-feedstock#71. --strict-channel-priority keeps the locally built
# packages authoritative and lets `microsoft` own only the azure-* SDK packages.
conda create -y -n $TestEnv "python=$PyVer" -c "file:///$BldDir" -c microsoft -c conda-forge --strict-channel-priority --override-channels mssql-python
Assert-LastExit "install mssql-python from local channel"

Write-Host "== 6a. Functional: import + version ==" -ForegroundColor Cyan
conda run -n $TestEnv python -c "import mssql_python; print('import OK, version =', mssql_python.__version__)"
Assert-LastExit "import mssql_python"

Write-Host "== 6b. Assert the driver companion is present (came from conda, not system) ==" -ForegroundColor Cyan
conda run -n $TestEnv python -c "import mssql_python_odbc, os; print('driver companion OK:', mssql_python_odbc.__version__); print('located at:', os.path.dirname(mssql_python_odbc.__file__))"
Assert-LastExit "import mssql_python_odbc companion"

Write-Host "== 6c. DB-less driver-load proof (real ODBC driver must load, not just the shim) ==" -ForegroundColor Cyan
conda run -n $TestEnv python "$RepoRoot\conda\driver_load_probe.py"
Assert-LastExit "driver-load proof"

if ($env:DB_CONNECTION_STRING) {
    Write-Host "== 6d. Live connect smoke (SELECT 1) ==" -ForegroundColor Cyan
    conda run -n $TestEnv python -c "import os,mssql_python; c=mssql_python.connect(os.environ['DB_CONNECTION_STRING']); print('SELECT 1 =>', c.cursor().execute('SELECT 1').fetchone())"
    Assert-LastExit "live connect smoke"
}
else {
    Write-Host "== 6d. SKIPPED live connect (set DB_CONNECTION_STRING to enable) ==" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "RESULT: PASS — conda packages build, resolve together, and import cleanly." -ForegroundColor Green
Write-Host "Local channel: $BldDir" -ForegroundColor Green
Write-Host "When permission lands, publish is one step: anaconda upload --user microsoft <pkgs>" -ForegroundColor Green
