<#
.SYNOPSIS
    Installs the mssql-mock-tds in-process TDS server (plus cryptography, used to
    mint the throwaway TLS identity it needs) from the public mssql-rs_Public
    Azure Artifacts PyPI feed so that tests/test_024_mock_tds_fedauth.py runs in
    CI instead of skipping.

.DESCRIPTION
    The package is currently published only as a dev pre-release on a sandbox
    feed. To keep pipeline legs green where no compatible wheel exists, a failed
    install is reported as a pipeline *warning* (not an error): the test then
    skips cleanly.

    The package version is read from eng/versions/mssql-mock-tds.version.

.PARAMETER FeedUrl
    The PyPI-format index URL for the mssql-rs_Public feed. Public -- no auth.
#>

param(
    [string]$FeedUrl = "https://pkgs.dev.azure.com/sqlclientdrivers/public/_packaging/mssql-rs_Public/pypi/simple/"
)

# Best-effort install: never fail the pipeline leg over a sandbox-feed dependency.
$ErrorActionPreference = 'Continue'
$ScriptDir = $PSScriptRoot
$RepoRoot = (Get-Item "$ScriptDir\..\..").FullName

function Write-PipelineWarning([string]$Message) {
    # Surface as an Azure DevOps pipeline warning while keeping the leg green.
    Write-Host "##vso[task.logissue type=warning]$Message"
    Write-Host "WARNING: $Message"
}

$versionFile = Join-Path $RepoRoot "eng\versions\mssql-mock-tds.version"
if (-not (Test-Path $versionFile)) {
    Write-PipelineWarning "Version file not found: $versionFile -- skipping mssql-mock-tds install."
    exit 0
}
$packageVersion = (Get-Content $versionFile -Raw).Trim()
if (-not $packageVersion) {
    Write-PipelineWarning "Version file is empty: $versionFile -- skipping mssql-mock-tds install."
    exit 0
}

Write-Host "=== Install mssql-mock-tds ($packageVersion) from $FeedUrl ==="

& python -m pip install --extra-index-url $FeedUrl "mssql-mock-tds==$packageVersion" cryptography
if ($LASTEXITCODE -ne 0) {
    Write-PipelineWarning "Could not install mssql-mock-tds==$packageVersion (no compatible wheel on this platform or feed unavailable) -- the mock TDS test will skip."
    exit 0
}

Write-Host "Verifying import..."
& python -c "import mssql_mock_tds; print('mssql_mock_tds import OK')"
if ($LASTEXITCODE -ne 0) {
    Write-PipelineWarning "mssql-mock-tds installed but failed to import -- the mock TDS test will skip."
    exit 0
}

Write-Host "=== mssql-mock-tds installed successfully ==="
exit 0
