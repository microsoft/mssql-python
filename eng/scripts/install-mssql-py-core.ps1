<#
.SYNOPSIS
    Installs the pinned released runtime, or MSSQL_PYTHON_RS_VERSION for feature tests.
.PARAMETER TargetArch
    Validates but does not install a cross-architecture Windows wheel.
#>
param(
    [string]$OutputDir = "",
    [ValidateSet("", "x64", "arm64")]
    [string]$TargetArch = ""
)

$ErrorActionPreference = 'Stop'
$python = if ($env:PYTHON) { $env:PYTHON } else { 'python' }
$arguments = @("$PSScriptRoot\install_mssql_python_rs.py")
if ($OutputDir) { $arguments += @('--output-dir', $OutputDir) }
if ($TargetArch) { $arguments += @('--target-arch', $TargetArch) }
& $python @arguments
if ($LASTEXITCODE -ne 0) { throw 'Failed to install or validate mssql-python-rs' }
