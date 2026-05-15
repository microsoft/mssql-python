# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

<#
.SYNOPSIS
    Validates that a version file contains a stable (non-prerelease) version.

.DESCRIPTION
    Reads a single .version file and rejects any version containing
    dev, alpha, beta, or rc tags. Intended to gate official releases.

.PARAMETER VersionFile
    Path to a .version file. Defaults to eng/versions/mssql-py-core.version
    relative to the repository root.

.EXAMPLE
    # Run from repo root (uses default path):
    .\eng\scripts\validate-release-versions.ps1

    # Explicit path:
    .\eng\scripts\validate-release-versions.ps1 -VersionFile C:\work\mssql-python\eng\versions\mssql-py-core.version
#>

param(
    [string]$VersionFile
)

$ErrorActionPreference = 'Stop'

if (-not $VersionFile) {
    $repoRoot = (Resolve-Path "$PSScriptRoot\..\..").Path
    $VersionFile = Join-Path $repoRoot 'eng\versions\mssql-py-core.version'
}

if (-not (Test-Path $VersionFile)) {
    Write-Error "Version file not found: $VersionFile"
    exit 1
}

$version = (Get-Content $VersionFile -Raw).Trim()
$name = [System.IO.Path]::GetFileNameWithoutExtension($VersionFile)

if ($version -match '(dev|alpha|beta|rc)') {
    Write-Host "FAIL: $name version '$version' is a pre-release ($($Matches[1]))" -ForegroundColor Red
    Write-Error "$name version is pre-release. Official releases require stable versions."
    exit 1
}

Write-Host "OK: $name version '$version'" -ForegroundColor Green
