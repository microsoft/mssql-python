# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

<#
.SYNOPSIS
    Gates Official builds and releases on an exact public PyPI runtime release.
.DESCRIPTION
    BuildOnly rejects overrides and non-release source pins without network access.
    Release validation additionally requires the selected artifact's transport
    receipt and verifies the exact version exists on public PyPI before ESRP.
#>
param(
    [string]$VersionFile,
    [string]$ReceiptFile,
    [switch]$BuildOnly
)

$ErrorActionPreference = 'Stop'

if (-not $VersionFile) {
    $VersionFile = Join-Path $PSScriptRoot '../../eng/versions/mssql-python-rs.version'
}

if (-not [string]::IsNullOrWhiteSpace($env:MSSQL_PYTHON_RS_VERSION)) {
    throw 'Official builds and releases forbid MSSQL_PYTHON_RS_VERSION overrides, including stable versions.'
}

$version = (Get-Content -LiteralPath $VersionFile -Raw).Trim()
if ($version -cnotmatch '^[0-9]+\.[0-9]+\.[0-9]+$') {
    throw "Official builds and releases require an exact stable X.Y.Z runtime version, got '$version'."
}

if (-not $BuildOnly) {
    if (-not $ReceiptFile) {
        $ReceiptFile = Join-Path (Split-Path -Parent $VersionFile) 'transport.json'
    }
    $receipt = Get-Content -LiteralPath $ReceiptFile -Raw | ConvertFrom-Json
    if ($receipt.version_override -isnot [bool] -or $receipt.version_override) {
        throw 'Official releases require a receipt with version_override=false.'
    }
    if ($receipt.distribution_version -cne $version) {
        throw 'The staged runtime version and transport receipt disagree.'
    }
    if ($receipt.index_url -cnotmatch '^https://pypi\.org/simple/?$') {
        throw 'Official releases require the public PyPI runtime source.'
    }
    if (-not $receipt.wheel_sha256 -or @($receipt.wheel_sha256.PSObject.Properties).Count -eq 0) {
        throw 'The runtime receipt must record wheel SHA256 hashes.'
    }
    foreach ($hash in $receipt.wheel_sha256.PSObject.Properties) {
        if ($hash.Name -notlike '*.whl' -or $hash.Value -cnotmatch '^[a-f0-9]{64}$') {
            throw 'The runtime receipt contains an invalid wheel SHA256 entry.'
        }
    }

    $release = Invoke-RestMethod -Uri "https://pypi.org/pypi/mssql-python-rs/$version/json" -Method Get
    if ($release.info.version -cne $version -or
        $release.info.name -notmatch '^mssql[-_]python[-_]rs$' -or
        @($release.urls | Where-Object { $_.packagetype -eq 'bdist_wheel' -and -not $_.yanked }).Count -eq 0) {
        throw "mssql-python-rs==$version has no matching public PyPI wheel release."
    }
}

Write-Host "OK: mssql-python-rs==$version is eligible for Official $(if ($BuildOnly) { 'build' } else { 'release' })."
