param(
    [Parameter(Mandatory = $true)]
    [string]$WheelsDir,

    [Parameter(Mandatory = $true)]
    [string]$VersionFile
)

$ErrorActionPreference = 'Stop'

$expectedVersion = (Get-Content $VersionFile -Raw).Trim()
if (-not $expectedVersion) {
    throw "Version file is empty: $VersionFile"
}

$wheels = @(Get-ChildItem $WheelsDir -Filter 'mssql_python-*.whl' -File)
if ($wheels.Count -eq 0) {
    throw "No mssql_python-*.whl files found in $WheelsDir"
}

Add-Type -AssemblyName System.IO.Compression.FileSystem
$pinPattern = '^Requires-Dist:\s*mssql[-_]python[-_]rs\s*==\s*' +
    [regex]::Escape($expectedVersion) + '\s*$'

foreach ($wheel in $wheels) {
    $zip = [System.IO.Compression.ZipFile]::OpenRead($wheel.FullName)
    try {
        $ownedCore = @(
            $zip.Entries | Where-Object {
                $_.FullName -match '^mssql_py_core(?:/|\.libs/)'
            }
        )
        if ($ownedCore.Count -gt 0) {
            throw "$($wheel.Name) vendors mssql_py_core files owned by mssql-python-rs."
        }

        $metadataEntries = @(
            $zip.Entries | Where-Object { $_.FullName -match '\.dist-info/METADATA$' }
        )
        if ($metadataEntries.Count -ne 1) {
            throw "$($wheel.Name) contains $($metadataEntries.Count) METADATA files; expected 1."
        }

        $reader = [System.IO.StreamReader]::new($metadataEntries[0].Open())
        try {
            $metadata = $reader.ReadToEnd()
        }
        finally {
            $reader.Dispose()
        }

        $dependencyLines = @(
            ($metadata -split "`n") |
                Where-Object { $_ -match '^Requires-Dist:\s*mssql[-_]python[-_]rs' }
        )
        if ($dependencyLines.Count -ne 1 -or $dependencyLines[0].Trim() -notmatch $pinPattern) {
            throw "$($wheel.Name) must declare exactly mssql-python-rs==$expectedVersion."
        }
    }
    finally {
        $zip.Dispose()
    }

    Write-Host "OK: $($wheel.Name) depends on mssql-python-rs==$expectedVersion and owns no mssql_py_core files."
}
