<#
.SYNOPSIS
    Downloads the mssql-py-core-wheels NuGet package from a public Azure Artifacts
    feed and installs the appropriate wheel for the current platform into the
    Python environment so that 'import mssql_py_core' works.

    The package version is read from eng/versions/mssql-py-core.version (required).

.PARAMETER FeedUrl
    The NuGet v3 feed URL. This is a public feed — no authentication required.

.PARAMETER OutputDir
    Temporary directory for downloaded artifacts. Cleaned up after install.
    Defaults to $env:TEMP\mssql-py-core-wheels.
#>

param(
    [string]$FeedUrl = "https://pkgs.dev.azure.com/sqlclientdrivers/public/_packaging/mssql-rs_Public/nuget/v3/index.json",
    [string]$OutputDir = "$env:TEMP\mssql-py-core-wheels"
)

$ErrorActionPreference = 'Stop'

Write-Host "=== Install mssql_py_core from NuGet wheel package ==="

# Read version from pinned version file (required)
$repoRoot = (Get-Item "$PSScriptRoot\..\..").FullName
$versionFile = Join-Path $repoRoot "eng\versions\mssql-py-core.version"
if (-not (Test-Path $versionFile)) {
    throw "Version file not found: $versionFile. This file must exist and contain the mssql-py-core-wheels NuGet package version."
}
$PackageVersion = (Get-Content $versionFile -Raw).Trim()
if (-not $PackageVersion) {
    throw "Version file is empty: $versionFile"
}
Write-Host "Using version from $versionFile : $PackageVersion"

# Determine platform info
$pyVersion = & python -c "import sys; print(f'cp{sys.version_info.major}{sys.version_info.minor}')"
$platform = & python -c "import platform; print(platform.system().lower())"
$arch = & python -c "import platform; print(platform.machine().lower())"

Write-Host "Python: $pyVersion | Platform: $platform | Arch: $arch"

# Map to wheel filename platform tags
switch ($platform) {
    'windows' {
        switch -Regex ($arch) {
            'amd64|x86_64' { $wheelPlatform = "win_amd64" }
            'arm64|aarch64' { $wheelPlatform = "win_arm64" }
            default { throw "Unsupported Windows architecture: $arch" }
        }
    }
    'linux' {
        switch -Regex ($arch) {
            'x86_64|amd64' { $wheelPlatform = "manylinux_2_28_x86_64" }
            'aarch64|arm64' { $wheelPlatform = "manylinux_2_28_aarch64" }
            default { throw "Unsupported Linux architecture: $arch" }
        }
    }
    'darwin' {
        $wheelPlatform = "macosx_15_0_universal2"
    }
    default { throw "Unsupported platform: $platform" }
}

$wheelPattern = "mssql_py_core-*-$pyVersion-$pyVersion-$wheelPlatform.whl"
Write-Host "Looking for wheel matching: $wheelPattern"

# Create temp directory
if (Test-Path $OutputDir) { Remove-Item $OutputDir -Recurse -Force }
New-Item -ItemType Directory -Path $OutputDir -Force | Out-Null

# Download NuGet package
$nugetDir = "$OutputDir\nuget"
New-Item -ItemType Directory -Path $nugetDir -Force | Out-Null

# Resolve NuGet v3 feed to find package base URL
Write-Host "Resolving feed: $FeedUrl"
$feedIndex = Invoke-RestMethod -Uri $FeedUrl
$packageBaseUrl = ($feedIndex.resources | Where-Object { $_.'@type' -like 'PackageBaseAddress*' }).'@id'
if (-not $packageBaseUrl) { throw "Could not resolve PackageBaseAddress from feed" }
Write-Host "Package base: $packageBaseUrl"

$packageId = "mssql-py-core-wheels"
$packageIdLower = $packageId.ToLower()

$versionLower = $PackageVersion.ToLower()
$nupkgUrl = "$packageBaseUrl$packageIdLower/$versionLower/$packageIdLower.$versionLower.nupkg"
$nupkgPath = "$nugetDir\$packageIdLower.$versionLower.nupkg"

Write-Host "Downloading: $nupkgUrl"
Invoke-WebRequest -Uri $nupkgUrl -OutFile $nupkgPath
Write-Host "Downloaded: $nupkgPath ($([math]::Round((Get-Item $nupkgPath).Length / 1MB, 2)) MB)"

# Extract NuGet (it's a ZIP — rename so Expand-Archive accepts it)
$zipPath = "$nugetDir\$packageIdLower.$versionLower.zip"
Rename-Item -Path $nupkgPath -NewName (Split-Path $zipPath -Leaf)
$extractDir = "$nugetDir\extracted"
Expand-Archive -Path $zipPath -DestinationPath $extractDir -Force

# Find the matching wheel
$wheelsDir = "$extractDir\wheels"
if (-not (Test-Path $wheelsDir)) {
    throw "No 'wheels' directory found in NuGet package. Contents: $(Get-ChildItem $extractDir -Recurse | Select-Object -ExpandProperty Name)"
}

$matchingWheel = Get-ChildItem $wheelsDir -Filter $wheelPattern | Select-Object -First 1
if (-not $matchingWheel) {
    Write-Host "Available wheels:"
    Get-ChildItem $wheelsDir -Filter *.whl | ForEach-Object { Write-Host "  $_" }
    throw "No wheel found matching pattern: $wheelPattern"
}

Write-Host "Found matching wheel: $($matchingWheel.Name)"

# Install the wheel with pip
Write-Host "Installing wheel with pip..."
& python -m pip install $matchingWheel.FullName --force-reinstall --no-deps
if ($LASTEXITCODE -ne 0) { throw "pip install failed with exit code $LASTEXITCODE" }

# Verify import works
Write-Host "Verifying mssql_py_core import..."
& python -c "import mssql_py_core; print(f'mssql_py_core loaded successfully: {dir(mssql_py_core)}')"
if ($LASTEXITCODE -ne 0) { throw "Failed to import mssql_py_core" }

# Cleanup
Remove-Item $OutputDir -Recurse -Force -ErrorAction SilentlyContinue

Write-Host "=== mssql_py_core installed successfully ==="
