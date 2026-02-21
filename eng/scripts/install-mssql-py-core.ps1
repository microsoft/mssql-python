<#
.SYNOPSIS
    Downloads the mssql-py-core-wheels NuGet package from a public Azure Artifacts
    feed and extracts the matching mssql_py_core binary into the repository root
    so that 'import mssql_py_core' works when running from the source tree.

    The extracted files are placed at <repo-root>/mssql_py_core/ which contains:
      - __init__.py
      - mssql_py_core.<cpython-tag>.<pyd|so>  (native extension)

    This script is used identically for:
      - Local development (dev runs it after build.bat/build.sh)
      - PR validation pipelines
      - Official build pipelines (before setup.py bdist_wheel)

    The package version is read from eng/versions/mssql-py-core.version (required).

.PARAMETER FeedUrl
    The NuGet v3 feed URL. This is a public feed — no authentication required.

.PARAMETER OutputDir
    Temporary directory for downloaded artifacts. Cleaned up after extraction.
    Defaults to $env:TEMP\mssql-py-core-wheels.
#>

param(
    [string]$FeedUrl = "https://pkgs.dev.azure.com/sqlclientdrivers/public/_packaging/mssql-rs_Public/nuget/v3/index.json",
    [string]$OutputDir = "$env:TEMP\mssql-py-core-wheels"
)

$ErrorActionPreference = 'Stop'

Write-Host "=== Install mssql_py_core from NuGet wheel package ==="

# Determine repository root (two levels up from this script)
$repoRoot = (Get-Item "$PSScriptRoot\..\..").FullName

# Read version from pinned version file (required)
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

# Extract mssql_py_core/ from the wheel into the repository root.
# The wheel is a ZIP file containing mssql_py_core/__init__.py and
# mssql_py_core/mssql_py_core.<cpython-tag>.<pyd|so>.
# We skip .dist-info/ metadata.
# mssql_py_core.libs/ won't exist because auditwheel=skip is set in pyproject.toml,
# but we skip it defensively in case an older wheel is used.
$targetDir = $repoRoot
$coreDir = Join-Path $targetDir "mssql_py_core"

# Clean previous extraction
if (Test-Path $coreDir) {
    Remove-Item $coreDir -Recurse -Force
    Write-Host "Cleaned previous mssql_py_core/ directory"
}

Write-Host "Extracting mssql_py_core from wheel into: $targetDir"

# Use Python to extract (zipfile handles wheel format reliably)
$wheelPath = $matchingWheel.FullName
& python -c @"
import zipfile, os, sys

wheel_path = r'$wheelPath'
target_dir = r'$targetDir'
extracted = 0

with zipfile.ZipFile(wheel_path, 'r') as zf:
    for entry in zf.namelist():
        # Skip dist-info metadata
        if '.dist-info/' in entry:
            continue
        # Skip vendored shared libraries if present (auditwheel=skip means
        # they won't be in the wheel; system OpenSSL is used at runtime)
        if entry.startswith('mssql_py_core.libs/'):
            continue
        if entry.startswith('mssql_py_core/'):
            out_path = os.path.join(target_dir, entry)
            if entry.endswith('/'):
                os.makedirs(out_path, exist_ok=True)
                continue
            os.makedirs(os.path.dirname(out_path), exist_ok=True)
            with open(out_path, 'wb') as f:
                f.write(zf.read(entry))
            extracted += 1
            print(f'  Extracted: {entry}')

if extracted == 0:
    print('ERROR: No mssql_py_core files found in wheel', file=sys.stderr)
    sys.exit(1)

print(f'Extracted {extracted} file(s) into {target_dir}')
"@
if ($LASTEXITCODE -ne 0) { throw "Failed to extract mssql_py_core from wheel" }

# Verify import works (from repo root so mssql_py_core/ is on sys.path)
Write-Host "Verifying mssql_py_core import..."
Push-Location $repoRoot
try {
    & python -c "import mssql_py_core; print(f'mssql_py_core loaded successfully: {dir(mssql_py_core)}')"
    if ($LASTEXITCODE -ne 0) { throw "Failed to import mssql_py_core" }
}
finally {
    Pop-Location
}

# Cleanup temp files
Remove-Item $OutputDir -Recurse -Force -ErrorAction SilentlyContinue

Write-Host "=== mssql_py_core extracted successfully ==="
