<#
.SYNOPSIS
    Installs mssql-python-rs from the pinned internal NuGet transport package.

.PARAMETER FeedUrl
    The NuGet v3 feed URL. This is a public feed — no authentication required.

.PARAMETER OutputDir
    Temporary directory for downloaded artifacts. Cleaned up after installation.
    Defaults to $env:TEMP\mssql-python-rs-wheels.

.PARAMETER TargetArch
    Target CPU architecture ('x64' or 'arm64') for cross-compilation builds.
    When set, selects the matching-architecture wheel. Cross-architecture wheels
    are not installed into the host interpreter.
#>

param(
    [string]$FeedUrl = "https://pkgs.dev.azure.com/sqlclientdrivers/public/_packaging/mssql-rs_Public/nuget/v3/index.json",
    [string]$OutputDir = "$env:TEMP\mssql-python-rs-wheels",
    [string]$TargetArch = ""
)

$ErrorActionPreference = 'Stop'
$ScriptDir = $PSScriptRoot
$RepoRoot = (Get-Item "$ScriptDir\..\..").FullName

function Read-PackageVersion {
    $distributionVersionFile = Join-Path $RepoRoot "eng\versions\mssql-python-rs.version"
    $transportVersionFile = Join-Path $RepoRoot "eng\versions\mssql-python-rs-nuget.version"
    foreach ($versionFile in @($distributionVersionFile, $transportVersionFile)) {
        if (-not (Test-Path $versionFile -PathType Leaf)) {
            throw "Version file not found: $versionFile"
        }
    }
    $script:DistributionVersion = (Get-Content $distributionVersionFile -Raw).Trim()
    $script:TransportVersion = (Get-Content $transportVersionFile -Raw).Trim()
    if (-not $script:DistributionVersion -or -not $script:TransportVersion) {
        throw "mssql-python-rs version files must not be empty"
    }
    Write-Host "Distribution version: $script:DistributionVersion"
    Write-Host "NuGet transport version: $script:TransportVersion"
}

function Get-PlatformInfo {
    # Single python call to get version, platform, and HOST arch
    $info = & python -c "import sys, platform; v = sys.version_info; print(f'cp{v.major}{v.minor} {platform.system().lower()} {platform.machine().lower()}')"
    if ($LASTEXITCODE -ne 0) { throw "Failed to detect Python platform info" }

    $parts = $info -split ' '
    $script:PyVersion = $parts[0]
    $script:Platform = $parts[1]
    $hostArch = $parts[2]

    # Normalize an arch string (platform.machine() or -TargetArch) to a wheel tag.
    function ConvertTo-ArchTag($a) {
        switch -Regex ($a) {
            'amd64|x86_64|x64' { return 'x86_64' }
            'arm64|aarch64'    { return 'aarch64' }
            default { throw "Unsupported architecture: $a" }
        }
    }

    # Select the wheel by TARGET arch, not host arch. Empty -TargetArch uses host.
    $hostArchTag = ConvertTo-ArchTag $hostArch
    $archTag = if ($TargetArch) { ConvertTo-ArchTag $TargetArch } else { $hostArchTag }
    $script:IsCrossArch = ($archTag -ne $hostArchTag)

    Write-Host "Python: $script:PyVersion | Platform: $script:Platform | Arch: $archTag (host: $hostArchTag, target: $(if ($TargetArch) { $TargetArch } else { '<host>' }))"

    if ($script:Platform -ne 'windows') {
        throw "The PowerShell installer supports Windows only; use install-mssql-py-core.sh on $script:Platform"
    }
    # aarch64 -> arm64 so a Windows arm64 target resolves win_arm64, not win_aarch64.
    $script:WheelPlatform = "win_$($archTag -replace 'x86_64','amd64' -replace 'aarch64','arm64')"

    Write-Host "Wheel target: $script:PyVersion | $script:WheelPlatform"
}

function Get-NupkgFromFeed {
    param([string]$FeedUrl, [string]$OutputDir)

    $resolvedOutput = & python "$ScriptDir\mssql_python_build_safety.py" $OutputDir
    if ($LASTEXITCODE -ne 0) { throw "Unsafe OutputDir: $OutputDir" }
    if (Test-Path $resolvedOutput) { Remove-Item $resolvedOutput -Recurse -Force }
    New-Item -ItemType Directory -Path $resolvedOutput -Force | Out-Null
    $script:ResolvedOutputDir = $resolvedOutput

    Write-Host "Resolving feed: $FeedUrl"
    # Fetch the NuGet v3 service index and extract the PackageBaseAddress URL.
    # See resolve_nuget_feed.py for the JSON schema and detailed explanation.
    $feedIndex = Invoke-RestMethod -Uri $FeedUrl
    $packageBaseUrl = ($feedIndex.resources | Where-Object { $_.'@type' -like 'PackageBaseAddress*' } | Select-Object -First 1).'@id'
    if (-not $packageBaseUrl) { throw "Could not resolve PackageBaseAddress from feed" }
    $packageBaseUrl = $packageBaseUrl.TrimEnd('/') + '/'

    $versionLower = $script:TransportVersion.ToLowerInvariant()
    $packageId = "mssql-python-rs-wheels"
    $nupkgUrl = "${packageBaseUrl}${packageId}/${versionLower}/${packageId}.${versionLower}.nupkg"
    $script:NupkgPath = Join-Path $resolvedOutput "${packageId}.${versionLower}.nupkg"
    Write-Host "Downloading: $nupkgUrl"
    Invoke-WebRequest -Uri $nupkgUrl -OutFile $script:NupkgPath

    $sizeMB = [math]::Round((Get-Item $script:NupkgPath).Length / 1MB, 2)
    Write-Host "Downloaded: $script:NupkgPath ($sizeMB MB)"
}

function Find-MatchingWheel {
    param([string]$OutputDir)

    # nupkg is a ZIP — rename so Expand-Archive accepts it
    $zipPath = $script:NupkgPath -replace '\.nupkg$', '.zip'
    Rename-Item -Path $script:NupkgPath -NewName (Split-Path $zipPath -Leaf)

    $extractDir = Join-Path $script:ResolvedOutputDir "extracted"
    Expand-Archive -Path $zipPath -DestinationPath $extractDir -Force

    $wheelsDir = Join-Path $extractDir "wheels"
    if (-not (Test-Path $wheelsDir)) {
        throw "No 'wheels' directory found in NuGet package"
    }

    $matchingWheelPath = & python "$ScriptDir\select_mssql_python_rs_wheel.py" `
        $wheelsDir $script:DistributionVersion $script:PyVersion $script:WheelPlatform
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Available wheels:"
        Get-ChildItem $wheelsDir -Filter *.whl | ForEach-Object { Write-Host "  $_" }
        throw "No compatible mssql-python-rs wheel found"
    }
    $script:MatchingWheel = Get-Item $matchingWheelPath

    Write-Host "Found: $($script:MatchingWheel.Name)"
}

function Install-AndVerify {
    $coreDir = Join-Path $RepoRoot "mssql_py_core"
    if (Test-Path $coreDir) {
        Remove-Item $coreDir -Recurse -Force
        Write-Host "Cleaned previous mssql_py_core/"
    }

    # An arm64 wheel cannot be installed into the x64 build-host interpreter.
    if ($script:IsCrossArch) {
        Write-Host "Skipping mssql-python-rs installation (cross-arch build: target != host)"
        return
    }

    & python -m pip install --force-reinstall --no-deps $script:MatchingWheel.FullName
    if ($LASTEXITCODE -ne 0) { throw "Failed to install mssql-python-rs" }

    & python -c "import importlib.metadata as m, mssql_py_core; assert m.version('mssql-python-rs') == '$script:DistributionVersion'; print('mssql-python-rs', m.version('mssql-python-rs'), 'loaded from', mssql_py_core.__file__)"
    if ($LASTEXITCODE -ne 0) { throw "Failed to verify installed mssql-python-rs" }
}

# --- main ---

Write-Host "=== Install mssql-python-rs from NuGet transport ==="

try {
    Read-PackageVersion
    Get-PlatformInfo
    Get-NupkgFromFeed -FeedUrl $FeedUrl -OutputDir $OutputDir
    Find-MatchingWheel -OutputDir $OutputDir
    Install-AndVerify
    Write-Host "=== mssql-python-rs installed successfully ==="
}
finally {
    if ($script:ResolvedOutputDir) {
        Remove-Item $script:ResolvedOutputDir -Recurse -Force -ErrorAction SilentlyContinue
    }
}
