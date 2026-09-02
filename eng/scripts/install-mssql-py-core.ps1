<#
.SYNOPSIS
    Downloads the mssql-py-core-wheels NuGet package from a public Azure Artifacts
    feed and extracts the matching mssql_py_core binary into the repository root
    so that 'import mssql_py_core' works when running from the source tree.

.PARAMETER FeedUrl
    The NuGet v3 feed URL. This is a public feed — no authentication required.

.PARAMETER OutputDir
    Temporary directory for downloaded artifacts. Cleaned up after extraction.
    Defaults to $env:TEMP\mssql-py-core-wheels.

.PARAMETER TargetArch
    Target CPU architecture ('x64' or 'arm64') for cross-compilation builds.
    When set, the matching-arch mssql_py_core wheel is selected instead of the
    host arch reported by platform.machine() -- required because Windows arm64
    wheels are cross-built on an x64 host, where an unset value would vendor the
    x64 core into the arm64 wheel. The import self-check is also skipped when the
    target differs from the host (a cross-arch .pyd cannot load here). Defaults to
    empty (use host arch), which is correct for native/local builds.
#>

param(
    [string]$FeedUrl = "https://pkgs.dev.azure.com/sqlclientdrivers/public/_packaging/mssql-rs_Public/nuget/v3/index.json",
    [string]$OutputDir = "$env:TEMP\mssql-py-core-wheels",
    [string]$TargetArch = ""
)

$ErrorActionPreference = 'Stop'
$ScriptDir = $PSScriptRoot
$RepoRoot = (Get-Item "$ScriptDir\..\..").FullName

function Read-PackageVersion {
    $versionFile = Join-Path $RepoRoot "eng\versions\mssql-py-core.version"
    if (-not (Test-Path $versionFile)) {
        throw "Version file not found: $versionFile"
    }
    $script:PackageVersion = (Get-Content $versionFile -Raw).Trim()
    if (-not $script:PackageVersion) {
        throw "Version file is empty: $versionFile"
    }
    Write-Host "Version: $script:PackageVersion"
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

    # Select the wheel by TARGET arch, not host arch. Windows arm64 wheels are
    # cross-built on an x64 host, where the running Python is x64 and reports the
    # HOST via platform.machine(); without this override the installer would vendor
    # the x64 core into the arm64 wheel. Empty -TargetArch (native/local) -> host.
    $hostArchTag = ConvertTo-ArchTag $hostArch
    $archTag = if ($TargetArch) { ConvertTo-ArchTag $TargetArch } else { $hostArchTag }
    $script:IsCrossArch = ($archTag -ne $hostArchTag)

    Write-Host "Python: $script:PyVersion | Platform: $script:Platform | Arch: $archTag (host: $hostArchTag, target: $(if ($TargetArch) { $TargetArch } else { '<host>' }))"

    $script:WheelPlatform = switch ($script:Platform) {
        # aarch64 -> arm64 so a Windows arm64 target resolves win_arm64, not win_aarch64.
        'windows' { "win_$($archTag -replace 'x86_64','amd64' -replace 'aarch64','arm64')" }
        'linux' { "linux_$archTag" }
        'darwin' { 'macosx_15_0_universal2' }
        default { throw "Unsupported platform: $script:Platform" }
    }

    $script:WheelPattern = "mssql_py_core-*-$script:PyVersion-$script:PyVersion-$script:WheelPlatform.whl"
    Write-Host "Wheel pattern: $script:WheelPattern"
}

function Get-NupkgFromFeed {
    param([string]$FeedUrl, [string]$OutputDir)

    if (Test-Path $OutputDir) { Remove-Item $OutputDir -Recurse -Force }
    New-Item -ItemType Directory -Path $OutputDir -Force | Out-Null

    Write-Host "Resolving feed: $FeedUrl"
    # Fetch the NuGet v3 service index and extract the PackageBaseAddress URL.
    # See resolve_nuget_feed.py for the JSON schema and detailed explanation.
    $feedIndex = Invoke-RestMethod -Uri $FeedUrl
    $packageBaseUrl = ($feedIndex.resources | Where-Object { $_.'@type' -like 'PackageBaseAddress*' }).'@id'
    if (-not $packageBaseUrl) { throw "Could not resolve PackageBaseAddress from feed" }

    $packageId = "mssql-py-core-wheels"
    $versionLower = $script:PackageVersion.ToLower()
    # e.g. https://pkgs.dev.azure.com/.../nuget/v3/flat2/mssql-py-core-wheels/0.1.0-dev.20260222.140833/mssql-py-core-wheels.0.1.0-dev.20260222.140833.nupkg
    $nupkgUrl = "${packageBaseUrl}${packageId}/${versionLower}/${packageId}.${versionLower}.nupkg"
    $script:NupkgPath = Join-Path $OutputDir "${packageId}.${versionLower}.nupkg"

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

    $extractDir = Join-Path $OutputDir "extracted"
    Expand-Archive -Path $zipPath -DestinationPath $extractDir -Force

    $wheelsDir = Join-Path $extractDir "wheels"
    if (-not (Test-Path $wheelsDir)) {
        throw "No 'wheels' directory found in NuGet package"
    }

    $script:MatchingWheel = Get-ChildItem $wheelsDir -Filter $script:WheelPattern | Select-Object -First 1
    if (-not $script:MatchingWheel) {
        Write-Host "Available wheels:"
        Get-ChildItem $wheelsDir -Filter *.whl | ForEach-Object { Write-Host "  $_" }
        throw "No wheel found matching: $script:WheelPattern"
    }

    Write-Host "Found: $($script:MatchingWheel.Name)"
}

function Install-AndVerify {
    $coreDir = Join-Path $RepoRoot "mssql_py_core"
    if (Test-Path $coreDir) {
        Remove-Item $coreDir -Recurse -Force
        Write-Host "Cleaned previous mssql_py_core/"
    }

    & python "$ScriptDir\extract_wheel.py" $script:MatchingWheel.FullName $RepoRoot
    if ($LASTEXITCODE -ne 0) { throw "Failed to extract mssql_py_core from wheel" }

    # Skip the import self-check on cross-arch builds: an arm64 .pyd cannot be
    # loaded by the x64 Python running on the build host. The wheel is still
    # vendored correctly and is exercised by tests on native-arch agents.
    if ($script:IsCrossArch) {
        Write-Host "Skipping import verification (cross-arch build: target != host)"
        return
    }

    Write-Host "Verifying import..."
    Push-Location $RepoRoot
    try {
        & python -c "import mssql_py_core; print(f'mssql_py_core loaded: {dir(mssql_py_core)}')"
        if ($LASTEXITCODE -ne 0) { throw "Failed to import mssql_py_core" }
    }
    finally {
        Pop-Location
    }
}

# --- main ---

Write-Host "=== Install mssql_py_core from NuGet wheel package ==="

Read-PackageVersion
Get-PlatformInfo
Get-NupkgFromFeed -FeedUrl $FeedUrl -OutputDir $OutputDir
Find-MatchingWheel -OutputDir $OutputDir
Install-AndVerify

Remove-Item $OutputDir -Recurse -Force -ErrorAction SilentlyContinue
Write-Host "=== mssql_py_core extracted successfully ==="
