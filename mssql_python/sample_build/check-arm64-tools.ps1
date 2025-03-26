# Sample PowerShell script to diagnose ARM64 build tools
# Save as check-arm64-tools.ps1 in the sample_build folder

Write-Host "Visual Studio ARM64 Build Tools Diagnostics" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan

# Check if running on ARM64 Windows
$arch = (Get-WmiObject Win32_OperatingSystem).OSArchitecture
Write-Host "Current OS architecture: $arch"

# Look for Visual Studio installation
Write-Host "`nChecking Visual Studio installations:" -ForegroundColor Green
$vsInstallLocations = @(
    "C:\Program Files\Microsoft Visual Studio",
    "C:\Program Files (x86)\Microsoft Visual Studio"
)

$vsEditions = @("BuildTools", "Community", "Professional", "Enterprise")
$vsYears = @("2022", "2019", "2017")

$foundVs = $false
foreach ($location in $vsInstallLocations) {
    if (Test-Path $location) {
        foreach ($year in $vsYears) {
            $yearPath = Join-Path $location $year
            if (Test-Path $yearPath) {
                foreach ($edition in $vsEditions) {
                    $fullPath = Join-Path $yearPath $edition
                    if (Test-Path $fullPath) {
                        Write-Host "  - Found: $fullPath" -ForegroundColor Green
                        $foundVs = $true
                    }
                }
            }
        }
    }
}

if (-not $foundVs) {
    Write-Host "  - No Visual Studio installations found in standard locations" -ForegroundColor Yellow
}

# Check for ARM64 compilers
Write-Host "`nChecking for ARM64 compilers:" -ForegroundColor Green
$arm64CompilerFound = $false

foreach ($location in $vsInstallLocations) {
    if (Test-Path $location) {
        foreach ($year in $vsYears) {
            foreach ($edition in $vsEditions) {
                $msvcPath = Join-Path $location "$year\$edition\VC\Tools\MSVC"
                if (Test-Path $msvcPath) {
                    # Get all MSVC version folders
                    $msvcVersions = Get-ChildItem -Path $msvcPath -Directory
                    foreach ($version in $msvcVersions) {
                        # Check for x64 host to arm64 target compiler
                        $compilerPath = Join-Path $version.FullName "bin\Hostx64\arm64\cl.exe"
                        if (Test-Path $compilerPath) {
                            Write-Host "  - Found cross-compiler: $compilerPath" -ForegroundColor Green
                            $arm64CompilerFound = $true
                        }
                        
                        # Check for arm64 host to arm64 target compiler
                        $nativeCompilerPath = Join-Path $version.FullName "bin\Hostarm64\arm64\cl.exe"
                        if (Test-Path $nativeCompilerPath) {
                            Write-Host "  - Found native ARM64 compiler: $nativeCompilerPath" -ForegroundColor Green
                            $arm64CompilerFound = $true
                        }
                    }
                }
            }
        }
    }
}

if (-not $arm64CompilerFound) {
    Write-Host "  - No ARM64 compilers found" -ForegroundColor Red
    Write-Host "  - You need to install the C++ ARM64 build tools component in Visual Studio" -ForegroundColor Yellow
}

# Check for vcvarsall.bat
Write-Host "`nChecking for vcvarsall.bat:" -ForegroundColor Green
$vcvarsallFound = $false

foreach ($location in $vsInstallLocations) {
    if (Test-Path $location) {
        foreach ($year in $vsYears) {
            foreach ($edition in $vsEditions) {
                $vcvarsPath = Join-Path $location "$year\$edition\VC\Auxiliary\Build\vcvarsall.bat"
                if (Test-Path $vcvarsPath) {
                    Write-Host "  - Found: $vcvarsPath" -ForegroundColor Green
                    $vcvarsallFound = $true
                }
            }
        }
    }
}

if (-not $vcvarsallFound) {
    Write-Host "  - vcvarsall.bat not found" -ForegroundColor Red
}

# Check for Windows SDK components
Write-Host "`nChecking for Windows SDK components:" -ForegroundColor Green
$sdkRoot = "C:\Program Files (x86)\Windows Kits\10"
if (Test-Path $sdkRoot) {
    Write-Host "  - Windows SDK found at: $sdkRoot" -ForegroundColor Green
    
    # Check Include directory
    $includeDir = Join-Path $sdkRoot "Include"
    if (Test-Path $includeDir) {
        $sdkVersions = Get-ChildItem -Path $includeDir -Directory
        Write-Host "  - SDK versions found:" -ForegroundColor Green
        foreach ($version in $sdkVersions) {
            Write-Host "    - $($version.Name)" -ForegroundColor Green
        }
    }
    
    # Check for ARM64 libraries
    $libDir = Join-Path $sdkRoot "Lib"
    if (Test-Path $libDir) {
        $arm64LibFound = $false
        $sdkVersions = Get-ChildItem -Path $libDir -Directory
        foreach ($version in $sdkVersions) {
            $ucrtArm64 = Join-Path $version.FullName "ucrt\arm64"
            $umArm64 = Join-Path $version.FullName "um\arm64"
            
            if (Test-Path $ucrtArm64) {
                Write-Host "  - ARM64 UCRT libraries found in SDK version $($version.Name)" -ForegroundColor Green
                $arm64LibFound = $true
            }
            
            if (Test-Path $umArm64) {
                Write-Host "  - ARM64 UM libraries found in SDK version $($version.Name)" -ForegroundColor Green
                $arm64LibFound = $true
            }
        }
        
        if (-not $arm64LibFound) {
            Write-Host "  - ARM64 SDK libraries not found" -ForegroundColor Red
        }
    }
} else {
    Write-Host "  - Windows SDK not found at standard location" -ForegroundColor Red
}

Write-Host "`nDiagnostics complete. Please provide this information when asking for help." -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan