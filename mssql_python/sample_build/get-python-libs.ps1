# Helper script to extract Python libraries for cross-compilation
# Save this as get-python-libs.ps1 in the sample_build folder
param (
    [string]$PythonVersion = "3.13"
)

Write-Host "Python Library Extractor for Cross-Compilation" -ForegroundColor Cyan
Write-Host "================================================" -ForegroundColor Cyan

# Get current Python installation info
try {
    $pythonExe = (Get-Command python -ErrorAction Stop).Source
    $pythonPath = Split-Path -Parent $pythonExe
    $pythonLibsPath = Join-Path $pythonPath "libs"
    
    Write-Host "Found Python at: $pythonExe"
    Write-Host "Python libs directory: $pythonLibsPath"
} catch {
    Write-Host "Python not found in PATH. Please ensure Python is installed." -ForegroundColor Red
    exit 1
}

# Create libs directory structure if it doesn't exist
$libsDir = Join-Path $PSScriptRoot "libs"
$arm64Dir = Join-Path $libsDir "arm64"
$x86Dir = Join-Path $libsDir "x86"

if (-not (Test-Path $libsDir)) {
    New-Item -ItemType Directory -Path $libsDir | Out-Null
}
if (-not (Test-Path $arm64Dir)) {
    New-Item -ItemType Directory -Path $arm64Dir | Out-Null
}
if (-not (Test-Path $x86Dir)) {
    New-Item -ItemType Directory -Path $x86Dir | Out-Null
}

# For x64, we just use the system Python
Write-Host "`nx64 Python libraries:" -ForegroundColor Green
Write-Host "For x64 builds, the script will use your installed Python libraries at:"
Write-Host "  $pythonLibsPath" -ForegroundColor Yellow

# For x86, we need to download the x86 Python installer
Write-Host "`nx86 Python libraries:" -ForegroundColor Green
$x86LibPath = Join-Path $x86Dir "python$($PythonVersion.Replace('.', '')).lib"
if (Test-Path $x86LibPath) {
    Write-Host "  x86 Python library already exists at: $x86LibPath" -ForegroundColor Yellow
} else {
    Write-Host "  x86 Python library not found" -ForegroundColor Yellow
    Write-Host "  You can get the x86 Python library by:" -ForegroundColor Yellow
    Write-Host "  1. Downloading Python x86 installer from https://www.python.org/downloads/"
    Write-Host "  2. Running the installer with the /layout option to extract files"
    Write-Host "  3. Copying the libs\python*.lib file to $x86Dir"
}

# For ARM64, we need to download the ARM64 Python installer (if available)
Write-Host "`nARM64 Python libraries:" -ForegroundColor Green
$arm64LibPath = Join-Path $arm64Dir "python$($PythonVersion.Replace('.', '')).lib"
if (Test-Path $arm64LibPath) {
    Write-Host "  ARM64 Python library already exists at: $arm64LibPath" -ForegroundColor Yellow
} else {
    Write-Host "  ARM64 Python library not found" -ForegroundColor Yellow
    Write-Host "  Getting ARM64 Python libraries requires one of these approaches:" -ForegroundColor Yellow
    Write-Host "  Option 1: Use Python libraries from the mssql-python project"
    Write-Host "    - Check if C:\Users\sharmag\OneDrive - Microsoft\Desktop\mssql-python\mssql_python\libs\winarm64 contains python*.lib"
    Write-Host "    - If available, copy it to $arm64Dir"
    Write-Host "  Option 2: Download ARM64 Python from Python.org (if available for your version)"
    Write-Host "  Option 3: Use the Microsoft Store version of Python for ARM64"
}

Write-Host "`nAlternative approach:" -ForegroundColor Green
Write-Host "If you don't have architecture-specific Python libraries, you can also try building"
Write-Host "with just your installed x64 Python, but this may not work for all extensions."
Write-Host "When using this approach, you'll need Python installed on the target machine."

Write-Host "`nNext steps:" -ForegroundColor Cyan
Write-Host "1. Place the appropriate Python libraries in the libs directories"
Write-Host "2. Run the build script for each architecture:"
Write-Host "   .\build.bat x64"
Write-Host "   .\build.bat x86"
Write-Host "   .\build.bat arm64"