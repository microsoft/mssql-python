#!/bin/bash
# Copy repository to Windows and run tests there

set -e

echo "=========================================="
echo "Windows Test Setup from WSL"
echo "=========================================="
echo ""

# Get Windows directory
echo "We need to copy the repository to a Windows-accessible directory"
echo "because Windows CMD doesn't support UNC paths (\\\\wsl.localhost\\...)"
echo ""
read -p "Windows directory path (e.g., C:\\Temp\\mssql-test): " WIN_DIR

if [ -z "$WIN_DIR" ]; then
    WIN_DIR="C:\\Temp\\mssql-python-test"
    echo "Using default: $WIN_DIR"
fi

# Convert to Windows format if needed
WIN_DIR=$(echo "$WIN_DIR" | sed 's/\//\\/g')

# Convert to Linux path for copying
LINUX_PATH=$(wslpath "$WIN_DIR" 2>/dev/null || echo "")
if [ -z "$LINUX_PATH" ]; then
    echo "ERROR: Could not convert Windows path to Linux path"
    exit 1
fi

echo ""
echo "Linux path: $LINUX_PATH"
echo "Windows path: $WIN_DIR"
echo ""

# Create directory and copy files
echo "Copying repository to Windows..."
mkdir -p "$LINUX_PATH"

# Copy necessary files
rsync -av --progress \
    --exclude='.git' \
    --exclude='__pycache__' \
    --exclude='*.pyc' \
    --exclude='.pytest_cache' \
    --exclude='build' \
    --exclude='*.egg-info' \
    --exclude='.venv' \
    --exclude='venv' \
    ./ "$LINUX_PATH/"

echo ""
echo "✓ Files copied successfully"
echo ""

# Get connection details
echo "=========================================="
echo "SQL Server Connection Details"
echo "=========================================="
read -p "SQL Server (default: localhost): " SQL_SERVER
SQL_SERVER=${SQL_SERVER:-localhost}

read -p "Database (default: test): " DATABASE
DATABASE=${DATABASE:-test}

read -p "Username (default: sa): " USERNAME
USERNAME=${USERNAME:-sa}

read -sp "Password: " PASSWORD
echo ""
echo ""

# Create a PowerShell script to run in Windows
cat > "$LINUX_PATH/run_test_windows.ps1" << 'PSEOF'
param(
    [string]$SqlServer = "localhost",
    [string]$Database = "test",
    [string]$Username = "sa",
    [string]$Password = ""
)

$ErrorActionPreference = "Stop"

Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "Windows C++ Build and Test" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Python: $(python --version)" -ForegroundColor Green
Write-Host "Location: $PWD" -ForegroundColor Gray
Write-Host ""

# Check for Visual Studio
Write-Host "Step 1: Checking build environment..." -ForegroundColor Yellow
$vsPath = & "${env:ProgramFiles(x86)}\Microsoft Visual Studio\Installer\vswhere.exe" -latest -property installationPath 2>$null
if ($vsPath) {
    Write-Host "  ✓ Visual Studio found: $vsPath" -ForegroundColor Green
} else {
    Write-Host "  ⚠ Visual Studio not detected" -ForegroundColor Yellow
    Write-Host "  Build may fail without Visual Studio C++ tools" -ForegroundColor Yellow
}
Write-Host ""

# Build C++ extension
Write-Host "Step 2: Building C++ extension..." -ForegroundColor Yellow
Push-Location "mssql_python\pybind"

if (-not (Test-Path "build.bat")) {
    Write-Host "  ERROR: build.bat not found!" -ForegroundColor Red
    Pop-Location
    exit 1
}

Write-Host "  Running: build.bat x64" -ForegroundColor Gray
$buildOutput = & cmd /c "build.bat x64 2>&1"
$buildExit = $LASTEXITCODE

if ($buildExit -ne 0) {
    Write-Host ""
    Write-Host "  ✗ Build failed!" -ForegroundColor Red
    Write-Host ""
    Write-Host "Build output:" -ForegroundColor Yellow
    Write-Host $buildOutput
    Write-Host ""
    Write-Host "Troubleshooting:" -ForegroundColor Yellow
    Write-Host "  1. Open 'Developer Command Prompt for VS'" -ForegroundColor Gray
    Write-Host "  2. Navigate to: $PWD" -ForegroundColor Gray
    Write-Host "  3. Run: cd mssql_python\pybind && build.bat x64" -ForegroundColor Gray
    Pop-Location
    exit 1
}

Pop-Location
Write-Host "  ✓ C++ extension built successfully" -ForegroundColor Green
Write-Host ""

# Install package
Write-Host "Step 3: Installing package..." -ForegroundColor Yellow
$installOutput = python -m pip install -e . 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "  ✗ Installation failed!" -ForegroundColor Red
    Write-Host $installOutput
    exit 1
}
Write-Host "  ✓ Package installed" -ForegroundColor Green
Write-Host ""

# Verify extension
Write-Host "Step 4: Verifying C++ extension..." -ForegroundColor Yellow
$verifyOutput = python -c "import mssql_python; print(f'  Version: {mssql_python.__version__}'); from mssql_python.ddbc_bindings import Connection; print('  ✓ Extension loaded')" 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "  ✗ Verification failed!" -ForegroundColor Red
    Write-Host $verifyOutput
    exit 1
}
Write-Host $verifyOutput
Write-Host ""

# Install dependencies
Write-Host "Step 5: Installing test dependencies..." -ForegroundColor Yellow
python -m pip install -q pytest sqlalchemy greenlet typing-extensions
Write-Host "  ✓ Dependencies installed" -ForegroundColor Green
Write-Host ""

# Update connection string
Write-Host "Step 6: Configuring connection..." -ForegroundColor Yellow
$connStr = "Driver={ODBC Driver 18 for SQL Server};Server=$SqlServer;Database=$Database;UID=$Username;PWD=$Password;Encrypt=No"
Write-Host "  Server: $SqlServer/$Database" -ForegroundColor Gray

$testFile = Get-Content 'test_windows_segfault_simple.py' -Raw
$testFile = $testFile -replace 'CONN_STR = ".*?"', "CONN_STR = `"$connStr`""
$testFile | Set-Content 'test_windows_segfault_simple.py' -NoNewline
Write-Host "  ✓ Connection configured" -ForegroundColor Green
Write-Host ""

# Run test
Write-Host "Step 7: Running segfault test..." -ForegroundColor Yellow
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host ""

python test_windows_segfault_simple.py
$testExit = $LASTEXITCODE

Write-Host ""
Write-Host "==========================================" -ForegroundColor Cyan
if ($testExit -eq 0) {
    Write-Host "✓✓✓ SUCCESS! All tests passed! ✓✓✓" -ForegroundColor Green
    Write-Host "The segfault fix is working on Windows." -ForegroundColor Green
} else {
    Write-Host "✗✗✗ FAILURE! Tests failed! ✗✗✗" -ForegroundColor Red
    Write-Host "The issue may still exist." -ForegroundColor Red
}
Write-Host "==========================================" -ForegroundColor Cyan

exit $testExit
PSEOF

echo "=========================================="
echo "Starting Windows test..."
echo "=========================================="
echo ""

# Run PowerShell script
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "$(wslpath -w "$LINUX_PATH/run_test_windows.ps1")" -SqlServer "$SQL_SERVER" -Database "$DATABASE" -Username "$USERNAME" -Password "$PASSWORD"

EXIT_CODE=$?

echo ""
echo "=========================================="
if [ $EXIT_CODE -eq 0 ]; then
    echo "✓ Windows test completed successfully!"
    echo ""
    echo "The C++ fix is working correctly on Windows!"
else
    echo "✗ Windows test failed with exit code: $EXIT_CODE"
fi
echo "=========================================="
echo ""
echo "Files are located at: $WIN_DIR"
echo "You can also run tests manually from Windows:"
echo "  1. Open PowerShell"
echo "  2. cd $WIN_DIR"
echo "  3. .\\run_test_windows.ps1 -SqlServer '$SQL_SERVER' -Database '$DATABASE' -Username '$USERNAME' -Password 'yourpass'"

exit $EXIT_CODE
