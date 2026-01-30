#!/bin/bash
# Helper script to run Windows tests from WSL

set -e

echo "=========================================="
echo "Launching Windows Test from WSL"
echo "=========================================="
echo ""

# Convert current path to Windows path
WINDOWS_PATH=$(wslpath -w "$PWD")
echo "Windows Path: $WINDOWS_PATH"
echo ""

# Check if Python is installed on Windows
echo "Checking Windows Python installation..."
if ! powershell.exe -Command "python --version" 2>/dev/null; then
    echo ""
    echo "ERROR: Python not found in Windows PATH"
    echo "Please install Python on Windows: https://www.python.org/downloads/"
    exit 1
fi

PYTHON_VERSION=$(powershell.exe -Command "python --version 2>&1" | tr -d '\r')
echo "Found: $PYTHON_VERSION"
echo ""

# Ask user for connection details
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

read -p "Number of test runs (default: 10): " TEST_RUNS
TEST_RUNS=${TEST_RUNS:-10}

echo ""
echo "=========================================="
echo "Starting Windows Build and Test"
echo "=========================================="
echo ""

# Build the C++ extension on Windows
echo "Step 1: Building C++ extension on Windows..."
echo "This requires Visual Studio with C++ tools installed"
echo ""

powershell.exe -NoProfile -ExecutionPolicy Bypass -Command "
    cd '$WINDOWS_PATH'
    Write-Host 'Current directory:' (Get-Location) -ForegroundColor Cyan
    Write-Host ''
    
    # Check for Visual Studio
    Write-Host 'Checking for Visual Studio...' -ForegroundColor Yellow
    if (-not (Test-Path 'C:\Program Files\Microsoft Visual Studio')) {
        Write-Host 'WARNING: Visual Studio not found in default location' -ForegroundColor Red
        Write-Host 'Build may fail without Visual Studio C++ tools' -ForegroundColor Red
        Write-Host ''
    }
    
    # Build the extension
    Write-Host 'Building C++ extension...' -ForegroundColor Yellow
    cd mssql_python\pybind
    
    if (-not (Test-Path 'build.bat')) {
        Write-Host 'ERROR: build.bat not found!' -ForegroundColor Red
        exit 1
    }
    
    & cmd /c 'build.bat x64 2>&1'
    if (\$LASTEXITCODE -ne 0) {
        Write-Host 'ERROR: Build failed!' -ForegroundColor Red
        Write-Host ''
        Write-Host 'Troubleshooting:' -ForegroundColor Yellow
        Write-Host '1. Install Visual Studio 2019+ with C++ build tools' -ForegroundColor Gray
        Write-Host '2. Open Visual Studio Developer Command Prompt' -ForegroundColor Gray
        Write-Host '3. Run: cd \"$WINDOWS_PATH\mssql_python\pybind\" && build.bat x64' -ForegroundColor Gray
        exit 1
    }
    
    cd ..\..
    Write-Host '✓ Build completed successfully' -ForegroundColor Green
    Write-Host ''
    
    # Install the package
    Write-Host 'Step 2: Installing package in development mode...' -ForegroundColor Yellow
    python -m pip install -e . 2>&1 | Out-Null
    if (\$LASTEXITCODE -ne 0) {
        Write-Host 'ERROR: Installation failed!' -ForegroundColor Red
        exit 1
    }
    Write-Host '✓ Package installed' -ForegroundColor Green
    Write-Host ''
    
    # Verify extension loads
    Write-Host 'Step 3: Verifying C++ extension...' -ForegroundColor Yellow
    python -c 'import mssql_python; print(f\"Version: {mssql_python.__version__}\"); from mssql_python.ddbc_bindings import Connection; print(\"✓ C++ extension loaded\")'
    if (\$LASTEXITCODE -ne 0) {
        Write-Host 'ERROR: Extension verification failed!' -ForegroundColor Red
        exit 1
    }
    Write-Host ''
    
    # Install test dependencies
    Write-Host 'Step 4: Installing test dependencies...' -ForegroundColor Yellow
    python -m pip install pytest sqlalchemy greenlet typing-extensions 2>&1 | Out-Null
    Write-Host '✓ Dependencies installed' -ForegroundColor Green
    Write-Host ''
    
    # Update connection string in test file
    Write-Host 'Step 5: Updating connection string...' -ForegroundColor Yellow
    \$connStr = \"Driver={ODBC Driver 18 for SQL Server};Server=$SQL_SERVER;Database=$DATABASE;UID=$USERNAME;PWD=$PASSWORD;Encrypt=No\"
    \$testFile = Get-Content 'test_windows_segfault_simple.py' -Raw
    \$testFile = \$testFile -replace 'CONN_STR = \".*?\"', \"CONN_STR = \`\"\$connStr\`\"\"
    \$testFile | Set-Content 'test_windows_segfault_simple.py' -NoNewline
    Write-Host '✓ Connection string updated' -ForegroundColor Green
    Write-Host ''
    
    # Run the simple test
    Write-Host 'Step 6: Running Windows segfault test...' -ForegroundColor Yellow
    Write-Host '========================================' -ForegroundColor Cyan
    python test_windows_segfault_simple.py
    \$testResult = \$LASTEXITCODE
    Write-Host '========================================' -ForegroundColor Cyan
    Write-Host ''
    
    if (\$testResult -eq 0) {
        Write-Host '✓✓✓ ALL TESTS PASSED! ✓✓✓' -ForegroundColor Green
        Write-Host 'The fix is working correctly on Windows.' -ForegroundColor Green
    } else {
        Write-Host '✗✗✗ TESTS FAILED! ✗✗✗' -ForegroundColor Red
        Write-Host 'The segfault issue still exists.' -ForegroundColor Red
    }
    
    exit \$testResult
"

EXIT_CODE=$?

echo ""
echo "=========================================="
if [ $EXIT_CODE -eq 0 ]; then
    echo "✓ Windows test completed successfully!"
else
    echo "✗ Windows test failed with exit code: $EXIT_CODE"
fi
echo "=========================================="

exit $EXIT_CODE
