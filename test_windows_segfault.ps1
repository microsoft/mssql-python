# PowerShell script to reproduce and test the segfault fix on Windows
# Run this from the mssql-python repository root

param(
    [string]$SqlServer = "localhost",
    [string]$Database = "test",
    [string]$Username = "sa",
    [string]$Password = "",
    [int]$TestRuns = 10
)

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Windows Segfault Reproduction Test" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Check Python version
$pythonVersion = python --version 2>&1
Write-Host "Python Version: $pythonVersion" -ForegroundColor Green
Write-Host ""

# Step 1: Build the C++ extension with the fix
Write-Host "Step 1: Building C++ extension with fix..." -ForegroundColor Yellow
Write-Host "-------------------------------------" -ForegroundColor Yellow

if (Test-Path "mssql_python\pybind\build") {
    Write-Host "Cleaning existing build directory..." -ForegroundColor Gray
    Remove-Item -Path "mssql_python\pybind\build" -Recurse -Force -ErrorAction SilentlyContinue
}

Push-Location "mssql_python\pybind"
Write-Host "Running build.bat..." -ForegroundColor Gray
$buildOutput = & cmd /c "build.bat" 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Build failed!" -ForegroundColor Red
    Write-Host $buildOutput
    Pop-Location
    exit 1
}
Write-Host "✓ C++ extension built successfully" -ForegroundColor Green
Pop-Location
Write-Host ""

# Step 2: Install the package in development mode
Write-Host "Step 2: Installing mssql-python with fix..." -ForegroundColor Yellow
Write-Host "-------------------------------------" -ForegroundColor Yellow
$installOutput = python -m pip install -e . 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Installation failed!" -ForegroundColor Red
    Write-Host $installOutput
    exit 1
}
Write-Host "✓ Package installed successfully" -ForegroundColor Green
Write-Host ""

# Step 3: Verify the C++ extension loads
Write-Host "Step 3: Verifying C++ extension..." -ForegroundColor Yellow
Write-Host "-------------------------------------" -ForegroundColor Yellow
$verifyScript = @"
import mssql_python
print('✓ mssql_python imported')
from mssql_python.ddbc_bindings import Connection
print('✓ C++ extension loaded successfully')
import sys
print(f'Python: {sys.version}')
print(f'mssql_python: {mssql_python.__version__}')
"@

$verifyOutput = python -c $verifyScript 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Extension verification failed!" -ForegroundColor Red
    Write-Host $verifyOutput
    exit 1
}
Write-Host $verifyOutput
Write-Host ""

# Step 4: Check if SQLAlchemy is available
Write-Host "Step 4: Checking SQLAlchemy installation..." -ForegroundColor Yellow
Write-Host "-------------------------------------" -ForegroundColor Yellow
$sqlalchemyCheck = python -c "import sqlalchemy; print(f'SQLAlchemy version: {sqlalchemy.__version__}')" 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "SQLAlchemy not found. Installing..." -ForegroundColor Yellow
    python -m pip install sqlalchemy pytest greenlet typing-extensions 2>&1 | Out-Null
    Write-Host "✓ SQLAlchemy installed" -ForegroundColor Green
} else {
    Write-Host $sqlalchemyCheck -ForegroundColor Green
}
Write-Host ""

# Step 5: Run the connection invalidation test
Write-Host "Step 5: Running connection invalidation test..." -ForegroundColor Yellow
Write-Host "-------------------------------------" -ForegroundColor Yellow

if ($Password -eq "") {
    Write-Host "ERROR: SQL Server password not provided!" -ForegroundColor Red
    Write-Host "Usage: .\test_windows_segfault.ps1 -SqlServer 'localhost' -Database 'test' -Username 'sa' -Password 'YourPassword'" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "Or set connection string and run basic test:" -ForegroundColor Yellow
    Write-Host "  `$env:TEST_CONN_STR = 'mssql+mssqlpython://user:pass@localhost/test'" -ForegroundColor Yellow
    exit 1
}

$connStr = "mssql+mssqlpython://${Username}:${Password}@${SqlServer}/${Database}?Encrypt=No"
Write-Host "Connection: mssql+mssqlpython://${Username}:***@${SqlServer}/${Database}" -ForegroundColor Gray
Write-Host ""

# Run the local test if it exists
if (Test-Path "tests\test_016_connection_invalidation_segfault.py") {
    Write-Host "Running local connection invalidation test..." -ForegroundColor Cyan
    $testResult = pytest tests\test_016_connection_invalidation_segfault.py -v --tb=short 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Test execution encountered issues:" -ForegroundColor Yellow
        Write-Host $testResult
    } else {
        Write-Host "✓ Local tests passed!" -ForegroundColor Green
    }
    Write-Host ""
}

# Step 6: Run multiple iterations of SQLAlchemy reconnect tests
Write-Host "Step 6: Running SQLAlchemy reconnect tests (${TestRuns} iterations)..." -ForegroundColor Yellow
Write-Host "-------------------------------------" -ForegroundColor Yellow

# Check if SQLAlchemy test directory exists
if (-not (Test-Path "..\sqlalchemy\test\engine\test_reconnect.py")) {
    Write-Host "SQLAlchemy test suite not found." -ForegroundColor Yellow
    Write-Host "To run full SQLAlchemy tests:" -ForegroundColor Yellow
    Write-Host "  1. Clone SQLAlchemy: git clone https://github.com/sqlalchemy/sqlalchemy.git" -ForegroundColor Gray
    Write-Host "  2. Install it: cd sqlalchemy && pip install -e ." -ForegroundColor Gray
    Write-Host "  3. Run: pytest test\engine\test_reconnect.py::RealReconnectTest --dburi '$connStr' -v" -ForegroundColor Gray
    Write-Host ""
} else {
    Write-Host "Running RealReconnectTest from SQLAlchemy..." -ForegroundColor Cyan
    
    $crashDetected = $false
    for ($i = 1; $i -le $TestRuns; $i++) {
        Write-Host "  Test Run $i/$TestRuns..." -ForegroundColor Gray
        
        Push-Location "..\sqlalchemy"
        $testOutput = pytest test\engine\test_reconnect.py::RealReconnectTest --dburi $connStr --disable-asyncio -v 2>&1
        $exitCode = $LASTEXITCODE
        Pop-Location
        
        if ($exitCode -ne 0) {
            Write-Host ""
            Write-Host "  ✗ CRASH DETECTED ON RUN $i" -ForegroundColor Red
            Write-Host $testOutput | Select-String -Pattern "Error|Exception|Segmentation"
            $crashDetected = $true
            break
        }
        
        Write-Host "  ✓ Run $i completed successfully" -ForegroundColor Green
        Start-Sleep -Seconds 1
    }
    
    Write-Host ""
    if (-not $crashDetected) {
        Write-Host "========================================" -ForegroundColor Green
        Write-Host "SUCCESS! No crash in $TestRuns runs." -ForegroundColor Green
        Write-Host "The fix appears to be working!" -ForegroundColor Green
        Write-Host "========================================" -ForegroundColor Green
    } else {
        Write-Host "========================================" -ForegroundColor Red
        Write-Host "FIX DID NOT WORK - Segfault occurred!" -ForegroundColor Red
        Write-Host "========================================" -ForegroundColor Red
    }
}

Write-Host ""
Write-Host "Test completed." -ForegroundColor Cyan
