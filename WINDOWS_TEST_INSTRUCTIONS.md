# Windows Segfault Fix - Testing Instructions

## Prerequisites

1. **Windows OS** (tested on Windows 10/11)
2. **Python 3.12 or 3.13** installed
3. **Visual Studio 2019 or later** with C++ build tools
4. **SQL Server** (LocalDB, Express, or full) running locally or accessible
5. **ODBC Driver 18 for SQL Server** installed

## Setup Steps

### 1. Install Build Dependencies

Open PowerShell as Administrator and install required tools:

```powershell
# Install Python (if not already installed)
# Download from: https://www.python.org/downloads/

# Install Visual Studio Build Tools
# Download from: https://visualstudio.microsoft.com/downloads/
# Select "Desktop development with C++" workload

# Install ODBC Driver 18
# Download from: https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server
```

### 2. Clone and Navigate to Repository

```powershell
cd C:\Users\YourUsername
git clone https://github.com/your-repo/mssql-python.git
cd mssql-python
git checkout subrata-ms/GCSegFault  # The branch with the fix
```

### 3. Build the C++ Extension with Fix

```powershell
# Navigate to pybind directory
cd mssql_python\pybind

# Build for your architecture (x64, x86, or arm64)
.\build.bat x64

# Return to root
cd ..\..
```

### 4. Install the Package

```powershell
# Install in development mode
pip install -e .

# Install testing dependencies
pip install pytest sqlalchemy greenlet typing-extensions
```

### 5. Verify Installation

```powershell
python -c "import mssql_python; print(f'Version: {mssql_python.__version__}'); from mssql_python.ddbc_bindings import Connection; print('C++ extension loaded!')"
```

Expected output:
```
Version: 1.2.0
C++ extension loaded!
```

## Running Tests

### Option 1: Simple Python Test (Recommended for Quick Validation)

1. **Update connection string** in `test_windows_segfault_simple.py`:
   ```python
   CONN_STR = "Driver={ODBC Driver 18 for SQL Server};Server=localhost;Database=test;UID=sa;PWD=YourPassword;Encrypt=No"
   ```

2. **Run the test**:
   ```powershell
   python test_windows_segfault_simple.py
   ```

3. **Expected output**:
   ```
   ============================================================
   SUCCESS: No segfault detected!
   The fix is working correctly.
   ============================================================
   ```

### Option 2: PowerShell Script (Full Test Suite)

```powershell
# Run with your SQL Server credentials
.\test_windows_segfault.ps1 -SqlServer "localhost" -Database "test" -Username "sa" -Password "YourPassword" -TestRuns 10
```

### Option 3: Run Local Unit Tests

```powershell
# Run the connection invalidation test
pytest tests\test_016_connection_invalidation_segfault.py -v
```

### Option 4: Full SQLAlchemy Reconnect Tests

If you want to reproduce the exact scenario from the stack trace:

```powershell
# Clone SQLAlchemy (if not already done)
cd ..
git clone https://github.com/sqlalchemy/sqlalchemy.git
cd sqlalchemy

# Fetch the specific gerrit review with reconnect tests
git fetch https://gerrit.sqlalchemy.org/sqlalchemy/sqlalchemy refs/changes/49/6149/14
git checkout FETCH_HEAD

# Install SQLAlchemy
pip install -e .

# Run the reconnect tests
pytest test\engine\test_reconnect.py::RealReconnectTest --dburi "mssql+mssqlpython://sa:YourPassword@localhost/test?Encrypt=No" --disable-asyncio -v
```

## What the Tests Validate

### 1. **Cursor Destruction During Initialization**
- Tests the exact scenario from the stack trace
- Creates cursors and triggers GC during object initialization
- Ensures `__del__` doesn't cause segfaults when called prematurely

### 2. **Connection Invalidation**
- Closes connections without explicitly closing cursors
- Mimics SQLAlchemy's connection pool behavior
- Verifies child handles are properly marked as freed

### 3. **Threading Scenario**
- Tests cursor cleanup during lock creation (RLock scenario)
- Ensures thread-safety of the fix
- Validates no deadlocks occur

### 4. **Multiple Iterations**
- Runs 10+ iterations to ensure stability
- Catches intermittent issues
- Validates fix works consistently

## Expected Results

### ✅ Success Indicators
- All tests complete without crashes
- No segmentation faults
- No "Access Violation" errors
- Clean exit codes (0)

### ❌ Failure Indicators
- Python crashes with exit code
- "Access Violation" exceptions
- Tests hanging indefinitely
- Segmentation fault errors

## Troubleshooting

### Build Errors

**Error: "CMake not found"**
```powershell
# Install CMake
pip install cmake
```

**Error: "Visual Studio not found"**
- Install Visual Studio Build Tools with C++ support
- Or set environment variable: `$env:VS_PATH = "C:\Path\To\VS"`

**Error: "Python.h not found"**
- Ensure Python development headers are installed
- Reinstall Python with "Include development tools" option

### Runtime Errors

**Error: "ODBC Driver not found"**
```powershell
# Check installed ODBC drivers
Get-OdbcDriver
```

**Error: "Cannot connect to SQL Server"**
- Verify SQL Server is running: `Get-Service MSSQL*`
- Check firewall settings
- Test connection: `sqlcmd -S localhost -U sa -P YourPassword`

**Error: "Import failed: DLL load failed"**
- Ensure Visual C++ Redistributable is installed
- Check PATH includes Python and ODBC driver directories

## Comparing Before/After

### Test WITHOUT Fix (PyPI Version)

```powershell
# Install PyPI version
pip uninstall mssql-python -y
pip install mssql-python==1.2.0

# Run test - should crash
python test_windows_segfault_simple.py
```

Expected: **Crash/Segfault**

### Test WITH Fix (Your Build)

```powershell
# Install your fixed version
pip uninstall mssql-python -y
cd C:\path\to\mssql-python
pip install -e .

# Run test - should succeed
python test_windows_segfault_simple.py
```

Expected: **Success**

## Reporting Results

When reporting test results, include:

1. **Environment**:
   - Windows version
   - Python version (`python --version`)
   - ODBC Driver version
   - SQL Server version

2. **Test Output**:
   - Full console output
   - Any error messages
   - Stack traces (if crash occurs)

3. **Build Info**:
   - Architecture (x64/x86/arm64)
   - Compiler version
   - Build warnings/errors

4. **Comparison**:
   - Results with PyPI version (should fail)
   - Results with fixed version (should pass)

## Additional Resources

- ODBC Driver Download: https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server
- Python Windows Installation: https://www.python.org/downloads/windows/
- Visual Studio Build Tools: https://visualstudio.microsoft.com/downloads/
- SQL Server Express: https://www.microsoft.com/en-us/sql-server/sql-server-downloads
