---
description: "Run pytest for mssql-python driver"
name: "test"
argument-hint: "What to test? (all/specific file/with coverage)"
agent: 'agent'
model: 'claude-sonnet-4'
tools:
  - terminalLastCommand
  - codebase
---
# Run Tests Prompt for microsoft/mssql-python

You are a development assistant helping run pytest for the mssql-python driver.

## PREREQUISITES

Before running tests, you MUST complete these checks **in order**:

### Step 1: Activate Virtual Environment

First, check if a venv is already active:

```bash
echo $VIRTUAL_ENV
```

**If a path is shown:** ✅ venv is active, skip to Step 2.

**If empty:** Look for existing venv directories:

```bash
ls -d myvenv venv .venv testenv 2>/dev/null | head -1
```

- **If found:** Activate it:
  ```bash
  source <venv-name>/bin/activate
  ```

- **If not found:** Ask the developer:
  > "No virtual environment found. Would you like me to:
  > 1. Create a new venv called `myvenv`
  > 2. Use a different venv (tell me the path)
  > 3. You'll activate it yourself"

  To create a new venv:
  ```bash
  python3 -m venv myvenv && source myvenv/bin/activate && pip install -r requirements.txt pytest pytest-cov && pip install -e .
  ```

Verify venv is active:
```bash
echo $VIRTUAL_ENV
# Expected: /path/to/mssql-python/<venv-name>
```

### Step 2: Verify pytest is Installed

```bash
python -c "import pytest; print('✅ pytest ready:', pytest.__version__)"
```

**If this fails:**
```bash
pip install pytest pytest-cov
```

### Step 3: Verify Database Connection String

```bash
if [ -n "$DB_CONNECTION_STRING" ]; then echo "✅ Connection string is set"; else echo "❌ Not set"; fi
```

**If not set:** Ask the developer for their connection string:

> "I need your database connection string to run tests. Please provide the connection details:
> - Server (e.g., localhost, your-server.database.windows.net)
> - Database name
> - Username
> - Password
> 
> Or provide the full connection string if you have one."

Once the developer provides the details, set it:

```bash
export DB_CONNECTION_STRING="Driver={ODBC Driver 18 for SQL Server};Server=<SERVER>;Database=<DATABASE>;UID=<USERNAME>;PWD=<PASSWORD>;TrustServerCertificate=yes"
```

> ⚠️ **SECURITY:** `TrustServerCertificate=yes` is for local development only. Never use in production.

---

## TASK

Help the developer run tests to validate their changes. Follow this process based on what they need.

---

## STEP 1: Choose What to Test

### Test Categories

| Category | Description | When to Use |
|----------|-------------|-------------|
| **All tests** | Full test suite (excluding stress) | Before creating PR |
| **Specific file** | Single test file | Testing one area |
| **Specific test** | Single test function | Debugging a failure |
| **Stress tests** | Long-running, resource-intensive | Performance validation |
| **With coverage** | Tests + coverage report | Checking coverage |

### Ask the Developer

> "What would you like to test?"
> 1. **All tests** - Run full suite (recommended before PR)
> 2. **Specific tests** - Tell me which file(s) or test name(s)
> 3. **With coverage** - Generate coverage report

---

## STEP 2: Run Tests

### Option A: Run All Tests (Default - Excludes Stress Tests)

```bash
# From repository root
python -m pytest -v

# This automatically applies: -m "not stress" (from pytest.ini)
```

### Option B: Run All Tests Including Stress Tests

```bash
python -m pytest -v -m ""
```

### Option C: Run Only Stress Tests

```bash
python -m pytest -v -m stress
```

### Option D: Run Specific Test File

```bash
# Single file
python -m pytest tests/test_003_connection.py -v

# Multiple files
python -m pytest tests/test_003_connection.py tests/test_004_cursor.py -v
```

### Option E: Run Specific Test Function

```bash
# Specific test
python -m pytest tests/test_003_connection.py::test_connection_basic -v

# Pattern matching
python -m pytest -k "connection" -v
python -m pytest -k "connection and not close" -v
```

### Option F: Run with Coverage

```bash
# Basic coverage
python -m pytest --cov=mssql_python -v

# Coverage with HTML report
python -m pytest --cov=mssql_python --cov-report=html -v

# Coverage with specific report location
python -m pytest --cov=mssql_python --cov-report=html:coverage_report -v
```

### Option G: Run Failed Tests Only (Re-run)

```bash
# Re-run only tests that failed in the last run
python -m pytest --lf -v

# Re-run failed tests first, then the rest
python -m pytest --ff -v
```

---

## STEP 3: Understanding Test Output

### Test Result Indicators

| Symbol | Meaning | Action |
|--------|---------|--------|
| `.` or `PASSED` | Test passed | ✅ Good |
| `F` or `FAILED` | Test failed | ❌ Fix needed |
| `E` or `ERROR` | Test error (setup/teardown) | ❌ Check fixtures |
| `s` or `SKIPPED` | Test skipped | ℹ️ Usually OK |
| `x` or `XFAIL` | Expected failure | ℹ️ Known issue |
| `X` or `XPASS` | Unexpected pass | ⚠️ Review |

### Example Output

```
tests/test_003_connection.py::test_connection_basic PASSED    [ 10%]
tests/test_003_connection.py::test_connection_close PASSED    [ 20%]
tests/test_004_cursor.py::test_cursor_execute FAILED          [ 30%]

====================== FAILURES ======================
________________ test_cursor_execute _________________

    def test_cursor_execute(cursor):
>       cursor.execute("SELECT 1")
E       mssql_python.exceptions.DatabaseError: Connection failed

tests/test_004_cursor.py:25: DatabaseError
======================================================
```

---

## STEP 4: Test File Reference

### Test Files and What They Cover

| File | Purpose | Requires DB? |
|------|---------|--------------|
| `test_000_dependencies.py` | Dependency checks | No |
| `test_001_globals.py` | Global state | No |
| `test_002_types.py` | Type conversions | No |
| `test_003_connection.py` | Connection lifecycle | **Yes** |
| `test_004_cursor.py` | Cursor operations | **Yes** |
| `test_005_connection_cursor_lifecycle.py` | Lifecycle management | **Yes** |
| `test_006_exceptions.py` | Error handling | Mixed |
| `test_007_logging.py` | Logging functionality | No |
| `test_008_auth.py` | Authentication | **Yes** |

---

## Troubleshooting

### ❌ "ModuleNotFoundError: No module named 'mssql_python'"

**Cause:** Package not installed in development mode

**Fix:**
```bash
pip install -e .
```

### ❌ "ModuleNotFoundError: No module named 'pytest'"

**Cause:** pytest not installed or venv not active

**Fix:**
```bash
# Check venv is active
echo $VIRTUAL_ENV

# If empty, activate it (run `#01-setup-dev-env`)
# If active, install pytest
pip install pytest pytest-cov
```

### ❌ "Connection failed" or "Login failed"

**Cause:** Invalid or missing `DB_CONNECTION_STRING`

**Fix:**
```bash
# Check the environment variable is set
echo $DB_CONNECTION_STRING

# Set it with correct values (LOCAL DEVELOPMENT ONLY)
# WARNING: Never commit real credentials. TrustServerCertificate=yes is for local dev only.
export DB_CONNECTION_STRING="Driver={ODBC Driver 18 for SQL Server};Server=localhost;Database=testdb;UID=sa;PWD=YourPassword;TrustServerCertificate=yes"
```

### ❌ "Timeout error"

**Cause:** Database server not reachable

**Fix:**
- Check server is running
- Verify network connectivity
- Check firewall rules
- Increase timeout: add `Connection Timeout=60` to connection string

### ❌ Tests hang indefinitely

**Cause:** Connection pool issues, deadlocks, or waiting for unavailable DB

**Fix:**
```bash
# Run with timeout
pip install pytest-timeout
python -m pytest --timeout=60 -v

# Run single test in isolation
python -m pytest tests/test_specific.py::test_name -v -s

# Skip integration tests if no DB available
python -m pytest tests/test_000_dependencies.py tests/test_001_globals.py tests/test_002_types.py tests/test_007_logging.py -v
```

### ❌ "ddbc_bindings" import error

**Cause:** C++ extension not built or Python version mismatch

**Fix:**
Use `#02-build-ddbc` to rebuild the extension:
```bash
cd mssql_python/pybind && ./build.sh && cd ../..
python -c "from mssql_python import connect; print('OK')"
```

### ❌ Tests pass locally but fail in CI

**Cause:** Environment differences (connection string, Python version, OS)

**Fix:**
- Check CI logs for specific error
- Ensure `DB_CONNECTION_STRING` is set in CI secrets
- Verify Python version matches CI

### ❌ Coverage report shows 0%

**Cause:** Package not installed or wrong source path

**Fix:**
```bash
# Reinstall in dev mode
pip install -e .

# Run with correct package path
python -m pytest --cov=mssql_python --cov-report=term-missing -v
```

### ❌ "collected 0 items"

**Cause:** pytest can't find tests (wrong directory or pattern)

**Fix:**
```bash
# Ensure you're in repository root
pwd  # Should be /path/to/mssql-python

# Check tests directory exists
ls tests/

# Run with explicit path
python -m pytest tests/ -v
```

---

## Quick Reference

### Common Commands

```bash
# Run all tests (default, excludes stress)
python -m pytest -v

# Run specific file
python -m pytest tests/test_003_connection.py -v

# Run with keyword filter
python -m pytest -k "connection" -v

# Run with coverage
python -m pytest --cov=mssql_python -v

# Run failed tests only
python -m pytest --lf -v

# Run with output capture disabled (see print statements)
python -m pytest -v -s

# Run with max 3 failures then stop
python -m pytest -v --maxfail=3

# Run with debugging on failure
python -m pytest -v --pdb
```

### pytest.ini Configuration

The project uses these default settings in `pytest.ini`:

```ini
[pytest]
markers =
    stress: marks tests as stress tests (long-running, resource-intensive)

# Default: Skips stress tests
addopts = -m "not stress"
```

---

## After Running Tests

Based on test results:

1. **All passed** → Ready to create/update PR → Use `#04-create-pr`
2. **Some failed** → Review failures, fix issues, re-run
3. **Coverage decreased** → Add tests for new code paths
4. **Need to debug** → Use `-s` flag to see print output, or `--pdb` to drop into debugger

> 💡 **Tip:** If you made C++ changes, ensure you've rebuilt using `#02-build-ddbc` first!
