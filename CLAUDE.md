# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**mssql-python** is Microsoft's Python driver for SQL Server and Azure SQL. It uses Direct Database Connectivity (DDBC) via a C++ pybind11 extension (`ddbc_bindings`) instead of an external ODBC driver manager. It's DB API 2.0 compliant (Python >= 3.10).

## Architecture

The driver has two layers:
- **Python layer** (`mssql_python/`): DB API 2.0 interface — `connection.py`, `cursor.py`, `db_connection.py`, `exceptions.py`, `pooling.py`, `type.py`, `auth.py`, `connection_string_parser.py`
- **C++ layer** (`mssql_python/pybind/`): pybind11 extension providing low-level ODBC connectivity. Key files: `ddbc_bindings.cpp`, `connection/connection.cpp`, `connection/connection_pool.cpp`, `logger_bridge.cpp`
- **Native core** (`mssql_py_core/`): Pre-built native extension extracted by `eng/scripts/install-mssql-py-core`. Not checked in — must be extracted before building wheels.
- **Platform libs** (`mssql_python/libs/`): Platform-specific native libraries (Windows/macOS/Linux)

Entry point: `mssql_python.connect()` (from `db_connection.py`) creates connections. `Cursor` handles query execution. `PoolingManager` manages connection pooling (enabled by default).

## Common Commands

### Dev Environment Setup
```bash
python -m venv myvenv && source myvenv/bin/activate
pip install -r requirements.txt && pip install -e .
```

### Build C++ Extension (after modifying .cpp/.h files)
```bash
cd mssql_python/pybind && ./build.sh && cd ../..    # macOS/Linux
cd mssql_python\pybind && build.bat && cd ..\..      # Windows
```

### Run Tests
```bash
# Requires DB_CONNECTION_STRING env var for integration tests
export DB_CONNECTION_STRING="SERVER=localhost;DATABASE=testdb;UID=sa;PWD=YourPassword;Encrypt=yes;TrustServerCertificate=yes"

python -m pytest -v                                    # All tests (excludes stress)
python -m pytest tests/test_003_connection.py -v       # Single file
python -m pytest tests/test_003_connection.py::test_connection_basic -v  # Single test
python -m pytest -k "connection" -v                    # Pattern match
python -m pytest -v -m ""                              # Include stress tests
python -m pytest --cov=mssql_python -v                 # With coverage
```

Do NOT include `Driver=` in connection strings — the driver adds it automatically.

### Linting / Formatting
```bash
black --check .          # Format check (line-length 100)
flake8 .                 # Lint
pylint mssql_python/     # Static analysis
```

### Verify Build
```bash
python -c "from mssql_python import connect; print('OK')"
```

## Code Style

- Max line length: 100 (black, flake8, pylint all configured)
- Formatter: black
- Python >= 3.10

## PR Conventions

- PR title **must** start with: `FEAT:`, `FIX:`, `DOC:`, `CHORE:`, `STYLE:`, `REFACTOR:`, or `RELEASE:`
- PR description must include `### Summary` (min 10 chars) and a work item/issue link
- Branch naming: `<name>/<type>/<description>` (e.g., `bewithgaurav/feat/connection-timeout`)
- Never commit binary files (`.dylib`, `.so`, `.pyd`, `.dll`) unless explicitly needed

## Test Structure

Tests in `tests/` are numbered by area. Tests `000-002`, `007` don't require a database. Tests `003+` generally require a live SQL Server via `DB_CONNECTION_STRING`. Fixtures in `conftest.py` provide `conn_str`, `db_connection`, and `cursor`. Stress tests are marked with `@pytest.mark.stress` and excluded by default.
