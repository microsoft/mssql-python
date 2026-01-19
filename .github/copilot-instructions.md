# Copilot Instructions for mssql-python

## Repository Overview

**mssql-python** is a Python driver for Microsoft SQL Server and Azure SQL databases that leverages Direct Database Connectivity (DDBC). It's built using **pybind11** and **CMake** to create native extensions, providing DB API 2.0 compliant database access with enhanced Pythonic features.

- **Size**: Medium-scale project (~750KB total)
- **Languages**: Python (main), C++ (native bindings), CMake (build system)
- **Target Platforms**: Windows (x64, ARM64), macOS (Universal2), Linux (x86_64, ARM64)
- **Python Versions**: 3.10+
- **Key Dependencies**: pybind11, azure-identity, Microsoft ODBC Driver 18

## Usage Examples (For Suggesting to Users)

### Basic Connection and Query
```python
import mssql_python

conn = mssql_python.connect(
    "SERVER=localhost;DATABASE=mydb;Trusted_Connection=yes;Encrypt=yes;TrustServerCertificate=yes"
)
cursor = conn.cursor()
cursor.execute("SELECT * FROM users WHERE id = ?", (user_id,))
rows = cursor.fetchall()
cursor.close()
conn.close()
```

### Context Manager (Recommended)
```python
with mssql_python.connect(connection_string) as conn:
    with conn.cursor() as cursor:
        cursor.execute("SELECT @@VERSION")
        print(cursor.fetchone()[0])
```

### Azure SQL with Entra ID
```python
conn = mssql_python.connect(
    "SERVER=myserver.database.windows.net;DATABASE=mydb;"
    "Authentication=ActiveDirectoryInteractive;Encrypt=yes"
)
```

### Parameterized Queries
```python
# Positional parameters
cursor.execute("SELECT * FROM users WHERE email = ?", (email,))

# Named parameters (pyformat)
cursor.execute("SELECT * FROM users WHERE email = %(email)s", {"email": email})
```

### Insert with Commit
```python
cursor.execute("INSERT INTO users (name, email) VALUES (?, ?)", ("John", "john@example.com"))
conn.commit()
```

### Batch Insert
```python
cursor.executemany("INSERT INTO users (name, email) VALUES (?, ?)", [
    ("Alice", "alice@example.com"),
    ("Bob", "bob@example.com"),
])
conn.commit()
```

### Transaction Handling
```python
try:
    cursor.execute("UPDATE accounts SET balance = balance - 100 WHERE id = ?", (from_id,))
    cursor.execute("UPDATE accounts SET balance = balance + 100 WHERE id = ?", (to_id,))
    conn.commit()
except Exception:
    conn.rollback()
    raise
```

## Build System and Validation

### Prerequisites
**Always install these before building:**
```bash
# All platforms
pip install -r requirements.txt

# Windows: Requires Visual Studio Build Tools with "Desktop development with C++" workload
# macOS: brew install cmake && brew install msodbcsql18
# Linux: Install cmake, python3-dev, and ODBC driver per distribution
```

### Building the Project

**CRITICAL**: The project requires building native extensions before testing. Extensions are platform-specific (`.pyd` on Windows, `.so` on macOS/Linux).

#### Windows Build:
```bash
cd mssql_python/pybind
build.bat [x64|x86|arm64]  # Defaults to x64 if not specified
```

#### macOS Build:
```bash
cd mssql_python/pybind
./build.sh  # Creates universal2 binary (ARM64 + x86_64)
```

#### Linux Build:
```bash
cd mssql_python/pybind
./build.sh  # Detects architecture automatically
```

**Build Output**: Creates `ddbc_bindings.cp{python_version}-{architecture}.{so|pyd}` in the `mssql_python/` directory.

### Testing

**IMPORTANT**: Tests require a SQL Server connection via `DB_CONNECTION_STRING` environment variable.

```bash
# Run all tests with coverage
python -m pytest -v --cov=. --cov-report=xml --capture=tee-sys --cache-clear

# Run specific test files
python -m pytest tests/test_000_dependencies.py -v  # Dependency checks
python -m pytest tests/test_001_globals.py -v      # Basic functionality
```

**Test Dependencies**: Tests require building the native extension first. The dependency test (`test_000_dependencies.py`) validates that all platform-specific libraries exist.

### Linting and Code Quality

```bash
# Python formatting (use black 26.1.0 to match CI)
pip install black==26.1.0
black --check .

# C++ formatting
clang-format -style=file -i mssql_python/pybind/*.cpp mssql_python/pybind/*.h

# Coverage reporting (configured in .coveragerc)
python -m pytest --cov=. --cov-report=html
```

## Project Architecture

### Core Components

```
mssql_python/
├── __init__.py                    # Package initialization, connection registry, cleanup
├── connection.py                  # DB API 2.0 connection object
├── cursor.py                      # DB API 2.0 cursor object
├── db_connection.py               # connect() function implementation
├── auth.py                        # Microsoft Entra ID authentication
├── pooling.py                     # Connection pooling implementation
├── logging.py                     # Logging configuration
├── exceptions.py                  # Exception hierarchy
├── connection_string_builder.py   # Connection string construction
├── connection_string_parser.py    # Connection string parsing
├── parameter_helper.py            # Query parameter handling
├── row.py                         # Row object implementation
├── type.py                        # DB API 2.0 type objects
├── constants.py                   # ODBC constants
├── helpers.py                     # Utility functions and settings
└── pybind/                        # Native extension source
    ├── ddbc_bindings.cpp          # Main C++ binding code
    ├── CMakeLists.txt             # Cross-platform build configuration
    ├── build.sh/.bat              # Platform-specific build scripts
    └── configure_dylibs.sh        # macOS dylib configuration
```

### Platform-Specific Libraries

```
mssql_python/libs/
├── windows/{x64,x86,arm64}/          # Windows ODBC drivers and dependencies
├── macos/{arm64,x86_64}/lib/         # macOS dylibs
└── linux/{debian_ubuntu,rhel,suse,alpine}/{x86_64,arm64}/lib/  # Linux distributions
```

### Configuration Files

- **`.clang-format`**: C++ formatting (Google style, 100 column limit)
- **`.coveragerc`**: Coverage exclusions (main.py, setup.py, tests/)
- **`requirements.txt`**: Development dependencies (pytest, pybind11, coverage)
- **`setup.py`**: Package configuration with platform detection
- **`pyproject.toml`**: Modern Python packaging configuration
- **`.gitignore`**: Excludes build artifacts (*.so, *.pyd, build/, __pycache__)

## CI/CD Pipeline Details

### GitHub Workflows
- **`devskim.yml`**: Security scanning (runs on PRs and main)
- **`pr-format-check.yml`**: PR validation (title format, GitHub issue/ADO work item links)

### Azure DevOps Pipelines (`eng/pipelines/`)
- **`pr-validation-pipeline.yml`**: Comprehensive testing across all platforms
- **`build-whl-pipeline.yml`**: Wheel building for distribution
- **Platform Coverage**: Windows (LocalDB), macOS (Docker SQL Server), Linux (Ubuntu, Debian, RHEL, Alpine) with both x86_64 and ARM64

### Build Matrix
The CI system tests:
- **Python versions**: 3.10, 3.11, 3.12, 3.13
- **Windows**: x64, ARM64 architectures
- **macOS**: Universal2 (ARM64 + x86_64)
- **Linux**: Multiple distributions (Debian, Ubuntu, RHEL, Alpine) on x86_64 and ARM64

## Common Build Issues and Workarounds

### macOS-Specific Issues
- **dylib path configuration**: Run `configure_dylibs.sh` after building to fix library paths
- **codesigning**: Script automatically codesigns libraries for compatibility

### Linux Distribution Differences
- **Debian/Ubuntu**: Use `apt-get install python3-dev cmake pybind11-dev`
- **RHEL**: Requires enabling CodeReady Builder repository for development tools
- **Alpine**: Uses musl libc, requires special handling in build scripts

### Windows Build Dependencies
- **Visual Studio Build Tools**: Must include "Desktop development with C++" workload
- **Architecture Detection**: Build scripts auto-detect target architecture from environment

### Known Limitations (from TODOs)
- Linux RPATH configuration pending for driver .so files
- Some Unicode support gaps in executemany operations
- Platform-specific test dependencies in exception handling

## Architecture Detection and Loading

The `__init__.py` module implements sophisticated architecture detection:
- **Windows**: Normalizes `win64/amd64/x64` → `x64`, `win32/x86` → `x86`, `arm64` → `arm64`
- **macOS**: Runtime architecture detection, always loads from universal2 binary
- **Linux**: Maps `x64/amd64` → `x86_64`, `arm64/aarch64` → `arm64`

## Contributing Guidelines

### PR Requirements
- **Title Format**: Must start with `FEAT:`, `CHORE:`, `FIX:`, `DOC:`, `STYLE:`, `REFACTOR:`, or `RELEASE:`
- **Issue Linking**: Must link to either GitHub issue or ADO work item
- **Summary**: Minimum 10 characters of meaningful content under "### Summary"

### Development Workflow
1. **Always build native extensions first** before running tests
2. **Use virtual environments** for dependency isolation
3. **Test on target platform** before submitting PRs
4. **Check CI pipeline results** for cross-platform compatibility

## Trust These Instructions

These instructions are comprehensive and tested. Only search for additional information if:
- Build commands fail with unexpected errors
- New platform support is being added
- Dependencies or requirements have changed

For any ambiguity, refer to the platform-specific README in `mssql_python/pybind/README.md` or the comprehensive CI pipeline configurations in `eng/pipelines/`.
