# Copilot Instructions for mssql-python

## Repository Overview

**mssql-python** is a Python driver for Microsoft SQL Server and Azure SQL databases that leverages Direct Database Connectivity (DDBC). It's built using **pybind11** and **CMake** to create native extensions, providing DB API 2.0 compliant database access with enhanced Pythonic features.

- **Size**: Medium-scale project (~750KB total)
- **Languages**: Python (main), C++ (native bindings), CMake (build system)
- **Target Platforms**: Windows (x64, ARM64), macOS (Universal2), Linux (x86_64, ARM64)
- **Python Versions**: 3.10+
- **Key Dependencies**: pybind11, azure-identity, Microsoft ODBC Driver 18

## Development Workflows

This repository includes detailed prompt files for common tasks. Reference these with `#`:

| Task | Prompt | When to Use |
|------|--------|-------------|
| First-time setup | `#setup-dev-env` | New machine, fresh clone |
| Build C++ extension | `#build-ddbc` | After modifying .cpp/.h files |
| Run tests | `#run-tests` | Validating changes |
| Create PR | `#create-pr` | Ready to submit changes |

**Workflow order for new contributors:**
1. `#setup-dev-env` — Set up venv and dependencies
2. `#build-ddbc` — Build native extension
3. Make your changes
4. `#run-tests` — Validate
5. `#create-pr` — Submit

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
# Python formatting
black --check --line-length=100 mssql_python/ tests/

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
├── ddbc_bindings.py               # Platform-specific extension loader with architecture detection
├── mssql_python.pyi               # Type stubs for IDE support
├── py.typed                       # PEP 561 type marker
└── pybind/                        # Native extension source
    ├── ddbc_bindings.cpp          # Main C++ binding code
    ├── ddbc_bindings.h            # Header for bindings
    ├── CMakeLists.txt             # Cross-platform build configuration
    ├── build.sh/.bat              # Platform-specific build scripts
    ├── configure_dylibs.sh        # macOS dylib configuration
    ├── logger_bridge.cpp/.hpp     # Python logging bridge
    ├── unix_utils.cpp/.h          # Unix platform utilities
    └── connection/                # Connection management
        ├── connection.cpp/.h      # Connection implementation
        └── connection_pool.cpp/.h # Connection pooling
```

### Platform-Specific Libraries

```
mssql_python/libs/
├── windows/{x64,x86,arm64}/          # Windows ODBC drivers and dependencies
├── macos/{arm64,x86_64}/lib/         # macOS dylibs
└── linux/{debian_ubuntu,rhel,suse,alpine}/{x86_64,arm64}/lib/  # Linux distributions
```

### Configuration Files

- **`.clang-format`**: C++ formatting (LLVM style, 100 column limit)
- **`.coveragerc`**: Coverage configuration
- **`requirements.txt`**: Development dependencies
- **`setup.py`**: Package configuration with platform detection
- **`pyproject.toml`**: Modern Python packaging configuration
- **`.gitignore`**: Excludes build artifacts (*.so, *.pyd, build/, __pycache__)

## CI/CD Pipeline Details

### GitHub Workflows
- **`devskim.yml`**: Security scanning (runs on PRs and main)
- **`pr-format-check.yml`**: PR validation (title format, GitHub issue/ADO work item links)
- **`lint-check.yml`**: Python (Black) and C++ (clang-format) linting
- **`pr-code-coverage.yml`**: Code coverage reporting
- **`forked-pr-coverage.yml`**: Coverage for forked PRs

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

The `ddbc_bindings.py` module handles architecture detection:
- **Windows**: Normalizes `win64/amd64/x64` → `x64`, `win32/x86` → `x86`, `arm64` → `arm64`
- **macOS**: Runtime architecture detection, always loads from universal2 binary
- **Linux**: Maps `x64/amd64` → `x86_64`, `arm64/aarch64` → `arm64`

## Exception Hierarchy

Critical for error handling guidance:

```
Exception (base)
├── Warning
└── Error
    ├── InterfaceError          # Driver/interface issues
    └── DatabaseError
        ├── DataError            # Invalid data processing
        ├── OperationalError     # Connection/timeout issues
        ├── IntegrityError       # Constraint violations
        ├── InternalError        # Internal driver/database errors
        ├── ProgrammingError     # SQL syntax errors
        └── NotSupportedError    # Unsupported features/operations
```

## Critical Anti-Patterns (DO NOT)

- **NEVER** hardcode connection strings - always use `DB_CONNECTION_STRING` env var for tests
- **NEVER** use `pyodbc` imports - this driver doesn't require external ODBC
- **NEVER** modify files in `mssql_python/libs/` - these are pre-built binaries
- **NEVER** skip `conn.commit()` after INSERT/UPDATE/DELETE operations
- **NEVER** use bare `except:` blocks - always catch specific exceptions
- **NEVER** leave connections open - use context managers or explicit `close()`

## When Modifying Code

### Python Changes
- Preserve existing error handling patterns from `exceptions.py`
- Use context managers (`with`) for all connection/cursor operations
- Update `__all__` exports if adding public API
- Add corresponding test in `tests/test_*.py`
- Follow Black formatting (line length 100)

### C++ Changes
- Follow RAII patterns for resource management
- Use `py::gil_scoped_release` for blocking ODBC operations
- Update `mssql_python.pyi` type stubs if changing Python API
- Follow `.clang-format` style (LLVM style, 100 column limit)

## Code Quality

- **Keep it tight**: Minimal code to solve the problem. No duplicate logic, no redundant validation.
- **Comments explain why, not what**: Don't restate what the code does. Comment intent, edge cases, and non-obvious decisions.
- **One-line docstrings**: For test functions and simple helpers. No "Validates:" bullet lists or "Note:" paragraphs.
- **Docstring examples must match the API**: If the signature changes, update the examples.
- **Catch specific exceptions**: Use `DatabaseError`, `IntegrityError`, etc. — never `except Exception` or bare `except:`.
- **Let the test framework work**: Don't wrap test bodies in `try/except: pytest.fail()`. Let pytest show the real traceback.
- **Assertions must match claims**: If a test says "all types", check all of them. Cover the case that motivated the fix.
- **No stale references**: If you reference a file, function, or prompt — verify it exists first.

## Debugging Quick Reference

| Error | Cause | Solution |
|-------|-------|----------|
| `ImportError: ddbc_bindings` | Extension not built | Run `#build-ddbc` |
| Connection timeout | Missing env var | Set `DB_CONNECTION_STRING` |
| `dylib not found` (macOS) | Library paths | Run `configure_dylibs.sh` |
| `ODBC Driver not found` | Missing driver | Install Microsoft ODBC Driver 18 |
| `ModuleNotFoundError` | Not in venv | Run `#setup-dev-env` |

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

