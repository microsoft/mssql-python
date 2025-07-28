# Build Guide

This guide provides comprehensive instructions for building and developing the Microsoft Python Driver for SQL Server (`mssql-python`) from source. It covers all supported platforms and explains how to build the native pybind bindings, create Python wheels, and run tests.

## Prerequisites

### Python Requirements
- **Python 3.10 or higher** is required
- Use the standard `python` and `pip` commands (no Anaconda or pyenv dependencies)

### General Dependencies
Install the required Python packages:
```bash
pip install -r requirements.txt
```

This will install:
- `pytest` - Testing framework
- `pytest-cov` - Test coverage reporting
- `pybind11` - Python/C++ binding library
- `coverage` - Code coverage measurement
- `unittest-xml-reporting` - XML test reporting
- `setuptools` - Package building tools

## Platform-Specific Prerequisites

### Windows

#### Required Software
1. **Visual Studio Build Tools 2022** or **Visual Studio 2022** with C++ support
   - Download from: https://visualstudio.microsoft.com/downloads/
   - During installation, select the **"Desktop development with C++"** workload
   - This automatically includes CMake

2. **PyBind11**:
   ```cmd
   pip install pybind11
   ```

#### Architecture Support
- x64 (64-bit Intel/AMD)
- x86 (32-bit Intel/AMD) 
- ARM64 (64-bit ARM)

### macOS

#### Required Software
1. **Xcode Command Line Tools**:
   ```bash
   xcode-select --install
   ```

2. **CMake and PyBind11**:
   ```bash
   brew install cmake
   pip install pybind11
   ```

3. **Microsoft ODBC Driver 18**:
   ```bash
   brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
   ACCEPT_EULA=Y brew install msodbcsql18
   ```

   > **Note**: This provides development headers (`sql.h`, `sqlext.h`) and the dynamic library (`libmsodbcsql.18.dylib`) required for building native bindings.

#### Architecture Support
- Universal2 binaries (ARM64 + x86_64 combined)

### Linux

#### Required Software
1. **Build essentials**:
   ```bash
   # Ubuntu/Debian
   sudo apt-get update
   sudo apt-get install build-essential cmake python3-dev

   # RHEL/CentOS/Fedora
   sudo yum groupinstall "Development Tools"
   sudo yum install cmake python3-devel
   ```

2. **PyBind11**:
   ```bash
   pip install pybind11
   ```

#### Architecture Support
- x86_64 (64-bit Intel/AMD)
- ARM64/aarch64 (64-bit ARM)

#### Distribution Support
- Ubuntu/Debian (manylinux2014)
- RHEL/CentOS/Fedora (manylinux2014)

## Building Native Pybind Bindings

The native bindings are located in `mssql_python/pybind/` and are built using CMake and pybind11.

### Windows

1. **Open Developer Command Prompt for VS 2022**

2. **Navigate to the pybind directory**:
   ```cmd
   cd mssql_python\pybind
   ```

3. **Run the build script**:
   ```cmd
   build.bat [ARCHITECTURE]
   ```
   
   Where `[ARCHITECTURE]` can be:
   - `x64` (default if not specified)
   - `x86` 
   - `arm64`

   Examples:
   ```cmd
   build.bat          # Builds for x64
   build.bat x64      # Builds for x64
   build.bat arm64    # Builds for ARM64
   ```

### macOS

1. **Navigate to the pybind directory**:
   ```bash
   cd mssql_python/pybind
   ```

2. **Run the build script**:
   ```bash
   ./build.sh
   ```

   This automatically builds universal2 binaries (ARM64 + x86_64).

### Linux

1. **Navigate to the pybind directory**:
   ```bash
   cd mssql_python/pybind
   ```

2. **Run the build script**:
   ```bash
   ./build.sh
   ```

   The script automatically detects your system architecture.

### Build Output

After successful compilation, you'll find the native binding file in the `mssql_python/` directory:

- **Windows**: `ddbc_bindings.cp{python_version}-{architecture}.pyd`
  - Example: `ddbc_bindings.cp312-amd64.pyd`
- **macOS**: `ddbc_bindings.cp{python_version}-universal2.so`
  - Example: `ddbc_bindings.cp312-universal2.so`
- **Linux**: `ddbc_bindings.cp{python_version}-{architecture}.so`
  - Example: `ddbc_bindings.cp312-x86_64.so`

## Creating Python Wheels

After building the native bindings, you can create a Python wheel for distribution.

### Build Wheel

```bash
python setup.py bdist_wheel
```

The wheel will be created in the `dist/` directory with platform-specific tags:
- **Windows**: `mssql_python-{version}-py3-none-win_{architecture}.whl`
- **macOS**: `mssql_python-{version}-py3-none-macosx_15_0_universal2.whl`
- **Linux**: `mssql_python-{version}-py3-none-manylinux2014_{architecture}.whl`

### Install from Wheel

```bash
pip install dist/mssql_python-{version}-py3-none-{platform}.whl
```

### Alternative: Development Installation

For development purposes, you can install the package in editable mode:

```bash
pip install -e .
```

This allows you to make changes to the Python code without reinstalling.

## Running Tests

The test suite uses pytest and requires a Microsoft SQL Server database connection.

### Environment Setup

Set the database connection string environment variable:

#### Windows
```cmd
set DB_CONNECTION_STRING="SERVER=your_server;DATABASE=your_database;UID=your_username;PWD=your_password;Encrypt=yes;TrustServerCertificate=yes;"
```

#### macOS/Linux
```bash
export DB_CONNECTION_STRING="SERVER=your_server;DATABASE=your_database;UID=your_username;PWD=your_password;Encrypt=yes;TrustServerCertificate=yes;"
```

### Connection String Examples

#### SQL Server Authentication
```
SERVER=localhost;DATABASE=master;UID=sa;PWD=YourPassword123;Encrypt=yes;TrustServerCertificate=yes;
```

#### Windows Authentication (Windows only)
```
SERVER=localhost;DATABASE=master;Trusted_Connection=yes;Encrypt=yes;TrustServerCertificate=yes;
```

#### Azure SQL Database
```
SERVER=your-server.database.windows.net;DATABASE=your-database;UID=your-username;PWD=your-password;Encrypt=yes;
```

### Running Tests

After setting the environment variable, run the tests:

```bash
# Run all tests
python -m pytest

# Run specific test file
python -m pytest tests/test_003_connection.py

# Run tests with coverage
python -m pytest --cov=mssql_python

# Run tests with verbose output
python -m pytest -v
```

### Test Structure

The test suite includes:
- `test_000_dependencies.py` - Dependency validation
- `test_001_globals.py` - Global functionality tests
- `test_002_types.py` - Data type handling tests
- `test_003_connection.py` - Connection management tests
- `test_004_cursor.py` - Cursor functionality tests
- `test_005_connection_cursor_lifecycle.py` - Lifecycle tests
- `test_006_exceptions.py` - Error handling tests
- `test_007_logging.py` - Logging functionality tests
- `test_008_auth.py` - Authentication tests

## Troubleshooting

### Common Build Issues

#### Windows
- **Error**: "Visual Studio not found"
  - **Solution**: Ensure Visual Studio Build Tools 2022 is installed with C++ workload
  - **Alternative**: Use Developer Command Prompt for VS 2022

- **Error**: "CMake not found"
  - **Solution**: CMake is included with Visual Studio Build Tools, or install separately

#### macOS
- **Error**: "No such file or directory: 'sql.h'"
  - **Solution**: Install Microsoft ODBC Driver 18 using the brew command above

- **Error**: "xcrun: error: active developer path does not exist"
  - **Solution**: Install Xcode Command Line Tools: `xcode-select --install`

#### Linux
- **Error**: "Python.h: No such file or directory"
  - **Solution**: Install Python development headers: `sudo apt-get install python3-dev`

- **Error**: "cmake: command not found"
  - **Solution**: Install CMake: `sudo apt-get install cmake`

### Test Issues

- **Error**: "Connection string should not be None"
  - **Solution**: Ensure `DB_CONNECTION_STRING` environment variable is set correctly

- **Error**: "Database connection failed"
  - **Solution**: Verify SQL Server is running and connection string is valid
  - **Check**: Network connectivity and firewall settings
  - **Verify**: Username/password or authentication method

### Architecture Issues

- **Windows**: Ensure you're building for the correct architecture matching your Python installation
- **macOS**: Universal2 binaries should work on both Intel and Apple Silicon Macs
- **Linux**: The build script auto-detects architecture, but you can verify with `uname -m`

## Development Workflow

### Recommended Development Process

1. **Set up development environment**:
   ```bash
   git clone https://github.com/microsoft/mssql-python.git
   cd mssql-python
   pip install -r requirements.txt
   ```

2. **Build native bindings**:
   ```bash
   cd mssql_python/pybind
   # Windows: build.bat
   # macOS/Linux: ./build.sh
   ```

3. **Install in development mode**:
   ```bash
   cd ../..
   pip install -e .
   ```

4. **Set up test database connection**:
   ```bash
   # Windows: set DB_CONNECTION_STRING="..."
   # macOS/Linux: export DB_CONNECTION_STRING="..."
   ```

5. **Run tests**:
   ```bash
   python -m pytest
   ```

6. **Make changes and test iteratively**:
   - For Python changes: No rebuild needed (using `-e` install)
   - For C++ changes: Rebuild native bindings and reinstall

### Code Quality

The project uses standard Python development practices:
- Follow PEP 8 style guidelines
- Write comprehensive tests for new features
- Ensure all tests pass before submitting changes
- Use type hints where appropriate

## Contributing

This project welcomes contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for detailed guidelines on how to contribute to this project.

For questions or support, please create an issue on the [GitHub repository](https://github.com/microsoft/mssql-python/issues).