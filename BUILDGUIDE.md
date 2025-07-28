# Build Guide for Contributors

Welcome to the development build guide for the **mssql-python** project!  
This guide will help you set up your environment, build the native bindings, and package the project as a Python wheel.

---

## Table of Contents

- [Prerequisites](#prerequisites)
- [Platform-Specific Setup](#platform-specific-setup)
  - [Windows](#windows)
  - [macOS](#macos)
  - [Linux](#linux)
- [Building Native Bindings](#building-native-bindings)
- [Building the Python Wheel (.whl)](#building-the-python-wheel-whl)
- [Running Tests](#running-tests)
- [Directory Structure](#directory-structure)
- [Troubleshooting](#troubleshooting)

---

## Prerequisites

- **Python:** Minimum supported version is 3.10.  
  Ensure `python` and `pip` commands refer to your Python 3.10+ installation.
- **pybind11:** Used for C++/Python bindings.
- **CMake:** For Unix and macOS builds.
- **Microsoft ODBC Driver:** For packaging driver dependencies and header files such as `sql.h`, `sqlext.h` etc.
- **setuptools, wheel, pytest:** For building and testing (`pip install setuptools wheel pytest`).

---

## Platform-Specific Setup

### Windows

1. **Install Python** (3.10+ from [python.org](https://www.python.org/downloads/)).
2. **Install Visual Studio Build Tools**
   - Include the “Desktop development with C++” workload.
   - CMake is included by default.
3. **Install Microsoft ODBC Driver for SQL Server:**  
   [Download here](https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server).
4. **Install required Python packages:**
   ```bash
   # Will install pybind11, setuptools etc.
   pip install -r requirements.txt
   ```

### macOS

1. **Install Python** (3.10+ from [python.org](https://www.python.org/downloads/)).
2. **Install CMake:**
   ```bash
   brew install cmake
   ```
3. **Install Microsoft ODBC Driver for SQL Server:**
   ```bash
   /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install.sh)"
   brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
   brew update
   HOMEBREW_ACCEPT_EULA=Y brew install msodbcsql18 mssql-tools18
   ```
4. **Install Python requirements:**
   ```bash
   # Will install pybind11, setuptools etc.
   pip install -r requirements.txt
   ```

### Linux

1. **Install Python and development tools:**
   ```bash
   sudo apt-get update
   sudo apt-get install python3.10 python3.10-dev python3-pip build-essential cmake
   ```
   Ensure `python` and `pip` refer to Python 3.10.
2. **Install Microsoft ODBC Driver for SQL Server:**  
   Follow [official instructions](https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server).
3. **Install Python packages:**
   ```bash
   # Will install pybind11, setuptools etc.
   pip install -r requirements.txt
   ```

---

## Building Native Bindings

The native bindings are in the `pybind` directory.

### Windows

Open a **Developer Command Prompt for VS** and run:

```bash
cd pybind
build.bat
```

This will:
- Clean previous builds
- Configure with CMake
- Build the extension
- Copy the generated `.pyd` file to the correct location

### macOS & Linux

```bash
cd pybind
./build.sh
```

This will:
- Clean previous builds
- Configure with CMake
- Build the extension
- Copy the generated `.so` file to the correct location

---

## Building the Python Wheel (.whl)

The wheel includes the native bindings.  
**You must build the native bindings first** (see above).

### Windows

From the project root:

```bash
python setup.py bdist_wheel
```

The wheel file will be created in the `dist/` directory.

### macOS & Linux

From the project root:

```bash
# Build the bindings first!
cd pybind
./build.sh
cd ..

# Then build the wheel:
python setup.py bdist_wheel
```

The wheel file will be created in the `dist/` directory.

---

## Running Tests

Tests require a database connection string.
Set the `DB_CONNECTION_STRING` environment variable before running tests:

### Windows (Command Prompt)
```cmd
set DB_CONNECTION_STRING=your-connection-string-here
python -m pytest -v
```

### macOS & Linux (bash/zsh)
```bash
export DB_CONNECTION_STRING=your-connection-string-here
python -m pytest -v
```

---

## Directory Structure

- `pybind/` — Native C++/pybind11 bindings and platform build scripts
- `mssql_python/` — Python package source
- `tests/` — Test suite
- `dist/` — Built wheel packages

---

## Troubleshooting

- Ensure all prerequisites are installed and on your PATH.
- If a build fails, clean up old artifacts and try again (`pybind/build.bat clean` or `./build.sh clean`).
- For wheel issues, ensure the native binding (`.pyd` or `.so`) is present in the expected location before building the wheel.
- For test failures, double-check your `DB_CONNECTION_STRING`.

---

For more details on the native bindings, see [`pybind/README.md`](pybind/README.md).

---

Happy coding!
