# Microsoft SQL Server Python Driver

This repository contains the Python driver for Microsoft SQL Server.

## Installation

```bash
pip install mssql-python
```

When installing from PyPI, the appropriate wheel package will be automatically selected based on your platform architecture (win32, win64, or winarm64).

## Development Setup

### Prerequisites

1. Python 3.8 or later
2. Visual C++ Build Tools - downloadable from [Visual Studio website](https://visualstudio.microsoft.com/visual-cpp-build-tools/)
   - For x64: Visual Studio 2017 or later with "Desktop development with C++" workload
   - For x86: Visual Studio 2017 or later with "Desktop development with C++" workload
   - For ARM64: Visual Studio 2019 or later with "C++ ARM64 build tools"

### Building from Source

To build the extension module:

```bash
# Install development dependencies
pip install pybind11 setuptools wheel

# Build the extension
python setup.py build_ext --inplace
```

This will create the extension module (ddbc_bindings.pyd) directly in the mssql_python package directory.

### Creating Distribution Packages

To create a wheel package:

```bash
python setup.py bdist_wheel
```

This will create a wheel file in the `dist` directory. The wheel will contain only the DLLs appropriate for the architecture on which it was built.

### Using cibuildwheel

For more control over the build process and to build for multiple Python versions:

```bash
# Install cibuildwheel
pip install cibuildwheel

# Build wheels
python -m cibuildwheel --output-dir dist
```

The cibuildwheel configuration is defined in `pyproject.toml`, specifying which Python versions and architectures to target.

## Architecture Support

The driver supports the following Windows architectures:
- x86 (32-bit)
- x64 (64-bit)
- ARM64

When building from source, the appropriate architecture-specific files will be included based on the machine's architecture where the build is performed.

## Building Wheels for Different Architectures

To create wheels for all supported architectures, you'll need to build on each target platform:

1. On a Windows x64 machine: `python setup.py bdist_wheel`
2. On a Windows x86 machine: `python setup.py bdist_wheel`
3. On a Windows ARM64 machine: `python setup.py bdist_wheel`

## DLL Organization

Architecture-specific DLLs are organized in the following directories:
- `mssql_python/dlls/win32/` - 32-bit DLLs
- `mssql_python/dlls/win64/` - 64-bit DLLs
- `mssql_python/dlls/winarm64/` - ARM64 DLLs

Each directory contains:
- `msodbcsql18.dll` - SQL Server ODBC driver
- Diagnostic DLLs
- Resource DLLs (.rll files)
