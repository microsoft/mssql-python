# PyBind11 Build Instructions

This README provides instructions to build a project using PyBind11.

## Prerequisites

1. **CMake**: Install CMake on your system - https://cmake.org/download/
2. **PyBind11**: Install PyBind11 using pip.
    ```sh
    pip install pybind11
    ```
3. **Visual Studio**: Install Visual Studio with the C++ development tools.
    - Download Visual Studio from https://visualstudio.microsoft.com/
    - During installation, select the "Desktop development with C++" workload.

## Build Instructions

1. Go to the pybind folder and run `build.bat` with your desired architecture:
    ```sh
    # For 64-bit Intel/AMD (default)
    build.bat x64
    
    # For ARM64
    build.bat arm64
    
    # For 32-bit Intel
    build.bat x86
    ```

2. The script will:
   - Clean up existing build directories
   - Detect your Python version
   - Create properly versioned PYD files (e.g., `ddbc_bindings.cp313-amd64.pyd`)
   - Move the built PYD file to the parent `mssql_python` directory

3. The versioned PYD filename follows the pattern: `ddbc_bindings.cp{python-version}-{architecture}.pyd`
   - Example: `ddbc_bindings.cp313-amd64.pyd` for Python 3.13 on x64 architecture
