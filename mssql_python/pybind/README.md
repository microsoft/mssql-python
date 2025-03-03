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

1. Go to pybind folder and run `build.bat`:
    ```sh
    build.bat
    ```

2. The built `ddbc_bindings.pyd` file will be moved to the `mssql_python` directory.
