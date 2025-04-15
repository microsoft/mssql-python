# Build Instructions for Developers

This README provides instructions to build the DDBC Bindings PYD for various architectures.

## Prerequisites

1. **PyBind11**: Install PyBind11 using pip.
    ```sh
    pip install pybind11
    ```
2. **Visual Studio Build Tools**:
    - Download Visual Studio Build Tools from https://visualstudio.microsoft.com/downloads/#build-tools-for-visual-studio-2022
    - During installation, select the **`Desktop development with C++`** workload, this should also install **CMake**.

## Build Steps

1. Start **Developer Command Prompt for VS 2022**.

2. Inside the Developer Command Prompt window, navigate to the pybind folder and run `build.bat` with your desired architecture:
    ```sh
    build.bat [ARCH]

    # e.g. Generate PYD for 32-bit
    build.bat x86
    ```
    - `[ARCH]` is target architecture, allowed values = `[x64, arm64, x86]`
    - Default `[ARCH]` if not specified = `x64`

### What happens inside the build script? 

- The script will:
    - Clean up existing build directories
    - Detect your Python version and architecture
    - Detect VS Build Tools Installation, and start cross compiler for target acrhitecture
    - Compile `ddbc_bindings.cpp` using CMake and create properly versioned PYD files (e.g., `ddbc_bindings.cp313-amd64.pyd`)
    - Move the built PYD file to the parent `mssql_python` directory

- Finally, you can now run **main.py** to test