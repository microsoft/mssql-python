# Build Instructions for Developers

This README provides instructions to build the DDBC Bindings PYD for your system (supports Windows x64 and arm64).

## Building on Windows

1. **PyBind11**: Install PyBind11 using pip.
    ```sh
    pip install pybind11
    ```
2. **Visual Studio Build Tools**:
    - Download Visual Studio Build Tools from https://visualstudio.microsoft.com/downloads/#build-tools-for-visual-studio-2022
    - During installation, select the **`Desktop development with C++`** workload, this should also install **CMake**.

3. Start **Developer Command Prompt for VS 2022**.

4. Inside the Developer Command Prompt window, navigate to the pybind folder and run:
    ```sh
    build.bat
    ```

### What happens inside the build script? 

- The script will:
    - Clean up existing build directories
    - Detect VS Build Tools Installation, and start compilation for your Python version and Windows architecture
    - Compile `ddbc_bindings.cpp` using CMake and create properly versioned PYD file (`ddbc_bindings.cp313-amd64.pyd`)
    - Move the built PYD file to the parent `mssql_python` directory

## Building on MacOS

1. **Install CMake & PyBind11**
   ```bash
   brew install cmake
   pip install pybind11
   ```

2. **Install Microsoft ODBC Driver (msodbcsql18)**
   - Visit the official Microsoft documentation:  
     [Download ODBC Driver for SQL Server](https://learn.microsoft.com/sql/connect/odbc/download-odbc-driver-for-sql-server)
   - Install via Homebrew:
     ```bash
     brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
     ACCEPT_EULA=Y brew install msodbcsql18
     ```

   > ✅ **Why this step is important**: This package provides development headers (`sql.h`, `sqlext.h`) and the dynamic library (`libmsodbcsql.18.dylib`) required for building `ddbc_bindings`.  
   > In future versions, we plan to bundle these headers as a developer SDK inside the `mssql-python` package itself. This will allow full cross-platform compatibility and remove the need for system-level ODBC driver installations during development builds.

3. Navigate to the `pybind` directory & run the build script:
   ```bash
   ./build.sh
   ```
### What happens inside the build script? 

- The script will:
   - Clean any existing build artifacts
   - Detect system architecture (only `arm64` is supported for MacOS)
   - Configure CMake with appropriate include/library paths
   - Compile `ddbc_bindings_mac.cpp` using CMake
   - Generate the `.so` file (e.g., `ddbc_bindings.cp313-arm64.so`)
   - Copy the output SO file to the parent `mssql_python` directory
