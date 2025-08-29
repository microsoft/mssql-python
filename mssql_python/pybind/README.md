# Build Instructions for Developers

This README provides instructions to build the DDBC Bindings for your system and documents the platform-specific dependencies.

## **Key Architecture Handling**

1. **Architecture Normalization** (from `mssql_python/ddbc_bindings.py`):
   - Windows: `win64/amd64/x64` → `x64`, `win32/x86` → `x86`, `arm64` → `arm64`
   - macOS: Always uses `universal2` for distribution
   - Linux: `x64/amd64` → `x86_64`, `arm64/aarch64` → `arm64`

2. **Build System Support**:
   - CMake handles universal binaries for macOS (`arm64;x86_64`)
   - Windows `mssql_python/pybind/build.bat` supports x64, x86, and ARM64
   - Linux builds are distribution-aware (Debian/Ubuntu vs RHEL)

3. **Runtime Loading**:
   - `LoadDriverLibrary()` in `mssql_python/pybind/ddbc_bindings.cpp` handles platform-specific loading
   - Windows uses `LoadLibraryW()`
   - macOS/Linux uses `dlopen()`

---

## Building on Windows
> Supporting Windows x64 & ARM64 only

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
    - Compile `mssql_python/pybind/ddbc_bindings.cpp` using CMake and create properly versioned PYD file (`ddbc_bindings.cp313-amd64.pyd`)
    - Move the built PYD file to the parent `mssql_python` directory

## Building on MacOS
> Supporting Apple Silicon Chip M Series (ARM64) as well as Intel based processors (x86_64)

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
   - Detect system architecture
   - Configure CMake with appropriate include/library paths
   - Compile `mssql_python/pybind/ddbc_bindings.cpp` using CMake
   - Generate the `.so` file (e.g., `ddbc_bindings.cp313-universal2.so`)
   - Copy the output SO file to the parent `mssql_python` directory

## Architecture Dependencies Summary

### **Directory Structure**
```
mssql_python/
├── libs/
│   ├── windows/
│   │   ├── x64/
│   │   ├── x86/
│   │   └── arm64/
│   ├── macos/
│   │   ├── arm64/lib/
│   │   └── x86_64/lib/
│   └── linux/
│       ├── debian_ubuntu/
│       │   ├── x86_64/lib/
│       │   └── arm64/lib/
│       ├── rhel/
│       │   ├── x86_64/lib/
│       │   └── arm64/lib/
│       ├── suse/
│       │   └── x86_64/lib/              # ARM64 not supported by Microsoft
│       └── alpine/
│           ├── x86_64/lib/
│           └── arm64/lib/
└── ddbc_bindings.cp{python_version}-{architecture}.{extension}
```

### **Windows (.dll files)**
Windows builds depend on these DLLs for different architectures:

**x64 (amd64):**
- `msodbcsql18.dll` - Main driver
- `msodbcdiag18.dll` - Diagnostic library
- `mssql-auth.dll` - Authentication library (for Entra ID)
- `vcredist/msvcp140.dll` - Visual C++ runtime (from vcredist)

**x86 (win32):**
- `msodbcsql18.dll` - Main driver
- `msodbcdiag18.dll` - Diagnostic library  
- `mssql-auth.dll` - Authentication library
- `vcredist/msvcp140.dll` - Visual C++ runtime (from vcredist)

**ARM64:**
- `msodbcsql18.dll` - Main driver
- `msodbcdiag18.dll` - Diagnostic library
- `mssql-auth.dll` - Authentication library
- `vcredist/msvcp140.dll` - Visual C++ runtime (from vcredist)

### **macOS (.dylib files)**
macOS builds use universal2 binaries and depend on:

**Packaged Dependencies (arm64 & x86_64):**
- `libmsodbcsql.18.dylib` - Main driver
- `libodbcinst.2.dylib` - Installer library

### **Linux (.so files)**
Linux builds support multiple distributions:

**Debian/Ubuntu x86_64:**
- `libmsodbcsql-18.5.so.1.1` - Main driver
- `libodbcinst.so.2` - Installer library

**Debian/Ubuntu ARM64:**
- `libmsodbcsql-18.5.so.1.1` - Main driver
- `libodbcinst.so.2` - Installer library

**RHEL/CentOS x86_64:**
- `libmsodbcsql-18.5.so.1.1` - Main driver
- `libodbcinst.so.2` - Installer library

**RHEL/CentOS ARM64:**
- `libmsodbcsql-18.5.so.1.1` - Main driver
- `libodbcinst.so.2` - Installer library

**SUSE/openSUSE x86_64:**
- `libmsodbcsql-18.5.so.1.1` - Main driver
- `libodbcinst.so.2` - Installer library

> **Note:** SUSE/openSUSE ARM64 is not supported by Microsoft ODBC Driver 18

**Alpine x86_64:**
- `libmsodbcsql-18.5.so.1.1` - Main driver
- `libodbcinst.so.2` - Installer library

**Alpine ARM64:**
- `libmsodbcsql-18.5.so.1.1` - Main driver
- `libodbcinst.so.2` - Installer library

## **Python Extension Modules**
Your build system generates architecture-specific Python extension modules:

**Naming Convention:** `ddbc_bindings.cp{python_version}-{architecture}.{extension}`

Examples:
- Windows x64: `ddbc_bindings.cp311-amd64.pyd`
- Windows x86: `ddbc_bindings.cp311-win32.pyd`
- Windows ARM64: `ddbc_bindings.cp311-arm64.pyd`
- macOS Universal: `ddbc_bindings.cp311-universal2.so`
- Linux x86_64: `ddbc_bindings.cp311-x86_64.so`
- Linux ARM64: `ddbc_bindings.cp311-arm64.so`

