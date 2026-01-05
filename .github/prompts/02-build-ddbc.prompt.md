---
description: "Build C++ pybind11 extension (ddbc_bindings)"
name: "build"
agent: 'agent'
model: Claude Sonnet 4.5 (copilot)
---
# Build DDBC Extensions Prompt for microsoft/mssql-python

You are a development assistant helping rebuild the DDBC C++ pybind11 extensions for the mssql-python driver.

## PREREQUISITES

> ⚠️ **This prompt assumes your development environment is already set up.**
> If you haven't set up your environment yet, use `#01-setup-dev-env` first.

**Quick sanity check:**
```bash
echo $VIRTUAL_ENV && python -c "import pybind11; print('✅ Ready to build')"
```

If this fails, run the setup prompt first.

---

## TASK

Help the developer rebuild the DDBC bindings after making C++ code changes. Follow this process sequentially.

---

## STEP 0: Understand What You're Building

### What Are DDBC Bindings?

The `ddbc_bindings` module is a **C++ pybind11 extension** that provides:
- Low-level ODBC connectivity to SQL Server
- High-performance database operations
- Platform-specific optimizations

### When to Rebuild

- ✅ After modifying any `.cpp` or `.h` files in `mssql_python/pybind/`
- ✅ After changing `CMakeLists.txt`
- ✅ After upgrading Python version
- ✅ After pulling changes that include C++ modifications
- ❌ After Python-only changes (no rebuild needed)

### Key Files

```
mssql_python/pybind/
├── ddbc_bindings.cpp      # Main bindings implementation
├── ddbc_bindings.h        # Header file
├── logger_bridge.cpp      # Python logging bridge
├── logger_bridge.hpp      # Logger header
├── connection/            # Connection implementation
│   ├── connection.cpp
│   ├── connection.h
│   ├── connection_pool.cpp
│   └── connection_pool.h
├── CMakeLists.txt         # CMake build configuration
├── build.sh               # macOS/Linux build script
└── build.bat              # Windows build script
```

---

## STEP 1: Build the Extension

### 1.1 Navigate to Build Directory

```bash
cd mssql_python/pybind
```

### 1.2 Run Build Script

#### macOS / Linux

```bash
# Standard build
./build.sh

# Build with code coverage instrumentation (Linux only)
./build.sh codecov
```

#### Windows (in Developer Command Prompt)

```cmd
build.bat
```

### 1.3 What the Build Does

1. **Cleans** existing `build/` directory
2. **Detects** Python version and architecture
3. **Configures** CMake with correct paths
4. **Compiles** C++ code to platform-specific extension
5. **Copies** the built extension to `mssql_python/` directory
6. **Signs** the extension (macOS only - for SIP compliance)

**Output files by platform:**
| Platform | Output File |
|----------|-------------|
| macOS | `ddbc_bindings.cp{version}-universal2.so` |
| Linux | `ddbc_bindings.cp{version}-{arch}.so` |
| Windows | `ddbc_bindings.cp{version}-{arch}.pyd` |

---

## STEP 2: Verify the Build

### 2.1 Check Output File Exists

```bash
# macOS/Linux
ls -la ../ddbc_bindings.*.so

# Windows
dir ..\ddbc_bindings.*.pyd
```

### 2.2 Verify Import Works

```bash
# From repository root (important!)
cd ../..
python -c "from mssql_python import connect; print('✅ Import successful')"
```

---

## STEP 3: Clean Build (If Needed)

If you need a completely fresh build:

```bash
# From repository root
rm -rf mssql_python/pybind/build/
rm -f mssql_python/ddbc_bindings.*.so
rm -f mssql_python/ddbc_bindings.*.pyd

# Rebuild
cd mssql_python/pybind
./build.sh  # or build.bat on Windows
```

---

## Troubleshooting

### ❌ "CMake configuration failed"

**Cause:** CMake can't find Python or pybind11 paths

**Fix:**
```bash
# Verify Python include directory exists
python -c "import sysconfig; print(sysconfig.get_path('include'))"
ls $(python -c "import sysconfig; print(sysconfig.get_path('include'))")

# Verify pybind11 include directory exists
python -c "import pybind11; print(pybind11.get_include())"
ls $(python -c "import pybind11; print(pybind11.get_include())")
```

If pybind11 path doesn't exist, run: `pip install pybind11`

### ❌ "pybind11 not found" during build

**Cause:** pybind11 not installed in active venv

**Fix:**
```bash
# Ensure venv is active
source myvenv/bin/activate  # adjust path if needed

# Install pybind11
pip install pybind11

# Verify
python -c "import pybind11; print(pybind11.get_include())"
```

### ❌ "sql.h not found" (macOS)

**Cause:** ODBC development headers not installed

**Fix:**
```bash
# Install Microsoft ODBC Driver (provides headers)
brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
ACCEPT_EULA=Y brew install msodbcsql18

# Or specify custom path
export ODBC_INCLUDE_DIR=/path/to/odbc/headers
./build.sh
```

### ❌ "undefined symbol" errors at runtime

**Cause:** Built with different Python than you're running

**Fix:**
```bash
# Check which Python was used to build (look at output filename)
ls mssql_python/ddbc_bindings.*.so
# e.g., ddbc_bindings.cp313-universal2.so means Python 3.13

# Check current Python
python --version

# If mismatch, rebuild with correct Python
rm -rf mssql_python/pybind/build/
cd mssql_python/pybind
./build.sh
```

### ❌ "cmake is not recognized" (Windows)

**Cause:** Not using Developer Command Prompt

**Fix:**
1. Close current terminal
2. Open **Start Menu** → search "Developer Command Prompt for VS 2022"
3. Navigate to project: `cd C:\path\to\mssql-python\mssql_python\pybind`
4. Run: `build.bat`

### ❌ "codesign failed" (macOS)

**Cause:** macOS SIP (System Integrity Protection) issues

**Fix:** The build script handles this automatically. If issues persist:
```bash
codesign -s - -f mssql_python/ddbc_bindings.*.so
```

### ❌ Build succeeds but import fails

**Cause:** Usually path issues or old cached files

**Fix:**
```bash
# Clear Python cache
find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null

# Clear any .pyc files
find . -name "*.pyc" -delete

# Reinstall in dev mode
pip install -e .

# Try import again
python -c "from mssql_python import connect; print('✅ OK')"
```

### ❌ "Permission denied" running build.sh

**Fix:**
```bash
chmod +x mssql_python/pybind/build.sh
./build.sh
```

### ❌ Build takes too long / seems stuck

**Cause:** Universal binary build on macOS compiles for both architectures

**Info:** This is normal. macOS builds for both arm64 and x86_64. First build takes longer, subsequent builds use cache.

**If truly stuck (>10 minutes):**
```bash
# Cancel with Ctrl+C, then clean and retry
rm -rf build/
./build.sh
```

---

## Quick Reference

### One-Liner Build Commands

```bash
# macOS/Linux - Full rebuild from repo root
cd mssql_python/pybind && rm -rf build && ./build.sh && cd ../.. && python -c "from mssql_python import connect; print('✅ Build successful')"
```

### Build Output Naming Convention

| Platform | Python | Architecture | Output File |
|----------|--------|--------------|-------------|
| macOS | 3.13 | Universal | `ddbc_bindings.cp313-universal2.so` |
| Linux | 3.12 | x86_64 | `ddbc_bindings.cp312-x86_64.so` |
| Linux | 3.11 | ARM64 | `ddbc_bindings.cp311-arm64.so` |
| Windows | 3.13 | x64 | `ddbc_bindings.cp313-amd64.pyd` |
| Windows | 3.12 | ARM64 | `ddbc_bindings.cp312-arm64.pyd` |

---

## After Building

Once the build succeeds:

1. **Run tests** → Use `#03-run-tests`
2. **Test manually** with a connection to SQL Server
3. **Create a PR** with your C++ changes → Use `#04-create-pr`
