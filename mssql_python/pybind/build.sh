#!/bin/bash
# Build script for macOS to compile the ddbc_bindings C++ code
# This script is designed to be run from the mssql_python/pybind directory
# Running this script will require CMake and a C++ compiler installed on your MacOS system
# It will also require Python 3.x, pybind11 and msodbcsql18 and unixODBC to be installed

# Usage: build.sh [ARCH], If ARCH is not specified, it defaults to the current architecture
ARCH=${1:-$(uname -m)}
echo "[DIAGNOSTIC] Target Architecture set to: $ARCH"

# Clean up main build directory if it exists
echo "Checking for main build directory..."
if [ -d "build" ]; then
    echo "Removing existing build directory..."
    rm -rf build
    echo "Build directory removed."
fi

# Get Python version from active interpreter
PYTAG=$(python -c "import sys; print(f'{sys.version_info.major}{sys.version_info.minor}')")

echo "==================================="
echo "Building for: $ARCH / Python $PYTAG"
echo "==================================="

# Save absolute source directory
SOURCE_DIR=$(pwd)

# Go to build output directory
BUILD_DIR="${SOURCE_DIR}/build/${ARCH}/py${PYTAG}"
mkdir -p "${BUILD_DIR}"
cd "${BUILD_DIR}"
echo "[DIAGNOSTIC] Changed to build directory: ${BUILD_DIR}"

# Set platform-specific flags for different architectures
if [ "$ARCH" = "x86_64" ]; then
    # x86_64 architecture
    echo "[DIAGNOSTIC] Detected Intel Chip x86_64 architecture"
    CMAKE_ARCH="x86_64"
    echo "[DIAGNOSTIC] Using x86_64 architecture for CMake"
elif [ "$ARCH" = "arm64" ]; then
    # arm64 architecture
    echo "[DIAGNOSTIC] Detected Apple Silicon Chip arm64 architecture"
    CMAKE_ARCH="arm64"
    echo "[DIAGNOSTIC] Using arm64 architecture for CMake"
else
    echo "[ERROR] Unsupported architecture: $ARCH"
    exit 1
fi

echo "[DIAGNOSTIC] Source directory: ${SOURCE_DIR}"

# Special handling for macOS ODBC headers and string conversion issues
if [ "$(uname)" = "Darwin" ]; then
    # Check if macOS-specific source file exists
    if [ -f "${SOURCE_DIR}/ddbc_bindings_mac.cpp" ]; then
        echo "[DIAGNOSTIC] Using macOS-specific source file: ddbc_bindings_mac.cpp"
    else
        echo "[WARNING] macOS-specific source file ddbc_bindings_mac.cpp not found"
        echo "[WARNING] Falling back to standard source file ddbc_bindings.cpp"
    fi
    
    # Configure CMake with macOS-specific flags
    echo "[DIAGNOSTIC] Running CMake configure with macOS-specific settings"
    cmake -DCMAKE_OSX_ARCHITECTURES=${CMAKE_ARCH} \
          -DARCHITECTURE=${ARCH} \
          -DMACOS_STRING_FIX=ON \
          "${SOURCE_DIR}"
else
    # Configure CMake for other platforms
    echo "[DIAGNOSTIC] Running CMake configure for non-macOS platform"
    cmake -DARCHITECTURE=${ARCH} "${SOURCE_DIR}"
fi

# Check if CMake configuration succeeded
if [ $? -ne 0 ]; then
    echo "[ERROR] CMake configuration failed"
    exit 1
fi

# Build the project
echo "[DIAGNOSTIC] Running CMake build with: cmake --build . --config Release"
cmake --build . --config Release

# Check if build succeeded
if [ $? -ne 0 ]; then
    echo "[ERROR] CMake build failed"
    exit 1
else
    echo "[SUCCESS] Build completed successfully"
    # List the built files
    echo "Built files:"
    ls -la *.so
    
    # Copy the built .so file to the mssql_python directory
    PARENT_DIR=$(dirname "$SOURCE_DIR")
    echo "[ACTION] Copying the .so file to $PARENT_DIR"
    cp -f *.so "$PARENT_DIR"
    if [ $? -eq 0 ]; then
        echo "[SUCCESS] .so file copied successfully"
        
        # Only on macOS, run the dylib configuration script to fix library paths and codesign
        if [ "$(uname)" = "Darwin" ]; then
            echo "[ACTION] Configuring and codesigning dylibs for macOS"
            chmod +x "${SOURCE_DIR}/configure_dylibs.sh"
            "${SOURCE_DIR}/configure_dylibs.sh"
            if [ $? -eq 0 ]; then
                echo "[SUCCESS] macOS dylibs configured and codesigned successfully"
            else
                echo "[WARNING] macOS dylib configuration encountered issues"
                # Don't exit with error, as the build itself was successful
            fi
        fi
    else
        echo "[ERROR] Failed to copy .so file"
        exit 1
    fi
fi
