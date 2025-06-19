#!/bin/bash
# Build script for macOS to compile a universal2 (arm64 + x86_64) binary
# This script is designed to be run from the mssql_python/pybind directory

# Get Python version from active interpreter
PYTAG=$(python -c "import sys; print(f'{sys.version_info.major}{sys.version_info.minor}')")

echo "==================================="
echo "Building Universal2 Binary for Python $PYTAG"
echo "==================================="

# Save absolute source directory
SOURCE_DIR=$(pwd)

# Clean up main build directory if it exists
echo "Checking for build directory..."
if [ -d "build" ]; then
    echo "Removing existing build directory..."
    rm -rf build
    echo "Build directory removed."
fi

# Create build directory for universal binary
BUILD_DIR="${SOURCE_DIR}/build"
mkdir -p "${BUILD_DIR}"
cd "${BUILD_DIR}"
echo "[DIAGNOSTIC] Changed to build directory: ${BUILD_DIR}"

# Configure CMake (architecture settings handled in CMakeLists.txt)
echo "[DIAGNOSTIC] Running CMake configure (universal2 is set automatically)"
cmake -DMACOS_STRING_FIX=ON "${SOURCE_DIR}"

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
    echo "[SUCCESS] Universal2 build completed successfully"
    
    # List the built files
    echo "Built files:"
    ls -la *.so
    
    # Copy the built .so file to the mssql_python directory
    PARENT_DIR=$(dirname "$SOURCE_DIR")
    echo "[ACTION] Copying the universal2 .so file to $PARENT_DIR"
    cp -f *.so "$PARENT_DIR"
    if [ $? -eq 0 ]; then
        echo "[SUCCESS] Universal2 .so file copied successfully"
        
        # Configure dylib paths and codesign
        echo "[ACTION] Configuring and codesigning dylibs for macOS"
        chmod +x "${SOURCE_DIR}/configure_dylibs.sh"
        "${SOURCE_DIR}/configure_dylibs.sh"
        if [ $? -eq 0 ]; then
            echo "[SUCCESS] macOS dylibs configured and codesigned successfully"
        else
            echo "[WARNING] macOS dylib configuration encountered issues"
        fi
    else
        echo "[ERROR] Failed to copy universal2 .so file"
        exit 1
    fi
fi

# Check if the file is a universal binary
SO_FILE=$(ls -1 *.so | head -n 1)
echo "[DIAGNOSTIC] Checking if ${SO_FILE} is a universal binary..."
lipo -info "${SO_FILE}"

# Check if the file has the correct naming convention
if [[ "${SO_FILE}" != *universal2* ]]; then
    echo "[WARNING] The .so file doesn't have 'universal2' in its name, even though it's a universal binary."
    echo "[WARNING] You may need to run the build again after the CMakeLists.txt changes are applied."
fi
