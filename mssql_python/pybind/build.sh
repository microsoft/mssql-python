#!/bin/bash
# Build script for macOS and Linux
# This script is designed to be run from the mssql_python/pybind directory

# Detect OS
OS_TYPE=$(uname -s)
if [[ "$OS_TYPE" == "Darwin" ]]; then
    OS="macOS"
    BUILD_TYPE="Universal2 Binary"
elif [[ "$OS_TYPE" == "Linux" ]]; then
    OS="Linux"
    BUILD_TYPE="Native Binary"
else
    echo "[ERROR] Unsupported OS: $OS_TYPE"
    exit 1
fi

# Get Python version from active interpreter
PYTAG=$(python -c "import sys; print(f'{sys.version_info.major}{sys.version_info.minor}')")

echo "==================================="
echo "Building $BUILD_TYPE for Python $PYTAG on $OS"
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
echo "[DIAGNOSTIC] Running CMake configure"
if [[ "$OS" == "macOS" ]]; then
    echo "[DIAGNOSTIC] Configuring for macOS (universal2 is set automatically)"
    cmake -DMACOS_STRING_FIX=ON "${SOURCE_DIR}"
else
    echo "[DIAGNOSTIC] Configuring for Linux"
    cmake "${SOURCE_DIR}"
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
    echo "[SUCCESS] $BUILD_TYPE build completed successfully"
    
    # List the built files
    echo "Built files:"
    ls -la *.so
    
    # Copy the built .so file to the mssql_python directory
    PARENT_DIR=$(dirname "$SOURCE_DIR")
    echo "[ACTION] Copying the .so file to $PARENT_DIR"
    cp -f *.so "$PARENT_DIR"
    if [ $? -eq 0 ]; then
        echo "[SUCCESS] .so file copied successfully"
        
        # macOS-specific: Configure dylib paths and codesign
        if [[ "$OS" == "macOS" ]]; then
            echo "[ACTION] Configuring and codesigning dylibs for macOS"
            chmod +x "${SOURCE_DIR}/configure_dylibs.sh"
            "${SOURCE_DIR}/configure_dylibs.sh"
            if [ $? -eq 0 ]; then
                echo "[SUCCESS] macOS dylibs configured and codesigned successfully"
            else
                echo "[WARNING] macOS dylib configuration encountered issues"
            fi
        fi
    else
        echo "[ERROR] Failed to copy .so file"
        exit 1
    fi
fi

# macOS-specific: Check if the file is a universal binary
if [[ "$OS" == "macOS" ]]; then
    SO_FILE=$(ls -1 *.so | head -n 1)
    echo "[DIAGNOSTIC] Checking if ${SO_FILE} is a universal binary..."
    lipo -info "${SO_FILE}"
    
    # Check if the file has the correct naming convention
    if [[ "${SO_FILE}" != *universal2* ]]; then
        echo "[WARNING] The .so file doesn't have 'universal2' in its name, even though it's a universal binary."
        echo "[WARNING] You may need to run the build again after the CMakeLists.txt changes are applied."
    fi
fi
