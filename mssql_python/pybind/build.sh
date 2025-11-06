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
    
    # Detect Linux architecture
    ARCH=$(uname -m)
    if [[ "$ARCH" == "aarch64" || "$ARCH" == "arm64" ]]; then
        DETECTED_ARCH="arm64"
    elif [[ "$ARCH" == "x86_64" ]]; then
        DETECTED_ARCH="x86_64"
    else
        DETECTED_ARCH="$ARCH"
    fi
    echo "[DIAGNOSTIC] Detected Linux architecture: $ARCH, using: $DETECTED_ARCH"
else
    echo "[ERROR] Unsupported OS: $OS_TYPE"
    exit 1
fi

# Check for coverage mode and profiling mode
COVERAGE_MODE=false
PROFILING_MODE=false

if [[ "${1:-}" == "codecov" || "${1:-}" == "--coverage" ]]; then
    COVERAGE_MODE=true
    echo "[MODE] Enabling Clang coverage instrumentation"
elif [[ "${1:-}" == "profile" || "${1:-}" == "--profile" ]]; then
    PROFILING_MODE=true
    echo "[MODE] Enabling C++ performance profiling instrumentation"
fi

# Get Python version from active interpreter
PYTAG=$(python -c "import sys; print(f'{sys.version_info.major}{sys.version_info.minor}')")

echo "==================================="
echo "Building $BUILD_TYPE for Python $PYTAG on $OS"
if [[ "$OS" == "Linux" ]]; then
    echo "Architecture: $DETECTED_ARCH"
fi
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

# Create build directory
BUILD_DIR="${SOURCE_DIR}/build"
mkdir -p "${BUILD_DIR}"
cd "${BUILD_DIR}"
echo "[DIAGNOSTIC] Changed to build directory: ${BUILD_DIR}"

# Configure CMake (with Clang coverage instrumentation or profiling)
echo "[DIAGNOSTIC] Running CMake configure"
if [[ "$COVERAGE_MODE" == "true" && "$OS" == "Linux" ]]; then
    echo "[ACTION] Configuring for Linux with Clang coverage instrumentation"
    cmake -DARCHITECTURE="$DETECTED_ARCH" \
          -DCMAKE_C_COMPILER=clang \
          -DCMAKE_CXX_COMPILER=clang++ \
          -DCMAKE_CXX_FLAGS="-fprofile-instr-generate -fcoverage-mapping" \
          -DCMAKE_C_FLAGS="-fprofile-instr-generate -fcoverage-mapping" \
          "${SOURCE_DIR}"
elif [[ "$PROFILING_MODE" == "true" ]]; then
    echo "[ACTION] Configuring with C++ profiling enabled (ENABLE_PROFILING flag)"
    if [[ "$OS" == "macOS" ]]; then
        cmake -DMACOS_STRING_FIX=ON \
              -DCMAKE_CXX_FLAGS="-DENABLE_PROFILING" \
              -DCMAKE_C_FLAGS="-DENABLE_PROFILING" \
              "${SOURCE_DIR}"
    else
        cmake -DARCHITECTURE="$DETECTED_ARCH" \
              -DCMAKE_CXX_FLAGS="-DENABLE_PROFILING" \
              -DCMAKE_C_FLAGS="-DENABLE_PROFILING" \
              "${SOURCE_DIR}"
    fi
else
    if [[ "$OS" == "macOS" ]]; then
        echo "[ACTION] Configuring for macOS (default build)"
        cmake -DMACOS_STRING_FIX=ON "${SOURCE_DIR}"
    else
        echo "[ACTION] Configuring for Linux with architecture: $DETECTED_ARCH"
        cmake -DARCHITECTURE="$DETECTED_ARCH" "${SOURCE_DIR}"
    fi
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

# TODO: Linux-specific: use patchelf to set RPATH of the driver .so file
# Currently added Driver SO files right now are already patched
# patchelf --set-rpath '$ORIGIN' libmsodbcsql-18.5.so.1.1
# This command sets the RPATH of the specified .so file to the directory containing the file (similar to Windows)
# Needed since libodbcinst.so.2 is located in the same directory and needs to be resolved

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
