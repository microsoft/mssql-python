# Design Document: Migrating to cibuildwheel for Cross-Platform Python Wheel Building

## Current System Analysis

Your current system uses:
- A custom build.bat script that handles architecture selection, Python version detection, and Visual Studio setup
- A CMakeLists.txt file that configures the pybind11 build environment
- A custom setup.py with CMakeExtension and CMakeBuild classes for building the extension

## 1. Overview of cibuildwheel

cibuildwheel is a powerful tool that simplifies building Python wheels for multiple platforms and Python versions. It automates the process of setting up build environments, compiling extension modules, and packaging wheels that work across different platforms.

### Key Benefits:
- Automates Python wheel building for Windows, macOS, and Linux
- Handles multiple Python versions and architectures automatically
- Integrates with CI systems (GitHub Actions, Azure Pipelines)
- Consistent build environment across platforms
- No need for platform-specific batch files or scripts

## 2. Implementation Plan

### 2.1 Project Structure Modifications

```
mssql-python/
├── pyproject.toml        # New build configuration (replaces setup.py)
├── setup.py              # Simplified version
├── setup.cfg             # Additional package metadata
├── README.md             # Main project documentation
├── README_Build.md       # New detailed build documentation
├── .github/
│   └── workflows/
│       └── build.yml     # GitHub Actions workflow for cibuildwheel
├── mssql_python/
│   ├── __init__.py
│   ├── ...
│   ├── libs/
│   │   ├── win32/        # Platform-specific ODBC drivers
│   │   ├── winamd64/
│   │   └── winarm64/
│   └── pybind/
│       ├── CMakeLists.txt   # Simplified CMake configuration
│       ├── ddbc_bindings.cpp
│       └── README.md
└── tests/
    └── ...
```

### 2.2 New Configuration Files

#### pyproject.toml
```toml
[build-system]
requires = ["setuptools>=42", "wheel", "cmake>=3.15", "pybind11>=2.6.0", "scikit-build"]
build-backend = "setuptools.build_meta"

[tool.cibuildwheel]
# Only build on Windows platforms
build = "cp313-win*"
# Skip 32-bit Python on 64-bit Windows and vice versa
skip = "*-win32:*_x86_64* *-win_amd64:*_x86 *-win_arm64:*_x86"
# Set environment variables for all builds
environment = { CIBW_BUILD_VERBOSITY="1" }

# Windows x86 builds
[[tool.cibuildwheel.overrides]]
select = "*-win32"
environment = { ARCHITECTURE="win32" }

# Windows x86_64 builds
[[tool.cibuildwheel.overrides]]
select = "*-win_amd64"
environment = { ARCHITECTURE="win64" }

# Windows ARM64 builds
[[tool.cibuildwheel.overrides]]
select = "*-win_arm64"
environment = { ARCHITECTURE="arm64" }

# Build steps
[tool.cibuildwheel.windows]
before-build = "pip install pybind11"
repair-wheel-command = """
python -c "
import os, shutil, glob, platform
from pathlib import Path

def copy_platform_dlls(wheel_dir, platform_type):
    # Map platform to folder name
    platform_map = {'win32': 'win32', 'win_amd64': 'winamd64', 'win_arm64': 'winarm64'}
    folder = platform_map.get(platform_type)
    
    # Source paths for platform-specific DLLs
    src_odbc_path = Path('mssql_python/libs') / folder
    
    # Target path inside the wheel
    wheel_path = list(Path(wheel_dir).glob('*.whl'))[0]
    target_dir = Path(wheel_dir) / 'temp_extract' / 'mssql_python'
    
    # Create necessary directories
    os.makedirs(target_dir, exist_ok=True)
    
    # Copy ODBC DLLs
    if src_odbc_path.exists():
        for file in src_odbc_path.glob('**/*.dll'):
            rel_path = file.relative_to(src_odbc_path.parent)
            target_file = target_dir / rel_path
            os.makedirs(target_file.parent, exist_ok=True)
            shutil.copy2(file, target_file)
            print(f'Copied {file} to {target_file}')
    
    # Copy appropriate msvcp140.dll based on platform
    if platform_type == 'win_arm64':
        src_dll = Path('mssql_python/libs/winarm64/msvcp140.dll')
    elif platform_type == 'win_amd64':
        src_dll = Path('mssql_python/libs/winamd64/msvcp140.dll')
    elif platform_type == 'win32':
        src_dll = Path('mssql_python/libs/win32/msvcp140.dll')

    if src_dll.exists():
        target_dll = target_dir / 'msvcp140.dll'
        shutil.copy2(src_dll, target_dll)
        print(f'Copied {src_dll} to {target_dll}')

# Determine platform from wheel filename
wheel_file = glob.glob('{wheel}')[0]
if 'win32' in wheel_file:
    platform_type = 'win32'
elif 'win_amd64' in wheel_file:
    platform_type = 'win_amd64'
elif 'win_arm64' in wheel_file:
    platform_type = 'win_arm64'
else:
    raise ValueError(f'Unknown platform in wheel: {wheel_file}')

copy_platform_dlls('{dest_dir}', platform_type)
"
"""
```

#### Simplified setup.py
```python
from setuptools import setup, find_packages
from skbuild import setup as sksetup

setup(
    name='mssql-python',
    version='0.1.5',
    description='A Python library for interacting with Microsoft SQL Server',
    long_description=open('README.md', encoding='utf-8').read(),
    long_description_content_type='text/markdown',
    author='Microsoft Corporation',
    author_email='pysqldriver@microsoft.com',
    url='https://github.com/microsoft/mssql-python',
    packages=find_packages(),
    package_data={
        'mssql_python': ['*.pyd', '*.dll', 'libs/**/*.dll']
    },
    include_package_data=True,
    python_requires='==3.13.*',
    cmake_install_dir='mssql_python',
    cmake_source_dir='mssql_python/pybind',
)
```

### 2.3 Enhanced CMakeLists.txt with ARM64 Support

```cmake
cmake_minimum_required(VERSION 3.15)
project(ddbc_bindings)

# Set C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Set default architecture if not provided
if(NOT DEFINED ARCHITECTURE)
    set(ARCHITECTURE "win64")
endif()

# Add architecture to compiler definitions
add_definitions(-DARCHITECTURE="${ARCHITECTURE}")

# Get Python version
execute_process(
    COMMAND ${PYTHON_EXECUTABLE} -c "import sys; print(f'{sys.version_info.major}{sys.version_info.minor}')"
    OUTPUT_VARIABLE PYTHON_VERSION
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

# Get the full Python version for nuget packages
execute_process(
    COMMAND ${PYTHON_EXECUTABLE} -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}')"
    OUTPUT_VARIABLE PYTHON_FULL_VERSION
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

# Detect Python major and minor versions for various checks
execute_process(
    COMMAND ${PYTHON_EXECUTABLE} -c "import sys; print(sys.version_info.major)"
    OUTPUT_VARIABLE PYTHON_MAJOR
    OUTPUT_STRIP_TRAILING_WHITESPACE
)
execute_process(
    COMMAND ${PYTHON_EXECUTABLE} -c "import sys; print(sys.version_info.minor)"
    OUTPUT_VARIABLE PYTHON_MINOR
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

# Map architecture to wheel format
if(CMAKE_GENERATOR_PLATFORM STREQUAL "ARM64" OR DEFINED ENV{BUILD_ARM64} OR ARCHITECTURE STREQUAL "arm64")
    set(WHEEL_ARCH "arm64")
    set(IS_ARM64 TRUE)
elseif(ARCHITECTURE STREQUAL "win64" OR ARCHITECTURE STREQUAL "amd64" OR ARCHITECTURE STREQUAL "x64")
    set(WHEEL_ARCH "amd64")
    set(IS_ARM64 FALSE)
elseif(ARCHITECTURE STREQUAL "win32" OR ARCHITECTURE STREQUAL "x86")
    set(WHEEL_ARCH "win32")
    set(IS_ARM64 FALSE)
else()
    message(FATAL_ERROR "Unsupported architecture: ${ARCHITECTURE}")
endif()

message(STATUS "Building for architecture: ${ARCHITECTURE} (${WHEEL_ARCH})")
message(STATUS "Python version: ${PYTHON_VERSION}")

# Check for pybind11
find_package(pybind11 CONFIG QUIET)
if(NOT pybind11_FOUND)
    # Try to find pybind11 using Python
    execute_process(
        COMMAND python -c "import pybind11; print(pybind11.get_include())"
        RESULT_VARIABLE PYBIND11_RESULT
        OUTPUT_VARIABLE PYBIND11_INCLUDE_DIR
        OUTPUT_STRIP_TRAILING_WHITESPACE
        ERROR_QUIET
    )
    
    if(NOT PYBIND11_RESULT EQUAL 0)
        message(STATUS "pybind11 not found, attempting to install it")
        execute_process(
            COMMAND python -m pip install pybind11
            RESULT_VARIABLE PIP_RESULT
        )
        
        if(NOT PIP_RESULT EQUAL 0)
            message(FATAL_ERROR "Failed to install pybind11")
        endif()
        
        execute_process(
            COMMAND python -c "import pybind11; print(pybind11.get_include())"
            RESULT_VARIABLE PYBIND11_RESULT
            OUTPUT_VARIABLE PYBIND11_INCLUDE_DIR
            OUTPUT_STRIP_TRAILING_WHITESPACE
        )
    endif()
    
    # Set up include directories manually if find_package failed
    set(PYBIND11_INCLUDE_DIRS ${PYBIND11_INCLUDE_DIR})
else()
    get_target_property(PYBIND11_INCLUDE_DIRS pybind11::headers INTERFACE_INCLUDE_DIRECTORIES)
endif()

message(STATUS "pybind11 include directory: ${PYBIND11_INCLUDE_DIRS}")

# Handle Python libraries based on architecture
if(NOT IS_ARM64)
    # For non-ARM64 platforms, use the standard Python package finder
    find_package(Python REQUIRED COMPONENTS Interpreter Development)
    set(PYTHON_INCLUDE_DIRS ${Python_INCLUDE_DIRS})
    set(PYTHON_LIBRARIES ${Python_LIBRARIES})
else()
    # For ARM64, use nuget to download Python libraries
    set(NUGET_CACHE_DIR "${CMAKE_SOURCE_DIR}/sdk_cache")
    set(PYTHON_NUGET_VERSION "${PYTHON_MAJOR}.${PYTHON_MINOR}")
    
    # Define possible Python versions to support
    set(SUPPORTED_PYTHON_VERSIONS "3.11" "3.12" "3.13")
    
    # If PYTHON_NUGET_VERSION is not in SUPPORTED_PYTHON_VERSIONS, use the latest
    list(FIND SUPPORTED_PYTHON_VERSIONS "${PYTHON_NUGET_VERSION}" VERSION_INDEX)
    if(VERSION_INDEX EQUAL -1)
        message(WARNING "Python version ${PYTHON_NUGET_VERSION} not in supported list, using latest")
        list(GET SUPPORTED_PYTHON_VERSIONS -1 PYTHON_NUGET_VERSION)
    endif()
    
    message(STATUS "Using Python version ${PYTHON_NUGET_VERSION} for ARM64 build")
    
    # Check if nuget package already exists
    file(GLOB PYTHON_ARM64_DIRS "${NUGET_CACHE_DIR}/pythonarm64.${PYTHON_NUGET_VERSION}*")
    list(LENGTH PYTHON_ARM64_DIRS PYTHON_ARM64_DIRS_COUNT)
    
    if(PYTHON_ARM64_DIRS_COUNT EQUAL 0)
        # Download Python ARM64 package using nuget
        message(STATUS "Downloading Python ${PYTHON_NUGET_VERSION} ARM64 libraries using nuget")
        execute_process(
            COMMAND nuget install pythonarm64 -Version ${PYTHON_NUGET_VERSION} -Source https://api.nuget.org/v3/index.json -OutputDirectory ${NUGET_CACHE_DIR}
            RESULT_VARIABLE NUGET_RESULT
        )
        
        if(NOT NUGET_RESULT EQUAL 0)
            message(FATAL_ERROR "Failed to download Python ${PYTHON_NUGET_VERSION} ARM64 libraries with nuget")
        endif()
        
        # Find the downloaded package directory
        file(GLOB PYTHON_ARM64_DIRS "${NUGET_CACHE_DIR}/pythonarm64.${PYTHON_NUGET_VERSION}*")
    endif()
    
    # Get the first matching directory
    list(GET PYTHON_ARM64_DIRS 0 PYTHON_ARM64_DIR)
    message(STATUS "Using Python ARM64 package from: ${PYTHON_ARM64_DIR}")
    
    # Set include and lib directories from nuget package
    set(PYTHON_INCLUDE_DIRS "${PYTHON_ARM64_DIR}/tools/include")
    set(PYTHON_LIBRARIES "${PYTHON_ARM64_DIR}/tools/libs/python${PYTHON_VERSION}.lib")
    
    # Check if the directories and library exist
    if(NOT EXISTS "${PYTHON_INCLUDE_DIRS}")
        message(FATAL_ERROR "Python include directory not found: ${PYTHON_INCLUDE_DIRS}")
    endif()
    if(NOT EXISTS "${PYTHON_LIBRARIES}")
        message(FATAL_ERROR "Python library not found: ${PYTHON_LIBRARIES}")
    endif()
    
    message(STATUS "Python ARM64 include directory: ${PYTHON_INCLUDE_DIRS}")
    message(STATUS "Python ARM64 library: ${PYTHON_LIBRARIES}")
endif()

# Include directories
include_directories(${PYBIND11_INCLUDE_DIRS} ${PYTHON_INCLUDE_DIRS})

# Create the extension module
if(pybind11_FOUND)
    # Use pybind11_add_module if pybind11 was found with find_package
    pybind11_add_module(ddbc_bindings ddbc_bindings.cpp)
else()
    # Manually create the module if find_package failed
    add_library(ddbc_bindings MODULE ddbc_bindings.cpp)
    target_include_directories(ddbc_bindings PRIVATE ${PYBIND11_INCLUDE_DIRS} ${PYTHON_INCLUDE_DIRS})
    target_compile_definitions(ddbc_bindings PRIVATE PYBIND11_DETAILED_ERROR_MESSAGES=1)
    
    # Link to Python libraries if ARM64 and using nuget
    if(IS_ARM64)
        target_link_libraries(ddbc_bindings PRIVATE ${PYTHON_LIBRARIES})
    endif()
endif()

# Set output name with Python version and architecture
set_target_properties(ddbc_bindings PROPERTIES 
    PREFIX ""
    OUTPUT_NAME "ddbc_bindings.cp${PYTHON_VERSION}-${WHEEL_ARCH}"
    SUFFIX ".pyd"
)

# Install target
install(TARGETS ddbc_bindings DESTINATION .)
```

### 2.4 GitHub Actions Workflow

```yaml
name: Build Wheels

on:
  push:
    branches: [ main ]
  pull_request:
    branches: [ main ]
  workflow_dispatch:

jobs:
  build_wheels:
    name: Build wheels on ${{ matrix.os }}
    runs-on: ${{ matrix.os }}
    strategy:
      matrix:
        os: [windows-latest]
        python-version: ['3.13']
        architecture: [x86, x64, ARM64]

    steps:
      - uses: actions/checkout@v3

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: ${{ matrix.python-version }}
          architecture: ${{ matrix.architecture == 'ARM64' && 'x64' || matrix.architecture }}

      - name: Install cibuildwheel
        run: python -m pip install cibuildwheel==2.16.2

      - name: Install build dependencies
        run: python -m pip install cmake pybind11 wheel setuptools scikit-build

      - name: Build wheels
        env:
          CIBW_BUILD: cp313-win*
          CIBW_ARCHS_WINDOWS: ${{ matrix.architecture == 'x86' && 'x86' || matrix.architecture == 'x64' && 'AMD64' || 'ARM64' }}
        run: python -m cibuildwheel --output-dir wheelhouse

      - name: Upload wheels
        uses: actions/upload-artifact@v3
        with:
          name: wheels-${{ matrix.os }}-${{ matrix.architecture }}
          path: ./wheelhouse/*.whl
```

### 2.5 Creating a standalone PYD build script (build_pyd.py)

```python
"""
build_pyd.py - Standalone script to build just the PYD extension

Usage:
    python build_pyd.py [--arch ARCH] [--debug]

Options:
    --arch ARCH     Target architecture: win32, win64 (default), or arm64
    --debug         Build in debug mode instead of release
"""

import os
import sys
import argparse
import subprocess
import platform
import shutil
from pathlib import Path

def build_pyd(architecture='win64', debug=False):
    """Build the PYD extension for the specified architecture."""
    print(f"Building PYD for architecture: {architecture}")
    
    # Determine build configuration
    config = 'Debug' if debug else 'Release'
    
    # Prepare build directory
    build_dir = Path('build') / architecture
    build_dir.mkdir(parents=True, exist_ok=True)
    
    # Get Python version
    py_version = f"{sys.version_info.major}{sys.version_info.minor}"
    print(f"Building for Python {py_version}")
    
    # Map architecture to wheel arch
    arch_map = {
        'win32': 'win32',
        'x86': 'win32',
        'win64': 'amd64',
        'x64': 'amd64',
        'amd64': 'amd64',
        'arm64': 'arm64'
    }
    wheel_arch = arch_map.get(architecture.lower())
    if not wheel_arch:
        print(f"Error: Unknown architecture {architecture}")
        return False
    
    # Set CMake generator platform
    generator_platform = {
        'win32': 'Win32',
        'win64': 'x64',
        'arm64': 'ARM64'
    }.get(architecture.lower())
    
    # Prepare CMake arguments
    cmake_args = [
        'cmake',
        '-S', 'mssql_python/pybind',
        '-B', str(build_dir),
        f'-DCMAKE_BUILD_TYPE={config}',
        f'-DARCHITECTURE={architecture.lower()}',
        '-DPYTHON_EXECUTABLE=' + sys.executable
    ]
    
    # Add generator platform if available
    if generator_platform:
        cmake_args.extend(['-A', generator_platform])
    
    # Configure the project
    print("Configuring CMake project...")
    subprocess.run(cmake_args, check=True)
    
    # Build the project
    print("Building the extension...")
    subprocess.run([
        'cmake',
        '--build', str(build_dir),
        '--config', config
    ], check=True)
    
    # Find the output PYD file
    pyd_name = f"ddbc_bindings.cp{py_version}-{wheel_arch}.pyd"
    pyd_paths = list(build_dir.glob(f"**/{pyd_name}"))
    
    if not pyd_paths:
        print(f"Error: Could not find built PYD file {pyd_name}")
        return False
    
    # Copy to the mssql_python directory
    target_dir = Path('mssql_python')
    for pyd_path in pyd_paths:
        shutil.copy2(pyd_path, target_dir / pyd_name)
        print(f"Copied {pyd_path} to {target_dir / pyd_name}")
    
    return True

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Build PYD extension for mssql-python')
    parser.add_argument('--arch', choices=['win32', 'x86', 'win64', 'x64', 'amd64', 'arm64'],
                        default='win64', help='Target architecture (default: win64)')
    parser.add_argument('--debug', action='store_true', help='Build in debug mode')
    
    args = parser.parse_args()
    success = build_pyd(architecture=args.arch, debug=args.debug)
    
    sys.exit(0 if success else 1)
```

### 2.6 Detailed README_Build.md

```markdown
# Building mssql-python

This document explains how to build the mssql-python package, including wheels for distribution and standalone PYD files for development.

## Prerequisites

- Python 3.13
- CMake 3.15 or newer
- Visual Studio 2022 with C++ build tools
- Git

## Building Wheels with cibuildwheel

cibuildwheel is the recommended method for building distributable wheels, as it ensures the wheels are compatible with the Python packaging ecosystem.

### Install Dependencies

```bash
pip install cibuildwheel cmake pybind11 wheel setuptools scikit-build
```

### Build Wheels Locally

To build wheels for all supported platforms:

```bash
python -m cibuildwheel --platform windows
```

To build for a specific architecture:

```bash
# For x86 (32-bit)
python -m cibuildwheel --platform windows --archs x86

# For x64 (64-bit)
python -m cibuildwheel --platform windows --archs AMD64

# For ARM64
python -m cibuildwheel --platform windows --archs ARM64
```

The built wheels will be in the `wheelhouse` directory.

## Building Just the PYD Files

For development purposes, you might want to build just the PYD extension files without creating a wheel.

```bash
# For x64 (default)
python build_pyd.py

# For x86
python build_pyd.py --arch win32

# For ARM64
python build_pyd.py --arch arm64

# Build in debug mode
python build_pyd.py --debug
```

This will build the PYD file and copy it to the mssql_python directory.

## Building in CI

The repository includes GitHub Actions workflows for automated builds:

- `.github/workflows/build.yml`: Builds wheels for all supported platforms on push to main and pull requests

## Manual Installation from Source

If you want to install the package directly from source:

```bash
pip install -e .
```

This will build the extension in development mode.

## Troubleshooting

### Common Issues

1. **Missing Visual Studio**: Ensure Visual Studio 2022 with C++ tools is installed.
   
2. **Python Version Mismatch**: Make sure you're using Python 3.13.

3. **Missing DLLs**: The wheel repair process should automatically include the necessary DLLs, but if you're experiencing DLL issues, check the paths in the `pyproject.toml` file.

### Validating Wheels

To check if a wheel contains all needed files:

```bash
pip install wheel
wheel unpack wheelhouse/mssql_python-*.whl
```

This will extract the wheel content for inspection.
```

## 3. Benefits of the New Design

1. **Simplified build process**: All configuration is in declarative files, no need for complex batch scripts
2. **Cross-platform support**: Single configuration for Windows x86, x64, and ARM64
3. **CI integration**: Easy to integrate with GitHub Actions, Azure DevOps
4. **Consistent builds**: Same process for all platforms and Python versions
5. **Easy maintenance**: Updating Python versions or adding new platforms is simple
6. **Better dependency management**: Clear declaration of build dependencies
7. **Separate PYD build**: Standalone script for building just the PYD files during development

## 4. Migration Steps

1. Remove `build.bat` as it will no longer be needed
2. Create `pyproject.toml` with cibuildwheel configuration
3. Simplify `setup.py` to use scikit-build
4. Update `CMakeLists.txt` to be simpler and work with cibuildwheel
5. Create GitHub Actions workflow file for CI/CD
6. Create `build_pyd.py` for standalone PYD building
7. Write detailed `README_Build.md`

## 5. Testing Strategy

1. **Local testing**: Build wheels locally with cibuildwheel for all supported platforms
2. **CI testing**: Set up GitHub Actions to build and test wheels
3. **Installation testing**: Test installing the wheels on different Windows platforms
4. **Runtime testing**: Run the test suite against the installed wheels

## Conclusion

Migrating to cibuildwheel will significantly simplify your build process while making it more powerful and consistent across platforms. The automatic handling of Python versions, architectures, and dependencies will make maintenance easier and reduce the risk of build errors. The separate PYD build script provides a quick way for developers to rebuild just the extension during development without needing to create full wheel packages.## 3. Benefits of the New Design

1. **Simplified build process**: All configuration is in declarative files, no need for complex batch scripts
2. **Cross-platform support**: Single configuration for Windows x86, x64, and ARM64
3. **CI integration**: Easy to integrate with GitHub Actions, Azure DevOps
4. **Consistent builds**: Same process for all platforms and Python versions
5. **Easy maintenance**: Updating Python versions or adding new platforms is simple
6. **Better dependency management**: Clear declaration of build dependencies
7. **Separate PYD build**: Standalone script for building just the PYD files during development

## 4. Migration Steps

1. Remove `build.bat` as it will no longer be needed
2. Create `pyproject.toml` with cibuildwheel configuration
3. Simplify `setup.py` to use scikit-build
4. Update `CMakeLists.txt` to be simpler and work with cibuildwheel
5. Create GitHub Actions workflow file for CI/CD
6. Create `build_pyd.py` for standalone PYD building
7. Write detailed `README_Build.md`

## 5. Testing Strategy

1. **Local testing**: Build wheels locally with cibuildwheel for all supported platforms
2. **CI testing**: Set up GitHub Actions to build and test wheels
3. **Installation testing**: Test installing the wheels on different Windows platforms
4. **Runtime testing**: Run the test suite against the installed wheels

## Conclusion

Migrating to cibuildwheel will significantly simplify your build process while making it more powerful and consistent across platforms. The automatic handling of Python versions, architectures, and dependencies will make maintenance easier and reduce the risk of build errors. The separate PYD build script provides a quick way for developers to rebuild just the extension during development without needing to create full wheel packages.
