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

# Get Python version and platform info
execute_process(
    COMMAND python -c "import sys; print(f'{sys.version_info.major}{sys.version_info.minor}')"
    OUTPUT_VARIABLE PYTHON_VERSION
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

# Map the architecture to a format similar to what wheels use
if(CMAKE_GENERATOR_PLATFORM STREQUAL "ARM64" OR DEFINED ENV{BUILD_ARM64})
    set(WHEEL_ARCH "arm64")
elseif(ARCHITECTURE STREQUAL "win64" OR ARCHITECTURE STREQUAL "amd64" OR ARCHITECTURE STREQUAL "x64")
    set(WHEEL_ARCH "amd64")
else()
    set(WHEEL_ARCH "win32")
endif()

# Get Python and pybind11 include paths (needed for all architectures)
execute_process(
    COMMAND python -c "import sysconfig; print(sysconfig.get_path('include'))"
    OUTPUT_VARIABLE PYTHON_INCLUDE_DIR
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

execute_process(
    COMMAND python -c "import sysconfig; print(sysconfig.get_config_var('LIBDIR'))"
    OUTPUT_VARIABLE PYTHON_LIB_DIR
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

execute_process(
    COMMAND python -c "import pybind11; print(pybind11.get_include())"
    OUTPUT_VARIABLE PYBIND11_INCLUDE_DIR
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

# Add module library
add_library(ddbc_bindings MODULE ddbc_bindings.cpp)

# Set the output name to include Python version and architecture
set_target_properties(ddbc_bindings PROPERTIES 
    PREFIX ""
    OUTPUT_NAME "ddbc_bindings.cp${PYTHON_VERSION}-${WHEEL_ARCH}"
    SUFFIX ".pyd"
)

# Include directories for all architectures
target_include_directories(ddbc_bindings PRIVATE 
    ${PYTHON_INCLUDE_DIR}
    ${PYBIND11_INCLUDE_DIR}
)

# Special handling for ARM64 builds
if(CMAKE_GENERATOR_PLATFORM STREQUAL "ARM64" OR DEFINED ENV{BUILD_ARM64})
    message(STATUS "Building for ARM64 architecture")
    set(CMAKE_SYSTEM_PROCESSOR "ARM64")
    set(CMAKE_VS_PLATFORM_NAME "ARM64")
    add_definitions(-DTARGET_ARM64=1)
    
    # Set ARM64-specific Python library path
    set(PYTHON_LIB_PATH "${CMAKE_CURRENT_SOURCE_DIR}/libs/arm64")
    set(PYTHON_LIBRARIES "${PYTHON_LIB_PATH}/python${PYTHON_VERSION}.lib")
else()
    # For x64, use the system Python library
    set(PYTHON_LIBRARIES "${PYTHON_LIB_DIR}/python${PYTHON_VERSION}.lib")
endif()

# Link Python library
message(STATUS "Using Python library: ${PYTHON_LIBRARIES}")
message(STATUS "Output PYD name: ddbc_bindings.cp${PYTHON_VERSION}-${WHEEL_ARCH}.pyd")
target_link_libraries(ddbc_bindings PRIVATE ${PYTHON_LIBRARIES})

# Compiler definitions
target_compile_definitions(ddbc_bindings PRIVATE 
    HAVE_SNPRINTF
    _USE_MATH_DEFINES
    PYBIND11_COMPILER_TYPE="_MSC_VER"
    NOMINMAX
)
