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
elseif(ARCHITECTURE STREQUAL "win32" OR ARCHITECTURE STREQUAL "x86")
    set(WHEEL_ARCH "win32")
else()
    message(FATAL_ERROR "Unsupported architecture: ${ARCHITECTURE}. Supported architectures are win32, win64, x86, amd64, arm64.")
endif()

# Get Python and pybind11 include paths (needed for all architectures)
execute_process(
    COMMAND python -c "import sysconfig; print(sysconfig.get_path('include'))"
    OUTPUT_VARIABLE PYTHON_INCLUDE_DIR
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

# Add debug messages for Python paths
message(STATUS "Python version: ${PYTHON_VERSION}")
message(STATUS "Python include directory: ${PYTHON_INCLUDE_DIR}")

# Get Python library directory with fallbacks if one method returns None
execute_process(
    COMMAND python -c "import sysconfig; print(sysconfig.get_config_var('LIBDIR'))"
    OUTPUT_VARIABLE PYTHON_LIB_DIR
    OUTPUT_STRIP_TRAILING_WHITESPACE
)
message(STATUS "Python LIBDIR from sysconfig: ${PYTHON_LIB_DIR}")

# If LIBDIR is None or empty, try alternative methods
if("${PYTHON_LIB_DIR}" STREQUAL "None" OR "${PYTHON_LIB_DIR}" STREQUAL "")
    message(STATUS "LIBDIR is None or empty, trying alternative methods")

    # Get the directory of the Python executable
    execute_process(
        COMMAND python -c "import sys, os; print(os.path.dirname(sys.executable))"
        OUTPUT_VARIABLE PYTHON_EXEC_DIR
        OUTPUT_STRIP_TRAILING_WHITESPACE
    )
    message(STATUS "Python executable directory: ${PYTHON_EXEC_DIR}")

    # Check if the libs directory exists, and set PYTHON_LIB_DIR accordingly
    execute_process(
        COMMAND python -c "import sys, os; libs_dir = os.path.join(os.path.dirname(sys.executable), 'libs'); print('EXISTS' if os.path.exists(libs_dir) else 'NOT_EXISTS')"
        OUTPUT_VARIABLE PYTHON_LIBS_EXISTS
        OUTPUT_STRIP_TRAILING_WHITESPACE
    )
    if("${PYTHON_LIBS_EXISTS}" STREQUAL "EXISTS")
        set(PYTHON_LIB_DIR "${PYTHON_EXEC_DIR}/libs")
        message(STATUS "Setting Python libs directory from sys.executable: ${PYTHON_LIB_DIR}")
    endif()
endif()

# If CUSTOM_PYTHON_LIB_DIR is set and exists, use it
if(DEFINED ENV{CUSTOM_PYTHON_LIB_DIR} AND NOT "$ENV{CUSTOM_PYTHON_LIB_DIR}" STREQUAL "" AND NOT "$ENV{CUSTOM_PYTHON_LIB_DIR}" STREQUAL "None")
    message(STATUS "CUSTOM_PYTHON_LIB_DIR is set, using it")
    set(PYTHON_LIB_DIR $ENV{CUSTOM_PYTHON_LIB_DIR})
    message(STATUS "Using custom Python library directory: ${PYTHON_LIB_DIR}")
else()
    message(STATUS "Custom Path is not set or empty, finally using: ${PYTHON_LIB_DIR}")
endif()

execute_process(
    COMMAND python -c "import pybind11; print(pybind11.get_include())"
    OUTPUT_VARIABLE PYBIND11_INCLUDE_DIR
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

# Add module library
add_library(ddbc_bindings MODULE ddbc_bindings.cpp connection/connection.cpp)

# Add include directories for your project
target_include_directories(ddbc_bindings PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}           # Root directory (for ddbc_bindings.h)
    ${CMAKE_CURRENT_SOURCE_DIR}/connection  # connection directory (for connection.h)
)

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
    
elseif(ARCHITECTURE STREQUAL "win32" OR ARCHITECTURE STREQUAL "x86")
    message(STATUS "Building for x86 architecture")
    set(CMAKE_SYSTEM_PROCESSOR "x86")
    set(CMAKE_VS_PLATFORM_NAME "Win32")
    add_definitions(-DTARGET_X86=1)
endif()

set(PYTHON_LIBRARIES "${PYTHON_LIB_DIR}/python${PYTHON_VERSION}.lib")
    
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
