cmake_minimum_required(VERSION 3.15)
project(ddbc_bindings)

# Set C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if (MSVC)
    # Enable PDB generation for all target types
    add_compile_options("$<$<CONFIG:Release>:/Zi>")
    add_link_options("$<$<CONFIG:Release>:/DEBUG /OPT:REF /OPT:ICF>")
endif()

# Detect platform
if(WIN32)
    set(PLATFORM_NAME "windows")
elseif(APPLE)
    set(PLATFORM_NAME "macos")
elseif(UNIX)
    set(PLATFORM_NAME "linux")
else()
    message(FATAL_ERROR "Unsupported platform")
endif()

# Set default architecture if not provided
if(NOT DEFINED ARCHITECTURE)
    if(WIN32)
        # Default to x64 for Windows
        set(ARCHITECTURE "win64")
    elseif(APPLE)
        # Always use universal2 for macOS
        set(ARCHITECTURE "universal2")
    else()
        # Default to x64 for other platforms
        set(ARCHITECTURE "x64")
    endif()
endif()

# Add platform and architecture to compiler definitions
add_definitions(-DARCHITECTURE="${ARCHITECTURE}")
add_definitions(-DPLATFORM_NAME="${PLATFORM_NAME}")

# For macOS, always set universal build
if(APPLE)
    set(CMAKE_OSX_ARCHITECTURES "arm64;x86_64" CACHE STRING "Build architectures for macOS" FORCE)
    message(STATUS "Setting universal2 build for macOS (arm64 + x86_64)")
endif()

# Get Python version and platform info
execute_process(
    COMMAND python -c "import sys; print(f'{sys.version_info.major}{sys.version_info.minor}')"
    OUTPUT_VARIABLE PYTHON_VERSION
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

# Map the architecture to a format similar to what wheels use
if(WIN32)
    # Windows architecture mapping
    if(CMAKE_GENERATOR_PLATFORM STREQUAL "ARM64" OR DEFINED ENV{BUILD_ARM64})
        set(WHEEL_ARCH "arm64")
    elseif(ARCHITECTURE STREQUAL "win64" OR ARCHITECTURE STREQUAL "amd64" OR ARCHITECTURE STREQUAL "x64")
        set(WHEEL_ARCH "amd64")
    elseif(ARCHITECTURE STREQUAL "win32" OR ARCHITECTURE STREQUAL "x86")
        set(WHEEL_ARCH "win32")
    else()
        message(FATAL_ERROR "Unsupported Windows architecture: ${ARCHITECTURE}. Supported architectures are win32, win64, x86, amd64, arm64.")
    endif()
elseif(APPLE)
    # macOS always uses universal2
    set(WHEEL_ARCH "universal2")
else()
    # Linux architecture mapping
    if(ARCHITECTURE STREQUAL "arm64")
        set(WHEEL_ARCH "aarch64")
    else()
        set(WHEEL_ARCH "x86_64")
    endif()
endif()

# Get Python and pybind11 include paths
if(DEFINED PYTHON_INCLUDE_DIR AND NOT "${PYTHON_INCLUDE_DIR}" STREQUAL "")
    message(STATUS "Using provided Python include directory: ${PYTHON_INCLUDE_DIR}")
else()
    # Try to detect Python include directory
    execute_process(
        COMMAND python -c "import sysconfig; print(sysconfig.get_path('include'))"
        OUTPUT_VARIABLE PYTHON_INCLUDE_DIR
        OUTPUT_STRIP_TRAILING_WHITESPACE
    )
    message(STATUS "Auto-detected Python include directory: ${PYTHON_INCLUDE_DIR}")
endif()

if(DEFINED PYBIND11_INCLUDE_DIR AND NOT "${PYBIND11_INCLUDE_DIR}" STREQUAL "")
    message(STATUS "Using provided pybind11 include directory: ${PYBIND11_INCLUDE_DIR}")
else()
    # Try to detect pybind11 include directory
    execute_process(
        COMMAND python -c "import pybind11; print(pybind11.get_include())"
        OUTPUT_VARIABLE PYBIND11_INCLUDE_DIR
        OUTPUT_STRIP_TRAILING_WHITESPACE
    )
    message(STATUS "Auto-detected pybind11 include directory: ${PYBIND11_INCLUDE_DIR}")
endif()


# Find Python Library for linking
execute_process(
    COMMAND python -c "import sysconfig; print(sysconfig.get_config_var('LIBDIR'))"
    OUTPUT_VARIABLE PYTHON_LIB_DIR
    OUTPUT_STRIP_TRAILING_WHITESPACE
)
message(STATUS "Python LIBDIR from sysconfig: ${PYTHON_LIB_DIR}")

# On macOS, we may need additional work to find the Python library
if(APPLE AND (NOT PYTHON_LIB_DIR OR "${PYTHON_LIB_DIR}" STREQUAL ""))
    message(STATUS "LIBDIR is None or empty, trying alternative methods")
    
    # Get Python executable path
    execute_process(
        COMMAND python -c "import sys; print(sys.executable)"
        OUTPUT_VARIABLE PYTHON_EXECUTABLE
        OUTPUT_STRIP_TRAILING_WHITESPACE
    )
    
    # Get the directory containing the Python executable
    get_filename_component(PYTHON_BIN_DIR "${PYTHON_EXECUTABLE}" DIRECTORY)
    message(STATUS "Python executable directory: ${PYTHON_BIN_DIR}")
    
    # Try to find the library in or near the executable directory
    set(POTENTIAL_PYTHON_LIB_DIRS
        "${PYTHON_BIN_DIR}/../lib"
        "${PYTHON_BIN_DIR}/../../lib"
        "/Library/Frameworks/Python.framework/Versions/Current/lib"
    )
    
    # Get Python version for library name
    execute_process(
        COMMAND python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')"
        OUTPUT_VARIABLE PYTHON_VERSION_FULL
        OUTPUT_STRIP_TRAILING_WHITESPACE
    )
    
    set(PYTHON_LIBRARY "")
    foreach(LIB_DIR ${POTENTIAL_PYTHON_LIB_DIRS})
        message(STATUS "Checking for Python library in ${LIB_DIR}")
        if(EXISTS "${LIB_DIR}/libpython${PYTHON_VERSION_FULL}.dylib")
            set(PYTHON_LIBRARY "${LIB_DIR}/libpython${PYTHON_VERSION_FULL}.dylib")
            break()
        endif()
    endforeach()
    
    if(PYTHON_LIBRARY)
        message(STATUS "Found Python library: ${PYTHON_LIBRARY}")
    else()
        message(STATUS "Python library not found in standard locations, will try using framework")
        # When using a framework, we don't specify the library directly
    endif()
elseif(WIN32 AND (NOT PYTHON_LIB_DIR OR "${PYTHON_LIB_DIR}" STREQUAL "" OR "${PYTHON_LIB_DIR}" STREQUAL "None"))
    # On Windows, if PYTHON_LIB_DIR is not set, we will try to find it using sysconfig
    message(STATUS "PYTHON_LIB_DIR is None or empty for Windows, trying to find it using sysconfig")
    # Use Python Executable to find the library directory
    execute_process(
        COMMAND python -c "import sys, os; print(os.path.dirname(sys.executable))"
        OUTPUT_VARIABLE PYTHON_EXEC_DIR
        OUTPUT_STRIP_TRAILING_WHITESPACE
    )
    message(STATUS "Python executable directory: ${PYTHON_EXEC_DIR}")
    # Set the library directory based on the executable directory
    set(PYTHON_LIB_DIR "${PYTHON_EXEC_DIR}/libs")
    message(STATUS "Using Python library directory: ${PYTHON_LIB_DIR}")
endif()

if(DEFINED ENV{CUSTOM_PYTHON_LIB_DIR} AND NOT "$ENV{CUSTOM_PYTHON_LIB_DIR}" STREQUAL "" AND NOT "$ENV{CUSTOM_PYTHON_LIB_DIR}" STREQUAL "None")
    message(STATUS "CUSTOM_PYTHON_LIB_DIR is set, using it")
    set(PYTHON_LIB_DIR $ENV{CUSTOM_PYTHON_LIB_DIR})
    message(STATUS "Using custom Python library directory: ${PYTHON_LIB_DIR}")
endif()

message(STATUS "Final Python library directory: ${PYTHON_LIB_DIR}")

# Determine which source file to use based on platform
if(APPLE)
    set(DDBC_SOURCE "ddbc_bindings_mac.cpp")
    message(STATUS "Using macOS-specific source file: ${DDBC_SOURCE}")
    # Only ddbc_bindings_mac.cpp is used on macOS
    # TODO: Implement smart pointer in macOS
    add_library(ddbc_bindings MODULE ${DDBC_SOURCE})
else()
    # This is just windows block
    set(DDBC_SOURCE "ddbc_bindings.cpp")
    message(STATUS "Using standard source file: ${DDBC_SOURCE}")
    # Include connection module for Windows
    add_library(ddbc_bindings MODULE ${DDBC_SOURCE} connection/connection.cpp connection/connection_pool.cpp bcp/bcp_wrapper.cpp)
endif()

# Set the output name to include Python version and architecture
# Use appropriate file extension based on platform
if(WIN32)
    set(MODULE_EXTENSION ".pyd")
else()
    # macOS and Linux both use .so for Python extensions
    set(MODULE_EXTENSION ".so")
endif()

# Set the output name based on the Python version and architecture
set_target_properties(ddbc_bindings PROPERTIES 
    PREFIX ""
    OUTPUT_NAME "ddbc_bindings.cp${PYTHON_VERSION}-${WHEEL_ARCH}"
    SUFFIX "${MODULE_EXTENSION}"
)
# Ensure PDB is generated in Release
set_target_properties(ddbc_bindings PROPERTIES
    COMPILE_PDB_NAME "ddbc_bindings.cp${PYTHON_VERSION}-${WHEEL_ARCH}"
    COMPILE_PDB_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}"
    PDB_NAME "ddbc_bindings.cp${PYTHON_VERSION}-${WHEEL_ARCH}"
    PDB_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}"
)

# Include directories for all architectures
if(NOT DEFINED ODBC_INCLUDE_DIR)
    if(APPLE)
        # On macOS, try to find ODBC headers in Homebrew's location
        message(STATUS "ODBC_INCLUDE_DIR not specified, trying Homebrew location")
        set(ODBC_INCLUDE_DIR "/opt/homebrew/include")
    endif()
endif()

message(STATUS "Using ODBC include directory: ${ODBC_INCLUDE_DIR}")

# Configure target include directories and link libraries
target_include_directories(ddbc_bindings PRIVATE 
    ${CMAKE_CURRENT_SOURCE_DIR}             # Root directory (for ddbc_bindings.h)
    ${CMAKE_CURRENT_SOURCE_DIR}/connection  # connection directory (for connection.h)
    ${CMAKE_CURRENT_SOURCE_DIR}/bcp         # connection directory (for bcp_wrapper.h)
    ${PYTHON_INCLUDE_DIR}
    ${PYBIND11_INCLUDE_DIR}
)

if(APPLE)
    # Add ODBC include directories on macOS
    target_include_directories(ddbc_bindings PRIVATE ${CMAKE_SOURCE_DIR}/../libs/odbc_include ${ODBC_INCLUDE_DIR})
    
    # For macOS, we need to link against the Python framework
    if(PYTHON_LIBRARY)
        # If we found a specific library file, use it
        target_link_libraries(ddbc_bindings PRIVATE ${PYTHON_LIBRARY} dl)
    else()
        # Otherwise, try to link to the framework
        execute_process(
            COMMAND python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')"
            OUTPUT_VARIABLE PYTHON_VERSION_FULL
            OUTPUT_STRIP_TRAILING_WHITESPACE
        )
        
        # Try to find the Python framework
        find_library(PYTHON_FRAMEWORK Python HINTS /Library/Frameworks/Python.framework/Versions/${PYTHON_VERSION_FULL}/lib)
        
        if(PYTHON_FRAMEWORK)
            message(STATUS "Found Python framework: ${PYTHON_FRAMEWORK}")
            target_link_libraries(ddbc_bindings PRIVATE ${PYTHON_FRAMEWORK} dl)
        else()
            # As a last resort, try with -undefined dynamic_lookup flag
            message(STATUS "Python framework not found, using dynamic lookup")
            set_target_properties(ddbc_bindings PROPERTIES LINK_FLAGS "-undefined dynamic_lookup")
            target_link_libraries(ddbc_bindings PRIVATE dl)
        endif()
    endif()
    
    # Set macOS-specific compile options
    target_compile_options(ddbc_bindings PRIVATE
        -Wno-unused-value
        -Wno-deprecated-declarations
    )
else()
    # For other platforms, use standard include and link setup
    # target_include_directories(ddbc_bindings PRIVATE ${ODBC_INCLUDE_DIR})
    
    if(WIN32 AND DEFINED PYTHON_LIB_DIR)
        # On Windows, link with the Python lib
        if(EXISTS "${PYTHON_LIB_DIR}/python${PYTHON_VERSION}.lib")
            message(STATUS "Found Python library: ${PYTHON_LIB_DIR}/python${PYTHON_VERSION}.lib")
            target_link_libraries(ddbc_bindings PRIVATE "${PYTHON_LIB_DIR}/python${PYTHON_VERSION}.lib")
        elseif(EXISTS "${PYTHON_LIB_DIR}/python${PYTHON_VERSION_MAJOR}${PYTHON_VERSION_MINOR}.lib")
            message(STATUS "Found Python library: ${PYTHON_LIB_DIR}/python${PYTHON_VERSION_MAJOR}${PYTHON_VERSION_MINOR}.lib")
            target_link_libraries(ddbc_bindings PRIVATE "${PYTHON_LIB_DIR}/python${PYTHON_VERSION_MAJOR}${PYTHON_VERSION_MINOR}.lib")
        endif()
    endif()
endif()

# Compiler definitions
target_compile_definitions(ddbc_bindings PRIVATE 
    HAVE_SNPRINTF
    _USE_MATH_DEFINES
    PYBIND11_COMPILER_TYPE="_MSC_VER"
    NOMINMAX
)

# Add warning level flags for MSVC
if(MSVC)
    target_compile_options(ddbc_bindings PRIVATE /W4 /WX)
endif()

# Add macOS-specific string conversion fix
if(APPLE)
    message(STATUS "Enabling macOS string conversion fix")
    target_compile_definitions(ddbc_bindings PRIVATE MACOS_STRING_FIX)
    target_compile_options(ddbc_bindings PRIVATE -DAPPLE_SILICON)
endif()
