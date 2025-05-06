cmake_minimum_required(VERSION 3.10)
project(ddbc_bindings)

# Set the C++ standard
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Locate Python interpreter
find_package(Python3 REQUIRED COMPONENTS Interpreter Development)

# Dynamically determine the path to pybind11
execute_process(
    COMMAND "${Python3_EXECUTABLE}" -m pybind11 --cmakedir
    OUTPUT_VARIABLE pybind11_DIR
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

# Normalize the path to ensure platform independence
file(TO_CMAKE_PATH "${pybind11_DIR}" pybind11_DIR)

# Strip double quotes from the path
string(REPLACE "\"" "" pybind11_DIR "${pybind11_DIR}")

message(STATUS "pybind11 directory: ${pybind11_DIR}")

# Add pybind11 module path
if(NOT EXISTS "${pybind11_DIR}/pybind11Config.cmake")
    message(FATAL_ERROR "pybind11 not found. Ensure pybind11 is installed in the Python environment.")
endif()

list(APPEND CMAKE_PREFIX_PATH "${pybind11_DIR}")
find_package(pybind11 REQUIRED)

# Allow specifying build type (default to Release)
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug CACHE STRING "Build type: Debug or Release" FORCE)
endif()

# Set the output name (for Python extension, it should be a .pyd on Windows)
set(CMAKE_WINDOWS_EXPORT_ALL_SYMBOLS ON)
set(PYTHON_EXTENSION_NAME ddbc_bindings)

# Add the shared library (DLL or .pyd) instead of an executable
add_library(${PYTHON_EXTENSION_NAME} MODULE connection/connection.cpp ddbc_bindings.cpp)

# Add include directories for your project
target_include_directories(${PYTHON_EXTENSION_NAME} PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}           # Root directory (for ddbc_bindings.h)
    ${CMAKE_CURRENT_SOURCE_DIR}/connection  # connection directory (for connection.h)
)

# Set output directories based on build type
set_target_properties(${PYTHON_EXTENSION_NAME} PROPERTIES
    PREFIX ""
    SUFFIX ".pyd"
    RUNTIME_OUTPUT_DIRECTORY_DEBUG ${CMAKE_BINARY_DIR}/Debug
    RUNTIME_OUTPUT_DIRECTORY_RELEASE ${CMAKE_BINARY_DIR}/Release
)

# Link pybind11 library
target_link_libraries(${PYTHON_EXTENSION_NAME} PRIVATE pybind11::module)

# Link the Python library
target_link_libraries(${PYTHON_EXTENSION_NAME} PRIVATE ${Python3_LIBRARIES})

# Compiler flags for Debug and Release builds
target_compile_definitions(${PYTHON_EXTENSION_NAME} PRIVATE 
    $<$<CONFIG:Debug>:_DEBUG>
    $<$<CONFIG:Release>:NDEBUG>
)

# Print the current build type and pybind11 path for debugging purposes
message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")
message(STATUS "pybind11 directory: ${pybind11_DIR}")
