cmake_minimum_required(VERSION 3.15)
project(sample_extension)

# Set C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Set target architecture
if(DEFINED ENV{TARGET_ARCH})
    set(TARGET_ARCH $ENV{TARGET_ARCH})
else()
    set(TARGET_ARCH ${CMAKE_SYSTEM_PROCESSOR})
endif()

# Configure architecture-specific settings
if(WIN32)
    if(TARGET_ARCH STREQUAL "ARM64")
        set(CMAKE_SYSTEM_PROCESSOR "ARM64")
        set(CMAKE_VS_PLATFORM_NAME "ARM64")
        add_definitions(-DTARGET_ARM64=1)
        set(PYTHON_LIB_PATH "${CMAKE_CURRENT_SOURCE_DIR}/libs/arm64")
    elseif(TARGET_ARCH STREQUAL "x86")
        set(CMAKE_SYSTEM_PROCESSOR "X86")
        set(CMAKE_VS_PLATFORM_NAME "Win32")
        add_definitions(-DTARGET_X86=1)
        set(PYTHON_LIB_PATH "${CMAKE_CURRENT_SOURCE_DIR}/libs/x86")
    else()
        execute_process(
            COMMAND python -c "import sysconfig; print(sysconfig.get_config_var('LIBDIR'))"
            OUTPUT_VARIABLE PYTHON_LIB_PATH
            OUTPUT_STRIP_TRAILING_WHITESPACE
        )
    endif()
endif()

# Get Python include dir and pybind11 paths
execute_process(
    COMMAND python -c "import pybind11, os; print(os.path.dirname(pybind11.__file__))"
    OUTPUT_VARIABLE PYBIND11_ROOT
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

execute_process(
    COMMAND python -c "import sysconfig; print(sysconfig.get_paths()['include'])"
    OUTPUT_VARIABLE PYTHON_INCLUDE_DIR
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

# Create the module
set(MODULE_NAME "sample_extension")
add_library(${MODULE_NAME} SHARED sample_extension.cpp)

target_include_directories(${MODULE_NAME} PRIVATE 
    "${PYBIND11_ROOT}/include"
    ${PYTHON_INCLUDE_DIR}
)

target_compile_definitions(${MODULE_NAME} PRIVATE 
    HAVE_SNPRINTF
    _USE_MATH_DEFINES
    PYBIND11_COMPILER_TYPE="_MSC_VER"
    NOMINMAX
)

# Link against appropriate Python library
if(CMAKE_SYSTEM_PROCESSOR STREQUAL "ARM64" OR CMAKE_SYSTEM_PROCESSOR STREQUAL "X86")
    target_link_libraries(${MODULE_NAME} PRIVATE "${PYTHON_LIB_PATH}/python313.lib")
else()
    target_link_libraries(${MODULE_NAME} PRIVATE "${PYTHON_LIB_PATH}/python313.lib")
endif()

# Set output properties
set_target_properties(${MODULE_NAME} PROPERTIES 
    PREFIX ""
    SUFFIX ".pyd"
)