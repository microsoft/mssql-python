@echo off
setlocal enabledelayedexpansion

echo Cleaning up build directories...
if exist build (
    echo Removing existing build directory...
    rmdir /s /q build
)
mkdir build

rem Get Python version for PYD naming
python -c "import sys; ver = f'{sys.version_info.major}{sys.version_info.minor}'; print(ver)" > temp.txt
set /p PYVER=<temp.txt
del temp.txt

rem Default to x64 if no architecture specified
if "%1"=="" (
    set ARCH=x64
) else (
    set ARCH=%1
)

echo Building for target architecture: %ARCH%

rem Set the output PYD path and determine wheel-compatible architecture
set PARENT_DIR=%~dp0..
set WHEEL_ARCH=none

rem Map architecture to wheel-compatible format
if /i "%ARCH%"=="x64" (
    set WHEEL_ARCH=amd64
) else if /i "%ARCH%"=="arm64" (
    set WHEEL_ARCH=arm64
) else (
    set WHEEL_ARCH=win32
)

rem Define the versioned PYD filename
set VERSIONED_PYD_NAME=ddbc_bindings.cp%PYVER%-%WHEEL_ARCH%.pyd

echo Will build: %VERSIONED_PYD_NAME%

rem Configure and build based on architecture
if /i "%ARCH%"=="x64" (
    call "C:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools\VC\Auxiliary\Build\vcvarsall.bat" x64
    cmake -S . -B build/build_dir -A x64 -DARCHITECTURE=win64
    cmake --build build/build_dir --config Release
    
    rem Copy only the versioned PYD file
    copy /Y "build\build_dir\Release\%VERSIONED_PYD_NAME%" "%PARENT_DIR%\%VERSIONED_PYD_NAME%"
    
) else if /i "%ARCH%"=="arm64" (
    call "C:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools\VC\Auxiliary\Build\vcvarsall.bat" x64_arm64
    set BUILD_ARM64=1
    cmake -S . -B build/build_dir -A ARM64 -DARCHITECTURE=arm64
    cmake --build build/build_dir --config Release
    
    rem Copy only the versioned PYD file
    copy /Y "build\build_dir\Release\%VERSIONED_PYD_NAME%" "%PARENT_DIR%\%VERSIONED_PYD_NAME%"
) else if /i "%ARCH%"=="x86" (
    call "C:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools\VC\Auxiliary\Build\vcvarsall.bat" x86
    cmake -S . -B build/build_dir -A Win32 -DARCHITECTURE=win32
    cmake --build build/build_dir --config Release
    
    rem Copy only the versioned PYD file
    copy /Y "build\build_dir\Release\%VERSIONED_PYD_NAME%" "%PARENT_DIR%\%VERSIONED_PYD_NAME%"
) else (
    echo Error: Unsupported architecture. Use x64 or arm64
    exit /b 1
)

if errorlevel 1 (
    echo Build failed
    exit /b 1
)

echo Build completed successfully
echo The versioned PYD file has been copied to:
echo - %PARENT_DIR%\%VERSIONED_PYD_NAME%
