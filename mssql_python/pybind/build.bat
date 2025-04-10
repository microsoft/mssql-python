@echo off
setlocal enabledelayedexpansion

REM Usage: build.bat [ARCH]
set ARCH=%1
if "%ARCH%"=="" set ARCH=x64

REM Get Python version from active interpreter
for /f %%v in ('python -c "import sys; print(f'{sys.version_info.major}{sys.version_info.minor}')"') do set PYTAG=%%v

echo ===================================
echo Building for: %ARCH% / Python %PYTAG%
echo ===================================

REM Save absolute source directory
set SOURCE_DIR=%~dp0

REM Go to build output directory
set BUILD_DIR=%SOURCE_DIR%build\%ARCH%\py%PYTAG%
if exist "%BUILD_DIR%" rd /s /q "%BUILD_DIR%"
mkdir "%BUILD_DIR%"
cd /d "%BUILD_DIR%"

REM Set CMake platform name
set PLATFORM_NAME=%ARCH%
REM Set CMake platform name
if "%ARCH%"=="x64" (
    set PLATFORM_NAME=x64
) else if "%ARCH%"=="x86" (
    set PLATFORM_NAME=Win32
) else if "%ARCH%"=="arm64" (
    set PLATFORM_NAME=ARM64
) else (
    echo Invalid architecture: %ARCH%
    exit /b 1
)


REM Initialize MSVC toolchain
call "%ProgramFiles(x86)%\Microsoft Visual Studio\2022\BuildTools\VC\Auxiliary\Build\vcvarsall.bat" %ARCH%

REM Now invoke CMake with correct source path (options first, path last!)
cmake -A %PLATFORM_NAME% -DARCHITECTURE=%ARCH% "%SOURCE_DIR%"
if errorlevel 1 exit /b 1

cmake --build . --config Release
if errorlevel 1 exit /b 1

echo ===== Build completed for %ARCH% Python %PYTAG% ======

REM Copy the built .pyd file to source directory
set WHEEL_ARCH=%ARCH%
if "%WHEEL_ARCH%"=="x64" set WHEEL_ARCH=amd64
if "%WHEEL_ARCH%"=="arm64" set WHEEL_ARCH=arm64
if "%WHEEL_ARCH%"=="x86" set WHEEL_ARCH=win32

set PYD_NAME=ddbc_bindings.cp%PYTAG%-%WHEEL_ARCH%.pyd
set OUTPUT_DIR=%BUILD_DIR%\Release

if exist "%OUTPUT_DIR%\%PYD_NAME%" (
    copy /Y "%OUTPUT_DIR%\%PYD_NAME%" "%SOURCE_DIR%\.."
    echo Copied %PYD_NAME% to %SOURCE_DIR%
) else (
    echo Could not find built .pyd file: %PYD_NAME%
)

endlocal
