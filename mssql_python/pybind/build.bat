@echo off
setlocal enabledelayedexpansion

REM Usage: build.bat [ARCH]
set ARCH=%1
if "%ARCH%"=="" set ARCH=x64

REM Clean up main build directory if it exists
echo Checking for main build directory...
if exist "build" (
    echo Removing existing build directory...
    rd /s /q build
    echo Build directory removed.
)

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
echo [DIAGNOSTIC] Changed to build directory: "%BUILD_DIR%"

REM Set CMake platform name
set PLATFORM_NAME=%ARCH%
echo [DIAGNOSTIC] Setting up for architecture: %ARCH%
REM Set CMake platform name
if "%ARCH%"=="x64" (
    set PLATFORM_NAME=x64
    echo [DIAGNOSTIC] Using platform name: x64
) else if "%ARCH%"=="x86" (
    set PLATFORM_NAME=Win32
    echo [DIAGNOSTIC] Using platform name: Win32
) else if "%ARCH%"=="arm64" (
    set PLATFORM_NAME=ARM64
    echo [DIAGNOSTIC] Using platform name: ARM64
) else (
    echo [ERROR] Invalid architecture: %ARCH%
    exit /b 1
)

echo [DIAGNOSTIC] Source directory: "%SOURCE_DIR%"

REM Check for Visual Studio - look in standard paths or check if vswhere is available
echo [DIAGNOSTIC] Searching for Visual Studio installation...

set VS_PATH=

REM Try direct paths first (most common locations)
for %%p in (
    "%ProgramFiles(x86)%\Microsoft Visual Studio\2022\Enterprise\VC\Auxiliary\Build\vcvarsall.bat"
    "%ProgramFiles(x86)%\Microsoft Visual Studio\2022\BuildTools\VC\Auxiliary\Build\vcvarsall.bat"
    "%ProgramFiles(x86)%\Microsoft Visual Studio\2022\Professional\VC\Auxiliary\Build\vcvarsall.bat"
    "%ProgramFiles(x86)%\Microsoft Visual Studio\2022\Community\VC\Auxiliary\Build\vcvarsall.bat"
    "%ProgramFiles%\Microsoft Visual Studio\2022\Enterprise\VC\Auxiliary\Build\vcvarsall.bat"
    "%ProgramFiles%\Microsoft Visual Studio\2022\BuildTools\VC\Auxiliary\Build\vcvarsall.bat"
) do (
    echo [DIAGNOSTIC] Checking path: %%p
    if exist %%p (
        set VS_PATH=%%p
        echo [DIAGNOSTIC] Found Visual Studio at: %%p
        goto vs_found
    )
)

REM If we reach here, we didn't find Visual Studio in the standard paths
echo [DIAGNOSTIC] Visual Studio not found in standard paths
echo [DIAGNOSTIC] Looking for vswhere.exe...

REM Try using vswhere if available (common on Azure DevOps agents)
set VSWHERE_PATH="%ProgramFiles(x86)%\Microsoft Visual Studio\Installer\vswhere.exe"
if exist %VSWHERE_PATH% (
    echo [DIAGNOSTIC] Found vswhere at: %VSWHERE_PATH%
    for /f "usebackq tokens=*" %%i in (`%VSWHERE_PATH% -latest -products * -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 -property installationPath`) do (
        set VS_DIR=%%i
        if exist "%%i\VC\Auxiliary\Build\vcvarsall.bat" (
            set VS_PATH="%%i\VC\Auxiliary\Build\vcvarsall.bat"
            echo [DIAGNOSTIC] Found Visual Studio using vswhere at: %%i\VC\Auxiliary\Build\vcvarsall.bat
            goto vs_found
        )
    )
)

echo [WARNING] Visual Studio not found in standard paths or using vswhere
echo [DIAGNOSTIC] Current directory structure:
dir "%ProgramFiles(x86)%\Microsoft Visual Studio" /s /b | findstr "vcvarsall.bat"
dir "%ProgramFiles%\Microsoft Visual Studio" /s /b | findstr "vcvarsall.bat"
echo [ERROR] Could not find Visual Studio installation
exit /b 1

:vs_found
REM Initialize MSVC toolchain
echo [DIAGNOSTIC] Initializing MSVC toolchain with: call %VS_PATH% %ARCH%
call %VS_PATH% %ARCH%
echo [DIAGNOSTIC] MSVC initialization exit code: %errorlevel%
if errorlevel 1 (
    echo [ERROR] Failed to initialize MSVC toolchain
    exit /b 1
)

REM Now invoke CMake with correct source path (options first, path last!)
echo [DIAGNOSTIC] Running CMake configure with: cmake -A %PLATFORM_NAME% -DARCHITECTURE=%ARCH% %SOURCE_DIR%
cmake -A %PLATFORM_NAME% -DARCHITECTURE=%ARCH% %SOURCE_DIR%
echo [DIAGNOSTIC] CMake configure exit code: %errorlevel%
if errorlevel 1 (
    echo [ERROR] CMake configuration failed
    exit /b 1
)

echo [DIAGNOSTIC] Running CMake build with: cmake --build . --config Release
cmake --build . --config Release
echo [DIAGNOSTIC] CMake build exit code: %errorlevel% 
if errorlevel 1 (
    echo [ERROR] CMake build failed
    exit /b 1
)

echo ===== Build completed for %ARCH% Python %PYTAG% ======

REM Delete other architecture directories that aren't needed
echo Removing unnecessary architecture directories...
set LIBS_BASE_DIR=%SOURCE_DIR%..\libs
if "%ARCH%"=="x64" (
    if exist "%LIBS_BASE_DIR%\x86" rd /s /q "%LIBS_BASE_DIR%\x86"
    if exist "%LIBS_BASE_DIR%\arm64" rd /s /q "%LIBS_BASE_DIR%\arm64"
    echo Kept x64, removed other architectures.
) else if "%ARCH%"=="x86" (
    if exist "%LIBS_BASE_DIR%\x64" rd /s /q "%LIBS_BASE_DIR%\x64"
    if exist "%LIBS_BASE_DIR%\arm64" rd /s /q "%LIBS_BASE_DIR%\arm64"
    echo Kept x86, removed other architectures.
) else if "%ARCH%"=="arm64" (
    if exist "%LIBS_BASE_DIR%\x64" rd /s /q "%LIBS_BASE_DIR%\x64"
    if exist "%LIBS_BASE_DIR%\x86" rd /s /q "%LIBS_BASE_DIR%\x86"
    echo Kept arm64, removed other architectures.
)

REM Copy the built .pyd file to source directory
set WHEEL_ARCH=%ARCH%
if "%WHEEL_ARCH%"=="x64" set WHEEL_ARCH=amd64
if "%WHEEL_ARCH%"=="arm64" set WHEEL_ARCH=arm64
if "%WHEEL_ARCH%"=="x86" set WHEEL_ARCH=win32

set PYD_NAME=ddbc_bindings.cp%PYTAG%-%WHEEL_ARCH%.pyd
set OUTPUT_DIR=%BUILD_DIR%\Release

if exist "%OUTPUT_DIR%\%PYD_NAME%" (
    copy /Y "%OUTPUT_DIR%\%PYD_NAME%" "%SOURCE_DIR%\.."
    echo Copied %PYD_NAME% to %SOURCE_DIR%..
    
    REM Copy msvcp140.dll from the libs folder for the appropriate architecture
    set VCREDIST_DLL_PATH=%SOURCE_DIR%..\libs\%ARCH%\vcredist\msvcp140.dll
    if exist "%VCREDIST_DLL_PATH%" (
        copy /Y "%VCREDIST_DLL_PATH%" "%SOURCE_DIR%\.."
        echo Copied msvcp140.dll from %VCREDIST_DLL_PATH% to %SOURCE_DIR%..
    ) else (
        echo [WARNING] Could not find msvcp140.dll at %VCREDIST_DLL_PATH%
    )
) else (
    echo Could not find built .pyd file: %PYD_NAME%
)

endlocal
