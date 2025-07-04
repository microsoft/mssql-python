@echo off
setlocal enabledelayedexpansion

REM Usage: build.bat [ARCH], If ARCH is not specified, it defaults to x64.
set ARCH=%1
if "%ARCH%"=="" set ARCH=x64
echo [DIAGNOSTIC] Target Architecture set to: %ARCH%

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
        echo [SUCCESS] Found Visual Studio at: %%p
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
            echo [SUCCESS] Found Visual Studio using vswhere at: %%i\VC\Auxiliary\Build\vcvarsall.bat
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
echo [DIAGNOSTIC] Running CMake configure with: cmake -A %PLATFORM_NAME% -DARCHITECTURE=%ARCH% "%SOURCE_DIR:~0,-1%"
cmake -A %PLATFORM_NAME% -DARCHITECTURE=%ARCH% "%SOURCE_DIR:~0,-1%"
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
echo [SUCCESS] Build completed successfully.
echo ===== Build completed for %ARCH% Python %PYTAG% ======

@REM REM Call the external script to preserve only the target architecture odbc libs
@REM REM This is commented out to avoid running it automatically. Uncomment if needed.
@REM call "%SOURCE_DIR%keep_single_arch.bat" "%ARCH%"

REM Copy the built .pyd file to source directory
set WHEEL_ARCH=%ARCH%
if "%WHEEL_ARCH%"=="x64" set WHEEL_ARCH=amd64
if "%WHEEL_ARCH%"=="arm64" set WHEEL_ARCH=arm64
if "%WHEEL_ARCH%"=="x86" set WHEEL_ARCH=win32

set PYD_NAME=ddbc_bindings.cp%PYTAG%-%WHEEL_ARCH%.pyd
set OUTPUT_DIR=%BUILD_DIR%\Release

if exist "%OUTPUT_DIR%\%PYD_NAME%" (
    copy /Y "%OUTPUT_DIR%\%PYD_NAME%" "%SOURCE_DIR%\.."
    echo [SUCCESS] Copied %PYD_NAME% to %SOURCE_DIR%\..

    echo [DIAGNOSTIC] Copying PDB file if it exists...
    set PDB_NAME=ddbc_bindings.cp%PYTAG%-%WHEEL_ARCH%.pdb
    echo [DEBUG] Computed PDB_NAME: !PDB_NAME!

    if exist "%OUTPUT_DIR%\!PDB_NAME!" (
        echo [DIAGNOSTIC] Found PDB file: "!PDB_NAME!"
        echo [DIAGNOSTIC] Copying PDB file to source directory...
        copy /Y "%OUTPUT_DIR%\!PDB_NAME!" "%SOURCE_DIR%\.."
        echo [SUCCESS] Copied !PDB_NAME! to %SOURCE_DIR%..
    ) else (
        echo [WARNING] PDB file !PDB_NAME! not found in output directory.
    )

    setlocal enabledelayedexpansion
    for %%I in ("%SOURCE_DIR%..") do (
        set PARENT_DIR=%%~fI
    )
    echo [DIAGNOSTIC] Parent is: !PARENT_DIR!

    set VCREDIST_DLL_PATH=!PARENT_DIR!\libs\windows\!ARCH!\vcredist\msvcp140.dll
    echo [DIAGNOSTIC] Looking for msvcp140.dll at "!VCREDIST_DLL_PATH!"

    if exist "!VCREDIST_DLL_PATH!" (
        copy /Y "!VCREDIST_DLL_PATH!" "%SOURCE_DIR%\.."
        echo [SUCCESS] Copied msvcp140.dll from !VCREDIST_DLL_PATH! to "%SOURCE_DIR%\.."
    ) else (
        echo [ERROR] Could not find msvcp140.dll at "!VCREDIST_DLL_PATH!"
        exit /b 1
    )
) else (
    echo [ERROR] Could not find built .pyd file: %PYD_NAME%
    REM Exit with an error code here if the .pyd file is not found
    exit /b 1
)

endlocal
