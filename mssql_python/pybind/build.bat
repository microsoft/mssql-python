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

rem Set the output PYD path
set PARENT_DIR=%~dp0..
set OUTPUT_PYD_PATH=%PARENT_DIR%\ddbc_bindings.pyd

rem Configure and build based on architecture
if /i "%ARCH%"=="x64" (
    call "C:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools\VC\Auxiliary\Build\vcvarsall.bat" x64
    cmake -S . -B build/build_dir -A x64 -DARCHITECTURE=win64
    cmake --build build/build_dir --config Release
    
    rem Copy the built PYD file
    copy /Y build\build_dir\Release\ddbc_bindings.pyd "%OUTPUT_PYD_PATH%"
    
) else if /i "%ARCH%"=="arm64" (
    call "C:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools\VC\Auxiliary\Build\vcvarsall.bat" x64_arm64
    set BUILD_ARM64=1
    cmake -S . -B build/build_dir -A ARM64 -DARCHITECTURE=arm64
    cmake --build build/build_dir --config Release
    
    rem Ensure DLL dependencies are properly set up
    if not exist "%PARENT_DIR%\libs\winarm64" mkdir "%PARENT_DIR%\libs\winarm64"
    copy /Y "%PARENT_DIR%\libs\winarm64\*.dll" "%PARENT_DIR%\libs\winarm64\"
    if exist "%PARENT_DIR%\libs\winarm64\1033" (
        if not exist "%PARENT_DIR%\libs\winarm64\1033" mkdir "%PARENT_DIR%\libs\winarm64\1033"
        copy /Y "%PARENT_DIR%\libs\winarm64\1033\*" "%PARENT_DIR%\libs\winarm64\1033\"
    )
    
    rem Copy the built PYD file
    copy /Y build\build_dir\Release\ddbc_bindings.pyd "%OUTPUT_PYD_PATH%"
) else (
    echo Error: Unsupported architecture. Use x64 or arm64
    exit /b 1
)

if errorlevel 1 (
    echo Build failed
    exit /b 1
)

echo Build completed successfully
echo The PYD file has been copied to: %OUTPUT_PYD_PATH%
