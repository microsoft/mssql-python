@echo off
setlocal

rem Get Python version for PYD naming
python -c "import sys; ver = f'{sys.version_info.major}{sys.version_info.minor}{sys.version_info.micro}'; print(ver)" > temp.txt
set /p PYVER=<temp.txt
del temp.txt

rem Default to x64 if no architecture specified
if "%1"=="" (
    set ARCH=x64
) else (
    set ARCH=%1
)

echo Building for target architecture: %ARCH% with Python %PYVER%

rem Configure and build based on architecture
if /i "%ARCH%"=="x64" (
    call "C:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools\VC\Auxiliary\Build\vcvarsall.bat" x64
    cmake -S . -B build/build_dir -A x64
    cmake --build build/build_dir --config Release
    copy /Y build\build_dir\Release\*.pyd sample_extension.cp%PYVER%-%ARCH%.pyd
) else if /i "%ARCH%"=="x86" (
    call "C:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools\VC\Auxiliary\Build\vcvarsall.bat" x86
    cmake -S . -B build/build_dir -A Win32 -DTARGET_ARCH=x86
    cmake --build build/build_dir --config Release
    copy /Y build\build_dir\Release\*.pyd sample_extension.cp%PYVER%-win32.pyd
) else if /i "%ARCH%"=="arm64" (
    call "C:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools\VC\Auxiliary\Build\vcvarsall.bat" x64_arm64
    cmake -S . -B build/build_dir -A ARM64
    cmake --build build/build_dir --config Release
    copy /Y build\build_dir\Release\*.pyd sample_extension.cp%PYVER%-%ARCH%.pyd
) else (
    echo Error: Unsupported architecture. Use x86, x64 or arm64
    exit /b 1
)

if errorlevel 1 (
    echo Build failed
    exit /b 1
)

echo Build completed successfully