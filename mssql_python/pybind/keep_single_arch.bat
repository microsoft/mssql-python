@echo off
REM keep_single_arch.bat - Preserves only the target architecture odbc libs and removes others - for packaging
REM This script is intended to be run after the build process to clean up unnecessary architecture libraries.
REM Usage: keep_single_arch.bat [Architecture]

setlocal

set ARCH=%1

if "%ARCH%"=="" (
    echo [ERROR] Architecture must be provided
    exit /b 1
)

echo Removing unnecessary architecture libraries for packaging...

REM Get the directory where this script is located
set SCRIPT_DIR=%~dp0
set LIBS_BASE_DIR=%SCRIPT_DIR%..\libs\windows

if "%ARCH%"=="x64" (
    echo Removing "%LIBS_BASE_DIR%\x86" and "%LIBS_BASE_DIR%\arm64" directories
    if exist "%LIBS_BASE_DIR%\x86" rd /s /q "%LIBS_BASE_DIR%\x86"
    if exist "%LIBS_BASE_DIR%\arm64" rd /s /q "%LIBS_BASE_DIR%\arm64"
    echo Kept x64, removed other architectures.
) else if "%ARCH%"=="x86" (
    echo Removing "%LIBS_BASE_DIR%\x64" and "%LIBS_BASE_DIR%\arm64" directories
    if exist "%LIBS_BASE_DIR%\x64" rd /s /q "%LIBS_BASE_DIR%\x64"
    if exist "%LIBS_BASE_DIR%\arm64" rd /s /q "%LIBS_BASE_DIR%\arm64"
    echo Kept x86, removed other architectures.
) else if "%ARCH%"=="arm64" (
    echo Removing "%LIBS_BASE_DIR%\x64" and "%LIBS_BASE_DIR%\x86" directories
    if exist "%LIBS_BASE_DIR%\x64" rd /s /q "%LIBS_BASE_DIR%\x64"
    if exist "%LIBS_BASE_DIR%\x86" rd /s /q "%LIBS_BASE_DIR%\x86"
    echo Kept arm64, removed other architectures.
)

endlocal
