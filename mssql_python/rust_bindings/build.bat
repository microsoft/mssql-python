@echo off
REM Build script for Rust bindings on Windows using maturin

echo Building Rust bindings with maturin...

cd mssql_python\rust_bindings

REM Check if maturin is installed
where maturin >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo Installing maturin...
    pip install maturin
)

REM Build the Rust extension
echo Building release version...
maturin build --release

echo Build complete!
echo To install for development, run: maturin develop
