#!/bin/bash
# Build script for Rust bindings using maturin

set -e

echo "Building Rust bindings with maturin..."

cd mssql_python/rust_bindings

# Install maturin if not already installed
if ! command -v maturin &> /dev/null; then
    echo "Installing maturin..."
    pip install maturin
fi

# Build the Rust extension
echo "Building release version..."
maturin build --release

echo "Build complete!"
echo "To install for development, run: maturin develop"
