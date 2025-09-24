#!/bin/sh
set -euo pipefail

echo "Starting Alpine Linux test pipeline..."

# Install system dependencies
apk update
apk add --no-cache python3 py3-pip python3-dev cmake make gcc g++ libc-dev curl wget gnupg unixodbc-dev

echo "Setting up Python environment..."
python3 -m venv /opt/venv
source /opt/venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

echo "Building C++ extensions..."
cd mssql_python/pybind
chmod +x build.sh
./build.sh

echo "Running tests..."
cd /workspace
source /opt/venv/bin/activate
python -m pytest tests/ -v

echo "Alpine test pipeline completed successfully!"