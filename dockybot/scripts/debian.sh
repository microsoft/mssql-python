#!/bin/bash
set -euo pipefail

echo "Starting Debian 11 test pipeline..."

# Install system dependencies
export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y python3 python3-pip python3-venv python3-dev cmake build-essential curl wget gnupg unixodbc-dev

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

echo "Debian test pipeline completed successfully!"