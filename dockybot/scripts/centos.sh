#!/bin/bash
set -euo pipefail

echo "Starting CentOS 7 test pipeline..."

# Install system dependencies
yum update -y
yum groupinstall -y "Development Tools"
yum install -y python3 python3-pip python3-devel cmake curl wget unixodbc-devel

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

echo "CentOS test pipeline completed successfully!"