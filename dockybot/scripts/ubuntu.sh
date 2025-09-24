#!/bin/bash
set -euo pipefail

echo "Starting Ubuntu 22.04 test pipeline..."

# Install system dependencies
export DEBIAN_FRONTEND=noninteractive
export TZ=UTC
ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
apt-get update && apt-get install -y python3 python3-pip python3-venv python3-full cmake curl wget gnupg software-properties-common build-essential python3-dev pybind11-dev

echo "Installing Microsoft ODBC Driver..."
curl -sSL -O https://packages.microsoft.com/config/ubuntu/22.04/packages-microsoft-prod.deb
dpkg -i packages-microsoft-prod.deb || true
rm packages-microsoft-prod.deb
apt-get update
ACCEPT_EULA=Y apt-get install -y msodbcsql18
ACCEPT_EULA=Y apt-get install -y mssql-tools18
apt-get install -y unixodbc-dev

echo "Setting up Python environment..."
python3 -m venv /opt/venv
source /opt/venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

echo "Verifying database connection setup..."
if [ -z "${DB_CONNECTION_STRING:-}" ]; then
    echo "❌ Warning: DB_CONNECTION_STRING environment variable is not set!"
    exit 1
else
    echo "✅ DB_CONNECTION_STRING is configured"
    # Print first part of connection string for verification (without password)
    echo "Connection target: $(echo "$DB_CONNECTION_STRING" | grep -o 'Server=[^;]*')"
fi

echo "Building C++ extensions..."
cd mssql_python/pybind
chmod +x build.sh
./build.sh

echo "Running tests..."
cd /workspace
source /opt/venv/bin/activate
python -m pytest tests/ -v

echo "Ubuntu test pipeline completed successfully!"