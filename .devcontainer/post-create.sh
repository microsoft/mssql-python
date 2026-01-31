#!/bin/bash

# Post-create script for MSSQL Python Driver devcontainer
set -e

echo "🚀 Setting up MSSQL Python Driver development environment..."

# Update package lists
echo "📦 Updating package lists..."
sudo apt-get update

# Install system dependencies required for the project
echo "🔧 Installing system dependencies..."
sudo apt-get install -y \
    python3 \
    python3-pip \
    python3-venv \
    python3-full \
    cmake \
    curl \
    wget \
    gnupg \
    software-properties-common \
    build-essential \
    python3-dev \
    pybind11-dev

export TZ=UTC
ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Note: ODBC Driver is already installed in the Dockerfile

# Create a Python virtual environment
echo "🐍 Creating Python virtual environment..."
python3 -m venv /workspaces/mssql-python/.venv
source /workspaces/mssql-python/.venv/bin/activate

python -m pip install --upgrade pip

# Make the virtual environment globally available
echo 'source /workspaces/mssql-python/.venv/bin/activate' >> ~/.bashrc

# Install project dependencies
echo "📚 Installing project dependencies..."
pip install -r requirements.txt

# Build the native extension
echo "🔨 Building native extension..."
cd /workspaces/mssql-python/mssql_python/pybind
./build.sh
cd /workspaces/mssql-python

# Create useful aliases
echo "⚡ Setting up aliases..."
cat >> ~/.bashrc << 'EOF'

# MSSQL Python Driver aliases
alias ll='ls -alF'
alias la='ls -A'
alias l='ls -CF'
alias clean='find . -type f -name "*.pyc" -delete && find . -type d -name "__pycache__" -delete'

EOF

# Set up git configuration (if not already configured)
echo "🔧 Configuring git..."
if [ -z "$(git config --global user.name)" ]; then
    echo "Git user name not set. You may want to configure it with:"
    echo "  git config --global user.name 'Your Name'"
fi
if [ -z "$(git config --global user.email)" ]; then
    echo "Git user email not set. You may want to configure it with:"
    echo "  git config --global user.email 'your.email@example.com'"
fi

# Display information about the environment
echo ""
echo "✅ Development environment setup complete!"

