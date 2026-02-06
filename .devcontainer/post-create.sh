#!/bin/bash

# Post-create script for MSSQL Python Driver devcontainer
set -e

echo "🚀 Setting up MSSQL Python Driver development environment..."

# Install Python packages from requirements.txt
echo "📦 Installing Python packages..."
pip install --upgrade pip setuptools wheel
pip install -r requirements.txt

# Create symlink for 'python' command (build.sh expects it)
echo "🔗 Creating python symlink..."
sudo ln -sf $(which python3) /usr/local/bin/python

# Set up useful shell aliases (for both bash and zsh)
echo "⚡ Setting up aliases..."
cat > ~/.shell_aliases << 'EOF'
# MSSQL Python Driver development aliases
alias build='cd /workspaces/mssql-python/mssql_python/pybind && ./build.sh && cd /workspaces/mssql-python'
alias test='python -m pytest -v'
EOF

# Ensure aliases are sourced in both shells
grep -qxF 'source ~/.shell_aliases' ~/.bashrc 2>/dev/null || echo 'source ~/.shell_aliases' >> ~/.bashrc
grep -qxF 'source ~/.shell_aliases' ~/.zshrc 2>/dev/null || echo 'source ~/.shell_aliases' >> ~/.zshrc

# Verify environment
echo ""
echo "🔍 Verifying environment..."
python --version
pip --version
cmake --version
if command -v sqlcmd &> /dev/null; then
    echo "✅ sqlcmd available"
else
    echo "❌ sqlcmd not found"
fi

# Build the C++ extension
echo ""
echo "🔨 Building C++ extension..."
if cd mssql_python/pybind && ./build.sh && cd ../..; then
    echo "✅ C++ extension built successfully"
else
    echo "❌ C++ extension build failed!"
    exit 1
fi

# Generate random password for SQL Server
echo ""
echo "Generating SQL Server password..."
SA_PASSWORD="$(openssl rand -base64 16 | tr -dc 'A-Za-z0-9' | head -c 16)Aa1!"
echo "$SA_PASSWORD" > /tmp/.sqlserver_sa_password
chmod 600 /tmp/.sqlserver_sa_password

# Start SQL Server container (use Azure SQL Edge for ARM64 compatibility)
# This is optional - if Docker-in-Docker fails, the devcontainer still works
echo ""
echo "Starting SQL Server container (optional)..."

ARCH=$(uname -m)
if [[ "$ARCH" == "aarch64" || "$ARCH" == "arm64" ]]; then
    echo "Detected ARM64 - using Azure SQL Edge..."
    docker run -e 'ACCEPT_EULA=Y' -e "MSSQL_SA_PASSWORD=$SA_PASSWORD" \
        -p 1433:1433 --name sqlserver \
        -d mcr.microsoft.com/azure-sql-edge:latest && SQL_STARTED=true || SQL_STARTED=false
else
    echo "Detected x86_64 - using SQL Server 2025..."
    docker run -e 'ACCEPT_EULA=Y' -e "MSSQL_SA_PASSWORD=$SA_PASSWORD" \
        -p 1433:1433 --name sqlserver \
        -d mcr.microsoft.com/mssql/server:2025-latest && SQL_STARTED=true || SQL_STARTED=false
fi

if [ "$SQL_STARTED" = "true" ]; then
    echo "Waiting for SQL Server to start..."
    sleep 15
else
    echo "WARNING: SQL Server container failed to start (Docker issue)"
    echo "  You can start it manually later with:"
    echo "  docker run -e 'ACCEPT_EULA=Y' -e 'MSSQL_SA_PASSWORD=YourPassword123!' -p 1433:1433 --name sqlserver -d mcr.microsoft.com/azure-sql-edge:latest"
fi

# Set DB_CONNECTION_STRING environment variable (persist across all terminals)
DB_CONNECTION_STRING="Server=localhost,1433;Database=master;UID=sa;PWD=$SA_PASSWORD;TrustServerCertificate=Yes;Encrypt=Yes"

# Write to /etc/environment for system-wide persistence
echo "DB_CONNECTION_STRING=\"$DB_CONNECTION_STRING\"" | sudo tee -a /etc/environment > /dev/null

# Also add to shell rc files for immediate availability in new terminals
echo "export DB_CONNECTION_STRING=\"$DB_CONNECTION_STRING\"" >> ~/.bashrc
echo "export DB_CONNECTION_STRING=\"$DB_CONNECTION_STRING\"" >> ~/.zshrc

# Export for current session
export DB_CONNECTION_STRING

# Display completion message and next steps
echo ""
echo "=============================================="
echo "🎉 Dev environment setup complete!"
echo "=============================================="
echo ""
echo "📦 What's ready:"
echo "  ✅ C++ extension built"
echo "  ✅ SQL Server running (localhost:1433)"
echo "  ✅ DB_CONNECTION_STRING set in environment"
echo ""
echo "🚀 Quick start - just type these commands:"
echo "  python main.py  → Test the connection"
echo "  test            → Run all pytest tests"
echo "  build           → Rebuild C++ extension"
echo ""
echo "=============================================="
echo ""
