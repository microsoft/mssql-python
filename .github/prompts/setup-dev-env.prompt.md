---
description: "Set up development environment for mssql-python"
name: "mssql-python-setup"
agent: 'agent'
model: Claude Sonnet 4.5 (copilot)
---
# Setup Development Environment Prompt for microsoft/mssql-python

You are a development assistant helping set up the development environment for the mssql-python driver.

## TASK

Help the developer set up their local environment for development. This is typically run **once** when:
- Cloning the repository for the first time
- Setting up a new machine
- After a major dependency change
- Troubleshooting environment issues

---

## STEP 1: Verify Python Version

### 1.1 Check Python Installation

```bash
python --version
# or
python3 --version
```

**Supported versions:** Refer to `pyproject.toml` or `setup.py` (`python_requires`/classifiers) for the authoritative list. Generally, Python 3.10 or later is required.

| Version | Status |
|---------|--------|
| 3.10+ (per project metadata) | ✅ Supported |
| 3.9 and below | ❌ Not supported |

### 1.2 Check Python Location

```bash
which python
# or on Windows
where python
```

> ⚠️ Make note of this path - you'll need to ensure your venv uses this Python.

---

## STEP 2: Virtual Environment Setup

### 2.1 Check for Existing Virtual Environment

```bash
# Check if a venv is already active
echo $VIRTUAL_ENV
```

**If output shows a path** → venv is active, skip to Step 2.4 to verify it

**If output is empty** → No venv active, continue to Step 2.2

### 2.2 Create Virtual Environment (if needed)

```bash
# From repository root
python -m venv myvenv

# Or with a specific Python version
python3.13 -m venv myvenv
```

### 2.3 Activate Virtual Environment

```bash
# macOS/Linux
source myvenv/bin/activate

# Windows (Command Prompt)
myvenv\Scripts\activate.bat

# Windows (PowerShell)
myvenv\Scripts\Activate.ps1
```

### 2.4 Verify Virtual Environment

```bash
# Check venv is active
echo $VIRTUAL_ENV
# Expected: /path/to/mssql-python/myvenv

# Verify Python is from venv
which python
# Expected: /path/to/mssql-python/myvenv/bin/python

# Verify Python version in venv
python --version
# Expected: Python 3.10+ 
```

---

## STEP 3: Install Python Dependencies

### 3.1 Upgrade pip (Recommended)

```bash
pip install --upgrade pip
```

### 3.2 Install requirements.txt

```bash
pip install -r requirements.txt
```

### 3.3 Install Development Dependencies

```bash
# Build dependencies
pip install pybind11

# Test dependencies
pip install pytest pytest-cov

# Linting/formatting (optional)
pip install black flake8 autopep8
```

### 3.4 Install Package in Development Mode

```bash
pip install -e .
```

### 3.5 Verify Python Dependencies

```bash
# Check critical packages
python -c "import pybind11; print('✅ pybind11:', pybind11.get_include())"
python -c "import pytest; print('✅ pytest:', pytest.__version__)"
python -c "import mssql_python; print('✅ mssql_python installed')"
```

---

## STEP 4: Platform-Specific Prerequisites

### 4.0 Detect Platform

```bash
uname -s
# Darwin → macOS
# Linux → Linux
# (Windows users: skip this, you know who you are)
```

---

### 4.1 macOS Prerequisites

#### Check CMake

```bash
cmake --version
# Expected: cmake version 3.15+
```

**If missing:**
```bash
brew install cmake
```

#### Check ODBC Headers

```bash
ls /opt/homebrew/include/sql.h 2>/dev/null || ls /usr/local/include/sql.h 2>/dev/null
```

**If missing:**
```bash
# Install Microsoft ODBC Driver (provides headers for development)
brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
ACCEPT_EULA=Y brew install msodbcsql18
```

#### Verify macOS Setup

```bash
echo "=== macOS Development Environment ===" && \
cmake --version | head -1 && \
python -c "import pybind11; print('pybind11:', pybind11.get_include())" && \
ls /opt/homebrew/include/sql.h 2>/dev/null && echo "✅ ODBC headers found" || echo "❌ ODBC headers missing"
```

---

### 4.2 Linux Prerequisites

#### Check CMake

```bash
cmake --version
# Expected: cmake version 3.15+
```

#### Check Compiler

```bash
gcc --version || clang --version
```

**If missing (Debian/Ubuntu):**
```bash
sudo apt-get update
sudo apt-get install -y cmake build-essential python3-dev
```

**If missing (RHEL/CentOS/Fedora):**
```bash
sudo dnf install -y cmake gcc-c++ python3-devel
```

**If missing (SUSE):**
```bash
sudo zypper install -y cmake gcc-c++ python3-devel
```

#### Verify Linux Setup

```bash
echo "=== Linux Development Environment ===" && \
cmake --version | head -1 && \
gcc --version | head -1 && \
python -c "import pybind11; print('pybind11:', pybind11.get_include())"
```

---

### 4.3 Windows Prerequisites

#### Visual Studio Build Tools 2022

1. Download from: https://visualstudio.microsoft.com/downloads/#build-tools-for-visual-studio-2022
2. Run installer
3. Select **"Desktop development with C++"** workload
4. This includes CMake automatically

#### Verify Windows Setup

Open **Developer Command Prompt for VS 2022** and run:

```cmd
cmake --version
cl
python -c "import pybind11; print('pybind11:', pybind11.get_include())"
```

> ⚠️ **Important:** Always use **Developer Command Prompt for VS 2022** for building, not regular cmd or PowerShell.

---

## STEP 5: Configure Environment Variables

### 5.1 Database Connection String (For Integration Tests)

> ⚠️ **SECURITY WARNING:**
> - **NEVER commit actual credentials** to version control or share them in documentation.
> - `TrustServerCertificate=yes` disables TLS certificate validation and should **ONLY be used for isolated local development**, never for remote or production connections.

```bash
# Set connection string for tests (LOCAL DEVELOPMENT ONLY)
# Replace placeholders with your own values - NEVER commit real credentials!
export DB_CONNECTION_STRING="Driver={ODBC Driver 18 for SQL Server};Server=localhost;Database=testdb;UID=your_user;PWD=your_password;TrustServerCertificate=yes"

# Verify it's set
echo $DB_CONNECTION_STRING
```

**Windows (LOCAL DEVELOPMENT ONLY):**
```cmd
REM Replace placeholders with your own values - NEVER commit real credentials!
set DB_CONNECTION_STRING=Driver={ODBC Driver 18 for SQL Server};Server=localhost;Database=testdb;UID=your_user;PWD=your_password;TrustServerCertificate=yes
```

> 💡 **Tip:** Add this to your shell profile (`.bashrc`, `.zshrc`) or venv's `activate` script to persist it.

### 5.2 Optional: Add to venv activate script

```bash
# Append to venv activate script so it's set automatically
echo 'export DB_CONNECTION_STRING="your_connection_string"' >> myvenv/bin/activate
```

---

## STEP 6: Start/Verify SQL Server

### 6.1 Check if SQL Server is Running

#### Option A: Using Docker (Recommended for Development)

**Check if SQL Server container exists:**

```bash
docker ps -a | grep mssql
```

**If container exists but is stopped:**

```bash
docker start mssql-dev
```

**If no container exists, create and start one:**

```bash
docker run -e "ACCEPT_EULA=Y" -e "MSSQL_SA_PASSWORD=YourStrongPassword123!" \
  -p 1433:1433 --name mssql-dev \
  -d mcr.microsoft.com/mssql/server:2022-latest
```

**Verify container is healthy:**

```bash
# Check container status
docker ps | grep mssql

# Check SQL Server logs for "ready" message
docker logs mssql-dev 2>&1 | grep "SQL Server is now ready"
```

**Useful Docker commands:**

```bash
# Stop SQL Server container
docker stop mssql-dev

# Start SQL Server container
docker start mssql-dev

# Restart SQL Server container
docker restart mssql-dev

# View SQL Server logs
docker logs -f mssql-dev

# Remove container (will delete data!)
docker rm -f mssql-dev
```

#### Option B: Native SQL Server Installation

**macOS:**

SQL Server doesn't run natively on macOS. Use Docker (Option A) or connect to a remote server.

**Linux (Ubuntu/Debian):**

```bash
# Check if SQL Server service is running
sudo systemctl status mssql-server

# Start SQL Server service
sudo systemctl start mssql-server

# Enable auto-start on boot
sudo systemctl enable mssql-server

# Restart SQL Server
sudo systemctl restart mssql-server

# Stop SQL Server
sudo systemctl stop mssql-server
```

**Linux (RHEL/CentOS):**

```bash
# Check status
sudo systemctl status mssql-server

# Start/Stop/Restart commands are the same as Ubuntu/Debian above
```

**Windows:**

```powershell
# Check SQL Server service status (PowerShell as Admin)
Get-Service -Name 'MSSQL$*' | Select-Object Name, Status

# Start SQL Server service
Start-Service -Name 'MSSQL$MSSQLSERVER'

# Stop SQL Server service
Stop-Service -Name 'MSSQL$MSSQLSERVER'

# Restart SQL Server service
Restart-Service -Name 'MSSQL$MSSQLSERVER'

# Or use SQL Server Configuration Manager (GUI)
```

#### Option C: Azure SQL Database

No local SQL Server needed. Just ensure:
- Your Azure SQL Database is running
- Firewall rules allow your IP address
- Connection string is correct with proper credentials

### 6.2 Test SQL Server Connectivity

#### Using sqlcmd (SQL Server Command Line Tool)

**Test local SQL Server connection:**

```bash
sqlcmd -S localhost -U sa -P 'YourPassword' -Q "SELECT @@VERSION"
```

**If sqlcmd is not installed:**

```bash
# macOS (via Homebrew)
brew install sqlcmd

# Linux (Ubuntu/Debian)
curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
sudo add-apt-repository "$(wget -qO- https://packages.microsoft.com/config/ubuntu/20.04/prod.list)"
sudo apt-get update
sudo apt-get install sqlcmd

# Windows
# Download from: https://learn.microsoft.com/en-us/sql/tools/sqlcmd/sqlcmd-utility
```

#### Using Python (Test with main.py)

```bash
# This should connect and list databases
python main.py
```

**Expected output:**
```
...Connection logs...
Database ID: 1, Name: master
Database ID: 2, Name: tempdb
...
Connection closed successfully.
```

**If this fails:** See troubleshooting section below.

### 6.3 Troubleshoot SQL Server Connectivity

#### Common Issues and Solutions

| Issue | Symptoms | Solution |
|-------|----------|----------|
| **SQL Server not running** | "Cannot open server", "No connection could be made" | Start SQL Server (see 6.1) |
| **Wrong credentials** | "Login failed for user" | Check username/password in connection string |
| **Port not accessible** | "TCP Provider: No connection could be made" | Check firewall, verify port 1433 is open |
| **SSL/TLS errors** | "SSL Provider: The certificate chain was issued by an authority" | Add `TrustServerCertificate=yes` to connection string (dev only) |
| **ODBC driver missing** | "Driver not found" | Install ODBC Driver 18 (see Step 4) |
| **Network timeout** | Connection times out | Check server address, network connectivity |

#### Verify SQL Server Port

```bash
# Check if port 1433 is listening (macOS/Linux)
lsof -i :1433

# Or use netstat
netstat -an | grep 1433

# Test port connectivity with telnet
telnet localhost 1433

# Or use nc (netcat)
nc -zv localhost 1433
```

**If port 1433 is not listening:**
- SQL Server is not running → Start it
- SQL Server is using a different port → Check configuration
- Firewall is blocking the port → Configure firewall

#### Check SQL Server Logs

**Docker:**

```bash
docker logs mssql-dev --tail 100
```

**Linux:**

```bash
# View error log
sudo cat /var/opt/mssql/log/errorlog

# View last 50 lines
sudo tail -50 /var/opt/mssql/log/errorlog

# Follow logs in real-time
sudo tail -f /var/opt/mssql/log/errorlog
```

**Windows:**

```
C:\Program Files\Microsoft SQL Server\MSSQL15.MSSQLSERVER\MSSQL\Log\ERRORLOG
```

Or use SQL Server Management Studio (SSMS) → Management → SQL Server Logs

#### Enable SQL Server Network Access (Linux)

```bash
# Allow SQL Server through firewall
sudo ufw allow 1433/tcp

# Configure SQL Server to listen on TCP port 1433
sudo /opt/mssql/bin/mssql-conf set network.tcpport 1433

# Enable remote connections
sudo /opt/mssql/bin/mssql-conf set network.tcpenabled true

# Restart SQL Server
sudo systemctl restart mssql-server
```

#### Docker Networking Issues

```bash
# Check Docker network
docker network inspect bridge

# Check if container is using the correct port mapping
docker port mssql-dev

# Recreate container with explicit port mapping
docker rm -f mssql-dev
docker run -e "ACCEPT_EULA=Y" -e "MSSQL_SA_PASSWORD=YourStrongPassword123!" \
  -p 1433:1433 --name mssql-dev \
  -d mcr.microsoft.com/mssql/server:2022-latest
```

#### Azure SQL Database Firewall

```bash
# Get your current IP address
curl -s https://api.ipify.org

# Add this IP to Azure SQL Database firewall rules:
# 1. Go to Azure Portal
# 2. Navigate to your SQL Server
# 3. Settings → Networking
# 4. Add your IP address to firewall rules
```

---

## STEP 7: Final Verification

Run this comprehensive check:

```bash
echo "========================================" && \
echo "Development Environment Verification" && \
echo "========================================" && \
echo "" && \
echo "1. Virtual Environment:" && \
if [ -n "$VIRTUAL_ENV" ]; then echo "   ✅ Active: $VIRTUAL_ENV"; else echo "   ❌ Not active"; fi && \
echo "" && \
echo "2. Python:" && \
echo "   $(python --version)" && \
echo "   Path: $(which python)" && \
echo "" && \
echo "3. Key Packages:" && \
python -c "import pybind11; print('   ✅ pybind11:', pybind11.__version__)" 2>/dev/null || echo "   ❌ pybind11 not installed" && \
python -c "import pytest; print('   ✅ pytest:', pytest.__version__)" 2>/dev/null || echo "   ❌ pytest not installed" && \
python -c "import mssql_python; print('   ✅ mssql_python installed')" 2>/dev/null || echo "   ❌ mssql_python not installed" && \
echo "" && \
echo "4. Build Tools:" && \
cmake --version 2>/dev/null | head -1 | sed 's/^/   ✅ /' || echo "   ❌ cmake not found" && \
echo "" && \
echo "5. Connection String:" && \
if [ -n "$DB_CONNECTION_STRING" ]; then echo "   ✅ Set (hidden for security)"; else echo "   ⚠️ Not set (integration tests will fail)"; fi && \
echo "" && \
echo "========================================"
```

---

## Troubleshooting

### ❌ "Python version not supported"

**Cause:** Python < 3.10

**Fix:**
```bash
# Install Python 3.13 (macOS)
brew install python@3.13

# Create venv with specific version
python3.13 -m venv myvenv
source myvenv/bin/activate
```

### ❌ "No module named venv"

**Cause:** venv module not installed (some Linux distros)

**Fix:**
```bash
# Debian/Ubuntu
sudo apt-get install python3-venv

# Then create venv
python3 -m venv myvenv
```

### ❌ "pip install fails with permission error"

**Cause:** Trying to install globally without sudo, or venv not active

**Fix:**
```bash
# Verify venv is active
echo $VIRTUAL_ENV

# If empty, activate it
source myvenv/bin/activate

# Then retry pip install
pip install -r requirements.txt
```

### ❌ "pybind11 installed but not found during build"

**Cause:** pybind11 installed in different Python than build uses

**Fix:**
```bash
# Check which Python has pybind11
python -c "import pybind11; print(pybind11.get_include())"

# Ensure same Python is used for build
which python

# Reinstall in correct venv if needed
pip install pybind11
```

### ❌ "cmake not found" (macOS)

**Fix:**
```bash
brew install cmake

# Or if Homebrew not in PATH
export PATH="/opt/homebrew/bin:$PATH"
```

### ❌ "cmake not found" (Windows)

**Cause:** Not using Developer Command Prompt

**Fix:** 
1. Close current terminal
2. Open **Developer Command Prompt for VS 2022** from Start Menu
3. Navigate to project and retry

### ❌ "gcc/g++ not found" (Linux)

**Fix:**
```bash
# Debian/Ubuntu
sudo apt-get install build-essential

# RHEL/CentOS/Fedora
sudo dnf groupinstall "Development Tools"
```

### ❌ "ODBC headers not found" (macOS)

**Cause:** Microsoft ODBC Driver not installed

**Fix:**
```bash
brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
ACCEPT_EULA=Y brew install msodbcsql18
```

### ❌ "requirements.txt installation fails"

**Cause:** Network issues, outdated pip, or conflicting packages

**Fix:**
```bash
# Upgrade pip first
pip install --upgrade pip

# Try with verbose output
pip install -r requirements.txt -v

# If specific package fails, install it separately
pip install <package-name>
```

### ❌ PowerShell: "Activate.ps1 cannot be loaded because running scripts is disabled"

**Cause:** PowerShell execution policy

**Fix:**
```powershell
# Run PowerShell as Administrator
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser

# Then activate
.\myvenv\Scripts\Activate.ps1
```

---

## Quick Reference

### One-Liner Fresh Setup (macOS/Linux)

```bash
# Complete setup from scratch
python3 -m venv myvenv && \
source myvenv/bin/activate && \
pip install --upgrade pip && \
pip install -r requirements.txt && \
pip install pybind11 pytest pytest-cov && \
pip install -e . && \
echo "✅ Setup complete!"
```

### Minimum Required Packages

| Package | Purpose | Required For |
|---------|---------|--------------|
| `pybind11` | C++ bindings | Building |
| `pytest` | Testing | Running tests |
| `pytest-cov` | Coverage | Coverage reports |
| `azure-identity` | Azure auth | Runtime (in requirements.txt) |

---

## After Setup

Once setup is complete, you can:

1. **Build DDBC extensions** → Use `#build-ddbc`
2. **Run tests** → Use `#run-tests`

> 💡 You typically only need to run this setup prompt **once** per machine or after major changes.
