# Build Guide for Contributors

Welcome to the development build guide for the **mssql-python** project!  
This guide will help you set up your environment, build the native bindings, and package the project as a Python wheel.

---

## Table of Contents

- [Getting Started](#getting-started)
- [Prerequisites](#prerequisites)
- [Platform-Specific Setup](#platform-specific-setup)
  - [Windows](#windows)
  - [macOS](#macos)
  - [Linux](#linux)
- [Building Native Bindings](#building-native-bindings)
- [Building the Python Wheel (.whl)](#building-the-python-wheel-whl)
- [Running Tests](#running-tests)
- [Setting Up a Test Database (Optional)](#setting-up-a-test-database-optional)
- [Directory Structure](#directory-structure)
- [Troubleshooting](#troubleshooting)

---

## Getting Started

To contribute to this project, you'll need to fork and clone the repository. You can do this using either the command line or Visual Studio Code.

### Option 1: Command Line

1. **Fork the repository** on GitHub by clicking the "Fork" button on the [mssql-python repository page](https://github.com/microsoft/mssql-python).
2. **Clone your fork** to your local machine:
   ```bash
   git clone https://github.com/YOUR-USERNAME/mssql-python.git
   cd mssql-python
   ```
3. **Set up the upstream remote** to keep your fork in sync:
   ```bash
   git remote add upstream https://github.com/microsoft/mssql-python.git
   ```

### Option 2: Visual Studio Code

1. **Install the GitHub extension** in VS Code:
   - Open VS Code
   - Go to the Extensions view (Ctrl+Shift+X)
   - Search for "GitHub Pull Requests and Issues" and install it
2. **Fork and clone the repository**:
   - Navigate to the [mssql-python repository page](https://github.com/microsoft/mssql-python)
   - Click "Create new fork" to create a fork in your GitHub account
   - Open VS Code
   - Open the Command Palette (Ctrl+Shift+P)
   - Type "Git: Clone" and select it
   - Select "Clone from GitHub"
   - Search for and select your forked repository
   - Choose a local directory to clone to
3. **The upstream remote will be set up automatically** when you fork through GitHub.

---

## Prerequisites

- **Python:** Minimum supported version is 3.10.  
  Ensure `python` and `pip` commands refer to your Python 3.10+ installation.
- **pybind11:** Used for C++/Python bindings.
- **CMake:** For Unix and macOS builds.
- **Microsoft ODBC Driver:** For packaging driver dependencies and header files such as `sql.h`, `sqlext.h` etc.
- **setuptools, wheel, pytest:** For building and testing (`pip install setuptools wheel pytest`).

---

## Platform-Specific Setup

### Windows

1. **Install Python** (3.10+ from [python.org](https://www.python.org/downloads/)).
2. **Install Visual Studio Build Tools**
   - Include the “Desktop development with C++” workload.
   - CMake is included by default.
   - **Alternative for VS Code users:** If you already have VS Code installed, you can configure it for C++ development by following [this guide](https://code.visualstudio.com/docs/cpp/config-msvc).
3. **Install Microsoft ODBC Driver for SQL Server:**  
   [Download here](https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server).
4. **Install required Python packages:**
   ```bash
   # Will install pybind11, setuptools etc.
   pip install -r requirements.txt
   ```

### macOS

1. **Install Python** (3.10+ from [python.org](https://www.python.org/downloads/)).
2. **Install CMake:**
   ```bash
   brew install cmake
   ```
3. **Install Microsoft ODBC Driver for SQL Server:**
   - Follow [official instructions](https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/install-microsoft-odbc-driver-sql-server-macos).
4. **Install Python requirements:**
   ```bash
   # Will install pybind11, setuptools etc.
   pip install -r requirements.txt
   ```

### Linux

1. **Install Python and development tools:**
   ```bash
   sudo apt-get update
   sudo apt-get install python3 python3-dev python3-pip build-essential cmake
   ```
   Ensure `python` and `pip` refer to Python 3.10+.
2. **Install Microsoft ODBC Driver for SQL Server:**  
   - Follow [official instructions](https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server).
3. **Install Python packages:**
   ```bash
   # Will install pybind11, setuptools etc.
   pip install -r requirements.txt
   ```

---

## Building Native Bindings

The native bindings are in the `mssql_python/pybind` directory.

### Windows

Open a **Developer Command Prompt for VS** and run:

```bash
cd mssql_python/pybind
build.bat
```

This will:
- Clean previous builds
- Configure with CMake
- Build the extension
- Copy the generated `.pyd` file to the correct location

### macOS & Linux

```bash
cd mssql_python/pybind
./build.sh
```

This will:
- Clean previous builds
- Configure with CMake
- Build the extension
- Copy the generated `.so` file to the correct location

---

## Running Tests

Tests require a database connection string and must be run from the project root directory.
Set the `DB_CONNECTION_STRING` environment variable before running tests:

### Windows (Command Prompt)
```cmd
# If you're in mssql_python/pybind/, navigate back to the project root:
cd ../..

set DB_CONNECTION_STRING=your-connection-string-here
python -m pytest -v
```

### macOS & Linux (bash/zsh)
```bash
# If you're in mssql_python/pybind/, navigate back to the project root:
cd ../..

export DB_CONNECTION_STRING=your-connection-string-here
python -m pytest -v
```

---

## Building the Python Wheel (.whl)

The wheel includes the native bindings.  
**You must build the native bindings first** (see above).

### Windows

From the project root:

```bash
python setup.py bdist_wheel
```

The wheel file will be created in the `dist/` directory.

### macOS & Linux

From the project root:

```bash
# Build the bindings first!
cd mssql_python/pybind
./build.sh
cd ../..

# Then build the wheel:
python setup.py bdist_wheel
```

The wheel file will be created in the `dist/` directory.

---

## Directory Structure

- `mssql_python/pybind/` — Native C++/pybind11 bindings and platform build scripts
- `mssql_python/` — Python package source
- `tests/` — Test suite
- `dist/` — Built wheel packages

---

## Setting Up a Test Database (Optional)

If you don't have access to a SQL Server instance, you can quickly set up a containerized SQL Server using go-sqlcmd:

### Windows
```bash
# Install Docker Desktop and sqlcmd
winget install Docker.DockerDesktop
```
Configure Docker, accept EULA, etc., then open a new terminal window:
```bash
winget install sqlcmd
```
Open a new window to get new path variables:
```bash
sqlcmd create mssql --name mssql-python --accept-eula tag 2025-latest --using https://github.com/Microsoft/sql-server-samples/releases/download/wide-world-importers-v1.0/WideWorldImporters-Full.bak
sqlcmd config connection-strings
```
Copy the ODBC connection string and remove the driver clause before storing it in your `DB_CONNECTION_STRING` environment variable.

### macOS
```bash
# Install Docker Desktop and sqlcmd
brew install --cask docker
brew install sqlcmd
```
Start Docker Desktop, then:
```bash
sqlcmd create mssql --name mssql-python --accept-eula tag 2025-latest --using https://github.com/Microsoft/sql-server-samples/releases/download/wide-world-importers-v1.0/WideWorldImporters-Full.bak
sqlcmd config connection-strings
```
Copy the ODBC connection string and remove the driver clause before storing it in your `DB_CONNECTION_STRING` environment variable.

### Linux
```bash
# Install Docker and sqlcmd (Ubuntu/Debian)
sudo apt-get update
sudo apt-get install docker.io
sudo systemctl start docker
sudo systemctl enable docker

# Install sqlcmd
curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | sudo gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg
echo "deb [arch=amd64,arm64,armhf signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/repos/microsoft-ubuntu-$(lsb_release -rs)-prod $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/mssql-release.list
sudo apt-get update
sudo apt-get install sqlcmd
```
Then create the SQL Server container:
```bash
sudo sqlcmd create mssql --name mssql-python --accept-eula tag 2025-latest --using https://github.com/Microsoft/sql-server-samples/releases/download/wide-world-importers-v1.0/WideWorldImporters-Full.bak
sqlcmd config connection-strings
```
Copy the ODBC connection string and remove the driver clause before storing it in your `DB_CONNECTION_STRING` environment variable.

---

## Troubleshooting

- Ensure all prerequisites are installed and on your PATH.
- If a build fails, clean up old artifacts and try again (`mssql_python/pybind/build.bat clean` or `./build.sh clean`).
- For wheel issues, ensure the native binding (`.pyd` or `.so`) is present in the expected location before building the wheel.
- For test failures, double-check your `DB_CONNECTION_STRING`.

---

For more details on the native bindings, see [`mssql_python/pybind/README.md`](mssql_python/pybind/README.md).

---

Happy coding!
