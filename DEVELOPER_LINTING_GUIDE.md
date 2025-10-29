# Developer Linting and Code Formatting Guide

This guide helps contributors set up and use linting tools to maintain code quality standards in the mssql-python project.

## Table of Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Installation Instructions](#installation-instructions)
  - [Windows](#windows)
  - [macOS](#macos)
  - [Linux](#linux)
- [Python Code Formatting](#python-code-formatting)
- [C++ Code Formatting](#c-code-formatting)
- [Pre-commit Hooks](#pre-commit-hooks)

## Overview

The mssql-python project uses multiple linting and formatting tools to ensure code quality:

- **Python**: `pylint` for linting (configured in `pyproject.toml`)
- **C++**: `clang-format` for formatting, `cpplint` for linting (configured in `cpplint.cfg`)
- **Pre-commit hooks**: Already configured in `.pre-commit-config.yml`

## Prerequisites

Before setting up linting tools, ensure you have:

- Python 3.8+ installed
- Git installed and configured
- A working development environment for the project

## Quick Start (TL;DR)

**For new contributors, this is all you need:**

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Set up pre-commit hooks:**
   ```bash
   pre-commit install
   ```

3. **Install clang-format** (see OS-specific instructions below)

4. **Verify setup:**
   ```bash
   pre-commit run --all-files
   ```

Now your code will be automatically checked before each commit!

## Installation Instructions

### Windows

#### Using PowerShell (Administrator recommended for some installations)

1. **Install Python linting tools:**
   ```powershell
   pip install -r requirements.txt
   ```

2. **Install clang-format:**
   
   **Option A: Using Chocolatey (Recommended)**
   ```powershell
   # Install Chocolatey first if not installed
   Set-ExecutionPolicy Bypass -Scope Process -Force; [System.Net.ServicePointManager]::SecurityProtocol = [System.Net.ServicePointManager]::SecurityProtocol -bor 3072; iex ((New-Object System.Net.WebClient).DownloadString('https://community.chocolatey.org/install.ps1'))
   
   # Install LLVM (includes clang-format)
   choco install llvm
   ```
   
   **Option B: Using winget**
   ```powershell
   winget install LLVM.LLVM
   ```
   

3. **Verify installations:**
   ```powershell
   python --version
   pylint --version
   clang-format --version
   cpplint --help
   ```

### macOS

1. **Install Python linting tools:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Install clang-format:**
   ```bash
   # Using Homebrew (recommended)
   brew install clang-format
   
   # Alternative: Install full LLVM suite
   brew install llvm
   ```

3. **Verify installations:**
   ```bash
   python --version
   pylint --version
   clang-format --version
   cpplint --help
   ```

### Linux

#### Ubuntu/Debian

1. **Install system dependencies:**
   ```bash
   sudo apt update
   sudo apt install clang-format python3-pip
   ```

2. **Install Python linting tools:**
   ```bash
   pip install -r requirements.txt
   ```

#### Red Hat/CentOS/Fedora

1. **Install system dependencies:**
   ```bash
   # For RHEL/CentOS 8+
   sudo dnf install clang-tools-extra python3-pip
   
   # For older versions
   sudo yum install clang python3-pip
   ```

2. **Install Python linting tools:**
   ```bash
   pip install -r requirements.txt
   ```

## Python Code Formatting

### Using Pylint

The project uses pylint for Python code quality checks with custom configuration in `pyproject.toml`.

**Check a single file:**
```bash
pylint mssql_python/connection.py
```

**Check all Python files:**
```bash
# Windows PowerShell
Get-ChildItem -Recurse -Path mssql_python -Include *.py | ForEach-Object { pylint $_.FullName }

# macOS/Linux
find mssql_python -name "*.py" -exec pylint {} \;
```

**Check specific directories:**
```bash
pylint mssql_python/ tests/
```

### Project Configuration

The project uses Pylint configuration defined in `pyproject.toml`:
- Line length: 100 characters
- Minimum score: 8.5
- Disabled checks: fixme, no-member, too-many-arguments, etc.

**Current project standards:** The project currently uses Pylint for both linting and enforcing formatting standards. While Black could be added for automatic formatting, the current setup relies on Pylint's formatting rules.

## C++ Code Formatting

### Using clang-format

The project has a `.clang-format` configuration file with Google style guidelines.

**Format a single file:**
```bash
clang-format -i mssql_python/pybind/ddbc_bindings.cpp
```

**Format all C++ files:**
```bash
# Windows PowerShell
Get-ChildItem -Recurse -Path mssql_python/pybind -Include *.cpp,*.h,*.hpp | ForEach-Object { clang-format -i $_.FullName }

# macOS/Linux
find mssql_python/pybind -name "*.cpp" -o -name "*.h" -o -name "*.hpp" | xargs clang-format -i
```

**Check formatting without modifying files:**
```bash
clang-format --dry-run --Werror mssql_python/pybind/ddbc_bindings.cpp
```

### Using cpplint

The project has a `cpplint.cfg` configuration file with:
- Line length: 100 characters
- Filtered out: readability/todo warnings
- Excludes: build directory

**Check C++ code style:**
```bash
# Single file (uses project config automatically)
cpplint mssql_python/pybind/ddbc_bindings.cpp

# All C++ files
# Windows PowerShell
Get-ChildItem -Recurse -Path mssql_python/pybind -Include *.cpp,*.h,*.hpp | ForEach-Object { cpplint $_.FullName }

# macOS/Linux
find mssql_python/pybind -name "*.cpp" -o -name "*.h" -o -name "*.hpp" | xargs cpplint
```

## Pre-commit Hooks

**Good news!** The project already has pre-commit hooks configured in `.pre-commit-config.yml`.

The current configuration includes:
- **Pylint**: Using `pre-commit/mirrors-pylint` (v3.0.0a5)
- **cpplint**: Local hook with project-specific arguments:
  - Filters out readability/todo warnings
  - Line length: 100 characters
  - Excludes build directory

### Setting up pre-commit hooks:

1. **Install pre-commit (already in requirements.txt):**
   ```bash
   pip install pre-commit
   ```

2. **Install the hooks:**
   ```bash
   pre-commit install
   ```

3. **Run hooks on all files (optional):**
   ```bash
   pre-commit run --all-files
   ```

4. **Run specific hooks:**
   ```bash
   # Run only pylint
   pre-commit run pylint
   
   # Run only cpplint
   pre-commit run cpplint
   ```

### How it works:
- Hooks run automatically on `git commit`
- Only files being committed are checked
- Commit is blocked if linting fails
- You can bypass with `git commit --no-verify` (not recommended)

## Best Practices

1. **Always run linting before committing:**
   - Set up pre-commit hooks
   - Run manual checks for critical changes

2. **Fix linting issues promptly:**
   - Don't accumulate technical debt
   - Address issues in the same PR that introduces them

3. **Understand the rules:**
   - Read pylint and cpplint documentation
   - Understand why certain patterns are discouraged

4. **Team consistency:**
   - Follow the project's existing style
   - Discuss style changes in team meetings

## Additional Resources

- [Pylint Documentation](https://pylint.pycqa.org/en/latest/)
- [Black Documentation](https://black.readthedocs.io/en/stable/)
- [Clang-Format Documentation](https://clang.llvm.org/docs/ClangFormat.html)
- [cpplint Style Guide](https://google.github.io/styleguide/cppguide.html)
- [Pre-commit Documentation](https://pre-commit.com/)
