#!/usr/bin/env python3
"""Pre-flight environment check for building mssql-python with GIL release support."""

import os
import platform
import shutil
import subprocess
import sys
import sysconfig


def check_compiler():
    system = platform.system()
    if system == "Darwin":
        compiler = shutil.which("clang++") or shutil.which("clang")
        if compiler:
            print(f"  [OK] Compiler: {compiler}")
            return True
        print("  [FAIL] clang not found. Install Xcode command line tools:")
        print("         xcode-select --install")
        return False
    elif system == "Linux":
        compiler = shutil.which("g++") or shutil.which("gcc")
        if compiler:
            print(f"  [OK] Compiler: {compiler}")
            return True
        print("  [FAIL] gcc/g++ not found. Install:")
        print("         sudo apt-get install build-essential")
        return False
    elif system == "Windows":
        # Check for cl.exe via vswhere or PATH
        compiler = shutil.which("cl") or shutil.which("cl.exe")
        if compiler:
            print(f"  [OK] Compiler: {compiler}")
            return True
        print("  [FAIL] cl.exe not found. Install Visual Studio Build Tools.")
        return False
    else:
        print(f"  [WARN] Unknown platform: {system}")
        return False


def check_python_headers():
    include_dir = sysconfig.get_path("include")
    python_h = os.path.join(include_dir, "Python.h")
    if os.path.isfile(python_h):
        print(f"  [OK] Python.h found: {python_h}")
        return True
    print(f"  [FAIL] Python.h not found at {include_dir}")
    system = platform.system()
    if system == "Linux":
        ver = f"{sys.version_info.major}.{sys.version_info.minor}"
        print(f"         sudo apt-get install python{ver}-dev")
    elif system == "Darwin":
        print("         Reinstall Python or use: brew install python")
    return False


def check_odbc_headers():
    system = platform.system()
    search_dirs = []
    if system == "Darwin":
        search_dirs = ["/opt/homebrew/include", "/usr/local/include", "/usr/include"]
    elif system == "Linux":
        search_dirs = ["/usr/include", "/usr/local/include"]
    elif system == "Windows":
        # Windows SDK includes ODBC headers
        print("  [OK] ODBC headers (provided by Windows SDK)")
        return True

    for d in search_dirs:
        sql_h = os.path.join(d, "sql.h")
        sqlext_h = os.path.join(d, "sqlext.h")
        if os.path.isfile(sql_h) and os.path.isfile(sqlext_h):
            print(f"  [OK] ODBC headers found: {d}")
            return True

    print("  [FAIL] sql.h / sqlext.h not found")
    if system == "Darwin":
        print("         brew install unixodbc")
    elif system == "Linux":
        print("         sudo apt-get install unixodbc-dev")
    return False


def check_pybind11():
    try:
        import pybind11
        print(f"  [OK] pybind11: {pybind11.__version__} ({pybind11.get_include()})")
        return True
    except ImportError:
        print("  [FAIL] pybind11 not installed")
        print("         pip install pybind11")
        return False


def main():
    print(f"Platform: {platform.system()} {platform.machine()}")
    print(f"Python:   {sys.version}")
    print()

    checks = [
        ("Compiler", check_compiler),
        ("Python headers", check_python_headers),
        ("ODBC headers", check_odbc_headers),
        ("pybind11", check_pybind11),
    ]

    all_ok = True
    for name, check_fn in checks:
        print(f"Checking {name}...")
        if not check_fn():
            all_ok = False
        print()

    if all_ok:
        print("All pre-flight checks passed.")
        return 0
    else:
        print("Some checks failed. See above for install commands.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
