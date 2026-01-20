"""
build_ddbc - Build system for mssql-python native extensions.

This package provides:
1. A CLI tool: `python -m build_ddbc`
2. A PEP 517 build backend that auto-compiles ddbc_bindings

Usage:
    python -m build_ddbc              # Compile ddbc_bindings only
    python -m build_ddbc --arch arm64 # Specify architecture (Windows)
    python -m build_ddbc --coverage   # Enable coverage (Linux)
    python -m build                   # Compile + create wheel (automatic)
"""

from .compiler import compile_ddbc, get_platform_info

__all__ = ["compile_ddbc", "get_platform_info"]
__version__ = "1.0.0"
