#!/usr/bin/env python3
"""Test script for mssql_core_tds Rust bindings."""

import sys

try:
    import mssql_core_tds
    print("✓ mssql_core_tds module loaded successfully!")
    print("\nAvailable attributes:")
    for attr in dir(mssql_core_tds):
        if not attr.startswith('_'):
            print(f"  - {attr}")
    print("\nModule location:", mssql_core_tds.__file__ if hasattr(mssql_core_tds, '__file__') else 'Built-in')
except ImportError as e:
    print(f"✗ Failed to import mssql_core_tds: {e}")
    sys.exit(1)
