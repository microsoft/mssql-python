# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Helpers for loading Rust components consumed by mssql-python."""

from importlib import import_module

RUST_CORE_MODULE = "mssql_python_rs"
LEGACY_RUST_CORE_MODULE = "mssql_py_core"
RUST_CORE_DISTRIBUTION = "mssql-python-rs"


def import_rust_core():
    """Import the Rust TDS core module used by bulk-copy operations."""
    try:
        return import_module(RUST_CORE_MODULE)
    except ImportError as primary_error:
        try:
            return import_module(LEGACY_RUST_CORE_MODULE)
        except ImportError:
            raise ImportError(
                f"Bulk copy requires the {RUST_CORE_DISTRIBUTION} library "
                f"({RUST_CORE_MODULE}) which is not available."
            ) from primary_error
