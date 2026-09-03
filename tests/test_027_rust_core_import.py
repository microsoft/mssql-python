# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Tests for Rust core module import resolution."""

from types import ModuleType
from unittest.mock import patch

import pytest

from mssql_python._rust import import_rust_core


def test_import_rust_core_prefers_renamed_module():
    renamed = ModuleType("mssql_python_rs")
    legacy = ModuleType("mssql_py_core")

    with patch.dict("sys.modules", {"mssql_python_rs": renamed, "mssql_py_core": legacy}):
        assert import_rust_core() is renamed


def test_import_rust_core_falls_back_to_legacy_module():
    legacy = ModuleType("mssql_py_core")

    with patch.dict("sys.modules", {"mssql_python_rs": None, "mssql_py_core": legacy}):
        assert import_rust_core() is legacy


def test_import_rust_core_error_names_new_distribution():
    with patch.dict("sys.modules", {"mssql_python_rs": None, "mssql_py_core": None}):
        with pytest.raises(ImportError, match="mssql-python-rs"):
            import_rust_core()
