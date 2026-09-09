from types import ModuleType

import pytest

from mssql_python.AsyncQuery import _native


def test_load_py_core_uses_direct_native_dependency(monkeypatch):
    py_core = ModuleType("mssql_py_core")
    py_core.PyAsyncConnection = object
    py_core.PyAsyncCursor = object
    imported_modules = []

    def import_module(name):
        imported_modules.append(name)
        return py_core

    monkeypatch.setattr(_native, "import_module", import_module)

    assert _native.load_py_core() is py_core
    assert imported_modules == ["mssql_py_core"]


def test_load_py_core_rejects_extension_without_async_types(monkeypatch):
    monkeypatch.setattr(_native, "import_module", lambda _name: ModuleType("mssql_py_core"))

    with pytest.raises(ImportError, match="PyAsyncConnection, PyAsyncCursor"):
        _native.load_py_core()
