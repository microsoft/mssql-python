from types import ModuleType

import pytest

import mssql_python.async_query._native as native


def test_load_py_core_uses_direct_native_dependency():
    pytest.importorskip("mssql_py_core", exc_type=ImportError)

    py_core = native.load_py_core()
    assert py_core.__name__ == "mssql_py_core"
    assert py_core.PyAsyncConnection.__name__ == "PyAsyncConnection"
    assert py_core.PyAsyncCursor.__name__ == "PyAsyncCursor"
    assert getattr(py_core, "PyAsyncCursor").__name__ == "PyAsyncCursor"


def test_load_py_core_reports_missing_dependency(monkeypatch):
    def raise_import_error(name):
        raise ImportError(f"No module named {name}")

    monkeypatch.setattr(native, "import_module", raise_import_error)

    with pytest.raises(ImportError, match="mssql-python-rs dependency") as exc_info:
        native.load_py_core()

    assert isinstance(exc_info.value.__cause__, ImportError)


def test_load_py_core_reports_missing_async_types(monkeypatch):
    py_core = ModuleType("mssql_py_core")
    monkeypatch.setattr(native, "import_module", lambda name: py_core)

    with pytest.raises(ImportError, match="PyAsyncConnection, PyAsyncCursor"):
        native.load_py_core()
