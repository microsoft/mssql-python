import sys
from importlib.machinery import ModuleSpec
from types import ModuleType

import pytest

import mssql_python.async_query as async_query
import mssql_python.async_query._native as native


def test_async_entry_points_are_internal():
    from mssql_python.async_query import _AsyncConnection  # pyright: ignore[reportPrivateUsage]
    from mssql_python.async_query import _AsyncCursor  # pyright: ignore[reportPrivateUsage]

    assert _AsyncConnection.__name__ == "_AsyncConnection"
    assert _AsyncCursor.__name__ == "_AsyncCursor"
    for name in ("AsyncConnection", "AsyncCursor"):
        assert not hasattr(async_query, name)
        assert name not in async_query.__all__
        assert f"_{name}" not in async_query.__all__


def test_load_py_core_uses_direct_native_dependency():
    expected = pytest.importorskip("mssql_py_core.mssql_py_core", exc_type=ImportError)

    py_core = native.load_py_core()
    assert py_core is expected
    assert py_core.PyAsyncConnection.__name__ == "PyAsyncConnection"
    assert py_core.PyAsyncCursor.__name__ == "PyAsyncCursor"
    assert getattr(py_core, "PyAsyncCursor").__name__ == "PyAsyncCursor"


def test_load_py_core_without_package_initializer(monkeypatch):
    package = ModuleType("mssql_py_core")
    package.__path__ = []
    package.__spec__ = ModuleSpec("mssql_py_core", loader=None, is_package=True)
    py_core = ModuleType("mssql_py_core.mssql_py_core")
    py_core.PyAsyncConnection = type("PyAsyncConnection", (), {})
    py_core.PyAsyncCursor = type("PyAsyncCursor", (), {})
    monkeypatch.setitem(sys.modules, "mssql_py_core", package)
    monkeypatch.setitem(sys.modules, "mssql_py_core.mssql_py_core", py_core)

    assert not hasattr(package, "PyAsyncConnection")
    assert native.load_py_core() is py_core


def test_load_py_core_reports_missing_dependency(monkeypatch):
    def raise_import_error(name):
        raise ImportError(f"No module named {name}")

    monkeypatch.setattr(native, "import_module", raise_import_error)

    with pytest.raises(ImportError, match="mssql-python-rs dependency") as exc_info:
        native.load_py_core()

    assert isinstance(exc_info.value.__cause__, ImportError)


def test_load_py_core_reports_missing_async_types(monkeypatch):
    py_core = ModuleType("mssql_py_core.mssql_py_core")
    monkeypatch.setattr(native, "import_module", lambda name: py_core)

    with pytest.raises(ImportError, match="PyAsyncConnection, PyAsyncCursor"):
        native.load_py_core()
