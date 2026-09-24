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


@pytest.mark.parametrize(
    "name", ("SQL_MONEY", "SQL_SMALLMONEY", "SQL_XML", "SQL_JSON", "SQL_VECTOR")
)
def test_async_type_hints_match_native_exports(name):
py_core = pytest.importorskip("mssql_py_core", exc_type=ImportError)
    assert getattr(async_query, name) == getattr(py_core, name)
    assert isinstance(getattr(async_query, name), int)
    assert name in async_query.__all__
    assert name in dir(async_query)


def test_async_tvp_is_internal_native_constructor():
    from mssql_python.async_query import _TableValuedParameter

    assert _TableValuedParameter is native.load_py_core().TableValuedParameter
    assert "_TableValuedParameter" not in async_query.__all__
    assert "_TableValuedParameter" in dir(async_query)
    assert not hasattr(async_query, "TableValuedParameter")
    value = _TableValuedParameter("TestType", [(4, 0, 0)], [(1,)], schema="dbo")
    assert (value.type_name, value.schema) == ("TestType", "dbo")
    assert (value.column_count, value.row_count, value.is_null) == (1, 1, False)
    assert _TableValuedParameter("dbo.TestType").is_null is True
    with pytest.raises(ValueError, match="requires column definitions"):
        _TableValuedParameter("dbo.TestType", rows=[(1,)])


def test_async_native_exports_load_only_when_requested(monkeypatch):
    calls = []
    py_core = ModuleType("mssql_py_core")
    setattr(py_core, "SQL_JSON", 244)

    def load():
        calls.append(True)
        return py_core

    monkeypatch.setattr(async_query, "load_py_core", load)
    assert "SQL_JSON" in dir(async_query)
    assert not hasattr(async_query, "unknown_export")
    assert calls == []
    assert async_query.SQL_JSON == 244
    assert calls == [True]


@pytest.mark.parametrize("name", ("_TableValuedParameter", "SQL_VECTOR"))
def test_async_native_exports_report_missing_feature(monkeypatch, name):
    monkeypatch.setattr(async_query, "load_py_core", lambda: ModuleType("mssql_py_core"))
    with pytest.raises(ImportError, match="installed mssql-python-rs dependency"):
        getattr(async_query, name)
