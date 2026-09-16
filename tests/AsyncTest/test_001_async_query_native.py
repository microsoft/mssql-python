import mssql_py_core


def test_load_py_core_uses_direct_native_dependency():
    assert mssql_py_core.__name__ == "mssql_py_core"
    assert getattr(mssql_py_core, "PyAsyncConnection").__name__ == "PyAsyncConnection"
    assert getattr(mssql_py_core, "PyAsyncCursor").__name__ == "PyAsyncCursor"
