from mssql_python.async_query._native import load_py_core


def test_load_py_core_uses_direct_native_dependency():
    py_core = load_py_core()
    assert py_core.__name__ == "mssql_py_core"
    assert py_core.PyAsyncConnection.__name__ == "PyAsyncConnection"
    assert py_core.PyAsyncCursor.__name__ == "PyAsyncCursor"
    assert getattr(py_core, "PyAsyncCursor").__name__ == "PyAsyncCursor"
