"""Native mssql-py-core dependency boundary for asynchronous queries."""

from importlib import import_module
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from .._pycore_types import PyCoreModule


def load_py_core() -> "PyCoreModule":
    """Load the PyO3 extension that owns asynchronous TDS operations."""
    try:
        py_core = import_module("mssql_py_core")
    except ImportError as exc:
        raise ImportError(
            "Async query support requires the mssql-python-rs dependency "
            "that provides mssql_py_core."
        ) from exc

    required_types = ("PyAsyncConnection", "PyAsyncCursor")
    missing_types = [name for name in required_types if not hasattr(py_core, name)]
    if missing_types:
        missing = ", ".join(missing_types)
        raise ImportError(f"mssql_py_core does not provide the required async types: {missing}")

    return cast("PyCoreModule", py_core)
