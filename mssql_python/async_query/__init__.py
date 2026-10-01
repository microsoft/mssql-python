"""Asynchronous query support backed directly by mssql-py-core.

The internal entry points are ``_AsyncConnection`` and ``_AsyncCursor``.
Their methods retain DB-API names, but the classes are not stable public API.
``_TableValuedParameter`` lazily exposes the native TVP constructor. The
``SQL_*`` tokens are native setinputsizes() hints; server support is required
for the corresponding SQL types, particularly JSON and VECTOR.

Warning:
    Async query execution APIs are under active development and are not intended
    for production use. Their signatures, behavior, error handling, and compatibility
    may change without notice.
"""

from ._native import load_py_core
from .async_connection import _AsyncConnection  # pyright: ignore[reportPrivateUsage]
from .async_cursor import _AsyncCursor  # pyright: ignore[reportPrivateUsage]
from .exception_translator import (
    DataError,
    DatabaseError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    Warning,
)

SQL_MONEY: int
SQL_SMALLMONEY: int
SQL_XML: int
SQL_JSON: int
SQL_VECTOR: int

_NATIVE_EXPORTS = {
    "_TableValuedParameter": "TableValuedParameter",
    "SQL_MONEY": "SQL_MONEY",
    "SQL_SMALLMONEY": "SQL_SMALLMONEY",
    "SQL_XML": "SQL_XML",
    "SQL_JSON": "SQL_JSON",
    "SQL_VECTOR": "SQL_VECTOR",
}


def __getattr__(name: str):
    native_name = _NATIVE_EXPORTS.get(name)
    if native_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    py_core = load_py_core()
    try:
        return getattr(py_core, native_name)
    except AttributeError as error:
        raise ImportError(
            f"The installed mssql-python-rs dependency does not provide {native_name}; "
            "install a version with this async feature."
        ) from error


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(_NATIVE_EXPORTS))


__all__ = [
    "DataError",
    "DatabaseError",
    "Error",
    "IntegrityError",
    "InterfaceError",
    "InternalError",
    "NotSupportedError",
    "OperationalError",
    "ProgrammingError",
    "SQL_MONEY",
    "SQL_SMALLMONEY",
    "SQL_XML",
    "SQL_JSON",
    "SQL_VECTOR",
    "Warning",
    "load_py_core",
]
