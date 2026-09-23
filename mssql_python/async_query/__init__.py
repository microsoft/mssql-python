"""Asynchronous query support backed directly by mssql-py-core.

The internal entry points are ``_AsyncConnection`` and ``_AsyncCursor``.
Their methods retain DB-API names, but the classes are not stable public API.

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
    "Warning",
    "load_py_core",
]
