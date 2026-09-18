"""Translate mssql-py-core failures to the public DB-API exception hierarchy."""

from contextlib import contextmanager
from typing import Iterator

from ..exceptions import (
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
from ..logging import logger

_EXCEPTION_TYPES = {
    "Warning": Warning,
    "Error": Error,
    "InterfaceError": InterfaceError,
    "DatabaseError": DatabaseError,
    "DataError": DataError,
    "OperationalError": OperationalError,
    "IntegrityError": IntegrityError,
    "InternalError": InternalError,
    "ProgrammingError": ProgrammingError,
    "NotSupportedError": NotSupportedError,
}

_ASYNC_DRIVER_ERROR = "Async operation failed"

_PROGRAMMING_RUNTIME_ERRORS = ("Cursor is closed",)
_OPERATIONAL_RUNTIME_ERROR_PREFIXES = (
    "Connection is closing",
    "Connection is closed",
    "Connection is broken",
    "Connection is busy",
)
_PROGRAMMING_TYPE_ERROR_PREFIXES = (
    "The SQL contains ",
    "Parameter style mismatch:",
    "Named parameter cannot be empty",
)
_DATA_ERROR_NUMBERS = {241, 245, 248, 8114, 8115, 8134, 8152, 2628}
_INTEGRITY_ERROR_NUMBERS = {515, 547, 2601, 2627}
_PROGRAMMING_ERROR_NUMBERS = {102, 156, 201, 207, 208, 2812, 8144}


def _translate_known_builtin_error(error: Exception) -> Exception:
    message = str(error)
    if isinstance(error, RuntimeError):
        if message in _PROGRAMMING_RUNTIME_ERRORS:
            return ProgrammingError(_ASYNC_DRIVER_ERROR, message)
        if message.startswith(_OPERATIONAL_RUNTIME_ERROR_PREFIXES):
            return OperationalError(_ASYNC_DRIVER_ERROR, message)
    if isinstance(error, TypeError) and message.startswith(_PROGRAMMING_TYPE_ERROR_PREFIXES):
        return ProgrammingError(_ASYNC_DRIVER_ERROR, message)
    return error


def _classify_database_error(error: Exception, default_type: type[DatabaseError]):
    diagnostics = getattr(error, "sql_errors", ())
    numbers = {item.get("number") for item in diagnostics if isinstance(item, dict)}
    if numbers & _DATA_ERROR_NUMBERS:
        return DataError
    if numbers & _INTEGRITY_ERROR_NUMBERS:
        return IntegrityError
    if numbers & _PROGRAMMING_ERROR_NUMBERS:
        return ProgrammingError
    return default_type


def translate_py_core_exception(error: Exception) -> Exception:
    """Translate py-core and recognized built-in errors to public exceptions."""
    for error_type in type(error).__mro__:
        if error_type.__module__ != "mssql_py_core":
            continue
        public_type = _EXCEPTION_TYPES.get(error_type.__name__)
        if public_type is None:
            continue
        if public_type is DatabaseError:
            public_type = _classify_database_error(error, public_type)

        logger.debug(
            "Async exception translation: %s -> %s",
            error_type.__name__,
            public_type.__name__,
        )
        translated = public_type(_ASYNC_DRIVER_ERROR, str(error))
        for attribute in ("sql_errors", "info_messages"):
            if hasattr(error, attribute):
                setattr(translated, attribute, getattr(error, attribute))
        return translated

    return _translate_known_builtin_error(error)


@contextmanager
def translate_py_core_exceptions() -> Iterator[None]:
    """Translate py-core and recognized built-in errors raised by the wrapped operation."""
    try:
        yield
    except Exception as error:
        translated = translate_py_core_exception(error)
        if translated is error:
            raise
        raise translated from error
