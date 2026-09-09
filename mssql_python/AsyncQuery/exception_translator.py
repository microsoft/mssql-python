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


def translate_py_core_exception(error: Exception) -> Exception:
    """Return the equivalent public exception, or the original non-py-core error."""
    for error_type in type(error).__mro__:
        if error_type.__module__ != "mssql_py_core":
            continue
        public_type = _EXCEPTION_TYPES.get(error_type.__name__)
        if public_type is None:
            continue

        logger.debug(
            "Async exception translation: %s -> %s",
            error_type.__name__,
            public_type.__name__,
        )
        translated = public_type(str(error), "")
        for attribute in ("sql_errors", "info_messages"):
            if hasattr(error, attribute):
                setattr(translated, attribute, getattr(error, attribute))
        return translated

    return error


@contextmanager
def translate_py_core_exceptions() -> Iterator[None]:
    """Translate only exceptions originating from mssql-py-core."""
    try:
        yield
    except Exception as error:
        translated = translate_py_core_exception(error)
        if translated is error:
            raise
        raise translated from error
