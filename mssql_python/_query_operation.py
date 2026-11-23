# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.
"""Admission and recovery for synchronous cursor operations."""

from __future__ import annotations

from functools import wraps
from threading import RLock
from typing import TYPE_CHECKING, Callable, Concatenate, ParamSpec, TypeVar, cast

from . import ddbc_bindings
from .exceptions import NotSupportedError, OperationalError, ProgrammingError
from .logging import logger

if TYPE_CHECKING:
    from .connection import Connection
    from .cursor import Cursor

P = ParamSpec("P")
R = TypeVar("R")
T = TypeVar("T", bound="Cursor | Connection")
_NATIVE_QUERY_RUNTIME: tuple[object, ...] | bool | None = None


class QueryConnectionState:
    __slots__ = ("lock", "_native")

    def __init__(self) -> None:
        # Allocation and exception creation can run same-thread finalizers.
        self.lock = RLock()
        self._native = ddbc_bindings._QueryAdmission()

    @property
    def busy(self) -> bool:
        return self._native.busy

    @busy.setter
    def busy(self, value: bool) -> None:
        self._native.busy = value

    def reserve(self) -> bool:
        return self._native.reserve()

    def __enter__(self) -> None:
        with self.lock:
            if not self.reserve():
                raise ProgrammingError("Connection has an operation in progress", "")

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        with self.lock:
            self.busy = False


class QuerySlot:
    __slots__ = ("_active", "_native")

    def __init__(self) -> None:
        self._active: QueryOperation | None = None
        self._native = ddbc_bindings._QueryOperationSlot()

    @property
    def active(self) -> QueryOperation | ddbc_bindings._StatementOperation | None:
        return self._active if self._active is not None else self._native.active

    @active.setter
    def active(self, value: QueryOperation | None) -> None:
        self._active = value
        self._native.active = None


class QueryOperation:
    __slots__ = ("cursor", "connection", "cancellable", "native", "_cancel_requested")

    def __init__(self, cursor: Cursor, cancellable: bool = True) -> None:
        cursor._check_closed()
        self.cursor = cursor
        self.connection = cursor.connection
        self.cancellable = cancellable
        self.native: ddbc_bindings._StatementOperation | None = None
        self._cancel_requested = False

    def __enter__(self) -> QueryOperation:
        connection = self.connection
        with connection._query_state.lock:
            self.cursor._check_closed()
            if connection.closed or not connection._query_state.reserve():
                raise ProgrammingError("Connection is closed or has an operation in progress", "")
            self.cursor._query_slot.active = self
        try:
            if self.cancellable:
                self.native = ddbc_bindings._StatementOperation(self.cursor.hstmt)
                if self._cancel_requested:
                    self.native.cancel()
        except BaseException:
            try:
                if self.native is not None:
                    self.native.finish()
            finally:
                with connection._query_state.lock:
                    self.cursor._query_slot.active = None
                    connection._query_state.busy = False
            raise
        return self

    def cancel(self) -> None:
        if not self.cancellable:
            raise NotSupportedError("Cancellation is not supported for this operation", "")
        self._cancel_requested = True
        if self.native is not None:
            self.native.cancel()

    def _end(self, failed: bool, *, interrupted: bool = False) -> None:
        try:
            try:
                requested = self.native.finish() if self.native is not None else False
                if interrupted:
                    raise OperationalError("Cancellation completion was interrupted", "")
                if requested:
                    self.cursor._reset_cursor(checked=True)
                    self.cursor.description = None
                    self.cursor.rowcount = -1
                    self.cursor.last_executed_stmt = ""
                    if self.native is not None and self.native.failed:
                        self._discard()
            except BaseException as error:
                # Signals can interrupt GIL reacquisition during finish/free.
                # Never admit new work after incomplete cancellation recovery.
                try:
                    self._discard()
                except BaseException as discard_error:
                    logger.error("Discarding cancelled connection failed: %s", discard_error)
                if not failed or not isinstance(error, Exception):
                    raise
                logger.error("Cancellation recovery failed; connection discarded: %s", error)
                return
            if requested and not failed:
                raise OperationalError("Operation canceled", "Cancellation requested")
        finally:
            with self.connection._query_state.lock:
                self.cursor._query_slot.active = None
                self.connection._query_state.busy = False

    def _discard(self) -> None:
        _discard_connection(self.connection)

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self._end(exc_type is not None)


def _guard(
    method: Callable[Concatenate[T, P], R], cancellable: bool, connection: bool = False
) -> Callable[Concatenate[T, P], R]:
    def fallback(self: T, *args: P.args, **kwargs: P.kwargs) -> R:
        operation = (
            cast("Connection", self)._query_state
            if connection
            else cast("Cursor", self)._query_operation(cancellable=cancellable)
        )
        with operation:
            return method(self, *args, **kwargs)

    @wraps(method)
    def guarded(self: T, *args: P.args, **kwargs: P.kwargs) -> R:
        runtime = _NATIVE_QUERY_RUNTIME
        if isinstance(runtime, tuple):
            return cast(
                R,
                ddbc_bindings._run_query_operation(
                    self, method, args, kwargs, runtime, fallback, cancellable, connection
                ),
            )
        return fallback(self, *args, **kwargs)

    return cast("Callable[Concatenate[T, P], R]", guarded)


def cursor_operation(
    method: Callable[Concatenate[Cursor, P], R],
) -> Callable[Concatenate[Cursor, P], R]:
    return _guard(method, True)


def uncancellable_cursor_operation(
    method: Callable[Concatenate[Cursor, P], R],
) -> Callable[Concatenate[Cursor, P], R]:
    return _guard(method, False)


def connection_operation(
    method: Callable[Concatenate[Connection, P], R],
) -> Callable[Concatenate[Connection, P], R]:
    return _guard(method, False, True)


def _initialize_native_runtime() -> tuple[object, ...] | bool:
    global _NATIVE_QUERY_RUNTIME
    # Reentrant finalizers during cold initialization keep the Python protocol.
    _NATIVE_QUERY_RUNTIME = False
    if not getattr(ddbc_bindings, "_query_policy_available", False):
        return False
    try:
        from .connection import Connection
        from .cursor import Cursor, _NATIVE_FETCH_RUNTIME
        from ._native_fetch import _Policy

        if _NATIVE_FETCH_RUNTIME is None:
            return False
        fetch = _NATIVE_FETCH_RUNTIME[2]
        if not isinstance(fetch, _Policy):
            raise TypeError("Invalid native fetch policy for query admission")
        fetch.classes += (Connection,)
        first = len(fetch.functions)
        fetch._record_types((Connection,), [])
        fetch.dicts += tuple(
            keywords for _, _, _, keywords, _ in fetch.functions[first:] if keywords is not None
        )
        ddbc_bindings._watch_query_policy(fetch.observer, fetch.dicts)
        _NATIVE_QUERY_RUNTIME = (
            ddbc_bindings._QueryPolicyCache(),
            fetch.query_eligible,
            Cursor,
            Connection,
            QueryConnectionState,
            QuerySlot,
            _recover_native_query,
            (
                "_connection",
                "_query_state",
                "closed",
                "_closed",
                "_query_slot",
                "hstmt",
                "_query_operation",
                "_check_closed",
                "connection",
            ),
        )
    except BaseException:
        _NATIVE_QUERY_RUNTIME = None
        raise
    return _NATIVE_QUERY_RUNTIME


def _recover_native_query(
    cursor: Cursor, native: ddbc_bindings._StatementOperation, failed: bool, interrupted: bool
) -> None:
    try:
        operation = object.__new__(QueryOperation)
        operation.cursor = cursor
        operation.connection = cursor.connection
        operation.cancellable = True
        operation._cancel_requested = True
        operation.native = native
    except BaseException:
        try:
            _discard_connection(cursor.connection)
        finally:
            cursor._query_slot.active = None
            cursor.connection._query_state.busy = False
        raise
    operation._end(failed, interrupted=interrupted)


def _discard_connection(connection: Connection) -> None:
    try:
        if connection._conn is not None:
            connection._conn.discard()
    finally:
        connection._conn = None
        connection._closed = True
        for cursor in list(connection._cursors):
            cursor.closed = True
