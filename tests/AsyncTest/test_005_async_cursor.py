import asyncio

import pytest

from mssql_python import OperationalError
from mssql_python.async_query import AsyncCursor


class FakeNativeCursor:
    def __init__(self):
        self.timeout = 15
        self.description = (("value", int, None, 10, 10, 0, True),)
        self.rowcount = 3
        self.arraysize = 1
        self.calls = []

    async def execute(self, operation, *parameters, use_prepare=True, reset_cursor=True):
        self.calls.append(("execute", operation, parameters, use_prepare, reset_cursor))
        return self

    async def executemany(self, operation, rows, *, use_prepare=True):
        self.calls.append(("executemany", operation, rows, use_prepare))
        return self

    async def fetchone(self):
        self.calls.append(("fetchone",))
        return (1,)

    async def fetchmany(self, *args):
        self.calls.append(("fetchmany", args))
        return [(1,), (2,)]

    async def fetchall(self):
        self.calls.append(("fetchall",))
        return [(3,)]

    async def nextset(self):
        self.calls.append(("nextset",))
        return True

    async def close(self):
        self.calls.append(("close",))

    def setinputsizes(self, sizes):
        self.calls.append(("setinputsizes", sizes))


def test_execute_methods_return_public_cursor_and_forward_arguments():
    native_cursor = FakeNativeCursor()
    cursor = AsyncCursor(native_cursor)
    rows = [(1,), (2,)]

    execute_result = asyncio.run(
        cursor.execute("SELECT ?", 1, use_prepare=False, reset_cursor=False)
    )
    executemany_result = asyncio.run(
        cursor.executemany("INSERT VALUES (?)", rows, use_prepare=False)
    )

    assert execute_result is cursor
    assert executemany_result is cursor
    assert native_cursor.calls == [
        ("execute", "SELECT ?", (1,), False, False),
        ("executemany", "INSERT VALUES (?)", rows, False),
    ]


def test_fetch_and_result_navigation_preserve_native_values():
    native_cursor = FakeNativeCursor()
    cursor = AsyncCursor(native_cursor)

    assert asyncio.run(cursor.fetchone()) == (1,)
    assert asyncio.run(cursor.fetchmany()) == [(1,), (2,)]
    assert asyncio.run(cursor.fetchmany(2)) == [(1,), (2,)]
    assert asyncio.run(cursor.fetchall()) == [(3,)]
    assert asyncio.run(cursor.nextset()) is True

    assert native_cursor.calls == [
        ("fetchone",),
        ("fetchmany", ()),
        ("fetchmany", (2,)),
        ("fetchall",),
        ("nextset",),
    ]


def test_properties_and_setinputsizes_delegate_to_native_cursor():
    native_cursor = FakeNativeCursor()
    cursor = AsyncCursor(native_cursor)
    sizes = [(4, 10, 0)]

    assert cursor.timeout == 15
    assert cursor.description == native_cursor.description
    assert cursor.rowcount == 3
    assert cursor.arraysize == 1
    cursor.arraysize = 50
    cursor.setinputsizes(sizes)

    assert native_cursor.arraysize == 50
    assert native_cursor.calls == [("setinputsizes", sizes)]


def test_close_delegates_and_resolves_to_none():
    native_cursor = FakeNativeCursor()
    cursor = AsyncCursor(native_cursor)

    assert asyncio.run(cursor.close()) is None
    assert native_cursor.calls == [("close",)]


def test_cursor_operation_translates_native_exception():
    native_error_type = type(
        "OperationalError",
        (Exception,),
        {"__module__": "mssql_py_core"},
    )

    class FailingNativeCursor(FakeNativeCursor):
        async def fetchone(self):
            raise native_error_type("connection lost")

    with pytest.raises(OperationalError) as caught:
        asyncio.run(AsyncCursor(FailingNativeCursor()).fetchone())

    assert isinstance(caught.value.__cause__, native_error_type)
