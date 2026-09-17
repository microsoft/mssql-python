from typing import Any, cast

import pytest

pytest.importorskip("mssql_py_core", exc_type=ImportError)

from mssql_python import DatabaseError


@pytest.mark.asyncio
async def test_fetch_and_result_navigation_preserve_native_values(async_connection):
    cursor = async_connection.cursor()
    try:
        await cursor.execute(
            "SELECT CAST(1 AS INT) AS value UNION ALL SELECT 2 ORDER BY value; "
            "SELECT CAST(3 AS INT) AS value"
        )

        assert await cursor.fetchone() == (1,)
        assert await cursor.fetchmany(1) == [(2,)]
        assert await cursor.fetchall() == []
        assert await cursor.nextset() is True
        assert await cursor.fetchone() == (3,)
        assert await cursor.nextset() is False
    finally:
        await cursor.close()


@pytest.mark.asyncio
async def test_fetchmany_uses_arraysize(async_connection):
    cursor = async_connection.cursor()
    try:
        cursor.arraysize = 2
        await cursor.execute(
            "SELECT CAST(1 AS INT) AS value UNION ALL SELECT 2 UNION ALL SELECT 3 ORDER BY value"
        )

        assert await cursor.fetchmany() == [(1,), (2,)]
        assert await cursor.fetchall() == [(3,)]
    finally:
        await cursor.close()


@pytest.mark.asyncio
async def test_properties_and_setinputsizes_use_py_core_async_cursor(async_connection):
    cursor = async_connection.cursor()
    try:
        assert cursor.timeout == async_connection.timeout
        assert cursor.description is None
        assert cursor.rowcount == -1
        assert cursor.arraysize == 1

        cursor.arraysize = 50
        cursor.setinputsizes([(4, 10, 0)])
        await cursor.execute("SELECT CAST(? AS INT) AS value", 9)

        assert cursor.arraysize == 50
        description = cast(Any, cursor.description)
        assert description[0][0] == "value"
        assert await cursor.fetchone() == (9,)
    finally:
        await cursor.close()


@pytest.mark.asyncio
async def test_close_is_idempotent(async_connection):
    cursor = async_connection.cursor()

    assert await cursor.close() is None
    assert await cursor.close() is None


@pytest.mark.asyncio
async def test_cursor_operation_translates_native_exception(async_connection):
    cursor = async_connection.cursor()
    try:
        await cursor.execute("SELECT 1 / 0")

        with pytest.raises(DatabaseError) as caught:
            await cursor.fetchone()

        assert type(caught.value.__cause__).__module__ == "mssql_py_core"
        assert type(caught.value.__cause__).__name__ == "DatabaseError"
        sql_errors = getattr(caught.value, "sql_errors")
        assert sql_errors
        assert sql_errors[0]["number"] == 8134
    finally:
        await cursor.close()
