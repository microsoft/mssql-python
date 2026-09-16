from typing import Any, cast
from uuid import uuid4

import pytest

pytest.importorskip("mssql_py_core", exc_type=ImportError)

from mssql_python import DatabaseError
from mssql_python.async_query import AsyncCursor


@pytest.mark.asyncio
@pytest.mark.parametrize("use_prepare", (True, False))
async def test_execute_returns_public_cursor_and_binds_parameters(
    async_connection,
    use_prepare,
):
    cursor = async_connection.cursor()
    try:
        result = await cursor.execute(
            "SELECT CAST(? AS INT) AS value",
            7,
            use_prepare=use_prepare,
            reset_cursor=False,
        )

        assert result is cursor
        assert await cursor.fetchone() == (7,)
    finally:
        await cursor.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("parameters", ((1, 2), [1, 2]))
@pytest.mark.parametrize("use_prepare", (True, False))
async def test_execute_accepts_single_parameter_sequence(
    async_cursor,
    parameters,
    use_prepare,
):
    await async_cursor.execute(
        "SELECT CAST(? AS INT), CAST(? AS INT)",
        parameters,
        use_prepare=use_prepare,
    )

    assert await async_cursor.fetchone() == (1, 2)


@pytest.mark.asyncio
async def test_executemany_returns_public_cursor_and_inserts_rows(async_connection):
    cursor = async_connection.cursor()
    rows = [(1, "one"), (2, "two")]
    table_name = f"async_cursor_test_{uuid4().hex}"
    try:
        await cursor.execute(
            f"CREATE TABLE {table_name} (id INT NOT NULL, value NVARCHAR(20) NOT NULL)"
        )
        result = await cursor.executemany(
            f"INSERT INTO {table_name} (id, value) VALUES (?, ?)",
            rows,
            use_prepare=False,
        )
        assert result is cursor

        await cursor.execute(f"SELECT id, value FROM {table_name} ORDER BY id")
        assert await cursor.fetchall() == rows
    finally:
        await cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        await cursor.close()


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
async def test_properties_and_setinputsizes_use_native_cursor(async_connection):
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
