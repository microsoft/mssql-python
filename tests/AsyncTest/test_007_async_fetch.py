import pytest

pytest.importorskip("mssql_py_core", exc_type=ImportError)

import mssql_python
from mssql_python import DataError, Row


@pytest.mark.asyncio
async def test_fetch_and_result_navigation_preserve_native_values(async_connection):
    cursor = async_connection.cursor()
    try:
        await cursor.execute(
            "SELECT CAST(1 AS INT) AS value UNION ALL SELECT 2 ORDER BY value; "
            "SELECT CAST(3 AS INT) AS value"
        )

        first = await cursor.fetchone()
        assert isinstance(first, Row)
        assert tuple(first) == (1,)
        assert first.value == 1
        assert cursor.rowcount == 1

        many = await cursor.fetchmany(1)
        assert all(isinstance(row, Row) for row in many)
        assert [tuple(row) for row in many] == [(2,)]
        assert cursor.rowcount == 2
        assert await cursor.fetchall() == []
        assert await cursor.fetchone() is None
        assert cursor.rowcount == 2
        assert await cursor.nextset() is True
        next_result = await cursor.fetchone()
        assert isinstance(next_result, Row)
        assert tuple(next_result) == (3,)
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

        assert await cursor.fetchmany(0) == []
        assert await cursor.fetchmany(-1) == []
        assert [tuple(row) for row in await cursor.fetchmany()] == [(1,), (2,)]
        assert [tuple(row) for row in await cursor.fetchall()] == [(3,)]
    finally:
        await cursor.close()


@pytest.mark.asyncio
async def test_fetch_respects_row_settings(async_connection):
    cursor = async_connection.cursor()
    previous_lowercase = mssql_python.lowercase
    previous_native_uuid = mssql_python.native_uuid
    try:
        mssql_python.lowercase = True
        mssql_python.native_uuid = False
        await cursor.execute(
            "SELECT CAST('6F9619FF-8B86-D011-B42D-00C04FC964FF' "
            "AS UNIQUEIDENTIFIER) AS MixedGuid"
        )

        row = await cursor.fetchone()

        assert isinstance(row, Row)
        assert cursor.description[0][0] == "mixedguid"
        assert row.MixedGuid == "6F9619FF-8B86-D011-B42D-00C04FC964FF"
        assert row.mixedguid == row.MixedGuid
        assert row.MIXEDGUID == row.MixedGuid
    finally:
        mssql_python.lowercase = previous_lowercase
        mssql_python.native_uuid = previous_native_uuid
        await cursor.close()


@pytest.mark.asyncio
async def test_fetch_translates_py_core_exception(async_connection):
    cursor = async_connection.cursor()
    try:
        await cursor.execute("SELECT 1 / 0")

        with pytest.raises(DataError) as caught:
            await cursor.fetchone()

        assert type(caught.value.__cause__).__module__ == "mssql_py_core"
        assert type(caught.value.__cause__).__name__ == "DatabaseError"
        sql_errors = getattr(caught.value, "sql_errors")
        assert sql_errors
        assert sql_errors[0]["number"] == 8134
    finally:
        await cursor.close()
