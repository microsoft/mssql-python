import pytest
from datetime import date, datetime, time
from decimal import Decimal
from uuid import UUID

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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fetch_plan",
    (
        ("many-one",),
        ("one-many",),
        ("many-one-many-one",),
    ),
)
async def test_fetchone_fetchmany_interleaving(async_cursor, fetch_plan):
    await async_cursor.execute(
        "SELECT value FROM (VALUES (1), (2), (3), (4)) AS values_table(value) ORDER BY value"
    )

    if fetch_plan == ("many-one",):
        assert [tuple(row) for row in await async_cursor.fetchmany(1)] == [(1,)]
        assert tuple(await async_cursor.fetchone()) == (2,)
    elif fetch_plan == ("one-many",):
        assert tuple(await async_cursor.fetchone()) == (1,)
        assert [tuple(row) for row in await async_cursor.fetchmany(2)] == [(2,), (3,)]
    else:
        assert [tuple(row) for row in await async_cursor.fetchmany(1)] == [(1,)]
        assert tuple(await async_cursor.fetchone()) == (2,)
        assert [tuple(row) for row in await async_cursor.fetchmany(1)] == [(3,)]
        assert tuple(await async_cursor.fetchone()) == (4,)


@pytest.mark.asyncio
async def test_fetchmany_more_than_available_and_repeated_exhaustion(async_cursor):
    await async_cursor.execute("SELECT value FROM (VALUES (1), (2), (3)) AS rows(value)")

    rows = await async_cursor.fetchmany(10)

    assert [row[0] for row in rows] == [1, 2, 3]
    assert await async_cursor.fetchmany(10) == []
    assert await async_cursor.fetchmany(10) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("fetch_method", ("fetchone", "fetchmany", "fetchall"))
async def test_fetch_empty_result_set(fetch_method, async_cursor):
    await async_cursor.execute("SELECT CAST(1 AS INT) AS value WHERE 1 = 0")

    result = await getattr(async_cursor, fetch_method)()

    assert result is None if fetch_method == "fetchone" else result == []
    assert async_cursor.rowcount == 0


@pytest.mark.asyncio
async def test_fetch_preserves_empty_string_binary_and_null(async_cursor):
    await async_cursor.execute(
        "SELECT * FROM (VALUES "
        "(1, CAST('' AS NVARCHAR(10)), CAST(0x AS VARBINARY(10))), "
        "(2, CAST(NULL AS NVARCHAR(10)), CAST(NULL AS VARBINARY(10))), "
        "(3, CAST('text' AS NVARCHAR(10)), CAST(0x1234 AS VARBINARY(10)))) "
        "AS values_table(id, text_value, binary_value) ORDER BY id"
    )

    first = await async_cursor.fetchone()
    remaining = await async_cursor.fetchall()

    assert tuple(first) == (1, "", b"")
    assert [tuple(row) for row in remaining] == [
        (2, None, None),
        (3, "text", b"\x12\x34"),
    ]


@pytest.mark.asyncio
async def test_fetchmany_handles_mixed_large_lob_sizes(async_cursor):
    medium = "x" * 1_000
    large = "y" * 10_000
    await async_cursor.execute(
        "SELECT * FROM (VALUES "
        "(1, CAST('' AS NVARCHAR(MAX))), "
        "(2, CAST(NULL AS NVARCHAR(MAX))), "
        "(3, CAST(? AS NVARCHAR(MAX))), "
        "(4, CAST(? AS NVARCHAR(MAX)))) AS values_table(id, value) ORDER BY id",
        medium,
        large,
    )

    first_batch = await async_cursor.fetchmany(3)
    second_batch = await async_cursor.fetchmany(3)

    assert [row[1] for row in first_batch] == ["", None, medium]
    assert [row[1] for row in second_batch] == [large]
    assert await async_cursor.fetchmany(3) == []


@pytest.mark.asyncio
async def test_fetch_roundtrips_representative_sync_result_types(async_cursor):
    expected = (
        True,
        -(2**63),
        3.25,
        Decimal("123.45"),
        date(2024, 1, 2),
        time(12, 34, 56, 123456),
        datetime(2024, 1, 2, 12, 34, 56, 123456),
        UUID("6f9619ff-8b86-d011-b42d-00c04fc964ff"),
    )
    await async_cursor.execute(
        "SELECT CAST(1 AS BIT), CAST(-9223372036854775808 AS BIGINT), CAST(3.25 AS FLOAT), "
        "CAST(123.45 AS DECIMAL(10, 2)), CAST('2024-01-02' AS DATE), "
        "CAST('12:34:56.123456' AS TIME(6)), "
        "CAST('2024-01-02T12:34:56.123456' AS DATETIME2(6)), "
        "CAST('6F9619FF-8B86-D011-B42D-00C04FC964FF' AS UNIQUEIDENTIFIER)"
    )

    row = await async_cursor.fetchone()

    assert tuple(row) == expected
