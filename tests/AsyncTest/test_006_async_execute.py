import pytest
from uuid import uuid4

pytest.importorskip("mssql_py_core", exc_type=ImportError)

from mssql_python.async_query import AsyncConnection, AsyncCursor
from mssql_python.row import Row


@pytest.mark.asyncio
@pytest.mark.parametrize("use_prepare", (True, False))
async def test_execute_returns_public_cursor_and_binds_parameters(
    async_connection,
    use_prepare,
):
    assert isinstance(async_connection, AsyncConnection)

    cursor = async_connection.cursor()
    try:
        assert isinstance(cursor, AsyncCursor)

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
async def test_execute_accepts_named_parameters(async_cursor):
    result = await async_cursor.execute(
        "SELECT CAST(%(first)s AS INT), CAST(%(second)s AS INT)",
        {"first": 1, "second": 2},
    )

    assert result is async_cursor
    assert await async_cursor.fetchone() == (1, 2)


@pytest.mark.asyncio
async def test_execute_accepts_dbapi_row(async_cursor):
    row = Row([1, 2], {"first": 0, "second": 1})

    result = await async_cursor.execute("SELECT CAST(? AS INT), CAST(? AS INT)", row)

    assert result is async_cursor
    assert await async_cursor.fetchone() == (1, 2)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "operation, rows",
    (
        ("INSERT INTO {table} (id, value) VALUES (?, ?)", [(1, "one"), (2, "two")]),
        (
            "INSERT INTO {table} (id, value) VALUES (%(id)s, %(value)s)",
            [{"id": 1, "value": "one"}, {"id": 2, "value": "two"}],
        ),
    ),
)
async def test_executemany_matches_sync_contract(async_connection, operation, rows):
    cursor = async_connection.cursor()
    table_name = f"async_execute_test_{uuid4().hex}"
    try:
        await cursor.execute(
            f"CREATE TABLE {table_name} (id INT NOT NULL, value NVARCHAR(20) NOT NULL)"
        )
        result = await cursor.executemany(operation.format(table=table_name), rows)
        assert result is None

        await cursor.execute(f"SELECT id, value FROM {table_name} ORDER BY id")
        assert await cursor.fetchall() == [(1, "one"), (2, "two")]
    finally:
        await cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        await cursor.close()
