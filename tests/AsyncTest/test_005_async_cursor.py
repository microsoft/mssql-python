import pytest

pytest.importorskip("mssql_py_core", exc_type=ImportError)


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
        await cursor.execute(
            "IF CAST(? AS INT) <> 9 THROW 50000, 'Unexpected parameter value', 1",
            9,
        )

        assert cursor.arraysize == 50
        assert cursor.description is None
    finally:
        await cursor.close()


@pytest.mark.asyncio
async def test_close_is_idempotent(async_connection):
    cursor = async_connection.cursor()

    assert await cursor.close() is None
    assert await cursor.close() is None
