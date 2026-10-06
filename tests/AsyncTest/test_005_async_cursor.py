import asyncio
import pytest

pytest.importorskip("mssql_py_core", exc_type=ImportError)

from mssql_python import OperationalError, ProgrammingError


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


@pytest.mark.asyncio
async def test_close_clears_cached_fetch_rowcount(async_connection):
    cursor = async_connection.cursor()
    await cursor.execute("SELECT 1 AS value")
    await cursor.fetchone()
    assert cursor.rowcount == 1

    await cursor.close()

    assert cursor.rowcount == -1


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel_close", (True, False))
async def test_interrupted_close_retires_result_before_releasing_fetch(
    async_connection, monkeypatch, cancel_close
):
    cursor = async_connection.cursor()
    await cursor.execute("SELECT 1 AS value UNION ALL SELECT 2 ORDER BY value")
    assert await cursor.fetchone() == [1]
    assert cursor.rowcount == 1
    previous_generation = getattr(cursor, "_result_generation")

    native_closed = asyncio.Event()
    release_close = asyncio.Event()
    native_cursor = getattr(cursor, "_py_core_async_cursor")
    close_error = RuntimeError("Cursor close failed: cleanup failed")

    class InterruptedNativeCursor:
        def __getattr__(self, name):
            return getattr(native_cursor, name)

        async def close(self):
            await native_cursor.close()
            native_closed.set()
            await release_close.wait()
            raise close_error

    monkeypatch.setattr(cursor, "_py_core_async_cursor", InterruptedNativeCursor())
    close_task = asyncio.create_task(cursor.close())
    fetch_task = None
    try:
        await asyncio.wait_for(native_closed.wait(), timeout=5)
        fetch_task = asyncio.create_task(cursor.fetchmany(0))
        await asyncio.sleep(0)
        assert not fetch_task.done()

        if cancel_close:
            close_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await close_task
        else:
            release_close.set()
            with pytest.raises(RuntimeError) as caught:
                await close_task
            assert caught.value is close_error

        assert cursor.description is None
        assert cursor.rowcount == native_cursor.rowcount
        assert getattr(cursor, "_result_generation") > previous_generation
        with pytest.raises(ProgrammingError, match="Cursor is closed"):
            await fetch_task
        with pytest.raises(ProgrammingError, match="Cursor is closed"):
            await cursor.fetchmany(0)
    finally:
        release_close.set()
        await asyncio.gather(close_task, return_exceptions=True)
        if fetch_task is not None:
            await asyncio.gather(fetch_task, return_exceptions=True)
        monkeypatch.setattr(cursor, "_py_core_async_cursor", native_cursor)
        await cursor.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("message", "public_error"),
    (
        ("Connection is busy with another cursor operation", OperationalError),
        ("close awaitable creation failed", RuntimeError),
    ),
)
async def test_close_call_time_rejection_preserves_result(
    async_connection, monkeypatch, message, public_error
):
    cursor = async_connection.cursor()
    await cursor.execute("SELECT 1 AS value UNION ALL SELECT 2 ORDER BY value")
    assert await cursor.fetchone() == [1]
    previous_description = cursor.description
    previous_generation = getattr(cursor, "_result_generation")
    native_cursor = getattr(cursor, "_py_core_async_cursor")

    class RejectingNativeCursor:
        def close(self):
            raise RuntimeError(message)

    try:
        with monkeypatch.context() as patch:
            patch.setattr(cursor, "_py_core_async_cursor", RejectingNativeCursor())
            with pytest.raises(public_error, match=message):
                await cursor.close()

        assert cursor.description is previous_description
        assert getattr(cursor, "_result_generation") == previous_generation
        assert cursor.rowcount == 1
        assert await cursor.fetchmany(0) == []
        row = await cursor.fetchone()
        assert row is not None
        assert row.value == 2
        assert cursor.rowcount == 2
    finally:
        monkeypatch.setattr(cursor, "_py_core_async_cursor", native_cursor)
        await cursor.close()
