"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.

Phase-5 end-to-end tests for the mssql-python async POC.

Only the two POC methods are exercised:

* :meth:`mssql_python.AsyncCursor.execute`
* :meth:`mssql_python.AsyncCursor.fetchone`

plus :meth:`mssql_python.AsyncCursor.cancel` for the cancellation contract.
"""

from __future__ import annotations

import asyncio
import os
import time

import pytest

import mssql_python

# ``asyncio_mode = strict`` in pytest.ini means each async test needs the
# marker — set it once at module scope.
pytestmark = pytest.mark.asyncio


# ---------------------------------------------------------------------------
# TestAsyncBasics — happy-path round-trips
# ---------------------------------------------------------------------------
class TestAsyncBasics:
    async def test_select_literal_returns_tuple(self, async_cursor):
        r = await async_cursor.execute("SELECT 42")
        # execute returns self so callers can chain fluently.
        assert r is async_cursor
        row = await async_cursor.fetchone()
        assert row == (42,)

    async def test_select_multiple_columns(self, async_cursor):
        await async_cursor.execute(
            "SELECT 1, N'hi', CAST(3.14 AS FLOAT)"
        )
        row = await async_cursor.fetchone()
        assert row is not None
        assert row[0] == 1
        assert row[1] == "hi"
        assert abs(row[2] - 3.14) < 1e-9

    async def test_fetchone_returns_none_after_exhausting(self, async_cursor):
        await async_cursor.execute("SELECT 1")
        assert (await async_cursor.fetchone()) == (1,)
        assert (await async_cursor.fetchone()) is None
        # Idempotent: a second post-exhaustion fetch is still None.
        assert (await async_cursor.fetchone()) is None

    async def test_multiple_rows_via_repeated_fetchone(self, async_cursor):
        await async_cursor.execute(
            "SELECT n FROM (VALUES (1),(2),(3)) AS t(n) ORDER BY n"
        )
        rows = []
        while (r := await async_cursor.fetchone()) is not None:
            rows.append(r)
        assert rows == [(1,), (2,), (3,)]

    async def test_reexecute_after_drain(self, async_cursor):
        await async_cursor.execute("SELECT 10")
        assert (await async_cursor.fetchone()) == (10,)
        assert (await async_cursor.fetchone()) is None
        await async_cursor.execute("SELECT 20")
        assert (await async_cursor.fetchone()) == (20,)

    async def test_context_manager(self, async_conn_str):
        async with await mssql_python.connect_async(async_conn_str) as conn:
            assert not conn.closed
            async with conn.cursor() as cur:
                assert not cur.closed
                await cur.execute("SELECT 7")
                assert (await cur.fetchone()) == (7,)
            assert cur.closed
        assert conn.closed


# ---------------------------------------------------------------------------
# TestAsyncErrors — error path fidelity
# ---------------------------------------------------------------------------
class TestAsyncErrors:
    async def test_bad_sql_raises(self, async_cursor):
        with pytest.raises(Exception):
            await async_cursor.execute("SELECT * FROM __no_such_table__xyz")

    async def test_execute_after_cursor_close_raises(self, async_cursor):
        await async_cursor.close()
        with pytest.raises(Exception):
            await async_cursor.execute("SELECT 1")

    async def test_fetchone_after_cursor_close_raises(self, async_cursor):
        await async_cursor.close()
        with pytest.raises(Exception):
            await async_cursor.fetchone()

    async def test_cursor_from_closed_connection_raises(self, async_connection):
        await async_connection.close()
        with pytest.raises(Exception):
            async_connection.cursor()


# ---------------------------------------------------------------------------
# TestAsyncNonBlocking — proves the event loop is not blocked
# ---------------------------------------------------------------------------
class TestAsyncNonBlocking:
    async def test_event_loop_remains_responsive(self, async_cursor):
        """A ticker task must make progress while a server-side WAITFOR runs."""
        ticks = 0

        async def ticker():
            nonlocal ticks
            # 40 iterations * 10ms = ~400ms max ticker duration, mirroring
            # the server WAITFOR budget below.
            for _ in range(40):
                await asyncio.sleep(0.01)
                ticks += 1

        t = asyncio.create_task(ticker())
        start = time.monotonic()
        await async_cursor.execute("WAITFOR DELAY '00:00:00.400'; SELECT 1")
        row = await async_cursor.fetchone()
        elapsed = time.monotonic() - start
        await t

        assert row == (1,)
        # A properly non-blocking driver lets the loop dispatch the ticker
        # ~40 times during a 400ms server wait. Allow half of that as slack
        # for slow CI runners.
        assert ticks >= 20, (
            f"event loop appears blocked "
            f"(ticks={ticks}, elapsed={elapsed:.3f}s)"
        )

    async def test_two_connections_run_concurrently(self, async_conn_str):
        """asyncio.gather across independent connections must overlap."""

        async def one():
            async with await mssql_python.connect_async(async_conn_str) as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "WAITFOR DELAY '00:00:00.500'; SELECT 1"
                    )
                    return await cur.fetchone()

        start = time.monotonic()
        results = await asyncio.gather(one(), one())
        elapsed = time.monotonic() - start

        assert results == [(1,), (1,)]
        # Two 500ms queries would take ~1s if serialised on a single-thread
        # runtime. Multi-thread + parallel connections should finish well
        # under that.
        assert elapsed < 0.9, (
            f"queries appear serialised (elapsed={elapsed:.3f}s)"
        )


# ---------------------------------------------------------------------------
# TestAsyncCancel — TDS attention plumbing
# ---------------------------------------------------------------------------
class TestAsyncCancel:
    async def test_explicit_cancel_aborts_long_query(self, async_connection):
        cur = async_connection.cursor()
        try:
            task = asyncio.create_task(
                cur.execute("WAITFOR DELAY '00:00:10'", timeout_sec=30)
            )
            # Let the batch reach the server and start waiting.
            await asyncio.sleep(0.2)

            start = time.monotonic()
            cur.cancel()

            with pytest.raises(Exception):
                await asyncio.wait_for(task, timeout=5)
            elapsed = time.monotonic() - start

            # The server acknowledges attention within a few ms once it's
            # in a wait; leave generous headroom for slow CI runners.
            assert elapsed < 5, (
                f"cancel took too long ({elapsed:.3f}s)"
            )
        finally:
            await cur.close()
