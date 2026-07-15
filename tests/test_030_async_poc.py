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


# ---------------------------------------------------------------------------
# TestAsyncScale — many concurrent connections against the Rust runtime
# ---------------------------------------------------------------------------
_ROWS_PER_TASK: int = 50


def _complex_long_running_query(task_id: int, wait_ms: int) -> str:
    """T-SQL that is both non-trivial and predictably verifiable per row.

    Structure:
      * ``WAITFOR DELAY`` prefix — makes each batch a **long-running**
        statement so the parallel wall-time assertion is meaningful.
      * Recursive CTE emitting ``_ROWS_PER_TASK`` rows.
      * Five computed columns exercising the row-decode pipeline:
        - ``task_id``   INT      — the interpolated task id (identity check).
        - ``n``         INT      — 1..ROWS_PER_TASK (row order).
        - ``square``    INT      — ``n * n`` (integer arithmetic).
        - ``label``     NVARCHAR — per-row string, checked exactly.
        - ``root``      FLOAT    — ``SQRT(n)`` (floating-point decode).

    ``task_id`` is interpolated into the SQL text because the POC does not
    yet support parameterised queries. Every value is server-computed so
    driver-side tampering would surface as a mismatch.
    """
    return f"""
        WAITFOR DELAY '00:00:00.{wait_ms:03d}';
        WITH nums AS (
            SELECT CAST(1 AS INT) AS n
            UNION ALL
            SELECT n + 1 FROM nums WHERE n < {_ROWS_PER_TASK}
        )
        SELECT
            CAST({task_id} AS INT) AS task_id,
            n,
            n * n AS square,
            CAST(N'task-' + CAST({task_id} AS nvarchar(8))
                 + N'-row-' + CAST(n AS nvarchar(8))
                 AS nvarchar(40)) AS label,
            CAST(SQRT(CAST(n AS FLOAT)) AS FLOAT) AS root
        FROM nums
        OPTION (MAXRECURSION {_ROWS_PER_TASK});
    """


def _verify_task_rows(task_id: int, rows: list) -> None:
    """Row-by-row validation for :func:`_complex_long_running_query`."""
    import math

    assert len(rows) == _ROWS_PER_TASK, (
        f"task {task_id} returned {len(rows)} rows, expected {_ROWS_PER_TASK}"
    )
    for expected_n, row in enumerate(rows, start=1):
        assert len(row) == 5, (
            f"task {task_id} row {expected_n} has {len(row)} columns, expected 5"
        )
        got_task_id, got_n, got_square, got_label, got_root = row
        assert got_task_id == task_id, (
            f"task {task_id} row {expected_n}: task_id={got_task_id!r}"
        )
        assert got_n == expected_n, (
            f"task {task_id}: row {expected_n} had n={got_n!r}"
        )
        assert got_square == expected_n * expected_n, (
            f"task {task_id} row {expected_n}: square={got_square!r}, "
            f"expected {expected_n * expected_n}"
        )
        assert got_label == f"task-{task_id}-row-{expected_n}", (
            f"task {task_id} row {expected_n}: label={got_label!r}"
        )
        assert isinstance(got_root, float), (
            f"task {task_id} row {expected_n}: "
            f"expected FLOAT to decode as Python float, got {type(got_root).__name__}"
        )
        assert abs(got_root - math.sqrt(expected_n)) < 1e-9, (
            f"task {task_id} row {expected_n}: "
            f"root={got_root!r}, expected {math.sqrt(expected_n)!r}"
        )


class TestAsyncScale:
    """Scale tests for the async POC — 100 concurrent connections running
    a complex, long-running SELECT with full per-row data verification.

    Each task opens a fresh :class:`AsyncConnection`, runs a query that:

      * server-waits (``WAITFOR DELAY``) — long-running behaviour,
      * emits ``_ROWS_PER_TASK`` rows from a recursive CTE,
      * has five computed columns of three distinct types (int, nvarchar,
        float) so every row can be validated exactly.

    Every value is server-computed, so any driver-side corruption (wrong
    column order, truncated string, wrong float bits, wrong integer
    width) surfaces as an assertion failure.

    Exercises simultaneously:
      * the process-wide Tokio runtime under load,
      * the pyo3-async-runtimes → asyncio bridge with 100 futures in flight,
      * GIL discipline — row decode of 5 000 rows over 100 connections must
        not serialise the event loop,
      * streaming ``fetchone`` correctness across many rows per connection.

    Marked ``slow`` so ``pytest -m "not slow"`` can skip them.
    """

    @pytest.mark.slow
    async def test_hundred_connections_complex_query_correctness(
        self, async_conn_str
    ):
        """100 tasks, complex long-running query, every row checked."""
        n_tasks = 100
        # Modest wait keeps this test's wall time low while still making
        # the batch "long-running" from the driver's point of view.
        wait_ms = 100

        async def one(task_id: int) -> list:
            async with await mssql_python.connect_async(async_conn_str) as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        _complex_long_running_query(task_id, wait_ms=wait_ms),
                        timeout_sec=60,
                    )
                    rows: list = []
                    while (r := await cur.fetchone()) is not None:
                        rows.append(r)
                    return rows

        start = time.monotonic()
        results = await asyncio.gather(*(one(i) for i in range(n_tasks)))
        elapsed = time.monotonic() - start

        # Per-task, per-row verification: server-computed values must match
        # driver-decoded values exactly for every one of the 5 000 rows.
        for task_id, rows in enumerate(results):
            _verify_task_rows(task_id, rows)

        total_rows = n_tasks * _ROWS_PER_TASK
        # 100 connections x ~100 ms wait + query = ~10 s serial baseline.
        # Parallel budget must be comfortably below that.
        assert elapsed < 10, (
            f"100 concurrent complex-query connections took {elapsed:.3f}s "
            f"(>10s ceiling; serial baseline ~10s)"
        )
        print(
            f"[scale.complex-correctness] 100 conns x {_ROWS_PER_TASK} rows "
            f"= {total_rows} rows in {elapsed:.3f}s "
            f"({total_rows / elapsed:.0f} rows/s)"
        )

    @pytest.mark.slow
    async def test_hundred_connections_long_running_parallelism(
        self, async_conn_str
    ):
        """100 tasks each running a 200 ms + complex CTE query.

        Serial baseline is ~20 s (100 × 200 ms just for the WAITFOR).
        Parallel target is well under that. Every row is still verified —
        this is not just a wall-time assertion.
        """
        n_tasks = 100
        wait_ms = 200

        async def one(task_id: int) -> list:
            async with await mssql_python.connect_async(async_conn_str) as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        _complex_long_running_query(task_id, wait_ms=wait_ms),
                        timeout_sec=60,
                    )
                    rows: list = []
                    while (r := await cur.fetchone()) is not None:
                        rows.append(r)
                    return rows

        start = time.monotonic()
        results = await asyncio.gather(*(one(i) for i in range(n_tasks)))
        elapsed = time.monotonic() - start

        for task_id, rows in enumerate(results):
            _verify_task_rows(task_id, rows)

        total_rows = n_tasks * _ROWS_PER_TASK
        assert elapsed < 10, (
            f"100 x (200 ms WAITFOR + complex CTE) took {elapsed:.3f}s "
            f"(expected << 20 s serial baseline)"
        )
        print(
            f"[scale.long-running-parallel] 100 conns x {_ROWS_PER_TASK} rows "
            f"= {total_rows} rows in {elapsed:.3f}s "
            f"(serial baseline ~20s, {total_rows / elapsed:.0f} rows/s)"
        )
