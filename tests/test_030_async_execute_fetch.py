"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.

Basic functional tests for the async POC on ``Cursor`` — ``execute_async``
and ``fetch_async`` (see ``docs/async_query_poc_spec.md``).

The suite is intentionally minimal for the POC. It covers:
  1. The Step-1 capability probe (``Connection.is_async_capable``) reports True
     on hosts where the Microsoft ODBC driver supports statement-level async.
  2. A single ``execute_async`` / ``fetch_async`` round-trip returns the
     expected row (end-to-end smoke).
  3. **The requested "100 concurrent async statements" workload** — fires 100
     ``execute_async`` + ``fetch_async`` pairs through ``asyncio.gather`` with
     a bounded semaphore, and asserts all 100 complete with the correct row.
     Uses one dedicated connection per task to avoid SQL Server's
     Multiple-Active-Result-Sets (MARS) restriction on a single connection.

The tests skip cleanly when:
  * ``DB_CONNECTION_STRING`` is not set in the environment, OR
  * the driver reports ``SQL_ASYNC_MODE == SQL_AM_NONE`` (async unavailable).

No dependency on ``pytest-asyncio`` — each test uses ``asyncio.run(...)``
directly, so the existing pytest install is sufficient.
"""

import asyncio
import os
import time

import pytest

from mssql_python import connect
from mssql_python.exceptions import ProgrammingError


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture(scope="module")
def conn_str():
    """Skip the whole module if ``DB_CONNECTION_STRING`` is unset."""
    cs = os.getenv("DB_CONNECTION_STRING")
    if not cs:
        pytest.skip("DB_CONNECTION_STRING environment variable not set")
    return cs


@pytest.fixture(scope="module")
def _async_capable(conn_str):
    """Skip all tests in the module if the driver doesn't advertise async support.

    Opens a short-lived connection just to run the SQLGetInfo(SQL_ASYNC_MODE)
    probe added in async POC step 1. Result is cached inside the C++ Connection
    object, so this doesn't cost anything for later tests that reconnect.
    """
    conn = connect(conn_str)
    try:
        if not conn._conn.is_async_capable():
            pytest.skip(
                "ODBC driver reports SQL_AM_NONE — async POC is not usable on this driver"
            )
    finally:
        conn.close()


# ============================================================================
# Sanity tests
# ============================================================================


def test_capability_probe_returns_true(conn_str, _async_capable):
    """``Connection.is_async_capable()`` reports True on this driver.

    Redundant with the ``_async_capable`` fixture, but keeps the probe visible
    in the test list so a regression on the C++ SQLGetInfo path shows up as a
    named failure rather than a silent module skip.
    """
    conn = connect(conn_str)
    try:
        assert conn._conn.is_async_capable() is True
    finally:
        conn.close()


def test_single_execute_async_and_fetch_async(conn_str, _async_capable):
    """One ``execute_async`` followed by ``fetch_async()`` returns the expected row."""

    async def _run():
        conn = connect(conn_str)
        try:
            cur = conn.cursor()
            try:
                await cur.execute_async(
                    "SELECT ? AS n, CAST(? AS NVARCHAR(16)) AS msg", 42, "hello"
                )
                row = await cur.fetch_async()
                assert row is not None
                assert row[0] == 42
                assert row[1] == "hello"

                # After the single row, fetch_async() should return None.
                assert await cur.fetch_async() is None
            finally:
                cur.close()
        finally:
            conn.close()

    asyncio.run(_run())


# ============================================================================
# The 100-concurrent async workload
# ============================================================================
# Fires ASYNC_TEST_CONCURRENCY (default 100) SELECT statements concurrently
# through ``asyncio.gather``. Each task runs on its own connection so we
# don't hit SQL Server's default no-MARS restriction (a single connection
# only supports one active statement at a time).
#
# A bounded ``asyncio.Semaphore`` limits how many are simultaneously
# in flight. Both knobs are env-overridable so CI environments with
# tight connection limits (or slow bring-up) can tune without editing.
# ============================================================================

CONCURRENT_TASKS = int(os.getenv("ASYNC_TEST_CONCURRENCY", "100"))
CONCURRENT_LIMIT = int(os.getenv("ASYNC_TEST_MAX_INFLIGHT", "16"))
# Loose upper bound to catch obvious hangs; not a perf assertion.
WALL_CLOCK_BUDGET_SECONDS = float(os.getenv("ASYNC_TEST_WALL_BUDGET", "120"))


def test_100_concurrent_async_selects(conn_str, _async_capable):
    """Fire 100 (or ``ASYNC_TEST_CONCURRENCY``) async SELECT statements concurrently.

    Each task:
      1. Opens its own connection (fresh HSTMT, avoids MARS restriction).
      2. Runs ``execute_async`` with two parameters (index + label).
      3. Runs ``fetch_async()`` and asserts the row matches its index.
      4. Closes cursor + connection.

    A ``Semaphore(CONCURRENT_LIMIT)`` throttles the number of simultaneously
    in-flight tasks so we don't exhaust the client's default asyncio executor
    (``ThreadPoolExecutor`` with ``max_workers = min(32, os.cpu_count()+4)``)
    or SQL Server's login-handshake queue.

    Correctness assertion: every task must return its own index — proves that
    responses don't get cross-wired between concurrent HSTMTs.
    """

    async def _one_query(idx: int, sem: asyncio.Semaphore):
        async with sem:
            conn = connect(conn_str)
            try:
                cur = conn.cursor()
                try:
                    await cur.execute_async(
                        "SELECT ? AS idx, CAST(? AS NVARCHAR(16)) AS label",
                        idx,
                        f"task-{idx}",
                    )
                    row = await cur.fetch_async()
                    assert row is not None, f"task {idx}: fetch_async returned None"
                    assert row[0] == idx, f"task {idx}: got idx={row[0]}, expected {idx}"
                    assert row[1] == f"task-{idx}", (
                        f"task {idx}: got label={row[1]!r}, expected 'task-{idx}'"
                    )
                    return idx
                finally:
                    cur.close()
            finally:
                conn.close()

    async def _run():
        sem = asyncio.Semaphore(CONCURRENT_LIMIT)
        start = time.perf_counter()
        results = await asyncio.gather(
            *[_one_query(i, sem) for i in range(CONCURRENT_TASKS)]
        )
        elapsed = time.perf_counter() - start
        return results, elapsed

    results, elapsed = asyncio.run(_run())

    # Every task returned its own index — proves no cross-wiring between HSTMTs.
    assert sorted(results) == list(range(CONCURRENT_TASKS)), (
        f"missing / duplicate task indices in results (got {len(results)} results, "
        f"unique={len(set(results))})"
    )

    # Loose sanity bound to catch runaway hangs; NOT a perf assertion.
    assert elapsed < WALL_CLOCK_BUDGET_SECONDS, (
        f"{CONCURRENT_TASKS} concurrent async queries took {elapsed:.1f}s "
        f"(>{WALL_CLOCK_BUDGET_SECONDS}s budget) — likely a hang or serialization bug"
    )

    print(
        f"\n[async POC] {CONCURRENT_TASKS} concurrent queries "
        f"(semaphore={CONCURRENT_LIMIT}) completed in {elapsed:.2f}s"
    )


# ============================================================================
# MARS variant: same workload on ONE MARS-enabled connection with N cursors
# ============================================================================
# Complements the one-connection-per-task test above. Here we open ONE
# connection with MARS enabled and create N cursors from it, then fire the
# same 100 concurrent execute_async + fetch_async pairs.
#
# Important distinctions from the connection-per-task variant:
#  * All requests share a single DBC handle and a single underlying TCP
#    socket. The ODBC driver multiplexes concurrent statements via MARS.
#    This is materially different from true network-level parallelism —
#    execution is interleaved rather than simultaneous.
#  * Exercises the per-HSTMT SQL_ATTR_ASYNC_ENABLE / AsyncEnableGuard path
#    when many HSTMTs share one DBC, which is the "N cursors on 1 conn"
#    pattern documented in async POC spec §4.5.
#
# History: an earlier revision of this test used ``MultipleActiveResultSets=Yes``
# in the connection string, which the bundled msodbcsql18 driver silently
# ignores (MARS was never actually enabled). Under that regime, concurrent
# async on the shared DBC crashed with SIGSEGV because the driver returned
# SQL_ERROR ("connection busy") for every second-in-flight statement and our
# concurrent DDBCSQLCheckError calls raced on shared DBC diagnostic state.
#
# Root-cause fix (2026-07): switched to the ODBC-standard alias
# ``MARS_Connection=Yes`` (the only MARS keyword msodbcsql18 actually honors)
# and added it to _ALLOWED_CONNECTION_STRING_PARAMS. With MARS genuinely on,
# concurrent async on shared DBC works cleanly (100 tasks, semaphore=16,
# ~0.13 s wall-clock on local Docker).
# ============================================================================


def _with_mars(conn_str: str) -> str:
    """Return ``conn_str`` with ``MARS_Connection=Yes`` appended, or the
    original string if the caller already set a MARS keyword.

    Uses the ODBC-standard spelling ``MARS_Connection`` rather than the
    SQL Server-native ``MultipleActiveResultSets`` because the bundled
    msodbcsql18 driver silently ignores the latter (empirically verified;
    see ``mssql_python/constants.py`` for the allowlist rationale).

    Returns an empty string as a sentinel when the caller explicitly *disabled*
    MARS — the caller should then skip the test rather than override the
    user's choice.
    """
    lower = conn_str.lower()
    if "multipleactiveresultsets=yes" in lower or "mars_connection=yes" in lower:
        return conn_str
    if "multipleactiveresultsets=no" in lower or "mars_connection=no" in lower:
        return ""  # sentinel — caller skips
    sep = "" if conn_str.rstrip().endswith(";") else ";"
    return f"{conn_str}{sep}MARS_Connection=Yes"


def test_100_concurrent_async_selects_on_single_mars_connection(conn_str, _async_capable):
    """N async SELECTs concurrently on ONE MARS-enabled connection (N cursors).

    Complements ``test_100_concurrent_async_selects``. That test opens one
    connection per task; this one opens ONE connection with
    ``MARS_Connection=Yes`` and creates N cursors on it, then fires the same
    workload through ``asyncio.gather``.

    Correctness assertions match the connection-per-task variant: every task
    must return its own index, proving that MARS + per-HSTMT
    ``SQL_ATTR_ASYNC_ENABLE`` toggling doesn't cross-wire results between
    cursors sharing a single DBC.

    Note: MARS multiplexes over one TCP socket, so this is interleaved
    (not truly parallel) execution — expect a different wall-clock profile
    than the connection-per-task variant.

    Kept forward-compatible: if a caller explicitly disables MARS via
    ``MARS_Connection=No`` in DB_CONNECTION_STRING, the test skips rather
    than override that choice. The keyword-rejection fallback below
    remains in place in case a future mssql-python version removes MARS
    from the allowlist (unlikely).
    """
    mars_conn_str = _with_mars(conn_str)
    if not mars_conn_str:
        pytest.skip(
            "DB_CONNECTION_STRING explicitly disables MARS — cannot run "
            "the same-connection concurrency test"
        )

    # Defensive: skip cleanly if a future mssql-python version rejects the
    # MARS keyword in its allowlist. Not expected on the current codebase,
    # where mars_connection is in _ALLOWED_CONNECTION_STRING_PARAMS.
    try:
        _probe_conn = connect(mars_conn_str)
        _probe_conn.close()
    except Exception as e:
        msg = str(e).lower()
        if "multipleactiveresultsets" in msg or "mars_connection" in msg or "unknown keyword" in msg:
            pytest.skip(
                f"mssql-python does not currently accept MARS in the connection "
                f"string allowlist — skipping same-connection concurrency test. "
                f"Underlying error: {e}"
            )
        raise

    async def _one_query(idx: int, cur, sem: asyncio.Semaphore):
        async with sem:
            await cur.execute_async(
                "SELECT ? AS idx, CAST(? AS NVARCHAR(16)) AS label",
                idx,
                f"task-{idx}",
            )
            row = await cur.fetch_async()
            assert row is not None, f"task {idx}: fetch_async returned None"
            assert row[0] == idx, f"task {idx}: got idx={row[0]}, expected {idx}"
            assert row[1] == f"task-{idx}", (
                f"task {idx}: got label={row[1]!r}, expected 'task-{idx}'"
            )
            return idx

    async def _run():
        conn = connect(mars_conn_str)
        try:
            cursors = [conn.cursor() for _ in range(CONCURRENT_TASKS)]
            try:
                sem = asyncio.Semaphore(CONCURRENT_LIMIT)
                start = time.perf_counter()
                results = await asyncio.gather(
                    *[_one_query(i, cursors[i], sem) for i in range(CONCURRENT_TASKS)]
                )
                elapsed = time.perf_counter() - start
                return results, elapsed
            finally:
                for c in cursors:
                    c.close()
        finally:
            conn.close()

    results, elapsed = asyncio.run(_run())

    # Every task returned its own index — proves no cross-wiring between
    # HSTMTs that share a single DBC via MARS.
    assert sorted(results) == list(range(CONCURRENT_TASKS)), (
        f"missing / duplicate task indices in MARS results "
        f"(got {len(results)} results, unique={len(set(results))})"
    )

    # Loose sanity bound; NOT a perf assertion (MARS interleaves over one
    # socket, so this is expected to be slower than the connection-per-task
    # variant, but nowhere near WALL_CLOCK_BUDGET_SECONDS).
    assert elapsed < WALL_CLOCK_BUDGET_SECONDS, (
        f"{CONCURRENT_TASKS} async queries on 1 MARS conn took {elapsed:.1f}s "
        f"(>{WALL_CLOCK_BUDGET_SECONDS}s budget) — likely a hang or serialization bug"
    )

    print(
        f"\n[async POC MARS] {CONCURRENT_TASKS} async queries on 1 MARS conn, "
        f"{CONCURRENT_TASKS} cursors (semaphore={CONCURRENT_LIMIT}) "
        f"completed in {elapsed:.2f}s"
    )


# ============================================================================
# Additional stability tests (cases 1, 2, 4, 6, 7, 8, 9, 10, 11, 12)
# ============================================================================
# These tests exercise the async surface across several axes: long queries,
# large result sets, event-loop non-blocking behavior, multi-batch fetches,
# and sequential vs concurrent invocation patterns. All safe patterns use
# one connection per concurrent task (see block comment on
# test_100_concurrent_async_selects for why).
#
# Tunables (env-overridable):
#   ASYNC_TEST_LARGE_ROWS          rows for the large-result-set test (default 5000)
#   ASYNC_TEST_STRESS_CONCURRENCY  N for the 1000-task stress test (default 1000)
#   ASYNC_TEST_WAITFOR_SECONDS     server-side delay for long-query tests (default 2)
# ============================================================================

LARGE_ROWS = int(os.getenv("ASYNC_TEST_LARGE_ROWS", "5000"))
# The fetch-heartbeat test needs a materially slower fetch (multi-hundred ms)
# for the 10 ms heartbeat to have time to tick a meaningful number of times.
# Default to ~10× LARGE_ROWS so even a fast local SQL Server produces enough
# TDS traffic to keep the fetch worker thread busy for a while.
HB_FETCH_ROWS = int(os.getenv("ASYNC_TEST_HB_FETCH_ROWS", "50000"))
STRESS_CONCURRENCY = int(os.getenv("ASYNC_TEST_STRESS_CONCURRENCY", "1000"))
WAITFOR_SECONDS = int(os.getenv("ASYNC_TEST_WAITFOR_SECONDS", "2"))
WAITFOR_SQL = f"WAITFOR DELAY '00:00:0{WAITFOR_SECONDS}'"


# ---------- Case 1: Execute long-running query asynchronously --------------


def test_execute_async_long_running_query(conn_str, _async_capable):
    """Run a ~2-second server-side WAITFOR through execute_async and verify
    the row afterward comes back correctly.

    Establishes that the polling loop survives realistic query durations
    (many polling iterations of SQL_STILL_EXECUTING) and doesn't lose the
    result set.
    """

    async def _run():
        conn = connect(conn_str)
        try:
            cur = conn.cursor()
            try:
                start = time.perf_counter()
                await cur.execute_async(f"{WAITFOR_SQL}; SELECT 42 AS n")
                elapsed = time.perf_counter() - start
                row = await cur.fetch_async()
                return elapsed, row
            finally:
                cur.close()
        finally:
            conn.close()

    elapsed, row = asyncio.run(_run())
    assert row is not None and row[0] == 42
    assert elapsed >= WAITFOR_SECONDS - 0.2, (
        f"execute_async returned too quickly ({elapsed:.2f}s < {WAITFOR_SECONDS}s) — "
        f"WAITFOR was probably not honored"
    )


# ---------- Case 2: Event loop continues while query executes --------------


def test_event_loop_progresses_during_execute_async(conn_str, _async_capable):
    """A background heartbeat coroutine must keep ticking while a long query
    is being polled by the C++ executor thread.

    Proves that ``execute_async`` does NOT starve the asyncio event loop:
    the polling loop runs in the run_in_executor worker thread with the
    GIL released, so the main thread's event loop stays free to schedule
    other coroutines.
    """

    async def _run():
        conn = connect(conn_str)
        heartbeats = 0

        async def heartbeat():
            nonlocal heartbeats
            while True:
                await asyncio.sleep(0.01)  # 10ms tick
                heartbeats += 1

        hb_task = asyncio.create_task(heartbeat())
        try:
            cur = conn.cursor()
            try:
                await cur.execute_async(f"{WAITFOR_SQL}; SELECT 1")
                _ = await cur.fetch_async()
            finally:
                cur.close()
        finally:
            hb_task.cancel()
            try:
                await hb_task
            except asyncio.CancelledError:
                pass
            conn.close()
        return heartbeats

    heartbeats = asyncio.run(_run())
    # Expect ~100 ticks/second × WAITFOR_SECONDS with plenty of margin for
    # scheduling jitter. A value < ~30 (i.e. 15% of ideal) indicates the
    # event loop was starved.
    min_expected = max(30, WAITFOR_SECONDS * 30)
    assert heartbeats >= min_expected, (
        f"event loop appears to have been blocked: only {heartbeats} heartbeats "
        f"in {WAITFOR_SECONDS}s (expected >= {min_expected}) — "
        f"execute_async is probably not releasing the event loop"
    )
    print(
        f"\n[async POC] heartbeat during {WAITFOR_SECONDS}s execute_async: "
        f"{heartbeats} ticks (>= {min_expected} required)"
    )


# ---------- Case 4: 1000 async executes on different connections -----------


@pytest.mark.stress
def test_1000_async_executes_on_different_connections(conn_str, _async_capable):
    """Scale the connection-per-task workload up to 1000 tasks.

    Marked ``stress`` (excluded from the default pytest run per pytest.ini)
    because opening 1000 connections stresses SQL Server's login-handshake
    queue and the local machine's ephemeral port pool. Bounded by
    ``ASYNC_TEST_MAX_INFLIGHT`` (default 16) so at most that many are being
    established at any instant.
    """

    async def _one_query(idx: int, sem: asyncio.Semaphore):
        async with sem:
            conn = connect(conn_str)
            try:
                cur = conn.cursor()
                try:
                    await cur.execute_async("SELECT ? AS idx", idx)
                    row = await cur.fetch_async()
                    assert row is not None and row[0] == idx
                    return idx
                finally:
                    cur.close()
            finally:
                conn.close()

    async def _run():
        sem = asyncio.Semaphore(CONCURRENT_LIMIT)
        start = time.perf_counter()
        results = await asyncio.gather(
            *[_one_query(i, sem) for i in range(STRESS_CONCURRENCY)]
        )
        return results, time.perf_counter() - start

    results, elapsed = asyncio.run(_run())
    assert sorted(results) == list(range(STRESS_CONCURRENCY))
    print(
        f"\n[async POC stress] {STRESS_CONCURRENCY} async queries "
        f"(semaphore={CONCURRENT_LIMIT}) completed in {elapsed:.2f}s"
    )


# ---------- Case 6: Fetch multiple rows asynchronously ---------------------


def test_fetch_async_returns_batch_of_rows(conn_str, _async_capable):
    """``fetch_async(size=N)`` should return a ``List[Row]`` with the first N
    rows of the result set."""

    async def _run():
        conn = connect(conn_str)
        try:
            cur = conn.cursor()
            try:
                # Simple VALUES clause yields 5 rows deterministically.
                await cur.execute_async(
                    "SELECT * FROM (VALUES (1),(2),(3),(4),(5)) AS t(n)"
                )
                rows = await cur.fetch_async(5)
                return rows
            finally:
                cur.close()
        finally:
            conn.close()

    rows = asyncio.run(_run())
    assert isinstance(rows, list) and len(rows) == 5
    assert [r[0] for r in rows] == [1, 2, 3, 4, 5]


# ---------- Case 7: Fetch large result set ---------------------------------


def test_fetch_async_large_result_set(conn_str, _async_capable):
    """``fetch_async(-1)`` should return all rows for a large result set.

    Uses a CROSS JOIN of ``sys.all_objects`` to generate ``LARGE_ROWS``
    rows, which produces a multi-KB result set spanning multiple TDS
    packets. Verifies the async fetch path handles multi-batch responses
    correctly.
    """

    async def _run():
        conn = connect(conn_str)
        try:
            cur = conn.cursor()
            try:
                await cur.execute_async(
                    f"SELECT TOP {LARGE_ROWS} "
                    f"ROW_NUMBER() OVER (ORDER BY a.object_id) AS n, "
                    f"CAST(a.name AS NVARCHAR(128)) AS obj_name "
                    f"FROM sys.all_objects a CROSS JOIN sys.all_objects b"
                )
                rows = await cur.fetch_async(-1)
                return rows
            finally:
                cur.close()
        finally:
            conn.close()

    rows = asyncio.run(_run())
    assert len(rows) == LARGE_ROWS, f"expected {LARGE_ROWS} rows, got {len(rows)}"
    # Row numbering is 1..LARGE_ROWS and sequential.
    assert rows[0][0] == 1
    assert rows[-1][0] == LARGE_ROWS
    # Spot-check that name column decoded correctly (non-empty string).
    assert isinstance(rows[0][1], str) and len(rows[0][1]) > 0


# ---------- Case 8: Fetch does not block event loop ------------------------


def test_event_loop_progresses_during_fetch_async(conn_str, _async_capable):
    """A background heartbeat must keep ticking during a large ``fetch_async``.

    Symmetric to test_event_loop_progresses_during_execute_async but for
    the fetch path. The fetch runs in an executor thread with the GIL
    released, so the event loop should stay live.

    Uses ``HB_FETCH_ROWS`` (default 50000) rather than ``LARGE_ROWS`` so the
    fetch is long enough (multi-hundred ms typical) for the 10 ms heartbeat
    to tick a meaningful number of times.
    """

    async def _run():
        conn = connect(conn_str)
        heartbeats = 0

        async def heartbeat():
            nonlocal heartbeats
            while True:
                await asyncio.sleep(0.01)
                heartbeats += 1

        try:
            cur = conn.cursor()
            try:
                # Prep the result set synchronously (fast). The interesting
                # timing is on fetch, not execute.
                await cur.execute_async(
                    f"SELECT TOP {HB_FETCH_ROWS} "
                    f"ROW_NUMBER() OVER (ORDER BY a.object_id) AS n, "
                    f"CAST(a.name AS NVARCHAR(128)) AS obj_name "
                    f"FROM sys.all_objects a CROSS JOIN sys.all_objects b"
                )
                hb_task = asyncio.create_task(heartbeat())
                fetch_start = time.perf_counter()
                rows = await cur.fetch_async(-1)
                fetch_elapsed = time.perf_counter() - fetch_start
                hb_task.cancel()
                try:
                    await hb_task
                except asyncio.CancelledError:
                    pass
                return rows, heartbeats, fetch_elapsed
            finally:
                cur.close()
        finally:
            conn.close()

    rows, heartbeats, fetch_elapsed = asyncio.run(_run())
    assert len(rows) == HB_FETCH_ROWS
    # If the fetch completes in less than ~50 ms the test is inconclusive
    # (the heartbeat's 10 ms timer may not have fired even once even in an
    # ideal system). Skip the tick-count assertion in that regime — a
    # sub-50 ms fetch on a local SQL Server means the event loop wasn't
    # blocked long enough to matter either way.
    if fetch_elapsed < 0.05:
        pytest.skip(
            f"fetch_async completed too fast ({fetch_elapsed*1000:.0f}ms) to "
            f"meaningfully measure event-loop responsiveness — increase "
            f"ASYNC_TEST_HB_FETCH_ROWS on faster hardware"
        )
    # Require at least ~1 tick per 30 ms of fetch (very loose to tolerate CI
    # jitter). If the event loop were fully blocked we'd expect 0 ticks.
    min_expected = max(1, int(fetch_elapsed * 30))
    assert heartbeats >= min_expected, (
        f"event loop blocked during fetch_async: {heartbeats} ticks in "
        f"{fetch_elapsed*1000:.0f}ms (>= {min_expected} required)"
    )
    print(
        f"\n[async POC] heartbeat during {HB_FETCH_ROWS}-row fetch_async "
        f"({fetch_elapsed*1000:.0f}ms): {heartbeats} ticks"
    )


# ---------- Case 9: Multiple concurrent execute_async operations ----------


def test_multiple_concurrent_execute_async_small_batch(conn_str, _async_capable):
    """A small variant of the connection-per-task pattern (N=10).

    Complementary to the 100-task test — kept small so it's included in
    quick smoke runs. Each task uses its own connection; concurrent
    execute on cursors sharing a DBC is intentionally NOT tested here
    because it hits the ODBC no-MARS restriction (see block comment on
    test_100_concurrent_async_selects_on_single_mars_connection).
    """
    N = 10

    async def _one(idx):
        conn = connect(conn_str)
        try:
            cur = conn.cursor()
            try:
                await cur.execute_async("SELECT ? AS v", idx * 10)
                row = await cur.fetch_async()
                return row[0]
            finally:
                cur.close()
        finally:
            conn.close()

    async def _run():
        return await asyncio.gather(*[_one(i) for i in range(N)])

    results = asyncio.run(_run())
    assert sorted(results) == [i * 10 for i in range(N)]


# ---------- Case 10: Execute async then fetch async -----------------------


def test_execute_async_followed_by_fetch_async(conn_str, _async_capable):
    """Verify the natural ``execute_async`` → ``fetch_async`` sequence for
    each of the three ``fetch_async`` modes on the SAME cursor.

    Distinct from ``test_single_execute_async_and_fetch_async`` (which
    exercises only the single-row mode) by covering all of size=None,
    size=positive, and size=-1 on the SAME cursor after independent
    executes.
    """

    async def _run():
        conn = connect(conn_str)
        try:
            cur = conn.cursor()
            try:
                # Mode 1: size=None → single Row
                await cur.execute_async("SELECT 100 AS v")
                r = await cur.fetch_async()
                assert r is not None and r[0] == 100

                # Mode 2: size=positive → List[Row]
                await cur.execute_async(
                    "SELECT * FROM (VALUES (1),(2),(3)) AS t(n)"
                )
                rows = await cur.fetch_async(3)
                assert [x[0] for x in rows] == [1, 2, 3]

                # Mode 3: size=-1 → List[Row] (all)
                await cur.execute_async(
                    "SELECT * FROM (VALUES ('a'),('b'),('c'),('d')) AS t(s)"
                )
                rows = await cur.fetch_async(-1)
                assert [x[0] for x in rows] == ["a", "b", "c", "d"]
            finally:
                cur.close()
        finally:
            conn.close()

    asyncio.run(_run())


# ---------- Case 11: Multiple concurrent fetch_async ----------------------


def test_multiple_concurrent_fetch_async_across_connections(conn_str, _async_capable):
    """N connections, each with a pre-executed statement, then
    ``fetch_async`` fired concurrently across all of them.

    Complements the execute-side concurrency tests: this exercises the
    fetch code path (DDBCSQLFetchOneAsync) under concurrent gather. Uses
    one connection per task so each fetch operates on its own DBC (safe
    per the root-cause investigation).
    """
    N = 20

    async def _worker(idx):
        conn = connect(conn_str)
        cur = None
        try:
            cur = conn.cursor()
            # Execute first (sequentially per task), then fetch in the
            # concurrent phase below.
            await cur.execute_async("SELECT ? AS v", idx * 7)
            return cur, conn  # keep alive for the fetch phase
        except Exception:
            if cur is not None:
                cur.close()
            conn.close()
            raise

    async def _run():
        # Phase 1: prepare N cursors with statements ready to fetch.
        prep = await asyncio.gather(*[_worker(i) for i in range(N)])
        # prep is [(cur, conn), ...]
        try:
            # Phase 2: concurrent fetch_async across all N.
            rows = await asyncio.gather(*[c.fetch_async() for c, _ in prep])
            return [r[0] for r in rows]
        finally:
            for c, conn in prep:
                c.close()
                conn.close()

    results = asyncio.run(_run())
    assert sorted(results) == [i * 7 for i in range(N)]


# ---------- Case 12: Sequential execute_async calls -----------------------


def test_sequential_execute_async_on_same_cursor(conn_str, _async_capable):
    """Run multiple ``execute_async`` calls back-to-back on the SAME cursor.

    Verifies that the ``AsyncEnableGuard`` correctly toggles
    ``SQL_ATTR_ASYNC_ENABLE`` OFF at the end of each call, so subsequent
    async executes on the same HSTMT are not affected by leftover state.
    Also verifies that ``_finalize_execute_async`` correctly resets cursor
    metadata (description, rowcount, column cache) between calls.
    """

    async def _run():
        conn = connect(conn_str)
        try:
            cur = conn.cursor()
            try:
                for i in range(5):
                    await cur.execute_async("SELECT ? AS v", i)
                    row = await cur.fetch_async()
                    assert row is not None
                    assert row[0] == i, f"call {i}: got {row[0]}"
                    # Second fetch_async on the exhausted result set
                    # returns None (consistent with sync fetchone semantics).
                    assert await cur.fetch_async() is None
            finally:
                cur.close()
        finally:
            conn.close()

    asyncio.run(_run())


# ============================================================================
# MARS stability test (multiple iterations, high concurrency)
# ============================================================================
# Complements test_100_concurrent_async_selects_on_single_mars_connection
# (a single-run correctness test) with a stability test designed to catch
# intermittent races, leaks, or state corruption that a single-run test
# might miss.
#
# Differences from the correctness test:
#   * NO semaphore — all N statements truly in flight at once, not bounded
#     to 16.
#   * ITERATIONS repetitions of the workload reuse the SAME MARS connection
#     across rounds, catching leaks / state corruption that only manifest
#     after N runs (e.g. an HSTMT counter that overflows, an internal MARS
#     session table that isn't reclaimed, or a slow diagnostic-record leak).
#
# Note on MARS + parallelism: MARS multiplexes multiple result sets over
# ONE TCP socket, but SQL Server assigns a single server-side worker
# thread per session (SPID). So 100 statements on one MARS connection do
# NOT execute in true parallel server-side — they queue on the shared
# session's worker. Real network-level parallelism requires N connections
# (see test_100_concurrent_async_selects). This test therefore stresses
# CLIENT-side concurrency (100 in-flight coroutines, executor threads
# calling into the MARS driver simultaneously) rather than server-side
# throughput.
#
# Marked @pytest.mark.stress (excluded from the default pytest run per
# pytest.ini).
# ============================================================================

MARS_STABILITY_N = int(os.getenv("ASYNC_TEST_MARS_STABILITY_N", "100"))
MARS_STABILITY_ITERS = int(os.getenv("ASYNC_TEST_MARS_STABILITY_ITERS", "10"))


@pytest.mark.stress
def test_mars_stability_100_cursors_high_concurrency_multi_iteration(
    conn_str, _async_capable
):
    """Stability: N cursors on ONE MARS connection, all truly concurrent,
    repeated across ITERATIONS rounds on the SAME connection.

    Each iteration:
      1. Opens ``N`` cursors on the (single, long-lived) MARS connection.
      2. Fires all ``N`` execute_async + fetch_async pairs through a single
         ``asyncio.gather`` with NO semaphore (all N in flight at once).
      3. Verifies every task returned its own (idx, iteration) pair — proves
         no cross-wiring across the ``ITERATIONS × N`` combined ops, and no
         leftover state from prior iterations.
      4. Closes all N cursors before the next iteration starts.

    Correctness AND stability are both asserted: the connection must remain
    usable across all ITERATIONS rounds (a per-iteration leak or state
    corruption would show up as a failure in later rounds).
    """
    mars_conn_str = _with_mars(conn_str)
    if not mars_conn_str:
        pytest.skip("DB_CONNECTION_STRING explicitly disables MARS")

    async def _one_iteration(iteration: int, conn):
        cursors = [conn.cursor() for _ in range(MARS_STABILITY_N)]
        try:
            async def one(i):
                await cursors[i].execute_async(
                    "SELECT ? AS idx, ? AS iter", i, iteration
                )
                row = await cursors[i].fetch_async()
                return (row[0], row[1])

            # NO semaphore — all N truly in flight simultaneously. The
            # asyncio default ThreadPoolExecutor caps the actual number of
            # concurrent OS threads calling into the ODBC driver, so this
            # is bounded in practice regardless of N.
            return await asyncio.gather(*[one(i) for i in range(MARS_STABILITY_N)])
        finally:
            for c in cursors:
                c.close()

    async def _run():
        conn = connect(mars_conn_str)
        try:
            start = time.perf_counter()
            all_results = []
            for it in range(MARS_STABILITY_ITERS):
                iter_start = time.perf_counter()
                iter_results = await _one_iteration(it, conn)
                all_results.append((it, iter_results, time.perf_counter() - iter_start))
            return all_results, time.perf_counter() - start
        finally:
            conn.close()

    all_results, total_elapsed = asyncio.run(_run())

    # Correctness: every iteration returned N rows, each carrying its own
    # (idx, iteration) tuple. Cross-wiring or corruption would break this.
    for it, iter_results, _iter_elapsed in all_results:
        assert len(iter_results) == MARS_STABILITY_N, (
            f"iter {it}: got {len(iter_results)} results, "
            f"expected {MARS_STABILITY_N}"
        )
        got_indices = sorted(r[0] for r in iter_results)
        assert got_indices == list(range(MARS_STABILITY_N)), (
            f"iter {it}: missing / duplicate indices — cross-wiring bug? "
            f"got unique={len(set(got_indices))}"
        )
        for idx, iter_val in iter_results:
            assert iter_val == it, (
                f"iter {it}, idx {idx}: got iter_val={iter_val} — "
                f"cross-iteration cross-wiring"
            )

    # Loose sanity bound to catch runaway hangs. MARS serializes statements
    # server-side per session, so we expect the total to scale with
    # ITERATIONS × N × per-query overhead. On local Docker this is a few
    # ms per query; on production networks add round-trip latency. Very
    # generous upper bound: 5 minutes total.
    assert total_elapsed < 300, (
        f"MARS stability test took {total_elapsed:.1f}s (>300s) — "
        f"probable hang or catastrophic serialization"
    )

    per_iter = [f"{elapsed*1000:.0f}ms" for _, _, elapsed in all_results]
    print(
        f"\n[async POC MARS stability] {MARS_STABILITY_ITERS} iters × "
        f"{MARS_STABILITY_N} concurrent cursors on 1 MARS conn "
        f"completed in {total_elapsed:.2f}s. Per-iter: {per_iter}"
    )
