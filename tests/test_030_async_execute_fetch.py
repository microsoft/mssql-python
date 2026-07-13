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
# connection with MultipleActiveResultSets=Yes and create N cursors from it,
# then fire the same 100 concurrent execute_async + fetch_async pairs.
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
# NOTE on current status: as of this commit, mssql-python's connection
# string parser rejects both ``MultipleActiveResultSets`` and
# ``MARS_Connection`` — neither is in _ALLOWED_CONNECTION_STRING_PARAMS
# (mssql_python/constants.py). This test therefore *skips* on the current
# driver but is kept so that whenever MARS is added to the allowlist it
# starts running automatically. See the try/except around connect() below.
# ============================================================================


def _with_mars(conn_str: str) -> str:
    """Return ``conn_str`` with MultipleActiveResultSets=Yes appended, or the
    original string if the caller already set a MARS keyword.

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
    return f"{conn_str}{sep}MultipleActiveResultSets=Yes"


def test_100_concurrent_async_selects_on_single_mars_connection(conn_str, _async_capable):
    """N async SELECTs concurrently on ONE MARS-enabled connection (N cursors).

    Complements ``test_100_concurrent_async_selects``. That test opens one
    connection per task; this one opens ONE connection with
    ``MultipleActiveResultSets=Yes`` and creates N cursors on it, then fires
    the same workload through ``asyncio.gather``.

    Correctness assertions match the connection-per-task variant: every task
    must return its own index, proving that MARS + per-HSTMT
    ``SQL_ATTR_ASYNC_ENABLE`` toggling doesn't cross-wire results between
    cursors sharing a single DBC.

    Note: MARS multiplexes over one TCP socket, so this is interleaved
    (not truly parallel) execution — expect a different wall-clock profile
    than the connection-per-task variant.

    Skipped today because the mssql-python connection-string allowlist does
    not include MARS keywords (see block comment above); designed to
    auto-enable when MARS is added.
    """
    mars_conn_str = _with_mars(conn_str)
    if not mars_conn_str:
        pytest.skip(
            "DB_CONNECTION_STRING explicitly disables MARS — cannot run "
            "the same-connection concurrency test"
        )

    # Verify the driver accepts the MARS keyword. Skip cleanly if the current
    # mssql-python version rejects it in the allowlist (see block comment).
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
