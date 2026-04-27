"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.

Functional tests for the statement-, fetch- and transaction-level GIL
release added in PR #541 (covering ``SQLExecute`` / ``SQLExecDirect`` /
``SQLFetch`` / ``SQLEndTran`` in ``mssql_python/pybind/ddbc_bindings.cpp``
and ``mssql_python/pybind/connection/connection.cpp``).

These are **not** performance/stress tests — they assert a binary
correctness property (the GIL must be released around blocking ODBC calls)
using a conservative threshold that doesn't depend on hardware speed:

* with the GIL released, a Python heartbeat thread keeps ticking while
  another thread sits in ``cursor.execute("WAITFOR DELAY '00:00:02'")``
  — without release the heartbeat is fully starved.
* same property holds across an explicit ``commit()`` (covers the
  ``SQLEndTran`` GIL-release path).

A wall-clock "N threads finish in ~one WAITFOR worth of time" assertion
was deliberately *not* added here — it depends on the SQL Server
scheduler/container CPU allocation and is too flaky for the functional
suite. That style of test lives in ``test_021_concurrent_connection_perf.py``
under ``@pytest.mark.stress``.

A 2-second server-side WAITFOR is short enough to keep these in the
default functional suite (~5s total) while still producing an unambiguous
signal that survives normal CI jitter.
"""

import os
import time
import threading

import pytest
import mssql_python
from mssql_python import connect


WAITFOR_SECONDS = 2
WAITFOR_SQL = f"WAITFOR DELAY '00:00:0{WAITFOR_SECONDS}'"


@pytest.fixture(scope="module")
def conn_str():
    """Get connection string from environment."""
    conn_str = os.getenv("DB_CONNECTION_STRING")
    if not conn_str:
        pytest.skip("DB_CONNECTION_STRING environment variable not set")
    return conn_str


def _run_waitfor(conn_str: str) -> float:
    """Open a fresh connection, run WAITFOR, return elapsed seconds."""
    conn = connect(conn_str)
    try:
        cursor = conn.cursor()
        try:
            start = time.perf_counter()
            cursor.execute(WAITFOR_SQL)
            return time.perf_counter() - start
        finally:
            cursor.close()
    finally:
        conn.close()


# ============================================================================
# Heartbeat: a Python thread must keep running while another thread blocks
# inside a server-side WAITFOR. This is the canonical repro from PR #541.
# ============================================================================


def test_query_does_not_block_other_python_threads(conn_str):
    """
    While one thread executes a 2-second ``WAITFOR DELAY``, a second pure-Python
    thread must continue to run. If the GIL were held across SQLExecDirect, the
    heartbeat would not advance until the WAITFOR returned.
    """
    mssql_python.pooling(enabled=False)

    heartbeat_interval = 0.05  # 50ms ticks
    expected_min_ticks = int(WAITFOR_SECONDS / heartbeat_interval * 0.5)  # 50% of theoretical max

    stop_event = threading.Event()
    tick_count = [0]
    query_error = []

    def heartbeat():
        while not stop_event.is_set():
            tick_count[0] += 1
            time.sleep(heartbeat_interval)

    def run_query():
        try:
            _run_waitfor(conn_str)
        except Exception as exc:
            query_error.append(str(exc))

    hb = threading.Thread(target=heartbeat, daemon=True)
    qt = threading.Thread(target=run_query, daemon=True)

    # Snapshot ticks just before/after the query so we measure ticks that
    # happened *during* the blocking ODBC call, not before/after.
    hb.start()
    time.sleep(0.1)  # let heartbeat warm up
    ticks_before = tick_count[0]

    qt.start()
    qt.join(timeout=WAITFOR_SECONDS + 30)
    ticks_after = tick_count[0]
    stop_event.set()
    hb.join(timeout=5)

    assert not qt.is_alive(), "Query thread did not finish in time"
    assert not query_error, f"Query thread error: {query_error}"

    ticks_during = ticks_after - ticks_before
    print(
        f"\n[HEARTBEAT] ticks during {WAITFOR_SECONDS}s WAITFOR: {ticks_during} "
        f"(expected >= {expected_min_ticks})"
    )

    assert ticks_during >= expected_min_ticks, (
        f"Heartbeat thread was starved during cursor.execute(WAITFOR). "
        f"Got {ticks_during} ticks, expected >= {expected_min_ticks}. "
        f"This indicates the GIL was not released around the blocking ODBC call."
    )


# ============================================================================
# Transaction: SQLEndTran (commit/rollback) is also wrapped in PR #541. Make
# sure a heartbeat can run while a long server-side WAITFOR holds an open
# transaction that is then committed.
# ============================================================================


def test_commit_does_not_block_other_python_threads(conn_str):
    """
    Smoke test for the SQLEndTran GIL-release added to ``Connection::commit``
    and ``Connection::rollback``. A heartbeat must keep ticking across an
    explicit commit on a connection that just executed a (short) WAITFOR.

    SQLEndTran on a localhost connection is typically sub-millisecond, so we
    can't reliably measure starvation from it alone. Instead we just assert
    that the commit completes and the heartbeat made meaningful progress
    over the whole transaction, including the WAITFOR.
    """
    mssql_python.pooling(enabled=False)

    heartbeat_interval = 0.05
    stop_event = threading.Event()
    tick_count = [0]
    txn_error = []

    def heartbeat():
        while not stop_event.is_set():
            tick_count[0] += 1
            time.sleep(heartbeat_interval)

    def run_txn():
        try:
            conn = connect(conn_str)
            try:
                cursor = conn.cursor()
                try:
                    cursor.execute(WAITFOR_SQL)
                finally:
                    cursor.close()
                conn.commit()
            finally:
                conn.close()
        except Exception as exc:
            txn_error.append(str(exc))

    hb = threading.Thread(target=heartbeat, daemon=True)
    tt = threading.Thread(target=run_txn, daemon=True)

    hb.start()
    time.sleep(0.1)
    ticks_before = tick_count[0]

    tt.start()
    tt.join(timeout=WAITFOR_SECONDS + 30)
    ticks_after = tick_count[0]
    stop_event.set()
    hb.join(timeout=5)

    assert not tt.is_alive(), "Transaction thread did not finish in time"
    assert not txn_error, f"Transaction thread error: {txn_error}"

    ticks_during = ticks_after - ticks_before
    expected_min_ticks = int(WAITFOR_SECONDS / heartbeat_interval * 0.5)
    print(
        f"\n[HEARTBEAT] ticks during WAITFOR+commit: {ticks_during} "
        f"(expected >= {expected_min_ticks})"
    )
    assert ticks_during >= expected_min_ticks, (
        f"Heartbeat thread was starved across cursor.execute+commit. "
        f"Got {ticks_during} ticks, expected >= {expected_min_ticks}."
    )
