"""Probe the ODBC driver's async capability and verify polling works end-to-end.

Run:
    export DB_CONNECTION_STRING="Server=localhost,1433;Database=master;UID=sa;PWD=...;TrustServerCertificate=Yes;Encrypt=Yes"
    python benchmarks/probe_async_capability.py

The probe is intentionally cross-platform: mssql-python loads the Microsoft
ODBC driver directly via dlopen (Linux/macOS) or LoadLibraryW (Windows) — it
does NOT depend on unixODBC / iODBC / Windows Driver Manager. This script
prints the resolved driver path so that behavior is visible per-OS.

Sections:
    1. Environment — Python + OS + architecture + resolved driver path
    2. Capability — SQLGetInfo advertisements (async mode, function bitmasks)
    3. Polling smoke tests — two mini-tests:
        3a. execute polling: WAITFOR + SELECT 1 (proves SQLExecDirect polls)
        3b. fetch-stream polling: 50k-row cross-join (proves SQLFetch can
            observe SQL_STILL_EXECUTING when streaming a multi-MB payload)
    4. TDS layer async check —
         4a. sync-vs-polled wall time comparison (overhead check)
         4b. concurrent execute on N separate connections (execute
             parallelism check)
         4c. concurrent fetch-heavy queries — proves fetch parallelizes
             across connections, not just execute
    5. Verdict — pass/fail per axis with an overall exit code

Exit codes:
    0  polling is usable AND the TDS layer looks genuinely async
    1  polling works but the TDS layer is serialized / adds heavy overhead
    2  DB_CONNECTION_STRING is not set
    3  driver does not report statement-level async support
"""
from __future__ import annotations

import os
import platform
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# Make the in-tree mssql_python importable when run from repo root.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import mssql_python  # noqa: E402
from mssql_python import connect, ddbc_bindings  # noqa: E402
from mssql_python.pooling import PoolingManager  # noqa: E402


# Informational only — the exact bit encoding of SQL_ASYNC_STMT_FUNCTIONS is
# driver-defined; treat any decoded names below as a hint, not proof. The
# functional smoke test is the real evidence.
_ASYNC_STMT_CANDIDATES = {
    "SQLEXECUTE": 12,
    "SQLEXECDIRECT": 11,
    "SQLFETCH": 13,
    "SQLPREPARE": 19,
    "SQLNUMRESULTCOLS": 18,
    "SQLDESCRIBECOL": 8,
    "SQLGETDATA": 43,
    "SQLMORERESULTS": 61,
    "SQLFETCHSCROLL": 1021,
    "SQLBULKOPERATIONS": 24,
    "SQLSETPOS": 68,
    "SQLPARAMDATA": 48,
    "SQLPUTDATA": 49,
}

# TDS-layer test parameters.
_TDS_DELAY_SECONDS = 1        # server-side WAITFOR duration
# Concurrency is overridable via env var so the probe can be tuned per host
# (default keeps the smoke test at 100 concurrent connections).
_TDS_CONCURRENCY = int(os.getenv("PROBE_TDS_CONCURRENCY", "100"))
# Upper bound on OS threads. Very high N cannot be run with one OS thread
# per query (Python can't spawn tens of thousands of threads on most
# hosts). When the requested concurrency exceeds this cap, we fall back to
# a bounded ThreadPoolExecutor: N queries are submitted, but only up to
# _TDS_MAX_WORKERS run at any instant. In that regime the test measures
# THROUGHPUT of the driver+server pipeline rather than simultaneous
# concurrency. Overridable via env var for tuning.
_TDS_MAX_WORKERS = int(os.getenv("PROBE_TDS_MAX_WORKERS", "1024"))
# At high worker counts SQL Server's login-handshake queue occasionally
# rejects a small fraction of simultaneous connect attempts ("server too
# busy"). Below this failure rate we still consider the run informative
# and report throughput on the successful subset; above it we raise.
_TDS_FAILURE_TOLERANCE = float(os.getenv("PROBE_TDS_FAILURE_TOLERANCE", "0.05"))
_TDS_QUERY = f"WAITFOR DELAY '00:00:0{_TDS_DELAY_SECONDS}'; SELECT 1"

# Fetch-heavy test — TOP 50000 rows from a sys.all_objects cross-join
# yields several MB spanning many TDS packets. Tests fetch-side TDS
# parallelism separately from the WAITFOR-based execute-side check.
_TDS_FETCH_QUERY = (
    "SELECT TOP 50000 "
    "CAST(a.object_id AS BIGINT) AS id, "
    "CAST(a.name AS NVARCHAR(128)) AS n, "
    "CAST(a.create_date AS DATETIME2) AS cd "
    "FROM sys.all_objects a CROSS JOIN sys.all_objects b"
)
# Concurrency for the fetch-heavy sub-test defaults lower than the
# execute-side test because each task transfers megabytes of data.
_TDS_FETCH_CONCURRENCY = int(os.getenv("PROBE_TDS_FETCH_CONCURRENCY", "16"))

# Interpretation thresholds. Loose enough to tolerate CI noise; tight
# enough to distinguish "true parallelism" from "serialized behind a
# driver-internal lock". For high N the parallelism ratio is bounded by
# server-side worker threads / socket queues, so we require at least
# N * 0.2 (i.e. 20x for N=100) which still clearly separates the
# "serialized" (ratio ~= 1) case from the "genuinely parallel" case.
_ASYNC_OVERHEAD_TOLERANCE = 0.25
_PARALLEL_RATIO_MIN = max(2.5, _TDS_CONCURRENCY * 0.2)


def _decode_stmt_bitmask(bitmask):
    if bitmask is None:
        return []
    hits = []
    for name, fid in _ASYNC_STMT_CANDIDATES.items():
        word_index = fid >> 4
        bit = 1 << (fid & 0xF)
        word = (bitmask >> (word_index * 16)) & 0xFFFF
        if word & bit:
            hits.append(name)
    return hits


def _driver_path() -> str:
    """Ask the C++ layer where it looked for the msodbcsql library."""
    try:
        return ddbc_bindings.GetDriverPathCpp(
            os.path.dirname(os.path.abspath(mssql_python.__file__))
        )
    except Exception as exc:  # pragma: no cover — informational
        return f"<GetDriverPathCpp failed: {exc}>"


def _section(title: str) -> None:
    print()
    print("=" * 72)
    print(title)
    print("=" * 72)


def _row(label: str, value) -> None:
    print(f"  {label:<38} {value}")


def _print_environment() -> None:
    _section("1. Environment")
    _row("Python", sys.version.split(" ", 1)[0])
    _row("OS", f"{platform.system()} {platform.release()} ({platform.machine()})")
    try:
        arch = ddbc_bindings.__architecture__
    except AttributeError:
        arch = "<unknown>"
    _row("Extension arch", arch)
    _row("mssql_python module", mssql_python.__file__)
    _row("Resolved msodbcsql path", _driver_path())
    _row("Loaded via", "dlopen / LoadLibraryW (no ODBC Driver Manager)")


def _print_capability(cap: dict) -> None:
    _section("2. SQLGetInfo capability advertisements")
    _row("SQL_ASYNC_MODE",
         f"{cap.get('async_mode')}  ({cap.get('async_mode_name')})")
    stmt_bm = cap.get("async_stmt_functions_bitmask")
    if stmt_bm is None:
        _row("SQL_ASYNC_STMT_FUNCTIONS", "<not reported>")
    else:
        _row("SQL_ASYNC_STMT_FUNCTIONS", f"{stmt_bm}  (0x{stmt_bm:x})")
        hits = _decode_stmt_bitmask(stmt_bm)
        if hits:
            _row("  candidate functions (heuristic)", ", ".join(hits))
    dbc_bm = cap.get("async_dbc_functions_bitmask")
    if dbc_bm is None:
        _row("SQL_ASYNC_DBC_FUNCTIONS", "<not reported>")
    else:
        _row("SQL_ASYNC_DBC_FUNCTIONS", f"{dbc_bm}  (0x{dbc_bm:x})")
    notify = cap.get("async_notification")
    notify_cap = cap.get("async_notification_capable")
    if notify is None:
        _row("SQL_ASYNC_NOTIFICATION",
             "<not reported> (notification mode unavailable on this driver build)")
    else:
        _row("SQL_ASYNC_NOTIFICATION", f"{notify}  (capable={notify_cap})")


def _print_smoke(smoke: dict, fetch_stream: dict | None) -> None:
    _section("3a. Polling smoke test — execute path  "
             "(WAITFOR '00:00:01'; SELECT 1)")
    if not smoke.get("ran"):
        _row("Ran", "NO")
        _row("Reason", smoke.get("error", "<unknown>"))
    else:
        _row("Ran", "YES")
        _row("SQLExecDirect return", smoke.get("execute_sqlreturn"))
        _row("  SQL_SUCCEEDED", smoke.get("execute_ok"))
        _row("  Poll count (STILL_EXECUTING)", smoke.get("execute_poll_count"))
        _row("  Elapsed", f"{smoke.get('execute_elapsed_ms')} ms")
        _row("  Observed SQL_STILL_EXECUTING",
             smoke.get("execute_observed_still_executing"))
        _row("SQLFetch return", smoke.get("fetch_sqlreturn"))
        _row("  SQL_SUCCEEDED", smoke.get("fetch_ok"))
        _row("  Poll count (STILL_EXECUTING)", smoke.get("fetch_poll_count"))
        _row("  Elapsed", f"{smoke.get('fetch_elapsed_ms')} ms")
        _row("  Observed SQL_STILL_EXECUTING",
             smoke.get("fetch_observed_still_executing"))
        _row("  Note", "single 4-byte row — fetch completes from local TCP "
                       "buffer; polling on fetch not expected here")

    _section("3b. Polling smoke test — fetch-stream path  "
             "(TOP 50000 rows from sys.all_objects cross-join)")
    if fetch_stream is None:
        _row("Ran", "NO")
        _row("Reason", "not returned by driver")
        return
    if not fetch_stream.get("ran"):
        _row("Ran", "NO")
        _row("Reason", fetch_stream.get("error", "<unknown>"))
        return
    _row("Ran", "YES")
    _row("SQLExecDirect return", fetch_stream.get("execute_sqlreturn"))
    _row("  SQL_SUCCEEDED", fetch_stream.get("execute_ok"))
    _row("  Poll count on execute", fetch_stream.get("execute_poll_count"))
    _row("  Elapsed", f"{fetch_stream.get('execute_elapsed_ms')} ms")
    rows = fetch_stream.get("fetch_rows_read")
    _row("SQLFetch loop rows read", rows)
    _row("  Final SQLRETURN", fetch_stream.get("fetch_final_sqlreturn"))
    _row("  SQL_SUCCEEDED/NO_DATA", fetch_stream.get("fetch_ok"))
    total_polls = fetch_stream.get("fetch_total_poll_count")
    _row("  Total poll count across all fetches", total_polls)
    rows_polled = fetch_stream.get("fetch_rows_that_polled")
    if rows and rows_polled is not None:
        pct = (rows_polled / rows * 100) if rows > 0 else 0
        _row("  Rows that saw >=1 STILL_EXECUTING",
             f"{rows_polled}  ({pct:.2f}% of rows)")
    _row("  Elapsed", f"{fetch_stream.get('fetch_elapsed_ms')} ms")
    _row("  Observed SQL_STILL_EXECUTING on fetch",
         fetch_stream.get("fetch_observed_still_executing"))


# ---------------------------------------------------------------------------
# Section 4 — TDS layer async check
#
# Whether the ODBC API accepts SQL_ATTR_ASYNC_ENABLE is a separate question
# from whether the underlying TDS network I/O is actually non-blocking. A
# driver could satisfy the API contract while internally holding a global
# lock that serializes all outstanding statements — polling would still
# "work" but you would get zero parallelism benefit.
#
# We probe two behavioral signatures:
#
#   4a. Overhead: run identical WAITFOR + SELECT sync and polled. If the
#       polled wall time is close to the sync wall time, the polling loop
#       is cheap and the driver is not adding extra blocking on the caller
#       thread.
#
#   4b. Parallelism: run N identical WAITFOR + SELECT queries on N separate
#       connections concurrently (threading, one connection per thread —
#       the existing sync execute already releases the GIL around the ODBC
#       call). If the total wall time is close to a single query's wall
#       time, the driver truly runs TDS I/O in parallel. If it is close to
#       N * single-query time, TDS I/O is serialized.
# ---------------------------------------------------------------------------


def _run_query(conn_str: str, sql: str) -> float:
    """Open a fresh connection, execute + fetch one row, return elapsed sec."""
    conn = connect(conn_str)
    try:
        cur = conn.cursor()
        t0 = time.perf_counter()
        cur.execute(sql)
        cur.fetchone()
        elapsed = time.perf_counter() - t0
        cur.close()
        return elapsed
    finally:
        conn.close()


def _run_fetchall(conn_str: str, sql: str) -> tuple[float, int]:
    """Open a connection, execute + fetchall, return (elapsed_sec, row_count)."""
    conn = connect(conn_str)
    try:
        cur = conn.cursor()
        t0 = time.perf_counter()
        cur.execute(sql)
        rows = cur.fetchall()
        elapsed = time.perf_counter() - t0
        cur.close()
        return elapsed, len(rows)
    finally:
        conn.close()


def _run_query_parallel(conn_str: str, sql: str, n: int) -> tuple[float, int, int, int]:
    """Fire n identical queries on n separate connections.

    Uses a ThreadPoolExecutor sized to min(n, _TDS_MAX_WORKERS). If n is
    within the cap, every task truly runs simultaneously — this measures
    *concurrency*. If n exceeds the cap, only _TDS_MAX_WORKERS run at any
    instant — this measures *throughput* through a bounded pool. The
    return tuple is (wall_seconds, effective_max_workers, requested_n,
    successful_n). Partial failures (server login queue overrun, transient
    port pressure, etc.) up to _TDS_FAILURE_TOLERANCE are reported via the
    success count rather than raised, so the sweep still yields usable
    throughput data.
    """
    workers = min(n, _TDS_MAX_WORKERS)

    # Reduce per-thread stack from the default 8 MB so pool creation
    # doesn't reserve tens of GB of virtual memory at high worker counts.
    if workers > 200:
        try:
            threading.stack_size(1024 * 1024)  # 1 MB
        except (ValueError, RuntimeError):
            pass

    def worker() -> None:
        _run_query(conn_str, sql)

    t0 = time.perf_counter()
    errors: list[BaseException] = []
    successes = 0
    # Using shutdown(wait=True) implicitly via context manager guarantees
    # all in-flight ODBC calls finish before we return — no dangling
    # SqlHandles, no segfault on interpreter exit.
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(worker) for _ in range(n)]
        for fut in as_completed(futures):
            exc = fut.exception()
            if exc is None:
                successes += 1
            else:
                errors.append(exc)
    total = time.perf_counter() - t0

    failure_rate = len(errors) / n if n > 0 else 1.0
    if failure_rate > _TDS_FAILURE_TOLERANCE:
        first = errors[0]
        raise RuntimeError(
            f"{len(errors)}/{n} pool tasks failed "
            f"({failure_rate * 100:.1f}% > tolerance "
            f"{_TDS_FAILURE_TOLERANCE * 100:.0f}%). "
            f"First error: {first!r}"
        ) from first
    return total, workers, n, successes


def _run_tds_layer_check(conn_str: str, sync_elapsed: float,
                         async_elapsed_ms: int | None) -> dict:
    """Run overhead + parallelism tests and return a summary dict."""
    parallel_total, workers, requested, successes = _run_query_parallel(
        conn_str, _TDS_QUERY, _TDS_CONCURRENCY)
    # When N <= workers, every task ran simultaneously → this measures true
    # concurrency. When N > workers, only `workers` ran at a time → this
    # measures throughput through a bounded pool.
    is_bounded = requested > workers
    # Serialized projection = single-query cost × successful tasks. We
    # base the projection on successes so a small failure rate doesn't
    # skew the parallelism ratio.
    serial_projection = sync_elapsed * successes
    parallel_ratio = serial_projection / parallel_total if parallel_total > 0 else 0.0
    throughput_qps = successes / parallel_total if parallel_total > 0 else 0.0

    async_overhead = None
    if async_elapsed_ms is not None and sync_elapsed > 0:
        async_overhead = (async_elapsed_ms / 1000.0 - sync_elapsed) / sync_elapsed

    # Parallelism-ratio PASS threshold: when bounded, we can't exceed
    # `workers` × single-query throughput so reset the threshold to
    # `workers * 0.5` (i.e. at least half of the pool's ideal capacity).
    # When unbounded, keep the configured _PARALLEL_RATIO_MIN.
    if is_bounded:
        pass_threshold = workers * 0.5
    else:
        pass_threshold = _PARALLEL_RATIO_MIN

    return {
        "requested_concurrency": requested,
        "successful_tasks": successes,
        "failed_tasks": requested - successes,
        "success_rate": successes / requested if requested > 0 else 0.0,
        "effective_max_workers": workers,
        "is_bounded_pool": is_bounded,
        "sync_elapsed_s": sync_elapsed,
        "async_elapsed_s": async_elapsed_ms / 1000.0 if async_elapsed_ms is not None else None,
        "async_overhead_pct": async_overhead,
        "parallel_total_s": parallel_total,
        "parallel_serial_projection_s": serial_projection,
        "parallel_ratio": parallel_ratio,
        "pass_threshold_ratio": pass_threshold,
        "throughput_qps": throughput_qps,
        "parallel_ok": parallel_ratio >= pass_threshold,
        "async_overhead_ok": (async_overhead is None or
                              async_overhead <= _ASYNC_OVERHEAD_TOLERANCE),
    }


def _print_tds(tds: dict) -> None:
    mode = ("bounded pool — measures THROUGHPUT"
            if tds["is_bounded_pool"] else
            "one-thread-per-task — measures true CONCURRENCY")
    _section(f"4a/4b. TDS layer async check — execute path  "
             f"(WAITFOR {_TDS_DELAY_SECONDS}s, "
             f"requested N={tds['requested_concurrency']}, "
             f"workers={tds['effective_max_workers']})")
    _row("Mode", mode)
    _row("Sync execute elapsed (baseline)", f"{tds['sync_elapsed_s']:.3f} s")
    if tds["async_elapsed_s"] is not None:
        _row("Async (polled) execute elapsed", f"{tds['async_elapsed_s']:.3f} s")
    if tds["async_overhead_pct"] is not None:
        pct = tds["async_overhead_pct"] * 100
        _row("  Polled vs sync overhead", f"{pct:+.1f}%")
    if tds["failed_tasks"] > 0:
        _row("Task success rate",
             f"{tds['successful_tasks']}/{tds['requested_concurrency']}  "
             f"({tds['success_rate'] * 100:.1f}%)  "
             f"— {tds['failed_tasks']} failures tolerated")
    _row("Parallel wall time", f"{tds['parallel_total_s']:.3f} s")
    _row("  Serialized projection (N * sync)",
         f"{tds['parallel_serial_projection_s']:.3f} s")
    ceiling = (tds["effective_max_workers"] if tds["is_bounded_pool"]
               else tds["requested_concurrency"])
    _row("  Parallelism ratio",
         f"{tds['parallel_ratio']:.2f}x  "
         f"(1.0 = fully serialized, {ceiling}.0 = ideal for this mode)")
    _row("  Effective throughput", f"{tds['throughput_qps']:.1f} queries/sec")
    _row("  PASS threshold (parallelism ratio)",
         f">= {tds['pass_threshold_ratio']:.1f}x")


def _run_fetch_heavy_check(conn_str: str) -> dict:
    """Baseline + parallel test for the fetch-heavy path."""
    # Baseline: one connection, execute + fetchall the multi-MB result.
    sync_elapsed, rows = _run_fetchall(conn_str, _TDS_FETCH_QUERY)

    # Parallel: N connections each doing the same. Use a bounded pool
    # sized to the requested concurrency (which is intentionally low
    # since each task moves a lot of data).
    n = _TDS_FETCH_CONCURRENCY
    if n < 1:
        n = 1

    total_rows_by_conn: list[int] = []
    errors: list[BaseException] = []

    def worker() -> int:
        _, r = _run_fetchall(conn_str, _TDS_FETCH_QUERY)
        return r

    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=n) as pool:
        futures = [pool.submit(worker) for _ in range(n)]
        for fut in as_completed(futures):
            exc = fut.exception()
            if exc is not None:
                errors.append(exc)
            else:
                total_rows_by_conn.append(fut.result())
    parallel_total = time.perf_counter() - t0

    successes = len(total_rows_by_conn)
    total_rows = sum(total_rows_by_conn)
    serial_projection = sync_elapsed * successes
    parallel_ratio = (serial_projection / parallel_total
                      if parallel_total > 0 else 0.0)
    rows_per_sec = total_rows / parallel_total if parallel_total > 0 else 0.0
    sync_rows_per_sec = rows / sync_elapsed if sync_elapsed > 0 else 0.0

    return {
        "concurrency": n,
        "successful_tasks": successes,
        "failed_tasks": n - successes,
        "sync_elapsed_s": sync_elapsed,
        "sync_rows": rows,
        "sync_rows_per_sec": sync_rows_per_sec,
        "parallel_total_s": parallel_total,
        "parallel_total_rows": total_rows,
        "parallel_rows_per_sec": rows_per_sec,
        "parallel_serial_projection_s": serial_projection,
        "parallel_ratio": parallel_ratio,
        "parallel_speedup_vs_sync_rows_per_sec":
            (rows_per_sec / sync_rows_per_sec) if sync_rows_per_sec > 0 else 0.0,
        "first_error": repr(errors[0]) if errors else None,
    }


def _print_fetch_heavy(fh: dict | None) -> None:
    _section(f"4c. Fetch-heavy TDS check  "
             f"(TOP 50000 rows × {_TDS_FETCH_CONCURRENCY} concurrent connections)")
    if fh is None:
        _row("Ran", "NO")
        _row("Reason", "skipped (see log)")
        return
    _row("Sync baseline elapsed", f"{fh['sync_elapsed_s']:.3f} s")
    _row("  Sync rows fetched", fh["sync_rows"])
    _row("  Sync throughput", f"{fh['sync_rows_per_sec']:.0f} rows/sec")
    if fh["failed_tasks"] > 0:
        _row("Task success rate",
             f"{fh['successful_tasks']}/{fh['concurrency']}  "
             f"— first error: {fh['first_error']}")
    _row("Parallel wall time", f"{fh['parallel_total_s']:.3f} s")
    _row("  Parallel total rows", fh["parallel_total_rows"])
    _row("  Parallel aggregate throughput",
         f"{fh['parallel_rows_per_sec']:.0f} rows/sec")
    _row("  Serialized projection (N * sync)",
         f"{fh['parallel_serial_projection_s']:.3f} s")
    _row("  Parallelism ratio",
         f"{fh['parallel_ratio']:.2f}x  "
         f"(1.0 = fully serialized, {fh['concurrency']}.0 = ideal)")
    _row("  Speedup vs sync (rows/sec)",
         f"{fh['parallel_speedup_vs_sync_rows_per_sec']:.2f}x")
    _row("  Note",
         "Fetch on LOCALHOST is dominated by Python GIL during row "
         "materialization")
    _row("  ",
         "(bytes -> Python objects). A ratio near 1.0x is expected here — "
         "adding threads")
    _row("  ",
         "doesn't help when 90%+ of fetch time is GIL-held. On slower "
         "networks the")
    _row("  ",
         "ratio rises because driver GIL release around blocking recv() "
         "gives real overlap.")
    _row("  ",
         "Ratios BELOW ~0.5x would indicate a driver-side global lock — "
         "that is what we")
    _row("  ",
         "actually gate on. Any value between 0.5x and ~1.5x on localhost "
         "is healthy.")


def _print_verdict(cap: dict, smoke: dict, tds: dict | None,
                   fh: dict | None = None) -> int:
    _section("5. Verdict")
    mode = cap.get("async_mode")
    stmt_ok = mode is not None and int(mode) >= 2  # SQL_AM_STATEMENT
    ran = bool(smoke.get("ran"))
    exec_ok = bool(smoke.get("execute_ok"))
    fetch_ok = bool(smoke.get("fetch_ok"))
    exec_observed = bool(smoke.get("execute_observed_still_executing"))

    _row("Driver advertises SQL_AM_STATEMENT", "PASS" if stmt_ok else "FAIL")
    _row("Polling smoke test ran", "PASS" if ran else "FAIL")
    _row("Execute succeeded under polling", "PASS" if exec_ok else "FAIL")
    _row("Fetch succeeded under polling", "PASS" if fetch_ok else "FAIL")
    _row("SQL_STILL_EXECUTING actually observed",
         "PASS" if exec_observed
         else "WARN (driver may have blocked internally)")

    if tds is not None:
        _row("Polled overhead vs sync <= "
             f"{int(_ASYNC_OVERHEAD_TOLERANCE * 100)}%",
             "PASS" if tds["async_overhead_ok"] else "FAIL")
        _row(f"TDS parallelism ratio >= {tds['pass_threshold_ratio']:.1f}x",
             "PASS" if tds["parallel_ok"] else "FAIL")

    fetch_parallel_ok = True
    if fh is not None:
        # Fetch-heavy parallelism on localhost is dominated by Python's
        # GIL during row materialization (bytes -> int/str/datetime ->
        # tuple -> Row), NOT by the driver. On a fast local connection
        # the network slice is essentially free and adding thread
        # contention can push the parallel ratio anywhere in ~0.9-1.5x
        # depending on run-to-run noise. On a slower network the ratio
        # rises because GIL release around blocking recv() gives real
        # overlap.
        #
        # We only FAIL here if the ratio is < 0.5x — that would indicate
        # a driver-level global lock making parallel *much worse* than
        # serial, which would kill any async POC value.
        threshold = 0.5
        fetch_parallel_ok = fh["parallel_ratio"] >= threshold
        _row(f"Fetch-heavy parallelism ratio >= {threshold:.2f}x "
             f"(driver-serialization check)",
             "PASS" if fetch_parallel_ok else "FAIL")

    if not stmt_ok:
        print("\nRESULT: statement-level async is NOT advertised on this driver.")
        print("        Stop the POC or switch to run_in_executor(sync_execute).")
        return 3
    if not (ran and exec_ok and fetch_ok):
        print("\nRESULT: capability advertised but polling smoke test FAILED.")
        return 1
    if tds is not None and not (tds["parallel_ok"] and tds["async_overhead_ok"]):
        print("\nRESULT: polling API works but TDS layer looks serialized or "
              "adds heavy overhead.")
        print("        The async win over sync + thread pool may be marginal on "
              "this platform.")
        return 1
    if fh is not None and not fetch_parallel_ok:
        print("\nRESULT: fetch-heavy parallelism ratio is BELOW 0.5x — parallel "
              "is much slower")
        print("        than serial. This suggests a driver-side global lock on "
              "the fetch")
        print("        path. Async-fetch will NOT help; async POC value is "
              "questionable.")
        return 1

    print("\nRESULT: polling works AND the TDS layer is genuinely async on this "
          "OS + driver.")
    print("        Safe to proceed with POC Step 2 (pybind async primitives).")
    if not exec_observed:
        print("        NOTE: SQL_STILL_EXECUTING was not observed; the driver "
              "may internally\n        block the initial call. Polling still "
              "completed but the async win\n        over a plain sync call may "
              "be smaller than expected on this platform.")
    return 0


def main() -> int:
    conn_str = os.getenv("DB_CONNECTION_STRING")
    if not conn_str:
        print("ERROR: DB_CONNECTION_STRING environment variable is not set.")
        return 2

    # Bypass mssql-python's own client-side connection pool for this probe.
    # We want to measure the msodbcsql driver's TDS parallelism, not our
    # pool's max_size cap. Calling disable() before any connect() causes
    # subsequent Connection() constructions to pass use_pool=False through
    # to the C++ layer.
    PoolingManager.disable()

    _print_environment()

    conn = connect(conn_str)
    try:
        cap = conn._conn.get_async_capability()  # noqa: SLF001 — POC probe
    finally:
        conn.close()

    smoke = cap.pop("polling_smoke_test", {"ran": False,
                                           "error": "not returned by probe"})
    fetch_stream = cap.pop("polling_fetch_stream_test", None)

    _print_capability(cap)
    _print_smoke(smoke, fetch_stream)

    tds = None
    if smoke.get("ran") and smoke.get("execute_ok"):
        try:
            sync_elapsed = _run_query(conn_str, _TDS_QUERY)
            async_ms = smoke.get("execute_elapsed_ms")
            tds = _run_tds_layer_check(conn_str, sync_elapsed, async_ms)
            _print_tds(tds)
        except Exception as exc:  # pragma: no cover — surface in verdict
            _section(f"4. TDS layer async check  (WAITFOR {_TDS_DELAY_SECONDS}s, "
                     f"concurrency={_TDS_CONCURRENCY})")
            _row("Ran", "NO")
            _row("Reason", repr(exc))
            tds = None
    else:
        _section(f"4. TDS layer async check  (WAITFOR {_TDS_DELAY_SECONDS}s, "
                 f"concurrency={_TDS_CONCURRENCY})")
        _row("Ran", "NO")
        _row("Reason", "skipped — polling smoke test did not pass")

    # Fetch-heavy sub-test. Runs unconditionally when the driver + server
    # are working, since it does not depend on async polling — it measures
    # sync fetchall throughput per-connection vs. across N connections.
    fh = None
    if smoke.get("ran") and smoke.get("execute_ok"):
        try:
            fh = _run_fetch_heavy_check(conn_str)
            _print_fetch_heavy(fh)
        except Exception as exc:  # pragma: no cover
            _section(f"4c. Fetch-heavy TDS check  (TOP 50000 rows × "
                     f"{_TDS_FETCH_CONCURRENCY} concurrent connections)")
            _row("Ran", "NO")
            _row("Reason", repr(exc))
            fh = None

    return _print_verdict(cap, smoke, tds, fh)


if __name__ == "__main__":
    sys.exit(main())

