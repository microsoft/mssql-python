"""
Concurrent connection performance test for GIL release during ODBC operations.

This test verifies that the GIL is properly released during blocking ODBC
connection establishment (SQLDriverConnect) and teardown (SQLDisconnect),
allowing multiple Python threads to establish connections in parallel.

Without GIL release, N threads would take ~N * single_connection_time
(serialized by the GIL). With GIL release, threads overlap their I/O and
total wall-clock time is close to single_connection_time.

Marked with @pytest.mark.stress to run in dedicated performance pipelines.
"""

import os
import time
import threading
import statistics

import pytest
import mssql_python
from mssql_python import connect


@pytest.fixture(scope="module")
def perf_conn_str():
    """Get connection string from environment."""
    conn_str = os.getenv("DB_CONNECTION_STRING")
    if not conn_str:
        pytest.skip("DB_CONNECTION_STRING environment variable not set")
    return conn_str


def _connect_and_close(conn_str: str) -> float:
    """Open a connection, close it, and return the elapsed time in seconds."""
    start = time.perf_counter()
    conn = connect(conn_str)
    conn.close()
    return time.perf_counter() - start


# ============================================================================
# GIL Release Performance Tests
# ============================================================================


@pytest.mark.stress
def test_concurrent_connection_gil_release(perf_conn_str):
    """
    Verify that concurrent connection establishment achieves parallelism,
    proving the GIL is released during SQLDriverConnect.

    Approach:
      1. Measure the baseline: average time for a single connection (no contention).
      2. Launch NUM_THREADS threads, each creating a fresh connection (no pooling).
      3. The wall-clock time for all threads should be much less than
         NUM_THREADS * baseline, because connections overlap their I/O.

    We require a speedup > 2x (very conservative). In practice, with proper
    GIL release, the speedup approaches NUM_THREADS for I/O-bound work.
    """
    NUM_THREADS = 10
    WARMUP_ROUNDS = 2
    BASELINE_ROUNDS = 5

    # Disable pooling so every connect() creates a brand-new ODBC connection.
    mssql_python.pooling(enabled=False)

    # ---- warm-up (prime DNS cache, driver loading, etc.) ----
    for _ in range(WARMUP_ROUNDS):
        _connect_and_close(perf_conn_str)

    # ---- baseline: serial single-connection time ----
    serial_times = [_connect_and_close(perf_conn_str) for _ in range(BASELINE_ROUNDS)]
    baseline = statistics.median(serial_times)
    print(f"\n[BASELINE] Single connection (median of {BASELINE_ROUNDS}): {baseline*1000:.1f} ms")

    # ---- concurrent: N threads each opening a connection ----
    barrier = threading.Barrier(NUM_THREADS)
    thread_times = [None] * NUM_THREADS
    errors = []

    def worker(idx):
        try:
            barrier.wait(timeout=30)
            thread_times[idx] = _connect_and_close(perf_conn_str)
        except Exception as exc:
            errors.append((idx, str(exc)))

    threads = [threading.Thread(target=worker, args=(i,), daemon=True) for i in range(NUM_THREADS)]

    wall_start = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=120)
    wall_time = time.perf_counter() - wall_start

    # ---- assertions ----
    assert not errors, f"Thread errors: {errors}"
    assert all(t is not None for t in thread_times), "Some threads did not complete"

    serial_estimate = NUM_THREADS * baseline
    speedup = serial_estimate / wall_time

    print(f"[CONCURRENT] {NUM_THREADS} threads wall-clock: {wall_time*1000:.1f} ms")
    print(f"[SERIAL EST] {NUM_THREADS} × baseline:        {serial_estimate*1000:.1f} ms")
    print(f"[SPEEDUP]    {speedup:.2f}x  (>{NUM_THREADS}x means full parallelism)")

    # Conservative threshold: even modest parallelism should beat 2x.
    # Without GIL release this would be ~1.0x (fully serialized).
    assert speedup > 2.0, (
        f"Concurrent connections are not running in parallel (speedup={speedup:.2f}x). "
        f"Expected >2x, got wall_time={wall_time*1000:.1f}ms vs serial_estimate={serial_estimate*1000:.1f}ms. "
        f"This likely indicates the GIL is not being released during SQLDriverConnect."
    )

    print(f"[PASSED] GIL release verified — {speedup:.1f}x speedup with {NUM_THREADS} threads")


@pytest.mark.stress
def test_concurrent_disconnect_gil_release(perf_conn_str):
    """
    Verify that concurrent disconnection works correctly with GIL release.

    Opens N connections serially, then closes them all concurrently.
    On localhost, disconnect is sub-millisecond so thread overhead dominates
    and speedup ratios are not meaningful. Instead, we verify that all
    concurrent disconnects complete without errors or deadlocks.
    """
    NUM_THREADS = 10

    mssql_python.pooling(enabled=False)

    # warm-up
    for _ in range(2):
        _connect_and_close(perf_conn_str)

    # open N connections serially
    connections = [connect(perf_conn_str) for _ in range(NUM_THREADS)]

    # concurrent close
    barrier = threading.Barrier(NUM_THREADS)
    thread_times = [None] * NUM_THREADS
    errors = []

    def close_worker(idx, conn):
        try:
            barrier.wait(timeout=30)
            start = time.perf_counter()
            conn.close()
            thread_times[idx] = time.perf_counter() - start
        except Exception as exc:
            errors.append((idx, str(exc)))

    threads = [
        threading.Thread(target=close_worker, args=(i, connections[i]), daemon=True)
        for i in range(NUM_THREADS)
    ]

    wall_start = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)
    wall_time = time.perf_counter() - wall_start

    assert not errors, f"Thread errors: {errors}"
    assert all(t is not None for t in thread_times), "Some threads did not complete"

    print(f"\n[CONCURRENT] {NUM_THREADS} threads close wall-clock: {wall_time*1000:.1f} ms")
    print(f"[PASSED] All {NUM_THREADS} concurrent disconnects completed without errors")


@pytest.mark.stress
def test_mixed_connect_disconnect_under_load(perf_conn_str):
    """
    Stress test: threads continuously connect and disconnect while
    other threads do CPU-bound Python work. Verifies that GIL release
    during ODBC I/O does not starve or deadlock Python threads.
    """
    NUM_IO_THREADS = 5
    NUM_CPU_THREADS = 3
    DURATION_SECS = 5

    mssql_python.pooling(enabled=False)

    stop_event = threading.Event()
    io_counts = [0] * NUM_IO_THREADS
    cpu_counts = [0] * NUM_CPU_THREADS
    errors = []

    def io_worker(idx):
        """Repeatedly connect/disconnect."""
        try:
            while not stop_event.is_set():
                conn = connect(perf_conn_str)
                conn.close()
                io_counts[idx] += 1
        except Exception as exc:
            errors.append((f"io-{idx}", str(exc)))

    def cpu_worker(idx):
        """Do CPU-bound work (must be able to acquire GIL)."""
        try:
            while not stop_event.is_set():
                # Busy work that requires the GIL
                total = sum(range(10000))
                _ = [x**2 for x in range(100)]
                cpu_counts[idx] += 1
        except Exception as exc:
            errors.append((f"cpu-{idx}", str(exc)))

    threads = []
    for i in range(NUM_IO_THREADS):
        threads.append(threading.Thread(target=io_worker, args=(i,), daemon=True))
    for i in range(NUM_CPU_THREADS):
        threads.append(threading.Thread(target=cpu_worker, args=(i,), daemon=True))

    for t in threads:
        t.start()

    time.sleep(DURATION_SECS)
    stop_event.set()

    for t in threads:
        t.join(timeout=30)

    total_io = sum(io_counts)
    total_cpu = sum(cpu_counts)

    print(f"\n[MIXED LOAD] Duration: {DURATION_SECS}s")
    print(f"  I/O threads ({NUM_IO_THREADS}): {total_io} connect/disconnect cycles")
    print(f"  CPU threads ({NUM_CPU_THREADS}): {total_cpu} iterations")

    assert not errors, f"Errors during mixed load: {errors}"

    # CPU threads must have made progress — if the GIL were held during
    # ODBC I/O, CPU threads would be starved.
    assert total_cpu > 0, (
        "CPU threads made no progress — GIL may be held during ODBC I/O, "
        "starving Python threads."
    )

    # I/O threads must have completed at least a few cycles
    assert total_io > 0, "I/O threads made no progress."

    print(f"[PASSED] Mixed I/O + CPU load: no starvation or deadlocks")
