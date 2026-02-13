"""
Multi-threaded stress tests for mssql-python driver.

These tests verify the driver's behavior under multi-threaded conditions:
- Concurrent connections with 2, 5, 10, 50, 100, and 1000 threads
- Connection pooling under stress
- Thread safety of query execution
- Memory and resource usage under load
- Race condition detection

Tests are marked with @pytest.mark.stress_threading and are designed to be run
in a dedicated pipeline separate from regular CI tests.

Inspired by: https://github.com/saurabh500/sqlclientrepros/tree/master/python/standalone
"""

import pytest
import os
import time
import threading
import queue
import psutil
from datetime import datetime
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field
import traceback

import mssql_python
from mssql_python import connect


# ============================================================================
# Data Classes for Test Results
# ============================================================================


@dataclass
class ThreadResult:
    """Result from a single thread execution"""

    thread_id: int
    iterations: int = 0
    total_time: float = 0.0
    total_rows: int = 0
    errors: int = 0
    min_time: float = float("inf")
    max_time: float = 0.0
    success: bool = True
    error_messages: List[str] = field(default_factory=list)


@dataclass
class StressTestResult:
    """Aggregated result from a stress test"""

    num_threads: int
    total_iterations: int = 0
    total_time: float = 0.0
    total_rows: int = 0
    total_errors: int = 0
    throughput_qps: float = 0.0
    avg_latency_ms: float = 0.0
    thread_results: List[ThreadResult] = field(default_factory=list)
    success: bool = True
    hung: bool = False


# ============================================================================
# Helper Functions
# ============================================================================


def drop_table_if_exists(cursor, table_name: str):
    """Helper to drop a table if it exists. Raises exception if drop fails (ignores 'not found')."""
    try:
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    except Exception as e:
        # Ignore "object does not exist" errors, raise everything else (e.g., lock conflicts)
        error_msg = str(e).lower()
        if "does not exist" not in error_msg and "could not find" not in error_msg:
            raise


def get_resource_usage() -> Dict[str, Any]:
    """Get current process resource usage"""
    process = psutil.Process()
    mem_info = process.memory_info()
    return {
        "rss_mb": round(mem_info.rss / (1024 * 1024), 2),
        "vms_mb": round(mem_info.vms / (1024 * 1024), 2),
        "num_threads": process.num_threads(),
        "cpu_percent": process.cpu_percent(interval=0.1),
        "timestamp": datetime.now().isoformat(),
    }


# ============================================================================
# Multi-Threaded Query Runner
# ============================================================================


class MultiThreadedQueryRunner:
    """
    Executes SQL queries across multiple threads with configurable parameters.

    This class is designed to stress test the mssql-python driver's
    thread safety and concurrent connection handling.
    """

    def __init__(
        self,
        conn_str: str,
        query: str = "SELECT 1 as num, 'test' as str, GETDATE() as dt",
        verbose: bool = False,
        enable_pooling: bool = True,
        timeout_seconds: int = 120,
    ):
        self.conn_str = conn_str
        self.query = query
        self.verbose = verbose
        self.enable_pooling = enable_pooling
        self.timeout_seconds = timeout_seconds

        self.stats_lock = threading.Lock()
        self.thread_results: Dict[int, ThreadResult] = {}
        self.process = psutil.Process()
        self.stop_event = threading.Event()
        self.start_barrier: Optional[threading.Barrier] = None

    def execute_single_query(self, thread_id: int, iteration: int) -> Dict[str, Any]:
        """
        Execute a single query cycle: connect -> query -> read results -> disconnect
        """
        start_time = time.time()
        result = {
            "thread_id": thread_id,
            "iteration": iteration,
            "success": False,
            "rows_read": 0,
            "execution_time": 0.0,
            "error": None,
        }

        conn = None
        cursor = None
        try:
            # Connect to database
            if self.verbose:
                print(f"[Thread-{thread_id}] Iteration {iteration}: Connecting...")

            conn = connect(self.conn_str)

            # Create cursor and execute query
            cursor = conn.cursor()
            cursor.execute(self.query)

            # Read all results
            rows_read = 0
            for row in cursor:
                rows_read += 1

            result["success"] = True
            result["rows_read"] = rows_read

            if self.verbose:
                print(f"[Thread-{thread_id}] Iteration {iteration}: Completed ({rows_read} rows)")

        except Exception as e:
            result["error"] = str(e)
            if self.verbose:
                print(f"[Thread-{thread_id}] Iteration {iteration}: ERROR - {e}")

        finally:
            # Clean up
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
            result["execution_time"] = time.time() - start_time

        return result

    def worker_thread(self, thread_id: int, iterations: int, delay: float = 0.0):
        """
        Worker thread that executes multiple query iterations.
        """
        thread_result = ThreadResult(thread_id=thread_id)

        try:
            # Wait at barrier for synchronized start
            if self.start_barrier:
                self.start_barrier.wait()

            for i in range(iterations):
                if self.stop_event.is_set():
                    break

                result = self.execute_single_query(thread_id, i + 1)

                # Update statistics
                thread_result.iterations += 1
                thread_result.total_time += result["execution_time"]
                thread_result.total_rows += result["rows_read"]

                if result["success"]:
                    thread_result.min_time = min(thread_result.min_time, result["execution_time"])
                    thread_result.max_time = max(thread_result.max_time, result["execution_time"])
                else:
                    thread_result.errors += 1
                    thread_result.error_messages.append(f"Iter {i+1}: {result['error']}")

                if delay > 0:
                    time.sleep(delay)

            thread_result.success = thread_result.errors == 0

        except Exception as e:
            thread_result.success = False
            thread_result.error_messages.append(f"Thread exception: {str(e)}")

        # Store result
        with self.stats_lock:
            self.thread_results[thread_id] = thread_result

    def run_parallel(
        self, num_threads: int, iterations_per_thread: int, delay: float = 0.0
    ) -> StressTestResult:
        """
        Run queries in parallel using multiple threads.

        Returns:
            StressTestResult with aggregated statistics
        """
        result = StressTestResult(num_threads=num_threads)

        # Reset state
        self.thread_results.clear()
        self.stop_event.clear()

        # Create barrier for synchronized start
        self.start_barrier = threading.Barrier(num_threads)

        # Configure pooling
        if self.enable_pooling:
            mssql_python.pooling(enabled=True, max_size=max(100, num_threads * 2))
        else:
            mssql_python.pooling(enabled=False)

        print(f"\n{'=' * 80}")
        print(f"Multi-Threaded Stress Test")
        print(f"{'=' * 80}")
        print(f"Threads:          {num_threads}")
        print(f"Iterations/Thread: {iterations_per_thread}")
        print(f"Total Iterations: {num_threads * iterations_per_thread}")
        print(f"Pooling:          {'Enabled' if self.enable_pooling else 'Disabled'}")
        print(f"Timeout:          {self.timeout_seconds}s")
        print(f"Query:            {self.query[:60]}{'...' if len(self.query) > 60 else ''}")
        print(f"{'=' * 80}")

        # Get initial resource usage
        initial_resources = get_resource_usage()
        print(f"Initial RSS: {initial_resources['rss_mb']} MB")

        start_time = time.time()

        # Create and start threads
        threads: List[threading.Thread] = []
        for i in range(num_threads):
            thread = threading.Thread(
                target=self.worker_thread,
                args=(i + 1, iterations_per_thread, delay),
                name=f"StressWorker-{i + 1}",
                daemon=True,  # Daemon threads so they die if main thread exits
            )
            threads.append(thread)
            thread.start()

        # Wait for threads with timeout
        all_completed = True
        for thread in threads:
            thread.join(timeout=self.timeout_seconds)
            if thread.is_alive():
                all_completed = False
                print(f"WARNING: Thread {thread.name} timed out!")

        if not all_completed:
            print("ERROR: Some threads did not complete - signaling stop")
            self.stop_event.set()
            result.hung = True
            result.success = False
            # Give threads a moment to exit
            time.sleep(2)

        total_time = time.time() - start_time

        # Get final resource usage
        final_resources = get_resource_usage()
        print(
            f"Final RSS: {final_resources['rss_mb']} MB (delta: {final_resources['rss_mb'] - initial_resources['rss_mb']:.2f} MB)"
        )

        # Aggregate results
        result.total_time = total_time
        for thread_id, tr in self.thread_results.items():
            result.total_iterations += tr.iterations
            result.total_rows += tr.total_rows
            result.total_errors += tr.errors
            result.thread_results.append(tr)

        if result.total_time > 0:
            result.throughput_qps = result.total_iterations / result.total_time

        if result.total_iterations > 0:
            total_exec_time = sum(tr.total_time for tr in result.thread_results)
            result.avg_latency_ms = (total_exec_time / result.total_iterations) * 1000

        if not result.hung:
            result.success = result.total_errors == 0

        # Print summary
        self._print_statistics(result)

        return result

    def _print_statistics(self, result: StressTestResult):
        """Print execution statistics"""
        print(f"\n{'=' * 80}")
        print("Execution Statistics")
        print(f"{'=' * 80}")

        # Per-thread summary
        for tr in sorted(result.thread_results, key=lambda x: x.thread_id):
            status = "OK" if tr.success else "FAIL"
            avg_time = tr.total_time / tr.iterations if tr.iterations > 0 else 0
            print(
                f"  Thread-{tr.thread_id}: {status} {tr.iterations} iters, "
                f"{tr.errors} errors, avg {avg_time*1000:.1f}ms"
            )
            if tr.error_messages and len(tr.error_messages) <= 3:
                for msg in tr.error_messages:
                    print(f"    - {msg}")

        # Overall statistics
        print(f"\n{'-' * 80}")
        print("Overall Statistics:")
        print(f"  Status:           {'PASSED' if result.success else 'FAILED'}")
        print(f"  Hung:             {'YES' if result.hung else 'No'}")
        print(f"  Total Time:       {result.total_time:.3f}s")
        print(f"  Total Iterations: {result.total_iterations}")
        print(f"  Total Rows:       {result.total_rows:,}")
        print(f"  Total Errors:     {result.total_errors}")
        print(f"  Throughput:       {result.throughput_qps:.2f} queries/sec")
        print(f"  Avg Latency:      {result.avg_latency_ms:.2f}ms")
        print(f"{'=' * 80}\n")


# ============================================================================
# Pytest Fixtures
# ============================================================================


@pytest.fixture(scope="module")
def stress_conn_str():
    """Get connection string from environment"""
    conn_str = os.getenv("DB_CONNECTION_STRING")
    if not conn_str:
        pytest.skip("DB_CONNECTION_STRING environment variable not set")
    return conn_str


# ============================================================================
# Basic Multi-Thread Tests (2, 5 threads)
# ============================================================================


@pytest.mark.stress_threading
@pytest.mark.parametrize(
    "num_threads,iterations",
    [
        (2, 50),
        (5, 30),
    ],
)
def test_basic_multithreaded_queries(stress_conn_str, num_threads, iterations):
    """
    Test basic multi-threaded query execution with low thread counts.

    These tests should pass reliably and serve as a baseline.
    """
    runner = MultiThreadedQueryRunner(
        conn_str=stress_conn_str,
        query="SELECT 1 as num, 'test' as str, GETDATE() as dt",
        enable_pooling=True,
        timeout_seconds=120,
    )

    result = runner.run_parallel(num_threads=num_threads, iterations_per_thread=iterations)

    assert not result.hung, f"Test hung with {num_threads} threads"
    assert result.success, f"Test failed with {result.total_errors} errors"
    assert result.total_iterations == num_threads * iterations
    print(
        f"[PASSED] {num_threads} threads x {iterations} iterations = {result.throughput_qps:.1f} qps"
    )


@pytest.mark.stress_threading
@pytest.mark.parametrize(
    "num_threads,iterations",
    [
        (2, 50),
        (5, 30),
    ],
)
def test_basic_multithreaded_without_pooling(stress_conn_str, num_threads, iterations):
    """
    Test multi-threaded query execution without connection pooling.

    Each thread creates and destroys its own connection for each query.
    """
    runner = MultiThreadedQueryRunner(
        conn_str=stress_conn_str,
        query="SELECT 1 as num, 'hello' as str",
        enable_pooling=False,
        timeout_seconds=180,
    )

    result = runner.run_parallel(num_threads=num_threads, iterations_per_thread=iterations)

    assert not result.hung, f"Test hung with {num_threads} threads (no pooling)"
    assert result.success, f"Test failed with {result.total_errors} errors"
    print(
        f"[PASSED] {num_threads} threads x {iterations} iterations (no pooling) = {result.throughput_qps:.1f} qps"
    )


# ============================================================================
# Medium Load Tests (10, 50 threads)
# ============================================================================


@pytest.mark.stress_threading
@pytest.mark.parametrize(
    "num_threads,iterations",
    [
        (10, 20),
        (50, 10),
    ],
)
def test_medium_load_multithreaded(stress_conn_str, num_threads, iterations):
    """
    Test medium load multi-threaded query execution.

    Tests driver behavior under moderate concurrent load.
    """
    runner = MultiThreadedQueryRunner(
        conn_str=stress_conn_str,
        query="SELECT TOP 10 name, object_id FROM sys.objects",
        enable_pooling=True,
        timeout_seconds=180,
    )

    result = runner.run_parallel(num_threads=num_threads, iterations_per_thread=iterations)

    assert not result.hung, f"Test hung with {num_threads} threads"
    # Allow some errors but expect majority success
    error_rate = result.total_errors / result.total_iterations if result.total_iterations > 0 else 1
    assert (
        error_rate < 0.1
    ), f"Error rate too high: {error_rate*100:.1f}% ({result.total_errors} errors)"
    print(
        f"[PASSED] {num_threads} threads x {iterations} iterations = {result.throughput_qps:.1f} qps, {error_rate*100:.1f}% errors"
    )


@pytest.mark.stress_threading
def test_50_threads_data_integrity(stress_conn_str):
    """
    Test data integrity with 50 concurrent threads.

    Each thread inserts and reads its own data, verifying no cross-contamination.
    """
    num_threads = 50
    iterations = 5
    results_queue = queue.Queue()
    errors_queue = queue.Queue()
    barrier = threading.Barrier(num_threads)

    def worker(thread_id: int):
        try:
            barrier.wait(timeout=30)  # Synchronize start

            conn = connect(stress_conn_str)
            cursor = conn.cursor()

            # Create thread-specific temp table
            table_name = f"#stress_t{thread_id}"
            drop_table_if_exists(cursor, table_name)

            cursor.execute(
                f"""
                CREATE TABLE {table_name} (
                    id INT PRIMARY KEY,
                    thread_id INT,
                    data NVARCHAR(100)
                )
            """
            )
            conn.commit()

            # Perform iterations
            for i in range(iterations):
                # Insert
                cursor.execute(
                    f"INSERT INTO {table_name} VALUES (?, ?, ?)",
                    (i, thread_id, f"Thread_{thread_id}_Iter_{i}"),
                )
                conn.commit()

                # Verify
                cursor.execute(f"SELECT * FROM {table_name} WHERE id = ?", (i,))
                row = cursor.fetchone()

                if row is None:
                    raise ValueError(f"Thread {thread_id}: Row {i} not found!")
                if row[1] != thread_id:
                    raise ValueError(f"Thread {thread_id}: Data corruption! Got thread_id {row[1]}")

            # Cleanup
            drop_table_if_exists(cursor, table_name)
            conn.commit()
            cursor.close()
            conn.close()

            results_queue.put({"thread_id": thread_id, "success": True})

        except Exception as e:
            errors_queue.put(
                {"thread_id": thread_id, "error": str(e), "traceback": traceback.format_exc()}
            )

    # Run threads
    threads = []
    for i in range(num_threads):
        t = threading.Thread(target=worker, args=(i,), daemon=True)
        threads.append(t)
        t.start()

    # Wait with timeout
    for t in threads:
        t.join(timeout=120)

    # Check results
    successes = results_queue.qsize()
    errors = []
    while not errors_queue.empty():
        errors.append(errors_queue.get())

    print(f"\n50-thread data integrity: {successes} successes, {len(errors)} errors")

    if errors:
        for e in errors[:5]:  # Show first 5 errors
            print(f"  Thread {e['thread_id']}: {e['error']}")

    # Allow up to 10% failure rate
    success_rate = successes / num_threads
    assert success_rate >= 0.9, f"Success rate too low: {success_rate*100:.1f}%"
    print(f"[PASSED] 50 threads data integrity test: {success_rate*100:.1f}% success rate")


# ============================================================================
# High Load Tests (100, 1000 threads)
# ============================================================================


@pytest.mark.stress_threading
@pytest.mark.parametrize(
    "num_threads,iterations",
    [
        (100, 5),
    ],
)
def test_high_load_100_threads(stress_conn_str, num_threads, iterations):
    """
    Test high load with 100 concurrent threads.

    This tests the driver's ability to handle significant concurrent load.
    """
    runner = MultiThreadedQueryRunner(
        conn_str=stress_conn_str, query="SELECT 1", enable_pooling=True, timeout_seconds=300
    )

    result = runner.run_parallel(num_threads=num_threads, iterations_per_thread=iterations)

    assert not result.hung, f"Test hung with {num_threads} threads"

    # For high load, we expect some degradation but not complete failure
    completion_rate = result.total_iterations / (num_threads * iterations)
    error_rate = result.total_errors / result.total_iterations if result.total_iterations > 0 else 1

    print(
        f"100-thread results: {completion_rate*100:.1f}% completion, {error_rate*100:.1f}% errors"
    )

    assert completion_rate >= 0.8, f"Completion rate too low: {completion_rate*100:.1f}%"
    assert error_rate < 0.2, f"Error rate too high: {error_rate*100:.1f}%"
    print(f"[PASSED] 100 threads test: {result.throughput_qps:.1f} qps")


@pytest.mark.stress_threading
@pytest.mark.slow
def test_extreme_load_1000_threads(stress_conn_str):
    """
    Test extreme load with 1000 concurrent threads.

    This is an extreme stress test to find the breaking point.
    Expects some failures but tests for graceful degradation.
    """
    num_threads = 1000
    iterations = 2  # Keep iterations low for 1000 threads

    runner = MultiThreadedQueryRunner(
        conn_str=stress_conn_str,
        query="SELECT 1",
        enable_pooling=True,
        timeout_seconds=600,  # 10 minutes for 1000 threads
    )

    result = runner.run_parallel(num_threads=num_threads, iterations_per_thread=iterations)

    # For 1000 threads, we just check it doesn't completely hang
    assert not result.hung, "Test completely hung with 1000 threads"

    completion_rate = result.total_iterations / (num_threads * iterations)
    error_rate = result.total_errors / result.total_iterations if result.total_iterations > 0 else 1

    print(f"\n1000-thread stress test results:")
    print(f"  Completion rate: {completion_rate*100:.1f}%")
    print(f"  Error rate: {error_rate*100:.1f}%")
    print(f"  Throughput: {result.throughput_qps:.1f} qps")

    # Very lenient assertions for extreme load
    assert completion_rate >= 0.5, f"Less than 50% of queries completed: {completion_rate*100:.1f}%"
    print(f"[PASSED] 1000 threads extreme stress test completed")


# ============================================================================
# Connection Pool Stress Tests
# ============================================================================


@pytest.mark.stress_threading
def test_pool_exhaustion_recovery(stress_conn_str):
    """
    Test that the driver recovers gracefully when connection pool is exhausted.

    Creates more threads than pool size to test queuing and recovery.
    """
    # Set small pool size
    mssql_python.pooling(enabled=True, max_size=10)

    num_threads = 50  # 5x the pool size
    iterations = 10

    runner = MultiThreadedQueryRunner(
        conn_str=stress_conn_str,
        query="SELECT 1; WAITFOR DELAY '00:00:00.050'",  # 50ms delay
        enable_pooling=True,
        timeout_seconds=180,
    )

    result = runner.run_parallel(num_threads=num_threads, iterations_per_thread=iterations)

    assert not result.hung, "Pool exhaustion caused hang"

    completion_rate = result.total_iterations / (num_threads * iterations)
    assert (
        completion_rate >= 0.8
    ), f"Too many queries failed under pool exhaustion: {completion_rate*100:.1f}%"

    print(
        f"[PASSED] Pool exhaustion test: {completion_rate*100:.1f}% completion with pool_size=10, threads=50"
    )


@pytest.mark.stress_threading
def test_rapid_pool_enable_disable(stress_conn_str):
    """
    Test rapid enabling and disabling of connection pooling.

    This tests for race conditions in pool management.
    """
    errors = []
    iterations = 20

    def query_worker(worker_id: int):
        try:
            for i in range(iterations):
                conn = connect(stress_conn_str)
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
                cursor.close()
                conn.close()
        except Exception as e:
            errors.append(f"Worker {worker_id}: {e}")

    # Run multiple cycles of pool toggle
    for cycle in range(5):
        # Enable pooling
        mssql_python.pooling(enabled=True, max_size=50)

        # Start threads
        threads = [threading.Thread(target=query_worker, args=(i,), daemon=True) for i in range(10)]
        for t in threads:
            t.start()

        # Toggle pooling mid-execution
        time.sleep(0.1)
        mssql_python.pooling(enabled=False)
        time.sleep(0.1)
        mssql_python.pooling(enabled=True, max_size=50)

        # Wait for threads
        for t in threads:
            t.join(timeout=30)

    error_count = len(errors)
    print(f"Pool toggle test: {error_count} errors across 5 cycles")

    # Some errors expected during toggling, but shouldn't crash
    assert error_count < 50, f"Too many errors during pool toggling: {error_count}"
    print(f"[PASSED] Rapid pool enable/disable test")


# ============================================================================
# Long Running Stress Tests
# ============================================================================


@pytest.mark.stress_threading
@pytest.mark.slow
def test_sustained_load_5_minutes(stress_conn_str):
    """
    Test sustained load over 5 minutes with moderate thread count.

    This tests for memory leaks and resource exhaustion over time.
    """
    num_threads = 20
    duration_seconds = 300  # 5 minutes

    # Track memory over time
    memory_samples = []
    stop_event = threading.Event()
    results_queue = queue.Queue()

    def worker(thread_id: int):
        iterations = 0
        errors = 0
        while not stop_event.is_set():
            try:
                conn = connect(stress_conn_str)
                cursor = conn.cursor()
                cursor.execute("SELECT TOP 100 name FROM sys.objects")
                rows = cursor.fetchall()
                cursor.close()
                conn.close()
                iterations += 1
            except Exception:
                errors += 1
            time.sleep(0.01)  # Small delay between iterations
        results_queue.put({"thread_id": thread_id, "iterations": iterations, "errors": errors})

    def memory_monitor():
        while not stop_event.is_set():
            mem = get_resource_usage()
            memory_samples.append(mem)
            time.sleep(10)  # Sample every 10 seconds

    # Start memory monitor
    monitor_thread = threading.Thread(target=memory_monitor, daemon=True)
    monitor_thread.start()

    # Start worker threads
    print(f"\nStarting {num_threads} threads for {duration_seconds}s sustained load test...")
    threads = [threading.Thread(target=worker, args=(i,), daemon=True) for i in range(num_threads)]
    start_time = time.time()

    for t in threads:
        t.start()

    # Run for specified duration
    time.sleep(duration_seconds)
    stop_event.set()

    # Wait for threads
    for t in threads:
        t.join(timeout=10)

    elapsed = time.time() - start_time

    # Collect results
    total_iterations = 0
    total_errors = 0
    while not results_queue.empty():
        r = results_queue.get()
        total_iterations += r["iterations"]
        total_errors += r["errors"]

    # Analyze memory trend
    if len(memory_samples) >= 2:
        initial_mem = memory_samples[0]["rss_mb"]
        final_mem = memory_samples[-1]["rss_mb"]
        mem_growth = final_mem - initial_mem

        print(f"\nSustained load test results:")
        print(f"  Duration: {elapsed:.1f}s")
        print(f"  Total iterations: {total_iterations}")
        print(f"  Total errors: {total_errors}")
        print(f"  Throughput: {total_iterations/elapsed:.1f} qps")
        print(f"  Memory: {initial_mem:.1f}MB -> {final_mem:.1f}MB (delta: {mem_growth:+.1f}MB)")

        # Check for excessive memory growth (potential leak)
        # Allow up to 100MB growth for long test
        assert mem_growth < 100, f"Potential memory leak: {mem_growth:.1f}MB growth"

    error_rate = total_errors / total_iterations if total_iterations > 0 else 1
    assert error_rate < 0.05, f"Error rate too high in sustained test: {error_rate*100:.1f}%"

    print(f"[PASSED] 5-minute sustained load test")


# ============================================================================
# Complex Query Stress Tests
# ============================================================================


@pytest.mark.stress_threading
def test_complex_queries_multithreaded(stress_conn_str):
    """
    Test multi-threaded execution with complex queries.

    Tests with JOINs, aggregations, and larger result sets.
    """
    complex_query = """
        SELECT TOP 50
            o.name as object_name,
            o.type_desc,
            s.name as schema_name,
            o.create_date,
            o.modify_date
        FROM sys.objects o
        INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
        WHERE o.is_ms_shipped = 0
        ORDER BY o.create_date DESC
    """

    runner = MultiThreadedQueryRunner(
        conn_str=stress_conn_str, query=complex_query, enable_pooling=True, timeout_seconds=180
    )

    result = runner.run_parallel(num_threads=20, iterations_per_thread=15)

    assert not result.hung, "Complex query test hung"
    assert result.success, f"Complex query test failed with {result.total_errors} errors"
    print(f"[PASSED] Complex query multi-threaded test: {result.throughput_qps:.1f} qps")


# ============================================================================
# Resource Monitoring Tests
# ============================================================================


@pytest.mark.stress_threading
def test_resource_cleanup_after_stress(stress_conn_str):
    """
    Test that resources are properly cleaned up after stress.

    Runs a stress test and verifies resources return to baseline.
    """
    # Get baseline
    baseline = get_resource_usage()
    print(f"Baseline RSS: {baseline['rss_mb']} MB")

    # Run stress
    runner = MultiThreadedQueryRunner(
        conn_str=stress_conn_str, query="SELECT 1", enable_pooling=True, timeout_seconds=60
    )

    result = runner.run_parallel(num_threads=50, iterations_per_thread=20)

    # Force cleanup
    import gc

    gc.collect()
    mssql_python.pooling(enabled=False)
    time.sleep(2)
    gc.collect()

    # Check resources
    after = get_resource_usage()
    print(f"After stress RSS: {after['rss_mb']} MB")

    mem_delta = after["rss_mb"] - baseline["rss_mb"]
    print(f"Memory delta: {mem_delta:+.1f} MB")

    # Allow some memory growth but not excessive
    assert mem_delta < 50, f"Memory not properly released: {mem_delta:.1f}MB retained"
    print(f"[PASSED] Resource cleanup test")


# ============================================================================
# Parameterized Comprehensive Test
# ============================================================================


@pytest.mark.stress_threading
@pytest.mark.parametrize(
    "num_threads,iterations,pooling",
    [
        (2, 100, True),
        (2, 50, False),
        (5, 50, True),
        (5, 25, False),
        (10, 30, True),
        (50, 10, True),
        (100, 5, True),
    ],
)
def test_comprehensive_thread_scaling(stress_conn_str, num_threads, iterations, pooling):
    """
    Comprehensive parameterized test for thread scaling behavior.

    Tests various combinations of thread counts and pooling settings.
    """
    runner = MultiThreadedQueryRunner(
        conn_str=stress_conn_str,
        query="SELECT 1 as n, 'test' as s",
        enable_pooling=pooling,
        timeout_seconds=300,
    )

    result = runner.run_parallel(num_threads=num_threads, iterations_per_thread=iterations)

    assert not result.hung, f"Test hung: {num_threads} threads, pooling={pooling}"

    # Adaptive expectations based on thread count
    if num_threads <= 10:
        min_completion = 0.95
        max_error_rate = 0.05
    elif num_threads <= 50:
        min_completion = 0.90
        max_error_rate = 0.10
    else:
        min_completion = 0.80
        max_error_rate = 0.20

    completion_rate = result.total_iterations / (num_threads * iterations)
    error_rate = result.total_errors / result.total_iterations if result.total_iterations > 0 else 1

    assert (
        completion_rate >= min_completion
    ), f"Completion rate {completion_rate*100:.1f}% < {min_completion*100}%"
    assert error_rate <= max_error_rate, f"Error rate {error_rate*100:.1f}% > {max_error_rate*100}%"

    print(
        f"[PASSED] {num_threads}T x {iterations}I, pooling={pooling}: "
        f"{result.throughput_qps:.1f} qps, {error_rate*100:.1f}% errors"
    )
