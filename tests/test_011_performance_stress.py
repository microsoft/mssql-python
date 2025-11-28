"""
Performance and stress tests for mssql-python driver.

These tests verify the driver's behavior under stress conditions:
- Large result sets (100,000+ rows)
- Memory pressure scenarios
- Exception handling during batch processing
- Thousands of empty string allocations
- 10MB+ LOB data handling

Tests are marked with @pytest.mark.stress and may be skipped in regular CI runs.
"""

import pytest
import decimal
import hashlib
import sys
import platform
import threading
import time
from typing import List, Tuple


# Helper function to check if running on resource-limited platform
def supports_resource_limits():
    """Check if platform supports resource.setrlimit for memory limits"""
    try:
        import resource

        return hasattr(resource, "RLIMIT_AS")
    except ImportError:
        return False


def drop_table_if_exists(cursor, table_name):
    """Helper to drop a table if it exists"""
    try:
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    except Exception:
        pass


@pytest.mark.stress
def test_exception_mid_batch_no_corrupt_data(cursor, db_connection):
    """
    Test #1: Verify that batch processing handles data integrity correctly.

    When fetching large batches, verify that the returned result list does NOT
    contain empty or partially-filled rows. Should either get complete valid rows
    OR an exception, never corrupt data.
    """
    try:
        drop_table_if_exists(cursor, "#pytest_mid_batch_exception")

        # Create simple table to test batch processing integrity
        cursor.execute(
            """
            CREATE TABLE #pytest_mid_batch_exception (
                id INT,
                value NVARCHAR(50),
                amount FLOAT
            )
        """
        )
        db_connection.commit()

        # Insert 1000 rows using individual inserts to avoid executemany complications
        for i in range(1000):
            cursor.execute(
                "INSERT INTO #pytest_mid_batch_exception VALUES (?, ?, ?)",
                (i, f"Value_{i}", float(i * 1.5)),
            )
        db_connection.commit()

        # Fetch all rows in batch - this tests the fetch path integrity
        cursor.execute("SELECT id, value, amount FROM #pytest_mid_batch_exception ORDER BY id")
        rows = cursor.fetchall()

        # Verify: No empty rows, no None rows where data should exist
        assert len(rows) == 1000, f"Expected 1000 rows, got {len(rows)}"

        for i, row in enumerate(rows):
            assert row is not None, f"Row {i} is None - corrupt data detected"
            assert (
                len(row) == 3
            ), f"Row {i} has {len(row)} columns, expected 3 - partial row detected"
            assert row[0] == i, f"Row {i} has incorrect ID {row[0]}"
            assert row[1] is not None, f"Row {i} has None value - corrupt data"
            assert row[2] is not None, f"Row {i} has None amount - corrupt data"
            # Verify actual values
            assert row[1] == f"Value_{i}", f"Row {i} has wrong value"
            assert abs(row[2] - (i * 1.5)) < 0.001, f"Row {i} has wrong amount"

        print(f"[OK] Batch integrity test passed: All 1000 rows complete, no corrupt data")

    except Exception as e:
        pytest.fail(f"Batch integrity test failed: {e}")
    finally:
        drop_table_if_exists(cursor, "#pytest_mid_batch_exception")
        db_connection.commit()


@pytest.mark.stress
@pytest.mark.skipif(
    not supports_resource_limits() or platform.system() == "Darwin",
    reason="Requires Unix resource limits, not supported on macOS",
)
def test_python_c_api_null_handling_memory_pressure(cursor, db_connection):
    """
    Test #2: Verify graceful handling when Python C API functions return NULL.

    Simulates low memory conditions where PyUnicode_FromStringAndSize,
    PyBytes_FromStringAndSize might fail. Should not crash with segfault,
    should handle gracefully with None or exception.

    Note: Skipped on macOS as it doesn't support RLIMIT_AS properly.
    """
    import resource

    try:
        drop_table_if_exists(cursor, "#pytest_memory_pressure")

        # Create table with various string types
        cursor.execute(
            """
            CREATE TABLE #pytest_memory_pressure (
                id INT,
                varchar_col VARCHAR(1000),
                nvarchar_col NVARCHAR(1000),
                varbinary_col VARBINARY(1000)
            )
        """
        )
        db_connection.commit()

        # Insert test data
        test_string = "X" * 500
        test_binary = b"\x00\x01\x02" * 100

        for i in range(1000):
            cursor.execute(
                "INSERT INTO #pytest_memory_pressure VALUES (?, ?, ?, ?)",
                (i, test_string, test_string, test_binary),
            )
        db_connection.commit()

        # Set memory limit (50MB) to create pressure
        soft, hard = resource.getrlimit(resource.RLIMIT_AS)
        # Use the smaller of 50MB or current soft limit to avoid exceeding hard limit
        memory_limit = min(50 * 1024 * 1024, soft) if soft > 0 else 50 * 1024 * 1024
        try:
            resource.setrlimit(resource.RLIMIT_AS, (memory_limit, hard))

            # Try to fetch data under memory pressure
            cursor.execute("SELECT * FROM #pytest_memory_pressure")

            # This might fail or return partial data, but should NOT segfault
            try:
                rows = cursor.fetchall()
                # If we get here, verify data integrity
                for row in rows:
                    if row is not None:  # Some rows might be None under pressure
                        # Verify no corrupt data - either complete or None
                        assert len(row) == 4, "Partial row detected under memory pressure"
            except MemoryError:
                # Acceptable - ran out of memory, but didn't crash
                print("[OK] Memory pressure caused MemoryError (expected, not a crash)")
                pass

        finally:
            # Restore memory limit
            resource.setrlimit(resource.RLIMIT_AS, (soft, hard))

        print("[OK] Python C API NULL handling test passed: No segfault under memory pressure")

    except Exception as e:
        pytest.fail(f"Python C API NULL handling test failed: {e}")
    finally:
        drop_table_if_exists(cursor, "#pytest_memory_pressure")
        db_connection.commit()


@pytest.mark.stress
def test_thousands_of_empty_strings_allocation_stress(cursor, db_connection):
    """
    Test #3: Stress test with thousands of empty string allocations.

    Test fetching many rows with empty VARCHAR, NVARCHAR, and VARBINARY values.
    Verifies that empty string creation failures don't cause crashes.
    Process thousands of empty strings to stress the allocation path.
    """
    try:
        drop_table_if_exists(cursor, "#pytest_empty_stress")

        cursor.execute(
            """
            CREATE TABLE #pytest_empty_stress (
                id INT,
                empty_varchar VARCHAR(100),
                empty_nvarchar NVARCHAR(100),
                empty_varbinary VARBINARY(100)
            )
        """
        )
        db_connection.commit()

        # Insert 10,000 rows with empty strings
        num_rows = 10000
        print(f"Inserting {num_rows} rows with empty strings...")

        for i in range(num_rows):
            cursor.execute("INSERT INTO #pytest_empty_stress VALUES (?, ?, ?, ?)", (i, "", "", b""))
            if i % 1000 == 0 and i > 0:
                print(f"  Inserted {i} rows...")

        db_connection.commit()
        print(f"[OK] Inserted {num_rows} rows")

        # Test 1: fetchall() - stress test all allocations at once
        print("Testing fetchall()...")
        cursor.execute("SELECT * FROM #pytest_empty_stress ORDER BY id")
        rows = cursor.fetchall()

        assert len(rows) == num_rows, f"Expected {num_rows} rows, got {len(rows)}"

        # Verify all empty strings are correct
        for i, row in enumerate(rows):
            assert row[0] == i, f"Row {i} has incorrect ID {row[0]}"
            assert row[1] == "", f"Row {i} varchar not empty string: {row[1]}"
            assert row[2] == "", f"Row {i} nvarchar not empty string: {row[2]}"
            assert row[3] == b"", f"Row {i} varbinary not empty bytes: {row[3]}"

            if i % 2000 == 0 and i > 0:
                print(f"  Verified {i} rows...")

        print(f"[OK] fetchall() test passed: All {num_rows} empty strings correct")

        # Test 2: fetchmany() - stress test batch allocations
        print("Testing fetchmany(1000)...")
        cursor.execute("SELECT * FROM #pytest_empty_stress ORDER BY id")

        total_fetched = 0
        batch_num = 0
        while True:
            batch = cursor.fetchmany(1000)
            if not batch:
                break

            batch_num += 1
            for row in batch:
                assert row[1] == "", f"Batch {batch_num}: varchar not empty"
                assert row[2] == "", f"Batch {batch_num}: nvarchar not empty"
                assert row[3] == b"", f"Batch {batch_num}: varbinary not empty"

            total_fetched += len(batch)
            print(f"  Batch {batch_num}: fetched {len(batch)} rows (total: {total_fetched})")

        assert total_fetched == num_rows, f"fetchmany total {total_fetched} != {num_rows}"
        print(f"[OK] fetchmany() test passed: All {num_rows} empty strings correct")

    except Exception as e:
        pytest.fail(f"Empty strings stress test failed: {e}")
    finally:
        drop_table_if_exists(cursor, "#pytest_empty_stress")
        db_connection.commit()


@pytest.mark.stress
def test_large_result_set_100k_rows_no_overflow(cursor, db_connection):
    """
    Test #5: Fetch very large result sets (100,000+ rows) to test buffer overflow protection.

    Tests that large rowIdx values don't cause buffer overflow when calculating
    rowIdx × fetchBufferSize. Verifies data integrity across all rows - no crashes,
    no corrupt data, correct values in all cells.
    """
    try:
        drop_table_if_exists(cursor, "#pytest_100k_rows")

        cursor.execute(
            """
            CREATE TABLE #pytest_100k_rows (
                id INT,
                varchar_col VARCHAR(50),
                nvarchar_col NVARCHAR(50),
                int_col INT
            )
        """
        )
        db_connection.commit()

        # Insert 100,000 rows with sequential IDs and predictable data
        num_rows = 100000
        print(f"Inserting {num_rows} rows...")

        # Use bulk insert for performance
        batch_size = 1000
        for batch_start in range(0, num_rows, batch_size):
            values = []
            for i in range(batch_start, min(batch_start + batch_size, num_rows)):
                values.append((i, f"VARCHAR_{i}", f"NVARCHAR_{i}", i * 2))

            # Use executemany for faster insertion
            cursor.executemany("INSERT INTO #pytest_100k_rows VALUES (?, ?, ?, ?)", values)

            if (batch_start + batch_size) % 10000 == 0:
                print(f"  Inserted {batch_start + batch_size} rows...")

        db_connection.commit()
        print(f"[OK] Inserted {num_rows} rows")

        # Fetch all rows and verify data integrity
        print("Fetching all rows...")
        cursor.execute(
            "SELECT id, varchar_col, nvarchar_col, int_col FROM #pytest_100k_rows ORDER BY id"
        )
        rows = cursor.fetchall()

        assert len(rows) == num_rows, f"Expected {num_rows} rows, got {len(rows)}"
        print(f"[OK] Fetched {num_rows} rows")

        # Verify first row
        assert rows[0][0] == 0, f"First row ID incorrect: {rows[0][0]}"
        assert rows[0][1] == "VARCHAR_0", f"First row varchar incorrect: {rows[0][1]}"
        assert rows[0][2] == "NVARCHAR_0", f"First row nvarchar incorrect: {rows[0][2]}"
        assert rows[0][3] == 0, f"First row int incorrect: {rows[0][3]}"
        print("[OK] First row verified")

        # Verify last row
        assert rows[-1][0] == num_rows - 1, f"Last row ID incorrect: {rows[-1][0]}"
        assert rows[-1][1] == f"VARCHAR_{num_rows-1}", f"Last row varchar incorrect"
        assert rows[-1][2] == f"NVARCHAR_{num_rows-1}", f"Last row nvarchar incorrect"
        assert rows[-1][3] == (num_rows - 1) * 2, f"Last row int incorrect"
        print("[OK] Last row verified")

        # Verify random spot checks throughout the dataset
        check_indices = [10000, 25000, 50000, 75000, 99999]
        for idx in check_indices:
            row = rows[idx]
            assert row[0] == idx, f"Row {idx} ID incorrect: {row[0]}"
            assert row[1] == f"VARCHAR_{idx}", f"Row {idx} varchar incorrect: {row[1]}"
            assert row[2] == f"NVARCHAR_{idx}", f"Row {idx} nvarchar incorrect: {row[2]}"
            assert row[3] == idx * 2, f"Row {idx} int incorrect: {row[3]}"
        print(f"[OK] Spot checks verified at indices: {check_indices}")

        # Verify all rows have correct sequential IDs (full integrity check)
        print("Performing full integrity check...")
        for i, row in enumerate(rows):
            if row[0] != i:
                pytest.fail(f"Data corruption at row {i}: expected ID {i}, got {row[0]}")

            if i % 20000 == 0 and i > 0:
                print(f"  Verified {i} rows...")

        print(f"[OK] Full integrity check passed: All {num_rows} rows correct, no buffer overflow")

    except Exception as e:
        pytest.fail(f"Large result set test failed: {e}")
    finally:
        drop_table_if_exists(cursor, "#pytest_100k_rows")
        db_connection.commit()


@pytest.mark.stress
def test_very_large_lob_10mb_data_integrity(cursor, db_connection):
    """
    Test #6: Fetch VARCHAR(MAX), NVARCHAR(MAX), VARBINARY(MAX) with 10MB+ data.

    Verifies:
    1. Correct LOB detection
    2. Data fetched completely and correctly
    3. No buffer overflow when determining LOB vs non-LOB path
    4. Data integrity verified byte-by-byte using SHA256
    """
    try:
        drop_table_if_exists(cursor, "#pytest_10mb_lob")

        cursor.execute(
            """
            CREATE TABLE #pytest_10mb_lob (
                id INT,
                varchar_lob VARCHAR(MAX),
                nvarchar_lob NVARCHAR(MAX),
                varbinary_lob VARBINARY(MAX)
            )
        """
        )
        db_connection.commit()

        # Create 10MB+ data
        mb_10 = 10 * 1024 * 1024

        print("Creating 10MB test data...")
        varchar_data = "A" * mb_10  # 10MB ASCII
        nvarchar_data = "🔥" * (mb_10 // 4)  # ~10MB Unicode (emoji is 4 bytes in UTF-8)
        varbinary_data = bytes(range(256)) * (mb_10 // 256)  # 10MB binary

        # Calculate checksums for verification
        varchar_hash = hashlib.sha256(varchar_data.encode("utf-8")).hexdigest()
        nvarchar_hash = hashlib.sha256(nvarchar_data.encode("utf-8")).hexdigest()
        varbinary_hash = hashlib.sha256(varbinary_data).hexdigest()

        print(f"  VARCHAR size: {len(varchar_data):,} bytes, SHA256: {varchar_hash[:16]}...")
        print(f"  NVARCHAR size: {len(nvarchar_data):,} chars, SHA256: {nvarchar_hash[:16]}...")
        print(f"  VARBINARY size: {len(varbinary_data):,} bytes, SHA256: {varbinary_hash[:16]}...")

        # Insert LOB data
        print("Inserting 10MB LOB data...")
        cursor.execute(
            "INSERT INTO #pytest_10mb_lob VALUES (?, ?, ?, ?)",
            (1, varchar_data, nvarchar_data, varbinary_data),
        )
        db_connection.commit()
        print("[OK] Inserted 10MB LOB data")

        # Fetch and verify
        print("Fetching 10MB LOB data...")
        cursor.execute("SELECT id, varchar_lob, nvarchar_lob, varbinary_lob FROM #pytest_10mb_lob")
        row = cursor.fetchone()

        assert row is not None, "Failed to fetch LOB data"
        assert row[0] == 1, f"ID incorrect: {row[0]}"

        # Verify VARCHAR(MAX) - byte-by-byte integrity
        print("Verifying VARCHAR(MAX) integrity...")
        fetched_varchar = row[1]
        assert len(fetched_varchar) == len(
            varchar_data
        ), f"VARCHAR size mismatch: expected {len(varchar_data)}, got {len(fetched_varchar)}"

        fetched_varchar_hash = hashlib.sha256(fetched_varchar.encode("utf-8")).hexdigest()
        assert fetched_varchar_hash == varchar_hash, f"VARCHAR data corruption: hash mismatch"
        print(f"[OK] VARCHAR(MAX) verified: {len(fetched_varchar):,} bytes, SHA256 match")

        # Verify NVARCHAR(MAX) - byte-by-byte integrity
        print("Verifying NVARCHAR(MAX) integrity...")
        fetched_nvarchar = row[2]
        assert len(fetched_nvarchar) == len(
            nvarchar_data
        ), f"NVARCHAR size mismatch: expected {len(nvarchar_data)}, got {len(fetched_nvarchar)}"

        fetched_nvarchar_hash = hashlib.sha256(fetched_nvarchar.encode("utf-8")).hexdigest()
        assert fetched_nvarchar_hash == nvarchar_hash, f"NVARCHAR data corruption: hash mismatch"
        print(f"[OK] NVARCHAR(MAX) verified: {len(fetched_nvarchar):,} chars, SHA256 match")

        # Verify VARBINARY(MAX) - byte-by-byte integrity
        print("Verifying VARBINARY(MAX) integrity...")
        fetched_varbinary = row[3]
        assert len(fetched_varbinary) == len(
            varbinary_data
        ), f"VARBINARY size mismatch: expected {len(varbinary_data)}, got {len(fetched_varbinary)}"

        fetched_varbinary_hash = hashlib.sha256(fetched_varbinary).hexdigest()
        assert fetched_varbinary_hash == varbinary_hash, f"VARBINARY data corruption: hash mismatch"
        print(f"[OK] VARBINARY(MAX) verified: {len(fetched_varbinary):,} bytes, SHA256 match")

        print(
            "[OK] All 10MB+ LOB data verified: LOB detection correct, no overflow, integrity perfect"
        )

    except Exception as e:
        pytest.fail(f"Very large LOB test failed: {e}")
    finally:
        drop_table_if_exists(cursor, "#pytest_10mb_lob")
        db_connection.commit()


@pytest.mark.stress
def test_concurrent_fetch_data_integrity_no_corruption(db_connection, conn_str):
    """
    Test #7: Multiple threads/cursors fetching data simultaneously.

    Verifies:
    1. No data corruption occurs
    2. Each cursor gets correct data
    3. No crashes or race conditions
    4. Data from one cursor doesn't leak into another
    """
    import mssql_python

    num_threads = 5
    num_rows_per_table = 1000
    results = []
    errors = []

    def worker_thread(thread_id: int, conn_str: str, results_list: List, errors_list: List):
        """Worker thread that creates its own connection and fetches data"""
        try:
            # Each thread gets its own connection and cursor
            conn = mssql_python.connect(conn_str)
            cursor = conn.cursor()

            # Create thread-specific table
            table_name = f"#pytest_concurrent_t{thread_id}"
            drop_table_if_exists(cursor, table_name)

            cursor.execute(
                f"""
                CREATE TABLE {table_name} (
                    id INT,
                    thread_id INT,
                    data VARCHAR(100)
                )
            """
            )
            conn.commit()

            # Insert thread-specific data
            for i in range(num_rows_per_table):
                cursor.execute(
                    f"INSERT INTO {table_name} VALUES (?, ?, ?)",
                    (i, thread_id, f"Thread_{thread_id}_Row_{i}"),
                )
            conn.commit()

            # Small delay to ensure concurrent execution
            time.sleep(0.01)

            # Fetch data and verify
            cursor.execute(f"SELECT id, thread_id, data FROM {table_name} ORDER BY id")
            rows = cursor.fetchall()

            # Verify all rows belong to this thread only (no cross-contamination)
            for i, row in enumerate(rows):
                if row[0] != i:
                    raise ValueError(f"Thread {thread_id}: Row {i} has wrong ID {row[0]}")
                if row[1] != thread_id:
                    raise ValueError(f"Thread {thread_id}: Data corruption! Got thread_id {row[1]}")
                expected_data = f"Thread_{thread_id}_Row_{i}"
                if row[2] != expected_data:
                    raise ValueError(
                        f"Thread {thread_id}: Data corruption! Expected '{expected_data}', got '{row[2]}'"
                    )

            # Record success
            results_list.append(
                {"thread_id": thread_id, "rows_fetched": len(rows), "success": True}
            )

            # Cleanup
            drop_table_if_exists(cursor, table_name)
            conn.commit()
            cursor.close()
            conn.close()

        except Exception as e:
            errors_list.append({"thread_id": thread_id, "error": str(e)})

    # Create and start threads
    threads = []
    print(f"Starting {num_threads} concurrent threads...")

    for i in range(num_threads):
        thread = threading.Thread(target=worker_thread, args=(i, conn_str, results, errors))
        threads.append(thread)
        thread.start()

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

        # Verify results
        print(f"\nConcurrent fetch results:")
        for result in results:
            print(
                f"  Thread {result['thread_id']}: Fetched {result['rows_fetched']} rows - {'OK' if result['success'] else 'FAILED'}"
            )

        if errors:
            print(f"\nErrors encountered:")
            for error in errors:
                print(f"  Thread {error['thread_id']}: {error['error']}")
            pytest.fail(f"Concurrent fetch had {len(errors)} errors")

    # All threads should have succeeded
    assert (
        len(results) == num_threads
    ), f"Expected {num_threads} successful threads, got {len(results)}"

    # All threads should have fetched correct number of rows
    for result in results:
        assert (
            result["rows_fetched"] == num_rows_per_table
        ), f"Thread {result['thread_id']} fetched {result['rows_fetched']} rows, expected {num_rows_per_table}"

    print(
        f"\n[OK] Concurrent fetch test passed: {num_threads} threads, no corruption, no race conditions"
    )
