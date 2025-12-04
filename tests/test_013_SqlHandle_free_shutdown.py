"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.

Comprehensive test suite for SqlHandle::free() behavior during Python shutdown.

This test validates the critical fix in ddbc_bindings.cpp SqlHandle::free() method
that prevents segfaults when Python is shutting down by skipping handle cleanup
for STMT (Type 3) and DBC (Type 2) handles whose parents may already be freed.

Handle Hierarchy:
- ENV (Type 1, SQL_HANDLE_ENV) - Static singleton, no parent
- DBC (Type 2, SQL_HANDLE_DBC) - Per connection, parent is ENV
- STMT (Type 3, SQL_HANDLE_STMT) - Per cursor, parent is DBC

Protection Logic:
- During Python shutdown (pythonShuttingDown=true):
  * Type 3 (STMT) handles: Skip SQLFreeHandle (parent DBC may be freed)
  * Type 2 (DBC) handles: Skip SQLFreeHandle (parent static ENV may be destructing)
  * Type 1 (ENV) handles: Normal cleanup (no parent, static lifetime)

Test Strategy:
- Use subprocess isolation to test actual Python interpreter shutdown
- Verify no segfaults occur when handles are freed during shutdown
- Test all three handle types with various cleanup scenarios
"""

import os
import subprocess
import sys
import textwrap


class TestHandleFreeShutdown:
    """Test SqlHandle::free() behavior for all handle types during Python shutdown."""

    def test_aggressive_dbc_segfault_reproduction(self, conn_str):
        """
        AGGRESSIVE TEST: Try to reproduce DBC handle segfault during shutdown.

        This test aggressively attempts to trigger the segfault described in the stack trace
        by creating many DBC handles and forcing Python to shut down while they're still alive.

        Current vulnerability: DBC handles (Type 2) are NOT protected during shutdown,
        so they will call SQLFreeHandle during finalization, potentially accessing
        the already-destructed static ENV handle.

        Expected with CURRENT CODE: May segfault (this is the bug we're testing for)
        Expected with FIXED CODE: No segfault
        """
        script = textwrap.dedent(
            f"""
            import sys
            import gc
            from mssql_python import connect
            
            print("=== AGGRESSIVE DBC SEGFAULT TEST ===")
            print("Creating many DBC handles and forcing shutdown...")
            
            # Create many connections without closing them
            # This maximizes the chance of DBC handles being finalized
            # AFTER the static ENV handle has destructed
            connections = []
            for i in range(10):  # Reduced from 20 to avoid timeout
                conn = connect("{conn_str}")
                # Don't even create cursors - just DBC handles
                connections.append(conn)
                if i % 3 == 0:
                    print(f"Created {{i+1}} connections...")
            
            print(f"Created {{len(connections)}} DBC handles")
            print("Forcing GC to ensure objects are tracked...")
            gc.collect()
            
            # Delete the list but objects are still alive in GC
            del connections
            
            print("WARNING: About to exit with unclosed DBC handles")
            print("If Type 2 (DBC) handles are not protected, this may SEGFAULT")
            print("Stack trace will show: SQLFreeHandle -> SqlHandle::free() -> finalize_garbage")
            
            # Force immediate exit - this triggers finalize_garbage
            sys.exit(0)
        """
        )

        result = subprocess.run(
            [sys.executable, "-c", script], capture_output=True, text=True, timeout=30
        )

        # Check for segfault
        if result.returncode < 0:
            signal_num = -result.returncode
            print(f"⚠ SEGFAULT DETECTED! Process killed by signal {signal_num} (likely SIGSEGV=11)")
            print(f"stderr: {result.stderr}")
            print(f"This confirms DBC handles (Type 2) need protection during shutdown")
            assert (
                False
            ), f"SEGFAULT reproduced with signal {signal_num} - DBC handles not protected"
        else:
            assert result.returncode == 0, f"Process failed. stderr: {result.stderr}"
            assert "Created 10 DBC handles" in result.stdout
            print(f"✓ No segfault - DBC handles properly protected during shutdown")

    def test_dbc_handle_outlives_env_handle(self, conn_str):
        """
        TEST: Reproduce scenario where DBC handle outlives ENV handle.

        The static ENV handle destructs during C++ static destruction phase.
        If DBC handles are finalized by Python GC AFTER ENV is gone,
        SQLFreeHandle will crash trying to access the freed ENV handle.

        Expected with CURRENT CODE: Likely segfault
        Expected with FIXED CODE: No segfault
        """
        script = textwrap.dedent(
            f"""
            import sys
            import atexit
            from mssql_python import connect
            
            print("=== DBC OUTLIVES ENV TEST ===")
            
            # Create connection in global scope
            global_conn = connect("{conn_str}")
            print("Created global DBC handle")
            
            def on_exit():
                print("atexit handler: Python is shutting down")
                print("ENV handle (static) may already be destructing")
                print("DBC handle still alive - this is dangerous!")
            
            atexit.register(on_exit)
            
            # Don't close connection - let it be finalized during shutdown
            print("Exiting without closing DBC handle")
            print("Python GC will finalize DBC during shutdown")
            print("If DBC cleanup isn't skipped, SQLFreeHandle will access freed ENV")
            sys.exit(0)
        """
        )

        result = subprocess.run(
            [sys.executable, "-c", script], capture_output=True, text=True, timeout=30
        )

        if result.returncode < 0:
            signal_num = -result.returncode
            print(f"⚠ SEGFAULT DETECTED! Process killed by signal {signal_num}")
            print(f"This confirms DBC outlived ENV handle")
            assert False, f"SEGFAULT: DBC handle outlived ENV handle, signal {signal_num}"
        else:
            assert result.returncode == 0, f"Process failed. stderr: {result.stderr}"
            print(f"✓ DBC handle cleanup properly skipped during shutdown")

    def test_force_gc_finalization_order_issue(self, conn_str):
        """
        TEST: Force specific GC finalization order to trigger segfault.

        By creating objects in specific order and forcing GC cycles,
        we try to ensure DBC handles are finalized after ENV handle destruction.

        Expected with CURRENT CODE: May segfault
        Expected with FIXED CODE: No segfault
        """
        script = textwrap.dedent(
            f"""
            import sys
            import gc
            import weakref
            from mssql_python import connect
            
            print("=== FORCED GC FINALIZATION ORDER TEST ===")
            
            # Create many connections
            connections = []
            weakrefs = []
            
            for i in range(10):  # Reduced from 15 to avoid timeout
                conn = connect("{conn_str}")
                wr = weakref.ref(conn)
                connections.append(conn)
                weakrefs.append(wr)
            
            print(f"Created {{len(connections)}} connections with weakrefs")
            
            # Force GC to track these objects
            gc.collect()
            
            # Delete strong references
            del connections
            
            # Force multiple GC cycles
            print("Forcing GC cycles...")
            for i in range(5):
                collected = gc.collect()
                print(f"GC cycle {{i+1}}: collected {{collected}} objects")
            
            # Check weakrefs
            alive = sum(1 for wr in weakrefs if wr() is not None)
            print(f"Weakrefs still alive: {{alive}}")
            
            print("Exiting - finalize_garbage will be called")
            print("If DBC handles aren't protected, segfault in SQLFreeHandle")
            sys.exit(0)
        """
        )

        result = subprocess.run(
            [sys.executable, "-c", script], capture_output=True, text=True, timeout=30
        )

        if result.returncode < 0:
            signal_num = -result.returncode
            print(f"⚠ SEGFAULT DETECTED! Process killed by signal {signal_num}")
            assert False, f"SEGFAULT during forced GC finalization, signal {signal_num}"
        else:
            assert result.returncode == 0, f"Process failed. stderr: {result.stderr}"
            print(f"✓ Forced GC finalization order handled safely")

    def test_stmt_handle_cleanup_at_shutdown(self, conn_str):
        """
        Test STMT handle (Type 3) cleanup during Python shutdown.

        Scenario:
        1. Create connection and cursor
        2. Execute query (creates STMT handle)
        3. Let Python shutdown without explicit cleanup
        4. STMT handle's __del__ should skip SQLFreeHandle during shutdown

        Expected: No segfault, clean exit
        """
        script = textwrap.dedent(
            f"""
            import sys
            from mssql_python import connect
            
            # Create connection and cursor with active STMT handle
            conn = connect("{conn_str}")
            cursor = conn.cursor()
            cursor.execute("SELECT 1 AS test_value")
            result = cursor.fetchall()
            print(f"Query result: {{result}}")
            
            # Intentionally skip cleanup - let Python shutdown handle it
            # This will trigger SqlHandle::free() during Python finalization
            # Type 3 (STMT) handle should be skipped when pythonShuttingDown=true
            print("STMT handle cleanup test: Exiting without explicit cleanup")
            sys.exit(0)
        """
        )

        result = subprocess.run(
            [sys.executable, "-c", script], capture_output=True, text=True, timeout=30
        )

        assert result.returncode == 0, f"Process crashed. stderr: {result.stderr}"
        assert "STMT handle cleanup test: Exiting without explicit cleanup" in result.stdout
        assert "Query result: [(1,)]" in result.stdout
        print(f"✓ STMT handle (Type 3) cleanup during shutdown: PASSED")

    def test_dbc_handle_cleanup_at_shutdown(self, conn_str):
        """
        Test DBC handle (Type 2) cleanup during Python shutdown.

        Scenario:
        1. Create multiple connections (multiple DBC handles)
        2. Close cursors but leave connections open
        3. Let Python shutdown without closing connections
        4. DBC handles' __del__ should skip SQLFreeHandle during shutdown

        Expected: No segfault, clean exit
        """
        script = textwrap.dedent(
            f"""
            import sys
            from mssql_python import connect
            
            # Create multiple connections (DBC handles)
            connections = []
            for i in range(3):
                conn = connect("{conn_str}")
                cursor = conn.cursor()
                cursor.execute(f"SELECT {{i}} AS test_value")
                result = cursor.fetchall()
                cursor.close()  # Close cursor, but keep connection
                connections.append(conn)
                print(f"Connection {{i}}: created and cursor closed")
            
            # Intentionally skip connection cleanup
            # This will trigger SqlHandle::free() for DBC handles during shutdown
            # Type 2 (DBC) handles should be skipped when pythonShuttingDown=true
            print("DBC handle cleanup test: Exiting without explicit connection cleanup")
            sys.exit(0)
        """
        )

        result = subprocess.run(
            [sys.executable, "-c", script], capture_output=True, text=True, timeout=30
        )

        assert result.returncode == 0, f"Process crashed. stderr: {result.stderr}"
        assert (
            "DBC handle cleanup test: Exiting without explicit connection cleanup" in result.stdout
        )
        assert "Connection 0: created and cursor closed" in result.stdout
        assert "Connection 1: created and cursor closed" in result.stdout
        assert "Connection 2: created and cursor closed" in result.stdout
        print(f"✓ DBC handle (Type 2) cleanup during shutdown: PASSED")

    def test_env_handle_cleanup_at_shutdown(self, conn_str):
        """
        Test ENV handle (Type 1) cleanup during Python shutdown.

        Scenario:
        1. Create and close connections (ENV handle is static singleton)
        2. Let Python shutdown
        3. ENV handle is static and should follow normal C++ destruction
        4. ENV handle should NOT be skipped (no protection needed)

        Expected: No segfault, clean exit
        Note: ENV handle is static and destructs via normal C++ mechanisms,
              not during Python GC. This test verifies the overall flow.
        """
        script = textwrap.dedent(
            f"""
            import sys
            from mssql_python import connect
            
            # Create and properly close connections
            # ENV handle is static singleton shared across all connections
            for i in range(3):
                conn = connect("{conn_str}")
                cursor = conn.cursor()
                cursor.execute(f"SELECT {{i}} AS test_value")
                cursor.fetchall()
                cursor.close()
                conn.close()
                print(f"Connection {{i}}: properly closed")
            
            # ENV handle is static and will destruct via C++ static destruction
            # It does NOT have pythonShuttingDown protection (Type 1 not in check)
            print("ENV handle cleanup test: All connections closed properly")
            sys.exit(0)
        """
        )

        result = subprocess.run(
            [sys.executable, "-c", script], capture_output=True, text=True, timeout=30
        )

        assert result.returncode == 0, f"Process crashed. stderr: {result.stderr}"
        assert "ENV handle cleanup test: All connections closed properly" in result.stdout
        assert "Connection 0: properly closed" in result.stdout
        assert "Connection 1: properly closed" in result.stdout
        assert "Connection 2: properly closed" in result.stdout
        print(f"✓ ENV handle (Type 1) cleanup during shutdown: PASSED")

    def test_mixed_handle_cleanup_at_shutdown(self, conn_str):
        """
        Test mixed scenario with all handle types during shutdown.

        Scenario:
        1. Create multiple connections (DBC handles)
        2. Create multiple cursors per connection (STMT handles)
        3. Some cursors closed, some left open
        4. Some connections closed, some left open
        5. Let Python shutdown handle the rest

        Expected: No segfault, clean exit
        This tests the real-world scenario where cleanup is partial
        """
        script = textwrap.dedent(
            f"""
            import sys
            from mssql_python import connect
            
            connections = []
            
            # Connection 1: Everything left open
            conn1 = connect("{conn_str}")
            cursor1a = conn1.cursor()
            cursor1a.execute("SELECT 1 AS test")
            cursor1a.fetchall()
            cursor1b = conn1.cursor()
            cursor1b.execute("SELECT 2 AS test")
            cursor1b.fetchall()
            connections.append((conn1, [cursor1a, cursor1b]))
            print("Connection 1: cursors left open")
            
            # Connection 2: Cursors closed, connection left open
            conn2 = connect("{conn_str}")
            cursor2a = conn2.cursor()
            cursor2a.execute("SELECT 3 AS test")
            cursor2a.fetchall()
            cursor2a.close()
            cursor2b = conn2.cursor()
            cursor2b.execute("SELECT 4 AS test")
            cursor2b.fetchall()
            cursor2b.close()
            connections.append((conn2, []))
            print("Connection 2: cursors closed, connection left open")
            
            # Connection 3: Everything properly closed
            conn3 = connect("{conn_str}")
            cursor3a = conn3.cursor()
            cursor3a.execute("SELECT 5 AS test")
            cursor3a.fetchall()
            cursor3a.close()
            conn3.close()
            print("Connection 3: everything properly closed")
            
            # Let Python shutdown with mixed cleanup state
            # - Type 3 (STMT) handles from conn1 cursors: skipped during shutdown
            # - Type 2 (DBC) handles from conn1, conn2: skipped during shutdown
            # - Type 1 (ENV) handle: normal C++ static destruction
            print("Mixed handle cleanup test: Exiting with partial cleanup")
            sys.exit(0)
        """
        )

        result = subprocess.run(
            [sys.executable, "-c", script], capture_output=True, text=True, timeout=30
        )

        assert result.returncode == 0, f"Process crashed. stderr: {result.stderr}"
        assert "Mixed handle cleanup test: Exiting with partial cleanup" in result.stdout
        assert "Connection 1: cursors left open" in result.stdout
        assert "Connection 2: cursors closed, connection left open" in result.stdout
        assert "Connection 3: everything properly closed" in result.stdout
        print(f"✓ Mixed handle cleanup during shutdown: PASSED")

    def test_rapid_connection_churn_with_shutdown(self, conn_str):
        """
        Test rapid connection creation/deletion followed by shutdown.

        Scenario:
        1. Create many connections rapidly
        2. Delete some connections explicitly
        3. Leave others for Python GC
        4. Trigger shutdown

        Expected: No segfault, proper handle cleanup order
        """
        script = textwrap.dedent(
            f"""
            import sys
            import gc
            from mssql_python import connect
            
            # Create and delete connections rapidly
            for i in range(10):
                conn = connect("{conn_str}")
                cursor = conn.cursor()
                cursor.execute(f"SELECT {{i}} AS test")
                cursor.fetchall()
                
                # Close every other cursor
                if i % 2 == 0:
                    cursor.close()
                    conn.close()
                # Leave odd-numbered connections open
            
            print("Created 10 connections, closed 5 explicitly")
            
            # Force GC before shutdown
            gc.collect()
            print("GC triggered before shutdown")
            
            # Shutdown with 5 connections still "open" (not explicitly closed)
            # Their DBC and STMT handles will be skipped during shutdown
            print("Rapid churn test: Exiting with mixed cleanup")
            sys.exit(0)
        """
        )

        result = subprocess.run(
            [sys.executable, "-c", script], capture_output=True, text=True, timeout=30
        )

        assert result.returncode == 0, f"Process crashed. stderr: {result.stderr}"
        assert "Created 10 connections, closed 5 explicitly" in result.stdout
        assert "Rapid churn test: Exiting with mixed cleanup" in result.stdout
        print(f"✓ Rapid connection churn with shutdown: PASSED")

    def test_exception_during_query_with_shutdown(self, conn_str):
        """
        Test handle cleanup when exception occurs during query execution.

        Scenario:
        1. Create connection and cursor
        2. Execute query that causes exception
        3. Exception leaves handles in inconsistent state
        4. Let Python shutdown clean up

        Expected: No segfault, graceful error handling
        """
        script = textwrap.dedent(
            f"""
            import sys
            from mssql_python import connect, ProgrammingError
            
            conn = connect("{conn_str}")
            cursor = conn.cursor()
            
            try:
                # This will fail - invalid SQL
                cursor.execute("SELECT * FROM NonExistentTable123456")
            except ProgrammingError as e:
                print(f"Expected error occurred: {{type(e).__name__}}")
                # Intentionally don't close cursor or connection
            
            print("Exception test: Exiting after exception without cleanup")
            sys.exit(0)
        """
        )

        result = subprocess.run(
            [sys.executable, "-c", script], capture_output=True, text=True, timeout=30
        )

        assert result.returncode == 0, f"Process crashed. stderr: {result.stderr}"
        assert "Expected error occurred: ProgrammingError" in result.stdout
        assert "Exception test: Exiting after exception without cleanup" in result.stdout
        print(f"✓ Exception during query with shutdown: PASSED")

    def test_weakref_cleanup_at_shutdown(self, conn_str):
        """
        Test handle cleanup when using weakrefs during shutdown.

        Scenario:
        1. Create connections with weakref monitoring
        2. Delete strong references
        3. Let weakrefs and Python shutdown interact

        Expected: No segfault, proper weakref finalization
        """
        script = textwrap.dedent(
            f"""
            import sys
            import weakref
            from mssql_python import connect
            
            weakrefs = []
            
            def callback(ref):
                print(f"Weakref callback triggered for {{ref}}")
            
            # Create connections with weakref monitoring
            for i in range(3):
                conn = connect("{conn_str}")
                cursor = conn.cursor()
                cursor.execute(f"SELECT {{i}} AS test")
                cursor.fetchall()
                
                # Create weakref with callback
                wr = weakref.ref(conn, callback)
                weakrefs.append(wr)
                
                # Delete strong reference for connection 0
                if i == 0:
                    cursor.close()
                    conn.close()
                    print(f"Connection {{i}}: closed explicitly")
                else:
                    print(f"Connection {{i}}: left open")
            
            print("Weakref test: Exiting with weakrefs active")
            sys.exit(0)
        """
        )

        result = subprocess.run(
            [sys.executable, "-c", script], capture_output=True, text=True, timeout=30
        )

        assert result.returncode == 0, f"Process crashed. stderr: {result.stderr}"
        assert "Weakref test: Exiting with weakrefs active" in result.stdout
        print(f"✓ Weakref cleanup at shutdown: PASSED")

    def test_gc_during_shutdown_with_circular_refs(self, conn_str):
        """
        Test handle cleanup with circular references during shutdown.

        Scenario:
        1. Create circular references between objects holding handles
        2. Force GC during shutdown sequence
        3. Verify no crashes from complex cleanup order

        Expected: No segfault, proper cycle breaking
        """
        script = textwrap.dedent(
            f"""
            import sys
            import gc
            from mssql_python import connect
            
            class QueryWrapper:
                def __init__(self, conn_str, query_id):
                    self.conn = connect(conn_str)
                    self.cursor = self.conn.cursor()
                    self.query_id = query_id
                    self.partner = None  # For circular reference
                    
                def execute_query(self):
                    self.cursor.execute(f"SELECT {{self.query_id}} AS test")
                    return self.cursor.fetchall()
            
            # Create circular references
            wrapper1 = QueryWrapper("{conn_str}", 1)
            wrapper2 = QueryWrapper("{conn_str}", 2)
            
            wrapper1.partner = wrapper2
            wrapper2.partner = wrapper1
            
            result1 = wrapper1.execute_query()
            result2 = wrapper2.execute_query()
            print(f"Executed queries: {{result1}}, {{result2}}")
            
            # Break strong references but leave cycle
            del wrapper1
            del wrapper2
            
            # Force GC to detect cycles
            collected = gc.collect()
            print(f"GC collected {{collected}} objects")
            
            print("Circular ref test: Exiting after GC with cycles")
            sys.exit(0)
        """
        )

        result = subprocess.run(
            [sys.executable, "-c", script], capture_output=True, text=True, timeout=30
        )

        assert result.returncode == 0, f"Process crashed. stderr: {result.stderr}"
        assert "Circular ref test: Exiting after GC with cycles" in result.stdout
        print(f"✓ GC during shutdown with circular refs: PASSED")

    def test_all_handle_types_comprehensive(self, conn_str):
        """
        Comprehensive test validating all three handle types in one scenario.

        This test creates a realistic scenario where:
        - ENV handle (Type 1): Static singleton used by all connections
        - DBC handles (Type 2): Multiple connection handles, some freed
        - STMT handles (Type 3): Multiple cursor handles, some freed

        Expected: Clean shutdown with no segfaults
        """
        script = textwrap.dedent(
            f"""
            import sys
            from mssql_python import connect
            
            print("=== Comprehensive Handle Test ===")
            print("Testing ENV (Type 1), DBC (Type 2), STMT (Type 3) handles")
            
            # Scenario 1: Normal cleanup (baseline)
            conn1 = connect("{conn_str}")
            cursor1 = conn1.cursor()
            cursor1.execute("SELECT 1 AS baseline_test")
            cursor1.fetchall()
            cursor1.close()
            conn1.close()
            print("Scenario 1: Normal cleanup completed")
            
            # Scenario 2: Cursor closed, connection open
            conn2 = connect("{conn_str}")
            cursor2 = conn2.cursor()
            cursor2.execute("SELECT 2 AS cursor_closed_test")
            cursor2.fetchall()
            cursor2.close()
            # conn2 intentionally left open - DBC handle cleanup skipped at shutdown
            print("Scenario 2: Cursor closed, connection left open")
            
            # Scenario 3: Both cursor and connection open
            conn3 = connect("{conn_str}")
            cursor3 = conn3.cursor()
            cursor3.execute("SELECT 3 AS both_open_test")
            cursor3.fetchall()
            # Both intentionally left open - STMT and DBC handle cleanup skipped
            print("Scenario 3: Both cursor and connection left open")
            
            # Scenario 4: Multiple cursors per connection
            conn4 = connect("{conn_str}")
            cursors = []
            for i in range(5):
                c = conn4.cursor()
                c.execute(f"SELECT {{i}} AS multi_cursor_test")
                c.fetchall()
                cursors.append(c)
            # All intentionally left open
            print("Scenario 4: Multiple cursors per connection left open")
            
            print("=== Shutdown Protection Summary ===")
            print("During Python shutdown:")
            print("- Type 3 (STMT) handles: SQLFreeHandle SKIPPED")
            print("- Type 2 (DBC) handles: SQLFreeHandle SKIPPED")
            print("- Type 1 (ENV) handle: Normal C++ static destruction")
            print("=== Exiting ===")
            sys.exit(0)
        """
        )

        result = subprocess.run(
            [sys.executable, "-c", script], capture_output=True, text=True, timeout=30
        )

        assert result.returncode == 0, f"Process crashed. stderr: {result.stderr}"
        assert "=== Comprehensive Handle Test ===" in result.stdout
        assert "Scenario 1: Normal cleanup completed" in result.stdout
        assert "Scenario 2: Cursor closed, connection left open" in result.stdout
        assert "Scenario 3: Both cursor and connection left open" in result.stdout
        assert "Scenario 4: Multiple cursors per connection left open" in result.stdout
        assert "=== Exiting ===" in result.stdout
        print(f"✓ Comprehensive all handle types test: PASSED")


if __name__ == "__main__":
    # Allow running test directly for debugging
    import sys

    conn_str = os.environ.get("DB_CONNECTION_STRING")
    if not conn_str:
        print("ERROR: DB_CONNECTION_STRING environment variable not set")
        sys.exit(1)

    test = TestHandleFreeShutdown()

    print("\n" + "=" * 70)
    print("Running AGGRESSIVE Handle Cleanup Tests")
    print("Testing for SEGFAULT reproduction from stack trace")
    print("=" * 70 + "\n")

    try:
        # Run aggressive segfault tests first
        print("\n--- AGGRESSIVE SEGFAULT REPRODUCTION TESTS ---\n")
        test.test_aggressive_dbc_segfault_reproduction(conn_str)
        test.test_dbc_handle_outlives_env_handle(conn_str)
        test.test_force_gc_finalization_order_issue(conn_str)

        print("\n--- STANDARD HANDLE CLEANUP TESTS ---\n")
        test.test_stmt_handle_cleanup_at_shutdown(conn_str)
        test.test_dbc_handle_cleanup_at_shutdown(conn_str)
        test.test_env_handle_cleanup_at_shutdown(conn_str)
        test.test_mixed_handle_cleanup_at_shutdown(conn_str)
        test.test_rapid_connection_churn_with_shutdown(conn_str)
        test.test_exception_during_query_with_shutdown(conn_str)
        test.test_weakref_cleanup_at_shutdown(conn_str)
        test.test_gc_during_shutdown_with_circular_refs(conn_str)
        test.test_all_handle_types_comprehensive(conn_str)

        print("\n" + "=" * 70)
        print("✓ ALL TESTS PASSED - No segfaults detected")
        print("=" * 70 + "\n")
    except AssertionError as e:
        print(f"\n✗ TEST FAILED: {e}")
        sys.exit(1)
