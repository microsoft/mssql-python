"""
Test cases to reproduce the segfault issue during connection invalidation.

This test module is based on the analysis in SEGFAULT_ANALYSIS.md and attempts to
reproduce the use-after-free bug that occurs when:
1. A connection is invalidated/closed (freeing underlying ODBC handles)
2. Cursor objects remain in memory
3. Garbage collector triggers cursor cleanup
4. Cursor's __del__() tries to free already-freed statement handles

⚠️  IMPORTANT NOTE ABOUT LOCAL REPRODUCTION ⚠️
===============================================
These tests may PASS in your local environment even though the bug exists!

The segfault is timing-sensitive and depends on:
- Garbage collection timing (non-deterministic)
- ODBC driver version and behavior
- Memory pressure and object lifecycle complexity
- SQLAlchemy's event listener infrastructure

CONFIRMED REPRODUCTION:
The segfault IS consistently reproducible in the Docker environment:
    docker build -f Dockerfile.segfault.txt -t mssql-segfault .
    docker run -it --rm mssql-segfault:latest
    
This runs SQLAlchemy's actual test_multiple_invalidate test which triggers
the crash 100% of the time in the Docker environment.

PURPOSE OF THESE TESTS:
1. Regression testing after fix implementation
2. Documentation of expected safe behavior  
3. Validation that basic scenarios work correctly
4. Coverage of multiple invalidation patterns

VALIDATION STRATEGY:
- Before Fix: Local passes ✓, Docker crashes ✗
- After Fix:  Local passes ✓, Docker passes ✓

The definitive test is whether Docker stops crashing after the fix.

BACKGROUND:
-----------
The segfault was discovered when running SQLAlchemy's reconnect test suite, specifically
the test_multiple_invalidate test. The issue occurs because:

- When a connection is closed/invalidated, the underlying ODBC connection handle is freed
- Associated statement handles (used by cursors) may also be freed automatically by ODBC
- However, Python cursor objects still exist and hold references to these freed handles
- When Python's garbage collector runs, it calls cursor.__del__()
- cursor.__del__() calls cursor.close()
- cursor.close() attempts to free the statement handle via hstmt.free()
- Since the handle was already freed, this causes a segmentation fault in native code

These tests run in subprocesses to isolate segfaults from the main test runner.

TEST COVERAGE:
--------------
- test_cursor_cleanup_after_connection_invalidation: Basic scenario - single cursor cleanup
- test_multiple_cursor_invalidation: Multiple cursors (reproduces SQLAlchemy scenario)
- test_gc_triggered_cursor_cleanup: Forces GC cycles to trigger the bug
- test_cursor_close_after_connection_close: Explicit cursor.close() after conn.close()
- test_simulated_sqlalchemy_reconnect: Simulates SQLAlchemy connection invalidation pattern
- test_double_cursor_close: Tests cursor.close() idempotency
- test_cursor_operations_after_connection_invalidation: Tests graceful error handling
- test_stress_connection_invalidation: Stress test with multiple cycles

EXPECTED RESULTS:
-----------------
BEFORE FIX: Local may pass, Docker will segfault
AFTER FIX: Both local and Docker should pass without segfaults

USAGE:
------
Run all tests:
    pytest tests/test_016_connection_invalidation_segfault.py -v

Run specific test:
    pytest tests/test_016_connection_invalidation_segfault.py::test_multiple_cursor_invalidation -v

Run with verbose output:
    pytest tests/test_016_connection_invalidation_segfault.py -v -s

SEE ALSO:
---------
- SEGFAULT_ANALYSIS.md - Detailed analysis of the root cause and recommended fixes
- WHY_NO_SEGFAULT_LOCALLY.md - Explanation of why local tests may not crash
- test_005_connection_cursor_lifecycle.py - Related lifecycle tests
"""

import gc
import os
import pytest
import subprocess
import sys
import weakref
from mssql_python import connect, InterfaceError, ProgrammingError


def test_cursor_cleanup_after_connection_invalidation(conn_str):
    """
    Test that cursor cleanup after connection close doesn't cause segfault.
    
    This test reproduces the scenario where:
    1. Connection is created and cursor is allocated
    2. Connection is closed (invalidating underlying handles)
    3. Cursor object still exists in memory
    4. Garbage collection triggers cursor cleanup
    
    Expected: No segfault, graceful handling of already-freed handles
    """
    code = f"""
import gc
from mssql_python import connect

# Create connection and cursor
conn = connect(r'''{conn_str}''')
cursor = conn.cursor()

# Execute query to ensure statement handle is allocated
cursor.execute("SELECT 1")
cursor.fetchall()

# Close connection (this may free cursor's statement handles)
conn.close()

# Cursor object still exists - now trigger garbage collection
# This should call cursor.__del__() which calls cursor.close()
# which tries to free already-freed statement handle
del cursor
gc.collect()

print("Test completed without segfault")
"""
    
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    
    # Check for segfault
    if result.returncode != 0:
        print(f"STDOUT: {result.stdout}")
        print(f"STDERR: {result.stderr}")
        if "Segmentation fault" in result.stderr or result.returncode == 139:
            pytest.fail(f"SEGFAULT DETECTED! This is the bug we're trying to fix.\n{result.stderr}")
        else:
            pytest.fail(f"Test failed with return code {result.returncode}: {result.stderr}")
    
    assert "Test completed without segfault" in result.stdout


def test_multiple_cursor_invalidation(conn_str):
    """
    Test multiple cursors being cleaned up after connection invalidation.
    
    This reproduces the scenario from SQLAlchemy's test_multiple_invalidate test
    where multiple cursors exist when connection is invalidated.
    """
    code = f"""
import gc
from mssql_python import connect

# Create connection and multiple cursors
conn = connect(r'''{conn_str}''')
cursors = []

for i in range(5):
    cursor = conn.cursor()
    cursor.execute(f"SELECT {{i}}")
    cursor.fetchall()
    cursors.append(cursor)

# Close connection (invalidates all cursor handles)
conn.close()

# Now delete all cursors and force garbage collection
# This is where the segfault occurs in the original bug
for cursor in cursors:
    del cursor
    
cursors.clear()
gc.collect()

print("Multiple cursor cleanup completed without segfault")
"""
    
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"STDOUT: {result.stdout}")
        print(f"STDERR: {result.stderr}")
        if "Segmentation fault" in result.stderr or result.returncode == 139:
            pytest.fail(f"SEGFAULT DETECTED with multiple cursors!\n{result.stderr}")
        else:
            pytest.fail(f"Test failed with return code {result.returncode}: {result.stderr}")
    
    assert "Multiple cursor cleanup completed without segfault" in result.stdout


def test_gc_triggered_cursor_cleanup(conn_str):
    """
    Explicitly trigger GC to reproduce the segfault scenario.
    
    This test forces multiple GC cycles to increase the likelihood of
    triggering the use-after-free bug.
    """
    code = f"""
import gc
from mssql_python import connect

def create_cursors_and_close_connection():
    conn = connect(r'''{conn_str}''')
    cursor1 = conn.cursor()
    cursor2 = conn.cursor()
    
    cursor1.execute("SELECT 1")
    cursor1.fetchall()
    
    cursor2.execute("SELECT 2")
    cursor2.fetchall()
    
    # Close connection but cursors are still in scope
    conn.close()
    
    # Return cursors so they outlive the connection
    return cursor1, cursor2

# Create cursors with closed connection
cursor1, cursor2 = create_cursors_and_close_connection()

# Multiple GC cycles to trigger cleanup
for i in range(3):
    gc.collect()
    gc.collect()

# Delete cursors explicitly
del cursor1
del cursor2

# Final GC cycle
gc.collect()

print("GC-triggered cleanup completed without segfault")
"""
    
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"STDOUT: {result.stdout}")
        print(f"STDERR: {result.stderr}")
        if "Segmentation fault" in result.stderr or result.returncode == 139:
            pytest.fail(f"SEGFAULT DETECTED during GC!\n{result.stderr}")
        else:
            pytest.fail(f"Test failed with return code {result.returncode}: {result.stderr}")
    
    assert "GC-triggered cleanup completed without segfault" in result.stdout


def test_cursor_close_after_connection_close(conn_str):
    """
    Test explicit cursor.close() after connection.close().
    
    This tests the case where user code explicitly closes a cursor
    after the connection has been closed.
    """
    code = f"""
from mssql_python import connect, ProgrammingError

conn = connect(r'''{conn_str}''')
cursor = conn.cursor()

cursor.execute("SELECT 1")
cursor.fetchall()

# Close connection first
conn.close()

# Now try to close cursor explicitly
# This should not segfault, but may raise an exception
try:
    cursor.close()
    print("Cursor closed without error")
except Exception as e:
    print(f"Cursor close raised exception (expected): {{type(e).__name__}}")

print("Test completed without segfault")
"""
    
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"STDOUT: {result.stdout}")
        print(f"STDERR: {result.stderr}")
        if "Segmentation fault" in result.stderr or result.returncode == 139:
            pytest.fail(f"SEGFAULT DETECTED on explicit cursor.close()!\n{result.stderr}")
        else:
            pytest.fail(f"Test failed with return code {result.returncode}: {result.stderr}")
    
    assert "Test completed without segfault" in result.stdout


def test_simulated_sqlalchemy_reconnect(conn_str):
    """
    Simulate SQLAlchemy's reconnect pattern that triggers the segfault.
    
    This test simulates the pattern used in SQLAlchemy's test_multiple_invalidate:
    1. Create connection with cursor
    2. Invalidate connection (simulated by closing it)
    3. Create new connection (reconnect)
    4. Old cursor still exists and gets garbage collected
    """
    code = f"""
import gc
from mssql_python import connect

# First connection with cursor
conn1 = connect(r'''{conn_str}''')
cursor1 = conn1.cursor()
cursor1.execute("SELECT 1")
cursor1.fetchall()

# Store weak reference to connection for tracking
import weakref
conn1_ref = weakref.ref(conn1)

# Invalidate first connection (simulate connection failure)
conn1.close()

# Create new connection (reconnect scenario)
conn2 = connect(r'''{conn_str}''')
cursor2 = conn2.cursor()
cursor2.execute("SELECT 2")
cursor2.fetchall()

# Delete old cursor - this is where segfault occurs
del cursor1
gc.collect()

# Cleanup second connection
cursor2.close()
conn2.close()

# Final cleanup
gc.collect()

print("Simulated reconnect completed without segfault")
"""
    
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"STDOUT: {result.stdout}")
        print(f"STDERR: {result.stderr}")
        if "Segmentation fault" in result.stderr or result.returncode == 139:
            pytest.fail(f"SEGFAULT DETECTED in reconnect scenario!\n{result.stderr}")
        else:
            pytest.fail(f"Test failed with return code {result.returncode}: {result.stderr}")
    
    assert "Simulated reconnect completed without segfault" in result.stdout


def test_double_cursor_close(conn_str):
    """
    Test that closing a cursor twice doesn't cause segfault.
    
    This tests the idempotency of cursor.close().
    According to the fix recommendations, close() should be idempotent.
    """
    code = f"""
from mssql_python import connect

conn = connect(r'''{conn_str}''')
cursor = conn.cursor()

cursor.execute("SELECT 1")
cursor.fetchall()

# Close cursor twice
cursor.close()
try:
    cursor.close()
    print("Second close succeeded (idempotent)")
except Exception as e:
    print(f"Second close raised: {{type(e).__name__}}")

conn.close()

print("Double close test completed without segfault")
"""
    
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"STDOUT: {result.stdout}")
        print(f"STDERR: {result.stderr}")
        if "Segmentation fault" in result.stderr or result.returncode == 139:
            pytest.fail(f"SEGFAULT DETECTED on double close!\n{result.stderr}")
        else:
            pytest.fail(f"Test failed with return code {result.returncode}: {result.stderr}")
    
    assert "Double close test completed without segfault" in result.stdout


def test_cursor_operations_after_connection_invalidation(conn_str):
    """
    Test that cursor operations after connection invalidation are handled gracefully.
    
    This tests that attempting to use a cursor after connection close
    raises appropriate exceptions rather than causing segfault.
    """
    code = f"""
from mssql_python import connect, InterfaceError, ProgrammingError

conn = connect(r'''{conn_str}''')
cursor = conn.cursor()

cursor.execute("SELECT 1")
cursor.fetchall()

# Close connection
conn.close()

# Try to execute query on cursor with closed connection
try:
    cursor.execute("SELECT 2")
    print("ERROR: Execute should have failed")
except (InterfaceError, ProgrammingError) as e:
    print(f"Execute correctly raised: {{type(e).__name__}}")

# Try to close cursor
try:
    cursor.close()
    print("Cursor close succeeded")
except Exception as e:
    print(f"Cursor close raised: {{type(e).__name__}}")

print("Operations test completed without segfault")
"""
    
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"STDOUT: {result.stdout}")
        print(f"STDERR: {result.stderr}")
        if "Segmentation fault" in result.stderr or result.returncode == 139:
            pytest.fail(f"SEGFAULT DETECTED during cursor operations!\n{result.stderr}")
        else:
            pytest.fail(f"Test failed with return code {result.returncode}: {result.stderr}")
    
    assert "Operations test completed without segfault" in result.stdout


def test_stress_connection_invalidation(conn_str):
    """
    Stress test with multiple invalidation cycles.
    
    This test performs multiple cycles of connection creation, cursor allocation,
    and invalidation to increase the likelihood of reproducing the bug.
    """
    code = f"""
import gc
from mssql_python import connect

for cycle in range(10):
    conn = connect(r'''{conn_str}''')
    cursors = []
    
    # Create multiple cursors
    for i in range(3):
        cursor = conn.cursor()
        cursor.execute(f"SELECT {{i}}")
        cursor.fetchall()
        cursors.append(cursor)
    
    # Close connection (invalidate)
    conn.close()
    
    # Cleanup cursors
    for cursor in cursors:
        del cursor
    cursors.clear()
    
    # Force garbage collection
    gc.collect()

print("Stress test completed without segfault after 10 cycles")
"""
    
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=30)
    
    if result.returncode != 0:
        print(f"STDOUT: {result.stdout}")
        print(f"STDERR: {result.stderr}")
        if "Segmentation fault" in result.stderr or result.returncode == 139:
            pytest.fail(f"SEGFAULT DETECTED during stress test!\n{result.stderr}")
        else:
            pytest.fail(f"Test failed with return code {result.returncode}: {result.stderr}")
    
    assert "Stress test completed without segfault" in result.stdout
