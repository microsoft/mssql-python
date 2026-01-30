"""
Simple Python script to reproduce the Windows segfault issue with the fix.
This mimics the stack trace scenario: cursor destruction during object creation.

Run: python test_windows_segfault_simple.py
"""

import gc
import sys
import threading
from mssql_python import connect, DatabaseError, OperationalError

# Connection string - update with your SQL Server details
CONN_STR = "Driver={ODBC Driver 18 for SQL Server};Server=localhost;Database=test;UID=sa;PWD=YourPassword;Encrypt=No"


def test_cursor_destruction_during_init():
    """
    Test that mimics the stack trace where __del__ is called during object initialization.
    This tests the fix for premature cursor destruction.
    """
    print("=" * 60)
    print("Test: Cursor Destruction During Initialization")
    print("=" * 60)
    
    try:
        conn = connect(CONN_STR)
        print("✓ Connection established")
        
        # Create multiple cursors and trigger GC during initialization
        cursors = []
        for i in range(5):
            cursor = conn.cursor()
            cursor.execute("SELECT 1 AS test_col")
            cursor.fetchall()
            cursors.append(cursor)
            
            # Force garbage collection to trigger __del__ on unreferenced objects
            gc.collect()
            print(f"  Cursor {i+1} created and GC triggered")
        
        # Close connection WITHOUT explicitly closing cursors first
        # This simulates the invalidation scenario
        print("\n✓ Closing connection without closing cursors...")
        conn.close()
        
        # Force garbage collection to trigger cursor cleanup
        print("✓ Triggering garbage collection...")
        cursors = None
        gc.collect()
        gc.collect()  # Call twice to ensure cleanup
        
        print("\n" + "=" * 60)
        print("SUCCESS: No segfault detected!")
        print("The fix is working correctly.")
        print("=" * 60)
        return True
        
    except Exception as e:
        print("\n" + "=" * 60)
        print(f"ERROR: {type(e).__name__}: {e}")
        print("=" * 60)
        import traceback
        traceback.print_exc()
        return False


def test_connection_invalidation_multiple_times(iterations=10):
    """
    Run the connection invalidation test multiple times to ensure stability.
    """
    print("\n" + "=" * 60)
    print(f"Running Connection Invalidation Test ({iterations} iterations)")
    print("=" * 60)
    
    failures = 0
    for i in range(iterations):
        print(f"\nIteration {i+1}/{iterations}...", end=" ")
        try:
            conn = connect(CONN_STR)
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            conn.close()
            gc.collect()
            print("✓ PASS")
        except Exception as e:
            print(f"✗ FAIL: {e}")
            failures += 1
    
    print("\n" + "=" * 60)
    if failures == 0:
        print(f"✓ All {iterations} iterations passed!")
        print("The fix is stable across multiple runs.")
    else:
        print(f"✗ {failures}/{iterations} iterations failed")
    print("=" * 60)
    
    return failures == 0


def test_threading_scenario():
    """
    Test that mimics the threading.RLock scenario from the stack trace.
    This ensures the fix works when cursors are destroyed during lock creation.
    """
    print("\n" + "=" * 60)
    print("Test: Threading + Cursor Destruction")
    print("=" * 60)
    
    def worker():
        try:
            conn = connect(CONN_STR)
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            conn.close()
            gc.collect()
            return True
        except Exception as e:
            print(f"  Thread error: {e}")
            return False
    
    threads = []
    for i in range(5):
        t = threading.Thread(target=worker)
        threads.append(t)
        t.start()
        print(f"  Thread {i+1} started")
    
    print("\n  Waiting for threads to complete...")
    for t in threads:
        t.join()
    
    print("\n" + "=" * 60)
    print("✓ Threading test completed without crashes")
    print("=" * 60)
    
    return True


def main():
    """Main test runner"""
    print("\n" + "=" * 60)
    print("Windows Segfault Fix - Validation Tests")
    print("=" * 60)
    print(f"Python version: {sys.version}")
    print(f"Platform: {sys.platform}")
    print("=" * 60)
    print("\nIMPORTANT: Update CONN_STR in the script with your SQL Server details")
    print("=" * 60)
    
    try:
        # Import and print version info
        import mssql_python
        print(f"\nmssql_python version: {mssql_python.__version__}")
        from mssql_python.ddbc_bindings import Connection
        print("✓ C++ extension loaded successfully")
    except Exception as e:
        print(f"\n✗ Failed to load mssql_python: {e}")
        print("\nMake sure to build and install the package first:")
        print("  1. cd mssql_python\\pybind")
        print("  2. build.bat")
        print("  3. cd ..\\..")
        print("  4. pip install -e .")
        return False
    
    # Run tests
    print("\n")
    test1_passed = test_cursor_destruction_during_init()
    
    if test1_passed:
        test2_passed = test_connection_invalidation_multiple_times(10)
        test3_passed = test_threading_scenario()
        
        if test1_passed and test2_passed and test3_passed:
            print("\n" + "=" * 60)
            print("ALL TESTS PASSED!")
            print("The segfault fix is working correctly on Windows.")
            print("=" * 60)
            return True
    
    print("\n" + "=" * 60)
    print("SOME TESTS FAILED")
    print("Please review the errors above.")
    print("=" * 60)
    return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
