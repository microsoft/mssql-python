"""
This file contains tests for the global variables in the mssql_python package.
Functions:
- test_apilevel: Check if apilevel has the expected value.
- test_threadsafety: Check if threadsafety has the expected value.
- test_paramstyle: Check if paramstyle has the expected value.
- test_lowercase: Check if lowercase has the expected value.
"""

import pytest
import threading
import time
import mssql_python

# Import global variables from the repository
from mssql_python import apilevel, threadsafety, paramstyle, lowercase

def test_apilevel():
    # Check if apilevel has the expected value
    assert apilevel == "2.0", "apilevel should be '2.0'"

def test_threadsafety():
    # Check if threadsafety has the expected value
    assert threadsafety == 1, "threadsafety should be 1"

def test_paramstyle():
    # Check if paramstyle has the expected value
    assert paramstyle == "qmark", "paramstyle should be 'qmark'"

def test_lowercase():
    # Check if lowercase has the expected default value
    assert lowercase is False, "lowercase should default to False"

def test_lowercase_thread_safety_no_db():
    """
    Tests concurrent modifications to mssql_python.lowercase without database interaction.
    This test ensures that the value is not corrupted by simultaneous writes from multiple threads.
    """
    original_lowercase = mssql_python.lowercase
    iterations = 100
    
    def worker():
        for _ in range(iterations):
            mssql_python.lowercase = True
            mssql_python.lowercase = False

    threads = [threading.Thread(target=worker) for _ in range(4)]

    for t in threads:
        t.start()

    for t in threads:
        t.join()

    # The final value will be False because it's the last write in the loop.
    # The main point is to ensure the lock prevented any corruption.
    assert mssql_python.lowercase is False, "Final state of lowercase should be False"
    
    # Restore original value
    mssql_python.lowercase = original_lowercase

def test_lowercase_concurrent_access_with_db(db_connection):
    """
    Tests concurrent modification of the 'lowercase' setting while simultaneously
    creating cursors and executing queries. This simulates a real-world race condition.
    """
    original_lowercase = mssql_python.lowercase
    stop_event = threading.Event()
    errors = []

    # Create a temporary table for the test
    cursor = None
    try:
        cursor = db_connection.cursor()
        cursor.execute("CREATE TABLE #pytest_thread_test (COLUMN_NAME INT)")
        db_connection.commit()
    except Exception as e:
        pytest.fail(f"Failed to create test table: {e}")
    finally:
        if cursor:
            cursor.close()

    def writer():
        """Continuously toggles the lowercase setting."""
        while not stop_event.is_set():
            try:
                mssql_python.lowercase = True
                time.sleep(0.001)
                mssql_python.lowercase = False
                time.sleep(0.001)
            except Exception as e:
                errors.append(f"Writer thread error: {e}")
                break

    def reader():
        """Continuously creates cursors and checks for valid description casing."""
        while not stop_event.is_set():
            cursor = None
            try:
                cursor = db_connection.cursor()
                cursor.execute("SELECT * FROM #pytest_thread_test")
                
                # The lock ensures the description is generated atomically.
                # We just need to check if the result is one of the two valid states.
                col_name = cursor.description[0][0]
                
                if col_name not in ('COLUMN_NAME', 'column_name'):
                    errors.append(f"Invalid column name '{col_name}' found. Race condition likely.")
            except Exception as e:
                errors.append(f"Reader thread error: {e}")
                break
            finally:
                if cursor:
                    cursor.close()

    # Start threads
    writer_thread = threading.Thread(target=writer)
    reader_threads = [threading.Thread(target=reader) for _ in range(3)]

    writer_thread.start()
    for t in reader_threads:
        t.start()

    # Let the threads run for a short period to induce race conditions
    time.sleep(1)
    stop_event.set()

    # Wait for threads to finish
    writer_thread.join()
    for t in reader_threads:
        t.join()

    # Clean up
    cursor = None
    try:
        cursor = db_connection.cursor()
        cursor.execute("DROP TABLE #pytest_thread_test")
        db_connection.commit()
    except Exception as e:
        # Log cleanup error but don't fail the test for it
        print(f"Warning: Failed to drop test table during cleanup: {e}")
    finally:
        if cursor:
            cursor.close()
        
    mssql_python.lowercase = original_lowercase

    # Assert that no errors occurred in the threads
    assert not errors, f"Thread safety test failed with errors: {errors}"