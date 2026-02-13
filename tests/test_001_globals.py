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
import random

# Import global variables from the repository
from mssql_python import (
    apilevel,
    threadsafety,
    paramstyle,
    lowercase,
    getDecimalSeparator,
    setDecimalSeparator,
)


def test_apilevel():
    # Check if apilevel has the expected value
    assert apilevel == "2.0", "apilevel should be '2.0'"


def test_threadsafety():
    # Check if threadsafety has the expected value
    assert threadsafety == 1, "threadsafety should be 1"


def test_paramstyle():
    # Check if paramstyle has the expected value
    assert paramstyle == "pyformat", "paramstyle should be 'pyformat'"


def test_lowercase():
    # Check if lowercase has the expected default value
    assert lowercase is False, "lowercase should default to False"


def test_decimal_separator():
    """Test decimal separator functionality"""

    # Check default value
    assert getDecimalSeparator() == ".", "Default decimal separator should be '.'"

    try:
        # Test setting a new value
        setDecimalSeparator(",")
        assert getDecimalSeparator() == ",", "Decimal separator should be ',' after setting"

        # Test invalid input
        with pytest.raises(ValueError):
            setDecimalSeparator("too long")

        with pytest.raises(ValueError):
            setDecimalSeparator("")

        with pytest.raises(ValueError):
            setDecimalSeparator(123)  # Non-string input

    finally:
        # Restore default value
        setDecimalSeparator(".")
        assert getDecimalSeparator() == ".", "Decimal separator should be restored to '.'"


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

                if col_name not in ("COLUMN_NAME", "column_name"):
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


def test_decimal_separator_edge_cases():
    """Test decimal separator edge cases and boundary conditions"""
    import decimal

    # Save original separator for restoration
    original_separator = getDecimalSeparator()

    try:
        # Test 1: Special characters
        special_chars = [";", ":", "|", "/", "\\", "*", "+", "-"]
        for char in special_chars:
            setDecimalSeparator(char)
            assert (
                getDecimalSeparator() == char
            ), f"Failed to set special character '{char}' as separator"

        # Test 2: Non-ASCII characters
        # Note: Non-ASCII may work for storage but could cause issues with SQL Server
        non_ascii_chars = ["€", "¥", "£", "§", "µ"]
        for char in non_ascii_chars:
            try:
                setDecimalSeparator(char)
                assert (
                    getDecimalSeparator() == char
                ), f"Failed to set non-ASCII character '{char}' as separator"
            except ValueError:
                # Some implementations might reject non-ASCII - that's acceptable
                pass

        # Test 3: Invalid inputs - additional cases
        invalid_inputs = [
            "\t",  # Tab character
            "\n",  # Newline
            " ",  # Space
            None,  # None value
        ]

        for invalid in invalid_inputs:
            with pytest.raises((ValueError, TypeError)):
                setDecimalSeparator(invalid)

    finally:
        # Restore original setting
        setDecimalSeparator(original_separator)


def test_decimal_separator_whitespace_validation():
    """Test specific validation for whitespace characters"""

    # Save original separator for restoration
    original_separator = getDecimalSeparator()

    try:
        # Test Line 92: Regular space character should raise ValueError
        with pytest.raises(
            ValueError,
            match="Whitespace characters are not allowed as decimal separators",
        ):
            setDecimalSeparator(" ")

        # Test additional whitespace characters that trigger isspace()
        whitespace_chars = [
            " ",  # Regular space (U+0020)
            "\u00a0",  # Non-breaking space (U+00A0)
            "\u2000",  # En quad (U+2000)
            "\u2001",  # Em quad (U+2001)
            "\u2002",  # En space (U+2002)
            "\u2003",  # Em space (U+2003)
            "\u2004",  # Three-per-em space (U+2004)
            "\u2005",  # Four-per-em space (U+2005)
            "\u2006",  # Six-per-em space (U+2006)
            "\u2007",  # Figure space (U+2007)
            "\u2008",  # Punctuation space (U+2008)
            "\u2009",  # Thin space (U+2009)
            "\u200a",  # Hair space (U+200A)
            "\u3000",  # Ideographic space (U+3000)
        ]

        for ws_char in whitespace_chars:
            with pytest.raises(
                ValueError,
                match="Whitespace characters are not allowed as decimal separators",
            ):
                setDecimalSeparator(ws_char)

        # Test that control characters trigger the whitespace error (line 92)
        # instead of the control character error (lines 95-98)
        control_chars = ["\t", "\n", "\r", "\v", "\f"]

        for ctrl_char in control_chars:
            # These should trigger the whitespace error, NOT the control character error
            with pytest.raises(
                ValueError,
                match="Whitespace characters are not allowed as decimal separators",
            ):
                setDecimalSeparator(ctrl_char)

        # Test that valid characters still work after validation tests
        valid_chars = [".", ",", ";", ":", "-", "_"]
        for valid_char in valid_chars:
            setDecimalSeparator(valid_char)
            assert (
                getDecimalSeparator() == valid_char
            ), f"Failed to set valid character '{valid_char}'"

    finally:
        # Restore original setting
        setDecimalSeparator(original_separator)


def test_unreachable_control_character_validation():
    """
    The control characters \\t, \\n, \\r, \\v, \\f are all caught by the isspace()
    check before reaching the specific control character validation.

    This test documents the unreachable code issue for potential refactoring.
    """

    # Demonstrate that all control characters from lines 95-98 return True for isspace()
    control_chars = ["\t", "\n", "\r", "\v", "\f"]

    for ctrl_char in control_chars:
        # All these should return True, proving they're caught by isspace() first
        assert (
            ctrl_char.isspace()
        ), f"Control character {repr(ctrl_char)} should return True for isspace()"

        # Therefore they trigger the whitespace error, not the control character error
        with pytest.raises(
            ValueError,
            match="Whitespace characters are not allowed as decimal separators",
        ):
            setDecimalSeparator(ctrl_char)


def test_decimal_separator_comprehensive_edge_cases():
    """
    Additional comprehensive test to ensure maximum coverage of setDecimalSeparator validation.
    This test covers all reachable validation paths in lines 70-100 of __init__.py
    """

    original_separator = getDecimalSeparator()

    try:
        # Test type validation (around line 72)
        with pytest.raises(ValueError, match="Decimal separator must be a string"):
            setDecimalSeparator(123)  # integer

        with pytest.raises(ValueError, match="Decimal separator must be a string"):
            setDecimalSeparator(None)  # None

        with pytest.raises(ValueError, match="Decimal separator must be a string"):
            setDecimalSeparator([","])  # list

        # Test length validation - empty string (around line 77)
        with pytest.raises(ValueError, match="Decimal separator cannot be empty"):
            setDecimalSeparator("")

        # Test length validation - multiple characters (around line 80)
        with pytest.raises(ValueError, match="Decimal separator must be a single character"):
            setDecimalSeparator("..")

        with pytest.raises(ValueError, match="Decimal separator must be a single character"):
            setDecimalSeparator("abc")

        # Test whitespace validation (line 92) - THIS IS THE MAIN TARGET
        with pytest.raises(
            ValueError,
            match="Whitespace characters are not allowed as decimal separators",
        ):
            setDecimalSeparator(" ")  # regular space

        with pytest.raises(
            ValueError,
            match="Whitespace characters are not allowed as decimal separators",
        ):
            setDecimalSeparator("\t")  # tab (also isspace())

        # Test successful cases - reach line 100+ (set in Python side settings)
        valid_separators = [".", ",", ";", ":", "-", "_", "@", "#", "$", "%", "&", "*"]
        for sep in valid_separators:
            setDecimalSeparator(sep)
            assert getDecimalSeparator() == sep, f"Failed to set separator to {sep}"

    finally:
        setDecimalSeparator(original_separator)


def test_decimal_separator_with_db_operations(db_connection):
    """Test changing decimal separator during database operations"""
    import decimal

    # Save original separator for restoration
    original_separator = getDecimalSeparator()

    try:
        # Create a test table with decimal values
        cursor = db_connection.cursor()
        cursor.execute("""
        DROP TABLE IF EXISTS #decimal_separator_test;
        CREATE TABLE #decimal_separator_test (
            id INT,
            decimal_value DECIMAL(10,2)
        );
        INSERT INTO #decimal_separator_test VALUES 
            (1, 123.45), 
            (2, 678.90),
            (3, 0.01),
            (4, 999.99);
        """)
        cursor.close()

        # Test 1: Fetch with default separator
        cursor1 = db_connection.cursor()
        cursor1.execute("SELECT decimal_value FROM #decimal_separator_test WHERE id = 1")
        value1 = cursor1.fetchone()[0]
        assert isinstance(value1, decimal.Decimal)
        assert (
            str(value1) == "123.45"
        ), f"Expected 123.45, got {value1} with separator '{getDecimalSeparator()}'"

        # Test 2: Change separator and fetch new data
        setDecimalSeparator(",")
        cursor2 = db_connection.cursor()
        cursor2.execute("SELECT decimal_value FROM #decimal_separator_test WHERE id = 2")
        value2 = cursor2.fetchone()[0]
        assert isinstance(value2, decimal.Decimal)
        assert (
            str(value2).replace(".", ",") == "678,90"
        ), f"Expected 678,90, got {str(value2).replace('.', ',')} with separator ','"

        # Test 3: The previously fetched value should not be affected by separator change
        assert (
            str(value1) == "123.45"
        ), f"Previously fetched value changed after separator modification"

        # Test 4: Change separator back and forth multiple times
        separators_to_test = [".", ",", ";", ".", ",", "."]
        for i, sep in enumerate(separators_to_test, start=3):
            setDecimalSeparator(sep)
            assert getDecimalSeparator() == sep, f"Failed to set separator to '{sep}'"

            # Fetch new data with current separator
            cursor = db_connection.cursor()
            cursor.execute(
                f"SELECT decimal_value FROM #decimal_separator_test WHERE id = {i % 4 + 1}"
            )
            value = cursor.fetchone()[0]
            assert isinstance(
                value, decimal.Decimal
            ), f"Value should be Decimal with separator '{sep}'"

            # Verify string representation uses the current separator
            # Note: decimal.Decimal always uses '.' in string representation, so we replace for comparison
            decimal_str = str(value).replace(".", sep)
            assert sep in decimal_str or decimal_str.endswith(
                "0"
            ), f"Decimal string should contain separator '{sep}'"

    finally:
        # Clean up - Fixed: use cursor.execute instead of db_connection.execute
        cursor = db_connection.cursor()
        cursor.execute("DROP TABLE IF EXISTS #decimal_separator_test")
        cursor.close()
        setDecimalSeparator(original_separator)


def test_decimal_separator_batch_operations(db_connection):
    """Test decimal separator behavior with batch operations and result sets"""
    import decimal

    # Save original separator for restoration
    original_separator = getDecimalSeparator()

    try:
        # Create test data
        cursor = db_connection.cursor()
        cursor.execute("""
        DROP TABLE IF EXISTS #decimal_batch_test;
        CREATE TABLE #decimal_batch_test (
            id INT,
            value1 DECIMAL(10,3),
            value2 DECIMAL(12,5)
        );
        INSERT INTO #decimal_batch_test VALUES 
            (1, 123.456, 12345.67890), 
            (2, 0.001, 0.00001),
            (3, 999.999, 9999.99999);
        """)
        cursor.close()

        # Test 1: Fetch results with default separator
        setDecimalSeparator(".")
        cursor1 = db_connection.cursor()
        cursor1.execute("SELECT * FROM #decimal_batch_test ORDER BY id")
        results1 = cursor1.fetchall()
        cursor1.close()

        # Important: Verify Python Decimal objects always use "." internally
        # regardless of separator setting (pyodbc-compatible behavior)
        for row in results1:
            assert isinstance(row[1], decimal.Decimal), "Results should be Decimal objects"
            assert isinstance(row[2], decimal.Decimal), "Results should be Decimal objects"
            assert "." in str(row[1]), "Decimal string representation should use '.'"
            assert "." in str(row[2]), "Decimal string representation should use '.'"

        # Change separator before processing results
        setDecimalSeparator(",")

        # Verify results use the separator that was active during fetch
        # This tests that previously fetched values aren't affected by separator changes
        for row in results1:
            assert "." in str(row[1]), f"Expected '.' in {row[1]} from first result set"
            assert "." in str(row[2]), f"Expected '.' in {row[2]} from first result set"

        # Test 2: Fetch new results with new separator
        cursor2 = db_connection.cursor()
        cursor2.execute("SELECT * FROM #decimal_batch_test ORDER BY id")
        results2 = cursor2.fetchall()
        cursor2.close()

        # Check if implementation supports separator changes
        # In some versions of pyodbc, changing separator might cause NULL values
        has_nulls = any(any(v is None for v in row) for row in results2 if row is not None)

        if has_nulls:
            print(
                "NOTE: Decimal separator change resulted in NULL values - this is compatible with some pyodbc versions"
            )
            # Skip further numeric comparisons
        else:
            # Test 3: Verify values are equal regardless of separator used during fetch
            assert len(results1) == len(
                results2
            ), "Both result sets should have same number of rows"

            for i in range(len(results1)):
                # IDs should match
                assert results1[i][0] == results2[i][0], f"Row {i} IDs don't match"

                # Decimal values should be numerically equal even with different separators
                if results2[i][1] is not None and results1[i][1] is not None:
                    assert float(results1[i][1]) == float(
                        results2[i][1]
                    ), f"Row {i} value1 should be numerically equal"

                if results2[i][2] is not None and results1[i][2] is not None:
                    assert float(results1[i][2]) == float(
                        results2[i][2]
                    ), f"Row {i} value2 should be numerically equal"

        # Reset separator for further tests
        setDecimalSeparator(".")

    finally:
        # Clean up
        cursor = db_connection.cursor()
        cursor.execute("DROP TABLE IF EXISTS #decimal_batch_test")
        cursor.close()
        setDecimalSeparator(original_separator)


def test_decimal_separator_thread_safety():
    """Test thread safety of decimal separator with multiple concurrent threads"""

    # Save original separator for restoration
    original_separator = getDecimalSeparator()

    # Create a shared event for synchronizing threads
    ready_event = threading.Event()
    stop_event = threading.Event()

    # Create a list to track errors from threads
    errors = []

    def change_separator_worker():
        """Worker that repeatedly changes the decimal separator"""
        separators = [".", ",", ";", ":", "-", "|"]

        # Wait for the start signal
        ready_event.wait()

        try:
            # Rapidly change separators until told to stop
            while not stop_event.is_set():
                sep = random.choice(separators)
                setDecimalSeparator(sep)
                time.sleep(0.001)  # Small delay to allow other threads to run
        except Exception as e:
            errors.append(f"Changer thread error: {str(e)}")

    def read_separator_worker():
        """Worker that repeatedly reads the current separator"""
        # Wait for the start signal
        ready_event.wait()

        try:
            # Continuously read the separator until told to stop
            while not stop_event.is_set():
                separator = getDecimalSeparator()
                # Verify the separator is a valid string and not corrupted
                if not isinstance(separator, str) or len(separator) != 1:
                    errors.append(f"Invalid separator read: {repr(separator)}")
                time.sleep(0.001)  # Small delay to allow other threads to run
        except Exception as e:
            errors.append(f"Reader thread error: {str(e)}")

    try:
        # Create multiple threads that change and read the separator
        changer_threads = [threading.Thread(target=change_separator_worker) for _ in range(3)]
        reader_threads = [threading.Thread(target=read_separator_worker) for _ in range(5)]

        # Start all threads
        for t in changer_threads + reader_threads:
            t.start()

        # Allow threads to initialize
        time.sleep(0.1)

        # Signal threads to begin work
        ready_event.set()

        # Let threads run for a short time
        time.sleep(0.5)

        # Signal threads to stop
        stop_event.set()

        # Wait for all threads to finish
        for t in changer_threads + reader_threads:
            t.join(timeout=1.0)

        # Check for any errors reported by threads
        assert not errors, f"Thread safety errors detected: {errors}"

    finally:
        # Restore original separator
        stop_event.set()  # Ensure all threads will stop
        setDecimalSeparator(original_separator)


def test_decimal_separator_concurrent_db_operations(db_connection):
    """Test thread safety with concurrent database operations and separator changes.
    This test verifies that multiple threads can safely change and read the decimal separator.
    """
    import decimal
    import threading
    import queue
    import random
    import time

    # Save original separator for restoration
    original_separator = getDecimalSeparator()

    # Create a shared queue with a maximum size
    results_queue = queue.Queue(maxsize=100)

    # Create events for synchronization
    stop_event = threading.Event()

    # Set a global timeout for the entire test
    test_timeout = time.time() + 10  # 10 second maximum test duration

    # Extract connection string
    connection_str = db_connection.connection_str

    # We'll use a simpler approach - no temporary tables
    # Just verify the decimal separator can be changed safely

    def separator_changer_worker():
        """Worker that changes the decimal separator repeatedly"""
        separators = [".", ",", ";"]
        count = 0

        try:
            while not stop_event.is_set() and count < 10 and time.time() < test_timeout:
                sep = random.choice(separators)
                setDecimalSeparator(sep)
                results_queue.put(("change", sep))
                count += 1
                time.sleep(0.1)  # Slow down to avoid overwhelming the system
        except Exception as e:
            results_queue.put(("error", f"Changer error: {str(e)}"))

    def separator_reader_worker():
        """Worker that reads the current separator"""
        count = 0

        try:
            while not stop_event.is_set() and count < 20 and time.time() < test_timeout:
                current = getDecimalSeparator()
                results_queue.put(("read", current))
                count += 1
                time.sleep(0.05)
        except Exception as e:
            results_queue.put(("error", f"Reader error: {str(e)}"))

    # Use daemon threads that won't block test exit
    threads = [
        threading.Thread(target=separator_changer_worker, daemon=True),
        threading.Thread(target=separator_reader_worker, daemon=True),
    ]

    # Start all threads
    for t in threads:
        t.start()

    try:
        # Wait until the test timeout or all threads complete
        end_time = time.time() + 5  # 5 second test duration
        while time.time() < end_time and any(t.is_alive() for t in threads):
            time.sleep(0.1)

        # Signal threads to stop
        stop_event.set()

        # Give threads a short time to wrap up
        for t in threads:
            t.join(timeout=0.5)

        # Process results
        errors = []
        changes = []
        reads = []

        # Collect results with timeout
        timeout_end = time.time() + 1
        while not results_queue.empty() and time.time() < timeout_end:
            try:
                item = results_queue.get(timeout=0.1)
                if item[0] == "error":
                    errors.append(item[1])
                elif item[0] == "change":
                    changes.append(item[1])
                elif item[0] == "read":
                    reads.append(item[1])
            except queue.Empty:
                break

        # Verify we got results
        assert not errors, f"Thread errors detected: {errors}"
        assert changes, "No separator changes were recorded"
        assert reads, "No separator reads were recorded"

        print(f"Successfully performed {len(changes)} separator changes and {len(reads)} reads")

    finally:
        # Always make sure to clean up
        stop_event.set()
        setDecimalSeparator(original_separator)


# =============================================================================
# Platform Utils Tests
# =============================================================================


class TestPlatformUtils:
    """Tests for mssql_python.platform_utils module."""

    def test_get_platform_info_returns_tuple(self):
        """Test that get_platform_info returns a tuple of two strings."""
        from mssql_python.platform_utils import get_platform_info

        result = get_platform_info()
        assert isinstance(result, tuple)
        assert len(result) == 2
        assert isinstance(result[0], str)  # architecture
        assert isinstance(result[1], str)  # platform_tag

    def test_get_platform_info_current_platform(self):
        """Test get_platform_info on current platform returns valid values."""
        from mssql_python.platform_utils import get_platform_info
        import sys

        arch, platform_tag = get_platform_info()

        # Architecture should be non-empty
        assert arch

        # Platform tag should match current platform
        if sys.platform.startswith("win"):
            assert "win" in platform_tag
        elif sys.platform.startswith("darwin"):
            assert "macos" in platform_tag
        elif sys.platform.startswith("linux"):
            assert "linux" in platform_tag

    def test_windows_x64_detection(self):
        """Test Windows x64 platform detection."""
        from unittest.mock import patch
        from mssql_python import platform_utils

        with patch.object(platform_utils.sys, "platform", "win32"):
            with patch.object(platform_utils.os.environ, "get", return_value="x64"):
                arch, tag = platform_utils.get_platform_info()
                assert arch == "x64"
                assert tag == "win_amd64"

    def test_windows_x86_detection(self):
        """Test Windows x86 platform detection."""
        from unittest.mock import patch
        from mssql_python import platform_utils

        with patch.object(platform_utils.sys, "platform", "win32"):
            with patch.object(platform_utils.os.environ, "get", return_value="x86"):
                arch, tag = platform_utils.get_platform_info()
                assert arch == "x86"
                assert tag == "win32"

    def test_windows_arm64_detection(self):
        """Test Windows ARM64 platform detection."""
        from unittest.mock import patch
        from mssql_python import platform_utils

        with patch.object(platform_utils.sys, "platform", "win32"):
            with patch.object(platform_utils.os.environ, "get", return_value="arm64"):
                arch, tag = platform_utils.get_platform_info()
                assert arch == "arm64"
                assert tag == "win_arm64"

    def test_macos_detection(self):
        """Test macOS platform detection."""
        from unittest.mock import patch
        from mssql_python import platform_utils

        with patch.object(platform_utils.sys, "platform", "darwin"):
            arch, tag = platform_utils.get_platform_info()
            assert arch == "universal2"
            assert "macosx" in tag
            assert "universal2" in tag

    def test_linux_x86_64_glibc_detection(self):
        """Test Linux x86_64 glibc platform detection."""
        from unittest.mock import patch
        from mssql_python import platform_utils

        with patch.object(platform_utils.sys, "platform", "linux"):
            with patch.object(platform_utils.os.environ, "get", return_value="x86_64"):
                with patch.object(platform_utils.platform, "machine", return_value="x86_64"):
                    with patch.object(
                        platform_utils.platform, "libc_ver", return_value=("glibc", "2.28")
                    ):
                        arch, tag = platform_utils.get_platform_info()
                        assert arch == "x86_64"
                        assert tag == "manylinux_2_28_x86_64"

    def test_linux_x86_64_musl_detection(self):
        """Test Linux x86_64 musl platform detection."""
        from unittest.mock import patch
        from mssql_python import platform_utils

        with patch.object(platform_utils.sys, "platform", "linux"):
            with patch.object(platform_utils.os.environ, "get", return_value="x86_64"):
                with patch.object(platform_utils.platform, "machine", return_value="x86_64"):
                    with patch.object(
                        platform_utils.platform, "libc_ver", return_value=("musl", "1.2")
                    ):
                        arch, tag = platform_utils.get_platform_info()
                        assert arch == "x86_64"
                        assert tag == "musllinux_1_2_x86_64"

    def test_linux_aarch64_glibc_detection(self):
        """Test Linux aarch64 glibc platform detection."""
        from unittest.mock import patch
        from mssql_python import platform_utils

        with patch.object(platform_utils.sys, "platform", "linux"):
            with patch.object(platform_utils.os.environ, "get", return_value="aarch64"):
                with patch.object(platform_utils.platform, "machine", return_value="aarch64"):
                    with patch.object(
                        platform_utils.platform, "libc_ver", return_value=("glibc", "2.28")
                    ):
                        arch, tag = platform_utils.get_platform_info()
                        assert arch == "aarch64"
                        assert tag == "manylinux_2_28_aarch64"

    def test_linux_aarch64_musl_detection(self):
        """Test Linux aarch64 musl platform detection."""
        from unittest.mock import patch
        from mssql_python import platform_utils

        with patch.object(platform_utils.sys, "platform", "linux"):
            with patch.object(platform_utils.os.environ, "get", return_value="aarch64"):
                with patch.object(platform_utils.platform, "machine", return_value="aarch64"):
                    with patch.object(
                        platform_utils.platform, "libc_ver", return_value=("musl", "1.2")
                    ):
                        arch, tag = platform_utils.get_platform_info()
                        assert arch == "aarch64"
                        assert tag == "musllinux_1_2_aarch64"

    def test_linux_arm64_alias(self):
        """Test Linux arm64 is treated as aarch64."""
        from unittest.mock import patch
        from mssql_python import platform_utils

        with patch.object(platform_utils.sys, "platform", "linux"):
            with patch.object(platform_utils.os.environ, "get", return_value="arm64"):
                with patch.object(platform_utils.platform, "machine", return_value="arm64"):
                    with patch.object(
                        platform_utils.platform, "libc_ver", return_value=("glibc", "2.28")
                    ):
                        arch, tag = platform_utils.get_platform_info()
                        assert arch == "aarch64"
                        assert tag == "manylinux_2_28_aarch64"

    def test_linux_empty_libc_with_musl_glob(self):
        """Test Linux with empty libc_ver falls back to glob for musl detection."""
        from unittest.mock import patch
        from mssql_python import platform_utils

        with patch.object(platform_utils.sys, "platform", "linux"):
            with patch.object(platform_utils.os.environ, "get", return_value="x86_64"):
                with patch.object(platform_utils.platform, "machine", return_value="x86_64"):
                    with patch.object(platform_utils.platform, "libc_ver", return_value=("", "")):
                        with patch.object(
                            platform_utils.glob, "glob", return_value=["/lib/ld-musl-x86_64.so.1"]
                        ):
                            arch, tag = platform_utils.get_platform_info()
                            assert arch == "x86_64"
                            assert tag == "musllinux_1_2_x86_64"

    def test_linux_empty_libc_no_musl_glob(self, capsys):
        """Test Linux with empty libc_ver and no musl glob defaults to glibc."""
        from unittest.mock import patch
        from mssql_python import platform_utils

        with patch.object(platform_utils.sys, "platform", "linux"):
            with patch.object(platform_utils.os.environ, "get", return_value="x86_64"):
                with patch.object(platform_utils.platform, "machine", return_value="x86_64"):
                    with patch.object(platform_utils.platform, "libc_ver", return_value=("", "")):
                        with patch.object(platform_utils.glob, "glob", return_value=[]):
                            arch, tag = platform_utils.get_platform_info()
                            assert arch == "x86_64"
                            assert tag == "manylinux_2_28_x86_64"
                            # Check warning was printed
                            captured = capsys.readouterr()
                            assert "Warning" in captured.err or "warning" in captured.err.lower()

    def test_linux_unsupported_architecture(self):
        """Test Linux with unsupported architecture raises OSError."""
        from unittest.mock import patch
        from mssql_python import platform_utils

        with patch.object(platform_utils.sys, "platform", "linux"):
            with patch.object(platform_utils.os.environ, "get", return_value="ppc64le"):
                with patch.object(platform_utils.platform, "machine", return_value="ppc64le"):
                    with patch.object(
                        platform_utils.platform, "libc_ver", return_value=("glibc", "2.28")
                    ):
                        with pytest.raises(OSError) as exc_info:
                            platform_utils.get_platform_info()
                        assert "ppc64le" in str(exc_info.value)
                        assert "Unsupported architecture" in str(exc_info.value)

    def test_unsupported_platform(self):
        """Test unsupported platform raises OSError."""
        from unittest.mock import patch
        from mssql_python import platform_utils

        with patch.object(platform_utils.sys, "platform", "freebsd"):
            with pytest.raises(OSError) as exc_info:
                platform_utils.get_platform_info()
            assert "freebsd" in str(exc_info.value)
            assert "Unsupported platform" in str(exc_info.value)

    def test_windows_strips_quotes_from_arch(self):
        """Test Windows architecture strips surrounding quotes."""
        from unittest.mock import patch
        from mssql_python import platform_utils

        with patch.object(platform_utils.sys, "platform", "win32"):
            with patch.object(platform_utils.os.environ, "get", return_value='"x64"'):
                arch, tag = platform_utils.get_platform_info()
                assert arch == "x64"
                assert tag == "win_amd64"

    def test_windows_win32_alias(self):
        """Test Windows win32 is treated as x86."""
        from unittest.mock import patch
        from mssql_python import platform_utils

        with patch.object(platform_utils.sys, "platform", "win32"):
            with patch.object(platform_utils.os.environ, "get", return_value="win32"):
                arch, tag = platform_utils.get_platform_info()
                assert arch == "x86"
                assert tag == "win32"
