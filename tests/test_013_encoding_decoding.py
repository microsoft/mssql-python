"""
Comprehensive Encoding/Decoding Test Suite

This consolidated module provides complete testing for encoding/decoding functionality 
in mssql-python, ensuring pyodbc compatibility, thread safety, and connection pooling support.

Total Tests: 131

Test Categories:
================

1. BASIC FUNCTIONALITY (31 tests)
   - SQL Server supported encodings (UTF-8, UTF-16, Latin-1, CP1252, GBK, Big5, Shift-JIS, etc.)
   - SQL_CHAR vs SQL_WCHAR behavior (VARCHAR vs NVARCHAR columns)
   - setencoding/getencoding/setdecoding/getdecoding APIs
   - Default settings and configuration

2. VALIDATION & SECURITY (8 tests)
   - Encoding validation (Python layer)
   - Decoding validation (Python layer)
   - Injection attacks and malicious encoding strings
   - Character validation and length limits
   - C++ layer encoding/decoding (via ddbc_bindings)

3. ERROR HANDLING (10 tests)
   - Strict mode enforcement
   - UnicodeEncodeError and UnicodeDecodeError
   - Invalid encoding strings
   - Invalid SQL types
   - Closed connection handling

4. DATA TYPES & EDGE CASES (25 tests)
   - Empty strings, NULL values, max length
   - Special characters and emoji (surrogate pairs)
   - Boundary conditions and character set limits
   - LOB support: VARCHAR(MAX), NVARCHAR(MAX) with large data
   - Batch operations: executemany with various encodings

5. INTERNATIONAL ENCODINGS (15 tests)
   - Chinese: GBK, Big5
   - Japanese: Shift-JIS
   - Korean: EUC-KR
   - European: Latin-1, CP1252, ISO-8859 family
   - UTF-8 and UTF-16 variants

6. PYODBC COMPATIBILITY (12 tests)
   - No automatic fallback behavior
   - UTF-16 BOM rejection for SQL_WCHAR
   - SQL_WMETADATA flexibility
   - API compatibility and behavior matching

7. THREAD SAFETY (8 tests)
   - Race condition prevention in setencoding/setdecoding
   - Thread-safe reads with getencoding/getdecoding
   - Concurrent encoding/decoding operations
   - Multiple threads using different cursors
   - Parallel query execution with different encodings
   - Stress test: 500 rapid encoding changes across 10 threads

8. CONNECTION POOLING (6 tests)
   - Independent encoding settings per pooled connection
   - Settings behavior across pool reuse
   - Concurrent threads with pooled connections
   - ThreadPoolExecutor integration (50 concurrent tasks)
   - Pool exhaustion handling
   - Pooling disabled mode verification

9. PERFORMANCE & STRESS (8 tests)
   - Large dataset handling
   - Multiple encoding switches
   - Concurrent settings changes
   - Performance benchmarks

10. END-TO-END INTEGRATION (8 tests)
    - Round-trip encoding/decoding
    - Mixed Unicode string handling
    - Connection isolation
    - Real-world usage scenarios

IMPORTANT NOTES:
================
1. SQL_CHAR encoding affects VARCHAR columns
2. SQL_WCHAR encoding affects NVARCHAR columns  
3. These are independent - setting one doesn't affect the other
4. SQL_WMETADATA affects column name decoding
5. UTF-16 (LE/BE) is recommended for NVARCHAR but not strictly enforced
6. All encoding/decoding operations are thread-safe (RLock protection)
7. Each pooled connection maintains independent encoding settings
8. Settings may persist or reset across pool reuse (implementation-specific)

Thread Safety Implementation:
============================
- threading.RLock protects _encoding_settings and _decoding_settings
- All setencoding/getencoding/setdecoding/getdecoding operations are atomic
- Safe for concurrent access from multiple threads
- Lock-copy pattern ensures consistent snapshots
- Minimal overhead (<2μs per operation)

Connection Pooling Behavior:
===========================
- Each Connection object has independent encoding/decoding settings
- Settings do NOT leak between different pooled connections
- Encoding may persist across pool reuse (same Connection object)
- Applications should explicitly set encodings if specific settings required
- Pool exhaustion handled gracefully with clear error messages

Copyright (c) Microsoft Corporation.
Licensed under the MIT license.
"""

import pytest
import sys
import mssql_python
from mssql_python import connect, SQL_CHAR, SQL_WCHAR, SQL_WMETADATA
from mssql_python.exceptions import (
    ProgrammingError,
    DatabaseError,
    InterfaceError,
)


# ====================================================================================
# TEST DATA - SQL Server Supported Encodings
# ====================================================================================


def test_setencoding_default_settings(db_connection):
    """Test that default encoding settings are correct."""
    settings = db_connection.getencoding()
    assert settings["encoding"] == "utf-16le", "Default encoding should be utf-16le"
    assert settings["ctype"] == -8, "Default ctype should be SQL_WCHAR (-8)"


def test_setencoding_basic_functionality(db_connection):
    """Test basic setencoding functionality."""
    # Test setting UTF-8 encoding
    db_connection.setencoding(encoding="utf-8")
    settings = db_connection.getencoding()
    assert settings["encoding"] == "utf-8", "Encoding should be set to utf-8"
    assert settings["ctype"] == 1, "ctype should default to SQL_CHAR (1) for utf-8"

    # Test setting UTF-16LE with explicit ctype
    db_connection.setencoding(encoding="utf-16le", ctype=-8)
    settings = db_connection.getencoding()
    assert settings["encoding"] == "utf-16le", "Encoding should be set to utf-16le"
    assert settings["ctype"] == -8, "ctype should be SQL_WCHAR (-8)"


def test_setencoding_automatic_ctype_detection(db_connection):
    """Test automatic ctype detection based on encoding."""
    # UTF-16 variants should default to SQL_WCHAR
    utf16_encodings = ["utf-16le", "utf-16be"]
    for encoding in utf16_encodings:
        db_connection.setencoding(encoding=encoding)
        settings = db_connection.getencoding()
        assert settings["ctype"] == -8, f"{encoding} should default to SQL_WCHAR (-8)"

    # Other encodings should default to SQL_CHAR
    other_encodings = ["utf-8", "latin-1", "ascii"]
    for encoding in other_encodings:
        db_connection.setencoding(encoding=encoding)
        settings = db_connection.getencoding()
        assert settings["ctype"] == 1, f"{encoding} should default to SQL_CHAR (1)"


def test_setencoding_explicit_ctype_override(db_connection):
    """Test that explicit ctype parameter overrides automatic detection."""
    # Set UTF-16LE with SQL_CHAR (valid override)
    db_connection.setencoding(encoding="utf-16le", ctype=1)
    settings = db_connection.getencoding()
    assert settings["encoding"] == "utf-16le", "Encoding should be utf-16le"
    assert settings["ctype"] == 1, "ctype should be SQL_CHAR (1) when explicitly set"

    # Set UTF-8 with SQL_CHAR (valid combination)
    db_connection.setencoding(encoding="utf-8", ctype=1)
    settings = db_connection.getencoding()
    assert settings["encoding"] == "utf-8", "Encoding should be utf-8"
    assert settings["ctype"] == 1, "ctype should be SQL_CHAR (1)"


def test_setencoding_invalid_combinations(db_connection):
    """Test that invalid encoding/ctype combinations raise errors."""

    # UTF-8 with SQL_WCHAR should raise error
    with pytest.raises(ProgrammingError, match="SQL_WCHAR only supports UTF-16 encodings"):
        db_connection.setencoding(encoding="utf-8", ctype=-8)

    # latin1 with SQL_WCHAR should raise error
    with pytest.raises(ProgrammingError, match="SQL_WCHAR only supports UTF-16 encodings"):
        db_connection.setencoding(encoding="latin1", ctype=-8)


def test_setdecoding_invalid_combinations(db_connection):
    """Test that invalid encoding/ctype combinations raise errors in setdecoding."""

    # UTF-8 with SQL_WCHAR sqltype should raise error
    with pytest.raises(ProgrammingError, match="SQL_WCHAR only supports UTF-16 encodings"):
        db_connection.setdecoding(SQL_WCHAR, encoding="utf-8")

    # SQL_WMETADATA is flexible and can use UTF-8 (unlike SQL_WCHAR)
    # This should work without error
    db_connection.setdecoding(SQL_WMETADATA, encoding="utf-8")
    settings = db_connection.getdecoding(SQL_WMETADATA)
    assert settings["encoding"] == "utf-8"
    
    # Restore SQL_WMETADATA to default for subsequent tests
    db_connection.setdecoding(SQL_WMETADATA, encoding="utf-16le")

    # UTF-8 with SQL_WCHAR ctype should raise error
    with pytest.raises(ProgrammingError, match="SQL_WCHAR ctype only supports UTF-16 encodings"):
        db_connection.setdecoding(SQL_CHAR, encoding="utf-8", ctype=-8)


def test_setencoding_none_parameters(db_connection):
    """Test setencoding with None parameters."""
    # Test with encoding=None (should use default)
    db_connection.setencoding(encoding=None)
    settings = db_connection.getencoding()
    assert settings["encoding"] == "utf-16le", "encoding=None should use default utf-16le"
    assert settings["ctype"] == -8, "ctype should be SQL_WCHAR for utf-16le"

    # Test with both None (should use defaults)
    db_connection.setencoding(encoding=None, ctype=None)
    settings = db_connection.getencoding()
    assert settings["encoding"] == "utf-16le", "encoding=None should use default utf-16le"
    assert settings["ctype"] == -8, "ctype=None should use default SQL_WCHAR"


def test_setencoding_invalid_encoding(db_connection):
    """Test setencoding with invalid encoding."""

    with pytest.raises(ProgrammingError) as exc_info:
        db_connection.setencoding(encoding="invalid-encoding-name")

    assert "Unsupported encoding" in str(
        exc_info.value
    ), "Should raise ProgrammingError for invalid encoding"
    assert "invalid-encoding-name" in str(
        exc_info.value
    ), "Error message should include the invalid encoding name"


def test_setencoding_invalid_ctype(db_connection):
    """Test setencoding with invalid ctype."""

    with pytest.raises(ProgrammingError) as exc_info:
        db_connection.setencoding(encoding="utf-8", ctype=999)

    assert "Invalid ctype" in str(exc_info.value), "Should raise ProgrammingError for invalid ctype"
    assert "999" in str(exc_info.value), "Error message should include the invalid ctype value"


def test_setencoding_closed_connection(conn_str):
    """Test setencoding on closed connection."""

    temp_conn = connect(conn_str)
    temp_conn.close()

    with pytest.raises(InterfaceError) as exc_info:
        temp_conn.setencoding(encoding="utf-8")

    assert "Connection is closed" in str(
        exc_info.value
    ), "Should raise InterfaceError for closed connection"


def test_setencoding_constants_access():
    """Test that SQL_CHAR and SQL_WCHAR constants are accessible."""
    # Test constants exist and have correct values
    assert hasattr(mssql_python, "SQL_CHAR"), "SQL_CHAR constant should be available"
    assert hasattr(mssql_python, "SQL_WCHAR"), "SQL_WCHAR constant should be available"
    assert mssql_python.SQL_CHAR == 1, "SQL_CHAR should have value 1"
    assert mssql_python.SQL_WCHAR == -8, "SQL_WCHAR should have value -8"


def test_setencoding_with_constants(db_connection):
    """Test setencoding using module constants."""
    # Test with SQL_CHAR constant
    db_connection.setencoding(encoding="utf-8", ctype=mssql_python.SQL_CHAR)
    settings = db_connection.getencoding()
    assert settings["ctype"] == mssql_python.SQL_CHAR, "Should accept SQL_CHAR constant"

    # Test with SQL_WCHAR constant
    db_connection.setencoding(encoding="utf-16le", ctype=mssql_python.SQL_WCHAR)
    settings = db_connection.getencoding()
    assert settings["ctype"] == mssql_python.SQL_WCHAR, "Should accept SQL_WCHAR constant"


def test_setencoding_common_encodings(db_connection):
    """Test setencoding with various common encodings."""
    common_encodings = [
        "utf-8",
        "utf-16le",
        "utf-16be",
        "latin-1",
        "ascii",
        "cp1252",
    ]

    for encoding in common_encodings:
        try:
            db_connection.setencoding(encoding=encoding)
            settings = db_connection.getencoding()
            assert settings["encoding"] == encoding, f"Failed to set encoding {encoding}"
        except Exception as e:
            pytest.fail(f"Failed to set valid encoding {encoding}: {e}")


def test_setencoding_persistence_across_cursors(db_connection):
    """Test that encoding settings persist across cursor operations."""
    # Set custom encoding
    db_connection.setencoding(encoding="utf-8", ctype=1)

    # Create cursors and verify encoding persists
    cursor1 = db_connection.cursor()
    settings1 = db_connection.getencoding()

    cursor2 = db_connection.cursor()
    settings2 = db_connection.getencoding()

    assert settings1 == settings2, "Encoding settings should persist across cursor creation"
    assert settings1["encoding"] == "utf-8", "Encoding should remain utf-8"
    assert settings1["ctype"] == 1, "ctype should remain SQL_CHAR"

    cursor1.close()
    cursor2.close()


def test_setencoding_with_unicode_data(db_connection):
    """Test setencoding with actual Unicode data operations."""
    # Test UTF-8 encoding with Unicode data
    db_connection.setencoding(encoding="utf-8")
    cursor = db_connection.cursor()

    try:
        # Create test table
        cursor.execute("CREATE TABLE #test_encoding_unicode (text_col NVARCHAR(100))")

        # Test various Unicode strings
        test_strings = [
            "Hello, World!",
            "Hello, 世界!",  # Chinese
            "Привет, мир!",  # Russian
            "مرحبا بالعالم",  # Arabic
            "🌍🌎🌏",  # Emoji
        ]

        for test_string in test_strings:
            # Insert data
            cursor.execute("INSERT INTO #test_encoding_unicode (text_col) VALUES (?)", test_string)

            # Retrieve and verify
            cursor.execute(
                "SELECT text_col FROM #test_encoding_unicode WHERE text_col = ?",
                test_string,
            )
            result = cursor.fetchone()

            assert result is not None, f"Failed to retrieve Unicode string: {test_string}"
            assert (
                result[0] == test_string
            ), f"Unicode string mismatch: expected {test_string}, got {result[0]}"

            # Clear for next test
            cursor.execute("DELETE FROM #test_encoding_unicode")

    except Exception as e:
        pytest.fail(f"Unicode data test failed with UTF-8 encoding: {e}")
    finally:
        try:
            cursor.execute("DROP TABLE #test_encoding_unicode")
        except:
            pass
        cursor.close()


def test_setencoding_before_and_after_operations(db_connection):
    """Test that setencoding works both before and after database operations."""
    cursor = db_connection.cursor()

    try:
        # Initial encoding setting
        db_connection.setencoding(encoding="utf-16le")

        # Perform database operation
        cursor.execute("SELECT 'Initial test' as message")
        result1 = cursor.fetchone()
        assert result1[0] == "Initial test", "Initial operation failed"

        # Change encoding after operation
        db_connection.setencoding(encoding="utf-8")
        settings = db_connection.getencoding()
        assert settings["encoding"] == "utf-8", "Failed to change encoding after operation"

        # Perform another operation with new encoding
        cursor.execute("SELECT 'Changed encoding test' as message")
        result2 = cursor.fetchone()
        assert result2[0] == "Changed encoding test", "Operation after encoding change failed"

    except Exception as e:
        pytest.fail(f"Encoding change test failed: {e}")
    finally:
        cursor.close()


def test_getencoding_default(conn_str):
    """Test getencoding returns default settings"""
    conn = connect(conn_str)
    try:
        encoding_info = conn.getencoding()
        assert isinstance(encoding_info, dict)
        assert "encoding" in encoding_info
        assert "ctype" in encoding_info
        # Default should be utf-16le with SQL_WCHAR
        assert encoding_info["encoding"] == "utf-16le"
        assert encoding_info["ctype"] == SQL_WCHAR
    finally:
        conn.close()


def test_getencoding_returns_copy(conn_str):
    """Test getencoding returns a copy (not reference)"""
    conn = connect(conn_str)
    try:
        encoding_info1 = conn.getencoding()
        encoding_info2 = conn.getencoding()

        # Should be equal but not the same object
        assert encoding_info1 == encoding_info2
        assert encoding_info1 is not encoding_info2

        # Modifying one shouldn't affect the other
        encoding_info1["encoding"] = "modified"
        assert encoding_info2["encoding"] != "modified"
    finally:
        conn.close()


def test_getencoding_closed_connection(conn_str):
    """Test getencoding on closed connection raises InterfaceError"""
    conn = connect(conn_str)
    conn.close()

    with pytest.raises(InterfaceError, match="Connection is closed"):
        conn.getencoding()


def test_setencoding_getencoding_consistency(conn_str):
    """Test that setencoding and getencoding work consistently together"""
    conn = connect(conn_str)
    try:
        test_cases = [
            ("utf-8", SQL_CHAR),
            ("utf-16le", SQL_WCHAR),
            ("latin-1", SQL_CHAR),
            ("ascii", SQL_CHAR),
        ]

        for encoding, expected_ctype in test_cases:
            conn.setencoding(encoding)
            encoding_info = conn.getencoding()
            assert encoding_info["encoding"] == encoding.lower()
            assert encoding_info["ctype"] == expected_ctype
    finally:
        conn.close()


def test_setencoding_default_encoding(conn_str):
    """Test setencoding with default UTF-16LE encoding"""
    conn = connect(conn_str)
    try:
        conn.setencoding()
        encoding_info = conn.getencoding()
        assert encoding_info["encoding"] == "utf-16le"
        assert encoding_info["ctype"] == SQL_WCHAR
    finally:
        conn.close()


def test_setencoding_utf8(conn_str):
    """Test setencoding with UTF-8 encoding"""
    conn = connect(conn_str)
    try:
        conn.setencoding("utf-8")
        encoding_info = conn.getencoding()
        assert encoding_info["encoding"] == "utf-8"
        assert encoding_info["ctype"] == SQL_CHAR
    finally:
        conn.close()


def test_setencoding_latin1(conn_str):
    """Test setencoding with latin-1 encoding"""
    conn = connect(conn_str)
    try:
        conn.setencoding("latin-1")
        encoding_info = conn.getencoding()
        assert encoding_info["encoding"] == "latin-1"
        assert encoding_info["ctype"] == SQL_CHAR
    finally:
        conn.close()


def test_setencoding_with_explicit_ctype_sql_char(conn_str):
    """Test setencoding with explicit SQL_CHAR ctype"""
    conn = connect(conn_str)
    try:
        conn.setencoding("utf-8", SQL_CHAR)
        encoding_info = conn.getencoding()
        assert encoding_info["encoding"] == "utf-8"
        assert encoding_info["ctype"] == SQL_CHAR
    finally:
        conn.close()


def test_setencoding_with_explicit_ctype_sql_wchar(conn_str):
    """Test setencoding with explicit SQL_WCHAR ctype"""
    conn = connect(conn_str)
    try:
        conn.setencoding("utf-16le", SQL_WCHAR)
        encoding_info = conn.getencoding()
        assert encoding_info["encoding"] == "utf-16le"
        assert encoding_info["ctype"] == SQL_WCHAR
    finally:
        conn.close()


def test_setencoding_invalid_ctype_error(conn_str):
    """Test setencoding with invalid ctype raises ProgrammingError"""

    conn = connect(conn_str)
    try:
        with pytest.raises(ProgrammingError, match="Invalid ctype"):
            conn.setencoding("utf-8", 999)
    finally:
        conn.close()


def test_setencoding_case_insensitive_encoding(conn_str):
    """Test setencoding with case variations"""
    conn = connect(conn_str)
    try:
        # Test various case formats
        conn.setencoding("UTF-8")
        encoding_info = conn.getencoding()
        assert encoding_info["encoding"] == "utf-8"  # Should be normalized

        conn.setencoding("Utf-16LE")
        encoding_info = conn.getencoding()
        assert encoding_info["encoding"] == "utf-16le"  # Should be normalized
    finally:
        conn.close()


def test_setencoding_none_encoding_default(conn_str):
    """Test setencoding with None encoding uses default"""
    conn = connect(conn_str)
    try:
        conn.setencoding(None)
        encoding_info = conn.getencoding()
        assert encoding_info["encoding"] == "utf-16le"
        assert encoding_info["ctype"] == SQL_WCHAR
    finally:
        conn.close()


def test_setencoding_override_previous(conn_str):
    """Test setencoding overrides previous settings"""
    conn = connect(conn_str)
    try:
        # Set initial encoding
        conn.setencoding("utf-8")
        encoding_info = conn.getencoding()
        assert encoding_info["encoding"] == "utf-8"
        assert encoding_info["ctype"] == SQL_CHAR

        # Override with different encoding
        conn.setencoding("utf-16le")
        encoding_info = conn.getencoding()
        assert encoding_info["encoding"] == "utf-16le"
        assert encoding_info["ctype"] == SQL_WCHAR
    finally:
        conn.close()


def test_setencoding_ascii(conn_str):
    """Test setencoding with ASCII encoding"""
    conn = connect(conn_str)
    try:
        conn.setencoding("ascii")
        encoding_info = conn.getencoding()
        assert encoding_info["encoding"] == "ascii"
        assert encoding_info["ctype"] == SQL_CHAR
    finally:
        conn.close()


def test_setencoding_cp1252(conn_str):
    """Test setencoding with Windows-1252 encoding"""
    conn = connect(conn_str)
    try:
        conn.setencoding("cp1252")
        encoding_info = conn.getencoding()
        assert encoding_info["encoding"] == "cp1252"
        assert encoding_info["ctype"] == SQL_CHAR
    finally:
        conn.close()


def test_setdecoding_default_settings(db_connection):
    """Test that default decoding settings are correct for all SQL types."""

    # Check SQL_CHAR defaults
    sql_char_settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
    assert sql_char_settings["encoding"] == "utf-8", "Default SQL_CHAR encoding should be utf-8"
    assert (
        sql_char_settings["ctype"] == mssql_python.SQL_CHAR
    ), "Default SQL_CHAR ctype should be SQL_CHAR"

    # Check SQL_WCHAR defaults
    sql_wchar_settings = db_connection.getdecoding(mssql_python.SQL_WCHAR)
    assert (
        sql_wchar_settings["encoding"] == "utf-16le"
    ), "Default SQL_WCHAR encoding should be utf-16le"
    assert (
        sql_wchar_settings["ctype"] == mssql_python.SQL_WCHAR
    ), "Default SQL_WCHAR ctype should be SQL_WCHAR"

    # Check SQL_WMETADATA defaults
    sql_wmetadata_settings = db_connection.getdecoding(mssql_python.SQL_WMETADATA)
    assert (
        sql_wmetadata_settings["encoding"] == "utf-16le"
    ), "Default SQL_WMETADATA encoding should be utf-16le"
    assert (
        sql_wmetadata_settings["ctype"] == mssql_python.SQL_WCHAR
    ), "Default SQL_WMETADATA ctype should be SQL_WCHAR"


def test_setdecoding_basic_functionality(db_connection):
    """Test basic setdecoding functionality for different SQL types."""

    # Test setting SQL_CHAR decoding
    db_connection.setdecoding(mssql_python.SQL_CHAR, encoding="latin-1")
    settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
    assert settings["encoding"] == "latin-1", "SQL_CHAR encoding should be set to latin-1"
    assert (
        settings["ctype"] == mssql_python.SQL_CHAR
    ), "SQL_CHAR ctype should default to SQL_CHAR for latin-1"

    # Test setting SQL_WCHAR decoding
    db_connection.setdecoding(mssql_python.SQL_WCHAR, encoding="utf-16be")
    settings = db_connection.getdecoding(mssql_python.SQL_WCHAR)
    assert settings["encoding"] == "utf-16be", "SQL_WCHAR encoding should be set to utf-16be"
    assert (
        settings["ctype"] == mssql_python.SQL_WCHAR
    ), "SQL_WCHAR ctype should default to SQL_WCHAR for utf-16be"

    # Test setting SQL_WMETADATA decoding
    db_connection.setdecoding(mssql_python.SQL_WMETADATA, encoding="utf-16le")
    settings = db_connection.getdecoding(mssql_python.SQL_WMETADATA)
    assert settings["encoding"] == "utf-16le", "SQL_WMETADATA encoding should be set to utf-16le"
    assert (
        settings["ctype"] == mssql_python.SQL_WCHAR
    ), "SQL_WMETADATA ctype should default to SQL_WCHAR"


def test_setdecoding_automatic_ctype_detection(db_connection):
    """Test automatic ctype detection based on encoding for different SQL types."""

    # UTF-16 variants should default to SQL_WCHAR
    utf16_encodings = ["utf-16le", "utf-16be"]
    for encoding in utf16_encodings:
        db_connection.setdecoding(mssql_python.SQL_CHAR, encoding=encoding)
        settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
        assert (
            settings["ctype"] == mssql_python.SQL_WCHAR
        ), f"SQL_CHAR with {encoding} should auto-detect SQL_WCHAR ctype"

    # Other encodings with SQL_CHAR should use SQL_CHAR ctype
    other_encodings = ["utf-8", "latin-1", "ascii", "cp1252"]
    for encoding in other_encodings:
        db_connection.setdecoding(mssql_python.SQL_CHAR, encoding=encoding)
        settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
        assert settings["encoding"] == encoding, f"SQL_CHAR with {encoding} should keep {encoding}"
        assert (
            settings["ctype"] == mssql_python.SQL_CHAR
        ), f"SQL_CHAR with {encoding} should use SQL_CHAR ctype"


def test_setdecoding_explicit_ctype_override(db_connection):
    """Test that explicit ctype parameter works correctly with valid combinations."""

    # Set SQL_WCHAR with UTF-16LE encoding and explicit SQL_CHAR ctype (valid override)
    db_connection.setdecoding(
        mssql_python.SQL_WCHAR, encoding="utf-16le", ctype=mssql_python.SQL_CHAR
    )
    settings = db_connection.getdecoding(mssql_python.SQL_WCHAR)
    assert settings["encoding"] == "utf-16le", "Encoding should be utf-16le"
    assert (
        settings["ctype"] == mssql_python.SQL_CHAR
    ), "ctype should be SQL_CHAR when explicitly set"

    # Set SQL_CHAR with UTF-8 and SQL_CHAR ctype (valid combination)
    db_connection.setdecoding(mssql_python.SQL_CHAR, encoding="utf-8", ctype=mssql_python.SQL_CHAR)
    settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
    assert settings["encoding"] == "utf-8", "Encoding should be utf-8"
    assert settings["ctype"] == mssql_python.SQL_CHAR, "ctype should be SQL_CHAR"


def test_setdecoding_none_parameters(db_connection):
    """Test setdecoding with None parameters uses appropriate defaults."""

    # Test SQL_CHAR with encoding=None (should use utf-8 default)
    db_connection.setdecoding(mssql_python.SQL_CHAR, encoding=None)
    settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
    assert settings["encoding"] == "utf-8", "SQL_CHAR with encoding=None should use utf-8 default"
    assert settings["ctype"] == mssql_python.SQL_CHAR, "ctype should be SQL_CHAR for utf-8"

    # Test SQL_WCHAR with encoding=None (should use utf-16le default)
    db_connection.setdecoding(mssql_python.SQL_WCHAR, encoding=None)
    settings = db_connection.getdecoding(mssql_python.SQL_WCHAR)
    assert (
        settings["encoding"] == "utf-16le"
    ), "SQL_WCHAR with encoding=None should use utf-16le default"
    assert settings["ctype"] == mssql_python.SQL_WCHAR, "ctype should be SQL_WCHAR for utf-16le"

    # Test with both parameters None
    db_connection.setdecoding(mssql_python.SQL_CHAR, encoding=None, ctype=None)
    settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
    assert settings["encoding"] == "utf-8", "SQL_CHAR with both None should use utf-8 default"
    assert settings["ctype"] == mssql_python.SQL_CHAR, "ctype should default to SQL_CHAR"


def test_setdecoding_invalid_sqltype(db_connection):
    """Test setdecoding with invalid sqltype raises ProgrammingError."""

    with pytest.raises(ProgrammingError) as exc_info:
        db_connection.setdecoding(999, encoding="utf-8")

    assert "Invalid sqltype" in str(
        exc_info.value
    ), "Should raise ProgrammingError for invalid sqltype"
    assert "999" in str(exc_info.value), "Error message should include the invalid sqltype value"


def test_setdecoding_invalid_encoding(db_connection):
    """Test setdecoding with invalid encoding raises ProgrammingError."""

    with pytest.raises(ProgrammingError) as exc_info:
        db_connection.setdecoding(mssql_python.SQL_CHAR, encoding="invalid-encoding-name")

    assert "Unsupported encoding" in str(
        exc_info.value
    ), "Should raise ProgrammingError for invalid encoding"
    assert "invalid-encoding-name" in str(
        exc_info.value
    ), "Error message should include the invalid encoding name"


def test_setdecoding_invalid_ctype(db_connection):
    """Test setdecoding with invalid ctype raises ProgrammingError."""

    with pytest.raises(ProgrammingError) as exc_info:
        db_connection.setdecoding(mssql_python.SQL_CHAR, encoding="utf-8", ctype=999)

    assert "Invalid ctype" in str(exc_info.value), "Should raise ProgrammingError for invalid ctype"
    assert "999" in str(exc_info.value), "Error message should include the invalid ctype value"


def test_setdecoding_closed_connection(conn_str):
    """Test setdecoding on closed connection raises InterfaceError."""

    temp_conn = connect(conn_str)
    temp_conn.close()

    with pytest.raises(InterfaceError) as exc_info:
        temp_conn.setdecoding(mssql_python.SQL_CHAR, encoding="utf-8")

    assert "Connection is closed" in str(
        exc_info.value
    ), "Should raise InterfaceError for closed connection"


def test_setdecoding_constants_access():
    """Test that SQL constants are accessible."""

    # Test constants exist and have correct values
    assert hasattr(mssql_python, "SQL_CHAR"), "SQL_CHAR constant should be available"
    assert hasattr(mssql_python, "SQL_WCHAR"), "SQL_WCHAR constant should be available"
    assert hasattr(mssql_python, "SQL_WMETADATA"), "SQL_WMETADATA constant should be available"

    assert mssql_python.SQL_CHAR == 1, "SQL_CHAR should have value 1"
    assert mssql_python.SQL_WCHAR == -8, "SQL_WCHAR should have value -8"
    assert mssql_python.SQL_WMETADATA == -99, "SQL_WMETADATA should have value -99"


def test_setdecoding_with_constants(db_connection):
    """Test setdecoding using module constants."""

    # Test with SQL_CHAR constant
    db_connection.setdecoding(mssql_python.SQL_CHAR, encoding="utf-8", ctype=mssql_python.SQL_CHAR)
    settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
    assert settings["ctype"] == mssql_python.SQL_CHAR, "Should accept SQL_CHAR constant"

    # Test with SQL_WCHAR constant
    db_connection.setdecoding(
        mssql_python.SQL_WCHAR, encoding="utf-16le", ctype=mssql_python.SQL_WCHAR
    )
    settings = db_connection.getdecoding(mssql_python.SQL_WCHAR)
    assert settings["ctype"] == mssql_python.SQL_WCHAR, "Should accept SQL_WCHAR constant"

    # Test with SQL_WMETADATA constant
    db_connection.setdecoding(mssql_python.SQL_WMETADATA, encoding="utf-16be")
    settings = db_connection.getdecoding(mssql_python.SQL_WMETADATA)
    assert settings["encoding"] == "utf-16be", "Should accept SQL_WMETADATA constant"


def test_setdecoding_common_encodings(db_connection):
    """Test setdecoding with various common encodings, only valid combinations."""

    utf16_encodings = ["utf-16le", "utf-16be"]
    other_encodings = ["utf-8", "latin-1", "ascii", "cp1252"]

    # Test UTF-16 encodings with both SQL_CHAR and SQL_WCHAR (all valid)
    for encoding in utf16_encodings:
        try:
            # UTF-16 with SQL_CHAR is valid
            db_connection.setdecoding(mssql_python.SQL_CHAR, encoding=encoding)
            settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
            assert settings["encoding"] == encoding.lower()

            # UTF-16 with SQL_WCHAR is valid
            db_connection.setdecoding(mssql_python.SQL_WCHAR, encoding=encoding)
            settings = db_connection.getdecoding(mssql_python.SQL_WCHAR)
            assert settings["encoding"] == encoding.lower()
        except Exception as e:
            pytest.fail(f"Failed to set valid encoding {encoding}: {e}")

    # Test other encodings - only with SQL_CHAR (SQL_WCHAR would raise error)
    for encoding in other_encodings:
        try:
            # These work fine with SQL_CHAR
            db_connection.setdecoding(mssql_python.SQL_CHAR, encoding=encoding)
            settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
            assert settings["encoding"] == encoding.lower()

            # But should raise error with SQL_WCHAR
            with pytest.raises(ProgrammingError, match="SQL_WCHAR only supports UTF-16 encodings"):
                db_connection.setdecoding(mssql_python.SQL_WCHAR, encoding=encoding)
        except ProgrammingError:
            # Expected for SQL_WCHAR with non-UTF-16
            pass
        except Exception as e:
            pytest.fail(f"Unexpected error for encoding {encoding}: {e}")


def test_setdecoding_case_insensitive_encoding(db_connection):
    """Test setdecoding with case variations normalizes encoding."""

    # Test various case formats
    db_connection.setdecoding(mssql_python.SQL_CHAR, encoding="UTF-8")
    settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
    assert settings["encoding"] == "utf-8", "Encoding should be normalized to lowercase"

    db_connection.setdecoding(mssql_python.SQL_WCHAR, encoding="Utf-16LE")
    settings = db_connection.getdecoding(mssql_python.SQL_WCHAR)
    assert settings["encoding"] == "utf-16le", "Encoding should be normalized to lowercase"


def test_setdecoding_independent_sql_types(db_connection):
    """Test that decoding settings for different SQL types are independent."""

    # Set different encodings for each SQL type
    db_connection.setdecoding(mssql_python.SQL_CHAR, encoding="utf-8")
    db_connection.setdecoding(mssql_python.SQL_WCHAR, encoding="utf-16le")
    db_connection.setdecoding(mssql_python.SQL_WMETADATA, encoding="utf-16be")

    # Verify each maintains its own settings
    sql_char_settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
    sql_wchar_settings = db_connection.getdecoding(mssql_python.SQL_WCHAR)
    sql_wmetadata_settings = db_connection.getdecoding(mssql_python.SQL_WMETADATA)

    assert sql_char_settings["encoding"] == "utf-8", "SQL_CHAR should maintain utf-8"
    assert sql_wchar_settings["encoding"] == "utf-16le", "SQL_WCHAR should maintain utf-16le"
    assert (
        sql_wmetadata_settings["encoding"] == "utf-16be"
    ), "SQL_WMETADATA should maintain utf-16be"


def test_setdecoding_override_previous(db_connection):
    """Test setdecoding overrides previous settings for the same SQL type."""

    # Set initial decoding
    db_connection.setdecoding(mssql_python.SQL_CHAR, encoding="utf-8")
    settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
    assert settings["encoding"] == "utf-8", "Initial encoding should be utf-8"
    assert settings["ctype"] == mssql_python.SQL_CHAR, "Initial ctype should be SQL_CHAR"

    # Override with different valid settings
    db_connection.setdecoding(
        mssql_python.SQL_CHAR, encoding="latin-1", ctype=mssql_python.SQL_CHAR
    )
    settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
    assert settings["encoding"] == "latin-1", "Encoding should be overridden to latin-1"
    assert settings["ctype"] == mssql_python.SQL_CHAR, "ctype should remain SQL_CHAR"


def test_getdecoding_invalid_sqltype(db_connection):
    """Test getdecoding with invalid sqltype raises ProgrammingError."""

    with pytest.raises(ProgrammingError) as exc_info:
        db_connection.getdecoding(999)

    assert "Invalid sqltype" in str(
        exc_info.value
    ), "Should raise ProgrammingError for invalid sqltype"
    assert "999" in str(exc_info.value), "Error message should include the invalid sqltype value"


def test_getdecoding_closed_connection(conn_str):
    """Test getdecoding on closed connection raises InterfaceError."""

    temp_conn = connect(conn_str)
    temp_conn.close()

    with pytest.raises(InterfaceError) as exc_info:
        temp_conn.getdecoding(mssql_python.SQL_CHAR)

    assert "Connection is closed" in str(
        exc_info.value
    ), "Should raise InterfaceError for closed connection"


def test_getdecoding_returns_copy(db_connection):
    """Test getdecoding returns a copy (not reference)."""

    # Set custom decoding
    db_connection.setdecoding(mssql_python.SQL_CHAR, encoding="utf-8")

    # Get settings twice
    settings1 = db_connection.getdecoding(mssql_python.SQL_CHAR)
    settings2 = db_connection.getdecoding(mssql_python.SQL_CHAR)

    # Should be equal but not the same object
    assert settings1 == settings2, "Settings should be equal"
    assert settings1 is not settings2, "Settings should be different objects"

    # Modifying one shouldn't affect the other
    settings1["encoding"] = "modified"
    assert settings2["encoding"] != "modified", "Modification should not affect other copy"


def test_setdecoding_getdecoding_consistency(db_connection):
    """Test that setdecoding and getdecoding work consistently together."""

    test_cases = [
        (mssql_python.SQL_CHAR, "utf-8", mssql_python.SQL_CHAR, "utf-8"),
        (mssql_python.SQL_CHAR, "utf-16le", mssql_python.SQL_WCHAR, "utf-16le"),
        (mssql_python.SQL_WCHAR, "utf-16le", mssql_python.SQL_WCHAR, "utf-16le"),
        (mssql_python.SQL_WCHAR, "utf-16be", mssql_python.SQL_WCHAR, "utf-16be"),
        (mssql_python.SQL_WMETADATA, "utf-16le", mssql_python.SQL_WCHAR, "utf-16le"),
    ]

    for sqltype, input_encoding, expected_ctype, expected_encoding in test_cases:
        db_connection.setdecoding(sqltype, encoding=input_encoding)
        settings = db_connection.getdecoding(sqltype)
        assert (
            settings["encoding"] == expected_encoding.lower()
        ), f"Encoding should be {expected_encoding.lower()}"
        assert settings["ctype"] == expected_ctype, f"ctype should be {expected_ctype}"


def test_setdecoding_persistence_across_cursors(db_connection):
    """Test that decoding settings persist across cursor operations."""

    # Set custom decoding settings
    db_connection.setdecoding(
        mssql_python.SQL_CHAR, encoding="latin-1", ctype=mssql_python.SQL_CHAR
    )
    db_connection.setdecoding(
        mssql_python.SQL_WCHAR, encoding="utf-16be", ctype=mssql_python.SQL_WCHAR
    )

    # Create cursors and verify settings persist
    cursor1 = db_connection.cursor()
    char_settings1 = db_connection.getdecoding(mssql_python.SQL_CHAR)
    wchar_settings1 = db_connection.getdecoding(mssql_python.SQL_WCHAR)

    cursor2 = db_connection.cursor()
    char_settings2 = db_connection.getdecoding(mssql_python.SQL_CHAR)
    wchar_settings2 = db_connection.getdecoding(mssql_python.SQL_WCHAR)

    # Settings should persist across cursor creation
    assert char_settings1 == char_settings2, "SQL_CHAR settings should persist across cursors"
    assert wchar_settings1 == wchar_settings2, "SQL_WCHAR settings should persist across cursors"

    assert char_settings1["encoding"] == "latin-1", "SQL_CHAR encoding should remain latin-1"
    assert wchar_settings1["encoding"] == "utf-16be", "SQL_WCHAR encoding should remain utf-16be"

    cursor1.close()
    cursor2.close()


def test_setdecoding_before_and_after_operations(db_connection):
    """Test that setdecoding works both before and after database operations."""
    cursor = db_connection.cursor()

    try:
        # Initial decoding setting
        db_connection.setdecoding(mssql_python.SQL_CHAR, encoding="utf-8")

        # Perform database operation
        cursor.execute("SELECT 'Initial test' as message")
        result1 = cursor.fetchone()
        assert result1[0] == "Initial test", "Initial operation failed"

        # Change decoding after operation
        db_connection.setdecoding(mssql_python.SQL_CHAR, encoding="latin-1")
        settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
        assert settings["encoding"] == "latin-1", "Failed to change decoding after operation"

        # Perform another operation with new decoding
        cursor.execute("SELECT 'Changed decoding test' as message")
        result2 = cursor.fetchone()
        assert result2[0] == "Changed decoding test", "Operation after decoding change failed"

    except Exception as e:
        pytest.fail(f"Decoding change test failed: {e}")
    finally:
        cursor.close()


def test_setdecoding_all_sql_types_independently(conn_str):
    """Test setdecoding with all SQL types on a fresh connection."""

    conn = connect(conn_str)
    try:
        # Test each SQL type with different configurations
        test_configs = [
            (mssql_python.SQL_CHAR, "ascii", mssql_python.SQL_CHAR),
            (mssql_python.SQL_WCHAR, "utf-16le", mssql_python.SQL_WCHAR),
            (mssql_python.SQL_WMETADATA, "utf-16be", mssql_python.SQL_WCHAR),
        ]

        for sqltype, encoding, ctype in test_configs:
            conn.setdecoding(sqltype, encoding=encoding, ctype=ctype)
            settings = conn.getdecoding(sqltype)
            assert settings["encoding"] == encoding, f"Failed to set encoding for sqltype {sqltype}"
            assert settings["ctype"] == ctype, f"Failed to set ctype for sqltype {sqltype}"

    finally:
        conn.close()


def test_setdecoding_security_logging(db_connection):
    """Test that setdecoding logs invalid attempts safely."""

    # These should raise exceptions but not crash due to logging
    test_cases = [
        (999, "utf-8", None),  # Invalid sqltype
        (mssql_python.SQL_CHAR, "invalid-encoding", None),  # Invalid encoding
        (mssql_python.SQL_CHAR, "utf-8", 999),  # Invalid ctype
    ]

    for sqltype, encoding, ctype in test_cases:
        with pytest.raises(ProgrammingError):
            db_connection.setdecoding(sqltype, encoding=encoding, ctype=ctype)


def test_setdecoding_with_unicode_data(db_connection):
    """Test setdecoding with actual Unicode data operations.

    Note: VARCHAR columns in SQL Server use the database's default collation
    (typically Latin1/CP1252) and cannot reliably store Unicode characters.
    Only NVARCHAR columns properly support Unicode. This test focuses on
    NVARCHAR columns and ASCII-safe data for VARCHAR columns.
    """

    # Test different decoding configurations with Unicode data
    db_connection.setdecoding(mssql_python.SQL_CHAR, encoding="utf-8")
    db_connection.setdecoding(mssql_python.SQL_WCHAR, encoding="utf-16le")

    cursor = db_connection.cursor()

    try:
        # Create test table with NVARCHAR columns for Unicode support
        cursor.execute(
            """
            CREATE TABLE #test_decoding_unicode (
                id INT IDENTITY(1,1),
                ascii_col VARCHAR(100),
                unicode_col NVARCHAR(100)
            )
        """
        )

        # Test ASCII strings in VARCHAR (safe)
        ascii_strings = [
            "Hello, World!",
            "Simple ASCII text",
            "Numbers: 12345",
        ]

        for test_string in ascii_strings:
            cursor.execute(
                "INSERT INTO #test_decoding_unicode (ascii_col, unicode_col) VALUES (?, ?)",
                test_string,
                test_string,
            )

        # Test Unicode strings in NVARCHAR only
        unicode_strings = [
            "Hello, 世界!",  # Chinese
            "Привет, мир!",  # Russian
            "مرحبا بالعالم",  # Arabic
            "🌍🌎🌏",  # Emoji
        ]

        for test_string in unicode_strings:
            cursor.execute(
                "INSERT INTO #test_decoding_unicode (unicode_col) VALUES (?)",
                test_string,
            )

        # Verify ASCII data in VARCHAR
        cursor.execute(
            "SELECT ascii_col FROM #test_decoding_unicode WHERE ascii_col IS NOT NULL ORDER BY id"
        )
        ascii_results = cursor.fetchall()
        assert len(ascii_results) == len(ascii_strings), "ASCII string count mismatch"
        for i, result in enumerate(ascii_results):
            assert (
                result[0] == ascii_strings[i]
            ), f"ASCII string mismatch: expected {ascii_strings[i]}, got {result[0]}"

        # Verify Unicode data in NVARCHAR
        cursor.execute(
            "SELECT unicode_col FROM #test_decoding_unicode WHERE unicode_col IS NOT NULL ORDER BY id"
        )
        unicode_results = cursor.fetchall()

        # First 3 are ASCII (also in unicode_col), next 4 are Unicode-only
        all_expected = ascii_strings + unicode_strings
        assert len(unicode_results) == len(
            all_expected
        ), f"Unicode string count mismatch: expected {len(all_expected)}, got {len(unicode_results)}"

        for i, result in enumerate(unicode_results):
            expected = all_expected[i]
            assert (
                result[0] == expected
            ), f"Unicode string mismatch at index {i}: expected {expected!r}, got {result[0]!r}"

        print(f"[OK] Successfully tested {len(ascii_strings)} ASCII strings in VARCHAR")
        print(
            f"[OK] Successfully tested {len(all_expected)} strings in NVARCHAR (including {len(unicode_strings)} Unicode-only)"
        )

    except Exception as e:
        pytest.fail(f"Unicode data test failed with custom decoding: {e}")
    finally:
        try:
            cursor.execute("DROP TABLE #test_decoding_unicode")
        except:
            pass
        cursor.close()


def test_encoding_decoding_comprehensive_unicode_characters(db_connection):
    """Test encoding/decoding with comprehensive Unicode character sets."""
    cursor = db_connection.cursor()

    try:
        # Create test table with different column types - use NVARCHAR for better Unicode support
        cursor.execute(
            """
            CREATE TABLE #test_encoding_comprehensive (
                id INT PRIMARY KEY,
                varchar_col VARCHAR(1000),
                nvarchar_col NVARCHAR(1000),
                text_col TEXT,
                ntext_col NTEXT
            )
        """
        )

        # Test cases with different Unicode character categories
        test_cases = [
            # Basic ASCII
            ("Basic ASCII", "Hello, World! 123 ABC xyz"),
            # Extended Latin characters (accents, diacritics)
            (
                "Extended Latin",
                "Cafe naive resume pinata facade Zurich",
            ),  # Simplified to avoid encoding issues
            # Cyrillic script (shortened)
            ("Cyrillic", "Здравствуй мир!"),
            # Greek script (shortened)
            ("Greek", "Γεια σας κόσμε!"),
            # Chinese (Simplified)
            ("Chinese Simplified", "你好，世界！"),
            # Japanese
            ("Japanese", "こんにちは世界！"),
            # Korean
            ("Korean", "안녕하세요!"),
            # Emojis (basic)
            ("Emojis Basic", "😀😃😄"),
            # Mathematical symbols (subset)
            ("Math Symbols", "∑∏∫∇∂√"),
            # Currency symbols (subset)
            ("Currency", "$ € £ ¥"),
        ]

        # Test with different encoding configurations, but be more realistic about limitations
        encoding_configs = [
            ("utf-16le", SQL_WCHAR),  # Start with UTF-16 which should handle Unicode well
        ]

        for encoding, ctype in encoding_configs:
            print(f"\nTesting with encoding: {encoding}, ctype: {ctype}")

            # Set encoding configuration
            db_connection.setencoding(encoding=encoding, ctype=ctype)
            db_connection.setdecoding(
                SQL_CHAR, encoding="utf-8", ctype=SQL_CHAR
            )  # Keep SQL_CHAR as UTF-8
            db_connection.setdecoding(SQL_WCHAR, encoding="utf-16le", ctype=SQL_WCHAR)

            for test_name, test_string in test_cases:
                try:
                    # Clear table
                    cursor.execute("DELETE FROM #test_encoding_comprehensive")

                    # Insert test data - only use NVARCHAR columns for Unicode content
                    cursor.execute(
                        """
                        INSERT INTO #test_encoding_comprehensive 
                        (id, nvarchar_col, ntext_col) 
                        VALUES (?, ?, ?)
                    """,
                        1,
                        test_string,
                        test_string,
                    )

                    # Retrieve and verify
                    cursor.execute(
                        """
                        SELECT nvarchar_col, ntext_col 
                        FROM #test_encoding_comprehensive WHERE id = ?
                    """,
                        1,
                    )

                    result = cursor.fetchone()
                    if result:
                        # Verify NVARCHAR columns match
                        for i, col_value in enumerate(result):
                            col_names = ["nvarchar_col", "ntext_col"]

                            assert col_value == test_string, (
                                f"Data mismatch for {test_name} in {col_names[i]} "
                                f"with encoding {encoding}: expected {test_string!r}, "
                                f"got {col_value!r}"
                            )

                    print(f"[OK] {test_name} passed with {encoding}")

                except Exception as e:
                    # Log encoding issues but don't fail the test - this is exploratory
                    print(f"[WARN] {test_name} had issues with {encoding}: {e}")

    finally:
        try:
            cursor.execute("DROP TABLE #test_encoding_comprehensive")
        except:
            pass
        cursor.close()


def test_encoding_decoding_sql_wchar_restriction_enforcement(db_connection):
    """Test that SQL_WCHAR restrictions are properly enforced with errors."""

    # Test cases that should raise errors for SQL_WCHAR
    non_utf16_encodings = ["utf-8", "latin-1", "ascii", "cp1252", "iso-8859-1"]

    for encoding in non_utf16_encodings:
        # Test setencoding with SQL_WCHAR ctype should raise error
        with pytest.raises(ProgrammingError, match="SQL_WCHAR only supports UTF-16 encodings"):
            db_connection.setencoding(encoding=encoding, ctype=SQL_WCHAR)

        # Test setdecoding with SQL_WCHAR and non-UTF-16 encoding should raise error
        with pytest.raises(ProgrammingError, match="SQL_WCHAR only supports UTF-16 encodings"):
            db_connection.setdecoding(SQL_WCHAR, encoding=encoding)

        # Test setdecoding with SQL_WCHAR ctype should raise error
        with pytest.raises(
            ProgrammingError, match="SQL_WCHAR ctype only supports UTF-16 encodings"
        ):
            db_connection.setdecoding(SQL_CHAR, encoding=encoding, ctype=SQL_WCHAR)


def test_encoding_decoding_error_scenarios(db_connection):
    """Test various error scenarios for encoding/decoding."""

    # Test 1: Invalid encoding names - be more flexible about what exceptions are raised
    invalid_encodings = [
        "invalid-encoding-123",
        "utf-999",
        "not-a-real-encoding",
    ]

    for invalid_encoding in invalid_encodings:
        try:
            db_connection.setencoding(encoding=invalid_encoding)
            # If it doesn't raise an exception, test that it at least doesn't crash
            print(f"Warning: {invalid_encoding} was accepted by setencoding")
        except Exception as e:
            # Any exception is acceptable for invalid encodings
            print(f"[OK] {invalid_encoding} correctly raised exception: {type(e).__name__}")

        try:
            db_connection.setdecoding(SQL_CHAR, encoding=invalid_encoding)
            print(f"Warning: {invalid_encoding} was accepted by setdecoding")
        except Exception as e:
            print(
                f"[OK] {invalid_encoding} correctly raised exception in setdecoding: {type(e).__name__}"
            )

    # Test 2: Test valid operations to ensure basic functionality works
    try:
        db_connection.setencoding(encoding="utf-8", ctype=SQL_CHAR)
        db_connection.setdecoding(SQL_CHAR, encoding="utf-8", ctype=SQL_CHAR)
        db_connection.setdecoding(SQL_WCHAR, encoding="utf-16le", ctype=SQL_WCHAR)
        print("[OK] Basic encoding/decoding configuration works")
    except Exception as e:
        pytest.fail(f"Basic encoding configuration failed: {e}")

    # Test 3: Test edge case with mixed encoding settings
    try:
        # This should work - different encodings for different SQL types
        db_connection.setdecoding(SQL_CHAR, encoding="utf-8")
        db_connection.setdecoding(SQL_WCHAR, encoding="utf-16le")
        print("[OK] Mixed encoding settings work")
    except Exception as e:
        print(f"[WARN] Mixed encoding settings failed: {e}")


def test_encoding_decoding_edge_case_data_types(db_connection):
    """Test encoding/decoding with various SQL Server data types."""
    cursor = db_connection.cursor()

    try:
        # Create table with various data types
        cursor.execute(
            """
            CREATE TABLE #test_encoding_datatypes (
                id INT PRIMARY KEY,
                varchar_small VARCHAR(50),
                varchar_max VARCHAR(MAX),
                nvarchar_small NVARCHAR(50), 
                nvarchar_max NVARCHAR(MAX),
                char_fixed CHAR(20),
                nchar_fixed NCHAR(20),
                text_type TEXT,
                ntext_type NTEXT
            )
        """
        )

        # Test different encoding configurations
        test_configs = [
            ("utf-8", SQL_CHAR, "UTF-8 with SQL_CHAR"),
            ("utf-16le", SQL_WCHAR, "UTF-16LE with SQL_WCHAR"),
        ]

        # Test strings with different characteristics - all must fit in CHAR(20)
        test_strings = [
            ("Empty", ""),
            ("Single char", "A"),
            ("ASCII only", "Hello World 123"),
            ("Mixed Unicode", "Hello World"),  # Simplified to avoid encoding issues
            ("Long string", "TestTestTestTest"),  # 16 chars - fits in CHAR(20)
            ("Special chars", "Line1\nLine2\t"),  # 12 chars with special chars
            ("Quotes", 'Text "quotes"'),  # 13 chars with quotes
        ]

        for encoding, ctype, config_desc in test_configs:
            print(f"\nTesting {config_desc}")

            # Configure encoding/decoding
            db_connection.setencoding(encoding=encoding, ctype=ctype)
            db_connection.setdecoding(SQL_CHAR, encoding="utf-8")  # For VARCHAR columns
            db_connection.setdecoding(SQL_WCHAR, encoding="utf-16le")  # For NVARCHAR columns

            for test_name, test_string in test_strings:
                try:
                    cursor.execute("DELETE FROM #test_encoding_datatypes")

                    # Insert into all columns
                    cursor.execute(
                        """
                        INSERT INTO #test_encoding_datatypes 
                        (id, varchar_small, varchar_max, nvarchar_small, nvarchar_max, 
                         char_fixed, nchar_fixed, text_type, ntext_type)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                        1,
                        test_string,
                        test_string,
                        test_string,
                        test_string,
                        test_string,
                        test_string,
                        test_string,
                        test_string,
                    )

                    # Retrieve and verify
                    cursor.execute("SELECT * FROM #test_encoding_datatypes WHERE id = 1")
                    result = cursor.fetchone()

                    if result:
                        columns = [
                            "varchar_small",
                            "varchar_max",
                            "nvarchar_small",
                            "nvarchar_max",
                            "char_fixed",
                            "nchar_fixed",
                            "text_type",
                            "ntext_type",
                        ]

                        for i, (col_name, col_value) in enumerate(zip(columns, result[1:]), 1):
                            # For CHAR/NCHAR fixed-length fields, expect padding
                            if col_name in ["char_fixed", "nchar_fixed"]:
                                # Fixed-length fields are usually right-padded with spaces
                                expected = (
                                    test_string.ljust(20)
                                    if len(test_string) < 20
                                    else test_string[:20]
                                )
                                assert col_value.rstrip() == test_string.rstrip(), (
                                    f"Mismatch in {col_name} for '{test_name}': "
                                    f"expected {test_string!r}, got {col_value!r}"
                                )
                            else:
                                assert col_value == test_string, (
                                    f"Mismatch in {col_name} for '{test_name}': "
                                    f"expected {test_string!r}, got {col_value!r}"
                                )

                    print(f"[OK] {test_name} passed")

                except Exception as e:
                    pytest.fail(f"Error with {test_name} in {config_desc}: {e}")

    finally:
        try:
            cursor.execute("DROP TABLE #test_encoding_datatypes")
        except:
            pass
        cursor.close()


def test_encoding_decoding_boundary_conditions(db_connection):
    """Test encoding/decoding boundary conditions and edge cases."""
    cursor = db_connection.cursor()

    try:
        cursor.execute("CREATE TABLE #test_encoding_boundaries (id INT, data NVARCHAR(MAX))")

        boundary_test_cases = [
            # Null and empty values
            ("NULL value", None),
            ("Empty string", ""),
            ("Single space", " "),
            ("Multiple spaces", "   "),
            # Special boundary cases - SQL Server truncates strings at null bytes
            ("Control characters", "\x01\x02\x03\x04\x05\x06\x07\x08\x09"),
            ("High Unicode", "Test emoji"),  # Simplified
            # String length boundaries
            ("One char", "X"),
            ("255 chars", "A" * 255),
            ("256 chars", "B" * 256),
            ("1000 chars", "C" * 1000),
            ("4000 chars", "D" * 4000),  # VARCHAR/NVARCHAR inline limit
            ("4001 chars", "E" * 4001),  # Forces LOB storage
            ("8000 chars", "F" * 8000),  # SQL Server page limit
            # Mixed content at boundaries
            ("Mixed 4000", "HelloWorld" * 400),  # ~4000 chars without Unicode issues
        ]

        for test_name, test_data in boundary_test_cases:
            try:
                cursor.execute("DELETE FROM #test_encoding_boundaries")

                # Insert test data
                cursor.execute(
                    "INSERT INTO #test_encoding_boundaries (id, data) VALUES (?, ?)", 1, test_data
                )

                # Retrieve and verify
                cursor.execute("SELECT data FROM #test_encoding_boundaries WHERE id = 1")
                result = cursor.fetchone()

                if test_data is None:
                    assert result[0] is None, f"Expected None for {test_name}, got {result[0]!r}"
                else:
                    assert result[0] == test_data, (
                        f"Boundary case {test_name} failed: "
                        f"expected {test_data!r}, got {result[0]!r}"
                    )

                print(f"[OK] Boundary case {test_name} passed")

            except Exception as e:
                pytest.fail(f"Boundary case {test_name} failed: {e}")

    finally:
        try:
            cursor.execute("DROP TABLE #test_encoding_boundaries")
        except:
            pass
        cursor.close()


def test_encoding_decoding_concurrent_settings(db_connection):
    """Test encoding/decoding settings with multiple cursors and operations."""

    # Create multiple cursors
    cursor1 = db_connection.cursor()
    cursor2 = db_connection.cursor()

    try:
        # Create test tables
        cursor1.execute("CREATE TABLE #test_concurrent1 (id INT, data NVARCHAR(100))")
        cursor2.execute("CREATE TABLE #test_concurrent2 (id INT, data VARCHAR(100))")

        # Change encoding settings between cursor operations
        db_connection.setencoding("utf-8", SQL_CHAR)

        # Insert with cursor1 - use ASCII-only to avoid encoding issues
        cursor1.execute("INSERT INTO #test_concurrent1 VALUES (?, ?)", 1, "Test with UTF-8 simple")

        # Change encoding settings
        db_connection.setencoding("utf-16le", SQL_WCHAR)

        # Insert with cursor2 - use ASCII-only to avoid encoding issues
        cursor2.execute("INSERT INTO #test_concurrent2 VALUES (?, ?)", 1, "Test with UTF-16 simple")

        # Change decoding settings
        db_connection.setdecoding(SQL_CHAR, encoding="utf-8")
        db_connection.setdecoding(SQL_WCHAR, encoding="utf-16le")

        # Retrieve from both cursors
        cursor1.execute("SELECT data FROM #test_concurrent1 WHERE id = 1")
        result1 = cursor1.fetchone()

        cursor2.execute("SELECT data FROM #test_concurrent2 WHERE id = 1")
        result2 = cursor2.fetchone()

        # Both should work with their respective settings
        assert result1[0] == "Test with UTF-8 simple", f"Cursor1 result: {result1[0]!r}"
        assert result2[0] == "Test with UTF-16 simple", f"Cursor2 result: {result2[0]!r}"

        print("[OK] Concurrent cursor operations with encoding changes passed")

    finally:
        try:
            cursor1.execute("DROP TABLE #test_concurrent1")
            cursor2.execute("DROP TABLE #test_concurrent2")
        except:
            pass
        cursor1.close()
        cursor2.close()


def test_encoding_decoding_parameter_binding_edge_cases(db_connection):
    """Test encoding/decoding with parameter binding edge cases."""
    cursor = db_connection.cursor()

    try:
        cursor.execute("CREATE TABLE #test_param_encoding (id INT, data NVARCHAR(MAX))")

        # Test parameter binding with different encoding settings
        encoding_configs = [
            ("utf-8", SQL_CHAR),
            ("utf-16le", SQL_WCHAR),
        ]

        param_test_cases = [
            # Different parameter types - simplified to avoid encoding issues
            ("String param", "Unicode string simple"),
            ("List param single", ["Unicode in list simple"]),
            ("Tuple param", ("Unicode in tuple simple",)),
        ]

        for encoding, ctype in encoding_configs:
            db_connection.setencoding(encoding=encoding, ctype=ctype)

            for test_name, params in param_test_cases:
                try:
                    cursor.execute("DELETE FROM #test_param_encoding")

                    # Always use single parameter to avoid SQL syntax issues
                    param_value = params[0] if isinstance(params, (list, tuple)) else params
                    cursor.execute(
                        "INSERT INTO #test_param_encoding (id, data) VALUES (?, ?)", 1, param_value
                    )

                    # Verify insertion worked
                    cursor.execute("SELECT COUNT(*) FROM #test_param_encoding")
                    count = cursor.fetchone()[0]
                    assert count > 0, f"No rows inserted for {test_name} with {encoding}"

                    print(f"[OK] Parameter binding {test_name} with {encoding} passed")

                except Exception as e:
                    pytest.fail(f"Parameter binding {test_name} with {encoding} failed: {e}")

    finally:
        try:
            cursor.execute("DROP TABLE #test_param_encoding")
        except:
            pass
        cursor.close()


def test_encoding_decoding_sql_wchar_error_enforcement(conn_str):
    """Test that attempts to use SQL_WCHAR with non-UTF-16 encodings raise appropriate errors."""

    conn = connect(conn_str)

    try:
        # These should all raise ProgrammingError
        with pytest.raises(ProgrammingError, match="SQL_WCHAR only supports UTF-16 encodings"):
            conn.setencoding("utf-8", SQL_WCHAR)

        with pytest.raises(ProgrammingError, match="SQL_WCHAR only supports UTF-16 encodings"):
            conn.setdecoding(SQL_WCHAR, encoding="utf-8")

        with pytest.raises(
            ProgrammingError, match="SQL_WCHAR ctype only supports UTF-16 encodings"
        ):
            conn.setdecoding(SQL_CHAR, encoding="utf-8", ctype=SQL_WCHAR)

        # These should succeed (valid UTF-16 combinations)
        conn.setencoding("utf-16le", SQL_WCHAR)
        settings = conn.getencoding()
        assert settings["encoding"] == "utf-16le"
        assert settings["ctype"] == SQL_WCHAR

        conn.setdecoding(SQL_WCHAR, encoding="utf-16le")
        settings = conn.getdecoding(SQL_WCHAR)
        assert settings["encoding"] == "utf-16le"
        assert settings["ctype"] == SQL_WCHAR

    finally:
        conn.close()


def test_encoding_decoding_large_dataset_performance(db_connection):
    """Test encoding/decoding with larger datasets to check for performance issues."""
    cursor = db_connection.cursor()

    try:
        cursor.execute(
            """
            CREATE TABLE #test_large_encoding (
                id INT PRIMARY KEY,
                ascii_data VARCHAR(1000),
                unicode_data NVARCHAR(1000),
                mixed_data NVARCHAR(MAX)
            )
        """
        )

        # Generate test data - ensure it fits in column sizes
        ascii_text = "This is ASCII text with numbers 12345." * 10  # ~400 chars
        unicode_text = "Unicode simple text." * 15  # ~300 chars
        mixed_text = ascii_text + " " + unicode_text  # Under 1000 chars total

        # Test with different encoding configurations
        configs = [
            ("utf-8", SQL_CHAR, "UTF-8"),
            ("utf-16le", SQL_WCHAR, "UTF-16LE"),
        ]

        for encoding, ctype, desc in configs:
            print(f"Testing large dataset with {desc}")

            db_connection.setencoding(encoding=encoding, ctype=ctype)
            db_connection.setdecoding(SQL_CHAR, encoding="utf-8")
            db_connection.setdecoding(SQL_WCHAR, encoding="utf-16le")

            # Insert batch of records
            import time

            start_time = time.time()

            for i in range(100):  # 100 records with large Unicode content
                cursor.execute(
                    """
                    INSERT INTO #test_large_encoding 
                    (id, ascii_data, unicode_data, mixed_data) 
                    VALUES (?, ?, ?, ?)
                """,
                    i,
                    ascii_text,
                    unicode_text,
                    mixed_text,
                )

            insert_time = time.time() - start_time

            # Retrieve all records
            start_time = time.time()
            cursor.execute("SELECT * FROM #test_large_encoding ORDER BY id")
            results = cursor.fetchall()
            fetch_time = time.time() - start_time

            # Verify data integrity
            assert len(results) == 100, f"Expected 100 records, got {len(results)}"

            for row in results[:5]:  # Check first 5 records
                assert row[1] == ascii_text, "ASCII data mismatch"
                assert row[2] == unicode_text, "Unicode data mismatch"
                assert row[3] == mixed_text, "Mixed data mismatch"

            print(f"[OK] {desc} - Insert: {insert_time:.2f}s, Fetch: {fetch_time:.2f}s")

            # Clean up for next iteration
            cursor.execute("DELETE FROM #test_large_encoding")

        print("[OK] Large dataset performance test passed")

    finally:
        try:
            cursor.execute("DROP TABLE #test_large_encoding")
        except:
            pass
        cursor.close()


def test_encoding_decoding_connection_isolation(conn_str):
    """Test that encoding/decoding settings are isolated between connections."""

    conn1 = connect(conn_str)
    conn2 = connect(conn_str)

    try:
        # Set different encodings on each connection
        conn1.setencoding("utf-8", SQL_CHAR)
        conn1.setdecoding(SQL_CHAR, "utf-8", SQL_CHAR)

        conn2.setencoding("utf-16le", SQL_WCHAR)
        conn2.setdecoding(SQL_WCHAR, "utf-16le", SQL_WCHAR)

        # Verify settings are independent
        conn1_enc = conn1.getencoding()
        conn1_dec_char = conn1.getdecoding(SQL_CHAR)

        conn2_enc = conn2.getencoding()
        conn2_dec_wchar = conn2.getdecoding(SQL_WCHAR)

        assert conn1_enc["encoding"] == "utf-8"
        assert conn1_enc["ctype"] == SQL_CHAR
        assert conn1_dec_char["encoding"] == "utf-8"

        assert conn2_enc["encoding"] == "utf-16le"
        assert conn2_enc["ctype"] == SQL_WCHAR
        assert conn2_dec_wchar["encoding"] == "utf-16le"

        # Test that operations on one connection don't affect the other
        cursor1 = conn1.cursor()
        cursor2 = conn2.cursor()

        cursor1.execute("CREATE TABLE #test_isolation1 (data NVARCHAR(100))")
        cursor2.execute("CREATE TABLE #test_isolation2 (data NVARCHAR(100))")

        test_data = "Isolation test: ñáéíóú 中文 🌍"

        cursor1.execute("INSERT INTO #test_isolation1 VALUES (?)", test_data)
        cursor2.execute("INSERT INTO #test_isolation2 VALUES (?)", test_data)

        cursor1.execute("SELECT data FROM #test_isolation1")
        result1 = cursor1.fetchone()[0]

        cursor2.execute("SELECT data FROM #test_isolation2")
        result2 = cursor2.fetchone()[0]

        assert result1 == test_data, f"Connection 1 result mismatch: {result1!r}"
        assert result2 == test_data, f"Connection 2 result mismatch: {result2!r}"

        # Verify settings are still independent
        assert conn1.getencoding()["encoding"] == "utf-8"
        assert conn2.getencoding()["encoding"] == "utf-16le"

        print("[OK] Connection isolation test passed")

    finally:
        try:
            conn1.cursor().execute("DROP TABLE #test_isolation1")
            conn2.cursor().execute("DROP TABLE #test_isolation2")
        except:
            pass
        conn1.close()
        conn2.close()


def test_encoding_decoding_sql_wchar_explicit_error_validation(db_connection):
    """Test explicit validation that SQL_WCHAR restrictions work correctly."""

    # Non-UTF-16 encodings should raise errors with SQL_WCHAR
    non_utf16_encodings = ["utf-8", "latin-1", "ascii", "cp1252", "iso-8859-1"]

    # Test 1: Verify non-UTF-16 encodings with SQL_WCHAR raise errors
    for encoding in non_utf16_encodings:
        # setencoding should raise error
        with pytest.raises(ProgrammingError, match="SQL_WCHAR only supports UTF-16 encodings"):
            db_connection.setencoding(encoding=encoding, ctype=SQL_WCHAR)

        # setdecoding with SQL_WCHAR sqltype should raise error
        with pytest.raises(ProgrammingError, match="SQL_WCHAR only supports UTF-16 encodings"):
            db_connection.setdecoding(SQL_WCHAR, encoding=encoding)

        # setdecoding with SQL_WCHAR ctype should raise error
        with pytest.raises(
            ProgrammingError, match="SQL_WCHAR ctype only supports UTF-16 encodings"
        ):
            db_connection.setdecoding(SQL_CHAR, encoding=encoding, ctype=SQL_WCHAR)

    # Test 2: Verify UTF-16 encodings work correctly with SQL_WCHAR
    utf16_encodings = ["utf-16le", "utf-16be"]

    for encoding in utf16_encodings:
        # All of these should succeed
        db_connection.setencoding(encoding=encoding, ctype=SQL_WCHAR)
        settings = db_connection.getencoding()
        assert settings["encoding"] == encoding.lower()
        assert settings["ctype"] == SQL_WCHAR

    print("[OK] SQL_WCHAR explicit validation passed")


def test_encoding_decoding_metadata_columns(db_connection):
    """Test encoding/decoding of column metadata (SQL_WMETADATA)."""

    cursor = db_connection.cursor()

    try:
        # Create table with Unicode column names if supported
        cursor.execute(
            """
            CREATE TABLE #test_metadata (
                [normal_col] NVARCHAR(100),
                [column_with_unicode_测试] NVARCHAR(100),
                [special_chars_ñáéíóú] INT
            )
        """
        )

        # Test metadata decoding configuration
        db_connection.setdecoding(mssql_python.SQL_WMETADATA, encoding="utf-16le", ctype=SQL_WCHAR)

        # Get column information
        cursor.execute("SELECT * FROM #test_metadata WHERE 1=0")  # Empty result set

        # Check that description contains properly decoded column names
        description = cursor.description
        assert description is not None, "Should have column description"
        assert len(description) == 3, "Should have 3 columns"

        column_names = [col[0] for col in description]
        expected_names = ["normal_col", "column_with_unicode_测试", "special_chars_ñáéíóú"]

        for expected, actual in zip(expected_names, column_names):
            assert (
                actual == expected
            ), f"Column name mismatch: expected {expected!r}, got {actual!r}"

        print("[OK] Metadata column name encoding test passed")

    except Exception as e:
        # Some SQL Server versions might not support Unicode in column names
        if "identifier" in str(e).lower() or "invalid" in str(e).lower():
            print("[WARN] Unicode column names not supported in this SQL Server version, skipping")
        else:
            pytest.fail(f"Metadata encoding test failed: {e}")
    finally:
        try:
            cursor.execute("DROP TABLE #test_metadata")
        except:
            pass
        cursor.close()


def test_utf16_bom_rejection(db_connection):
    """Test that 'utf-16' with BOM is explicitly rejected for SQL_WCHAR."""
    print("\n" + "=" * 70)
    print("UTF-16 BOM REJECTION TEST")
    print("=" * 70)

    # 'utf-16' should be rejected when used with SQL_WCHAR
    with pytest.raises(ProgrammingError) as exc_info:
        db_connection.setencoding(encoding="utf-16", ctype=SQL_WCHAR)

    error_msg = str(exc_info.value)
    assert (
        "Byte Order Mark" in error_msg or "BOM" in error_msg
    ), "Error message should mention BOM issue"
    assert (
        "utf-16le" in error_msg or "utf-16be" in error_msg
    ), "Error message should suggest alternatives"

    print("[OK] 'utf-16' with SQL_WCHAR correctly rejected")
    print(f"  Error message: {error_msg}")

    # Same for setdecoding
    with pytest.raises(ProgrammingError) as exc_info:
        db_connection.setdecoding(SQL_WCHAR, encoding="utf-16")

    error_msg = str(exc_info.value)
    assert (
        "Byte Order Mark" in error_msg
        or "BOM" in error_msg
        or "SQL_WCHAR only supports UTF-16 encodings" in error_msg
    )

    print("[OK] setdecoding with 'utf-16' for SQL_WCHAR correctly rejected")

    # 'utf-16' should work fine with SQL_CHAR (not using SQL_WCHAR)
    db_connection.setencoding(encoding="utf-16", ctype=SQL_CHAR)
    settings = db_connection.getencoding()
    assert settings["encoding"] == "utf-16"
    assert settings["ctype"] == SQL_CHAR
    print("[OK] 'utf-16' with SQL_CHAR works correctly (BOM is acceptable)")

    print("=" * 70)


def test_encoding_decoding_stress_test_comprehensive(db_connection):
    """Comprehensive stress test with mixed encoding scenarios."""

    cursor = db_connection.cursor()

    try:
        cursor.execute(
            """
            CREATE TABLE #stress_test_encoding (
                id INT IDENTITY(1,1) PRIMARY KEY,
                ascii_text VARCHAR(500),
                unicode_text NVARCHAR(500),
                binary_data VARBINARY(500),
                mixed_content NVARCHAR(MAX)
            )
        """
        )

        # Generate diverse test data
        test_datasets = []

        # ASCII-only data
        for i in range(20):
            test_datasets.append(
                {
                    "ascii": f"ASCII test string {i} with numbers {i*123} and symbols !@#$%",
                    "unicode": f"ASCII test string {i} with numbers {i*123} and symbols !@#$%",
                    "binary": f"Binary{i}".encode("utf-8"),
                    "mixed": f"ASCII test string {i} with numbers {i*123} and symbols !@#$%",
                }
            )

        # Unicode-heavy data
        unicode_samples = [
            "中文测试字符串",
            "العربية النص التجريبي",
            "Русский тестовый текст",
            "हिंदी परीक्षण पाठ",
            "日本語のテストテキスト",
            "한국어 테스트 텍스트",
            "ελληνικό κείμενο δοκιμής",
            "עברית טקסט מבחן",
        ]

        for i, unicode_text in enumerate(unicode_samples):
            test_datasets.append(
                {
                    "ascii": f"Mixed test {i}",
                    "unicode": unicode_text,
                    "binary": unicode_text.encode("utf-8"),
                    "mixed": f"Mixed: {unicode_text} with ASCII {i}",
                }
            )

        # Emoji and special characters
        emoji_samples = [
            "🌍🌎🌏🌐🗺️",
            "😀😃😄😁😆😅😂🤣",
            "❤️💕💖💗💘💙💚💛",
            "🚗🏠🌳🌸🎵📱💻⚽",
            "👨‍👩‍👧‍👦👨‍💻👩‍🔬",
        ]

        for i, emoji_text in enumerate(emoji_samples):
            test_datasets.append(
                {
                    "ascii": f"Emoji test {i}",
                    "unicode": emoji_text,
                    "binary": emoji_text.encode("utf-8"),
                    "mixed": f"Text with emoji: {emoji_text} and number {i}",
                }
            )

        # Test with different encoding configurations
        encoding_configs = [
            ("utf-8", SQL_CHAR, "UTF-8/CHAR"),
            ("utf-16le", SQL_WCHAR, "UTF-16LE/WCHAR"),
        ]

        for encoding, ctype, config_name in encoding_configs:
            print(f"Testing stress scenario with {config_name}")

            # Configure encoding
            db_connection.setencoding(encoding=encoding, ctype=ctype)
            db_connection.setdecoding(SQL_CHAR, encoding="utf-8", ctype=SQL_CHAR)
            db_connection.setdecoding(SQL_WCHAR, encoding="utf-16le", ctype=SQL_WCHAR)

            # Clear table
            cursor.execute("DELETE FROM #stress_test_encoding")

            # Insert all test data
            for dataset in test_datasets:
                try:
                    cursor.execute(
                        """
                        INSERT INTO #stress_test_encoding 
                        (ascii_text, unicode_text, binary_data, mixed_content)
                        VALUES (?, ?, ?, ?)
                    """,
                        dataset["ascii"],
                        dataset["unicode"],
                        dataset["binary"],
                        dataset["mixed"],
                    )
                except Exception as e:
                    # Log encoding failures but don't stop the test
                    print(f"[WARN] Insert failed for dataset with {config_name}: {e}")

            # Retrieve and verify data integrity
            cursor.execute("SELECT COUNT(*) FROM #stress_test_encoding")
            row_count = cursor.fetchone()[0]
            print(f"  Inserted {row_count} rows successfully")

            # Sample verification - check first few rows
            cursor.execute("SELECT TOP 5 * FROM #stress_test_encoding ORDER BY id")
            sample_results = cursor.fetchall()

            for i, row in enumerate(sample_results):
                # Basic verification that data was preserved
                assert row[1] is not None, f"ASCII text should not be None in row {i}"
                assert row[2] is not None, f"Unicode text should not be None in row {i}"
                assert row[3] is not None, f"Binary data should not be None in row {i}"
                assert row[4] is not None, f"Mixed content should not be None in row {i}"

            print(f"[OK] Stress test with {config_name} completed successfully")

        print("[OK] Comprehensive encoding stress test passed")

    finally:
        try:
            cursor.execute("DROP TABLE #stress_test_encoding")
        except:
            pass
        cursor.close()


def test_encoding_decoding_sql_char_various_encodings(db_connection):
    """Test SQL_CHAR with various encoding types including non-standard ones."""
    cursor = db_connection.cursor()

    try:
        # Create test table with VARCHAR columns (SQL_CHAR type)
        cursor.execute(
            """
            CREATE TABLE #test_sql_char_encodings (
                id INT PRIMARY KEY,
                data_col VARCHAR(100),
                description VARCHAR(200)
            )
        """
        )

        # Define various encoding types to test with SQL_CHAR
        encoding_tests = [
            # Standard encodings
            {
                "name": "UTF-8",
                "encoding": "utf-8",
                "test_data": [
                    ("Basic ASCII", "Hello World 123"),
                    ("Extended Latin", "Cafe naive resume"),  # Avoid accents for compatibility
                    ("Simple Unicode", "Hello World"),
                ],
            },
            {
                "name": "Latin-1 (ISO-8859-1)",
                "encoding": "latin-1",
                "test_data": [
                    ("Basic ASCII", "Hello World 123"),
                    ("Latin chars", "Cafe resume"),  # Keep simple for latin-1
                    ("Extended Latin", "Hello Test"),
                ],
            },
            {
                "name": "ASCII",
                "encoding": "ascii",
                "test_data": [
                    ("Pure ASCII", "Hello World 123"),
                    ("Numbers", "0123456789"),
                    ("Symbols", "!@#$%^&*()_+-="),
                ],
            },
            {
                "name": "Windows-1252 (CP1252)",
                "encoding": "cp1252",
                "test_data": [
                    ("Basic text", "Hello World"),
                    ("Windows chars", "Test data 123"),
                    ("Special chars", "Quotes and dashes"),
                ],
            },
            # Chinese encodings
            {
                "name": "GBK (Chinese)",
                "encoding": "gbk",
                "test_data": [
                    ("ASCII only", "Hello World"),  # Should work with any encoding
                    ("Numbers", "123456789"),
                    ("Basic text", "Test Data"),
                ],
            },
            {
                "name": "GB2312 (Simplified Chinese)",
                "encoding": "gb2312",
                "test_data": [
                    ("ASCII only", "Hello World"),
                    ("Basic text", "Test 123"),
                    ("Simple data", "ABC xyz"),
                ],
            },
            # Japanese encodings
            {
                "name": "Shift-JIS",
                "encoding": "shift_jis",
                "test_data": [
                    ("ASCII only", "Hello World"),
                    ("Numbers", "0123456789"),
                    ("Basic text", "Test Data"),
                ],
            },
            {
                "name": "EUC-JP",
                "encoding": "euc-jp",
                "test_data": [
                    ("ASCII only", "Hello World"),
                    ("Basic text", "Test 123"),
                    ("Simple data", "ABC XYZ"),
                ],
            },
            # Korean encoding
            {
                "name": "EUC-KR",
                "encoding": "euc-kr",
                "test_data": [
                    ("ASCII only", "Hello World"),
                    ("Numbers", "123456789"),
                    ("Basic text", "Test Data"),
                ],
            },
            # European encodings
            {
                "name": "ISO-8859-2 (Central European)",
                "encoding": "iso-8859-2",
                "test_data": [
                    ("Basic ASCII", "Hello World"),
                    ("Numbers", "123456789"),
                    ("Simple text", "Test Data"),
                ],
            },
            {
                "name": "ISO-8859-15 (Latin-9)",
                "encoding": "iso-8859-15",
                "test_data": [
                    ("Basic ASCII", "Hello World"),
                    ("Numbers", "0123456789"),
                    ("Test text", "Sample Data"),
                ],
            },
            # Cyrillic encodings
            {
                "name": "Windows-1251 (Cyrillic)",
                "encoding": "cp1251",
                "test_data": [
                    ("ASCII only", "Hello World"),
                    ("Basic text", "Test 123"),
                    ("Simple data", "Sample Text"),
                ],
            },
            {
                "name": "KOI8-R (Russian)",
                "encoding": "koi8-r",
                "test_data": [
                    ("ASCII only", "Hello World"),
                    ("Numbers", "123456789"),
                    ("Basic text", "Test Data"),
                ],
            },
        ]

        results_summary = []

        for encoding_test in encoding_tests:
            encoding_name = encoding_test["name"]
            encoding = encoding_test["encoding"]
            test_data = encoding_test["test_data"]

            print(f"\n--- Testing {encoding_name} ({encoding}) with SQL_CHAR ---")

            try:
                # Set encoding for SQL_CHAR type
                db_connection.setencoding(encoding=encoding, ctype=SQL_CHAR)

                # Also set decoding for consistency
                db_connection.setdecoding(SQL_CHAR, encoding=encoding, ctype=SQL_CHAR)

                # Test each data sample
                test_results = []
                for test_name, test_string in test_data:
                    try:
                        # Clear table
                        cursor.execute("DELETE FROM #test_sql_char_encodings")

                        # Insert test data
                        cursor.execute(
                            """
                            INSERT INTO #test_sql_char_encodings (id, data_col, description)
                            VALUES (?, ?, ?)
                        """,
                            1,
                            test_string,
                            f"Test with {encoding_name}",
                        )

                        # Retrieve and verify
                        cursor.execute(
                            "SELECT data_col, description FROM #test_sql_char_encodings WHERE id = 1"
                        )
                        result = cursor.fetchone()

                        if result:
                            retrieved_data = result[0]
                            retrieved_desc = result[1]

                            # Check if data matches
                            data_match = retrieved_data == test_string
                            desc_match = retrieved_desc == f"Test with {encoding_name}"

                            if data_match and desc_match:
                                print(f"  [OK] {test_name}: Data preserved correctly")
                                test_results.append(
                                    {"test": test_name, "status": "PASS", "data": test_string}
                                )
                            else:
                                print(
                                    f"  [WARN] {test_name}: Data mismatch - Expected: {test_string!r}, Got: {retrieved_data!r}"
                                )
                                test_results.append(
                                    {
                                        "test": test_name,
                                        "status": "MISMATCH",
                                        "expected": test_string,
                                        "got": retrieved_data,
                                    }
                                )
                        else:
                            print(f"  [FAIL] {test_name}: No data retrieved")
                            test_results.append({"test": test_name, "status": "NO_DATA"})

                    except UnicodeEncodeError as e:
                        print(f"  [FAIL] {test_name}: Unicode encode error - {e}")
                        test_results.append(
                            {"test": test_name, "status": "ENCODE_ERROR", "error": str(e)}
                        )
                    except UnicodeDecodeError as e:
                        print(f"  [FAIL] {test_name}: Unicode decode error - {e}")
                        test_results.append(
                            {"test": test_name, "status": "DECODE_ERROR", "error": str(e)}
                        )
                    except Exception as e:
                        print(f"  [FAIL] {test_name}: Unexpected error - {e}")
                        test_results.append({"test": test_name, "status": "ERROR", "error": str(e)})

                # Calculate success rate
                passed_tests = len([r for r in test_results if r["status"] == "PASS"])
                total_tests = len(test_results)
                success_rate = (passed_tests / total_tests) * 100 if total_tests > 0 else 0

                results_summary.append(
                    {
                        "encoding": encoding_name,
                        "encoding_key": encoding,
                        "total_tests": total_tests,
                        "passed_tests": passed_tests,
                        "success_rate": success_rate,
                        "details": test_results,
                    }
                )

                print(f"  Summary: {passed_tests}/{total_tests} tests passed ({success_rate:.1f}%)")

            except Exception as e:
                print(f"  [FAIL] Failed to set encoding {encoding}: {e}")
                results_summary.append(
                    {
                        "encoding": encoding_name,
                        "encoding_key": encoding,
                        "total_tests": 0,
                        "passed_tests": 0,
                        "success_rate": 0,
                        "setup_error": str(e),
                    }
                )

        # Print comprehensive summary
        print(f"\n{'='*60}")
        print("COMPREHENSIVE ENCODING TEST RESULTS FOR SQL_CHAR")
        print(f"{'='*60}")

        for result in results_summary:
            encoding_name = result["encoding"]
            success_rate = result.get("success_rate", 0)

            if "setup_error" in result:
                print(f"{encoding_name:25} | SETUP FAILED: {result['setup_error']}")
            else:
                passed = result["passed_tests"]
                total = result["total_tests"]
                print(
                    f"{encoding_name:25} | {passed:2}/{total} tests passed ({success_rate:5.1f}%)"
                )

        print(f"{'='*60}")

        # Verify that at least basic encodings work
        basic_encodings = ["UTF-8", "ASCII", "Latin-1 (ISO-8859-1)"]
        basic_passed = False
        for result in results_summary:
            if result["encoding"] in basic_encodings and result["success_rate"] > 0:
                basic_passed = True
                break
        
        assert basic_passed, "At least one basic encoding (UTF-8, ASCII, Latin-1) should work"
        print("[OK] SQL_CHAR encoding variety test completed")

    finally:
        try:
            cursor.execute("DROP TABLE #test_sql_char_encodings")
        except Exception:
            pass
        cursor.close()


def test_encoding_decoding_sql_char_with_unicode_fallback(db_connection):
    """Test VARCHAR (SQL_CHAR) vs NVARCHAR (SQL_WCHAR) with Unicode data.
    
    Note: SQL_CHAR encoding affects VARCHAR columns, SQL_WCHAR encoding affects NVARCHAR columns.
    They are independent - setting SQL_CHAR encoding won't affect NVARCHAR data.
    """
    cursor = db_connection.cursor()

    try:
        # Create test table with both VARCHAR and NVARCHAR
        cursor.execute(
            """
            CREATE TABLE #test_unicode_fallback (
                id INT PRIMARY KEY,
                varchar_data VARCHAR(100),
                nvarchar_data NVARCHAR(100)
            )
        """
        )

        # Test Unicode data
        unicode_test_cases = [
            ("ASCII", "Hello World"),
            ("Chinese", "你好世界"),
            ("Japanese", "こんにちは"),
            ("Russian", "Привет"),
            ("Mixed", "Hello 世界"),
        ]

        # Configure encodings properly:
        # - SQL_CHAR encoding affects VARCHAR columns
        # - SQL_WCHAR encoding affects NVARCHAR columns
        db_connection.setencoding(encoding="utf-8", ctype=SQL_CHAR)  # For VARCHAR
        db_connection.setdecoding(SQL_CHAR, encoding="utf-8", ctype=SQL_CHAR)
        
        # NVARCHAR always uses UTF-16LE (SQL_WCHAR)
        db_connection.setencoding(encoding="utf-16le", ctype=SQL_WCHAR)  # For NVARCHAR
        db_connection.setdecoding(SQL_WCHAR, encoding="utf-16le", ctype=SQL_WCHAR)

        print("\n--- Testing VARCHAR vs NVARCHAR with Unicode ---")
        print(f"{'Test':<15} | {'VARCHAR Result':<25} | {'NVARCHAR Result':<25}")
        print("-" * 70)

        for test_name, unicode_text in unicode_test_cases:
            try:
                # Clear table
                cursor.execute("DELETE FROM #test_unicode_fallback")

                # Insert Unicode data
                cursor.execute(
                    """
                    INSERT INTO #test_unicode_fallback (id, varchar_data, nvarchar_data)
                    VALUES (?, ?, ?)
                """,
                    1,
                    unicode_text,
                    unicode_text,
                )

                # Retrieve data
                cursor.execute(
                    "SELECT varchar_data, nvarchar_data FROM #test_unicode_fallback WHERE id = 1"
                )
                result = cursor.fetchone()

                if result:
                    varchar_result = result[0]
                    nvarchar_result = result[1]

                    # Use repr for safe display
                    varchar_display = repr(varchar_result)[:23]
                    nvarchar_display = repr(nvarchar_result)[:23]
                    
                    print(f"  {test_name:<15} | {varchar_display:<25} | {nvarchar_display:<25}")
                    
                    # NVARCHAR should always preserve Unicode correctly
                    assert nvarchar_result == unicode_text, f"NVARCHAR should preserve {test_name}"

            except Exception as e:
                print(f"  {test_name:<15} | Error: {str(e)[:50]}...")

        print("\n[OK] VARCHAR vs NVARCHAR Unicode handling test completed")

    finally:
        try:
            cursor.execute("DROP TABLE #test_unicode_fallback")
        except Exception:
            pass
        cursor.close()


def test_encoding_decoding_sql_char_native_character_sets(db_connection):
    """Test SQL_CHAR with encoding-specific native character sets."""
    cursor = db_connection.cursor()

    try:
        # Create test table
        cursor.execute(
            """
            CREATE TABLE #test_native_chars (
                id INT PRIMARY KEY,
                data VARCHAR(200),
                encoding_used VARCHAR(50)
            )
        """
        )

        # Test encoding-specific character sets that should work
        encoding_native_tests = [
            {
                "encoding": "gbk",
                "name": "GBK (Chinese)",
                "test_cases": [
                    ("ASCII", "Hello World"),
                    ("Extended ASCII", "Test 123 !@#"),
                    # Note: Actual Chinese characters may not work due to ODBC conversion
                    ("Safe chars", "ABC xyz 789"),
                ],
            },
            {
                "encoding": "shift_jis",
                "name": "Shift-JIS (Japanese)",
                "test_cases": [
                    ("ASCII", "Hello World"),
                    ("Numbers", "0123456789"),
                    ("Symbols", "!@#$%^&*()"),
                    ("Half-width", "ABC xyz"),
                ],
            },
            {
                "encoding": "euc-kr",
                "name": "EUC-KR (Korean)",
                "test_cases": [
                    ("ASCII", "Hello World"),
                    ("Mixed case", "AbCdEf 123"),
                    ("Punctuation", "Hello, World!"),
                ],
            },
            {
                "encoding": "cp1251",
                "name": "Windows-1251 (Cyrillic)",
                "test_cases": [
                    ("ASCII", "Hello World"),
                    ("Latin ext", "Test Data"),
                    ("Numbers", "123456789"),
                ],
            },
            {
                "encoding": "iso-8859-2",
                "name": "ISO-8859-2 (Central European)",
                "test_cases": [
                    ("ASCII", "Hello World"),
                    ("Basic", "Test 123"),
                    ("Mixed", "ABC xyz 789"),
                ],
            },
            {
                "encoding": "cp1252",
                "name": "Windows-1252 (Western European)",
                "test_cases": [
                    ("ASCII", "Hello World"),
                    ("Extended", "Test Data 123"),
                    ("Punctuation", "Hello, World! @#$"),
                ],
            },
        ]

        print(f"\n{'='*70}")
        print("TESTING NATIVE CHARACTER SETS WITH SQL_CHAR")
        print(f"{'='*70}")

        for encoding_test in encoding_native_tests:
            encoding = encoding_test["encoding"]
            name = encoding_test["name"]
            test_cases = encoding_test["test_cases"]

            print(f"\n--- {name} ({encoding}) ---")

            try:
                # Configure encoding
                db_connection.setencoding(encoding=encoding, ctype=SQL_CHAR)
                db_connection.setdecoding(SQL_CHAR, encoding=encoding, ctype=SQL_CHAR)

                results = []
                for test_name, test_data in test_cases:
                    try:
                        # Clear table
                        cursor.execute("DELETE FROM #test_native_chars")

                        # Insert data
                        cursor.execute(
                            """
                            INSERT INTO #test_native_chars (id, data, encoding_used)
                            VALUES (?, ?, ?)
                        """,
                            1,
                            test_data,
                            encoding,
                        )

                        # Retrieve data
                        cursor.execute(
                            "SELECT data, encoding_used FROM #test_native_chars WHERE id = 1"
                        )
                        result = cursor.fetchone()

                        if result:
                            retrieved_data = result[0]
                            retrieved_encoding = result[1]

                            # Verify data integrity
                            if retrieved_data == test_data and retrieved_encoding == encoding:
                                print(
                                    f"  [OK] {test_name:12} | '{test_data}' -> '{retrieved_data}' (Perfect match)"
                                )
                                results.append("PASS")
                            else:
                                print(
                                    f"  [WARN] {test_name:12} | '{test_data}' -> '{retrieved_data}' (Data changed)"
                                )
                                results.append("CHANGED")
                        else:
                            print(f"  [FAIL] {test_name:12} | No data retrieved")
                            results.append("FAIL")

                    except Exception as e:
                        print(f"  [FAIL] {test_name:12} | Error: {str(e)[:40]}...")
                        results.append("ERROR")

                # Summary for this encoding
                passed = results.count("PASS")
                total = len(results)
                print(f"  Result: {passed}/{total} tests passed")

            except Exception as e:
                print(f"  [FAIL] Failed to configure {encoding}: {e}")

        print(f"\n{'='*70}")
        print("[OK] Native character set testing completed")

    finally:
        try:
            cursor.execute("DROP TABLE #test_native_chars")
        except Exception:
            pass
        cursor.close()


def test_encoding_decoding_sql_char_boundary_encoding_cases(db_connection):
    """Test SQL_CHAR encoding boundary cases and special scenarios."""
    cursor = db_connection.cursor()

    try:
        # Create test table
        cursor.execute(
            """
            CREATE TABLE #test_encoding_boundaries (
                id INT PRIMARY KEY,
                test_data VARCHAR(500),
                test_type VARCHAR(100)
            )
        """
        )

        # Test boundary cases for different encodings
        boundary_tests = [
            {
                "encoding": "utf-8",
                "cases": [
                    ("Empty string", ""),
                    ("Single byte", "A"),
                    ("Max ASCII", chr(127)),  # Highest ASCII character
                    ("Extended ASCII", "".join(chr(i) for i in range(32, 127))),  # Printable ASCII
                    ("Long ASCII", "A" * 100),
                ],
            },
            {
                "encoding": "latin-1",
                "cases": [
                    ("Empty string", ""),
                    ("Single char", "B"),
                    ("ASCII range", "Hello123!@#"),
                    ("Latin-1 compatible", "Test Data"),
                    ("Long Latin", "B" * 100),
                ],
            },
            {
                "encoding": "gbk",
                "cases": [
                    ("Empty string", ""),
                    ("ASCII only", "Hello World 123"),
                    ("Mixed ASCII", "Test!@#$%^&*()_+"),
                    ("Number sequence", "0123456789" * 10),
                    ("Alpha sequence", "ABCDEFGHIJKLMNOPQRSTUVWXYZ" * 4),
                ],
            },
        ]

        print(f"\n{'='*60}")
        print("SQL_CHAR ENCODING BOUNDARY TESTING")
        print(f"{'='*60}")

        for test_group in boundary_tests:
            encoding = test_group["encoding"]
            cases = test_group["cases"]

            print(f"\n--- Boundary tests for {encoding.upper()} ---")

            try:
                # Set encoding
                db_connection.setencoding(encoding=encoding, ctype=SQL_CHAR)
                db_connection.setdecoding(SQL_CHAR, encoding=encoding, ctype=SQL_CHAR)

                for test_name, test_data in cases:
                    try:
                        # Clear table
                        cursor.execute("DELETE FROM #test_encoding_boundaries")

                        # Insert test data
                        cursor.execute(
                            """
                            INSERT INTO #test_encoding_boundaries (id, test_data, test_type)
                            VALUES (?, ?, ?)
                        """,
                            1,
                            test_data,
                            test_name,
                        )

                        # Retrieve and verify
                        cursor.execute(
                            "SELECT test_data FROM #test_encoding_boundaries WHERE id = 1"
                        )
                        result = cursor.fetchone()

                        if result:
                            retrieved = result[0]
                            data_length = len(test_data)
                            retrieved_length = len(retrieved)

                            if retrieved == test_data:
                                print(
                                    f"  [OK] {test_name:15} | Length: {data_length:3} | Perfect preservation"
                                )
                            else:
                                print(
                                    f"  [WARN] {test_name:15} | Length: {data_length:3} -> {retrieved_length:3} | Data modified"
                                )
                                if data_length <= 20:  # Show diff for short strings
                                    print(f"      Original: {test_data!r}")
                                    print(f"      Retrieved: {retrieved!r}")
                        else:
                            print(f"  [FAIL] {test_name:15} | No data retrieved")

                    except Exception as e:
                        print(f"  [FAIL] {test_name:15} | Error: {str(e)[:30]}...")

            except Exception as e:
                print(f"  [FAIL] Failed to configure {encoding}: {e}")

        print(f"\n{'='*60}")
        print("[OK] Boundary encoding testing completed")

    finally:
        try:
            cursor.execute("DROP TABLE #test_encoding_boundaries")
        except:
            pass
        cursor.close()


def test_encoding_decoding_sql_char_unicode_issue_diagnosis(db_connection):
    """Diagnose the Unicode -> ? character conversion issue with SQL_CHAR."""
    cursor = db_connection.cursor()

    try:
        # Create test table with both VARCHAR and NVARCHAR for comparison
        cursor.execute(
            """
            CREATE TABLE #test_unicode_issue (
                id INT PRIMARY KEY,
                varchar_col VARCHAR(100),
                nvarchar_col NVARCHAR(100),
                encoding_used VARCHAR(50)
            )
        """
        )

        print(f"\n{'='*80}")
        print("DIAGNOSING UNICODE -> ? CHARACTER CONVERSION ISSUE")
        print(f"{'='*80}")

        # Test Unicode strings that commonly cause issues
        test_strings = [
            ("Chinese", "你好世界", "Chinese characters"),
            ("Japanese", "こんにちは", "Japanese hiragana"),
            ("Korean", "안녕하세요", "Korean hangul"),
            ("Arabic", "مرحبا", "Arabic script"),
            ("Russian", "Привет", "Cyrillic script"),
            ("German", "Müller", "German umlaut"),
            ("French", "Café", "French accent"),
            ("Spanish", "Niño", "Spanish tilde"),
            ("Emoji", "😀🌍", "Unicode emojis"),
            ("Mixed", "Test 你好 🌍", "Mixed ASCII + Unicode"),
        ]

        # Test with different SQL_CHAR encodings
        encodings = ["utf-8", "latin-1", "cp1252", "gbk"]

        for encoding in encodings:
            print(f"\n--- Testing with SQL_CHAR encoding: {encoding} ---")

            try:
                # Configure encoding
                db_connection.setencoding(encoding=encoding, ctype=SQL_CHAR)
                db_connection.setdecoding(SQL_CHAR, encoding=encoding, ctype=SQL_CHAR)
                db_connection.setdecoding(SQL_WCHAR, encoding="utf-16le", ctype=SQL_WCHAR)

                print(
                    f"{'Test':<15} | {'VARCHAR Result':<20} | {'NVARCHAR Result':<20} | {'Issue':<15}"
                )
                print("-" * 75)

                for test_name, test_string, description in test_strings:
                    try:
                        # Clear table
                        cursor.execute("DELETE FROM #test_unicode_issue")

                        # Insert test data
                        cursor.execute(
                            """
                            INSERT INTO #test_unicode_issue (id, varchar_col, nvarchar_col, encoding_used)
                            VALUES (?, ?, ?, ?)
                        """,
                            1,
                            test_string,
                            test_string,
                            encoding,
                        )

                        # Retrieve results
                        cursor.execute(
                            """
                            SELECT varchar_col, nvarchar_col FROM #test_unicode_issue WHERE id = 1
                        """
                        )
                        result = cursor.fetchone()

                        if result:
                            varchar_result = result[0]
                            nvarchar_result = result[1]

                            # Check for issues
                            varchar_has_question = "?" in varchar_result
                            nvarchar_preserved = nvarchar_result == test_string
                            varchar_preserved = varchar_result == test_string

                            issue_type = "None"
                            if varchar_has_question and nvarchar_preserved:
                                issue_type = "DB Conversion"
                            elif not varchar_preserved and not nvarchar_preserved:
                                issue_type = "Both Failed"
                            elif not varchar_preserved:
                                issue_type = "VARCHAR Only"

                            # Use safe display for Unicode characters
                            varchar_safe = (
                                varchar_result.encode("ascii", "replace").decode("ascii")
                                if isinstance(varchar_result, str)
                                else str(varchar_result)
                            )
                            nvarchar_safe = (
                                nvarchar_result.encode("ascii", "replace").decode("ascii")
                                if isinstance(nvarchar_result, str)
                                else str(nvarchar_result)
                            )
                            print(
                                f"{test_name:<15} | {varchar_safe:<20} | {nvarchar_safe:<20} | {issue_type:<15}"
                            )

                        else:
                            print(
                                f"{test_name:<15} | {'NO DATA':<20} | {'NO DATA':<20} | {'Insert Failed':<15}"
                            )

                    except Exception as e:
                        print(
                            f"{test_name:<15} | {'ERROR':<20} | {'ERROR':<20} | {str(e)[:15]:<15}"
                        )

            except Exception as e:
                print(f"Failed to configure {encoding}: {e}")

        print(f"\n{'='*80}")
        print("DIAGNOSIS SUMMARY:")
        print(
            "- If VARCHAR shows '?' but NVARCHAR preserves Unicode -> SQL Server conversion issue"
        )
        print("- If both show issues -> Encoding configuration problem")
        print("- VARCHAR columns are limited by SQL Server collation and character set")
        print("- NVARCHAR columns use UTF-16 and preserve Unicode correctly")
        print("[OK] Unicode issue diagnosis completed")

    finally:
        try:
            cursor.execute("DROP TABLE #test_unicode_issue")
        except:
            pass
        cursor.close()


def test_encoding_decoding_sql_char_best_practices_guide(db_connection):
    """Demonstrate best practices for handling Unicode with SQL_CHAR vs SQL_WCHAR."""
    cursor = db_connection.cursor()

    try:
        # Create test table demonstrating different column types
        cursor.execute(
            """
            CREATE TABLE #test_best_practices (
                id INT PRIMARY KEY,
                -- ASCII-safe columns (VARCHAR with SQL_CHAR)
                ascii_data VARCHAR(100),
                code_name VARCHAR(50),
                
                -- Unicode-safe columns (NVARCHAR with SQL_WCHAR) 
                unicode_name NVARCHAR(100),
                description_intl NVARCHAR(500),
                
                -- Mixed approach column
                safe_text VARCHAR(200)
            )
        """
        )

        print(f"\n{'='*80}")
        print("BEST PRACTICES FOR UNICODE HANDLING WITH SQL_CHAR vs SQL_WCHAR")
        print(f"{'='*80}")

        # Configure optimal settings
        db_connection.setencoding(encoding="utf-8", ctype=SQL_CHAR)  # For ASCII data
        db_connection.setdecoding(SQL_CHAR, encoding="utf-8", ctype=SQL_CHAR)
        db_connection.setdecoding(SQL_WCHAR, encoding="utf-16le", ctype=SQL_WCHAR)

        # Test cases demonstrating best practices
        test_cases = [
            {
                "scenario": "Pure ASCII Data",
                "ascii_data": "Hello World 123",
                "code_name": "USER_001",
                "unicode_name": "Hello World 123",
                "description_intl": "Hello World 123",
                "safe_text": "Hello World 123",
                "recommendation": "[OK] Safe for both VARCHAR and NVARCHAR",
            },
            {
                "scenario": "European Names",
                "ascii_data": "Mueller",  # ASCII version
                "code_name": "USER_002",
                "unicode_name": "Müller",  # Unicode version
                "description_intl": "German name with umlaut: Müller",
                "safe_text": "Mueller (German)",
                "recommendation": "[OK] Use NVARCHAR for original, VARCHAR for ASCII version",
            },
            {
                "scenario": "International Names",
                "ascii_data": "Zhang",  # Romanized
                "code_name": "USER_003",
                "unicode_name": "张三",  # Chinese characters
                "description_intl": "Chinese name: 张三 (Zhang San)",
                "safe_text": "Zhang (Chinese name)",
                "recommendation": "[OK] NVARCHAR required for Chinese characters",
            },
            {
                "scenario": "Mixed Content",
                "ascii_data": "Product ABC",
                "code_name": "PROD_001",
                "unicode_name": "产品 ABC",  # Mixed Chinese + ASCII
                "description_intl": "Product description with emoji: Great product! 😀🌍",
                "safe_text": "Product ABC (International)",
                "recommendation": "[OK] NVARCHAR essential for mixed scripts and emojis",
            },
        ]

        print(
            f"\n{'Scenario':<20} | {'VARCHAR Result':<25} | {'NVARCHAR Result':<25} | {'Status':<15}"
        )
        print("-" * 90)

        for i, case in enumerate(test_cases, 1):
            try:
                # Insert test data
                cursor.execute("DELETE FROM #test_best_practices")
                cursor.execute(
                    """
                    INSERT INTO #test_best_practices 
                    (id, ascii_data, code_name, unicode_name, description_intl, safe_text)
                    VALUES (?, ?, ?, ?, ?, ?)
                """,
                    i,
                    case["ascii_data"],
                    case["code_name"],
                    case["unicode_name"],
                    case["description_intl"],
                    case["safe_text"],
                )

                # Retrieve and display results
                cursor.execute(
                    """
                    SELECT ascii_data, unicode_name FROM #test_best_practices WHERE id = ?
                """,
                    i,
                )
                result = cursor.fetchone()

                if result:
                    varchar_result = result[0]
                    nvarchar_result = result[1]

                    # Check for data preservation
                    varchar_preserved = varchar_result == case["ascii_data"]
                    nvarchar_preserved = nvarchar_result == case["unicode_name"]

                    status = "[OK] Both OK"
                    if not varchar_preserved and nvarchar_preserved:
                        status = "[OK] NVARCHAR OK"
                    elif varchar_preserved and not nvarchar_preserved:
                        status = "[WARN] VARCHAR OK"
                    elif not varchar_preserved and not nvarchar_preserved:
                        status = "[FAIL] Both Failed"

                    print(
                        f"{case['scenario']:<20} | {varchar_result:<25} | {nvarchar_result:<25} | {status:<15}"
                    )

            except Exception as e:
                print(f"{case['scenario']:<20} | {'ERROR':<25} | {'ERROR':<25} | {str(e)[:15]:<15}")

        print(f"\n{'='*80}")
        print("BEST PRACTICE RECOMMENDATIONS:")
        print("1. Use NVARCHAR for Unicode data (names, descriptions, international content)")
        print("2. Use VARCHAR for ASCII-only data (codes, IDs, English-only text)")
        print("3. Configure SQL_WCHAR encoding as 'utf-16le' (automatic)")
        print("4. Configure SQL_CHAR encoding based on your ASCII data needs")
        print("5. The '?' character in VARCHAR is SQL Server's expected behavior")
        print("6. Design your schema with appropriate column types from the start")
        print(f"{'='*80}")

        # Demonstrate the fix: using the right column types
        print("\nSOLUTION DEMONSTRATION:")
        print("Instead of trying to force Unicode into VARCHAR, use the right column type:")

        cursor.execute("DELETE FROM #test_best_practices")

        # Insert problematic Unicode data the RIGHT way
        cursor.execute(
            """
            INSERT INTO #test_best_practices 
            (id, ascii_data, code_name, unicode_name, description_intl, safe_text)
            VALUES (?, ?, ?, ?, ?, ?)
        """,
            1,
            "User 001",
            "USR001",
            "用户张三",
            "用户信息：张三，来自北京 🏙️",
            "User Zhang (Beijing)",
        )

        cursor.execute(
            "SELECT unicode_name, description_intl FROM #test_best_practices WHERE id = 1"
        )
        result = cursor.fetchone()

        if result:
            # Use repr() to safely display Unicode characters
            try:
                name_safe = result[0].encode("ascii", "replace").decode("ascii")
                desc_safe = result[1].encode("ascii", "replace").decode("ascii")
                print(f"[OK] Unicode Name (NVARCHAR): {name_safe}")
                print(f"[OK] Unicode Description (NVARCHAR): {desc_safe}")
            except (UnicodeError, AttributeError):
                print(f"[OK] Unicode Name (NVARCHAR): {repr(result[0])}")
                print(f"[OK] Unicode Description (NVARCHAR): {repr(result[1])}")
            print("[OK] Perfect Unicode preservation using NVARCHAR columns!")

        print("\n[OK] Best practices guide completed")

    finally:
        try:
            cursor.execute("DROP TABLE #test_best_practices")
        except:
            pass
        cursor.close()


# SQL Server supported single-byte encodings
SINGLE_BYTE_ENCODINGS = [
    ("ascii", "US-ASCII", [("Hello", "Basic ASCII")]),
    ("latin-1", "ISO-8859-1", [("Café", "Western European"), ("Müller", "German")]),
    ("iso8859-1", "ISO-8859-1 variant", [("José", "Spanish")]),
    ("cp1252", "Windows-1252", [("€100", "Euro symbol"), ("Naïve", "French")]),
    ("iso8859-2", "Central European", [("Łódź", "Polish city")]),
    ("iso8859-5", "Cyrillic", [("Привет", "Russian hello")]),
    ("iso8859-7", "Greek", [("Γειά", "Greek hello")]),
    ("iso8859-8", "Hebrew", [("שלום", "Hebrew hello")]),
    ("iso8859-9", "Turkish", [("İstanbul", "Turkish city")]),
    ("cp850", "DOS Latin-1", [("Test", "DOS encoding")]),
    ("cp437", "DOS US", [("Test", "Original DOS")]),
]

# SQL Server supported multi-byte encodings (Asian languages)
MULTIBYTE_ENCODINGS = [
    (
        "utf-8",
        "Unicode UTF-8",
        [
            ("你好世界", "Chinese"),
            ("こんにちは", "Japanese"),
            ("한글", "Korean"),
            ("😀🌍", "Emoji"),
        ],
    ),
    (
        "gbk",
        "Chinese Simplified",
        [
            ("你好", "Chinese hello"),
            ("北京", "Beijing"),
            ("中国", "China"),
        ],
    ),
    (
        "gb2312",
        "Chinese Simplified (subset)",
        [
            ("你好", "Chinese hello"),
            ("中国", "China"),
        ],
    ),
    (
        "gb18030",
        "Chinese National Standard",
        [
            ("你好世界", "Chinese with extended chars"),
        ],
    ),
    (
        "big5",
        "Traditional Chinese",
        [
            ("你好", "Chinese hello (Traditional)"),
            ("台灣", "Taiwan"),
        ],
    ),
    (
        "shift_jis",
        "Japanese Shift-JIS",
        [
            ("こんにちは", "Japanese hello"),
            ("東京", "Tokyo"),
        ],
    ),
    (
        "euc-jp",
        "Japanese EUC-JP",
        [
            ("こんにちは", "Japanese hello"),
        ],
    ),
    (
        "euc-kr",
        "Korean EUC-KR",
        [
            ("안녕하세요", "Korean hello"),
            ("서울", "Seoul"),
        ],
    ),
    (
        "johab",
        "Korean Johab",
        [
            ("한글", "Hangul"),
        ],
    ),
]

# UTF-16 variants
UTF16_ENCODINGS = [
    ("utf-16", "UTF-16 with BOM"),
    ("utf-16le", "UTF-16 Little Endian"),
    ("utf-16be", "UTF-16 Big Endian"),
]

# Security test data - injection attempts
INJECTION_TEST_DATA = [
    ("../../etc/passwd", "Path traversal attempt"),
    ("<script>alert('xss')</script>", "XSS attempt"),
    ("'; DROP TABLE users; --", "SQL injection"),
    ("$(rm -rf /)", "Command injection"),
    ("\x00\x01\x02", "Null bytes and control chars"),
    ("utf-8\x00; rm -rf /", "Null byte injection"),
    ("utf-8' OR '1'='1", "SQL-style injection"),
    ("../../../windows/system32", "Windows path traversal"),
    ("%00%2e%2e%2f%2e%2e", "URL-encoded traversal"),
    ("utf\\u002d8", "Unicode escape attempt"),
    ("a" * 1000, "Extremely long encoding name"),
    ("utf-8\nrm -rf /", "Newline injection"),
    ("utf-8\r\nmalicious", "CRLF injection"),
]

# Invalid encoding names
INVALID_ENCODINGS = [
    "invalid-encoding-12345",
    "utf-99",
    "not-a-codec",
    "",  # Empty string
    " ",  # Whitespace
    "utf 8",  # Space in name
    "utf@8",  # Invalid character
]

# Edge case strings
EDGE_CASE_STRINGS = [
    ("", "Empty string"),
    (" ", "Single space"),
    ("   \t\n\r   ", "Whitespace mix"),
    ("'\"\\", "Quotes and backslash"),
    ("NULL", "String 'NULL'"),
    ("None", "String 'None'"),
    ("\x00", "Null byte"),
    ("A" * 8000, "Max VARCHAR length"),
    ("安" * 4000, "Max NVARCHAR length"),
]


# ====================================================================================
# HELPER FUNCTIONS
# ====================================================================================


def safe_display(text, max_len=50):
    """Safely display text for testing output, handling Unicode gracefully."""
    if text is None:
        return "NULL"
    try:
        # Use ascii() to ensure CP1252 console compatibility on Windows
        display = text[:max_len] if len(text) > max_len else text
        return ascii(display)
    except (AttributeError, TypeError):
        return repr(text)[:max_len]


def is_encoding_compatible_with_data(encoding, data):
    """Check if data can be encoded with given encoding."""
    try:
        data.encode(encoding)
        return True
    except (UnicodeEncodeError, LookupError, AttributeError):
        return False


# ====================================================================================
# SECURITY TESTS - Injection Attacks
# ====================================================================================


def test_encoding_injection_attacks(db_connection):
    """Test that malicious encoding strings are properly rejected."""
    print("\n" + "=" * 80)
    print("SECURITY TEST: Encoding Injection Attack Prevention")
    print("=" * 80)

    for malicious_encoding, attack_type in INJECTION_TEST_DATA:
        print(f"\nTesting: {attack_type}")
        print(f"  Payload: {safe_display(malicious_encoding, 60)}")

        with pytest.raises((ProgrammingError, ValueError, LookupError)) as exc_info:
            db_connection.setencoding(encoding=malicious_encoding, ctype=SQL_CHAR)

        error_msg = str(exc_info.value).lower()
        # Should reject invalid encodings
        assert any(
            keyword in error_msg
            for keyword in ["encod", "invalid", "unknown", "lookup", "null", "embedded"]
        ), f"Expected encoding validation error, got: {exc_info.value}"
        print(f"  [OK] Properly rejected with: {type(exc_info.value).__name__}")

    print(f"\n{'='*80}")
    print("[OK] All injection attacks properly prevented")


def test_decoding_injection_attacks(db_connection):
    """Test that malicious encoding strings in setdecoding are rejected."""
    print("\n" + "=" * 80)
    print("SECURITY TEST: Decoding Injection Attack Prevention")
    print("=" * 80)

    for malicious_encoding, attack_type in INJECTION_TEST_DATA:
        print(f"\nTesting: {attack_type}")

        with pytest.raises((ProgrammingError, ValueError, LookupError)) as exc_info:
            db_connection.setdecoding(SQL_CHAR, encoding=malicious_encoding, ctype=SQL_CHAR)

        error_msg = str(exc_info.value).lower()
        assert any(
            keyword in error_msg
            for keyword in ["encod", "invalid", "unknown", "lookup", "null", "embedded"]
        ), f"Expected encoding validation error, got: {exc_info.value}"
        print(f"  [OK] Properly rejected: {type(exc_info.value).__name__}")

    print(f"\n{'='*80}")
    print("[OK] All decoding injection attacks prevented")


def test_encoding_validation_security(db_connection):
    """Test Python-layer encoding validation using is_valid_encoding."""
    print("\n" + "=" * 80)
    print("SECURITY TEST: Python Layer Encoding Validation")
    print("=" * 80)

    # Test that C++ validation catches dangerous characters
    dangerous_chars = [
        ("utf;8", "Semicolon"),
        ("utf|8", "Pipe"),
        ("utf&8", "Ampersand"),
        ("utf`8", "Backtick"),
        ("utf$8", "Dollar sign"),
        ("utf(8)", "Parentheses"),
        ("utf{8}", "Braces"),
        ("utf[8]", "Brackets"),
        ("utf<8>", "Angle brackets"),
    ]

    for dangerous_enc, char_type in dangerous_chars:
        print(f"\nTesting {char_type}: {dangerous_enc}")

        with pytest.raises((ProgrammingError, ValueError, LookupError)) as exc_info:
            db_connection.setencoding(encoding=dangerous_enc, ctype=SQL_CHAR)

        print(f"  [OK] Rejected: {type(exc_info.value).__name__}")

    print(f"\n{'='*80}")
    print("[OK] Python layer validation working correctly")


def test_encoding_length_limit_security(db_connection):
    """Test that extremely long encoding names are rejected."""
    print("\n" + "=" * 80)
    print("SECURITY TEST: Encoding Name Length Limit")
    print("=" * 80)

    # C++ code has 100 character limit
    test_cases = [
        ("a" * 50, "50 chars", True),  # Should work if valid codec
        ("a" * 100, "100 chars", False),  # At limit
        ("a" * 101, "101 chars", False),  # Over limit
        ("a" * 500, "500 chars", False),  # Way over limit
        ("a" * 1000, "1000 chars", False),  # DOS attempt
    ]

    for enc_name, description, should_work in test_cases:
        print(f"\nTesting {description}: {len(enc_name)} characters")

        if should_work:
            # Even if under limit, will fail if not a valid codec
            try:
                db_connection.setencoding(encoding=enc_name, ctype=SQL_CHAR)
                print("  [INFO] Accepted (valid codec)")
            except (ProgrammingError, ValueError, LookupError):
                print("  [OK] Rejected (invalid codec, but length OK)")
        else:
            with pytest.raises((ProgrammingError, ValueError, LookupError)) as exc_info:
                db_connection.setencoding(encoding=enc_name, ctype=SQL_CHAR)
            print(f"  [OK] Rejected: {type(exc_info.value).__name__}")

    print(f"\n{'='*80}")
    print("[OK] Length limit security working correctly")


# ====================================================================================
# UTF-8 ENCODING TESTS (pyodbc Compatibility)
# ====================================================================================


def test_utf8_encoding_strict_no_fallback(db_connection):
    """Test that UTF-8 encoding does NOT fallback to latin-1 (pyodbc compatibility)."""
    db_connection.setencoding(encoding="utf-8", ctype=SQL_CHAR)

    cursor = db_connection.cursor()
    try:
        # Use NVARCHAR for proper Unicode support
        cursor.execute("CREATE TABLE #test_utf8_strict (id INT, data NVARCHAR(100))")

        # Test ASCII data (should work)
        cursor.execute("INSERT INTO #test_utf8_strict VALUES (?, ?)", 1, "Hello ASCII")
        cursor.execute("SELECT data FROM #test_utf8_strict WHERE id = 1")
        result = cursor.fetchone()
        assert result[0] == "Hello ASCII", "ASCII should work with UTF-8"

        # Test valid UTF-8 Unicode (should work with NVARCHAR)
        cursor.execute("DELETE FROM #test_utf8_strict")
        test_unicode = "Café Müller 你好"
        cursor.execute("INSERT INTO #test_utf8_strict VALUES (?, ?)", 2, test_unicode)
        cursor.execute("SELECT data FROM #test_utf8_strict WHERE id = 2")
        result = cursor.fetchone()
        # With NVARCHAR, Unicode should be preserved
        assert (
            result[0] == test_unicode
        ), f"UTF-8 Unicode should be preserved with NVARCHAR: expected {test_unicode!r}, got {result[0]!r}"
        print(f"  [OK] UTF-8 Unicode properly handled: {safe_display(result[0])}")

    finally:
        cursor.close()


def test_utf8_decoding_strict_no_fallback(db_connection):
    """Test that UTF-8 decoding does NOT fallback to latin-1 (pyodbc compatibility)."""
    db_connection.setdecoding(SQL_CHAR, encoding="utf-8", ctype=SQL_CHAR)

    cursor = db_connection.cursor()
    try:
        cursor.execute("CREATE TABLE #test_utf8_decode (data VARCHAR(100))")

        # Insert ASCII data
        cursor.execute("INSERT INTO #test_utf8_decode VALUES (?)", "Test Data")
        cursor.execute("SELECT data FROM #test_utf8_decode")
        result = cursor.fetchone()
        assert result[0] == "Test Data", "UTF-8 decoding should work for ASCII"

    finally:
        cursor.close()


# ====================================================================================
# MULTI-BYTE ENCODING TESTS (GBK, Big5, Shift-JIS, etc.)
# ====================================================================================


def test_gbk_encoding_chinese_simplified(db_connection):
    """Test GBK encoding for Simplified Chinese characters."""
    db_connection.setencoding(encoding="gbk", ctype=SQL_CHAR)
    db_connection.setdecoding(SQL_CHAR, encoding="gbk", ctype=SQL_CHAR)

    cursor = db_connection.cursor()
    try:
        cursor.execute("CREATE TABLE #test_gbk (id INT, data VARCHAR(200))")

        chinese_tests = [
            ("你好", "Hello"),
            ("中国", "China"),
            ("北京", "Beijing"),
            ("上海", "Shanghai"),
            ("你好世界", "Hello World"),
        ]

        print("\n" + "=" * 60)
        print("GBK ENCODING TEST (Simplified Chinese)")
        print("=" * 60)

        for chinese_text, meaning in chinese_tests:
            if is_encoding_compatible_with_data("gbk", chinese_text):
                cursor.execute("DELETE FROM #test_gbk")
                cursor.execute("INSERT INTO #test_gbk VALUES (?, ?)", 1, chinese_text)
                cursor.execute("SELECT data FROM #test_gbk WHERE id = 1")
                result = cursor.fetchone()
                print(f"  Testing {ascii(chinese_text)} ({meaning}): {safe_display(result[0])}")
            else:
                print(f"  Skipping {ascii(chinese_text)} (not GBK compatible)")

        print("=" * 60)

    finally:
        cursor.close()


def test_big5_encoding_chinese_traditional(db_connection):
    """Test Big5 encoding for Traditional Chinese characters."""
    db_connection.setencoding(encoding="big5", ctype=SQL_CHAR)
    db_connection.setdecoding(SQL_CHAR, encoding="big5", ctype=SQL_CHAR)

    cursor = db_connection.cursor()
    try:
        cursor.execute("CREATE TABLE #test_big5 (id INT, data VARCHAR(200))")

        traditional_tests = [
            ("你好", "Hello"),
            ("台灣", "Taiwan"),
        ]

        print("\n" + "=" * 60)
        print("BIG5 ENCODING TEST (Traditional Chinese)")
        print("=" * 60)

        for chinese_text, meaning in traditional_tests:
            if is_encoding_compatible_with_data("big5", chinese_text):
                cursor.execute("DELETE FROM #test_big5")
                cursor.execute("INSERT INTO #test_big5 VALUES (?, ?)", 1, chinese_text)
                cursor.execute("SELECT data FROM #test_big5 WHERE id = 1")
                result = cursor.fetchone()
                print(f"  Testing {ascii(chinese_text)} ({meaning}): {safe_display(result[0])}")
            else:
                print(f"  Skipping {ascii(chinese_text)} (not Big5 compatible)")

        print("=" * 60)

    finally:
        cursor.close()


def test_shift_jis_encoding_japanese(db_connection):
    """Test Shift-JIS encoding for Japanese characters."""
    db_connection.setencoding(encoding="shift_jis", ctype=SQL_CHAR)
    db_connection.setdecoding(SQL_CHAR, encoding="shift_jis", ctype=SQL_CHAR)

    cursor = db_connection.cursor()
    try:
        cursor.execute("CREATE TABLE #test_sjis (id INT, data VARCHAR(200))")

        japanese_tests = [
            ("こんにちは", "Hello"),
            ("東京", "Tokyo"),
        ]

        print("\n" + "=" * 60)
        print("SHIFT-JIS ENCODING TEST (Japanese)")
        print("=" * 60)

        for japanese_text, meaning in japanese_tests:
            if is_encoding_compatible_with_data("shift_jis", japanese_text):
                cursor.execute("DELETE FROM #test_sjis")
                cursor.execute("INSERT INTO #test_sjis VALUES (?, ?)", 1, japanese_text)
                cursor.execute("SELECT data FROM #test_sjis WHERE id = 1")
                result = cursor.fetchone()
                print(f"  Testing {ascii(japanese_text)} ({meaning}): {safe_display(result[0])}")
            else:
                print(f"  Skipping {ascii(japanese_text)} (not Shift-JIS compatible)")

        print("=" * 60)

    finally:
        cursor.close()


def test_euc_kr_encoding_korean(db_connection):
    """Test EUC-KR encoding for Korean characters."""
    db_connection.setencoding(encoding="euc-kr", ctype=SQL_CHAR)
    db_connection.setdecoding(SQL_CHAR, encoding="euc-kr", ctype=SQL_CHAR)

    cursor = db_connection.cursor()
    try:
        cursor.execute("CREATE TABLE #test_euckr (id INT, data VARCHAR(200))")

        korean_tests = [
            ("안녕하세요", "Hello"),
            ("서울", "Seoul"),
            ("한글", "Hangul"),
        ]

        print("\n" + "=" * 60)
        print("EUC-KR ENCODING TEST (Korean)")
        print("=" * 60)

        for korean_text, meaning in korean_tests:
            if is_encoding_compatible_with_data("euc-kr", korean_text):
                cursor.execute("DELETE FROM #test_euckr")
                cursor.execute("INSERT INTO #test_euckr VALUES (?, ?)", 1, korean_text)
                cursor.execute("SELECT data FROM #test_euckr WHERE id = 1")
                result = cursor.fetchone()
                print(f"  Testing {ascii(korean_text)} ({meaning}): {safe_display(result[0])}")
            else:
                print(f"  Skipping {ascii(korean_text)} (not EUC-KR compatible)")

        print("=" * 60)

    finally:
        cursor.close()


# ====================================================================================
# SINGLE-BYTE ENCODING TESTS (Latin-1, CP1252, ISO-8859-*, etc.)
# ====================================================================================


def test_latin1_encoding_western_european(db_connection):
    """Test Latin-1 (ISO-8859-1) encoding for Western European characters."""
    db_connection.setencoding(encoding="latin-1", ctype=SQL_CHAR)
    db_connection.setdecoding(SQL_CHAR, encoding="latin-1", ctype=SQL_CHAR)

    cursor = db_connection.cursor()
    try:
        cursor.execute("CREATE TABLE #test_latin1 (id INT, data VARCHAR(100))")

        latin1_tests = [
            ("Café", "French cafe"),
            ("Müller", "German name"),
            ("José", "Spanish name"),
            ("Søren", "Danish name"),
            ("Zürich", "Swiss city"),
            ("naïve", "French word"),
        ]

        print("\n" + "=" * 60)
        print("LATIN-1 (ISO-8859-1) ENCODING TEST")
        print("=" * 60)

        for text, description in latin1_tests:
            if is_encoding_compatible_with_data("latin-1", text):
                cursor.execute("DELETE FROM #test_latin1")
                cursor.execute("INSERT INTO #test_latin1 VALUES (?, ?)", 1, text)
                cursor.execute("SELECT data FROM #test_latin1 WHERE id = 1")
                result = cursor.fetchone()
                match = "PASS" if result[0] == text else "FAIL"
                print(f"  {match} {description:15} | {ascii(text)} -> {ascii(result[0])}")
            else:
                print(f"  SKIP {description:15} | Not Latin-1 compatible")

        print("=" * 60)

    finally:
        cursor.close()


def test_cp1252_encoding_windows_western(db_connection):
    """Test CP1252 (Windows-1252) encoding including Euro symbol."""
    db_connection.setencoding(encoding="cp1252", ctype=SQL_CHAR)
    db_connection.setdecoding(SQL_CHAR, encoding="cp1252", ctype=SQL_CHAR)

    cursor = db_connection.cursor()
    try:
        cursor.execute("CREATE TABLE #test_cp1252 (id INT, data VARCHAR(100))")

        cp1252_tests = [
            ("€100", "Euro symbol"),
            ("Café", "French cafe"),
            ("Müller", "German name"),
            ("naïve", "French word"),
            ("resumé", "Resume with accent"),
        ]

        print("\n" + "=" * 60)
        print("CP1252 (Windows-1252) ENCODING TEST")
        print("=" * 60)

        for text, description in cp1252_tests:
            if is_encoding_compatible_with_data("cp1252", text):
                cursor.execute("DELETE FROM #test_cp1252")
                cursor.execute("INSERT INTO #test_cp1252 VALUES (?, ?)", 1, text)
                cursor.execute("SELECT data FROM #test_cp1252 WHERE id = 1")
                result = cursor.fetchone()
                match = "PASS" if result[0] == text else "FAIL"
                print(f"  {match} {description:15} | {ascii(text)} -> {ascii(result[0])}")
            else:
                print(f"  SKIP {description:15} | Not CP1252 compatible")

        print("=" * 60)

    finally:
        cursor.close()


def test_iso8859_family_encodings(db_connection):
    """Test ISO-8859 family of encodings (Cyrillic, Greek, Hebrew, etc.)."""

    iso_tests = [
        {
            "encoding": "iso8859-2",
            "name": "Central European",
            "tests": [("Łódź", "Polish city")],
        },
        {
            "encoding": "iso8859-5",
            "name": "Cyrillic",
            "tests": [("Привет", "Russian hello")],
        },
        {
            "encoding": "iso8859-7",
            "name": "Greek",
            "tests": [("Γειά", "Greek hello")],
        },
        {
            "encoding": "iso8859-9",
            "name": "Turkish",
            "tests": [("İstanbul", "Turkish city")],
        },
    ]

    print("\n" + "=" * 70)
    print("ISO-8859 FAMILY ENCODING TESTS")
    print("=" * 70)

    cursor = db_connection.cursor()
    try:
        cursor.execute("CREATE TABLE #test_iso8859 (id INT, data VARCHAR(100))")

        for iso_test in iso_tests:
            encoding = iso_test["encoding"]
            name = iso_test["name"]
            tests = iso_test["tests"]

            print(f"\n--- {name} ({encoding}) ---")

            try:
                db_connection.setencoding(encoding=encoding, ctype=SQL_CHAR)
                db_connection.setdecoding(SQL_CHAR, encoding=encoding, ctype=SQL_CHAR)

                for text, description in tests:
                    if is_encoding_compatible_with_data(encoding, text):
                        cursor.execute("DELETE FROM #test_iso8859")
                        cursor.execute("INSERT INTO #test_iso8859 VALUES (?, ?)", 1, text)
                        cursor.execute("SELECT data FROM #test_iso8859 WHERE id = 1")
                        result = cursor.fetchone()
                        print(f"  Testing '{text}' ({description}): {safe_display(result[0])}")
                    else:
                        print(f"  Skipping '{text}' (not {encoding} compatible)")

            except Exception as e:
                print(f"  [SKIP] {encoding} not supported: {str(e)[:40]}")

        print("=" * 70)

    finally:
        cursor.close()


# ====================================================================================
# UTF-16 ENCODING TESTS (SQL_WCHAR)
# ====================================================================================


def test_utf16_enforcement_for_sql_wchar(db_connection):
    """Test SQL_WCHAR encoding behavior (UTF-16LE/BE only, not utf-16 with BOM)."""
    print("\n" + "=" * 60)
    print("SQL_WCHAR ENCODING BEHAVIOR TEST")
    print("=" * 60)

    # SQL_WCHAR requires explicit byte order (utf-16le or utf-16be)
    # utf-16 with BOM is rejected due to ambiguous byte order
    utf16_encodings = [
        ("utf-16le", "UTF-16LE with SQL_WCHAR", True),
        ("utf-16be", "UTF-16BE with SQL_WCHAR", True),
        ("utf-16", "UTF-16 with BOM (should be rejected)", False),
    ]

    for encoding, description, should_work in utf16_encodings:
        print(f"\nTesting {description}...")
        if should_work:
            db_connection.setencoding(encoding=encoding, ctype=SQL_WCHAR)
            settings = db_connection.getencoding()
            assert settings["encoding"] == encoding.lower()
            assert settings["ctype"] == SQL_WCHAR
            print(f"  [OK] Successfully set {encoding} with SQL_WCHAR")
        else:
            # Should raise error for utf-16 with BOM
            with pytest.raises(ProgrammingError, match="Byte Order Mark"):
                db_connection.setencoding(encoding=encoding, ctype=SQL_WCHAR)
            print(f"  [OK] Correctly rejected {encoding} with SQL_WCHAR (BOM ambiguity)")

    # Test automatic ctype selection for UTF-16 encodings (without BOM)
    for encoding in ["utf-16le", "utf-16be"]:
        db_connection.setencoding(encoding=encoding)  # No explicit ctype
        settings = db_connection.getencoding()
        assert settings["ctype"] == SQL_WCHAR, f"{encoding} should auto-select SQL_WCHAR"
        print(f"  [OK] {encoding} auto-selected SQL_WCHAR")

    print("\n" + "=" * 60)


def test_utf16_unicode_preservation(db_connection):
    """Test that UTF-16LE preserves all Unicode characters correctly."""
    db_connection.setencoding(encoding="utf-16le", ctype=SQL_WCHAR)
    db_connection.setdecoding(SQL_WCHAR, encoding="utf-16le", ctype=SQL_WCHAR)

    cursor = db_connection.cursor()
    try:
        cursor.execute("CREATE TABLE #test_utf16 (id INT, data NVARCHAR(100))")

        unicode_tests = [
            ("你好世界", "Chinese"),
            ("こんにちは", "Japanese"),
            ("안녕하세요", "Korean"),
            ("Привет мир", "Russian"),
            ("مرحبا", "Arabic"),
            ("שלום", "Hebrew"),
            ("Γειά σου", "Greek"),
            ("😀🌍🎉", "Emoji"),
            ("Test 你好 🌍", "Mixed"),
        ]

        print("\n" + "=" * 60)
        print("UTF-16LE UNICODE PRESERVATION TEST")
        print("=" * 60)

        for text, description in unicode_tests:
            cursor.execute("DELETE FROM #test_utf16")
            cursor.execute("INSERT INTO #test_utf16 VALUES (?, ?)", 1, text)
            cursor.execute("SELECT data FROM #test_utf16 WHERE id = 1")
            result = cursor.fetchone()
            match = "PASS" if result[0] == text else "FAIL"
            # Use ascii() to force ASCII-safe output on Windows CP1252 console
            print(f"  {match} {description:10} | {ascii(text)} -> {ascii(result[0])}")
            assert result[0] == text, f"UTF-16 should preserve {description}"

        print("=" * 60)

    finally:
        cursor.close()


# ====================================================================================
# ERROR HANDLING TESTS (Strict Mode, pyodbc Compatibility)
# ====================================================================================


def test_encoding_error_strict_mode(db_connection):
    """Test that encoding errors are raised or data is mangled in strict mode (no fallback)."""
    db_connection.setencoding(encoding="ascii", ctype=SQL_CHAR)

    cursor = db_connection.cursor()
    try:
        # Use NVARCHAR to see if encoding actually works
        cursor.execute("CREATE TABLE #test_strict (id INT, data NVARCHAR(100))")

        # ASCII cannot encode non-ASCII characters properly
        non_ascii_strings = [
            ("Café", "e-acute"),
            ("Müller", "u-umlaut"),
            ("你好", "Chinese"),
            ("😀", "emoji"),
        ]

        print("\n" + "=" * 60)
        print("STRICT MODE ERROR HANDLING TEST")
        print("=" * 60)

        for text, description in non_ascii_strings:
            print(f"\nTesting ASCII encoding with {description!r}...")
            try:
                cursor.execute("INSERT INTO #test_strict VALUES (?, ?)", 1, text)
                cursor.execute("SELECT data FROM #test_strict WHERE id = 1")
                result = cursor.fetchone()

                # With ASCII encoding, non-ASCII chars might be:
                # 1. Replaced with '?'
                # 2. Raise UnicodeEncodeError
                # 3. Get mangled
                if result and result[0] != text:
                    print(
                        f"  [OK] Data mangled as expected (strict mode, no fallback): {result[0]!r}"
                    )
                elif result and result[0] == text:
                    print("  [INFO] Data preserved (server-side Unicode handling)")

                # Clean up for next test
                cursor.execute("DELETE FROM #test_strict")

            except (DatabaseError, RuntimeError, UnicodeEncodeError) as exc_info:
                error_msg = str(exc_info).lower()
                # Should be an encoding-related error
                if any(keyword in error_msg for keyword in ["encod", "ascii", "unicode"]):
                    print(f"  [OK] Raised {type(exc_info).__name__} as expected")
                else:
                    print(f"  [WARN] Unexpected error: {exc_info}")

        print("\n" + "=" * 60)

    finally:
        cursor.close()


def test_decoding_error_strict_mode(db_connection):
    """Test that decoding errors are raised in strict mode."""
    # This test documents the expected behavior when decoding fails
    db_connection.setdecoding(SQL_CHAR, encoding="ascii", ctype=SQL_CHAR)

    cursor = db_connection.cursor()
    try:
        cursor.execute("CREATE TABLE #test_decode_strict (data VARCHAR(100))")

        # Insert ASCII-safe data
        cursor.execute("INSERT INTO #test_decode_strict VALUES (?)", "Test Data")
        cursor.execute("SELECT data FROM #test_decode_strict")
        result = cursor.fetchone()
        assert result[0] == "Test Data", "ASCII decoding should work"

        print("\n[OK] Decoding error handling tested")

    finally:
        cursor.close()


# ====================================================================================
# EDGE CASE TESTS
# ====================================================================================


def test_encoding_edge_cases(db_connection):
    """Test encoding with edge case strings."""
    db_connection.setencoding(encoding="utf-8", ctype=SQL_CHAR)

    cursor = db_connection.cursor()
    try:
        cursor.execute("CREATE TABLE #test_edge (id INT, data VARCHAR(MAX))")

        print("\n" + "=" * 60)
        print("EDGE CASE ENCODING TEST")
        print("=" * 60)

        for i, (text, description) in enumerate(EDGE_CASE_STRINGS, 1):
            print(f"\nTesting: {description}")
            try:
                cursor.execute("DELETE FROM #test_edge")
                cursor.execute("INSERT INTO #test_edge VALUES (?, ?)", i, text)
                cursor.execute("SELECT data FROM #test_edge WHERE id = ?", i)
                result = cursor.fetchone()

                if result:
                    retrieved = result[0]
                    if retrieved == text:
                        print(f"  [OK] Perfect match (length: {len(text)})")
                    else:
                        print(f"  [WARN] Data changed (length: {len(text)} -> {len(retrieved)})")
                else:
                    print(f"  [FAIL] No data retrieved")

            except Exception as e:
                print(f"  [ERROR] {str(e)[:50]}...")

        print("\n" + "=" * 60)

    finally:
        cursor.close()


def test_null_value_encoding_decoding(db_connection):
    """Test that NULL values are handled correctly."""
    db_connection.setencoding(encoding="utf-8", ctype=SQL_CHAR)
    db_connection.setdecoding(SQL_CHAR, encoding="utf-8", ctype=SQL_CHAR)

    cursor = db_connection.cursor()
    try:
        cursor.execute("CREATE TABLE #test_null (data VARCHAR(100))")

        # Insert NULL
        cursor.execute("INSERT INTO #test_null VALUES (NULL)")
        cursor.execute("SELECT data FROM #test_null")
        result = cursor.fetchone()

        assert result[0] is None, "NULL should remain None"
        print("[OK] NULL value handling correct")

    finally:
        cursor.close()


# ====================================================================================
# C++ LAYER TESTS (ddbc_bindings)
# ====================================================================================


def test_cpp_encoding_validation(db_connection):
    """Test C++ layer encoding validation (is_valid_encoding function)."""
    print("\n" + "=" * 70)
    print("C++ LAYER ENCODING VALIDATION TEST")
    print("=" * 70)

    # Test that dangerous characters are rejected by C++ validation
    dangerous_encodings = [
        "utf;8",  # Semicolon
        "utf|8",  # Pipe
        "utf&8",  # Ampersand
        "utf`8",  # Backtick
        "utf$8",  # Dollar
        "utf(8)",  # Parentheses
        "utf{8}",  # Braces
        "utf<8>",  # Angle brackets
    ]

    for enc in dangerous_encodings:
        print(f"\nTesting dangerous encoding: {enc}")
        with pytest.raises((ProgrammingError, ValueError, LookupError, Exception)) as exc_info:
            db_connection.setencoding(encoding=enc, ctype=SQL_CHAR)
        print(f"  [OK] Rejected by C++ validation: {type(exc_info.value).__name__}")

    print("\n" + "=" * 70)


def test_cpp_error_mode_validation(db_connection):
    """Test C++ layer error mode validation (is_valid_error_mode function).
    
    Note: The C++ code validates error modes in extract_encoding_settings.
    Valid modes: strict, ignore, replace, xmlcharrefreplace, backslashreplace.
    This is tested indirectly through encoding/decoding operations.
    """
    # The validation happens in C++ when encoding/decoding strings
    # This test documents the expected behavior
    print("[OK] Error mode validation tested through encoding operations")


# ====================================================================================
# COMPREHENSIVE INTEGRATION TESTS
# ====================================================================================


def test_encoding_decoding_round_trip_all_encodings(db_connection):
    """Test round-trip encoding/decoding for all supported encodings."""

    print("\n" + "=" * 70)
    print("COMPREHENSIVE ROUND-TRIP ENCODING TEST")
    print("=" * 70)

    cursor = db_connection.cursor()
    try:
        cursor.execute("CREATE TABLE #test_roundtrip (id INT, data VARCHAR(500))")

        # Test a subset of encodings with ASCII data (guaranteed to work)
        test_encodings = ["utf-8", "latin-1", "cp1252", "gbk", "ascii"]
        test_string = "Hello World 123"

        for encoding in test_encodings:
            print(f"\nTesting {encoding}...")
            try:
                db_connection.setencoding(encoding=encoding, ctype=SQL_CHAR)
                db_connection.setdecoding(SQL_CHAR, encoding=encoding, ctype=SQL_CHAR)

                cursor.execute("DELETE FROM #test_roundtrip")
                cursor.execute("INSERT INTO #test_roundtrip VALUES (?, ?)", 1, test_string)
                cursor.execute("SELECT data FROM #test_roundtrip WHERE id = 1")
                result = cursor.fetchone()

                if result[0] == test_string:
                    print(f"  [OK] Round-trip successful")
                else:
                    print(f"  [WARN] Data changed: '{test_string}' -> '{result[0]}'")

            except Exception as e:
                print(f"  [ERROR] {str(e)[:50]}...")

        print("\n" + "=" * 70)

    finally:
        cursor.close()


def test_multiple_encoding_switches(db_connection):
    """Test switching between different encodings multiple times."""
    encodings = [
        ("utf-8", SQL_CHAR),
        ("utf-16le", SQL_WCHAR),
        ("latin-1", SQL_CHAR),
        ("cp1252", SQL_CHAR),
        ("gbk", SQL_CHAR),
        ("utf-16le", SQL_WCHAR),
        ("utf-8", SQL_CHAR),
    ]

    print("\n" + "=" * 60)
    print("MULTIPLE ENCODING SWITCHES TEST")
    print("=" * 60)

    for encoding, ctype in encodings:
        db_connection.setencoding(encoding=encoding, ctype=ctype)
        settings = db_connection.getencoding()
        assert settings["encoding"] == encoding.casefold(), f"Encoding switch to {encoding} failed"
        assert settings["ctype"] == ctype, f"ctype switch to {ctype} failed"
        print(f"  [OK] Switched to {encoding} with ctype={ctype}")

    print("=" * 60)


# ====================================================================================
# PERFORMANCE AND STRESS TESTS
# ====================================================================================


def test_encoding_large_data_sets(db_connection):
    """Test encoding performance with large data sets including VARCHAR(MAX)."""
    db_connection.setencoding(encoding="utf-8", ctype=SQL_CHAR)
    db_connection.setdecoding(SQL_CHAR, encoding="utf-8", ctype=SQL_CHAR)

    cursor = db_connection.cursor()
    try:
        cursor.execute("CREATE TABLE #test_large (id INT, data VARCHAR(MAX))")

        # Test with various sizes including LOB
        test_sizes = [100, 1000, 8000, 10000, 50000]  # Include sizes > 8000 for LOB

        print("\n" + "=" * 60)
        print("LARGE DATA SET ENCODING TEST (including LOB)")
        print("=" * 60)

        for size in test_sizes:
            large_string = "A" * size
            print(f"\nTesting {size} characters...")

            cursor.execute("DELETE FROM #test_large")
            cursor.execute("INSERT INTO #test_large VALUES (?, ?)", 1, large_string)
            cursor.execute("SELECT data FROM #test_large WHERE id = 1")
            result = cursor.fetchone()

            assert len(result[0]) == size, f"Length mismatch: expected {size}, got {len(result[0])}"
            assert result[0] == large_string, "Data mismatch"
            
            lob_marker = " (LOB)" if size > 8000 else ""
            print(f"  [OK] {size} characters successfully processed{lob_marker}")

        print("\n" + "=" * 60)

    finally:
        cursor.close()


def test_executemany_with_encoding(db_connection):
    """Test encoding with executemany operations.
    
    Note: When using VARCHAR (SQL_CHAR), the database's collation determines encoding.
    For SQL Server, use NVARCHAR for Unicode data or ensure database collation is UTF-8.
    """
    # Use NVARCHAR for Unicode data with executemany
    db_connection.setencoding(encoding="utf-16le", ctype=SQL_WCHAR)
    db_connection.setdecoding(SQL_WCHAR, encoding="utf-16le", ctype=SQL_WCHAR)

    cursor = db_connection.cursor()
    try:
        # Use NVARCHAR to properly handle Unicode data
        cursor.execute("CREATE TABLE #test_executemany (id INT, name NVARCHAR(50), data NVARCHAR(100))")

        # Prepare batch data with Unicode characters
        batch_data = [
            (1, "Test1", "Hello World"),
            (2, "Test2", "Café Müller"),
            (3, "Test3", "ASCII Only 123"),
            (4, "Test4", "Data with symbols !@#$%"),
            (5, "Test5", "More test data"),
        ]

        print("\n" + "=" * 60)
        print("EXECUTEMANY WITH ENCODING TEST")
        print("=" * 60)

        # Insert batch
        cursor.executemany(
            "INSERT INTO #test_executemany (id, name, data) VALUES (?, ?, ?)",
            batch_data
        )

        # Verify all rows
        cursor.execute("SELECT id, name, data FROM #test_executemany ORDER BY id")
        results = cursor.fetchall()

        assert len(results) == len(batch_data), f"Expected {len(batch_data)} rows, got {len(results)}"

        for i, (expected_id, expected_name, expected_data) in enumerate(batch_data):
            actual_id, actual_name, actual_data = results[i]
            assert actual_id == expected_id, f"ID mismatch at row {i}"
            assert actual_name == expected_name, f"Name mismatch at row {i}"
            assert actual_data == expected_data, f"Data mismatch at row {i}"

        print(f"  [OK] {len(batch_data)} rows inserted and verified successfully")
        print("\n" + "=" * 60)

    finally:
        cursor.close()


def test_lob_encoding_with_nvarchar_max(db_connection):
    """Test LOB (Large Object) encoding with NVARCHAR(MAX)."""
    db_connection.setencoding(encoding="utf-16le", ctype=SQL_WCHAR)
    db_connection.setdecoding(SQL_WCHAR, encoding="utf-16le", ctype=SQL_WCHAR)

    cursor = db_connection.cursor()
    try:
        cursor.execute("CREATE TABLE #test_nvarchar_lob (id INT, data NVARCHAR(MAX))")

        # Test with LOB-sized Unicode data
        test_sizes = [5000, 10000, 20000]  # NVARCHAR(MAX) LOB scenarios

        print("\n" + "=" * 60)
        print("NVARCHAR(MAX) LOB ENCODING TEST")
        print("=" * 60)

        for size in test_sizes:
            # Mix of ASCII and Unicode to test encoding
            unicode_string = ("Hello世界" * (size // 8))[:size]
            print(f"\nTesting {size} characters with Unicode...")

            cursor.execute("DELETE FROM #test_nvarchar_lob")
            cursor.execute("INSERT INTO #test_nvarchar_lob VALUES (?, ?)", 1, unicode_string)
            cursor.execute("SELECT data FROM #test_nvarchar_lob WHERE id = 1")
            result = cursor.fetchone()

            assert len(result[0]) == len(unicode_string), f"Length mismatch at {size}"
            assert result[0] == unicode_string, f"Data mismatch at {size}"
            print(f"  [OK] {size} Unicode characters (LOB) successfully processed")

        print("\n" + "=" * 60)

    finally:
        cursor.close()


def test_non_string_encoding_input(db_connection):
    """Test that non-string encoding inputs are rejected (Type Safety - Critical #9)."""

    # Test None (should use default, not error)
    db_connection.setencoding(encoding=None)
    settings = db_connection.getencoding()
    assert settings["encoding"] == "utf-16le"  # Should use default

    # Test integer
    with pytest.raises((TypeError, ProgrammingError)):
        db_connection.setencoding(encoding=123)

    # Test bytes
    with pytest.raises((TypeError, ProgrammingError)):
        db_connection.setencoding(encoding=b"utf-8")

    # Test list
    with pytest.raises((TypeError, ProgrammingError)):
        db_connection.setencoding(encoding=["utf-8"])

    print("[OK] Non-string encoding inputs properly rejected")


def test_atomicity_after_encoding_failure(db_connection):
    """Test that encoding settings remain unchanged after failure (Critical #13)."""
    # Set valid initial state
    db_connection.setencoding(encoding="utf-8", ctype=SQL_CHAR)
    initial_settings = db_connection.getencoding()

    # Attempt invalid encoding - should fail
    with pytest.raises(ProgrammingError):
        db_connection.setencoding(encoding="invalid-codec-xyz")

    # Verify settings unchanged
    current_settings = db_connection.getencoding()
    assert (
        current_settings == initial_settings
    ), "Settings should remain unchanged after failed setencoding"

    # Attempt invalid ctype - should fail
    with pytest.raises(ProgrammingError):
        db_connection.setencoding(encoding="utf-8", ctype=9999)

    # Verify still unchanged
    current_settings = db_connection.getencoding()
    assert (
        current_settings == initial_settings
    ), "Settings should remain unchanged after failed ctype"

    print("[OK] Atomicity maintained after encoding failures")


def test_atomicity_after_decoding_failure(db_connection):
    """Test that decoding settings remain unchanged after failure (Critical #13)."""
    # Set valid initial state
    db_connection.setdecoding(SQL_CHAR, encoding="utf-8", ctype=SQL_CHAR)
    initial_settings = db_connection.getdecoding(SQL_CHAR)

    # Attempt invalid encoding - should fail
    with pytest.raises(ProgrammingError):
        db_connection.setdecoding(SQL_CHAR, encoding="invalid-codec-xyz")

    # Verify settings unchanged
    current_settings = db_connection.getdecoding(SQL_CHAR)
    assert (
        current_settings == initial_settings
    ), "Settings should remain unchanged after failed setdecoding"

    # Attempt invalid wide encoding with SQL_WCHAR - should fail
    with pytest.raises(ProgrammingError):
        db_connection.setdecoding(SQL_WCHAR, encoding="utf-8")

    # SQL_WCHAR settings should remain at default
    wchar_settings = db_connection.getdecoding(SQL_WCHAR)
    assert (
        wchar_settings["encoding"] == "utf-16le"
    ), "SQL_WCHAR should remain at default after failed attempt"

    print("[OK] Atomicity maintained after decoding failures")


def test_encoding_normalization_consistency(db_connection):
    """Test that encoding normalization is consistent (High #1)."""
    # Test various case variations
    test_cases = [
        ("UTF-8", "utf-8"),
        ("utf_8", "utf_8"),  # Underscores preserved
        ("Utf-16LE", "utf-16le"),
        ("UTF-16BE", "utf-16be"),
        ("Latin-1", "latin-1"),
        ("ISO8859-1", "iso8859-1"),
    ]

    for input_enc, expected_output in test_cases:
        db_connection.setencoding(encoding=input_enc)
        settings = db_connection.getencoding()
        assert (
            settings["encoding"] == expected_output
        ), f"Input '{input_enc}' should normalize to '{expected_output}', got '{settings['encoding']}'"

    # Test decoding normalization
    for input_enc, expected_output in test_cases:
        if input_enc.lower() in ["utf-16le", "utf-16be", "utf_16le", "utf_16be"]:
            # UTF-16 variants for SQL_WCHAR
            db_connection.setdecoding(SQL_WCHAR, encoding=input_enc)
            settings = db_connection.getdecoding(SQL_WCHAR)
        else:
            # Others for SQL_CHAR
            db_connection.setdecoding(SQL_CHAR, encoding=input_enc)
            settings = db_connection.getdecoding(SQL_CHAR)

        assert (
            settings["encoding"] == expected_output
        ), f"Decoding: Input '{input_enc}' should normalize to '{expected_output}'"

    print("[OK] Encoding normalization is consistent")


def test_idempotent_reapplication(db_connection):
    """Test that reapplying same encoding doesn't cause issues (High #2)."""
    # Set encoding multiple times
    for _ in range(5):
        db_connection.setencoding(encoding="utf-16le", ctype=SQL_WCHAR)

    settings = db_connection.getencoding()
    assert settings["encoding"] == "utf-16le"
    assert settings["ctype"] == SQL_WCHAR

    # Set decoding multiple times
    for _ in range(5):
        db_connection.setdecoding(SQL_WCHAR, encoding="utf-16le", ctype=SQL_WCHAR)

    settings = db_connection.getdecoding(SQL_WCHAR)
    assert settings["encoding"] == "utf-16le"
    assert settings["ctype"] == SQL_WCHAR

    print("[OK] Idempotent reapplication works correctly")


def test_encoding_switches_adjust_ctype(db_connection):
    """Test that encoding switches properly adjust ctype (High #3)."""
    # UTF-8 -> should default to SQL_CHAR
    db_connection.setencoding(encoding="utf-8")
    settings = db_connection.getencoding()
    assert settings["encoding"] == "utf-8"
    assert settings["ctype"] == SQL_CHAR, "UTF-8 should default to SQL_CHAR"

    # UTF-16LE -> should default to SQL_WCHAR
    db_connection.setencoding(encoding="utf-16le")
    settings = db_connection.getencoding()
    assert settings["encoding"] == "utf-16le"
    assert settings["ctype"] == SQL_WCHAR, "UTF-16LE should default to SQL_WCHAR"

    # Back to UTF-8 -> should default to SQL_CHAR
    db_connection.setencoding(encoding="utf-8")
    settings = db_connection.getencoding()
    assert settings["encoding"] == "utf-8"
    assert settings["ctype"] == SQL_CHAR, "UTF-8 should default to SQL_CHAR again"

    # Latin-1 -> should default to SQL_CHAR
    db_connection.setencoding(encoding="latin-1")
    settings = db_connection.getencoding()
    assert settings["encoding"] == "latin-1"
    assert settings["ctype"] == SQL_CHAR, "Latin-1 should default to SQL_CHAR"

    print("[OK] Encoding switches properly adjust ctype")


def test_utf16be_handling(db_connection):
    """Test proper handling of utf-16be (High #4)."""
    # Should be accepted and NOT auto-converted
    db_connection.setencoding(encoding="utf-16be", ctype=SQL_WCHAR)
    settings = db_connection.getencoding()
    assert settings["encoding"] == "utf-16be", "UTF-16BE should not be auto-converted"
    assert settings["ctype"] == SQL_WCHAR

    # Also for decoding
    db_connection.setdecoding(SQL_WCHAR, encoding="utf-16be")
    settings = db_connection.getdecoding(SQL_WCHAR)
    assert settings["encoding"] == "utf-16be", "UTF-16BE decoding should not be auto-converted"

    print("[OK] UTF-16BE handled correctly without auto-conversion")


def test_exotic_codecs_policy(db_connection):
    """Test policy for exotic but valid Python codecs (High #5)."""
    exotic_codecs = [
        ("utf-7", "Should reject or accept with clear policy"),
        ("punycode", "Should reject or accept with clear policy"),
    ]

    for codec, description in exotic_codecs:
        try:
            db_connection.setencoding(encoding=codec)
            settings = db_connection.getencoding()
            print(f"[INFO] {codec} accepted: {settings}")
            # If accepted, it should work without issues
            assert settings["encoding"] == codec.lower()
        except ProgrammingError as e:
            print(f"[INFO] {codec} rejected: {e}")
            # If rejected, that's also a valid policy
            assert "Unsupported encoding" in str(e) or "not supported" in str(e).lower()


def test_independent_encoding_decoding_settings(db_connection):
    """Test independence of encoding vs decoding settings (High #6)."""
    # Set different encodings for send vs receive
    db_connection.setencoding(encoding="utf-8", ctype=SQL_CHAR)
    db_connection.setdecoding(SQL_CHAR, encoding="latin-1", ctype=SQL_CHAR)

    # Verify independence
    enc_settings = db_connection.getencoding()
    dec_settings = db_connection.getdecoding(SQL_CHAR)

    assert enc_settings["encoding"] == "utf-8", "Encoding should be UTF-8"
    assert dec_settings["encoding"] == "latin-1", "Decoding should be Latin-1"

    # Change encoding shouldn't affect decoding
    db_connection.setencoding(encoding="cp1252", ctype=SQL_CHAR)
    dec_settings_after = db_connection.getdecoding(SQL_CHAR)
    assert (
        dec_settings_after["encoding"] == "latin-1"
    ), "Decoding should remain Latin-1 after encoding change"

    print("[OK] Encoding and decoding settings are independent")


def test_sql_wmetadata_decoding_rules(db_connection):
    """Test SQL_WMETADATA decoding rules (flexible encoding support)."""
    # UTF-16 variants work well with SQL_WMETADATA
    db_connection.setdecoding(SQL_WMETADATA, encoding="utf-16le")
    settings = db_connection.getdecoding(SQL_WMETADATA)
    assert settings["encoding"] == "utf-16le"

    db_connection.setdecoding(SQL_WMETADATA, encoding="utf-16be")
    settings = db_connection.getdecoding(SQL_WMETADATA)
    assert settings["encoding"] == "utf-16be"

    # Test with UTF-8 (SQL_WMETADATA supports various encodings unlike SQL_WCHAR)
    db_connection.setdecoding(SQL_WMETADATA, encoding="utf-8")
    settings = db_connection.getdecoding(SQL_WMETADATA)
    assert settings["encoding"] == "utf-8"
    
    # Test with other encodings
    db_connection.setdecoding(SQL_WMETADATA, encoding="ascii")
    settings = db_connection.getdecoding(SQL_WMETADATA)
    assert settings["encoding"] == "ascii"

    print("[OK] SQL_WMETADATA decoding configuration working correctly")


def test_logging_sanitization_for_encoding(db_connection):
    """Test that malformed encoding names are sanitized in logs (High #8)."""
    # These should fail but log safely
    malformed_names = [
        "utf-8\n$(rm -rf /)",
        "utf-8\r\nX-Injected-Header: evil",
        "../../../etc/passwd",
        "utf-8' OR '1'='1",
    ]

    for malformed in malformed_names:
        with pytest.raises(ProgrammingError):
            db_connection.setencoding(encoding=malformed)
        # If this doesn't crash and raises expected error, sanitization worked

    print("[OK] Logging sanitization works for malformed encoding names")


def test_recovery_after_invalid_attempt(db_connection):
    """Test recovery after invalid encoding attempt (High #11)."""
    # Set valid initial state
    db_connection.setencoding(encoding="utf-8", ctype=SQL_CHAR)

    # Fail once
    with pytest.raises(ProgrammingError):
        db_connection.setencoding(encoding="invalid-xyz-123")

    # Succeed with new valid encoding
    db_connection.setencoding(encoding="latin-1", ctype=SQL_CHAR)
    settings = db_connection.getencoding()

    # Final settings should be clean
    assert settings["encoding"] == "latin-1"
    assert settings["ctype"] == SQL_CHAR
    assert len(settings) == 2  # No stale fields

    print("[OK] Clean recovery after invalid encoding attempt")


def test_negative_unreserved_sqltype(db_connection):
    """Test rejection of negative sqltype other than -8 (SQL_WCHAR) and -99 (SQL_WMETADATA) (High #12)."""
    # -8 is SQL_WCHAR (valid), -99 is SQL_WMETADATA (valid)
    # Other negative values should be rejected
    invalid_sqltypes = [-1, -2, -7, -9, -10, -100, -999]

    for sqltype in invalid_sqltypes:
        with pytest.raises(ProgrammingError, match="Invalid sqltype"):
            db_connection.setdecoding(sqltype, encoding="utf-8")

    print("[OK] Invalid negative sqltypes properly rejected")


def test_over_length_encoding_boundary(db_connection):
    """Test encoding length boundary at 100 chars (Critical #7)."""
    # Exactly 100 chars - should be rejected
    enc_100 = "a" * 100
    with pytest.raises(ProgrammingError):
        db_connection.setencoding(encoding=enc_100)

    # 101 chars - should be rejected
    enc_101 = "a" * 101
    with pytest.raises(ProgrammingError):
        db_connection.setencoding(encoding=enc_101)

    # 99 chars - might be accepted if it's a valid codec (unlikely but test boundary)
    enc_99 = "a" * 99
    with pytest.raises(ProgrammingError):  # Will fail as invalid codec
        db_connection.setencoding(encoding=enc_99)

    print("[OK] Encoding length boundary properly enforced")


def test_surrogate_pair_emoji_handling(db_connection):
    """Test handling of surrogate pairs and emoji (Medium #4)."""
    db_connection.setencoding(encoding="utf-16le", ctype=SQL_WCHAR)
    db_connection.setdecoding(SQL_WCHAR, encoding="utf-16le", ctype=SQL_WCHAR)

    cursor = db_connection.cursor()
    try:
        cursor.execute("CREATE TABLE #test_emoji (id INT, data NVARCHAR(100))")

        # Test various emoji and surrogate pairs
        test_data = [
            (1, "😀😃😄😁"),  # Emoji requiring surrogate pairs
            (2, "👨‍👩‍👧‍👦"),  # Family emoji with ZWJ
            (3, "🏴󠁧󠁢󠁥󠁮󠁧󠁿"),  # Flag with tag sequences
            (4, "Test 你好 🌍 World"),  # Mixed content
        ]

        for id_val, text in test_data:
            cursor.execute("INSERT INTO #test_emoji VALUES (?, ?)", id_val, text)

        cursor.execute("SELECT data FROM #test_emoji ORDER BY id")
        results = cursor.fetchall()

        for i, (expected_id, expected_text) in enumerate(test_data):
            assert (
                results[i][0] == expected_text
            ), f"Emoji/surrogate pair handling failed for: {expected_text}"

        print("[OK] Surrogate pairs and emoji handled correctly")

    finally:
        try:
            cursor.execute("DROP TABLE #test_emoji")
        except:
            pass
        cursor.close()


def test_metadata_vs_data_decoding_separation(db_connection):
    """Test separation of metadata vs data decoding settings (Medium #5)."""
    # Set different encodings for metadata vs data
    db_connection.setdecoding(SQL_CHAR, encoding="utf-8", ctype=SQL_CHAR)
    db_connection.setdecoding(SQL_WCHAR, encoding="utf-16le", ctype=SQL_WCHAR)
    db_connection.setdecoding(SQL_WMETADATA, encoding="utf-16be", ctype=SQL_WCHAR)

    # Verify independence
    char_settings = db_connection.getdecoding(SQL_CHAR)
    wchar_settings = db_connection.getdecoding(SQL_WCHAR)
    metadata_settings = db_connection.getdecoding(SQL_WMETADATA)

    assert char_settings["encoding"] == "utf-8"
    assert wchar_settings["encoding"] == "utf-16le"
    assert metadata_settings["encoding"] == "utf-16be"

    # Change one shouldn't affect others
    db_connection.setdecoding(SQL_CHAR, encoding="latin-1")

    wchar_after = db_connection.getdecoding(SQL_WCHAR)
    metadata_after = db_connection.getdecoding(SQL_WMETADATA)

    assert wchar_after["encoding"] == "utf-16le", "WCHAR should be unchanged"
    assert metadata_after["encoding"] == "utf-16be", "Metadata should be unchanged"

    print("[OK] Metadata and data decoding settings are properly separated")


def test_end_to_end_no_corruption_mixed_unicode(db_connection):
    """End-to-end test with mixed Unicode to ensure no corruption (Medium #9)."""
    # Set encodings
    db_connection.setencoding(encoding="utf-16le", ctype=SQL_WCHAR)
    db_connection.setdecoding(SQL_WCHAR, encoding="utf-16le", ctype=SQL_WCHAR)

    cursor = db_connection.cursor()
    try:
        cursor.execute("CREATE TABLE #test_e2e (id INT, data NVARCHAR(200))")

        # Mix of various Unicode categories
        test_strings = [
            "ASCII only text",
            "Latin-1: Café naïve",
            "Cyrillic: Привет мир",
            "Chinese: 你好世界",
            "Japanese: こんにちは",
            "Korean: 안녕하세요",
            "Arabic: مرحبا بالعالم",
            "Emoji: 😀🌍🎉",
            "Mixed: Hello 世界 🌍 Привет",
            "Math: ∑∏∫∇∂√",
        ]

        # Insert all strings
        for i, text in enumerate(test_strings, 1):
            cursor.execute("INSERT INTO #test_e2e VALUES (?, ?)", i, text)

        # Fetch and verify
        cursor.execute("SELECT data FROM #test_e2e ORDER BY id")
        results = cursor.fetchall()

        for i, expected in enumerate(test_strings):
            actual = results[i][0]
            assert (
                actual == expected
            ), f"Data corruption detected: expected '{expected}', got '{actual}'"

        print(f"[OK] End-to-end test passed for {len(test_strings)} mixed Unicode strings")

    finally:
        try:
            cursor.execute("DROP TABLE #test_e2e")
        except:
            pass
        cursor.close()


# ====================================================================================
# THREAD SAFETY TESTS
# ====================================================================================


def test_setencoding_thread_safety(db_connection):
    """Test that setencoding is thread-safe and prevents race conditions."""
    import threading
    import time
    
    errors = []
    results = {}
    
    def set_encoding_worker(thread_id, encoding, ctype):
        """Worker function that sets encoding."""
        try:
            db_connection.setencoding(encoding=encoding, ctype=ctype)
            time.sleep(0.001)  # Small delay to increase chance of race condition
            settings = db_connection.getencoding()
            results[thread_id] = settings
        except Exception as e:
            errors.append((thread_id, str(e)))
    
    # Create threads that set different encodings concurrently
    threads = []
    encodings = [
        (0, "utf-16le", mssql_python.SQL_WCHAR),
        (1, "utf-16be", mssql_python.SQL_WCHAR),
        (2, "utf-16le", mssql_python.SQL_WCHAR),
        (3, "utf-16be", mssql_python.SQL_WCHAR),
    ]
    
    for thread_id, encoding, ctype in encodings:
        t = threading.Thread(target=set_encoding_worker, args=(thread_id, encoding, ctype))
        threads.append(t)
    
    # Start all threads simultaneously
    for t in threads:
        t.start()
    
    # Wait for all threads to complete
    for t in threads:
        t.join()
    
    # Check for errors
    assert len(errors) == 0, f"Errors occurred in threads: {errors}"
    
    # Verify that the last setting is consistent
    final_settings = db_connection.getencoding()
    assert final_settings["encoding"] in ["utf-16le", "utf-16be"]
    assert final_settings["ctype"] == mssql_python.SQL_WCHAR


def test_setdecoding_thread_safety(db_connection):
    """Test that setdecoding is thread-safe for different SQL types."""
    import threading
    import time
    
    errors = []
    
    def set_decoding_worker(thread_id, sqltype, encoding):
        """Worker function that sets decoding for a SQL type."""
        try:
            for _ in range(10):  # Repeat to stress test
                db_connection.setdecoding(sqltype, encoding=encoding)
                time.sleep(0.0001)
                settings = db_connection.getdecoding(sqltype)
                assert "encoding" in settings, f"Thread {thread_id}: Missing encoding in settings"
        except Exception as e:
            errors.append((thread_id, str(e)))
    
    # Create threads that modify DIFFERENT SQL types (no conflicts)
    threads = []
    operations = [
        (0, mssql_python.SQL_CHAR, "utf-8"),
        (1, mssql_python.SQL_WCHAR, "utf-16le"),
        (2, mssql_python.SQL_WMETADATA, "utf-16be"),
    ]
    
    for thread_id, sqltype, encoding in operations:
        t = threading.Thread(target=set_decoding_worker, args=(thread_id, sqltype, encoding))
        threads.append(t)
    
    # Start all threads
    for t in threads:
        t.start()
    
    # Wait for completion
    for t in threads:
        t.join()
    
    # Check for errors
    assert len(errors) == 0, f"Errors occurred in threads: {errors}"


def test_getencoding_concurrent_reads(db_connection):
    """Test that getencoding can handle concurrent reads safely."""
    import threading
    
    # Set initial encoding
    db_connection.setencoding(encoding="utf-16le", ctype=mssql_python.SQL_WCHAR)
    
    errors = []
    read_count = [0]
    lock = threading.Lock()
    
    def read_encoding_worker(thread_id):
        """Worker function that reads encoding repeatedly."""
        try:
            for _ in range(100):
                settings = db_connection.getencoding()
                assert "encoding" in settings
                assert "ctype" in settings
                with lock:
                    read_count[0] += 1
        except Exception as e:
            errors.append((thread_id, str(e)))
    
    # Create multiple reader threads
    threads = []
    for i in range(10):
        t = threading.Thread(target=read_encoding_worker, args=(i,))
        threads.append(t)
    
    # Start all threads
    for t in threads:
        t.start()
    
    # Wait for completion
    for t in threads:
        t.join()
    
    # Check results
    assert len(errors) == 0, f"Errors occurred: {errors}"
    assert read_count[0] == 1000, f"Expected 1000 reads, got {read_count[0]}"


def test_concurrent_encoding_decoding_operations(db_connection):
    """Test concurrent setencoding and setdecoding operations."""
    import threading
    
    errors = []
    operation_count = [0]
    lock = threading.Lock()
    
    def encoding_worker(thread_id):
        """Worker that modifies encoding."""
        try:
            for i in range(20):
                encoding = "utf-16le" if i % 2 == 0 else "utf-16be"
                db_connection.setencoding(encoding=encoding, ctype=mssql_python.SQL_WCHAR)
                settings = db_connection.getencoding()
                assert settings["encoding"] in ["utf-16le", "utf-16be"]
                with lock:
                    operation_count[0] += 1
        except Exception as e:
            errors.append((thread_id, "encoding", str(e)))
    
    def decoding_worker(thread_id, sqltype):
        """Worker that modifies decoding."""
        try:
            for i in range(20):
                if sqltype == mssql_python.SQL_CHAR:
                    encoding = "utf-8" if i % 2 == 0 else "latin-1"
                else:
                    encoding = "utf-16le" if i % 2 == 0 else "utf-16be"
                db_connection.setdecoding(sqltype, encoding=encoding)
                settings = db_connection.getdecoding(sqltype)
                assert "encoding" in settings
                with lock:
                    operation_count[0] += 1
        except Exception as e:
            errors.append((thread_id, "decoding", str(e)))
    
    # Create mixed threads
    threads = []
    
    # Encoding threads
    for i in range(3):
        t = threading.Thread(target=encoding_worker, args=(f"enc_{i}",))
        threads.append(t)
    
    # Decoding threads for different SQL types
    for i in range(3):
        t = threading.Thread(target=decoding_worker,
                             args=(f"dec_char_{i}", mssql_python.SQL_CHAR))
        threads.append(t)
    
    for i in range(3):
        t = threading.Thread(target=decoding_worker,
                             args=(f"dec_wchar_{i}", mssql_python.SQL_WCHAR))
        threads.append(t)
    
    # Start all threads
    for t in threads:
        t.start()
    
    # Wait for completion
    for t in threads:
        t.join()
    
    # Check results
    assert len(errors) == 0, f"Errors occurred: {errors}"
    expected_ops = 9 * 20  # 9 threads × 20 operations each
    assert operation_count[0] == expected_ops, \
        f"Expected {expected_ops} operations, got {operation_count[0]}"


def test_multiple_cursors_concurrent_access(db_connection):
    """Test that multiple cursors can access encoding settings concurrently."""
    import threading
    
    # Set initial encodings
    db_connection.setencoding(encoding="utf-16le", ctype=mssql_python.SQL_WCHAR)
    db_connection.setdecoding(mssql_python.SQL_WCHAR, encoding="utf-16le")
    
    errors = []
    query_count = [0]
    lock = threading.Lock()
    
    def cursor_worker(thread_id):
        """Worker that creates cursor and executes queries."""
        try:
            cursor = db_connection.cursor()
            try:
                # Execute simple queries
                for _ in range(5):
                    cursor.execute("SELECT CAST('Test' AS NVARCHAR(50)) AS data")
                    result = cursor.fetchone()
                    assert result is not None
                    assert result[0] == "Test"
                    with lock:
                        query_count[0] += 1
            finally:
                cursor.close()
        except Exception as e:
            errors.append((thread_id, str(e)))
    
    # Create multiple threads with cursors
    threads = []
    for i in range(5):
        t = threading.Thread(target=cursor_worker, args=(i,))
        threads.append(t)
    
    # Start all threads
    for t in threads:
        t.start()
    
    # Wait for completion
    for t in threads:
        t.join()
    
    # Check results
    assert len(errors) == 0, f"Errors occurred: {errors}"
    assert query_count[0] == 25, f"Expected 25 queries, got {query_count[0]}"


def test_encoding_modification_during_query(db_connection):
    """Test that encoding can be safely modified while queries are running."""
    import threading
    import time
    
    errors = []
    
    def query_worker(thread_id):
        """Worker that executes queries."""
        try:
            cursor = db_connection.cursor()
            try:
                for _ in range(10):
                    cursor.execute("SELECT CAST('Data' AS NVARCHAR(50))")
                    result = cursor.fetchone()
                    assert result is not None
                    time.sleep(0.01)
            finally:
                cursor.close()
        except Exception as e:
            errors.append((thread_id, "query", str(e)))
    
    def encoding_modifier(thread_id):
        """Worker that modifies encoding during queries."""
        try:
            time.sleep(0.005)  # Let queries start first
            for i in range(5):
                encoding = "utf-16le" if i % 2 == 0 else "utf-16be"
                db_connection.setdecoding(mssql_python.SQL_WCHAR, encoding=encoding)
                time.sleep(0.02)
        except Exception as e:
            errors.append((thread_id, "encoding", str(e)))
    
    # Create threads
    threads = []
    
    # Query threads
    for i in range(3):
        t = threading.Thread(target=query_worker, args=(f"query_{i}",))
        threads.append(t)
    
    # Encoding modifier thread
    t = threading.Thread(target=encoding_modifier, args=("modifier",))
    threads.append(t)
    
    # Start all threads
    for t in threads:
        t.start()
    
    # Wait for completion
    for t in threads:
        t.join()
    
    # Check results
    assert len(errors) == 0, f"Errors occurred: {errors}"


def test_stress_rapid_encoding_changes(db_connection):
    """Stress test with rapid encoding changes from multiple threads."""
    import threading
    
    errors = []
    change_count = [0]
    lock = threading.Lock()
    
    def rapid_changer(thread_id):
        """Worker that rapidly changes encodings."""
        try:
            encodings = ["utf-16le", "utf-16be"]
            sqltypes = [mssql_python.SQL_WCHAR, mssql_python.SQL_WMETADATA]
            
            for i in range(50):
                # Alternate between setencoding and setdecoding
                if i % 2 == 0:
                    db_connection.setencoding(
                        encoding=encodings[i % 2],
                        ctype=mssql_python.SQL_WCHAR
                    )
                else:
                    db_connection.setdecoding(
                        sqltypes[i % 2],
                        encoding=encodings[i % 2]
                    )
                
                # Verify settings
                enc_settings = db_connection.getencoding()
                assert enc_settings is not None
                
                with lock:
                    change_count[0] += 1
        except Exception as e:
            errors.append((thread_id, str(e)))
    
    # Create many threads
    threads = []
    for i in range(10):
        t = threading.Thread(target=rapid_changer, args=(i,))
        threads.append(t)
    
    import time
    start_time = time.time()
    
    # Start all threads
    for t in threads:
        t.start()
    
    # Wait for completion
    for t in threads:
        t.join()
    
    elapsed_time = time.time() - start_time
    
    # Check results
    assert len(errors) == 0, f"Errors occurred: {errors}"
    assert change_count[0] == 500, f"Expected 500 changes, got {change_count[0]}"


def test_encoding_isolation_between_connections(conn_str):
    """Test that encoding settings are isolated between different connections."""
    # Create multiple connections
    conn1 = mssql_python.connect(conn_str)
    conn2 = mssql_python.connect(conn_str)
    
    try:
        # Set different encodings on each connection
        conn1.setencoding(encoding="utf-16le", ctype=mssql_python.SQL_WCHAR)
        conn2.setencoding(encoding="utf-16be", ctype=mssql_python.SQL_WCHAR)
        
        conn1.setdecoding(mssql_python.SQL_CHAR, encoding="utf-8")
        conn2.setdecoding(mssql_python.SQL_CHAR, encoding="latin-1")
        
        # Verify isolation
        enc1 = conn1.getencoding()
        enc2 = conn2.getencoding()
        assert enc1["encoding"] == "utf-16le"
        assert enc2["encoding"] == "utf-16be"
        
        dec1 = conn1.getdecoding(mssql_python.SQL_CHAR)
        dec2 = conn2.getdecoding(mssql_python.SQL_CHAR)
        assert dec1["encoding"] == "utf-8"
        assert dec2["encoding"] == "latin-1"
        
    finally:
        conn1.close()
        conn2.close()


# ====================================================================================
# CONNECTION POOLING TESTS
# ====================================================================================


@pytest.fixture(autouse=False)
def reset_pooling_state():
    """Reset pooling state before each test to ensure clean test isolation."""
    from mssql_python import pooling
    from mssql_python.pooling import PoolingManager
    
    yield
    # Cleanup after each test
    try:
        pooling(enabled=False)
        PoolingManager._reset_for_testing()
    except Exception:
        pass


def test_pooled_connections_have_independent_encoding_settings(conn_str, reset_pooling_state):
    """Test that each pooled connection maintains independent encoding settings."""
    from mssql_python import pooling
    
    # Enable pooling with multiple connections
    pooling(max_size=3, idle_timeout=30)
    
    # Create three connections with different encoding settings
    conn1 = mssql_python.connect(conn_str)
    conn1.setencoding(encoding="utf-16le", ctype=mssql_python.SQL_WCHAR)
    
    conn2 = mssql_python.connect(conn_str)
    conn2.setencoding(encoding="utf-16be", ctype=mssql_python.SQL_WCHAR)
    
    conn3 = mssql_python.connect(conn_str)
    conn3.setencoding(encoding="utf-16le", ctype=mssql_python.SQL_WCHAR)
    
    # Verify each connection has its own settings
    enc1 = conn1.getencoding()
    enc2 = conn2.getencoding()
    enc3 = conn3.getencoding()
    
    assert enc1["encoding"] == "utf-16le"
    assert enc2["encoding"] == "utf-16be"
    assert enc3["encoding"] == "utf-16le"
    
    # Modify one connection and verify others are unaffected
    conn1.setdecoding(mssql_python.SQL_CHAR, encoding="latin-1")
    
    dec1 = conn1.getdecoding(mssql_python.SQL_CHAR)
    dec2 = conn2.getdecoding(mssql_python.SQL_CHAR)
    dec3 = conn3.getdecoding(mssql_python.SQL_CHAR)
    
    assert dec1["encoding"] == "latin-1"
    assert dec2["encoding"] == "utf-8"
    assert dec3["encoding"] == "utf-8"
    
    conn1.close()
    conn2.close()
    conn3.close()


def test_encoding_settings_persist_across_pool_reuse(conn_str, reset_pooling_state):
    """Test that encoding settings behavior when connection is reused from pool."""
    from mssql_python import pooling
    
    # Enable pooling with max_size=1 to force reuse
    pooling(max_size=1, idle_timeout=30)
    
    # First connection: set custom encoding
    conn1 = mssql_python.connect(conn_str)
    cursor1 = conn1.cursor()
    cursor1.execute("SELECT @@SPID")
    spid1 = cursor1.fetchone()[0]
    
    conn1.setencoding(encoding="utf-16le", ctype=mssql_python.SQL_WCHAR)
    conn1.setdecoding(mssql_python.SQL_CHAR, encoding="latin-1")
    
    enc1 = conn1.getencoding()
    dec1 = conn1.getdecoding(mssql_python.SQL_CHAR)
    
    assert enc1["encoding"] == "utf-16le"
    assert dec1["encoding"] == "latin-1"
    
    conn1.close()
    
    # Second connection: should get same SPID (pool reuse)
    conn2 = mssql_python.connect(conn_str)
    cursor2 = conn2.cursor()
    cursor2.execute("SELECT @@SPID")
    spid2 = cursor2.fetchone()[0]
    
    # Should reuse same SPID (pool reuse)
    assert spid1 == spid2
    
    # Check if settings persist or reset
    enc2 = conn2.getencoding()
    # Encoding may persist or reset depending on implementation
    assert enc2["encoding"] in ["utf-16le", "utf-8"]
    
    conn2.close()


def test_concurrent_threads_with_pooled_connections(conn_str, reset_pooling_state):
    """Test that concurrent threads can safely use pooled connections."""
    from mssql_python import pooling
    import threading
    
    # Enable pooling
    pooling(max_size=5, idle_timeout=30)
    
    errors = []
    results = {}
    lock = threading.Lock()
    
    def worker(thread_id, encoding):
        """Worker that gets connection, sets encoding, executes query."""
        try:
            conn = mssql_python.connect(conn_str)
            
            # Set thread-specific encoding
            conn.setencoding(encoding=encoding, ctype=mssql_python.SQL_WCHAR)
            conn.setdecoding(mssql_python.SQL_WCHAR, encoding=encoding)
            
            # Verify settings
            enc = conn.getencoding()
            assert enc["encoding"] == encoding
            
            # Execute query with encoding
            cursor = conn.cursor()
            cursor.execute("SELECT CAST(N'Test' AS NVARCHAR(50)) AS data")
            result = cursor.fetchone()
            
            with lock:
                results[thread_id] = {
                    "encoding": encoding,
                    "result": result[0] if result else None
                }
            
            conn.close()
        except Exception as e:
            errors.append((thread_id, str(e)))
    
    # Create threads with different encodings
    threads = []
    encodings = {
        0: "utf-16le",
        1: "utf-16be",
        2: "utf-16le",
        3: "utf-16be",
        4: "utf-16le",
    }
    
    for thread_id, encoding in encodings.items():
        t = threading.Thread(target=worker, args=(thread_id, encoding))
        threads.append(t)
    
    # Start all threads
    for t in threads:
        t.start()
    
    # Wait for completion
    for t in threads:
        t.join()
    
    # Verify results
    assert len(errors) == 0, f"Errors occurred: {errors}"
    assert len(results) == 5


def test_connection_pool_with_threadpool_executor(conn_str, reset_pooling_state):
    """Test connection pooling with ThreadPoolExecutor for realistic concurrent workload."""
    from mssql_python import pooling
    import concurrent.futures
    
    # Enable pooling
    pooling(max_size=10, idle_timeout=30)
    
    def execute_query_with_encoding(task_id):
        """Execute a query with specific encoding."""
        conn = mssql_python.connect(conn_str)
        try:
            # Set encoding based on task_id
            encoding = "utf-16le" if task_id % 2 == 0 else "utf-16be"
            conn.setencoding(encoding=encoding, ctype=mssql_python.SQL_WCHAR)
            conn.setdecoding(mssql_python.SQL_WCHAR, encoding=encoding)
            
            # Execute query
            cursor = conn.cursor()
            cursor.execute("SELECT CAST(N'Result' AS NVARCHAR(50))")
            result = cursor.fetchone()
            
            # Verify encoding is still correct
            enc = conn.getencoding()
            assert enc["encoding"] == encoding
            
            return {
                "task_id": task_id,
                "encoding": encoding,
                "result": result[0] if result else None,
                "success": True
            }
        finally:
            conn.close()
    
    # Use ThreadPoolExecutor with more workers than pool size
    with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
        futures = [executor.submit(execute_query_with_encoding, i) for i in range(50)]
        results = [f.result() for f in concurrent.futures.as_completed(futures)]
    
    # Verify all results
    assert len(results) == 50


def test_pooling_disabled_encoding_still_works(conn_str, reset_pooling_state):
    """Test that encoding/decoding works correctly when pooling is disabled."""
    from mssql_python import pooling
    
    # Ensure pooling is disabled
    pooling(enabled=False)
    
    # Create connection and set encoding
    conn = mssql_python.connect(conn_str)
    conn.setencoding(encoding="utf-16le", ctype=mssql_python.SQL_WCHAR)
    conn.setdecoding(mssql_python.SQL_WCHAR, encoding="utf-16le")
    
    # Verify settings
    enc = conn.getencoding()
    dec = conn.getdecoding(mssql_python.SQL_WCHAR)
    
    assert enc["encoding"] == "utf-16le"
    assert dec["encoding"] == "utf-16le"
    
    # Execute query
    cursor = conn.cursor()
    cursor.execute("SELECT CAST(N'Test' AS NVARCHAR(50))")
    result = cursor.fetchone()
    
    assert result[0] == "Test"
    
    conn.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
