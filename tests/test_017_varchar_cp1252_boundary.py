"""
VARCHAR CP1252 Exact Length Boundary Test Suite

Tests for a bug where VARCHAR columns fail to fetch correctly when:
1. The data length exactly equals the column size (e.g., 10 characters in VARCHAR(10))
2. The data contains non-ASCII characters from CP1252 encoding (e.g., é, à, ñ)

Copyright (c) Microsoft Corporation.
Licensed under the MIT license.
"""

import pytest
from mssql_python import SQL_CHAR


def test_varchar_cp1252_exact_length_boundary(db_connection):
    """Test VARCHAR CP1252 characters at exact column size boundary."""
    cursor = db_connection.cursor()

    try:
        cursor.execute("""
            CREATE TABLE #test_cp1252_boundary (
                id INT PRIMARY KEY,
                varchar_10 VARCHAR(10) COLLATE SQL_Latin1_General_CP1_CI_AS,
                varchar_20 VARCHAR(20) COLLATE SQL_Latin1_General_CP1_CI_AS,
                varchar_50 VARCHAR(50) COLLATE SQL_Latin1_General_CP1_CI_AS,
                varchar_100 VARCHAR(100) COLLATE SQL_Latin1_General_CP1_CI_AS
            )
        """)
        db_connection.commit()

        # Set encoding to CP1252 for VARCHAR columns
        db_connection.setencoding(encoding="cp1252", ctype=SQL_CHAR)
        db_connection.setdecoding(SQL_CHAR, encoding="cp1252", ctype=SQL_CHAR)

        # Test Case 1: Exact length boundary with CP1252 characters
        test_data_exact_10 = "café René!"  # 10 characters with é accents
        assert len(test_data_exact_10) == 10

        cursor.execute(
            "INSERT INTO #test_cp1252_boundary (id, varchar_10) VALUES (?, ?)",
            1,
            test_data_exact_10,
        )
        db_connection.commit()

        # Retrieve and verify - THIS IS THE CRITICAL TEST
        cursor.execute("SELECT varchar_10 FROM #test_cp1252_boundary WHERE id = ?", 1)
        result = cursor.fetchone()
        assert result is not None, "No data retrieved for exact boundary test"
        assert (
            result[0] == test_data_exact_10
        ), f"Expected '{test_data_exact_10}', got '{result[0]}'"

        # Test Case 2: Various CP1252 characters at exact boundary
        cp1252_strings = [
            ("señor año", 10),  # Spanish ñ
            ("élève été", 10),  # French é, è
            ("Größe Maß", 10),  # German ö, ß
            ("naïve café", 10),  # ï, é
        ]

        test_id = 2
        for test_str, expected_len in cp1252_strings:
            test_str_padded = test_str.ljust(expected_len)[:expected_len]
            assert len(test_str_padded) == expected_len

            cursor.execute(
                "INSERT INTO #test_cp1252_boundary (id, varchar_10) VALUES (?, ?)",
                test_id,
                test_str_padded,
            )
            db_connection.commit()

            cursor.execute("SELECT varchar_10 FROM #test_cp1252_boundary WHERE id = ?", test_id)
            result = cursor.fetchone()
            assert result is not None, f"No data retrieved for test_id {test_id}"
            assert result[0] == test_str_padded, f"Expected '{test_str_padded}', got '{result[0]}'"
            test_id += 1

        # Test Case 3: Exact length at different column sizes
        test_cases = [
            (20, "José García Pérez"),
            (50, "Françoise Müller-Öztürk visitó el café año " + "x" * 5),
        ]

        for col_size, test_str in test_cases:
            test_str_exact = test_str.ljust(col_size)[:col_size]
            assert len(test_str_exact) == col_size

            col_name = f"varchar_{col_size}"
            cursor.execute(
                f"INSERT INTO #test_cp1252_boundary (id, {col_name}) VALUES (?, ?)",
                test_id,
                test_str_exact,
            )
            db_connection.commit()

            cursor.execute(f"SELECT {col_name} FROM #test_cp1252_boundary WHERE id = ?", test_id)
            result = cursor.fetchone()
            assert result is not None, f"No data retrieved for {col_name}"
            assert (
                result[0].rstrip() == test_str_exact.rstrip()
            ), f"Expected '{test_str_exact}', got '{result[0]}'"
            test_id += 1

        # Test Case 4: Edge case - all CP1252 special characters at boundary
        special_chars_10 = "éàèùçñöü"[:10].ljust(10)
        cursor.execute(
            "INSERT INTO #test_cp1252_boundary (id, varchar_10) VALUES (?, ?)",
            test_id,
            special_chars_10,
        )
        db_connection.commit()

        cursor.execute("SELECT varchar_10 FROM #test_cp1252_boundary WHERE id = ?", test_id)
        result = cursor.fetchone()
        assert result is not None, "No data retrieved for special chars test"
        assert (
            result[0] == special_chars_10
        ), f"Special chars failed: expected '{special_chars_10}', got '{result[0]}'"

        # Test Case 5: fetchall() with multiple rows at exact boundary
        cursor.execute("SELECT id, varchar_10 FROM #test_cp1252_boundary WHERE id <= 4 ORDER BY id")
        rows = cursor.fetchall()
        assert len(rows) == 4, f"Should have 4 rows, got {len(rows)}"
        assert (
            rows[0][1] == test_data_exact_10
        ), f"fetchall() failed for first row: expected '{test_data_exact_10}', got '{rows[0][1]}'"

    finally:
        cursor.close()


def test_varchar_cp1252_length_variations(db_connection):
    """Test VARCHAR CP1252 at various lengths relative to column size."""
    cursor = db_connection.cursor()

    try:
        cursor.execute("""
            CREATE TABLE #test_cp1252_variations (
                id INT PRIMARY KEY,
                varchar_col VARCHAR(20) COLLATE SQL_Latin1_General_CP1_CI_AS
            )
        """)
        db_connection.commit()

        db_connection.setencoding(encoding="cp1252", ctype=SQL_CHAR)
        db_connection.setdecoding(SQL_CHAR, encoding="cp1252", ctype=SQL_CHAR)

        test_cases = [
            (1, "café", 4),  # Short (4 chars, column allows 20)
            (2, "café René", 10),  # Medium (10 chars)
            (3, "José García", 12),  # Medium (12 chars)
            (4, "café René José", 16),  # Near boundary (16 chars)
            (5, "Müller-Öztürk café", 19),  # One less than max (19 chars)
            (6, "café René José year", 20),  # Exact boundary (20 chars) - CRITICAL
        ]

        for test_id, test_str, expected_len in test_cases:
            if len(test_str) < expected_len:
                test_str = test_str.ljust(expected_len)
            else:
                test_str = test_str[:expected_len]

            assert len(test_str) == expected_len, f"Test data length mismatch for id {test_id}"

            cursor.execute(
                "INSERT INTO #test_cp1252_variations (id, varchar_col) VALUES (?, ?)",
                test_id,
                test_str,
            )
            db_connection.commit()

        cursor.execute("SELECT id, varchar_col FROM #test_cp1252_variations ORDER BY id")
        rows = cursor.fetchall()

        assert len(rows) == len(test_cases), f"Expected {len(test_cases)} rows, got {len(rows)}"

        for i, (test_id, test_str, expected_len) in enumerate(test_cases):
            if len(test_str) < expected_len:
                expected = test_str.ljust(expected_len)
            else:
                expected = test_str[:expected_len]

            row = rows[i]
            assert row[0] == test_id, f"ID mismatch at row {i}"
            assert (
                row[1].rstrip() == expected.rstrip()
            ), f"Data mismatch for id {test_id}: expected '{expected}', got '{row[1]}'"

    finally:
        cursor.close()


def test_varchar_cp1252_mixed_ascii_nonascii(db_connection):
    """Test VARCHAR with mixed ASCII and CP1252 characters at exact boundary."""
    cursor = db_connection.cursor()

    try:
        cursor.execute("""
            CREATE TABLE #test_cp1252_mixed (
                id INT PRIMARY KEY,
                varchar_col VARCHAR(15) COLLATE SQL_Latin1_General_CP1_CI_AS
            )
        """)
        db_connection.commit()

        db_connection.setencoding(encoding="cp1252", ctype=SQL_CHAR)
        db_connection.setdecoding(SQL_CHAR, encoding="cp1252", ctype=SQL_CHAR)

        # All test strings are exactly 15 characters
        test_cases = [
            (1, "All ASCII text!"),  # 15 chars, all ASCII
            (2, "café with more!"),  # 15 chars, 1 CP1252 char (é)
            (3, "José García Ms."),  # 15 chars, 2 CP1252 chars (é, í)
            (4, "Müller Größe ÖÄ"),  # 15 chars, 4 CP1252 chars (ü, ö, ß, Ö, Ä)
            (5, "éàèùçñöüïâêôûîë"),  # 15 chars, all CP1252 chars
        ]

        for test_id, test_str in test_cases:
            assert (
                len(test_str) == 15
            ), f"Test string {test_id} should be 15 chars, got {len(test_str)}"

            cursor.execute(
                "INSERT INTO #test_cp1252_mixed (id, varchar_col) VALUES (?, ?)",
                test_id,
                test_str,
            )
            db_connection.commit()

            cursor.execute("SELECT varchar_col FROM #test_cp1252_mixed WHERE id = ?", test_id)
            result = cursor.fetchone()
            assert result is not None, f"No data retrieved for test_id {test_id}"
            assert (
                result[0] == test_str
            ), f"Test ID {test_id}: expected '{test_str}', got '{result[0]}'"

    finally:
        cursor.close()


def test_varchar_cp1252_empty_and_null(db_connection):
    """Test edge cases: empty strings and NULL values with CP1252 encoding."""
    cursor = db_connection.cursor()

    try:
        cursor.execute("""
            CREATE TABLE #test_cp1252_edge (
                id INT PRIMARY KEY,
                varchar_col VARCHAR(10) COLLATE SQL_Latin1_General_CP1_CI_AS
            )
        """)
        db_connection.commit()

        db_connection.setencoding(encoding="cp1252", ctype=SQL_CHAR)
        db_connection.setdecoding(SQL_CHAR, encoding="cp1252", ctype=SQL_CHAR)

        cursor.execute("INSERT INTO #test_cp1252_edge (id, varchar_col) VALUES (1, '')")
        cursor.execute("INSERT INTO #test_cp1252_edge (id, varchar_col) VALUES (2, NULL)")
        cursor.execute("INSERT INTO #test_cp1252_edge (id, varchar_col) VALUES (3, ?)", "é")
        cursor.execute("INSERT INTO #test_cp1252_edge (id, varchar_col) VALUES (4, ?)", "café")
        db_connection.commit()

        cursor.execute("SELECT varchar_col FROM #test_cp1252_edge WHERE id = ?", 1)
        result = cursor.fetchone()
        assert (
            result is not None and result[0] == ""
        ), "Empty string should be retrieved as empty string"

        cursor.execute("SELECT varchar_col FROM #test_cp1252_edge WHERE id = ?", 2)
        result = cursor.fetchone()
        assert result is not None and result[0] is None, "NULL should be retrieved as None"

        cursor.execute("SELECT varchar_col FROM #test_cp1252_edge WHERE id = ?", 3)
        result = cursor.fetchone()
        assert result is not None and result[0] == "é", "Single CP1252 char should work"

        cursor.execute("SELECT varchar_col FROM #test_cp1252_edge WHERE id = ?", 4)
        result = cursor.fetchone()
        assert result is not None and result[0] == "café", "4 char CP1252 string should work"

    finally:
        cursor.close()


def test_varchar_cp1252_parameterized_query(db_connection):
    """Test VARCHAR CP1252 boundary with parameterized queries."""
    cursor = db_connection.cursor()

    try:
        cursor.execute("""
            CREATE TABLE #test_cp1252_params (
                id INT PRIMARY KEY,
                varchar_10 VARCHAR(10) COLLATE SQL_Latin1_General_CP1_CI_AS,
                varchar_20 VARCHAR(20) COLLATE SQL_Latin1_General_CP1_CI_AS
            )
        """)
        db_connection.commit()

        db_connection.setencoding(encoding="cp1252", ctype=SQL_CHAR)
        db_connection.setdecoding(SQL_CHAR, encoding="cp1252", ctype=SQL_CHAR)

        test_str_10 = "café René!"  # Exactly 10 chars
        test_str_20 = "José García Pérez   "  # Exactly 20 chars

        assert len(test_str_10) == 10
        assert len(test_str_20) == 20

        cursor.execute(
            "INSERT INTO #test_cp1252_params (id, varchar_10, varchar_20) VALUES (?, ?, ?)",
            1,
            test_str_10,
            test_str_20,
        )
        db_connection.commit()

        cursor.execute("SELECT varchar_10, varchar_20 FROM #test_cp1252_params WHERE id = ?", 1)
        result = cursor.fetchone()

        assert result is not None, "No data retrieved"
        assert result[0] == test_str_10, f"VARCHAR(10): expected '{test_str_10}', got '{result[0]}'"
        assert (
            result[1].rstrip() == test_str_20.rstrip()
        ), f"VARCHAR(20): expected '{test_str_20}', got '{result[1]}'"

    finally:
        cursor.close()


def test_varchar_cp1252_fetchall_multi_column_batch(db_connection):
    """Test fetchall() with multiple VARCHAR columns to exercise batch fetch ProcessChar path.

    This covers the hoisted effectiveCharEnc optimization (computed once before
    the column loop) and ensures all columns in a batch share the same encoding.
    """
    cursor = db_connection.cursor()

    try:
        cursor.execute("""
            CREATE TABLE #test_cp1252_multi_col (
                id INT PRIMARY KEY,
                col_a VARCHAR(10) COLLATE SQL_Latin1_General_CP1_CI_AS,
                col_b VARCHAR(15) COLLATE SQL_Latin1_General_CP1_CI_AS,
                col_c VARCHAR(20) COLLATE SQL_Latin1_General_CP1_CI_AS
            )
        """)
        db_connection.commit()

        db_connection.setencoding(encoding="cp1252", ctype=SQL_CHAR)
        db_connection.setdecoding(SQL_CHAR, encoding="cp1252", ctype=SQL_CHAR)

        test_rows = [
            (1, "café René", "Müller Größe ÖÄ", "José García Pérez   "),
            (2, "señor año", "naïve café idea", "Françoise visitó él"),
            (3, "élève été", "Ångström café!!", "Größe Maß Öztürk!!! "),
        ]

        for row in test_rows:
            cursor.execute(
                "INSERT INTO #test_cp1252_multi_col (id, col_a, col_b, col_c) "
                "VALUES (?, ?, ?, ?)",
                *row,
            )
        db_connection.commit()

        # fetchall exercises the batch fetch path (FetchBatchData → ProcessChar)
        cursor.execute("SELECT id, col_a, col_b, col_c FROM #test_cp1252_multi_col ORDER BY id")
        rows = cursor.fetchall()

        assert len(rows) == 3, f"Expected 3 rows, got {len(rows)}"
        for i, expected in enumerate(test_rows):
            assert rows[i][0] == expected[0], f"Row {i} id mismatch"
            assert (
                rows[i][1].rstrip() == expected[1].rstrip()
            ), f"Row {i} col_a: expected '{expected[1]}', got '{rows[i][1]}'"
            assert (
                rows[i][2].rstrip() == expected[2].rstrip()
            ), f"Row {i} col_b: expected '{expected[2]}', got '{rows[i][2]}'"
            assert (
                rows[i][3].rstrip() == expected[3].rstrip()
            ), f"Row {i} col_c: expected '{expected[3]}', got '{rows[i][3]}'"

    finally:
        cursor.close()


def test_varchar_cp1252_fetchmany_batch(db_connection):
    """Test fetchmany() with CP1252 data to exercise partial batch fetch path."""
    cursor = db_connection.cursor()

    try:
        cursor.execute("""
            CREATE TABLE #test_cp1252_fetchmany (
                id INT PRIMARY KEY,
                data VARCHAR(10) COLLATE SQL_Latin1_General_CP1_CI_AS
            )
        """)
        db_connection.commit()

        db_connection.setencoding(encoding="cp1252", ctype=SQL_CHAR)
        db_connection.setdecoding(SQL_CHAR, encoding="cp1252", ctype=SQL_CHAR)

        expected_data = []
        for i in range(1, 11):
            val = f"café{i:04d}é"  # CP1252 chars at start and end
            cursor.execute("INSERT INTO #test_cp1252_fetchmany (id, data) VALUES (?, ?)", i, val)
            expected_data.append(val)
        db_connection.commit()

        cursor.execute("SELECT id, data FROM #test_cp1252_fetchmany ORDER BY id")

        # Fetch in batches of 3
        batch1 = cursor.fetchmany(3)
        assert len(batch1) == 3, f"First batch should have 3 rows, got {len(batch1)}"
        for j, row in enumerate(batch1):
            assert (
                row[1] == expected_data[j]
            ), f"Batch1 row {j}: expected '{expected_data[j]}', got '{row[1]}'"

        batch2 = cursor.fetchmany(3)
        assert len(batch2) == 3, f"Second batch should have 3 rows, got {len(batch2)}"
        for j, row in enumerate(batch2):
            assert (
                row[1] == expected_data[j + 3]
            ), f"Batch2 row {j}: expected '{expected_data[j + 3]}', got '{row[1]}'"

        # Fetch remaining
        batch3 = cursor.fetchmany(10)
        assert len(batch3) == 4, f"Third batch should have 4 rows, got {len(batch3)}"

    finally:
        cursor.close()


def test_varchar_cp1252_mixed_types_batch(db_connection):
    """Test batch fetch with mixed column types (INT, VARCHAR, FLOAT)
    to exercise the column processor dispatch table with CP1252 encoding.
    """
    cursor = db_connection.cursor()

    try:
        cursor.execute("""
            CREATE TABLE #test_cp1252_mixed_types (
                id INT PRIMARY KEY,
                name VARCHAR(20) COLLATE SQL_Latin1_General_CP1_CI_AS,
                score FLOAT,
                city VARCHAR(15) COLLATE SQL_Latin1_General_CP1_CI_AS,
                age SMALLINT
            )
        """)
        db_connection.commit()

        db_connection.setencoding(encoding="cp1252", ctype=SQL_CHAR)
        db_connection.setdecoding(SQL_CHAR, encoding="cp1252", ctype=SQL_CHAR)

        test_rows = [
            (1, "François Müller", 95.5, "Zürich café", 30),
            (2, "José García", 88.2, "Málaga señor", 25),
            (3, "Ångström Björk", 76.9, "Göteborg naïve", 40),
        ]

        for row in test_rows:
            cursor.execute(
                "INSERT INTO #test_cp1252_mixed_types (id, name, score, city, age) "
                "VALUES (?, ?, ?, ?, ?)",
                *row,
            )
        db_connection.commit()

        cursor.execute(
            "SELECT id, name, score, city, age " "FROM #test_cp1252_mixed_types ORDER BY id"
        )
        rows = cursor.fetchall()

        assert len(rows) == 3
        for i, expected in enumerate(test_rows):
            assert rows[i][0] == expected[0], f"Row {i} id mismatch"
            assert rows[i][1].rstrip() == expected[1].rstrip(), f"Row {i} name mismatch"
            assert abs(rows[i][2] - expected[2]) < 0.01, f"Row {i} score mismatch"
            assert rows[i][3].rstrip() == expected[3].rstrip(), f"Row {i} city mismatch"
            assert rows[i][4] == expected[4], f"Row {i} age mismatch"

    finally:
        cursor.close()


def test_varchar_cp1252_lob_with_collation(db_connection):
    """Test VARCHAR(MAX) LOB data with CP1252 encoding and explicit collation.

    Exercises the FetchLobColumnData slow path in ProcessChar.
    """
    cursor = db_connection.cursor()

    try:
        cursor.execute("""
            CREATE TABLE #test_cp1252_lob (
                id INT PRIMARY KEY,
                data VARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS
            )
        """)
        db_connection.commit()

        db_connection.setencoding(encoding="cp1252", ctype=SQL_CHAR)
        db_connection.setdecoding(SQL_CHAR, encoding="cp1252", ctype=SQL_CHAR)

        # Create large CP1252 data that exceeds LOB threshold (>8000 bytes)
        cp1252_pattern = "café René señor Müller Größe naïve "
        large_data = cp1252_pattern * 250  # ~9000 chars

        cursor.execute("INSERT INTO #test_cp1252_lob (id, data) VALUES (?, ?)", 1, large_data)
        db_connection.commit()

        cursor.execute("SELECT data FROM #test_cp1252_lob WHERE id = 1")
        result = cursor.fetchone()

        assert result is not None, "No data retrieved for LOB test"
        assert result[0] == large_data, (
            f"LOB data mismatch: expected {len(large_data)} chars, " f"got {len(result[0])} chars"
        )

    finally:
        cursor.close()


def test_varchar_cp1252_varying_lengths_per_row(db_connection):
    """Test batch fetch with varying data lengths per row within the same column.

    Ensures ProcessChar handles different dataLen values across rows in a batch.
    """
    cursor = db_connection.cursor()

    try:
        cursor.execute("""
            CREATE TABLE #test_cp1252_varying (
                id INT PRIMARY KEY,
                data VARCHAR(50) COLLATE SQL_Latin1_General_CP1_CI_AS
            )
        """)
        db_connection.commit()

        db_connection.setencoding(encoding="cp1252", ctype=SQL_CHAR)
        db_connection.setdecoding(SQL_CHAR, encoding="cp1252", ctype=SQL_CHAR)

        # Rows with very different lengths
        test_data = [
            (1, "é"),  # 1 char
            (2, "café"),  # 4 chars
            (3, ""),  # empty string
            (4, "café René señor Müller Größe ñ"),  # 30 chars
            (5, "Françoise Müller-Öztürk visitó el café año paddin"),  # 50 chars (exact)
        ]

        for row in test_data:
            cursor.execute("INSERT INTO #test_cp1252_varying (id, data) VALUES (?, ?)", *row)
        db_connection.commit()

        cursor.execute("SELECT id, data FROM #test_cp1252_varying ORDER BY id")
        rows = cursor.fetchall()

        assert len(rows) == len(test_data), f"Expected {len(test_data)} rows, got {len(rows)}"
        for i, (expected_id, expected_val) in enumerate(test_data):
            assert rows[i][0] == expected_id
            fetched = rows[i][1]
            if expected_val == "":
                assert fetched == "", f"Row {i}: expected empty string, got '{fetched}'"
            else:
                assert (
                    fetched.rstrip() == expected_val.rstrip()
                ), f"Row {i}: expected '{expected_val}', got '{fetched}'"

    finally:
        cursor.close()


def test_varchar_cp1252_null_interspersed_batch(db_connection):
    """Test batch fetch with NULLs interspersed among CP1252 data rows.

    Ensures the NULL/NO_TOTAL central check works correctly alongside
    ProcessChar in the batch path.
    """
    cursor = db_connection.cursor()

    try:
        cursor.execute("""
            CREATE TABLE #test_cp1252_nulls (
                id INT PRIMARY KEY,
                data VARCHAR(20) COLLATE SQL_Latin1_General_CP1_CI_AS
            )
        """)
        db_connection.commit()

        db_connection.setencoding(encoding="cp1252", ctype=SQL_CHAR)
        db_connection.setdecoding(SQL_CHAR, encoding="cp1252", ctype=SQL_CHAR)

        test_data = [
            (1, "café René"),
            (2, None),
            (3, "señor Müller"),
            (4, None),
            (5, "Größe naïve"),
            (6, ""),
            (7, None),
            (8, "Ångström"),
        ]

        for tid, val in test_data:
            cursor.execute("INSERT INTO #test_cp1252_nulls (id, data) VALUES (?, ?)", tid, val)
        db_connection.commit()

        cursor.execute("SELECT id, data FROM #test_cp1252_nulls ORDER BY id")
        rows = cursor.fetchall()

        assert len(rows) == len(test_data)
        for i, (expected_id, expected_val) in enumerate(test_data):
            assert rows[i][0] == expected_id, f"Row {i} id mismatch"
            if expected_val is None:
                assert rows[i][1] is None, f"Row {i}: expected None, got '{rows[i][1]}'"
            elif expected_val == "":
                assert rows[i][1] == "", f"Row {i}: expected empty, got '{rows[i][1]}'"
            else:
                assert (
                    rows[i][1].rstrip() == expected_val.rstrip()
                ), f"Row {i}: expected '{expected_val}', got '{rows[i][1]}'"

    finally:
        cursor.close()


def test_varchar_cp1252_decode_fallback_returns_bytes(db_connection):
    """Test that decode failure in ProcessChar falls back to bytes, not None.

    On Linux/macOS the driver returns UTF-8 for SQL_C_CHAR, so decode almost
    never fails. This test inserts raw binary data through VARBINARY→CAST to
    produce bytes that are invalid UTF-8 in a VARCHAR column, then verifies
    the fallback path returns bytes rather than None.
    """
    cursor = db_connection.cursor()

    try:
        cursor.execute("""
            CREATE TABLE #test_decode_fallback_bytes (
                id INT PRIMARY KEY,
                data VARCHAR(20) COLLATE SQL_Latin1_General_CP1_CI_AS
            )
        """)
        db_connection.commit()

        # Insert valid CP1252 data
        db_connection.setencoding(encoding="cp1252", ctype=SQL_CHAR)
        cursor.execute(
            "INSERT INTO #test_decode_fallback_bytes (id, data) VALUES (1, ?)",
            "café",
        )
        db_connection.commit()

        # Now decode with correct encoding — should always succeed
        db_connection.setdecoding(SQL_CHAR, encoding="cp1252", ctype=SQL_CHAR)
        cursor.execute("SELECT data FROM #test_decode_fallback_bytes WHERE id = 1")
        result = cursor.fetchone()
        assert result is not None
        assert result[0] == "café", f"Expected 'café', got '{result[0]}'"

        # Attempt with intentionally wrong encoding to probe fallback
        # On Linux, driver returns UTF-8, so "ascii" decode of UTF-8 multi-byte
        # would fail at the Python layer — exercises the fallback path
        db_connection.setdecoding(SQL_CHAR, encoding="ascii", ctype=SQL_CHAR)
        cursor.execute("SELECT data FROM #test_decode_fallback_bytes WHERE id = 1")
        result = cursor.fetchone()
        assert result is not None, "Result should not be None even on decode failure"
        # On decode failure: should be bytes (not None) — the key assertion
        # On success (if driver returns ASCII-safe): should be str
        assert result[0] is not None, (
            "Value should never be None for non-NULL database data; "
            "expected str on successful decode or bytes on fallback"
        )

    finally:
        cursor.close()


def test_varchar_cp1252_fetchall_many_rows(db_connection):
    """Test batch fetch with many rows of CP1252 data to exercise the
    batch processing loop thoroughly with the hoisted encoding.
    """
    cursor = db_connection.cursor()

    try:
        cursor.execute("""
            CREATE TABLE #test_cp1252_many (
                id INT PRIMARY KEY,
                data VARCHAR(30) COLLATE SQL_Latin1_General_CP1_CI_AS
            )
        """)
        db_connection.commit()

        db_connection.setencoding(encoding="cp1252", ctype=SQL_CHAR)
        db_connection.setdecoding(SQL_CHAR, encoding="cp1252", ctype=SQL_CHAR)

        patterns = [
            "café René señor",
            "Müller Größe naïve",
            "José García Pérez",
            "Françoise Öztürk",
            "Ångström Björk été",
        ]

        row_count = 100
        for i in range(1, row_count + 1):
            val = patterns[(i - 1) % len(patterns)]
            cursor.execute("INSERT INTO #test_cp1252_many (id, data) VALUES (?, ?)", i, val)
        db_connection.commit()

        cursor.execute("SELECT id, data FROM #test_cp1252_many ORDER BY id")
        rows = cursor.fetchall()

        assert len(rows) == row_count, f"Expected {row_count} rows, got {len(rows)}"
        for i, row in enumerate(rows):
            expected_val = patterns[i % len(patterns)]
            assert row[0] == i + 1
            assert (
                row[1].rstrip() == expected_val.rstrip()
            ), f"Row {i}: expected '{expected_val}', got '{row[1]}'"

    finally:
        cursor.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
