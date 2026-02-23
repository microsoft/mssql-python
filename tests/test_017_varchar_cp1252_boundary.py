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
        cursor.execute(
            """
            CREATE TABLE #test_cp1252_boundary (
                id INT PRIMARY KEY,
                varchar_10 VARCHAR(10),
                varchar_20 VARCHAR(20),
                varchar_50 VARCHAR(50),
                varchar_100 VARCHAR(100)
            )
        """
        )
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
        cursor.execute(
            """
            CREATE TABLE #test_cp1252_variations (
                id INT PRIMARY KEY,
                varchar_col VARCHAR(20)
            )
        """
        )
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
        cursor.execute(
            """
            CREATE TABLE #test_cp1252_mixed (
                id INT PRIMARY KEY,
                varchar_col VARCHAR(15)
            )
        """
        )
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
        cursor.execute(
            """
            CREATE TABLE #test_cp1252_edge (
                id INT PRIMARY KEY,
                varchar_col VARCHAR(10)
            )
        """
        )
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
        cursor.execute(
            """
            CREATE TABLE #test_cp1252_params (
                id INT PRIMARY KEY,
                varchar_10 VARCHAR(10),
                varchar_20 VARCHAR(20)
            )
        """
        )
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


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
