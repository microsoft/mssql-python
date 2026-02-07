# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Polars integration tests for mssql-python driver (Issue #352)."""

import datetime
import platform

import pytest

# Skip on Alpine ARM64 — polars crashes with "Illegal instruction" during import.
_machine = platform.machine().lower()
_is_arm = _machine in ("aarch64", "arm64", "armv8")

# Check if running on Alpine (musl libc)
_is_alpine = False
try:
    with open("/etc/os-release", "r") as f:
        _is_alpine = "alpine" in f.read().lower()
except (FileNotFoundError, PermissionError):
    # /etc/os-release missing or unreadable — not Alpine, continue.
    pass

if _is_arm and _is_alpine:
    pytest.skip(
        "Skipping polars tests on Alpine ARM64 (polars crashes during import)",
        allow_module_level=True,
    )

# Now safe to import polars on supported platforms
pl = pytest.importorskip("polars", reason="polars not available on this platform")


class TestPolarsIntegration:
    """Integration tests for polars compatibility with mssql-python."""

    def test_polars_read_database_basic(self, cursor, db_connection):
        """
        Test polars can read basic data types via pl.read_database().

        This is the exact scenario reported in issue #352.
        """
        # Create test table with various types
        cursor.execute("""
            CREATE TABLE #pytest_polars_basic (
                id INT,
                name NVARCHAR(100),
                value FLOAT
            );
        """)
        cursor.execute("""
            INSERT INTO #pytest_polars_basic VALUES
            (1, 'Alice', 100.5),
            (2, 'Bob', 200.75),
            (3, 'Charlie', 300.25);
        """)
        db_connection.commit()

        try:
            # Use polars read_database with our connection
            df = pl.read_database(
                query="SELECT id, name, value FROM #pytest_polars_basic ORDER BY id",
                connection=db_connection,
            )

            assert len(df) == 3, "Should have 3 rows"
            assert df.columns == ["id", "name", "value"], "Column names should match"
            assert df["id"].to_list() == [1, 2, 3], "id values should match"
            assert df["name"].to_list() == ["Alice", "Bob", "Charlie"], "name values should match"

        finally:
            cursor.execute("DROP TABLE IF EXISTS #pytest_polars_basic;")
            db_connection.commit()

    def test_polars_read_database_with_dates(self, cursor, db_connection):
        """
        Test polars can read DATE columns - the specific failure case from issue #352.

        The original error was:
        ComputeError: could not append value: 2013-01-01 of type: date to the builder
        """
        cursor.execute("""
            CREATE TABLE #pytest_polars_dates (
                id INT,
                date_col DATE,
                datetime_col DATETIME
            );
        """)
        cursor.execute("""
            INSERT INTO #pytest_polars_dates VALUES
            (1, '2013-01-01', '2013-01-01 10:30:00'),
            (2, '2024-06-15', '2024-06-15 14:45:30'),
            (3, '2025-12-31', '2025-12-31 23:59:59');
        """)
        db_connection.commit()

        try:
            df = pl.read_database(
                query="SELECT id, date_col, datetime_col FROM #pytest_polars_dates ORDER BY id",
                connection=db_connection,
            )

            assert len(df) == 3, "Should have 3 rows"
            assert "date_col" in df.columns, "date_col should be present"
            assert "datetime_col" in df.columns, "datetime_col should be present"

            # Verify date values are correct
            dates = df["date_col"].to_list()
            assert dates[0] == datetime.date(2013, 1, 1), "First date should be 2013-01-01"
            assert dates[1] == datetime.date(2024, 6, 15), "Second date should be 2024-06-15"

        finally:
            cursor.execute("DROP TABLE IF EXISTS #pytest_polars_dates;")
            db_connection.commit()

    def test_polars_read_database_all_common_types(self, cursor, db_connection):
        """
        Test polars can read all common SQL Server data types.
        """
        cursor.execute("""
            CREATE TABLE #pytest_polars_types (
                int_col INT,
                bigint_col BIGINT,
                float_col FLOAT,
                decimal_col DECIMAL(10,2),
                varchar_col VARCHAR(100),
                nvarchar_col NVARCHAR(100),
                bit_col BIT,
                date_col DATE,
                datetime_col DATETIME,
                time_col TIME
            );
        """)
        cursor.execute("""
            INSERT INTO #pytest_polars_types VALUES
            (42, 9223372036854775807, 3.14159, 123.45, 'hello', N'世界', 1, 
             '2025-01-15', '2025-01-15 10:30:00', '14:30:00');
        """)
        db_connection.commit()

        try:
            df = pl.read_database(
                query="SELECT * FROM #pytest_polars_types",
                connection=db_connection,
            )

            assert len(df) == 1, "Should have 1 row"
            assert len(df.columns) == 10, "Should have 10 columns"

            # Verify all column values
            row = df.row(0)
            assert row[0] == 42, "int_col should be 42"
            assert row[1] == 9223372036854775807, "bigint_col should be max BIGINT"
            assert abs(row[2] - 3.14159) < 1e-4, "float_col should be ~3.14159"
            assert row[4] == "hello", "varchar_col should be 'hello'"
            assert row[5] == "世界", "nvarchar_col should be '世界' (Unicode)"
            assert row[6] in (1, True), "bit_col should be 1 or True"

        finally:
            cursor.execute("DROP TABLE IF EXISTS #pytest_polars_types;")
            db_connection.commit()

    def test_polars_read_database_with_nulls(self, cursor, db_connection):
        """
        Test polars can handle NULL values correctly.
        """
        cursor.execute("""
            CREATE TABLE #pytest_polars_nulls (
                id INT,
                nullable_str NVARCHAR(100) NULL,
                nullable_int INT NULL,
                nullable_date DATE NULL
            );
        """)
        cursor.execute("""
            INSERT INTO #pytest_polars_nulls VALUES
            (1, 'has value', 100, '2025-01-01'),
            (2, NULL, NULL, NULL),
            (3, 'another', 200, '2025-12-31');
        """)
        db_connection.commit()

        try:
            df = pl.read_database(
                query="SELECT * FROM #pytest_polars_nulls ORDER BY id",
                connection=db_connection,
            )

            assert len(df) == 3, "Should have 3 rows"

            # Check NULL handling across all nullable columns
            str_values = df["nullable_str"].to_list()
            assert str_values[0] == "has value"
            assert str_values[1] is None, "NULL should become None"
            assert str_values[2] == "another"

            int_values = df["nullable_int"].to_list()
            assert int_values[0] == 100
            assert int_values[1] is None, "NULL int should become None"
            assert int_values[2] == 200

            # Issue #352: date NULL handling was the original bug
            date_values = df["nullable_date"].to_list()
            assert date_values[1] is None, "NULL date should become None (Issue #352)"
            assert date_values[0] is not None, "Non-NULL date should have a value"
            assert date_values[2] is not None, "Non-NULL date should have a value"

        finally:
            cursor.execute("DROP TABLE IF EXISTS #pytest_polars_nulls;")
            db_connection.commit()

    def test_polars_read_database_large_result(self, cursor, db_connection):
        """
        Test polars can handle larger result sets.
        """
        cursor.execute("""
            CREATE TABLE #pytest_polars_large (
                id INT,
                data NVARCHAR(100)
            );
        """)

        # Insert 1000 rows
        for i in range(100):
            values = ", ".join([f"({i*10+j}, 'row_{i*10+j}')" for j in range(10)])
            cursor.execute(f"INSERT INTO #pytest_polars_large VALUES {values};")
        db_connection.commit()

        try:
            df = pl.read_database(
                query="SELECT * FROM #pytest_polars_large ORDER BY id",
                connection=db_connection,
            )

            assert len(df) == 1000, "Should have 1000 rows"
            assert df["id"].min() == 0, "Min id should be 0"
            assert df["id"].max() == 999, "Max id should be 999"

        finally:
            cursor.execute("DROP TABLE IF EXISTS #pytest_polars_large;")
            db_connection.commit()
