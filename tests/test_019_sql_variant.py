"""
Tests for SQL_VARIANT data type support.

This test file validates that sql_variant columns correctly preserve base data types
and return appropriate native Python types for each SQL Server base type.

**SQL Server sql_variant Behavior** (per Microsoft docs):
- Stores value PLUS base data type information (up to 8,016 bytes total)
- Can contain: Most SQL Server base types
- Cannot contain: text, ntext, image, timestamp, xml, varchar(max), nvarchar(max),
  varbinary(max), sql_variant itself, geometry, geography, hierarchyid, user-defined types

**Supported Base Types and Expected Python Mappings**:
- INT, SMALLINT, TINYINT, BIGINT → Python int
- REAL, FLOAT → Python float
- DECIMAL, NUMERIC → Python Decimal
- BIT → Python bool/int
- CHAR, VARCHAR, NCHAR, NVARCHAR → Python str
- DATETIME, SMALLDATETIME, DATETIME2 → Python datetime
- DATE → Python date
- TIME → Python time
- BINARY, VARBINARY → Python bytes
- UNIQUEIDENTIFIER → Python UUID (as string or UUID object)
- NULL → Python None

This test suite uses explicit CAST statements to ensure we're testing specific base types.
"""

import pytest
import decimal
from datetime import datetime, date, time
import uuid


def drop_table_if_exists(cursor, table_name):
    """Drop the table if it exists"""
    try:
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    except Exception as e:
        pytest.fail(f"Failed to drop table {table_name}: {e}")


@pytest.fixture(scope="module")
def variant_test_table(cursor, db_connection):
    """
    Create a test table with sql_variant column and populate with various SQL base types.
    Uses explicit CAST to ensure each value is stored with the intended base type.
    """
    table_name = "#pytest_sql_variant"
    drop_table_if_exists(cursor, table_name)

    cursor.execute(f"""
        CREATE TABLE {table_name} (
            id INT PRIMARY KEY,
            variant_col SQL_VARIANT,
            base_type NVARCHAR(50),  -- What SQL type is stored in variant
            description NVARCHAR(100)
        )
    """)
    db_connection.commit()

    # Insert test data with explicit CAST for each SQL base type
    test_data = [
        # Numeric integer types
        (1, "CAST(123 AS INT)", "int", "Integer (INT)"),
        (2, "CAST(255 AS TINYINT)", "tinyint", "Tiny Integer (TINYINT)"),
        (3, "CAST(32000 AS SMALLINT)", "smallint", "Small Integer (SMALLINT)"),
        (4, "CAST(9223372036854775807 AS BIGINT)", "bigint", "Big Integer (BIGINT)"),
        # Floating point types
        (5, "CAST(123.45 AS REAL)", "real", "Real (REAL)"),
        (6, "CAST(123.456789 AS FLOAT)", "float", "Float/Double (FLOAT)"),
        # Exact numeric types
        (7, "CAST(999.99 AS DECIMAL(10,2))", "decimal", "Decimal (DECIMAL)"),
        (8, "CAST(888.88 AS NUMERIC(10,2))", "numeric", "Numeric (NUMERIC)"),
        # Bit type
        (9, "CAST(1 AS BIT)", "bit", "Bit True (BIT)"),
        (10, "CAST(0 AS BIT)", "bit", "Bit False (BIT)"),
        # Character types
        (11, "CAST('Hello' AS VARCHAR(50))", "varchar", "Varchar (VARCHAR)"),
        (12, "CAST(N'World' AS NVARCHAR(50))", "nvarchar", "NVarchar (NVARCHAR)"),
        (13, "CAST('Fixed' AS CHAR(10))", "char", "Fixed CHAR (CHAR)"),
        (14, "CAST(N'Fixed' AS NCHAR(10))", "nchar", "Fixed NCHAR (NCHAR)"),
        # Date/Time types
        (15, "CAST('2024-05-20' AS DATE)", "date", "Date (DATE)"),
        (16, "CAST('12:34:56' AS TIME)", "time", "Time (TIME)"),
        (17, "CAST('2024-05-20 12:34:56.123' AS DATETIME)", "datetime", "DateTime (DATETIME)"),
        (
            18,
            "CAST('2024-05-20 12:34:00' AS SMALLDATETIME)",
            "smalldatetime",
            "SmallDateTime (SMALLDATETIME)",
        ),
        (
            19,
            "CAST('2024-05-20 12:34:56.1234567' AS DATETIME2)",
            "datetime2",
            "DateTime2 (DATETIME2)",
        ),
        # Binary type
        (20, "CAST(0x48656C6C6F AS BINARY(10))", "binary", "Fixed BINARY (BINARY)"),
        (21, "CAST(0x48656C6C6F AS VARBINARY(50))", "varbinary", "VarBinary (VARBINARY)"),
        # GUID type
        (
            22,
            "CAST('6F9619FF-8B86-D011-B42D-00C04FC964FF' AS UNIQUEIDENTIFIER)",
            "uniqueidentifier",
            "GUID (UNIQUEIDENTIFIER)",
        ),
        # NULL
        (23, "NULL", "NULL", "NULL value"),
    ]

    for row in test_data:
        cursor.execute(f"""
            INSERT INTO {table_name} (id, variant_col, base_type, description)
            VALUES ({row[0]}, {row[1]}, '{row[2]}', '{row[3]}')
        """)

    # Also test implicit type conversion (what SQL Server chooses)
    cursor.execute(f"INSERT INTO {table_name} VALUES (24, 123, 'int', 'Implicit int literal')")
    cursor.execute(
        f"INSERT INTO {table_name} VALUES (25, 45.67, 'numeric', 'Implicit decimal literal')"
    )
    cursor.execute(
        f"INSERT INTO {table_name} VALUES (26, N'Test', 'nvarchar', 'Implicit nvarchar literal')"
    )

    db_connection.commit()

    yield table_name

    # Cleanup
    drop_table_if_exists(cursor, table_name)
    db_connection.commit()


# ============================================================================
# Tests for Integer Types
# ============================================================================


def test_sql_variant_int(cursor, variant_test_table):
    """Test sql_variant with INT base type returns Python int"""
    cursor.execute(
        f"SELECT id, variant_col, base_type, description FROM {variant_test_table} WHERE id = 1"
    )
    row = cursor.fetchone()

    assert row is not None
    assert row[0] == 1
    assert row[1] == 123, f"Expected 123, got {row[1]}"
    assert isinstance(row[1], int), f"INT should return Python int, got {type(row[1])}"
    assert row[2] == "int"


def test_sql_variant_tinyint(cursor, variant_test_table):
    """Test sql_variant with TINYINT base type returns Python int"""
    cursor.execute(f"SELECT id, variant_col, base_type FROM {variant_test_table} WHERE id = 2")
    row = cursor.fetchone()

    assert row is not None
    assert row[1] == 255, f"Expected 255, got {row[1]}"
    assert isinstance(row[1], int), f"TINYINT should return Python int, got {type(row[1])}"


def test_sql_variant_smallint(cursor, variant_test_table):
    """Test sql_variant with SMALLINT base type returns Python int"""
    cursor.execute(f"SELECT id, variant_col, base_type FROM {variant_test_table} WHERE id = 3")
    row = cursor.fetchone()

    assert row is not None
    assert row[1] == 32000, f"Expected 32000, got {row[1]}"
    assert isinstance(row[1], int), f"SMALLINT should return Python int, got {type(row[1])}"


def test_sql_variant_bigint(cursor, variant_test_table):
    """Test sql_variant with BIGINT base type returns Python int"""
    cursor.execute(f"SELECT id, variant_col, base_type FROM {variant_test_table} WHERE id = 4")
    row = cursor.fetchone()

    assert row is not None
    assert row[1] == 9223372036854775807, f"Expected max bigint, got {row[1]}"
    assert isinstance(row[1], int), f"BIGINT should return Python int, got {type(row[1])}"


# ============================================================================
# Tests for Floating Point Types
# ============================================================================


def test_sql_variant_real(cursor, variant_test_table):
    """Test sql_variant with REAL base type returns Python float"""
    cursor.execute(f"SELECT id, variant_col, base_type FROM {variant_test_table} WHERE id = 5")
    row = cursor.fetchone()

    assert row is not None
    assert isinstance(row[1], float), f"REAL should return Python float, got {type(row[1])}"
    assert row[1] == pytest.approx(123.45, rel=1e-5), f"Expected ~123.45, got {row[1]}"


def test_sql_variant_float(cursor, variant_test_table):
    """Test sql_variant with FLOAT base type returns Python float"""
    cursor.execute(f"SELECT id, variant_col, base_type FROM {variant_test_table} WHERE id = 6")
    row = cursor.fetchone()

    assert row is not None
    assert isinstance(row[1], float), f"FLOAT should return Python float, got {type(row[1])}"
    assert row[1] == pytest.approx(123.456789, rel=1e-7), f"Expected ~123.456789, got {row[1]}"


# ============================================================================
# Tests for Exact Numeric Types
# ============================================================================


def test_sql_variant_decimal(cursor, variant_test_table):
    """Test sql_variant with DECIMAL base type returns Python Decimal"""
    cursor.execute(f"SELECT id, variant_col, base_type FROM {variant_test_table} WHERE id = 7")
    row = cursor.fetchone()

    assert row is not None
    assert isinstance(
        row[1], decimal.Decimal
    ), f"DECIMAL should return Python Decimal, got {type(row[1])}"
    assert float(row[1]) == pytest.approx(999.99, rel=1e-5), f"Expected ~999.99, got {row[1]}"


def test_sql_variant_numeric(cursor, variant_test_table):
    """Test sql_variant with NUMERIC base type returns Python Decimal"""
    cursor.execute(f"SELECT id, variant_col, base_type FROM {variant_test_table} WHERE id = 8")
    row = cursor.fetchone()

    assert row is not None
    assert isinstance(
        row[1], decimal.Decimal
    ), f"NUMERIC should return Python Decimal, got {type(row[1])}"
    assert float(row[1]) == pytest.approx(888.88, rel=1e-5), f"Expected ~888.88, got {row[1]}"


# ============================================================================
# Tests for Bit Type
# ============================================================================


def test_sql_variant_bit_true(cursor, variant_test_table):
    """Test sql_variant with BIT base type (TRUE) returns Python bool/int"""
    cursor.execute(f"SELECT id, variant_col, base_type FROM {variant_test_table} WHERE id = 9")
    row = cursor.fetchone()

    assert row is not None
    # BIT can be returned as bool or int depending on driver
    assert row[1] in [True, 1], f"BIT(1) should return True or 1, got {row[1]}"


def test_sql_variant_bit_false(cursor, variant_test_table):
    """Test sql_variant with BIT base type (FALSE) returns Python bool/int"""
    cursor.execute(f"SELECT id, variant_col, base_type FROM {variant_test_table} WHERE id = 10")
    row = cursor.fetchone()

    assert row is not None
    # BIT can be returned as bool or int depending on driver
    assert row[1] in [False, 0], f"BIT(0) should return False or 0, got {row[1]}"


# ============================================================================
# Tests for Character Types
# ============================================================================


def test_sql_variant_varchar(cursor, variant_test_table):
    """Test sql_variant with VARCHAR base type returns Python str"""
    cursor.execute(f"SELECT id, variant_col, base_type FROM {variant_test_table} WHERE id = 11")
    row = cursor.fetchone()

    assert row is not None
    assert isinstance(row[1], str), f"VARCHAR should return Python str, got {type(row[1])}"
    assert row[1] == "Hello", f"Expected 'Hello', got '{row[1]}'"


def test_sql_variant_nvarchar(cursor, variant_test_table):
    """Test sql_variant with NVARCHAR base type returns Python str"""
    cursor.execute(f"SELECT id, variant_col, base_type FROM {variant_test_table} WHERE id = 12")
    row = cursor.fetchone()

    assert row is not None
    assert isinstance(row[1], str), f"NVARCHAR should return Python str, got {type(row[1])}"
    assert row[1] == "World", f"Expected 'World', got '{row[1]}'"


def test_sql_variant_char(cursor, variant_test_table):
    """Test sql_variant with CHAR base type returns Python str"""
    cursor.execute(f"SELECT id, variant_col, base_type FROM {variant_test_table} WHERE id = 13")
    row = cursor.fetchone()

    assert row is not None
    assert isinstance(row[1], str), f"CHAR should return Python str, got {type(row[1])}"
    # CHAR(10) pads with spaces, so strip for comparison
    assert row[1].strip() == "Fixed", f"Expected 'Fixed', got '{row[1]}'"


def test_sql_variant_nchar(cursor, variant_test_table):
    """Test sql_variant with NCHAR base type returns Python str"""
    cursor.execute(f"SELECT id, variant_col, base_type FROM {variant_test_table} WHERE id = 14")
    row = cursor.fetchone()

    assert row is not None
    assert isinstance(row[1], str), f"NCHAR should return Python str, got {type(row[1])}"
    # NCHAR(10) pads with spaces, so strip for comparison
    assert row[1].strip() == "Fixed", f"Expected 'Fixed', got '{row[1]}'"


# ============================================================================
# Tests for Date/Time Types
# ============================================================================


def test_sql_variant_date(cursor, variant_test_table):
    """Test sql_variant with DATE base type returns Python date"""
    cursor.execute(f"SELECT id, variant_col, base_type FROM {variant_test_table} WHERE id = 15")
    row = cursor.fetchone()

    assert row is not None
    assert isinstance(row[1], date), f"DATE should return Python date, got {type(row[1])}"
    assert row[1].year == 2024 and row[1].month == 5 and row[1].day == 20


def test_sql_variant_time(cursor, variant_test_table):
    """Test sql_variant with TIME base type returns Python time"""
    cursor.execute(f"SELECT id, variant_col, base_type FROM {variant_test_table} WHERE id = 16")
    row = cursor.fetchone()

    assert row is not None
    assert isinstance(row[1], time), f"TIME should return Python time, got {type(row[1])}"
    assert row[1].hour == 12 and row[1].minute == 34 and row[1].second == 56


def test_sql_variant_datetime(cursor, variant_test_table):
    """Test sql_variant with DATETIME base type returns Python datetime"""
    cursor.execute(f"SELECT id, variant_col, base_type FROM {variant_test_table} WHERE id = 17")
    row = cursor.fetchone()

    assert row is not None
    assert isinstance(
        row[1], datetime
    ), f"DATETIME should return Python datetime, got {type(row[1])}"
    assert row[1].year == 2024 and row[1].month == 5 and row[1].day == 20


def test_sql_variant_datetime2(cursor, variant_test_table):
    """Test sql_variant with DATETIME2 base type returns Python datetime"""
    cursor.execute(f"SELECT id, variant_col, base_type FROM {variant_test_table} WHERE id = 19")
    row = cursor.fetchone()

    assert row is not None
    assert isinstance(
        row[1], datetime
    ), f"DATETIME2 should return Python datetime, got {type(row[1])}"
    assert row[1].year == 2024 and row[1].month == 5 and row[1].day == 20


def test_sql_variant_smalldatetime(cursor, variant_test_table):
    """Test sql_variant with SMALLDATETIME base type returns Python datetime"""
    cursor.execute(f"SELECT id, variant_col, base_type FROM {variant_test_table} WHERE id = 18")
    row = cursor.fetchone()

    assert row is not None
    assert isinstance(
        row[1], datetime
    ), f"SMALLDATETIME should return Python datetime, got {type(row[1])}"
    assert row[1].year == 2024 and row[1].month == 5 and row[1].day == 20
    # SMALLDATETIME has minute precision, seconds should be 0
    assert row[1].hour == 12 and row[1].minute == 34


# ============================================================================
# Tests for Binary and GUID Types
# ============================================================================


def test_sql_variant_binary(cursor, variant_test_table):
    """Test sql_variant with BINARY base type returns Python bytes"""
    cursor.execute(f"SELECT id, variant_col, base_type FROM {variant_test_table} WHERE id = 20")
    row = cursor.fetchone()

    assert row is not None
    assert isinstance(
        row[1], (bytes, bytearray)
    ), f"BINARY should return Python bytes, got {type(row[1])}"
    # BINARY(10) pads with zeros, so check prefix
    assert row[1][:5] == b"Hello", f"Expected b'Hello' prefix, got {row[1][:5]}"
    assert len(row[1]) == 10, f"BINARY(10) should be 10 bytes, got {len(row[1])}"


def test_sql_variant_varbinary(cursor, variant_test_table):
    """Test sql_variant with VARBINARY base type returns Python bytes"""
    cursor.execute(f"SELECT id, variant_col, base_type FROM {variant_test_table} WHERE id = 21")
    row = cursor.fetchone()

    assert row is not None
    assert isinstance(
        row[1], (bytes, bytearray)
    ), f"VARBINARY should return Python bytes, got {type(row[1])}"
    # 0x48656C6C6F = "Hello" in ASCII
    assert row[1] == b"Hello", f"Expected b'Hello', got {row[1]}"


def test_sql_variant_uniqueidentifier(cursor, variant_test_table):
    """Test sql_variant with UNIQUEIDENTIFIER base type returns UUID-compatible type"""
    cursor.execute(f"SELECT id, variant_col, base_type FROM {variant_test_table} WHERE id = 22")
    row = cursor.fetchone()

    assert row is not None
    # GUID can be returned as string or uuid.UUID object
    if isinstance(row[1], str):
        # Verify it's a valid GUID format
        uuid.UUID(row[1])  # This will raise ValueError if invalid
    elif isinstance(row[1], uuid.UUID):
        # Already a UUID object, that's fine
        pass
    else:
        pytest.fail(f"UNIQUEIDENTIFIER should return str or UUID, got {type(row[1])}")


# ============================================================================
# Tests for NULL
# ============================================================================


def test_sql_variant_null(cursor, variant_test_table):
    """Test sql_variant with NULL value returns Python None"""
    cursor.execute(f"SELECT id, variant_col, base_type FROM {variant_test_table} WHERE id = 23")
    row = cursor.fetchone()

    assert row is not None
    assert row[1] is None, f"NULL should return Python None, got {row[1]} (type: {type(row[1])})"


# ============================================================================
# Tests for Implicit Type Conversion (SQL Server's type choices)
# ============================================================================


def test_sql_variant_implicit_int(cursor, variant_test_table):
    """Test that integer literal without CAST is stored as INT"""
    cursor.execute(f"SELECT id, variant_col, base_type FROM {variant_test_table} WHERE id = 24")
    row = cursor.fetchone()

    assert row is not None
    assert row[1] == 123
    assert isinstance(
        row[1], int
    ), f"Implicit int literal should return Python int, got {type(row[1])}"
    assert row[2] == "int", "SQL Server should store integer literal as INT"


def test_sql_variant_implicit_decimal(cursor, variant_test_table):
    """Test that decimal literal without CAST is stored as NUMERIC"""
    cursor.execute(f"SELECT id, variant_col, base_type FROM {variant_test_table} WHERE id = 25")
    row = cursor.fetchone()

    assert row is not None
    assert isinstance(
        row[1], decimal.Decimal
    ), f"Implicit decimal literal should return Decimal, got {type(row[1])}"
    assert float(row[1]) == pytest.approx(45.67, rel=1e-5)
    assert row[2] == "numeric", "SQL Server should store decimal literal as NUMERIC"


def test_sql_variant_implicit_nvarchar(cursor, variant_test_table):
    """Test that string literal with N prefix is stored as NVARCHAR"""
    cursor.execute(f"SELECT id, variant_col, base_type FROM {variant_test_table} WHERE id = 26")
    row = cursor.fetchone()

    assert row is not None
    assert isinstance(
        row[1], str
    ), f"Implicit nvarchar literal should return str, got {type(row[1])}"
    assert row[1] == "Test"
    assert row[2] == "nvarchar", "SQL Server should store N-prefixed literal as NVARCHAR"


# ============================================================================
# Tests for fetchmany() and fetchall()
# ============================================================================


def test_sql_variant_fetchmany_mixed_types(cursor, variant_test_table):
    """Test sql_variant with fetchmany() returns correct native types"""
    cursor.execute(
        f"SELECT id, variant_col, base_type FROM {variant_test_table} WHERE id IN (1, 5, 7, 9) ORDER BY id"
    )
    rows = cursor.fetchmany(4)

    assert len(rows) == 4
    # INT → int
    assert isinstance(rows[0][1], int) and rows[0][1] == 123
    # REAL → float
    assert isinstance(rows[1][1], float) and rows[1][1] == pytest.approx(123.45, rel=1e-5)
    # DECIMAL → Decimal
    assert isinstance(rows[2][1], decimal.Decimal) and float(rows[2][1]) == pytest.approx(
        999.99, rel=1e-5
    )
    # BIT → bool/int
    assert rows[3][1] in [True, 1]


def test_sql_variant_fetchall_all_base_types(cursor, variant_test_table):
    """Test sql_variant with fetchall() validates all SQL base types"""
    cursor.execute(
        f"SELECT id, variant_col, base_type FROM {variant_test_table} WHERE id <= 23 ORDER BY id"
    )
    rows = cursor.fetchall()

    assert len(rows) == 23

    # Integer types (INT, TINYINT, SMALLINT, BIGINT)
    assert isinstance(rows[0][1], int)  # INT
    assert isinstance(rows[1][1], int)  # TINYINT
    assert isinstance(rows[2][1], int)  # SMALLINT
    assert isinstance(rows[3][1], int)  # BIGINT

    # Float types (REAL, FLOAT)
    assert isinstance(rows[4][1], float)  # REAL
    assert isinstance(rows[5][1], float)  # FLOAT

    # Exact numeric (DECIMAL, NUMERIC)
    assert isinstance(rows[6][1], decimal.Decimal)  # DECIMAL
    assert isinstance(rows[7][1], decimal.Decimal)  # NUMERIC

    # BIT
    assert rows[8][1] in [True, 1, False, 0]  # BIT true
    assert rows[9][1] in [True, 1, False, 0]  # BIT false

    # Character types (VARCHAR, NVARCHAR, CHAR, NCHAR)
    assert isinstance(rows[10][1], str)  # VARCHAR
    assert isinstance(rows[11][1], str)  # NVARCHAR
    assert isinstance(rows[12][1], str)  # CHAR
    assert isinstance(rows[13][1], str)  # NCHAR

    # Date/time types
    assert isinstance(rows[14][1], date)  # DATE
    assert isinstance(rows[15][1], time)  # TIME
    assert isinstance(rows[16][1], datetime)  # DATETIME
    assert isinstance(rows[17][1], datetime)  # SMALLDATETIME
    assert isinstance(rows[18][1], datetime)  # DATETIME2

    # Binary and GUID
    assert isinstance(rows[19][1], (bytes, bytearray))  # BINARY
    assert isinstance(rows[20][1], (bytes, bytearray))  # VARBINARY
    # GUID can be str or UUID
    assert isinstance(rows[21][1], (str, uuid.UUID))  # UNIQUEIDENTIFIER

    # NULL
    assert rows[22][1] is None  # NULL


def test_sql_variant_large_dataset(cursor, db_connection):
    """Test sql_variant with larger dataset using explicit CAST"""
    table_name = "#pytest_sql_variant_large"
    drop_table_if_exists(cursor, table_name)

    cursor.execute(f"""
        CREATE TABLE {table_name} (
            id INT PRIMARY KEY,
            variant_col SQL_VARIANT
        )
    """)
    db_connection.commit()

    # Insert 100 rows with explicit CAST for each type
    for i in range(1, 101):
        if i % 4 == 1:
            cursor.execute(f"INSERT INTO {table_name} VALUES ({i}, CAST({i} AS INT))")
        elif i % 4 == 2:
            cursor.execute(
                f"INSERT INTO {table_name} VALUES ({i}, CAST(N'String_{i}' AS NVARCHAR(50)))"
            )
        elif i % 4 == 3:
            cursor.execute(
                f"INSERT INTO {table_name} VALUES ({i}, CAST({float(i) * 1.5} AS FLOAT))"
            )
        else:
            cursor.execute(f"INSERT INTO {table_name} VALUES ({i}, NULL)")

    db_connection.commit()

    # Fetch all with fetchall
    cursor.execute(f"SELECT id, variant_col FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()

    assert len(rows) == 100

    # Verify type patterns with explicit CAST
    assert isinstance(rows[0][1], int), "CAST AS INT should return int"
    assert isinstance(rows[1][1], str), "CAST AS NVARCHAR should return str"
    assert isinstance(rows[2][1], float), "CAST AS FLOAT should return float"
    assert rows[3][1] is None, "NULL should return None"

    # Verify last few rows follow the pattern
    assert isinstance(rows[96][1], int)
    assert isinstance(rows[97][1], str)
    assert isinstance(rows[98][1], float)
    assert rows[99][1] is None

    # Cleanup
    drop_table_if_exists(cursor, table_name)
    db_connection.commit()
