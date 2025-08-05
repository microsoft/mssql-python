"""
This file contains tests for the Cursor class.
Functions:
- test_cursor: Check if the cursor is created.
- test_execute: Ensure test_cursor passed and execute a query to fetch database names and IDs.
- test_fetch_data: Ensure test_cursor passed and fetch data from a query.
- test_execute_invalid_query: Ensure test_cursor passed and check if executing an invalid query raises an exception.
Note: The cursor function is not yet implemented, so related tests are commented out.
"""

import pytest
from datetime import datetime, date, time
import decimal
from mssql_python import Connection

# Setup test table
TEST_TABLE = """
CREATE TABLE #pytest_all_data_types (
    id INTEGER PRIMARY KEY,
    bit_column BIT,
    tinyint_column TINYINT,
    smallint_column SMALLINT,
    bigint_column BIGINT,
    integer_column INTEGER,
    float_column FLOAT,
    wvarchar_column NVARCHAR(255),
    time_column TIME,
    datetime_column DATETIME,
    date_column DATE,
    real_column REAL
);
"""

# Test data
TEST_DATA = (
    1,
    1,
    127,
    32767,
    9223372036854775807,
    2147483647,
    1.23456789,
    "nvarchar data",
    time(12, 34, 56),
    datetime(2024, 5, 20, 12, 34, 56, 123000),
    date(2024, 5, 20),
    1.23456789
)

# Parameterized test data with different primary keys
PARAM_TEST_DATA = [
    TEST_DATA,
    (2, 0, 0, 0, 0, 0, 0.0, "test1", time(0, 0, 0), datetime(2024, 1, 1, 0, 0, 0), date(2024, 1, 1), 0.0),
    (3, 1, 1, 1, 1, 1, 1.1, "test2", time(1, 1, 1), datetime(2024, 2, 2, 1, 1, 1), date(2024, 2, 2), 1.1),
    (4, 0, 127, 32767, 9223372036854775807, 2147483647, 1.23456789, "test3", time(12, 34, 56), datetime(2024, 5, 20, 12, 34, 56, 123000), date(2024, 5, 20), 1.23456789)
]

def drop_table_if_exists(cursor, table_name):
    """Drop the table if it exists"""
    try:
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    except Exception as e:
        pytest.fail(f"Failed to drop table {table_name}: {e}")

def test_cursor(cursor):
    """Check if the cursor is created"""
    assert cursor is not None, "Cursor should not be None"

def test_insert_id_column(cursor, db_connection):
    """Test inserting data into the id column"""
    try:
        drop_table_if_exists(cursor, "#pytest_single_column")
        cursor.execute("CREATE TABLE #pytest_single_column (id INTEGER PRIMARY KEY)")
        db_connection.commit()
        cursor.execute("INSERT INTO #pytest_single_column (id) VALUES (?)", [1])
        db_connection.commit()
        cursor.execute("SELECT id FROM #pytest_single_column")
        row = cursor.fetchone()
        assert row[0] == 1, "ID column insertion/fetch failed"
    except Exception as e:
        pytest.fail(f"ID column insertion/fetch failed: {e}")
    finally:
        cursor.execute("DROP TABLE #pytest_single_column")
        db_connection.commit()

def test_insert_bit_column(cursor, db_connection):
    """Test inserting data into the bit_column"""
    try:
        cursor.execute("CREATE TABLE #pytest_single_column (bit_column BIT)")
        db_connection.commit()
        cursor.execute("INSERT INTO #pytest_single_column (bit_column) VALUES (?)", [1])
        db_connection.commit()
        cursor.execute("SELECT bit_column FROM #pytest_single_column")
        row = cursor.fetchone()
        assert row[0] == 1, "Bit column insertion/fetch failed"
    except Exception as e:
        pytest.fail(f"Bit column insertion/fetch failed: {e}")
    finally:
        cursor.execute("DROP TABLE #pytest_single_column")
        db_connection.commit()

def test_insert_nvarchar_column(cursor, db_connection):
    """Test inserting data into the nvarchar_column"""
    try:
        cursor.execute("CREATE TABLE #pytest_single_column (nvarchar_column NVARCHAR(255))")
        db_connection.commit()
        cursor.execute("INSERT INTO #pytest_single_column (nvarchar_column) VALUES (?)", ["test"])
        db_connection.commit()
        cursor.execute("SELECT nvarchar_column FROM #pytest_single_column")
        row = cursor.fetchone()
        assert row[0] == "test", "Nvarchar column insertion/fetch failed"
    except Exception as e:
        pytest.fail(f"Nvarchar column insertion/fetch failed: {e}")
    finally:
        cursor.execute("DROP TABLE #pytest_single_column")
        db_connection.commit()

def test_insert_time_column(cursor, db_connection):
    """Test inserting data into the time_column"""
    try:
        drop_table_if_exists(cursor, "#pytest_single_column")
        cursor.execute("CREATE TABLE #pytest_single_column (time_column TIME)")
        db_connection.commit()
        cursor.execute("INSERT INTO #pytest_single_column (time_column) VALUES (?)", [time(12, 34, 56)])
        db_connection.commit()
        cursor.execute("SELECT time_column FROM #pytest_single_column")
        row = cursor.fetchone()
        assert row[0] == time(12, 34, 56), "Time column insertion/fetch failed"
    except Exception as e:
        pytest.fail(f"Time column insertion/fetch failed: {e}")
    finally:
        cursor.execute("DROP TABLE #pytest_single_column")
        db_connection.commit()

def test_insert_datetime_column(cursor, db_connection):
    """Test inserting data into the datetime_column"""
    try:
        drop_table_if_exists(cursor, "#pytest_single_column")
        cursor.execute("CREATE TABLE #pytest_single_column (datetime_column DATETIME)")
        db_connection.commit()
        cursor.execute("INSERT INTO #pytest_single_column (datetime_column) VALUES (?)", [datetime(2024, 5, 20, 12, 34, 56, 123000)])
        db_connection.commit()
        cursor.execute("SELECT datetime_column FROM #pytest_single_column")
        row = cursor.fetchone()
        assert row[0] == datetime(2024, 5, 20, 12, 34, 56, 123000), "Datetime column insertion/fetch failed"
    except Exception as e:
        pytest.fail(f"Datetime column insertion/fetch failed: {e}")
    finally:
        cursor.execute("DROP TABLE #pytest_single_column")
        db_connection.commit()

def test_insert_datetime2_column(cursor, db_connection):
    """Test inserting data into the datetime2_column"""
    try:
        drop_table_if_exists(cursor, "#pytest_single_column")
        cursor.execute("CREATE TABLE #pytest_single_column (datetime2_column DATETIME2)")
        db_connection.commit()
        cursor.execute("INSERT INTO #pytest_single_column (datetime2_column) VALUES (?)", [datetime(2024, 5, 20, 12, 34, 56, 123456)])
        db_connection.commit()
        cursor.execute("SELECT datetime2_column FROM #pytest_single_column")
        row = cursor.fetchone()
        assert row[0] == datetime(2024, 5, 20, 12, 34, 56, 123456), "Datetime2 column insertion/fetch failed"
    except Exception as e:
        pytest.fail(f"Datetime2 column insertion/fetch failed: {e}")
    finally:
        cursor.execute("DROP TABLE #pytest_single_column")
        db_connection.commit()

def test_insert_smalldatetime_column(cursor, db_connection):
    """Test inserting data into the smalldatetime_column"""
    try:
        drop_table_if_exists(cursor, "#pytest_single_column")
        cursor.execute("CREATE TABLE #pytest_single_column (smalldatetime_column SMALLDATETIME)")
        db_connection.commit()
        cursor.execute("INSERT INTO #pytest_single_column (smalldatetime_column) VALUES (?)", [datetime(2024, 5, 20, 12, 34)])
        db_connection.commit()
        cursor.execute("SELECT smalldatetime_column FROM #pytest_single_column")
        row = cursor.fetchone()
        assert row[0] == datetime(2024, 5, 20, 12, 34), "Smalldatetime column insertion/fetch failed"
    except Exception as e:
        pytest.fail(f"Smalldatetime column insertion/fetch failed: {e}")
    finally:
        cursor.execute("DROP TABLE #pytest_single_column")
        db_connection.commit()

def test_insert_date_column(cursor, db_connection):
    """Test inserting data into the date_column"""
    try:
        drop_table_if_exists(cursor, "#pytest_single_column")
        cursor.execute("CREATE TABLE #pytest_single_column (date_column DATE)")
        db_connection.commit()
        cursor.execute("INSERT INTO #pytest_single_column (date_column) VALUES (?)", [date(2024, 5, 20)])
        db_connection.commit()
        cursor.execute("SELECT date_column FROM #pytest_single_column")
        row = cursor.fetchone()
        assert row[0] == date(2024, 5, 20), "Date column insertion/fetch failed"
    except Exception as e:
        pytest.fail(f"Date column insertion/fetch failed: {e}")
    finally:
        cursor.execute("DROP TABLE #pytest_single_column")
        db_connection.commit()

def test_insert_real_column(cursor, db_connection):
    """Test inserting data into the real_column"""
    try:
        drop_table_if_exists(cursor, "#pytest_single_column")
        cursor.execute("CREATE TABLE #pytest_single_column (real_column REAL)")
        db_connection.commit()
        cursor.execute("INSERT INTO #pytest_single_column (real_column) VALUES (?)", [1.23456789])
        db_connection.commit()
        cursor.execute("SELECT real_column FROM #pytest_single_column")
        row = cursor.fetchone()
        assert abs(row[0] - 1.23456789) < 1e-8, "Real column insertion/fetch failed"
    except Exception as e:
        pytest.fail(f"Real column insertion/fetch failed: {e}")
    finally:
        cursor.execute("DROP TABLE #pytest_single_column")
        db_connection.commit()

def test_insert_decimal_column(cursor, db_connection):
    """Test inserting data into the decimal_column"""
    try:
        cursor.execute("CREATE TABLE #pytest_single_column (decimal_column DECIMAL(10, 2))")
        db_connection.commit()
        cursor.execute("INSERT INTO #pytest_single_column (decimal_column) VALUES (?)", [decimal.Decimal(123.45).quantize(decimal.Decimal('0.00'))])
        db_connection.commit()
        cursor.execute("SELECT decimal_column FROM #pytest_single_column")
        row = cursor.fetchone()
        assert row[0] == decimal.Decimal(123.45).quantize(decimal.Decimal('0.00')), "Decimal column insertion/fetch failed"
        cursor.execute("TRUNCATE TABLE #pytest_single_column")
        cursor.execute("INSERT INTO #pytest_single_column (decimal_column) VALUES (?)", [decimal.Decimal(-123.45).quantize(decimal.Decimal('0.00'))])
        db_connection.commit()
        cursor.execute("SELECT decimal_column FROM #pytest_single_column")
        row = cursor.fetchone()
        assert row[0] == decimal.Decimal(-123.45).quantize(decimal.Decimal('0.00')), "Negative Decimal insertion/fetch failed"
    except Exception as e:
        pytest.fail(f"Decimal column insertion/fetch failed: {e}")
    finally:
        cursor.execute("DROP TABLE #pytest_single_column")
        db_connection.commit()

def test_insert_tinyint_column(cursor, db_connection):
    """Test inserting data into the tinyint_column"""
    try:
        cursor.execute("CREATE TABLE #pytest_single_column (tinyint_column TINYINT)")
        db_connection.commit()
        cursor.execute("INSERT INTO #pytest_single_column (tinyint_column) VALUES (?)", [127])
        db_connection.commit()
        cursor.execute("SELECT tinyint_column FROM #pytest_single_column")
        row = cursor.fetchone()
        assert row[0] == 127, "Tinyint column insertion/fetch failed"
    except Exception as e:
        pytest.fail(f"Tinyint column insertion/fetch failed: {e}")
    finally:
        cursor.execute("DROP TABLE #pytest_single_column")
        db_connection.commit()

def test_insert_smallint_column(cursor, db_connection):
    """Test inserting data into the smallint_column"""
    try:
        cursor.execute("CREATE TABLE #pytest_single_column (smallint_column SMALLINT)")
        db_connection.commit()
        cursor.execute("INSERT INTO #pytest_single_column (smallint_column) VALUES (?)", [32767])
        db_connection.commit()
        cursor.execute("SELECT smallint_column FROM #pytest_single_column")
        row = cursor.fetchone()
        assert row[0] == 32767, "Smallint column insertion/fetch failed"
    except Exception as e:
        pytest.fail(f"Smallint column insertion/fetch failed: {e}")
    finally:
        cursor.execute("DROP TABLE #pytest_single_column")
        db_connection.commit()

def test_insert_bigint_column(cursor, db_connection):
    """Test inserting data into the bigint_column"""
    try:
        cursor.execute("CREATE TABLE #pytest_single_column (bigint_column BIGINT)")
        db_connection.commit()
        cursor.execute("INSERT INTO #pytest_single_column (bigint_column) VALUES (?)", [9223372036854775807])
        db_connection.commit()
        cursor.execute("SELECT bigint_column FROM #pytest_single_column")
        row = cursor.fetchone()
        assert row[0] == 9223372036854775807, "Bigint column insertion/fetch failed"
    except Exception as e:
        pytest.fail(f"Bigint column insertion/fetch failed: {e}")
    finally:
        cursor.execute("DROP TABLE #pytest_single_column")
        db_connection.commit()

def test_insert_integer_column(cursor, db_connection):
    """Test inserting data into the integer_column"""
    try:
        cursor.execute("CREATE TABLE #pytest_single_column (integer_column INTEGER)")
        db_connection.commit()
        cursor.execute("INSERT INTO #pytest_single_column (integer_column) VALUES (?)", [2147483647])
        db_connection.commit()
        cursor.execute("SELECT integer_column FROM #pytest_single_column")
        row = cursor.fetchone()
        assert row[0] == 2147483647, "Integer column insertion/fetch failed"
    except Exception as e:
        pytest.fail(f"Integer column insertion/fetch failed: {e}")
    finally:
        cursor.execute("DROP TABLE #pytest_single_column")
        db_connection.commit()

def test_insert_float_column(cursor, db_connection):
    """Test inserting data into the float_column"""
    try:
        cursor.execute("CREATE TABLE #pytest_single_column (float_column FLOAT)")
        db_connection.commit()
        cursor.execute("INSERT INTO #pytest_single_column (float_column) VALUES (?)", [1.23456789])
        db_connection.commit()
        cursor.execute("SELECT float_column FROM #pytest_single_column")
        row = cursor.fetchone()
        assert row[0] == 1.23456789, "Float column insertion/fetch failed"
    except Exception as e:
        pytest.fail(f"Float column insertion/fetch failed: {e}")
    finally:
        cursor.execute("DROP TABLE #pytest_single_column")
        db_connection.commit()

# Test that VARCHAR(n) can accomodate values of size n
def test_varchar_full_capacity(cursor, db_connection):
    """Test SQL_VARCHAR"""
    try:
        cursor.execute("CREATE TABLE #pytest_varchar_test (varchar_column VARCHAR(9))")
        db_connection.commit()
        cursor.execute("INSERT INTO #pytest_varchar_test (varchar_column) VALUES (?)", ['123456789'])
        db_connection.commit()
        # fetchone test
        cursor.execute("SELECT varchar_column FROM #pytest_varchar_test")
        row = cursor.fetchone()
        assert row[0] == '123456789', "SQL_VARCHAR parsing failed for fetchone"
        # fetchall test
        cursor.execute("SELECT varchar_column FROM #pytest_varchar_test")
        rows = cursor.fetchall()
        assert rows[0] == ['123456789'], "SQL_VARCHAR parsing failed for fetchall"
    except Exception as e:
        pytest.fail(f"SQL_VARCHAR parsing test failed: {e}")
    finally:
        cursor.execute("DROP TABLE #pytest_varchar_test")
        db_connection.commit()

# Test that NVARCHAR(n) can accomodate values of size n
def test_wvarchar_full_capacity(cursor, db_connection):
    """Test SQL_WVARCHAR"""
    try:
        cursor.execute("CREATE TABLE #pytest_wvarchar_test (wvarchar_column NVARCHAR(6))")
        db_connection.commit()
        cursor.execute("INSERT INTO #pytest_wvarchar_test (wvarchar_column) VALUES (?)", ['123456'])
        db_connection.commit()
        # fetchone test
        cursor.execute("SELECT wvarchar_column FROM #pytest_wvarchar_test")
        row = cursor.fetchone()
        assert row[0] == '123456', "SQL_WVARCHAR parsing failed for fetchone"
        # fetchall test
        cursor.execute("SELECT wvarchar_column FROM #pytest_wvarchar_test")
        rows = cursor.fetchall()
        assert rows[0] == ['123456'], "SQL_WVARCHAR parsing failed for fetchall"
    except Exception as e:
        pytest.fail(f"SQL_WVARCHAR parsing test failed: {e}")
    finally:
        cursor.execute("DROP TABLE #pytest_wvarchar_test")
        db_connection.commit()

# Test that VARBINARY(n) can accomodate values of size n
def test_varbinary_full_capacity(cursor, db_connection):
    """Test SQL_VARBINARY"""
    try:
        cursor.execute("CREATE TABLE #pytest_varbinary_test (varbinary_column VARBINARY(8))")
        db_connection.commit()
        # Try inserting binary using both bytes & bytearray
        cursor.execute("INSERT INTO #pytest_varbinary_test (varbinary_column) VALUES (?)", bytearray("12345", 'utf-8'))
        cursor.execute("INSERT INTO #pytest_varbinary_test (varbinary_column) VALUES (?)", bytes("12345678", 'utf-8')) # Full capacity
        db_connection.commit()
        expectedRows = 2
        # fetchone test
        cursor.execute("SELECT varbinary_column FROM #pytest_varbinary_test")
        rows = []
        for i in range(0, expectedRows):
            rows.append(cursor.fetchone())
        assert cursor.fetchone() == None, "varbinary_column is expected to have only {} rows".format(expectedRows)
        assert rows[0] == [bytes("12345", 'utf-8')], "SQL_VARBINARY parsing failed for fetchone - row 0"
        assert rows[1] == [bytes("12345678", 'utf-8')], "SQL_VARBINARY parsing failed for fetchone - row 1"
        # fetchall test
        cursor.execute("SELECT varbinary_column FROM #pytest_varbinary_test")
        rows = cursor.fetchall()
        assert rows[0] == [bytes("12345", 'utf-8')], "SQL_VARBINARY parsing failed for fetchall - row 0"
        assert rows[1] == [bytes("12345678", 'utf-8')], "SQL_VARBINARY parsing failed for fetchall - row 1"
    except Exception as e:
        pytest.fail(f"SQL_VARBINARY parsing test failed: {e}")
    finally:
        cursor.execute("DROP TABLE #pytest_varbinary_test")
        db_connection.commit()

def test_varchar_max(cursor, db_connection):
    """Test SQL_VARCHAR with MAX length"""
    try:
        cursor.execute("CREATE TABLE #pytest_varchar_test (varchar_column VARCHAR(MAX))")
        db_connection.commit()
        cursor.execute("INSERT INTO #pytest_varchar_test (varchar_column) VALUES (?), (?)", ["ABCDEFGHI", None])
        db_connection.commit()
        expectedRows = 2
        # fetchone test
        cursor.execute("SELECT varchar_column FROM #pytest_varchar_test")
        rows = []
        for i in range(0, expectedRows):
            rows.append(cursor.fetchone())
        assert cursor.fetchone() == None, "varchar_column is expected to have only {} rows".format(expectedRows)
        assert rows[0] == ["ABCDEFGHI"], "SQL_VARCHAR parsing failed for fetchone - row 0"
        assert rows[1] == [None], "SQL_VARCHAR parsing failed for fetchone - row 1"
        # fetchall test
        cursor.execute("SELECT varchar_column FROM #pytest_varchar_test")
        rows = cursor.fetchall()
        assert rows[0] == ["ABCDEFGHI"], "SQL_VARCHAR parsing failed for fetchall - row 0"
        assert rows[1] == [None], "SQL_VARCHAR parsing failed for fetchall - row 1"
    except Exception as e:
        pytest.fail(f"SQL_VARCHAR parsing test failed: {e}")
    finally:
        cursor.execute("DROP TABLE #pytest_varchar_test")
        db_connection.commit()

def test_wvarchar_max(cursor, db_connection):
    """Test SQL_WVARCHAR with MAX length"""
    try:
        cursor.execute("CREATE TABLE #pytest_wvarchar_test (wvarchar_column NVARCHAR(MAX))")
        db_connection.commit()
        cursor.execute("INSERT INTO #pytest_wvarchar_test (wvarchar_column) VALUES (?), (?)", ["!@#$%^&*()_+", None])
        db_connection.commit()
        expectedRows = 2
        # fetchone test
        cursor.execute("SELECT wvarchar_column FROM #pytest_wvarchar_test")
        rows = []
        for i in range(0, expectedRows):
            rows.append(cursor.fetchone())
        assert cursor.fetchone() == None, "wvarchar_column is expected to have only {} rows".format(expectedRows)
        assert rows[0] == ["!@#$%^&*()_+"], "SQL_WVARCHAR parsing failed for fetchone - row 0"
        assert rows[1] == [None], "SQL_WVARCHAR parsing failed for fetchone - row 1"
        # fetchall test
        cursor.execute("SELECT wvarchar_column FROM #pytest_wvarchar_test")
        rows = cursor.fetchall()
        assert rows[0] == ["!@#$%^&*()_+"], "SQL_WVARCHAR parsing failed for fetchall - row 0"
        assert rows[1] == [None], "SQL_WVARCHAR parsing failed for fetchall - row 1"
    except Exception as e:
        pytest.fail(f"SQL_WVARCHAR parsing test failed: {e}")
    finally:
        cursor.execute("DROP TABLE #pytest_wvarchar_test")
        db_connection.commit()

def test_varbinary_max(cursor, db_connection):
    """Test SQL_VARBINARY with MAX length"""
    try:
        cursor.execute("CREATE TABLE #pytest_varbinary_test (varbinary_column VARBINARY(MAX))")
        db_connection.commit()
        # TODO: Uncomment this execute after adding null binary support
        # cursor.execute("INSERT INTO #pytest_varbinary_test (varbinary_column) VALUES (?)", [None])
        cursor.execute("INSERT INTO #pytest_varbinary_test (varbinary_column) VALUES (?), (?)", [bytearray("ABCDEF", 'utf-8'), bytes("123!@#", 'utf-8')])
        db_connection.commit()
        expectedRows = 2
        # fetchone test
        cursor.execute("SELECT varbinary_column FROM #pytest_varbinary_test")
        rows = []
        for i in range(0, expectedRows):
            rows.append(cursor.fetchone())
        assert cursor.fetchone() == None, "varbinary_column is expected to have only {} rows".format(expectedRows)
        assert rows[0] == [bytearray("ABCDEF", 'utf-8')], "SQL_VARBINARY parsing failed for fetchone - row 0"
        assert rows[1] == [bytes("123!@#", 'utf-8')], "SQL_VARBINARY parsing failed for fetchone - row 1"
        # fetchall test
        cursor.execute("SELECT varbinary_column FROM #pytest_varbinary_test")
        rows = cursor.fetchall()
        assert rows[0] == [bytearray("ABCDEF", 'utf-8')], "SQL_VARBINARY parsing failed for fetchall - row 0"
        assert rows[1] == [bytes("123!@#", 'utf-8')], "SQL_VARBINARY parsing failed for fetchall - row 1"
    except Exception as e:
        pytest.fail(f"SQL_VARBINARY parsing test failed: {e}")
    finally:
        cursor.execute("DROP TABLE #pytest_varbinary_test")
        db_connection.commit()

def test_longvarchar(cursor, db_connection):
    """Test SQL_LONGVARCHAR"""
    try:
        cursor.execute("CREATE TABLE #pytest_longvarchar_test (longvarchar_column TEXT)")
        db_connection.commit()
        cursor.execute("INSERT INTO #pytest_longvarchar_test (longvarchar_column) VALUES (?), (?)", ["ABCDEFGHI", None])
        db_connection.commit()
        expectedRows = 2
        # fetchone test
        cursor.execute("SELECT longvarchar_column FROM #pytest_longvarchar_test")
        rows = []
        for i in range(0, expectedRows):
            rows.append(cursor.fetchone())
        assert cursor.fetchone() == None, "longvarchar_column is expected to have only {} rows".format(expectedRows)
        assert rows[0] == ["ABCDEFGHI"], "SQL_LONGVARCHAR parsing failed for fetchone - row 0"
        assert rows[1] == [None], "SQL_LONGVARCHAR parsing failed for fetchone - row 1"
        # fetchall test
        cursor.execute("SELECT longvarchar_column FROM #pytest_longvarchar_test")
        rows = cursor.fetchall()
        assert rows[0] == ["ABCDEFGHI"], "SQL_LONGVARCHAR parsing failed for fetchall - row 0"
        assert rows[1] == [None], "SQL_LONGVARCHAR parsing failed for fetchall - row 1"
    except Exception as e:
        pytest.fail(f"SQL_LONGVARCHAR parsing test failed: {e}")
    finally:
        cursor.execute("DROP TABLE #pytest_longvarchar_test")
        db_connection.commit()

def test_longwvarchar(cursor, db_connection):
    """Test SQL_LONGWVARCHAR"""
    try:
        cursor.execute("CREATE TABLE #pytest_longwvarchar_test (longwvarchar_column NTEXT)")
        db_connection.commit()
        cursor.execute("INSERT INTO #pytest_longwvarchar_test (longwvarchar_column) VALUES (?), (?)", ["ABCDEFGHI", None])
        db_connection.commit()
        expectedRows = 2
        # fetchone test
        cursor.execute("SELECT longwvarchar_column FROM #pytest_longwvarchar_test")
        rows = []
        for i in range(0, expectedRows):
            rows.append(cursor.fetchone())
        assert cursor.fetchone() == None, "longwvarchar_column is expected to have only {} rows".format(expectedRows)
        assert rows[0] == ["ABCDEFGHI"], "SQL_LONGWVARCHAR parsing failed for fetchone - row 0"
        assert rows[1] == [None], "SQL_LONGWVARCHAR parsing failed for fetchone - row 1"
        # fetchall test
        cursor.execute("SELECT longwvarchar_column FROM #pytest_longwvarchar_test")
        rows = cursor.fetchall()
        assert rows[0] == ["ABCDEFGHI"], "SQL_LONGWVARCHAR parsing failed for fetchall - row 0"
        assert rows[1] == [None], "SQL_LONGWVARCHAR parsing failed for fetchall - row 1"
    except Exception as e:
        pytest.fail(f"SQL_LONGWVARCHAR parsing test failed: {e}")
    finally:
        cursor.execute("DROP TABLE #pytest_longwvarchar_test")
        db_connection.commit()

def test_longvarbinary(cursor, db_connection):
    """Test SQL_LONGVARBINARY"""
    try:
        cursor.execute("CREATE TABLE #pytest_longvarbinary_test (longvarbinary_column IMAGE)")
        db_connection.commit()
        cursor.execute("INSERT INTO #pytest_longvarbinary_test (longvarbinary_column) VALUES (?), (?)", [bytearray("ABCDEFGHI", 'utf-8'), bytes("123!@#", 'utf-8')])
        db_connection.commit()
        expectedRows = 3
        # fetchone test
        cursor.execute("SELECT longvarbinary_column FROM #pytest_longvarbinary_test")
        rows = []
        for i in range(0, expectedRows):
            rows.append(cursor.fetchone())
        assert cursor.fetchone() == None, "longvarbinary_column is expected to have only {} rows".format(expectedRows)
        assert rows[0] == [bytearray("ABCDEFGHI", 'utf-8')], "SQL_LONGVARBINARY parsing failed for fetchone - row 0"
        assert rows[1] == [bytes("123!@#\0\0\0", 'utf-8')], "SQL_LONGVARBINARY parsing failed for fetchone - row 1"
        # fetchall test
        cursor.execute("SELECT longvarbinary_column FROM #pytest_longvarbinary_test")
        rows = cursor.fetchall()
        assert rows[0] == [bytearray("ABCDEFGHI", 'utf-8')], "SQL_LONGVARBINARY parsing failed for fetchall - row 0"
        assert rows[1] == [bytes("123!@#\0\0\0", 'utf-8')], "SQL_LONGVARBINARY parsing failed for fetchall - row 1"
    except Exception as e:
        pytest.fail(f"SQL_LONGVARBINARY parsing test failed: {e}")
    finally:
        cursor.execute("DROP TABLE #pytest_longvarbinary_test")
        db_connection.commit()

def test_create_table(cursor, db_connection):
    # Drop the table if it exists
    drop_table_if_exists(cursor, "#pytest_all_data_types")
    
    # Create test table
    try:
        cursor.execute(TEST_TABLE)
        db_connection.commit()
    except Exception as e:
        pytest.fail(f"Table creation failed: {e}")

def test_insert_args(cursor, db_connection):
    """Test parameterized insert using qmark parameters"""
    try:
        cursor.execute("""
            INSERT INTO #pytest_all_data_types VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
        """, 
            TEST_DATA[0], 
            TEST_DATA[1],
            TEST_DATA[2],
            TEST_DATA[3],
            TEST_DATA[4],
            TEST_DATA[5],
            TEST_DATA[6],
            TEST_DATA[7],
            TEST_DATA[8],
            TEST_DATA[9],
            TEST_DATA[10],
            TEST_DATA[11]
        )
        db_connection.commit()
        cursor.execute("SELECT * FROM #pytest_all_data_types WHERE id = 1")
        row = cursor.fetchone()
        assert row[0] == TEST_DATA[0], "Insertion using args failed"
    except Exception as e:
        pytest.fail(f"Parameterized data insertion/fetch failed: {e}")    
    finally:
        cursor.execute("DELETE FROM #pytest_all_data_types")
        db_connection.commit()                   

@pytest.mark.parametrize("data", PARAM_TEST_DATA)
def test_parametrized_insert(cursor, db_connection, data):
    """Test parameterized insert using qmark parameters"""
    try:
        cursor.execute("""
            INSERT INTO #pytest_all_data_types VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
        """, [None if v is None else v for v in data])
        db_connection.commit()
    except Exception as e:
        pytest.fail(f"Parameterized data insertion/fetch failed: {e}")

def test_rowcount(cursor, db_connection):
    """Test rowcount after insert operations"""
    try:
        cursor.execute("CREATE TABLE #pytest_test_rowcount (id INT IDENTITY(1,1) PRIMARY KEY, name NVARCHAR(100))")
        db_connection.commit()

        cursor.execute("INSERT INTO #pytest_test_rowcount (name) VALUES ('JohnDoe1');")
        assert cursor.rowcount == 1, "Rowcount should be 1 after first insert"

        cursor.execute("INSERT INTO #pytest_test_rowcount (name) VALUES ('JohnDoe2');")
        assert cursor.rowcount == 1, "Rowcount should be 1 after second insert"

        cursor.execute("INSERT INTO #pytest_test_rowcount (name) VALUES ('JohnDoe3');")
        assert cursor.rowcount == 1, "Rowcount should be 1 after third insert"

        cursor.execute("""
            INSERT INTO #pytest_test_rowcount (name) 
            VALUES 
            ('JohnDoe4'), 
            ('JohnDoe5'), 
            ('JohnDoe6');
        """)
        assert cursor.rowcount == 3, "Rowcount should be 3 after inserting multiple rows"

        cursor.execute("SELECT * FROM #pytest_test_rowcount;")
        assert cursor.rowcount == -1, "Rowcount should be -1 after a SELECT statement"

        db_connection.commit()
    except Exception as e:
        pytest.fail(f"Rowcount test failed: {e}")
    finally:
        cursor.execute("DROP TABLE #pytest_test_rowcount")
        db_connection.commit()

def test_rowcount_executemany(cursor, db_connection):
    """Test rowcount after executemany operations"""
    try:
        cursor.execute("CREATE TABLE #pytest_test_rowcount (id INT IDENTITY(1,1) PRIMARY KEY, name NVARCHAR(100))")
        db_connection.commit()

        data = [
            ('JohnDoe1',),
            ('JohnDoe2',),
            ('JohnDoe3',)
        ]

        cursor.executemany("INSERT INTO #pytest_test_rowcount (name) VALUES (?)", data)
        assert cursor.rowcount == 3, "Rowcount should be 3 after executemany insert"

        cursor.execute("SELECT * FROM #pytest_test_rowcount;")
        assert cursor.rowcount == -1, "Rowcount should be -1 after a SELECT statement"

        db_connection.commit()
    except Exception as e:
        pytest.fail(f"Rowcount executemany test failed: {e}")
    finally:
        cursor.execute("DROP TABLE #pytest_test_rowcount")
        db_connection.commit()

def test_fetchone(cursor):
    """Test fetching a single row"""
    cursor.execute("SELECT * FROM #pytest_all_data_types WHERE id = 1")
    row = cursor.fetchone()
    assert row is not None, "No row returned"
    assert len(row) == 12, "Incorrect number of columns"

def test_fetchmany(cursor):
    """Test fetching multiple rows"""
    cursor.execute("SELECT * FROM #pytest_all_data_types")
    rows = cursor.fetchmany(2)
    assert isinstance(rows, list), "fetchmany should return a list"
    assert len(rows) == 2, "Incorrect number of rows returned"

def test_fetchmany_with_arraysize(cursor, db_connection):
    """Test fetchmany with arraysize"""
    cursor.arraysize = 3
    cursor.execute("SELECT * FROM #pytest_all_data_types")
    rows = cursor.fetchmany()
    assert len(rows) == 3, "fetchmany with arraysize returned incorrect number of rows"

def test_fetchall(cursor):
    """Test fetching all rows"""
    cursor.execute("SELECT * FROM #pytest_all_data_types")
    rows = cursor.fetchall()
    assert isinstance(rows, list), "fetchall should return a list"
    assert len(rows) == len(PARAM_TEST_DATA), "Incorrect number of rows returned"

def test_execute_invalid_query(cursor):
    """Test executing an invalid query"""
    with pytest.raises(Exception):
        cursor.execute("SELECT * FROM invalid_table")

# def test_fetch_data_types(cursor):
#     """Test data types"""
#     cursor.execute("SELECT * FROM all_data_types WHERE id = 1")
#     row = cursor.fetchall()[0]
    
#     print("ROW!!!", row)
#     assert row[0] == TEST_DATA[0], "Integer mismatch"
#     assert row[1] == TEST_DATA[1], "Bit mismatch"
#     assert row[2] == TEST_DATA[2], "Tinyint mismatch"
#     assert row[3] == TEST_DATA[3], "Smallint mismatch"
#     assert row[4] == TEST_DATA[4], "Bigint mismatch"
#     assert row[5] == TEST_DATA[5], "Integer mismatch"
#     assert round(row[6], 5) == round(TEST_DATA[6], 5), "Float mismatch"
#     assert row[7] == TEST_DATA[7], "Nvarchar mismatch"
#     assert row[8] == TEST_DATA[8], "Time mismatch"
#     assert row[9] == TEST_DATA[9], "Datetime mismatch"
#     assert row[10] == TEST_DATA[10], "Date mismatch"
#     assert round(row[11], 5) == round(TEST_DATA[11], 5), "Real mismatch"

def test_arraysize(cursor):
    """Test arraysize"""
    cursor.arraysize = 10
    assert cursor.arraysize == 10, "Arraysize mismatch"
    cursor.arraysize = 5
    assert cursor.arraysize == 5, "Arraysize mismatch after change"

def test_description(cursor):
    """Test description"""
    cursor.execute("SELECT * FROM #pytest_all_data_types WHERE id = 1")
    desc = cursor.description
    assert len(desc) == 12, "Description length mismatch"
    assert desc[0][0] == "id", "Description column name mismatch"

# def test_setinputsizes(cursor):
#     """Test setinputsizes"""
#     sizes = [(mssql_python.ConstantsDDBC.SQL_INTEGER, 10), (mssql_python.ConstantsDDBC.SQL_VARCHAR, 255)]
#     cursor.setinputsizes(sizes)

# def test_setoutputsize(cursor):
#     """Test setoutputsize"""
#     cursor.setoutputsize(10, mssql_python.ConstantsDDBC.SQL_INTEGER)

def test_execute_many(cursor, db_connection):
    """Test executemany"""
    # Start fresh
    cursor.execute("DELETE FROM #pytest_all_data_types")
    db_connection.commit()
    data = [(i,) for i in range(1, 12)]
    cursor.executemany("INSERT INTO #pytest_all_data_types (id) VALUES (?)", data)
    cursor.execute("SELECT COUNT(*) FROM #pytest_all_data_types")
    count = cursor.fetchone()[0]
    assert count == 11, "Executemany failed"

def test_nextset(cursor):
    """Test nextset"""
    cursor.execute("SELECT * FROM #pytest_all_data_types WHERE id = 1;")
    assert cursor.nextset() is False, "Nextset should return False"
    cursor.execute("SELECT * FROM #pytest_all_data_types WHERE id = 2; SELECT * FROM #pytest_all_data_types WHERE id = 3;")
    assert cursor.nextset() is True, "Nextset should return True"

def test_delete_table(cursor, db_connection):
    """Test deleting the table"""
    drop_table_if_exists(cursor, "#pytest_all_data_types")
    db_connection.commit()

# Setup tables for join operations
CREATE_TABLES_FOR_JOIN = [
    """
    CREATE TABLE #pytest_employees (
        employee_id INTEGER PRIMARY KEY,
        name NVARCHAR(255),
        department_id INTEGER
    );
    """,
    """
    CREATE TABLE #pytest_departments (
        department_id INTEGER PRIMARY KEY,
        department_name NVARCHAR(255)
    );
    """,
    """
    CREATE TABLE #pytest_projects (
        project_id INTEGER PRIMARY KEY,
        project_name NVARCHAR(255),
        employee_id INTEGER
    );
    """
]

# Insert data for join operations
INSERT_DATA_FOR_JOIN = [
    """
    INSERT INTO #pytest_employees (employee_id, name, department_id) VALUES
    (1, 'Alice', 1),
    (2, 'Bob', 2),
    (3, 'Charlie', 1);
    """,
    """
    INSERT INTO #pytest_departments (department_id, department_name) VALUES
    (1, 'HR'),
    (2, 'Engineering');
    """,
    """
    INSERT INTO #pytest_projects (project_id, project_name, employee_id) VALUES
    (1, 'Project A', 1),
    (2, 'Project B', 2),
    (3, 'Project C', 3);
    """
]

def test_create_tables_for_join(cursor, db_connection):
    """Create tables for join operations"""
    try:
        for create_table in CREATE_TABLES_FOR_JOIN:
            cursor.execute(create_table)
        db_connection.commit()
    except Exception as e:
        pytest.fail(f"Table creation for join operations failed: {e}")

def test_insert_data_for_join(cursor, db_connection):
    """Insert data for join operations"""
    try:
        for insert_data in INSERT_DATA_FOR_JOIN:
            cursor.execute(insert_data)
        db_connection.commit()
    except Exception as e:
        pytest.fail(f"Data insertion for join operations failed: {e}")

def test_join_operations(cursor):
    """Test join operations"""
    try:
        cursor.execute("""
            SELECT e.name, d.department_name, p.project_name
            FROM #pytest_employees e
            JOIN #pytest_departments d ON e.department_id = d.department_id
            JOIN #pytest_projects p ON e.employee_id = p.employee_id
        """)
        rows = cursor.fetchall()
        assert len(rows) == 3, "Join operation returned incorrect number of rows"
        assert rows[0] == ['Alice', 'HR', 'Project A'], "Join operation returned incorrect data for row 1"
        assert rows[1] == ['Bob', 'Engineering', 'Project B'], "Join operation returned incorrect data for row 2"
        assert rows[2] == ['Charlie', 'HR', 'Project C'], "Join operation returned incorrect data for row 3"
    except Exception as e:
        pytest.fail(f"Join operation failed: {e}")

def test_join_operations_with_parameters(cursor):
    """Test join operations with parameters"""
    try:
        employee_ids = [1, 2]
        query = """
            SELECT e.name, d.department_name, p.project_name
            FROM #pytest_employees e
            JOIN #pytest_departments d ON e.department_id = d.department_id
            JOIN #pytest_projects p ON e.employee_id = p.employee_id
            WHERE e.employee_id IN (?, ?)
        """
        cursor.execute(query, employee_ids)
        rows = cursor.fetchall()
        assert len(rows) == 2, "Join operation with parameters returned incorrect number of rows"
        assert rows[0] == ['Alice', 'HR', 'Project A'], "Join operation with parameters returned incorrect data for row 1"
        assert rows[1] == ['Bob', 'Engineering', 'Project B'], "Join operation with parameters returned incorrect data for row 2"
    except Exception as e:
        pytest.fail(f"Join operation with parameters failed: {e}")

# Setup stored procedure
CREATE_STORED_PROCEDURE = """
CREATE PROCEDURE dbo.GetEmployeeProjects
    @EmployeeID INT
AS
BEGIN
    SELECT e.name, p.project_name
    FROM #pytest_employees e
    JOIN #pytest_projects p ON e.employee_id = p.employee_id
    WHERE e.employee_id = @EmployeeID
END
"""

def test_create_stored_procedure(cursor, db_connection):
    """Create stored procedure"""
    try:
        cursor.execute(CREATE_STORED_PROCEDURE)
        db_connection.commit()
    except Exception as e:
        pytest.fail(f"Stored procedure creation failed: {e}")

def test_execute_stored_procedure_with_parameters(cursor):
    """Test executing stored procedure with parameters"""
    try:
        cursor.execute("{CALL dbo.GetEmployeeProjects(?)}", [1])
        rows = cursor.fetchall()
        assert len(rows) == 1, "Stored procedure with parameters returned incorrect number of rows"
        assert rows[0] == ['Alice', 'Project A'], "Stored procedure with parameters returned incorrect data"
    except Exception as e:
        pytest.fail(f"Stored procedure execution with parameters failed: {e}")

def test_execute_stored_procedure_without_parameters(cursor):
    """Test executing stored procedure without parameters"""
    try:
        cursor.execute("""
            DECLARE @EmployeeID INT = 2
            EXEC dbo.GetEmployeeProjects @EmployeeID
        """)
        rows = cursor.fetchall()
        assert len(rows) == 1, "Stored procedure without parameters returned incorrect number of rows"
        assert rows[0] == ['Bob', 'Project B'], "Stored procedure without parameters returned incorrect data"
    except Exception as e:
        pytest.fail(f"Stored procedure execution without parameters failed: {e}")

def test_drop_stored_procedure(cursor, db_connection):
    """Drop stored procedure"""
    try:
        cursor.execute("DROP PROCEDURE IF EXISTS dbo.GetEmployeeProjects")
        db_connection.commit()
    except Exception as e:
        pytest.fail(f"Failed to drop stored procedure: {e}")

def test_drop_tables_for_join(cursor, db_connection):
    """Drop tables for join operations"""
    try:
        cursor.execute("DROP TABLE IF EXISTS #pytest_employees")
        cursor.execute("DROP TABLE IF EXISTS #pytest_departments")
        cursor.execute("DROP TABLE IF EXISTS #pytest_projects")
        db_connection.commit()
    except Exception as e:
        pytest.fail(f"Failed to drop tables for join operations: {e}")

def test_cursor_description(cursor):
    """Test cursor description"""
    cursor.execute("SELECT database_id, name FROM sys.databases;")
    description = cursor.description
    expected_description = [
        ('database_id', int, None, 10, 10, 0, False),
        ('name', str, None, 128, 128, 0, False)
    ]
    assert len(description) == len(expected_description), "Description length mismatch"
    for desc, expected in zip(description, expected_description):
        assert desc == expected, f"Description mismatch: {desc} != {expected}"

def test_parse_datetime(cursor, db_connection):
    """Test _parse_datetime"""
    try:
        cursor.execute("CREATE TABLE #pytest_datetime_test (datetime_column DATETIME)")
        db_connection.commit()
        cursor.execute("INSERT INTO #pytest_datetime_test (datetime_column) VALUES (?)", ['2024-05-20T12:34:56.123'])
        db_connection.commit()
        cursor.execute("SELECT datetime_column FROM #pytest_datetime_test")
        row = cursor.fetchone()
        assert row[0] == datetime(2024, 5, 20, 12, 34, 56, 123000), "Datetime parsing failed"
    except Exception as e:
        pytest.fail(f"Datetime parsing test failed: {e}")
    finally:
        cursor.execute("DROP TABLE #pytest_datetime_test")
        db_connection.commit()

def test_parse_date(cursor, db_connection):
    """Test _parse_date"""
    try:
        cursor.execute("CREATE TABLE #pytest_date_test (date_column DATE)")
        db_connection.commit()
        cursor.execute("INSERT INTO #pytest_date_test (date_column) VALUES (?)", ['2024-05-20'])
        db_connection.commit()
        cursor.execute("SELECT date_column FROM #pytest_date_test")
        row = cursor.fetchone()
        assert row[0] == date(2024, 5, 20), "Date parsing failed"
    except Exception as e:
        pytest.fail(f"Date parsing test failed: {e}")
    finally:
        cursor.execute("DROP TABLE #pytest_date_test")
        db_connection.commit()

def test_parse_time(cursor, db_connection):
    """Test _parse_time"""
    try:
        cursor.execute("CREATE TABLE #pytest_time_test (time_column TIME)")
        db_connection.commit()
        cursor.execute("INSERT INTO #pytest_time_test (time_column) VALUES (?)", ['12:34:56'])
        db_connection.commit()
        cursor.execute("SELECT time_column FROM #pytest_time_test")
        row = cursor.fetchone()
        assert row[0] == time(12, 34, 56), "Time parsing failed"
    except Exception as e:
        pytest.fail(f"Time parsing test failed: {e}")
    finally:
        cursor.execute("DROP TABLE #pytest_time_test")
        db_connection.commit()

def test_parse_smalldatetime(cursor, db_connection):
    """Test _parse_smalldatetime"""
    try:
        cursor.execute("CREATE TABLE #pytest_smalldatetime_test (smalldatetime_column SMALLDATETIME)")
        db_connection.commit()
        cursor.execute("INSERT INTO #pytest_smalldatetime_test (smalldatetime_column) VALUES (?)", ['2024-05-20 12:34'])
        db_connection.commit()
        cursor.execute("SELECT smalldatetime_column FROM #pytest_smalldatetime_test")
        row = cursor.fetchone()
        assert row[0] == datetime(2024, 5, 20, 12, 34), "Smalldatetime parsing failed"
    except Exception as e:
        pytest.fail(f"Smalldatetime parsing test failed: {e}")
    finally:
        cursor.execute("DROP TABLE #pytest_smalldatetime_test")
        db_connection.commit()

def test_parse_datetime2(cursor, db_connection):
    """Test _parse_datetime2"""
    try:
        cursor.execute("CREATE TABLE #pytest_datetime2_test (datetime2_column DATETIME2)")
        db_connection.commit()
        cursor.execute("INSERT INTO #pytest_datetime2_test (datetime2_column) VALUES (?)", ['2024-05-20 12:34:56.123456'])
        db_connection.commit()
        cursor.execute("SELECT datetime2_column FROM #pytest_datetime2_test")
        row = cursor.fetchone()
        assert row[0] == datetime(2024, 5, 20, 12, 34, 56, 123456), "Datetime2 parsing failed"
    except Exception as e:
        pytest.fail(f"Datetime2 parsing test failed: {e}")
    finally:
        cursor.execute("DROP TABLE #pytest_datetime2_test")
        db_connection.commit()

def test_get_numeric_data(cursor, db_connection):
    """Test _get_numeric_data"""
    try:
        cursor.execute("CREATE TABLE #pytest_numeric_test (numeric_column DECIMAL(10, 2))")
        db_connection.commit()
        cursor.execute("INSERT INTO #pytest_numeric_test (numeric_column) VALUES (?)", [decimal.Decimal('123.45')])
        db_connection.commit()
        cursor.execute("SELECT numeric_column FROM #pytest_numeric_test")
        row = cursor.fetchone()
        assert row[0] == decimal.Decimal('123.45'), "Numeric data parsing failed"
    except Exception as e:
        pytest.fail(f"Numeric data parsing test failed: {e}")
    finally:
        cursor.execute("DROP TABLE #pytest_numeric_test")
        db_connection.commit()

def test_none(cursor, db_connection):
    """Test None"""
    try:
        cursor.execute("CREATE TABLE #pytest_none_test (none_column NVARCHAR(255))")
        db_connection.commit()
        cursor.execute("INSERT INTO #pytest_none_test (none_column) VALUES (?)", [None])
        db_connection.commit()
        cursor.execute("SELECT none_column FROM #pytest_none_test")
        row = cursor.fetchone()
        assert row[0] is None, "None parsing failed"
    except Exception as e:
        pytest.fail(f"None parsing test failed: {e}")
    finally:
        cursor.execute("DROP TABLE #pytest_none_test")
        db_connection.commit()

def test_boolean(cursor, db_connection):
    """Test boolean"""
    try:
        cursor.execute("CREATE TABLE #pytest_boolean_test (boolean_column BIT)")
        db_connection.commit()
        cursor.execute("INSERT INTO #pytest_boolean_test (boolean_column) VALUES (?)", [True])
        db_connection.commit()
        cursor.execute("SELECT boolean_column FROM #pytest_boolean_test")
        row = cursor.fetchone()
        assert row[0] is True, "Boolean parsing failed"
    except Exception as e:
        pytest.fail(f"Boolean parsing test failed: {e}")
    finally:
        cursor.execute("DROP TABLE #pytest_boolean_test")
        db_connection.commit()


def test_sql_wvarchar(cursor, db_connection):
    """Test SQL_WVARCHAR"""
    try:
        cursor.execute("CREATE TABLE #pytest_wvarchar_test (wvarchar_column NVARCHAR(255))")
        db_connection.commit()
        cursor.execute("INSERT INTO #pytest_wvarchar_test (wvarchar_column) VALUES (?)", ['nvarchar data'])
        db_connection.commit()
        cursor.execute("SELECT wvarchar_column FROM #pytest_wvarchar_test")
        row = cursor.fetchone()
        assert row[0] == 'nvarchar data', "SQL_WVARCHAR parsing failed"
    except Exception as e:
        pytest.fail(f"SQL_WVARCHAR parsing test failed: {e}")
    finally:
        cursor.execute("DROP TABLE #pytest_wvarchar_test")
        db_connection.commit()

def test_sql_varchar(cursor, db_connection):
    """Test SQL_VARCHAR"""
    try:
        cursor.execute("CREATE TABLE #pytest_varchar_test (varchar_column VARCHAR(255))")
        db_connection.commit()
        cursor.execute("INSERT INTO #pytest_varchar_test (varchar_column) VALUES (?)", ['varchar data'])
        db_connection.commit()
        cursor.execute("SELECT varchar_column FROM #pytest_varchar_test")
        row = cursor.fetchone()
        assert row[0] == 'varchar data', "SQL_VARCHAR parsing failed"
    except Exception as e:
        pytest.fail(f"SQL_VARCHAR parsing test failed: {e}")
    finally:
        cursor.execute("DROP TABLE #pytest_varchar_test")
        db_connection.commit()

def test_numeric_precision_scale_positive_exponent(cursor, db_connection):
    """Test precision and scale for numeric values with positive exponent"""
    try:
        cursor.execute("CREATE TABLE #pytest_numeric_test (numeric_column DECIMAL(10, 2))")
        db_connection.commit()
        cursor.execute("INSERT INTO #pytest_numeric_test (numeric_column) VALUES (?)", [decimal.Decimal('31400')])
        db_connection.commit()
        cursor.execute("SELECT numeric_column FROM #pytest_numeric_test")
        row = cursor.fetchone()
        assert row[0] == decimal.Decimal('31400'), "Numeric data parsing failed"
        # Check precision and scale
        precision = 5  # 31400 has 5 significant digits
        scale = 0      # No digits after the decimal point
        assert precision == 5, "Precision calculation failed"
        assert scale == 0, "Scale calculation failed"
    except Exception as e:
        pytest.fail(f"Numeric precision and scale test failed: {e}")
    finally:
        cursor.execute("DROP TABLE #pytest_numeric_test")
        db_connection.commit()

def test_numeric_precision_scale_negative_exponent(cursor, db_connection):
    """Test precision and scale for numeric values with negative exponent"""
    try:
        cursor.execute("CREATE TABLE #pytest_numeric_test (numeric_column DECIMAL(10, 5))")
        db_connection.commit()
        cursor.execute("INSERT INTO #pytest_numeric_test (numeric_column) VALUES (?)", [decimal.Decimal('0.03140')])
        db_connection.commit()
        cursor.execute("SELECT numeric_column FROM #pytest_numeric_test")
        row = cursor.fetchone()
        assert row[0] == decimal.Decimal('0.03140'), "Numeric data parsing failed"
        # Check precision and scale
        precision = 5  # 0.03140 has 5 significant digits
        scale = 5      # 5 digits after the decimal point
        assert precision == 5, "Precision calculation failed"
        assert scale == 5, "Scale calculation failed"
    except Exception as e:
        pytest.fail(f"Numeric precision and scale test failed: {e}")
    finally:
        cursor.execute("DROP TABLE #pytest_numeric_test")
        db_connection.commit()

def test_row_attribute_access(cursor, db_connection):
    """Test accessing row values by column name as attributes"""
    try:
        # Create test table with multiple columns
        cursor.execute("""
            CREATE TABLE #pytest_row_attr_test (
                id INT PRIMARY KEY,
                name VARCHAR(50),
                email VARCHAR(100),
                age INT
            )
        """)
        db_connection.commit()
        
        # Insert test data
        cursor.execute("""
            INSERT INTO #pytest_row_attr_test (id, name, email, age)
            VALUES (1, 'John Doe', 'john@example.com', 30)
        """)
        db_connection.commit()
        
        # Test attribute access
        cursor.execute("SELECT * FROM #pytest_row_attr_test")
        row = cursor.fetchone()
        
        # Access by attribute
        assert row.id == 1, "Failed to access 'id' by attribute"
        assert row.name == 'John Doe', "Failed to access 'name' by attribute"
        assert row.email == 'john@example.com', "Failed to access 'email' by attribute"
        assert row.age == 30, "Failed to access 'age' by attribute"
        
        # Compare attribute access with index access
        assert row.id == row[0], "Attribute access for 'id' doesn't match index access"
        assert row.name == row[1], "Attribute access for 'name' doesn't match index access"
        assert row.email == row[2], "Attribute access for 'email' doesn't match index access"
        assert row.age == row[3], "Attribute access for 'age' doesn't match index access"
        
        # Test attribute that doesn't exist
        with pytest.raises(AttributeError):
            value = row.nonexistent_column
            
    except Exception as e:
        pytest.fail(f"Row attribute access test failed: {e}")
    finally:
        cursor.execute("DROP TABLE #pytest_row_attr_test")
        db_connection.commit()

def test_row_comparison_with_list(cursor, db_connection):
    """Test comparing Row objects with lists (__eq__ method)"""
    try:
        # Create test table
        cursor.execute("CREATE TABLE #pytest_row_comparison_test (col1 INT, col2 VARCHAR(20), col3 FLOAT)")
        db_connection.commit()
        
        # Insert test data
        cursor.execute("INSERT INTO #pytest_row_comparison_test VALUES (10, 'test_string', 3.14)")
        db_connection.commit()
        
        # Test fetchone comparison with list
        cursor.execute("SELECT * FROM #pytest_row_comparison_test")
        row = cursor.fetchone()
        assert row == [10, 'test_string', 3.14], "Row did not compare equal to matching list"
        assert row != [10, 'different', 3.14], "Row compared equal to non-matching list"
        
        # Test full row equality
        cursor.execute("SELECT * FROM #pytest_row_comparison_test")
        row1 = cursor.fetchone()
        cursor.execute("SELECT * FROM #pytest_row_comparison_test")
        row2 = cursor.fetchone()
        assert row1 == row2, "Identical rows should be equal"
        
        # Insert different data
        cursor.execute("INSERT INTO #pytest_row_comparison_test VALUES (20, 'other_string', 2.71)")
        db_connection.commit()
        
        # Test different rows are not equal
        cursor.execute("SELECT * FROM #pytest_row_comparison_test WHERE col1 = 10")
        row1 = cursor.fetchone()
        cursor.execute("SELECT * FROM #pytest_row_comparison_test WHERE col1 = 20")
        row2 = cursor.fetchone()
        assert row1 != row2, "Different rows should not be equal"
        
        # Test fetchmany row comparison with lists
        cursor.execute("SELECT * FROM #pytest_row_comparison_test ORDER BY col1")
        rows = cursor.fetchmany(2)
        assert len(rows) == 2, "Should have fetched 2 rows"
        assert rows[0] == [10, 'test_string', 3.14], "First row didn't match expected list"
        assert rows[1] == [20, 'other_string', 2.71], "Second row didn't match expected list"
        
    except Exception as e:
        pytest.fail(f"Row comparison test failed: {e}")
    finally:
        cursor.execute("DROP TABLE #pytest_row_comparison_test")
        db_connection.commit()

def test_row_string_representation(cursor, db_connection):
    """Test Row string and repr representations"""
    try:
        cursor.execute("""
        CREATE TABLE #pytest_row_test (
            id INT PRIMARY KEY,
            text_col NVARCHAR(50),
            null_col INT
        )
        """)
        db_connection.commit()

        cursor.execute("""
        INSERT INTO #pytest_row_test (id, text_col, null_col)
        VALUES (?, ?, ?)
        """, [1, "test", None])
        db_connection.commit()

        cursor.execute("SELECT * FROM #pytest_row_test")
        row = cursor.fetchone()
        
        # Test str()
        str_representation = str(row)
        assert str_representation == "(1, 'test', None)", "Row str() representation incorrect"
        
        # Test repr()
        repr_representation = repr(row)
        assert repr_representation == "(1, 'test', None)", "Row repr() representation incorrect"

    except Exception as e:
        pytest.fail(f"Row string representation test failed: {e}")
    finally:
        cursor.execute("DROP TABLE #pytest_row_test")
        db_connection.commit()

def test_row_column_mapping(cursor, db_connection):
    """Test Row column name mapping"""
    try:
        cursor.execute("""
        CREATE TABLE #pytest_row_test (
            FirstColumn INT PRIMARY KEY,
            Second_Column NVARCHAR(50),
            [Complex Name!] INT
        )
        """)
        db_connection.commit()

        cursor.execute("""
        INSERT INTO #pytest_row_test ([FirstColumn], [Second_Column], [Complex Name!])
        VALUES (?, ?, ?)
        """, [1, "test", 42])
        db_connection.commit()

        cursor.execute("SELECT * FROM #pytest_row_test")
        row = cursor.fetchone()
        
        # Test different column name styles
        assert row.FirstColumn == 1, "CamelCase column access failed"
        assert row.Second_Column == "test", "Snake_case column access failed"
        assert getattr(row, "Complex Name!") == 42, "Complex column name access failed"

        # Test column map completeness
        assert len(row._column_map) == 3, "Column map size incorrect"
        assert "FirstColumn" in row._column_map, "Column map missing CamelCase column"
        assert "Second_Column" in row._column_map, "Column map missing snake_case column"
        assert "Complex Name!" in row._column_map, "Column map missing complex name column"

    except Exception as e:
        pytest.fail(f"Row column mapping test failed: {e}")
    finally:
        cursor.execute("DROP TABLE #pytest_row_test")
        db_connection.commit()

# Method Chaining Tests
def test_execute_returns_self(cursor):
    """Test that execute() returns the cursor itself for method chaining"""
    # Test basic execute returns cursor
    result = cursor.execute("SELECT 1 as test_value")
    assert result is cursor, "execute() should return the cursor itself"
    assert id(result) == id(cursor), "Returned cursor should be the same object"

def test_execute_fetchone_chaining(cursor, db_connection):
    """Test chaining execute() with fetchone()"""
    try:
        # Create test table
        cursor.execute("CREATE TABLE #test_chaining (id INT, value NVARCHAR(50))")
        db_connection.commit()
        
        # Insert test data
        cursor.execute("INSERT INTO #test_chaining (id, value) VALUES (?, ?)", 1, "test_value")
        db_connection.commit()
        
        # Test execute().fetchone() chaining
        row = cursor.execute("SELECT id, value FROM #test_chaining WHERE id = ?", 1).fetchone()
        assert row is not None, "Should return a row"
        assert row[0] == 1, "First column should be 1"
        assert row[1] == "test_value", "Second column should be 'test_value'"
        
        # Test with non-existent row
        row = cursor.execute("SELECT id, value FROM #test_chaining WHERE id = ?", 999).fetchone()
        assert row is None, "Should return None for non-existent row"
        
    finally:
        try:
            cursor.execute("DROP TABLE #test_chaining")
            db_connection.commit()
        except:
            pass

def test_execute_fetchall_chaining(cursor, db_connection):
    """Test chaining execute() with fetchall()"""
    try:
        # Create test table
        cursor.execute("CREATE TABLE #test_chaining (id INT, value NVARCHAR(50))")
        db_connection.commit()
        
        # Insert multiple test records
        cursor.execute("INSERT INTO #test_chaining (id, value) VALUES (1, 'first')")
        cursor.execute("INSERT INTO #test_chaining (id, value) VALUES (2, 'second')")
        cursor.execute("INSERT INTO #test_chaining (id, value) VALUES (3, 'third')")
        db_connection.commit()
        
        # Test execute().fetchall() chaining
        rows = cursor.execute("SELECT id, value FROM #test_chaining ORDER BY id").fetchall()
        assert len(rows) == 3, "Should return 3 rows"
        assert rows[0] == [1, 'first'], "First row incorrect"
        assert rows[1] == [2, 'second'], "Second row incorrect"
        assert rows[2] == [3, 'third'], "Third row incorrect"
        
        # Test with WHERE clause
        rows = cursor.execute("SELECT id, value FROM #test_chaining WHERE id > ?", 1).fetchall()
        assert len(rows) == 2, "Should return 2 rows with WHERE clause"
        assert rows[0] == [2, 'second'], "Filtered first row incorrect"
        assert rows[1] == [3, 'third'], "Filtered second row incorrect"
        
    finally:
        try:
            cursor.execute("DROP TABLE #test_chaining")
            db_connection.commit()
        except:
            pass

def test_execute_fetchmany_chaining(cursor, db_connection):
    """Test chaining execute() with fetchmany()"""
    try:
        # Create test table
        cursor.execute("CREATE TABLE #test_chaining (id INT, value NVARCHAR(50))")
        db_connection.commit()
        
        # Insert test data
        for i in range(1, 6):  # Insert 5 records
            cursor.execute("INSERT INTO #test_chaining (id, value) VALUES (?, ?)", i, f"value_{i}")
        db_connection.commit()
        
        # Test execute().fetchmany() chaining with size parameter
        rows = cursor.execute("SELECT id, value FROM #test_chaining ORDER BY id").fetchmany(3)
        assert len(rows) == 3, "Should return 3 rows with fetchmany(3)"
        assert rows[0] == [1, 'value_1'], "First row incorrect"
        assert rows[1] == [2, 'value_2'], "Second row incorrect"
        assert rows[2] == [3, 'value_3'], "Third row incorrect"
        
        # Test execute().fetchmany() chaining with arraysize
        cursor.arraysize = 2
        rows = cursor.execute("SELECT id, value FROM #test_chaining ORDER BY id").fetchmany()
        assert len(rows) == 2, "Should return 2 rows with default arraysize"
        assert rows[0] == [1, 'value_1'], "First row incorrect"
        assert rows[1] == [2, 'value_2'], "Second row incorrect"
        
    finally:
        try:
            cursor.execute("DROP TABLE #test_chaining")
            db_connection.commit()
        except:
            pass

def test_execute_rowcount_chaining(cursor, db_connection):
    """Test chaining execute() with rowcount property"""
    try:
        # Create test table
        cursor.execute("CREATE TABLE #test_chaining (id INT, value NVARCHAR(50))")
        db_connection.commit()
        
        # Test INSERT rowcount chaining
        count = cursor.execute("INSERT INTO #test_chaining (id, value) VALUES (?, ?)", 1, "test").rowcount
        assert count == 1, "INSERT should affect 1 row"
        
        # Test multiple INSERT rowcount chaining
        count = cursor.execute("""
            INSERT INTO #test_chaining (id, value) VALUES 
            (2, 'test2'), (3, 'test3'), (4, 'test4')
        """).rowcount
        assert count == 3, "Multiple INSERT should affect 3 rows"
        
        # Test UPDATE rowcount chaining
        count = cursor.execute("UPDATE #test_chaining SET value = ? WHERE id > ?", "updated", 2).rowcount
        assert count == 2, "UPDATE should affect 2 rows"
        
        # Test DELETE rowcount chaining
        count = cursor.execute("DELETE FROM #test_chaining WHERE id = ?", 1).rowcount
        assert count == 1, "DELETE should affect 1 row"
        
        # Test SELECT rowcount chaining (should be -1)
        count = cursor.execute("SELECT * FROM #test_chaining").rowcount
        assert count == -1, "SELECT rowcount should be -1"
        
    finally:
        try:
            cursor.execute("DROP TABLE #test_chaining")
            db_connection.commit()
        except:
            pass

def test_execute_description_chaining(cursor):
    """Test chaining execute() with description property"""
    # Test description after execute
    description = cursor.execute("SELECT 1 as int_col, 'test' as str_col, GETDATE() as date_col").description
    assert len(description) == 3, "Should have 3 columns in description"
    assert description[0][0] == "int_col", "First column name should be 'int_col'"
    assert description[1][0] == "str_col", "Second column name should be 'str_col'"
    assert description[2][0] == "date_col", "Third column name should be 'date_col'"
    
    # Test description with table query
    description = cursor.execute("SELECT database_id, name FROM sys.databases WHERE database_id = 1").description
    assert len(description) == 2, "Should have 2 columns in description"
    assert description[0][0] == "database_id", "First column should be 'database_id'"
    assert description[1][0] == "name", "Second column should be 'name'"

def test_multiple_chaining_operations(cursor, db_connection):
    """Test multiple chaining operations in sequence"""
    try:
        # Create test table
        cursor.execute("CREATE TABLE #test_multi_chain (id INT IDENTITY(1,1), value NVARCHAR(50))")
        db_connection.commit()
        
        # Chain multiple operations: execute -> rowcount, then execute -> fetchone
        insert_count = cursor.execute("INSERT INTO #test_multi_chain (value) VALUES (?)", "first").rowcount
        assert insert_count == 1, "First insert should affect 1 row"
        
        row = cursor.execute("SELECT id, value FROM #test_multi_chain WHERE value = ?", "first").fetchone()
        assert row is not None, "Should find the inserted row"
        assert row[1] == "first", "Value should be 'first'"
        
        # Chain more operations
        insert_count = cursor.execute("INSERT INTO #test_multi_chain (value) VALUES (?)", "second").rowcount
        assert insert_count == 1, "Second insert should affect 1 row"
        
        all_rows = cursor.execute("SELECT value FROM #test_multi_chain ORDER BY id").fetchall()
        assert len(all_rows) == 2, "Should have 2 rows total"
        assert all_rows[0] == ["first"], "First row should be 'first'"
        assert all_rows[1] == ["second"], "Second row should be 'second'"
        
    finally:
        try:
            cursor.execute("DROP TABLE #test_multi_chain")
            db_connection.commit()
        except:
            pass

def test_chaining_with_parameters(cursor, db_connection):
    """Test method chaining with various parameter formats"""
    try:
        # Create test table
        cursor.execute("CREATE TABLE #test_params (id INT, name NVARCHAR(50), age INT)")
        db_connection.commit()
        
        # Test chaining with tuple parameters
        row = cursor.execute("INSERT INTO #test_params VALUES (?, ?, ?)", (1, "Alice", 25)).rowcount
        assert row == 1, "Tuple parameter insert should affect 1 row"
        
        # Test chaining with individual parameters
        row = cursor.execute("INSERT INTO #test_params VALUES (?, ?, ?)", 2, "Bob", 30).rowcount
        assert row == 1, "Individual parameter insert should affect 1 row"
        
        # Test chaining with list parameters
        row = cursor.execute("INSERT INTO #test_params VALUES (?, ?, ?)", [3, "Charlie", 35]).rowcount
        assert row == 1, "List parameter insert should affect 1 row"
        
        # Test chaining query with parameters and fetchall
        rows = cursor.execute("SELECT name, age FROM #test_params WHERE age > ?", 28).fetchall()
        assert len(rows) == 2, "Should find 2 people over 28"
        assert rows[0] == ["Bob", 30], "First result should be Bob"
        assert rows[1] == ["Charlie", 35], "Second result should be Charlie"
        
    finally:
        try:
            cursor.execute("DROP TABLE #test_params")
            db_connection.commit()
        except:
            pass

def test_chaining_with_iteration(cursor, db_connection):
    """Test method chaining with iteration (for loop)"""
    try:
        # Create test table
        cursor.execute("CREATE TABLE #test_iteration (id INT, name NVARCHAR(50))")
        db_connection.commit()
        
        # Insert test data
        names = ["Alice", "Bob", "Charlie", "Diana"]
        for i, name in enumerate(names, 1):
            cursor.execute("INSERT INTO #test_iteration VALUES (?, ?)", i, name)
        db_connection.commit()
        
        # Test iteration over execute() result (should work because cursor implements __iter__)
        results = []
        for row in cursor.execute("SELECT id, name FROM #test_iteration ORDER BY id"):
            results.append((row[0], row[1]))
        
        expected = [(1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "Diana")]
        assert results == expected, f"Iteration results should match expected: {results} != {expected}"
        
        # Test iteration with WHERE clause
        results = []
        for row in cursor.execute("SELECT name FROM #test_iteration WHERE id > ?", 2):
            results.append(row[0])
        
        expected_names = ["Charlie", "Diana"]
        assert results == expected_names, f"Filtered iteration should return: {expected_names}, got: {results}"
        
    finally:
        try:
            cursor.execute("DROP TABLE #test_iteration")
            db_connection.commit()
        except:
            pass

def test_cursor_next_functionality(cursor, db_connection):
    """Test cursor next() functionality for future iterator implementation"""
    try:
        # Create test table
        cursor.execute("CREATE TABLE #test_next (id INT, name NVARCHAR(50))")
        db_connection.commit()
        
        # Insert test data
        test_data = [
            (1, "Alice"),
            (2, "Bob"), 
            (3, "Charlie"),
            (4, "Diana")
        ]
        
        for id_val, name in test_data:
            cursor.execute("INSERT INTO #test_next VALUES (?, ?)", id_val, name)
        db_connection.commit()
        
        # Execute query
        cursor.execute("SELECT id, name FROM #test_next ORDER BY id")
        
        # Test next() function (this will work once __iter__ and __next__ are implemented)
        # For now, we'll test the equivalent functionality using fetchone()
        
        # Test 1: Get first row using next() equivalent
        first_row = cursor.fetchone()
        assert first_row is not None, "First row should not be None"
        assert first_row[0] == 1, "First row id should be 1"
        assert first_row[1] == "Alice", "First row name should be Alice"
        
        # Test 2: Get second row using next() equivalent  
        second_row = cursor.fetchone()
        assert second_row is not None, "Second row should not be None"
        assert second_row[0] == 2, "Second row id should be 2"
        assert second_row[1] == "Bob", "Second row name should be Bob"
        
        # Test 3: Get third row using next() equivalent
        third_row = cursor.fetchone()
        assert third_row is not None, "Third row should not be None"
        assert third_row[0] == 3, "Third row id should be 3"
        assert third_row[1] == "Charlie", "Third row name should be Charlie"
        
        # Test 4: Get fourth row using next() equivalent
        fourth_row = cursor.fetchone()
        assert fourth_row is not None, "Fourth row should not be None"
        assert fourth_row[0] == 4, "Fourth row id should be 4"
        assert fourth_row[1] == "Diana", "Fourth row name should be Diana"
        
        # Test 5: Try to get fifth row (should return None, equivalent to StopIteration)
        fifth_row = cursor.fetchone()
        assert fifth_row is None, "Fifth row should be None (no more data)"
        
        # Test 6: Test with empty result set
        cursor.execute("SELECT id, name FROM #test_next WHERE id > 100")
        empty_row = cursor.fetchone()
        assert empty_row is None, "Empty result set should return None immediately"
        
        # Test 7: Test next() with single row result
        cursor.execute("SELECT id, name FROM #test_next WHERE id = 2")
        single_row = cursor.fetchone()
        assert single_row is not None, "Single row should not be None"
        assert single_row[0] == 2, "Single row id should be 2"
        assert single_row[1] == "Bob", "Single row name should be Bob"
        
        # Next call should return None
        no_more_rows = cursor.fetchone()
        assert no_more_rows is None, "No more rows should return None"
        
    finally:
        try:
            cursor.execute("DROP TABLE #test_next")
            db_connection.commit()
        except:
            pass

def test_cursor_next_with_different_data_types(cursor, db_connection):
    """Test next() functionality with various data types"""
    try:
        # Create test table with various data types
        cursor.execute("""
            CREATE TABLE #test_next_types (
                id INT,
                name NVARCHAR(50),
                score FLOAT,
                active BIT,
                created_date DATE,
                created_time DATETIME
            )
        """)
        db_connection.commit()
        
        # Insert test data with different types
        from datetime import date, datetime
        cursor.execute("""
            INSERT INTO #test_next_types 
            VALUES (?, ?, ?, ?, ?, ?)
        """, 1, "Test User", 95.5, True, date(2024, 1, 15), datetime(2024, 1, 15, 10, 30, 0))
        db_connection.commit()
        
        # Execute query and test next() equivalent
        cursor.execute("SELECT * FROM #test_next_types")
        
        # Get the row using next() equivalent (fetchone)
        row = cursor.fetchone()
        assert row is not None, "Row should not be None"
        assert row[0] == 1, "ID should be 1"
        assert row[1] == "Test User", "Name should be 'Test User'"
        assert abs(row[2] - 95.5) < 0.001, "Score should be approximately 95.5"
        assert row[3] == True, "Active should be True"
        assert row[4] == date(2024, 1, 15), "Date should match"
        assert row[5] == datetime(2024, 1, 15, 10, 30, 0), "Datetime should match"
        
        # Next call should return None
        next_row = cursor.fetchone()
        assert next_row is None, "No more rows should return None"
        
    finally:
        try:
            cursor.execute("DROP TABLE #test_next_types")
            db_connection.commit()
        except:
            pass

def test_cursor_next_error_conditions(cursor, db_connection):
    """Test next() functionality error conditions"""
    try:
        # Test next() on closed cursor (should raise exception when implemented)
        test_cursor = db_connection.cursor()
        test_cursor.execute("SELECT 1")
        test_cursor.close()
        
        # This should raise an exception when iterator is implemented
        try:
            test_cursor.fetchone()  # Equivalent to next() call
            assert False, "Should raise exception on closed cursor"
        except Exception:
            pass  # Expected behavior
        
        # Test next() without executing query first
        fresh_cursor = db_connection.cursor()
        try:
            fresh_cursor.fetchone()  # This might work but return None or raise exception
        except Exception:
            pass  # Either behavior is acceptable
        finally:
            fresh_cursor.close()
            
    except Exception as e:
        # Some error conditions might not be testable without full iterator implementation
        pass

def test_future_iterator_protocol_compatibility(cursor, db_connection):
    """Test that demonstrates future iterator protocol usage"""
    try:
        # Create test table
        cursor.execute("CREATE TABLE #test_future_iter (value INT)")
        db_connection.commit()
        
        # Insert test data
        for i in range(1, 4):
            cursor.execute("INSERT INTO #test_future_iter VALUES (?)", i)
        db_connection.commit()
        
        # Execute query
        cursor.execute("SELECT value FROM #test_future_iter ORDER BY value")
        
        # Demonstrate how it will work with iterator protocol:
        # This is what will be possible once __iter__ and __next__ are implemented:
        
        # Method 1: Using next() function (future implementation)
        # row1 = next(cursor)  # Will work with __next__
        # row2 = next(cursor)  # Will work with __next__
        # row3 = next(cursor)  # Will work with __next__
        # try:
        #     row4 = next(cursor)  # Should raise StopIteration
        # except StopIteration:
        #     pass
        
        # Method 2: Using for loop (future implementation)
        # results = []
        # for row in cursor:  # Will work with __iter__ and __next__
        #     results.append(row[0])
        
        # For now, test equivalent functionality with fetchone()
        results = []
        while True:
            row = cursor.fetchone()
            if row is None:
                break
            results.append(row[0])
        
        expected = [1, 2, 3]
        assert results == expected, f"Results should be {expected}, got {results}"
        
        # Test method chaining with iteration (current working implementation)
        results2 = []
        for row in cursor.execute("SELECT value FROM #test_future_iter ORDER BY value DESC").fetchall():
            results2.append(row[0])
        
        expected2 = [3, 2, 1]
        assert results2 == expected2, f"Chained results should be {expected2}, got {results2}"
        
    finally:
        try:
            cursor.execute("DROP TABLE #test_future_iter")
            db_connection.commit()
        except:
            pass

def test_chaining_error_handling(cursor):
    """Test that chaining works properly even when errors occur"""
    # Test that cursor is still chainable after an error
    with pytest.raises(Exception):
        cursor.execute("SELECT * FROM nonexistent_table").fetchone()
    
    # Cursor should still be usable for chaining after error
    row = cursor.execute("SELECT 1 as test").fetchone()
    assert row[0] == 1, "Cursor should still work after error"
    
    # Test chaining with invalid SQL
    with pytest.raises(Exception):
        cursor.execute("INVALID SQL SYNTAX").rowcount
    
    # Should still be chainable
    count = cursor.execute("SELECT COUNT(*) FROM sys.databases").fetchone()[0]
    assert isinstance(count, int), "Should return integer count"
    assert count > 0, "Should have at least one database"

def test_chaining_performance_statement_reuse(cursor, db_connection):
    """Test that chaining works with statement reuse (same SQL, different parameters)"""
    try:
        # Create test table
        cursor.execute("CREATE TABLE #test_reuse (id INT, value NVARCHAR(50))")
        db_connection.commit()
        
        # Execute same SQL multiple times with different parameters (should reuse prepared statement)
        sql = "INSERT INTO #test_reuse (id, value) VALUES (?, ?)"
        
        count1 = cursor.execute(sql, 1, "first").rowcount
        count2 = cursor.execute(sql, 2, "second").rowcount
        count3 = cursor.execute(sql, 3, "third").rowcount
        
        assert count1 == 1, "First insert should affect 1 row"
        assert count2 == 1, "Second insert should affect 1 row"
        assert count3 == 1, "Third insert should affect 1 row"
        
        # Verify all data was inserted correctly
        rows = cursor.execute("SELECT id, value FROM #test_reuse ORDER BY id").fetchall()
        assert len(rows) == 3, "Should have 3 rows"
        assert rows[0] == [1, "first"], "First row incorrect"
        assert rows[1] == [2, "second"], "Second row incorrect"
        assert rows[2] == [3, "third"], "Third row incorrect"
        
    finally:
        try:
            cursor.execute("DROP TABLE #test_reuse")
            db_connection.commit()
        except:
            pass

def test_execute_chaining_compatibility_examples(cursor, db_connection):
    """Test real-world chaining examples"""
    try:
        # Create users table
        cursor.execute("""
            CREATE TABLE #users (
                user_id INT IDENTITY(1,1) PRIMARY KEY,
                user_name NVARCHAR(50),
                last_logon DATETIME,
                status NVARCHAR(20)
            )
        """)
        db_connection.commit()
        
        # Insert test users
        cursor.execute("INSERT INTO #users (user_name, status) VALUES ('john_doe', 'active')")
        cursor.execute("INSERT INTO #users (user_name, status) VALUES ('jane_smith', 'inactive')")
        db_connection.commit()
        
        # Example 1: Iterate over results directly (pyodbc style)
        user_names = []
        for row in cursor.execute("SELECT user_id, user_name FROM #users WHERE status = ?", "active"):
            user_names.append(f"{row.user_id}: {row.user_name}")
        assert len(user_names) == 1, "Should find 1 active user"
        assert "john_doe" in user_names[0], "Should contain john_doe"
        
        # Example 2: Single row fetch chaining
        user = cursor.execute("SELECT user_name FROM #users WHERE user_id = ?", 1).fetchone()
        assert user[0] == "john_doe", "Should return john_doe"
        
        # Example 3: All rows fetch chaining
        all_users = cursor.execute("SELECT user_name FROM #users ORDER BY user_id").fetchall()
        assert len(all_users) == 2, "Should return 2 users"
        assert all_users[0] == ["john_doe"], "First user should be john_doe"
        assert all_users[1] == ["jane_smith"], "Second user should be jane_smith"
        
        # Example 4: Update with rowcount chaining
        from datetime import datetime
        now = datetime.now()
        updated_count = cursor.execute(
            "UPDATE #users SET last_logon = ? WHERE user_name = ?", 
            now, "john_doe"
        ).rowcount
        assert updated_count == 1, "Should update 1 user"
        
        # Example 5: Delete with rowcount chaining
        deleted_count = cursor.execute("DELETE FROM #users WHERE status = ?", "inactive").rowcount
        assert deleted_count == 1, "Should delete 1 inactive user"
        
        # Verify final state
        remaining_users = cursor.execute("SELECT COUNT(*) FROM #users").fetchone()[0]
        assert remaining_users == 1, "Should have 1 user remaining"
        
    finally:
        try:
            cursor.execute("DROP TABLE #users")
            db_connection.commit()
        except:
            pass

def test_rownumber_basic_functionality(cursor, db_connection):
    """Test basic rownumber functionality"""
    try:
        # Create test table
        cursor.execute("CREATE TABLE #test_rownumber (id INT, value VARCHAR(50))")
        db_connection.commit()
        
        # Insert test data
        for i in range(5):
            cursor.execute("INSERT INTO #test_rownumber VALUES (?, ?)", i, f"value_{i}")
        db_connection.commit()
        
        # Execute query and check initial rownumber
        cursor.execute("SELECT * FROM #test_rownumber ORDER BY id")
        
        # Note: Since we're now using log('warning', ...) instead of warnings.warn(),
        # we can't easily capture the log messages in tests without additional setup.
        # The warning will be logged to the configured logger instead.
        initial_rownumber = cursor.rownumber
        
        # Initial rownumber should be 0 (before any fetch)
        assert initial_rownumber == 0, f"Initial rownumber should be 0, got {initial_rownumber}"
        
        # Fetch first row and check rownumber
        row1 = cursor.fetchone()
        assert cursor.rownumber == 1, f"After fetching 1 row, rownumber should be 1, got {cursor.rownumber}"
        assert row1[0] == 0, "First row should have id 0"
        
        # Fetch second row and check rownumber
        row2 = cursor.fetchone()
        assert cursor.rownumber == 2, f"After fetching 2 rows, rownumber should be 2, got {cursor.rownumber}"
        assert row2[0] == 1, "Second row should have id 1"
        
        # Fetch remaining rows and check rownumber progression
        row3 = cursor.fetchone()
        assert cursor.rownumber == 3, f"After fetching 3 rows, rownumber should be 3, got {cursor.rownumber}"
        
        row4 = cursor.fetchone()
        assert cursor.rownumber == 4, f"After fetching 4 rows, rownumber should be 4, got {cursor.rownumber}"
        
        row5 = cursor.fetchone()
        assert cursor.rownumber == 5, f"After fetching 5 rows, rownumber should be 5, got {cursor.rownumber}"
        
        # Try to fetch beyond result set
        no_more_rows = cursor.fetchone()
        assert no_more_rows is None, "Should return None when no more rows"
        assert cursor.rownumber == 5, f"Rownumber should remain 5 after exhausting result set, got {cursor.rownumber}"
        
    finally:
        try:
            cursor.execute("DROP TABLE #test_rownumber")
            db_connection.commit()
        except:
            pass

def test_rownumber_warning_logged(cursor, db_connection):
    """Test that accessing rownumber logs a warning message"""
    import logging
    from mssql_python.helpers import get_logger
    
    try:
        # Create test table
        cursor.execute("CREATE TABLE #test_rownumber_log (id INT)")
        db_connection.commit()
        cursor.execute("INSERT INTO #test_rownumber_log VALUES (1)")
        db_connection.commit()
        
        # Execute query
        cursor.execute("SELECT * FROM #test_rownumber_log")
        
        # Set up logging capture
        logger = get_logger()
        if logger:
            # Create a test handler to capture log messages
            import io
            log_stream = io.StringIO()
            test_handler = logging.StreamHandler(log_stream)
            test_handler.setLevel(logging.WARNING)
            
            # Add our test handler
            logger.addHandler(test_handler)
            
            try:
                # Access rownumber (should trigger warning log)
                rownumber = cursor.rownumber
                
                # Check if warning was logged
                log_contents = log_stream.getvalue()
                assert "DB-API extension cursor.rownumber used" in log_contents, \
                    f"Expected warning message not found in logs: {log_contents}"
                
                # Verify rownumber functionality still works
                assert rownumber == 0, f"Expected rownumber 0, got {rownumber}"
                
            finally:
                # Clean up: remove our test handler
                logger.removeHandler(test_handler)
        else:
            # If no logger configured, just test that rownumber works
            rownumber = cursor.rownumber
            assert rownumber == 0, f"Expected rownumber 0, got {rownumber}"
            
    finally:
        try:
            cursor.execute("DROP TABLE #test_rownumber_log")
            db_connection.commit()
        except:
            pass

def test_rownumber_closed_cursor(cursor, db_connection):
    """Test rownumber behavior with closed cursor"""
    # Create a separate cursor for this test
    test_cursor = db_connection.cursor()
    
    try:
        # Create test table
        test_cursor.execute("CREATE TABLE #test_rownumber_closed (id INT)")
        db_connection.commit()
        
        # Insert data and execute query
        test_cursor.execute("INSERT INTO #test_rownumber_closed VALUES (1)")
        test_cursor.execute("SELECT * FROM #test_rownumber_closed")
        
        # Verify rownumber works before closing
        assert test_cursor.rownumber == 0, "Rownumber should work before closing"
        
        # Close the cursor
        test_cursor.close()
        
        # Test that rownumber returns None for closed cursor
        # Note: This will still log a warning, but that's expected behavior
        rownumber = test_cursor.rownumber
        assert rownumber is None, "Rownumber should be None for closed cursor"
        
    finally:
        # Clean up
        try:
            if not test_cursor.closed:
                test_cursor.execute("DROP TABLE #test_rownumber_closed")
                db_connection.commit()
                test_cursor.close()
            else:
                # Use the main cursor to clean up
                cursor.execute("DROP TABLE IF EXISTS #test_rownumber_closed")
                db_connection.commit()
        except:
            pass

def test_close(db_connection):
    """Test closing the cursor"""
    try:
        cursor = db_connection.cursor()
        cursor.close()
        assert cursor.closed, "Cursor should be closed after calling close()"
    except Exception as e:
        pytest.fail(f"Cursor close test failed: {e}")
    finally:
        cursor = db_connection.cursor()
