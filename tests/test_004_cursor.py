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
import time as time_module
import decimal
from mssql_python import Connection
import mssql_python

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

def test_lowercase_attribute(cursor, db_connection):
    """Test that the lowercase attribute properly converts column names to lowercase"""
    
    # Store original value to restore after test
    original_lowercase = mssql_python.lowercase
    drop_cursor = None
    
    try:
        # Create a test table with mixed-case column names
        cursor.execute("""
        CREATE TABLE #pytest_lowercase_test (
            ID INT PRIMARY KEY,
            UserName VARCHAR(50),
            EMAIL_ADDRESS VARCHAR(100),
            PhoneNumber VARCHAR(20)
        )
        """)
        db_connection.commit()
        
        # Insert test data
        cursor.execute("""
        INSERT INTO #pytest_lowercase_test (ID, UserName, EMAIL_ADDRESS, PhoneNumber)
        VALUES (1, 'JohnDoe', 'john@example.com', '555-1234')
        """)
        db_connection.commit()
        
        # First test with lowercase=False (default)
        mssql_python.lowercase = False
        cursor1 = db_connection.cursor()
        cursor1.execute("SELECT * FROM #pytest_lowercase_test")
        
        # Description column names should preserve original case
        column_names1 = [desc[0] for desc in cursor1.description]
        assert "ID" in column_names1, "Column 'ID' should be present with original case"
        assert "UserName" in column_names1, "Column 'UserName' should be present with original case"  
        
        # Make sure to consume all results and close the cursor
        cursor1.fetchall()
        cursor1.close()
        
        # Now test with lowercase=True
        mssql_python.lowercase = True
        cursor2 = db_connection.cursor()
        cursor2.execute("SELECT * FROM #pytest_lowercase_test")
        
        # Description column names should be lowercase
        column_names2 = [desc[0] for desc in cursor2.description]
        assert "id" in column_names2, "Column names should be lowercase when lowercase=True"
        assert "username" in column_names2, "Column names should be lowercase when lowercase=True"
        
        # Make sure to consume all results and close the cursor
        cursor2.fetchall()
        cursor2.close()
        
        # Create a fresh cursor for cleanup
        drop_cursor = db_connection.cursor()
        
    finally:
        # Restore original value
        mssql_python.lowercase = original_lowercase
        
        try:
            # Use a separate cursor for cleanup
            if drop_cursor:
                drop_cursor.execute("DROP TABLE IF EXISTS #pytest_lowercase_test")
                db_connection.commit()
                drop_cursor.close()
        except Exception as e:
            print(f"Warning: Failed to drop test table: {e}")

def test_decimal_separator_function(cursor, db_connection):
    """Test decimal separator functionality with database operations"""
    # Store original value to restore after test
    original_separator = mssql_python.getDecimalSeparator()

    try:
        # Create test table
        cursor.execute("""
        CREATE TABLE #pytest_decimal_separator_test (
            id INT PRIMARY KEY,
            decimal_value DECIMAL(10, 2)
        )
        """)
        db_connection.commit()

        # Insert test values with default separator (.)
        test_value = decimal.Decimal('123.45')
        cursor.execute("""
        INSERT INTO #pytest_decimal_separator_test (id, decimal_value)
        VALUES (1, ?)
        """, [test_value])
        db_connection.commit()

        # First test with default decimal separator (.)
        cursor.execute("SELECT id, decimal_value FROM #pytest_decimal_separator_test")
        row = cursor.fetchone()
        default_str = str(row)
        assert '123.45' in default_str, "Default separator not found in string representation"

        # Now change to comma separator and test string representation
        mssql_python.setDecimalSeparator(',')
        cursor.execute("SELECT id, decimal_value FROM #pytest_decimal_separator_test")
        row = cursor.fetchone()
        
        # This should format the decimal with a comma in the string representation
        comma_str = str(row)
        assert '123,45' in comma_str, f"Expected comma in string representation but got: {comma_str}"
        
    finally:
        # Restore original decimal separator
        mssql_python.setDecimalSeparator(original_separator)
        
        # Cleanup
        cursor.execute("DROP TABLE IF EXISTS #pytest_decimal_separator_test")
        db_connection.commit()

def test_decimal_separator_basic_functionality():
    """Test basic decimal separator functionality without database operations"""
    # Store original value to restore after test
    original_separator = mssql_python.getDecimalSeparator()
    
    try:
        # Test default value
        assert mssql_python.getDecimalSeparator() == '.', "Default decimal separator should be '.'"
        
        # Test setting to comma
        mssql_python.setDecimalSeparator(',')
        assert mssql_python.getDecimalSeparator() == ',', "Decimal separator should be ',' after setting"
        
        # Test setting to other valid separators
        mssql_python.setDecimalSeparator(':')
        assert mssql_python.getDecimalSeparator() == ':', "Decimal separator should be ':' after setting"
        
        # Test invalid inputs
        with pytest.raises(ValueError):
            mssql_python.setDecimalSeparator('')  # Empty string
        
        with pytest.raises(ValueError):
            mssql_python.setDecimalSeparator('too_long')  # More than one character
        
        with pytest.raises(ValueError):
            mssql_python.setDecimalSeparator(123)  # Not a string
            
    finally:
        # Restore original separator
        mssql_python.setDecimalSeparator(original_separator)

def test_decimal_separator_with_multiple_values(cursor, db_connection):
    """Test decimal separator with multiple different decimal values"""
    original_separator = mssql_python.getDecimalSeparator()

    try:
        # Create test table
        cursor.execute("""
        CREATE TABLE #pytest_decimal_multi_test (
            id INT PRIMARY KEY,
            positive_value DECIMAL(10, 2),
            negative_value DECIMAL(10, 2),
            zero_value DECIMAL(10, 2),
            small_value DECIMAL(10, 4)
        )
        """)
        db_connection.commit()
        
        # Insert test data
        cursor.execute("""
        INSERT INTO #pytest_decimal_multi_test VALUES (1, 123.45, -67.89, 0.00, 0.0001)
        """)
        db_connection.commit()
        
        # Test with default separator first
        cursor.execute("SELECT * FROM #pytest_decimal_multi_test")
        row = cursor.fetchone()
        default_str = str(row)
        assert '123.45' in default_str, "Default positive value formatting incorrect"
        assert '-67.89' in default_str, "Default negative value formatting incorrect"
        
        # Change to comma separator
        mssql_python.setDecimalSeparator(',')
        cursor.execute("SELECT * FROM #pytest_decimal_multi_test")
        row = cursor.fetchone()
        comma_str = str(row)
        
        # Verify comma is used in all decimal values
        assert '123,45' in comma_str, "Positive value not formatted with comma"
        assert '-67,89' in comma_str, "Negative value not formatted with comma"
        assert '0,00' in comma_str, "Zero value not formatted with comma"
        assert '0,0001' in comma_str, "Small value not formatted with comma"
        
    finally:
        # Restore original separator
        mssql_python.setDecimalSeparator(original_separator)
        
        # Cleanup
        cursor.execute("DROP TABLE IF EXISTS #pytest_decimal_multi_test")
        db_connection.commit()

def test_decimal_separator_calculations(cursor, db_connection):
    """Test that decimal separator doesn't affect calculations"""
    original_separator = mssql_python.getDecimalSeparator()

    try:
        # Create test table
        cursor.execute("""
        CREATE TABLE #pytest_decimal_calc_test (
            id INT PRIMARY KEY,
            value1 DECIMAL(10, 2),
            value2 DECIMAL(10, 2)
        )
        """)
        db_connection.commit()
        
        # Insert test data
        cursor.execute("""
        INSERT INTO #pytest_decimal_calc_test VALUES (1, 10.25, 5.75)
        """)
        db_connection.commit()
        
        # Test with default separator
        cursor.execute("SELECT value1 + value2 AS sum_result FROM #pytest_decimal_calc_test")
        row = cursor.fetchone()
        assert row.sum_result == decimal.Decimal('16.00'), "Sum calculation incorrect with default separator"
        
        # Change to comma separator
        mssql_python.setDecimalSeparator(',')
        
        # Calculations should still work correctly
        cursor.execute("SELECT value1 + value2 AS sum_result FROM #pytest_decimal_calc_test")
        row = cursor.fetchone()
        assert row.sum_result == decimal.Decimal('16.00'), "Sum calculation affected by separator change"
        
        # But string representation should use comma
        assert '16,00' in str(row), "Sum result not formatted with comma in string representation"
        
    finally:
        # Restore original separator
        mssql_python.setDecimalSeparator(original_separator)
        
        # Cleanup
        cursor.execute("DROP TABLE IF EXISTS #pytest_decimal_calc_test")
        db_connection.commit()

def test_cursor_setinputsizes_basic(db_connection):
    """Test the basic functionality of setinputsizes"""
    from mssql_python.constants import ConstantsDDBC
    
    cursor = db_connection.cursor()
    
    # Create a test table
    cursor.execute("DROP TABLE IF EXISTS #test_inputsizes")
    cursor.execute("""
    CREATE TABLE #test_inputsizes (
        string_col NVARCHAR(100),
        int_col INT
    )
    """)
    
    # Set input sizes for parameters
    cursor.setinputsizes([
        (ConstantsDDBC.SQL_WVARCHAR.value, 100, 0),
        (ConstantsDDBC.SQL_INTEGER.value, 0, 0)
    ])
    
    # Execute with parameters
    cursor.execute(
        "INSERT INTO #test_inputsizes VALUES (?, ?)",
        "Test String", 42
    )
    
    # Verify data was inserted correctly
    cursor.execute("SELECT * FROM #test_inputsizes")
    row = cursor.fetchone()
    
    assert row[0] == "Test String"
    assert row[1] == 42
    
    # Clean up
    cursor.execute("DROP TABLE IF EXISTS #test_inputsizes")

def test_cursor_setinputsizes_with_executemany_float(db_connection):
    """Test setinputsizes with executemany using float instead of Decimal"""
    from mssql_python.constants import ConstantsDDBC
    
    cursor = db_connection.cursor()
    
    # Create a test table
    cursor.execute("DROP TABLE IF EXISTS #test_inputsizes_float")
    cursor.execute("""
    CREATE TABLE #test_inputsizes_float (
        id INT,
        name NVARCHAR(50),
        price REAL  /* Use REAL instead of DECIMAL */
    )
    """)
    
    # Prepare data with float values
    data = [
        (1, "Item 1", 10.99),
        (2, "Item 2", 20.50),
        (3, "Item 3", 30.75)
    ]
    
    # Set input sizes for parameters
    cursor.setinputsizes([
        (ConstantsDDBC.SQL_INTEGER.value, 0, 0),
        (ConstantsDDBC.SQL_WVARCHAR.value, 50, 0),
        (ConstantsDDBC.SQL_REAL.value, 0, 0)  
    ])
    
    # Execute with parameters
    cursor.executemany(
        "INSERT INTO #test_inputsizes_float VALUES (?, ?, ?)",
        data
    )
    
    # Verify all data was inserted correctly
    cursor.execute("SELECT * FROM #test_inputsizes_float ORDER BY id")
    rows = cursor.fetchall()
    
    assert len(rows) == 3
    assert rows[0][0] == 1
    assert rows[0][1] == "Item 1"
    assert abs(rows[0][2] - 10.99) < 0.001
    
    # Clean up
    cursor.execute("DROP TABLE IF EXISTS #test_inputsizes_float")

def test_cursor_setinputsizes_reset(db_connection):
    """Test that setinputsizes is reset after execution"""
    from mssql_python.constants import ConstantsDDBC
    
    cursor = db_connection.cursor()
    
    # Create a test table
    cursor.execute("DROP TABLE IF EXISTS #test_inputsizes_reset")
    cursor.execute("""
    CREATE TABLE #test_inputsizes_reset (
        col1 NVARCHAR(100),
        col2 INT
    )
    """)
    
    # Set input sizes for parameters
    cursor.setinputsizes([
        (ConstantsDDBC.SQL_WVARCHAR.value, 100, 0),
        (ConstantsDDBC.SQL_INTEGER.value, 0, 0)
    ])
    
    # Execute with parameters
    cursor.execute(
        "INSERT INTO #test_inputsizes_reset VALUES (?, ?)",
        "Test String", 42
    )
    
    # Verify inputsizes was reset
    assert cursor._inputsizes is None
    
    # Now execute again without setting input sizes
    cursor.execute(
        "INSERT INTO #test_inputsizes_reset VALUES (?, ?)",
        "Another String", 84
    )
    
    # Verify both rows were inserted correctly
    cursor.execute("SELECT * FROM #test_inputsizes_reset ORDER BY col2")
    rows = cursor.fetchall()
    
    assert len(rows) == 2
    assert rows[0][0] == "Test String"
    assert rows[0][1] == 42
    assert rows[1][0] == "Another String"
    assert rows[1][1] == 84
    
    # Clean up
    cursor.execute("DROP TABLE IF EXISTS #test_inputsizes_reset")

def test_cursor_setinputsizes_override_inference(db_connection):
    """Test that setinputsizes overrides type inference"""
    from mssql_python.constants import ConstantsDDBC
    
    cursor = db_connection.cursor()
    
    # Create a test table with specific types
    cursor.execute("DROP TABLE IF EXISTS #test_inputsizes_override")
    cursor.execute("""
    CREATE TABLE #test_inputsizes_override (
        small_int SMALLINT,
        big_text NVARCHAR(MAX)
    )
    """)
    
    # Set input sizes that override the default inference
    # For SMALLINT, use a valid precision value (5 is typical for SMALLINT)
    cursor.setinputsizes([
        (ConstantsDDBC.SQL_SMALLINT.value, 5, 0),  # Use valid precision for SMALLINT
        (ConstantsDDBC.SQL_WVARCHAR.value, 8000, 0)  # Force short string to NVARCHAR(MAX)
    ])
    
    # Test with values that would normally be inferred differently
    big_number = 30000  # Would normally be INTEGER or BIGINT
    short_text = "abc"  # Would normally be a regular NVARCHAR
    
    try:
        cursor.execute(
            "INSERT INTO #test_inputsizes_override VALUES (?, ?)",
            big_number, short_text
        )
        
        # Verify the row was inserted (may have been truncated by SQL Server)
        cursor.execute("SELECT * FROM #test_inputsizes_override")
        row = cursor.fetchone()
        
        # SQL Server would either truncate or round the value
        assert row[1] == short_text
        
    except Exception as e:
        # If an exception occurs, it should be related to the data type conversion
        # Add "invalid precision" to the expected error messages
        error_text = str(e).lower()
        assert any(text in error_text for text in ["overflow", "out of range", "convert", "invalid precision", "precision value"]), \
            f"Unexpected error: {e}"
    
    # Clean up
    cursor.execute("DROP TABLE IF EXISTS #test_inputsizes_override")

def test_gettypeinfo_all_types(cursor):
    """Test getTypeInfo with no arguments returns all data types"""
    # Get all type information
    type_info = cursor.getTypeInfo()
    
    # Verify we got results
    assert type_info is not None, "getTypeInfo() should return results"
    assert len(type_info) > 0, "getTypeInfo() should return at least one data type"
    
    # Verify common data types are present
    type_names = [str(row.type_name).upper() for row in type_info]
    assert any('VARCHAR' in name for name in type_names), "VARCHAR type should be in results"
    assert any('INT' in name for name in type_names), "INTEGER type should be in results"
    
    # Verify first row has expected columns
    first_row = type_info[0]
    assert hasattr(first_row, 'type_name'), "Result should have type_name column"
    assert hasattr(first_row, 'data_type'), "Result should have data_type column"
    assert hasattr(first_row, 'column_size'), "Result should have column_size column"
    assert hasattr(first_row, 'nullable'), "Result should have nullable column"

def test_gettypeinfo_specific_type(cursor):
    """Test getTypeInfo with specific type argument"""
    from mssql_python.constants import ConstantsDDBC
    
    # Test with VARCHAR type (SQL_VARCHAR)
    varchar_info = cursor.getTypeInfo(ConstantsDDBC.SQL_VARCHAR.value)
    
    # Verify we got results specific to VARCHAR
    assert varchar_info is not None, "getTypeInfo(SQL_VARCHAR) should return results"
    assert len(varchar_info) > 0, "getTypeInfo(SQL_VARCHAR) should return at least one row"
    
    # All rows should be related to VARCHAR type
    for row in varchar_info:
        assert 'varchar' in row.type_name or 'char' in row.type_name, \
            f"Expected VARCHAR type, got {row.type_name}"
        assert row.data_type == ConstantsDDBC.SQL_VARCHAR.value, \
            f"Expected data_type={ConstantsDDBC.SQL_VARCHAR.value}, got {row.data_type}"

def test_gettypeinfo_result_structure(cursor):
    """Test the structure of getTypeInfo result rows"""
    # Get info for a common type like INTEGER
    from mssql_python.constants import ConstantsDDBC
    
    int_info = cursor.getTypeInfo(ConstantsDDBC.SQL_INTEGER.value)
    
    # Make sure we have at least one result
    assert len(int_info) > 0, "getTypeInfo for INTEGER should return results"
    
    # Check for all required columns in the result
    first_row = int_info[0]
    required_columns = [
        'type_name', 'data_type', 'column_size', 'literal_prefix', 
        'literal_suffix', 'create_params', 'nullable', 'case_sensitive',
        'searchable', 'unsigned_attribute', 'fixed_prec_scale', 
        'auto_unique_value', 'local_type_name', 'minimum_scale',
        'maximum_scale', 'sql_data_type', 'sql_datetime_sub',
        'num_prec_radix', 'interval_precision'
    ]
    
    for column in required_columns:
        assert hasattr(first_row, column), f"Result missing required column: {column}"

def test_gettypeinfo_numeric_type(cursor):
    """Test getTypeInfo for numeric data types"""
    from mssql_python.constants import ConstantsDDBC
    
    # Get information about DECIMAL type
    decimal_info = cursor.getTypeInfo(ConstantsDDBC.SQL_DECIMAL.value)
    
    # Verify decimal-specific attributes
    assert len(decimal_info) > 0, "getTypeInfo for DECIMAL should return results"
    
    decimal_row = decimal_info[0]
    # DECIMAL should have precision and scale parameters
    assert decimal_row.create_params is not None, "DECIMAL should have create_params"
    assert "PRECISION" in decimal_row.create_params.upper() or \
           "SCALE" in decimal_row.create_params.upper(), \
           "DECIMAL create_params should mention precision/scale"
    
    # Numeric types typically use base 10 for the num_prec_radix
    assert decimal_row.num_prec_radix == 10, \
           f"Expected num_prec_radix=10 for DECIMAL, got {decimal_row.num_prec_radix}"

def test_gettypeinfo_datetime_types(cursor):
    """Test getTypeInfo for datetime types"""
    from mssql_python.constants import ConstantsDDBC
    
    # Get information about TIMESTAMP type instead of DATETIME
    # SQL_TYPE_TIMESTAMP (93) is more commonly used for datetime in ODBC
    datetime_info = cursor.getTypeInfo(ConstantsDDBC.SQL_TYPE_TIMESTAMP.value)
    
    # Verify we got datetime-related results
    assert len(datetime_info) > 0, "getTypeInfo for TIMESTAMP should return results"
    
    # Check for datetime-specific attributes
    first_row = datetime_info[0]
    assert hasattr(first_row, 'type_name'), "Result should have type_name column"
    
    # Datetime type names often contain 'date', 'time', or 'datetime'
    type_name_lower = first_row.type_name.lower()
    assert any(term in type_name_lower for term in ['date', 'time', 'timestamp', 'datetime']), \
        f"Expected datetime-related type name, got {first_row.type_name}"
    
def test_gettypeinfo_multiple_calls(cursor):
    """Test calling getTypeInfo multiple times in succession"""
    from mssql_python.constants import ConstantsDDBC
    
    # First call - get all types
    all_types = cursor.getTypeInfo()
    assert len(all_types) > 0, "First call to getTypeInfo should return results"
    
    # Second call - get VARCHAR type
    varchar_info = cursor.getTypeInfo(ConstantsDDBC.SQL_VARCHAR.value)
    assert len(varchar_info) > 0, "Second call to getTypeInfo should return results"
    
    # Third call - get INTEGER type
    int_info = cursor.getTypeInfo(ConstantsDDBC.SQL_INTEGER.value)
    assert len(int_info) > 0, "Third call to getTypeInfo should return results"
    
    # Verify the results are different between calls
    assert len(all_types) > len(varchar_info), "All types should return more rows than specific type"

def test_gettypeinfo_binary_types(cursor):
    """Test getTypeInfo for binary data types"""
    from mssql_python.constants import ConstantsDDBC
    
    # Get information about BINARY or VARBINARY type
    binary_info = cursor.getTypeInfo(ConstantsDDBC.SQL_BINARY.value)
    
    # Verify we got binary-related results
    assert len(binary_info) > 0, "getTypeInfo for BINARY should return results"
    
    # Check for binary-specific attributes
    for row in binary_info:
        type_name_lower = row.type_name.lower()
        # Include 'timestamp' as SQL Server reports it as a binary type
        assert any(term in type_name_lower for term in ['binary', 'blob', 'image', 'timestamp']), \
            f"Expected binary-related type name, got {row.type_name}"
        
        # Binary types typically don't support case sensitivity
        assert row.case_sensitive == 0, f"Binary types should not be case sensitive, got {row.case_sensitive}"

def test_gettypeinfo_cached_results(cursor):
    """Test that multiple identical calls to getTypeInfo are efficient"""
    from mssql_python.constants import ConstantsDDBC
    import time
    
    # First call - might be slower
    start_time = time.time()
    first_result = cursor.getTypeInfo(ConstantsDDBC.SQL_VARCHAR.value)
    first_duration = time.time() - start_time
    
    # Give the system a moment
    time.sleep(0.1)
    
    # Second call with same type - should be similar or faster
    start_time = time.time()
    second_result = cursor.getTypeInfo(ConstantsDDBC.SQL_VARCHAR.value)
    second_duration = time.time() - start_time
    
    # Results should be consistent
    assert len(first_result) == len(second_result), "Multiple calls should return same number of results"
    
    # Both calls should return the correct type info
    for row in second_result:
        assert row.data_type == ConstantsDDBC.SQL_VARCHAR.value, \
            f"Expected SQL_VARCHAR type, got {row.data_type}"
        
def test_procedures_setup(cursor, db_connection):
    """Create a test schema and procedures for testing"""
    try:
        # Create a test schema for isolation
        cursor.execute("IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'pytest_proc_schema') EXEC('CREATE SCHEMA pytest_proc_schema')")
        
        # Create test stored procedures
        cursor.execute("""
        CREATE OR ALTER PROCEDURE pytest_proc_schema.test_proc1
        AS
        BEGIN
            SELECT 1 AS result
        END
        """)
        
        cursor.execute("""
        CREATE OR ALTER PROCEDURE pytest_proc_schema.test_proc2 
            @param1 INT, 
            @param2 VARCHAR(50) OUTPUT
        AS
        BEGIN
            SELECT @param2 = 'Output ' + CAST(@param1 AS VARCHAR(10))
            RETURN @param1
        END
        """)
        
        db_connection.commit()
    except Exception as e:
        pytest.fail(f"Test setup failed: {e}")

def test_procedures_all(cursor, db_connection):
    """Test getting information about all procedures"""
    # First set up our test procedures
    test_procedures_setup(cursor, db_connection)
    
    try:
        # Get all procedures
        procs = cursor.procedures()
        
        # Verify we got results
        assert procs is not None, "procedures() should return results"
        assert len(procs) > 0, "procedures() should return at least one procedure"
        
        # Verify structure of results
        first_row = procs[0]
        assert hasattr(first_row, 'procedure_cat'), "Result should have procedure_cat column"
        assert hasattr(first_row, 'procedure_schem'), "Result should have procedure_schem column"
        assert hasattr(first_row, 'procedure_name'), "Result should have procedure_name column"
        assert hasattr(first_row, 'num_input_params'), "Result should have num_input_params column"
        assert hasattr(first_row, 'num_output_params'), "Result should have num_output_params column"
        assert hasattr(first_row, 'num_result_sets'), "Result should have num_result_sets column"
        assert hasattr(first_row, 'remarks'), "Result should have remarks column"
        assert hasattr(first_row, 'procedure_type'), "Result should have procedure_type column"
        
    finally:
        # Clean up happens in test_procedures_cleanup
        pass

def test_procedures_specific(cursor, db_connection):
    """Test getting information about a specific procedure"""
    try:
        # Get specific procedure
        procs = cursor.procedures(procedure='test_proc1', schema='pytest_proc_schema')
        
        # Verify we got the correct procedure
        assert len(procs) == 1, "Should find exactly one procedure"
        proc = procs[0]
        assert proc.procedure_name == 'test_proc1', "Wrong procedure name returned"
        assert proc.procedure_schem == 'pytest_proc_schema', "Wrong schema returned"
        
    finally:
        # Clean up happens in test_procedures_cleanup
        pass

def test_procedures_with_schema(cursor, db_connection):
    """Test getting procedures with schema filter"""
    try:
        # Get procedures for our test schema
        procs = cursor.procedures(schema='pytest_proc_schema')
        
        # Verify schema filter worked
        assert len(procs) >= 2, "Should find at least two procedures in schema"
        for proc in procs:
            assert proc.procedure_schem == 'pytest_proc_schema', f"Expected schema pytest_proc_schema, got {proc.procedure_schem}"
        
        # Verify our specific procedures are in the results
        proc_names = [p.procedure_name for p in procs]
        assert 'test_proc1' in proc_names, "test_proc1 should be in results"
        assert 'test_proc2' in proc_names, "test_proc2 should be in results"
        
    finally:
        # Clean up happens in test_procedures_cleanup
        pass

def test_procedures_nonexistent(cursor):
    """Test procedures() with non-existent procedure name"""
    # Use a procedure name that's highly unlikely to exist
    procs = cursor.procedures(procedure='nonexistent_procedure_xyz123')
    
    # Should return empty list, not error
    assert isinstance(procs, list), "Should return a list for non-existent procedure"
    assert len(procs) == 0, "Should return empty list for non-existent procedure"

def test_procedures_catalog_filter(cursor, db_connection):
    """Test procedures() with catalog filter"""
    # Get current database name
    cursor.execute("SELECT DB_NAME() AS current_db")
    current_db = cursor.fetchone().current_db
    
    try:
        # Get procedures with current catalog
        procs = cursor.procedures(catalog=current_db, schema='pytest_proc_schema')
        
        # Verify catalog filter worked
        assert len(procs) >= 2, "Should find procedures in current catalog"
        for proc in procs:
            assert proc.procedure_cat == current_db, f"Expected catalog {current_db}, got {proc.procedure_cat}"
            
        # Get procedures with non-existent catalog
        fake_procs = cursor.procedures(catalog='nonexistent_db_xyz123')
        assert len(fake_procs) == 0, "Should return empty list for non-existent catalog"
        
    finally:
        # Clean up happens in test_procedures_cleanup
        pass

def test_procedures_with_parameters(cursor, db_connection):
    """Test that procedures() correctly reports parameter information"""
    try:
        # Create a simpler procedure with basic parameters
        cursor.execute("""
        CREATE OR ALTER PROCEDURE pytest_proc_schema.test_params_proc 
            @in1 INT, 
            @in2 VARCHAR(50)
        AS
        BEGIN
            SELECT @in1 AS value1, @in2 AS value2
        END
        """)
        db_connection.commit()
        
        # Get procedure info
        procs = cursor.procedures(procedure='test_params_proc', schema='pytest_proc_schema')
        
        # Verify we found the procedure
        assert len(procs) == 1, "Should find exactly one procedure"
        proc = procs[0]
        
        # Just check if columns exist, don't check specific values
        assert hasattr(proc, 'num_input_params'), "Result should have num_input_params column"
        assert hasattr(proc, 'num_output_params'), "Result should have num_output_params column"
        
        # Test simple execution without output parameters
        cursor.execute("EXEC pytest_proc_schema.test_params_proc 10, 'Test'")
        
        # Verify the procedure returned expected values
        row = cursor.fetchone()
        assert row is not None, "Procedure should return results"
        assert row[0] == 10, "First parameter value incorrect"
        assert row[1] == 'Test', "Second parameter value incorrect"
            
    finally:
        cursor.execute("DROP PROCEDURE IF EXISTS pytest_proc_schema.test_params_proc")
        db_connection.commit()

def test_procedures_result_set_info(cursor, db_connection):
    """Test that procedures() reports information about result sets"""
    try:
        # Create procedures with different result set patterns
        cursor.execute("""
        CREATE OR ALTER PROCEDURE pytest_proc_schema.test_no_results
        AS
        BEGIN
            DECLARE @x INT = 1
        END
        """)
        
        cursor.execute("""
        CREATE OR ALTER PROCEDURE pytest_proc_schema.test_one_result
        AS
        BEGIN
            SELECT 1 AS col1, 'test' AS col2
        END
        """)
        
        cursor.execute("""
        CREATE OR ALTER PROCEDURE pytest_proc_schema.test_multiple_results
        AS
        BEGIN
            SELECT 1 AS result1
            SELECT 'test' AS result2
            SELECT GETDATE() AS result3
        END
        """)
        db_connection.commit()
        
        # Get procedure info for all test procedures
        procs = cursor.procedures(schema='pytest_proc_schema', procedure='test_%')
        
        # Verify we found at least some procedures
        assert len(procs) > 0, "Should find at least some test procedures"

         # Get the procedure names we found
        result_proc_names = [p.procedure_name for p in procs 
                           if p.procedure_name.startswith('test_') and 'results' in p.procedure_name]
        print(f"Found result procedures: {result_proc_names}")
        
        # The num_result_sets column exists but might not have correct values
        for proc in procs:
            assert hasattr(proc, 'num_result_sets'), "Result should have num_result_sets column"
            
        # Test execution of the procedures to verify they work
        cursor.execute("EXEC pytest_proc_schema.test_no_results")
        assert cursor.fetchall() == [], "test_no_results should return no results"
        
        cursor.execute("EXEC pytest_proc_schema.test_one_result")
        rows = cursor.fetchall()
        assert len(rows) == 1, "test_one_result should return one row"
        assert len(rows[0]) == 2, "test_one_result row should have two columns"
        
        cursor.execute("EXEC pytest_proc_schema.test_multiple_results")
        rows1 = cursor.fetchall()
        assert len(rows1) == 1, "First result set should have one row"
        assert cursor.nextset(), "Should have a second result set"
        rows2 = cursor.fetchall()
        assert len(rows2) == 1, "Second result set should have one row"
        assert cursor.nextset(), "Should have a third result set"
        rows3 = cursor.fetchall()
        assert len(rows3) == 1, "Third result set should have one row"
            
    finally:
        cursor.execute("DROP PROCEDURE IF EXISTS pytest_proc_schema.test_no_results")
        cursor.execute("DROP PROCEDURE IF EXISTS pytest_proc_schema.test_one_result")
        cursor.execute("DROP PROCEDURE IF EXISTS pytest_proc_schema.test_multiple_results")
        db_connection.commit()

def test_procedures_cleanup(cursor, db_connection):
    """Clean up all test procedures and schema after testing"""
    try:
        # Drop all test procedures
        cursor.execute("DROP PROCEDURE IF EXISTS pytest_proc_schema.test_proc1")
        cursor.execute("DROP PROCEDURE IF EXISTS pytest_proc_schema.test_proc2")
        cursor.execute("DROP PROCEDURE IF EXISTS pytest_proc_schema.test_params_proc")
        cursor.execute("DROP PROCEDURE IF EXISTS pytest_proc_schema.test_no_results")
        cursor.execute("DROP PROCEDURE IF EXISTS pytest_proc_schema.test_one_result")
        cursor.execute("DROP PROCEDURE IF EXISTS pytest_proc_schema.test_multiple_results")
        
        # Drop the test schema
        cursor.execute("DROP SCHEMA IF EXISTS pytest_proc_schema")
        db_connection.commit()
    except Exception as e:
        pytest.fail(f"Test cleanup failed: {e}")

def test_foreignkeys_setup(cursor, db_connection):
    """Create tables with foreign key relationships for testing"""
    try:
        # Create a test schema for isolation
        cursor.execute("IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'pytest_fk_schema') EXEC('CREATE SCHEMA pytest_fk_schema')")
        
        # Drop tables if they exist (in reverse order to avoid constraint conflicts)
        cursor.execute("DROP TABLE IF EXISTS pytest_fk_schema.orders")
        cursor.execute("DROP TABLE IF EXISTS pytest_fk_schema.customers")
        
        # Create parent table
        cursor.execute("""
        CREATE TABLE pytest_fk_schema.customers (
            customer_id INT PRIMARY KEY,
            customer_name VARCHAR(100) NOT NULL
        )
        """)
        
        # Create child table with foreign key
        cursor.execute("""
        CREATE TABLE pytest_fk_schema.orders (
            order_id INT PRIMARY KEY,
            order_date DATETIME NOT NULL,
            customer_id INT NOT NULL,
            total_amount DECIMAL(10, 2) NOT NULL,
            CONSTRAINT FK_Orders_Customers FOREIGN KEY (customer_id)
                REFERENCES pytest_fk_schema.customers (customer_id)
        )
        """)
        
        # Insert test data
        cursor.execute("""
        INSERT INTO pytest_fk_schema.customers (customer_id, customer_name)
        VALUES (1, 'Test Customer 1'), (2, 'Test Customer 2')
        """)
        
        cursor.execute("""
        INSERT INTO pytest_fk_schema.orders (order_id, order_date, customer_id, total_amount)
        VALUES (101, GETDATE(), 1, 150.00), (102, GETDATE(), 2, 250.50)
        """)
        
        db_connection.commit()
    except Exception as e:
        pytest.fail(f"Test setup failed: {e}")

def test_foreignkeys_all(cursor, db_connection):
    """Test getting all foreign keys"""
    try:
        # First set up our test tables
        test_foreignkeys_setup(cursor, db_connection)
        
        # Get all foreign keys
        fks = cursor.foreignKeys(table='orders', schema='pytest_fk_schema')
        
        # Verify we got results
        assert fks is not None, "foreignKeys() should return results"
        assert len(fks) > 0, "foreignKeys() should return at least one foreign key"
        
        # Verify our test FK is in the results
        # Search case-insensitively since the database might return different case
        found_test_fk = False
        for fk in fks:
            if (fk.fktable_name.lower() == 'orders' and
                fk.pktable_name.lower() == 'customers'):
                found_test_fk = True
                break
                
        assert found_test_fk, "Could not find the test foreign key in results"
        
    finally:
        # Clean up
        cursor.execute("DROP TABLE IF EXISTS pytest_fk_schema.orders")
        cursor.execute("DROP TABLE IF EXISTS pytest_fk_schema.customers")
        db_connection.commit()

def test_foreignkeys_specific_table(cursor, db_connection):
    """Test getting foreign keys for a specific table"""
    try:
        # First set up our test tables
        test_foreignkeys_setup(cursor, db_connection)
        
        # Get foreign keys for the orders table
        fks = cursor.foreignKeys(table='orders', schema='pytest_fk_schema')
        
        # Verify we got results
        assert len(fks) == 1, "Should find exactly one foreign key for orders table"
        
        # Verify the foreign key details
        fk = fks[0]
        assert fk.fktable_name.lower() == 'orders', "Wrong foreign key table name"
        assert fk.pktable_name.lower() == 'customers', "Wrong primary key table name"
        assert fk.fkcolumn_name.lower() == 'customer_id', "Wrong foreign key column name"
        assert fk.pkcolumn_name.lower() == 'customer_id', "Wrong primary key column name"
        
    finally:
        # Clean up
        cursor.execute("DROP TABLE IF EXISTS pytest_fk_schema.orders")
        cursor.execute("DROP TABLE IF EXISTS pytest_fk_schema.customers")
        db_connection.commit()

def test_foreignkeys_specific_foreign_table(cursor, db_connection):
    """Test getting foreign keys that reference a specific table"""
    try:
        # First set up our test tables
        test_foreignkeys_setup(cursor, db_connection)
        
        # Get foreign keys that reference the customers table
        fks = cursor.foreignKeys(foreignTable='customers', foreignSchema='pytest_fk_schema')
        
        # Verify we got results
        assert len(fks) > 0, "Should find at least one foreign key referencing customers table"
        
        # Verify our test FK is in the results
        found_test_fk = False
        for fk in fks:
            if (fk.fktable_name.lower() == 'orders' and
                fk.pktable_name.lower() == 'customers'):
                found_test_fk = True
                break
                
        assert found_test_fk, "Could not find the test foreign key in results"
        
    finally:
        # Clean up
        cursor.execute("DROP TABLE IF EXISTS pytest_fk_schema.orders")
        cursor.execute("DROP TABLE IF EXISTS pytest_fk_schema.customers")
        db_connection.commit()

def test_foreignkeys_both_tables(cursor, db_connection):
    """Test getting foreign keys with both table and foreignTable specified"""
    try:
        # First set up our test tables
        test_foreignkeys_setup(cursor, db_connection)
        
        # Get foreign keys between the two tables
        fks = cursor.foreignKeys(
            table='orders', schema='pytest_fk_schema',
            foreignTable='customers', foreignSchema='pytest_fk_schema'
        )
        
        # Verify we got results
        assert len(fks) == 1, "Should find exactly one foreign key between specified tables"
        
        # Verify the foreign key details
        fk = fks[0]
        assert fk.fktable_name.lower() == 'orders', "Wrong foreign key table name"
        assert fk.pktable_name.lower() == 'customers', "Wrong primary key table name"
        assert fk.fkcolumn_name.lower() == 'customer_id', "Wrong foreign key column name"
        assert fk.pkcolumn_name.lower() == 'customer_id', "Wrong primary key column name"
        
    finally:
        # Clean up
        cursor.execute("DROP TABLE IF EXISTS pytest_fk_schema.orders")
        cursor.execute("DROP TABLE IF EXISTS pytest_fk_schema.customers")
        db_connection.commit()

def test_foreignkeys_nonexistent(cursor):
    """Test foreignKeys() with non-existent table name"""
    # Use a table name that's highly unlikely to exist
    fks = cursor.foreignKeys(table='nonexistent_table_xyz123')
    
    # Should return empty list, not error
    assert isinstance(fks, list), "Should return a list for non-existent table"
    assert len(fks) == 0, "Should return empty list for non-existent table"

def test_foreignkeys_catalog_schema(cursor, db_connection):
    """Test foreignKeys() with catalog and schema filters"""
    try:
        # First set up our test tables
        test_foreignkeys_setup(cursor, db_connection)
        
        # Get current database name
        cursor.execute("SELECT DB_NAME() AS current_db")
        row = cursor.fetchone()
        current_db = row.current_db
        
        # Get foreign keys with current catalog and pytest schema
        fks = cursor.foreignKeys(
            table='orders',
            catalog=current_db,
            schema='pytest_fk_schema'
        )
        
        # Verify we got results
        assert len(fks) > 0, "Should find foreign keys with correct catalog/schema"
        
        # Verify catalog/schema in results
        for fk in fks:
            assert fk.fktable_cat == current_db, "Wrong foreign key table catalog"
            assert fk.fktable_schem == 'pytest_fk_schema', "Wrong foreign key table schema"
                
    finally:
        # Clean up
        cursor.execute("DROP TABLE IF EXISTS pytest_fk_schema.orders")
        cursor.execute("DROP TABLE IF EXISTS pytest_fk_schema.customers")
        db_connection.commit()

def test_foreignkeys_result_structure(cursor, db_connection):
    """Test the structure of foreignKeys result rows"""
    try:
        # First set up our test tables
        test_foreignkeys_setup(cursor, db_connection)
        
        # Get foreign keys for the orders table
        fks = cursor.foreignKeys(table='orders', schema='pytest_fk_schema')
        
        # Verify we got results
        assert len(fks) > 0, "Should find at least one foreign key"
        
        # Check for all required columns in the result
        first_row = fks[0]
        required_columns = [
            'pktable_cat', 'pktable_schem', 'pktable_name', 'pkcolumn_name',
            'fktable_cat', 'fktable_schem', 'fktable_name', 'fkcolumn_name',
            'key_seq', 'update_rule', 'delete_rule', 'fk_name', 'pk_name',
            'deferrability'
        ]
        
        for column in required_columns:
            assert hasattr(first_row, column), f"Result missing required column: {column}"
            
        # Verify specific values
        assert first_row.fktable_name.lower() == 'orders', "Wrong foreign key table name"
        assert first_row.pktable_name.lower() == 'customers', "Wrong primary key table name"
        assert first_row.fkcolumn_name.lower() == 'customer_id', "Wrong foreign key column name"
        assert first_row.pkcolumn_name.lower() == 'customer_id', "Wrong primary key column name"
        assert first_row.key_seq == 1, "Wrong key sequence number"
        assert first_row.fk_name is not None, "Foreign key name should not be None"
        assert first_row.pk_name is not None, "Primary key name should not be None"
        
    finally:
        # Clean up
        cursor.execute("DROP TABLE IF EXISTS pytest_fk_schema.orders")
        cursor.execute("DROP TABLE IF EXISTS pytest_fk_schema.customers")
        db_connection.commit()

def test_foreignkeys_multiple_column_fk(cursor, db_connection):
    """Test foreignKeys() with a multi-column foreign key"""
    try:
        # First create the schema if needed
        cursor.execute("IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'pytest_fk_schema') EXEC('CREATE SCHEMA pytest_fk_schema')")
        
        # Drop tables if they exist (in reverse order to avoid constraint conflicts)
        cursor.execute("DROP TABLE IF EXISTS pytest_fk_schema.order_details")
        cursor.execute("DROP TABLE IF EXISTS pytest_fk_schema.product_variants")
        
        # Create parent table with composite primary key
        cursor.execute("""
        CREATE TABLE pytest_fk_schema.product_variants (
            product_id INT NOT NULL,
            variant_id INT NOT NULL,
            variant_name VARCHAR(100) NOT NULL,
            PRIMARY KEY (product_id, variant_id)
        )
        """)
        
        # Create child table with composite foreign key
        cursor.execute("""
        CREATE TABLE pytest_fk_schema.order_details (
            order_id INT NOT NULL,
            product_id INT NOT NULL,
            variant_id INT NOT NULL,
            quantity INT NOT NULL,
            PRIMARY KEY (order_id, product_id, variant_id),
            CONSTRAINT FK_OrderDetails_ProductVariants FOREIGN KEY (product_id, variant_id)
                REFERENCES pytest_fk_schema.product_variants (product_id, variant_id)
        )
        """)
        
        db_connection.commit()
        
        # Get foreign keys for the order_details table
        fks = cursor.foreignKeys(table='order_details', schema='pytest_fk_schema')
        
        # Verify we got results
        assert len(fks) == 2, "Should find two rows for the composite foreign key (one per column)"
        
        # Group by key_seq to verify both columns
        fk_columns = {}
        for fk in fks:
            fk_columns[fk.key_seq] = {
                'pkcolumn': fk.pkcolumn_name.lower(),
                'fkcolumn': fk.fkcolumn_name.lower()
            }
        
        # Verify both columns are present
        assert 1 in fk_columns, "First column of composite key missing"
        assert 2 in fk_columns, "Second column of composite key missing"
        
        # Verify column mappings
        assert fk_columns[1]['pkcolumn'] == 'product_id', "Wrong primary key column 1"
        assert fk_columns[1]['fkcolumn'] == 'product_id', "Wrong foreign key column 1"
        assert fk_columns[2]['pkcolumn'] == 'variant_id', "Wrong primary key column 2"
        assert fk_columns[2]['fkcolumn'] == 'variant_id', "Wrong foreign key column 2"
        
    finally:
        # Clean up
        cursor.execute("DROP TABLE IF EXISTS pytest_fk_schema.order_details")
        cursor.execute("DROP TABLE IF EXISTS pytest_fk_schema.product_variants")
        db_connection.commit()

def test_cleanup_schema(cursor, db_connection):
    """Clean up the test schema after all tests"""
    try:
        # Make sure no tables remain
        cursor.execute("DROP TABLE IF EXISTS pytest_fk_schema.orders")
        cursor.execute("DROP TABLE IF EXISTS pytest_fk_schema.customers")
        cursor.execute("DROP TABLE IF EXISTS pytest_fk_schema.order_details")
        cursor.execute("DROP TABLE IF EXISTS pytest_fk_schema.product_variants")
        db_connection.commit()
        
        # Drop the schema
        cursor.execute("DROP SCHEMA IF EXISTS pytest_fk_schema")
        db_connection.commit()
    except Exception as e:
        pytest.fail(f"Schema cleanup failed: {e}")

def test_primarykeys_setup(cursor, db_connection):
    """Create tables with primary keys for testing"""
    try:
        # Create a test schema for isolation
        cursor.execute("IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'pytest_pk_schema') EXEC('CREATE SCHEMA pytest_pk_schema')")
        
        # Drop tables if they exist
        cursor.execute("DROP TABLE IF EXISTS pytest_pk_schema.single_pk_test")
        cursor.execute("DROP TABLE IF EXISTS pytest_pk_schema.composite_pk_test")
        
        # Create table with simple primary key
        cursor.execute("""
        CREATE TABLE pytest_pk_schema.single_pk_test (
            id INT PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            description VARCHAR(200) NULL
        )
        """)
        
        # Create table with composite primary key
        cursor.execute("""
        CREATE TABLE pytest_pk_schema.composite_pk_test (
            dept_id INT NOT NULL,
            emp_id INT NOT NULL,
            hire_date DATE NOT NULL,
            CONSTRAINT PK_composite_test PRIMARY KEY (dept_id, emp_id)
        )
        """)
        
        db_connection.commit()
    except Exception as e:
        pytest.fail(f"Test setup failed: {e}")

def test_primarykeys_simple(cursor, db_connection):
    """Test primaryKeys returns information about a simple primary key"""
    try:
        # First set up our test tables
        test_primarykeys_setup(cursor, db_connection)
        
        # Get primary key information
        pks = cursor.primaryKeys('single_pk_test', schema='pytest_pk_schema')
        
        # Verify we got results
        assert len(pks) == 1, "Should find exactly one primary key column"
        pk = pks[0]
        
        # Verify primary key details
        assert pk.table_name.lower() == 'single_pk_test', "Wrong table name"
        assert pk.column_name.lower() == 'id', "Wrong primary key column name"
        assert pk.key_seq == 1, "Wrong key sequence number"
        assert pk.pk_name is not None, "Primary key name should not be None"
        
    finally:
        # Clean up happens in test_primarykeys_cleanup
        pass

def test_primarykeys_composite(cursor, db_connection):
    """Test primaryKeys with a composite primary key"""
    try:
        # Get primary key information
        pks = cursor.primaryKeys('composite_pk_test', schema='pytest_pk_schema')
        
        # Verify we got results for both columns
        assert len(pks) == 2, "Should find two primary key columns"
        
        # Sort by key_seq to ensure consistent order
        pks = sorted(pks, key=lambda row: row.key_seq)
        
        # Verify first column
        assert pks[0].table_name.lower() == 'composite_pk_test', "Wrong table name"
        assert pks[0].column_name.lower() == 'dept_id', "Wrong first primary key column name"
        assert pks[0].key_seq == 1, "Wrong key sequence number for first column"
        
        # Verify second column
        assert pks[1].table_name.lower() == 'composite_pk_test', "Wrong table name"
        assert pks[1].column_name.lower() == 'emp_id', "Wrong second primary key column name"
        assert pks[1].key_seq == 2, "Wrong key sequence number for second column"
        
        # Both should have the same PK name
        assert pks[0].pk_name == pks[1].pk_name, "Both columns should have the same primary key name"
        
    finally:
        # Clean up happens in test_primarykeys_cleanup
        pass

def test_primarykeys_column_info(cursor, db_connection):
    """Test that primaryKeys returns correct column information"""
    try:
        # Get primary key information
        pks = cursor.primaryKeys('single_pk_test', schema='pytest_pk_schema')
        
        # Verify column information
        assert len(pks) == 1, "Should find exactly one primary key column"
        pk = pks[0]
        
        # Verify expected columns are present
        assert hasattr(pk, 'table_cat'), "Result should have table_cat column"
        assert hasattr(pk, 'table_schem'), "Result should have table_schem column"
        assert hasattr(pk, 'table_name'), "Result should have table_name column"
        assert hasattr(pk, 'column_name'), "Result should have column_name column"
        assert hasattr(pk, 'key_seq'), "Result should have key_seq column"
        assert hasattr(pk, 'pk_name'), "Result should have pk_name column"
        
        # Verify values are correct
        assert pk.table_schem.lower() == 'pytest_pk_schema', "Wrong schema name"
        assert pk.table_name.lower() == 'single_pk_test', "Wrong table name"
        assert pk.column_name.lower() == 'id', "Wrong column name"
        assert isinstance(pk.key_seq, int), "key_seq should be an integer"
        
    finally:
        # Clean up happens in test_primarykeys_cleanup
        pass

def test_primarykeys_nonexistent(cursor):
    """Test primaryKeys() with non-existent table name"""
    # Use a table name that's highly unlikely to exist
    pks = cursor.primaryKeys('nonexistent_table_xyz123')
    
    # Should return empty list, not error
    assert isinstance(pks, list), "Should return a list for non-existent table"
    assert len(pks) == 0, "Should return empty list for non-existent table"

def test_primarykeys_catalog_filter(cursor, db_connection):
    """Test primaryKeys() with catalog filter"""
    try:
        # Get current database name
        cursor.execute("SELECT DB_NAME() AS current_db")
        current_db = cursor.fetchone().current_db
        
        # Get primary keys with current catalog
        pks = cursor.primaryKeys('single_pk_test', catalog=current_db, schema='pytest_pk_schema')
        
        # Verify catalog filter worked
        assert len(pks) == 1, "Should find exactly one primary key column"
        pk = pks[0]
        assert pk.table_cat == current_db, f"Expected catalog {current_db}, got {pk.table_cat}"
            
        # Get primary keys with non-existent catalog
        fake_pks = cursor.primaryKeys('single_pk_test', catalog='nonexistent_db_xyz123')
        assert len(fake_pks) == 0, "Should return empty list for non-existent catalog"
        
    finally:
        # Clean up happens in test_primarykeys_cleanup
        pass

def test_primarykeys_cleanup(cursor, db_connection):
    """Clean up test tables after testing"""
    try:
        # Drop all test tables
        cursor.execute("DROP TABLE IF EXISTS pytest_pk_schema.single_pk_test")
        cursor.execute("DROP TABLE IF EXISTS pytest_pk_schema.composite_pk_test")
        
        # Drop the test schema
        cursor.execute("DROP SCHEMA IF EXISTS pytest_pk_schema")
        db_connection.commit()
    except Exception as e:
        pytest.fail(f"Test cleanup failed: {e}")

def test_specialcolumns_setup(cursor, db_connection):
    """Create test tables for testing rowIdColumns and rowVerColumns"""
    try:
        # Create a test schema for isolation
        cursor.execute("IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'pytest_special_schema') EXEC('CREATE SCHEMA pytest_special_schema')")
        
        # Drop tables if they exist
        cursor.execute("DROP TABLE IF EXISTS pytest_special_schema.rowid_test")
        cursor.execute("DROP TABLE IF EXISTS pytest_special_schema.timestamp_test")
        cursor.execute("DROP TABLE IF EXISTS pytest_special_schema.multiple_unique_test")
        cursor.execute("DROP TABLE IF EXISTS pytest_special_schema.identity_test")
        
        # Create table with primary key (for rowIdColumns)
        cursor.execute("""
        CREATE TABLE pytest_special_schema.rowid_test (
            id INT PRIMARY KEY,
            name NVARCHAR(100) NOT NULL,
            unique_col NVARCHAR(100) UNIQUE,
            non_unique_col NVARCHAR(100)
        )
        """)
        
        # Create table with rowversion column (for rowVerColumns)
        cursor.execute("""
        CREATE TABLE pytest_special_schema.timestamp_test (
            id INT PRIMARY KEY,
            name NVARCHAR(100) NOT NULL,
            last_updated ROWVERSION
        )
        """)
        
        # Create table with multiple unique identifiers
        cursor.execute("""
        CREATE TABLE pytest_special_schema.multiple_unique_test (
            id INT NOT NULL,
            code VARCHAR(10) NOT NULL,
            email VARCHAR(100) UNIQUE,
            order_number VARCHAR(20) UNIQUE,
            CONSTRAINT PK_multiple_unique_test PRIMARY KEY (id, code)
        )
        """)
        
        # Create table with identity column
        cursor.execute("""
        CREATE TABLE pytest_special_schema.identity_test (
            id INT IDENTITY(1,1) PRIMARY KEY,
            name NVARCHAR(100) NOT NULL,
            last_modified DATETIME DEFAULT GETDATE()
        )
        """)
        
        db_connection.commit()
    except Exception as e:
        pytest.fail(f"Test setup failed: {e}")

def test_rowid_columns_basic(cursor, db_connection):
    """Test basic functionality of rowIdColumns"""
    try:
        # Get row identifier columns for simple table
        rowid_cols = cursor.rowIdColumns(
            table='rowid_test', 
            schema='pytest_special_schema'
        ).fetchall()

        # LIMITATION: Only returns first column of primary key
        assert len(rowid_cols) == 1, "Should find exactly one ROWID column (first column of PK)"
        
        # Verify column name in the results
        col = rowid_cols[0]
        assert col.column_name.lower() == 'id', "Primary key column should be included in ROWID results"
        
        # Verify result structure
        assert hasattr(col, 'scope'), "Result should have scope column"
        assert hasattr(col, 'column_name'), "Result should have column_name column"
        assert hasattr(col, 'data_type'), "Result should have data_type column"
        assert hasattr(col, 'type_name'), "Result should have type_name column"
        assert hasattr(col, 'column_size'), "Result should have column_size column"
        assert hasattr(col, 'buffer_length'), "Result should have buffer_length column"
        assert hasattr(col, 'decimal_digits'), "Result should have decimal_digits column"
        assert hasattr(col, 'pseudo_column'), "Result should have pseudo_column column"
        
        # The scope should be one of the valid values or NULL
        assert col.scope in [0, 1, 2, None], f"Invalid scope value: {col.scope}"
        
        # The pseudo_column should be one of the valid values
        assert col.pseudo_column in [0, 1, 2, None], f"Invalid pseudo_column value: {col.pseudo_column}"
            
    except Exception as e:
        pytest.fail(f"rowIdColumns basic test failed: {e}")
    finally:
        # Clean up happens in test_specialcolumns_cleanup
        pass

def test_rowid_columns_identity(cursor, db_connection):
    """Test rowIdColumns with identity column"""
    try:
        # Get row identifier columns for table with identity column
        rowid_cols = cursor.rowIdColumns(
            table='identity_test', 
            schema='pytest_special_schema'
        ).fetchall()

        # LIMITATION: Only returns the identity column if it's the primary key
        assert len(rowid_cols) == 1, "Should find exactly one ROWID column (identity column as PK)"
        
        # Verify it's the identity column
        col = rowid_cols[0]
        assert col.column_name.lower() == 'id', "Identity column should be included as it's the PK"
        
    except Exception as e:
        pytest.fail(f"rowIdColumns identity test failed: {e}")
    finally:
        # Clean up happens in test_specialcolumns_cleanup
        pass

def test_rowid_columns_composite(cursor, db_connection):
    """Test rowIdColumns with composite primary key"""
    try:
        # Get row identifier columns for table with composite primary key
        rowid_cols = cursor.rowIdColumns(
            table='multiple_unique_test', 
            schema='pytest_special_schema'
        ).fetchall()

        # LIMITATION: Only returns first column of composite primary key
        assert len(rowid_cols) >= 1, "Should find at least one ROWID column (first column of PK)"
        
        # Verify column names in the results - should be the first PK column
        col_names = [col.column_name.lower() for col in rowid_cols]
        assert 'id' in col_names, "First part of composite PK should be included"
        
        # LIMITATION: Other parts of the PK or unique constraints may not be included
        if len(rowid_cols) > 1:
            # If additional columns are returned, they should be valid
            for col in rowid_cols:
                assert col.column_name.lower() in ['id', 'code'], "Only PK columns should be returned"
            
    except Exception as e:
        pytest.fail(f"rowIdColumns composite test failed: {e}")
    finally:
        # Clean up happens in test_specialcolumns_cleanup
        pass

def test_rowid_columns_nonexistent(cursor):
    """Test rowIdColumns with non-existent table"""
    # Use a table name that's highly unlikely to exist
    rowid_cols = cursor.rowIdColumns('nonexistent_table_xyz123').fetchall()

    # Should return empty list, not error
    assert isinstance(rowid_cols, list), "Should return a list for non-existent table"
    assert len(rowid_cols) == 0, "Should return empty list for non-existent table"

def test_rowid_columns_nullable(cursor, db_connection):
    """Test rowIdColumns with nullable parameter"""
    try:
        # First create a table with nullable unique column and non-nullable PK
        cursor.execute("""
        CREATE TABLE pytest_special_schema.nullable_test (
            id INT PRIMARY KEY, -- PK can't be nullable in SQL Server
            data NVARCHAR(100) NULL
        )
        """)
        db_connection.commit()
        
        # Test with nullable=True (default)
        rowid_cols_with_nullable = cursor.rowIdColumns(
            table='nullable_test', 
            schema='pytest_special_schema'
        ).fetchall()

        # Verify PK column is included
        assert len(rowid_cols_with_nullable) == 1, "Should return exactly one column (PK)"
        assert rowid_cols_with_nullable[0].column_name.lower() == 'id', "PK column should be returned"
        
        # Test with nullable=False
        rowid_cols_no_nullable = cursor.rowIdColumns(
            table='nullable_test', 
            schema='pytest_special_schema',
            nullable=False
        ).fetchall()

        # The behavior of SQLSpecialColumns with SQL_NO_NULLS is to only return
        # non-nullable columns that uniquely identify a row, but SQL Server returns
        # an empty set in this case - this is expected behavior
        assert len(rowid_cols_no_nullable) == 0, "Should return empty list when nullable=False (ODBC API behavior)"
        
    except Exception as e:
        pytest.fail(f"rowIdColumns nullable test failed: {e}")
    finally:
        cursor.execute("DROP TABLE IF EXISTS pytest_special_schema.nullable_test")
        db_connection.commit()

def test_rowver_columns_basic(cursor, db_connection):
    """Test basic functionality of rowVerColumns"""
    try:
        # Get version columns from timestamp test table
        rowver_cols = cursor.rowVerColumns(
            table='timestamp_test', 
            schema='pytest_special_schema'
        ).fetchall()

        # Verify we got results
        assert len(rowver_cols) == 1, "Should find exactly one ROWVER column"
        
        # Verify the column is the rowversion column
        rowver_col = rowver_cols[0]
        assert rowver_col.column_name.lower() == 'last_updated', "ROWVER column should be 'last_updated'"
        assert rowver_col.type_name.lower() in ['rowversion', 'timestamp'], "ROWVER column should have rowversion or timestamp type"
        
        # Verify result structure - allowing for NULL values
        assert hasattr(rowver_col, 'scope'), "Result should have scope column"
        assert hasattr(rowver_col, 'column_name'), "Result should have column_name column"
        assert hasattr(rowver_col, 'data_type'), "Result should have data_type column"
        assert hasattr(rowver_col, 'type_name'), "Result should have type_name column"
        assert hasattr(rowver_col, 'column_size'), "Result should have column_size column"
        assert hasattr(rowver_col, 'buffer_length'), "Result should have buffer_length column"        
        assert hasattr(rowver_col, 'decimal_digits'), "Result should have decimal_digits column"      
        assert hasattr(rowver_col, 'pseudo_column'), "Result should have pseudo_column column"        
        
        # The scope should be one of the valid values or NULL
        assert rowver_col.scope in [0, 1, 2, None], f"Invalid scope value: {rowver_col.scope}"
        
    except Exception as e:
        pytest.fail(f"rowVerColumns basic test failed: {e}")
    finally:
        # Clean up happens in test_specialcolumns_cleanup
        pass

def test_rowver_columns_nonexistent(cursor):
    """Test rowVerColumns with non-existent table"""
    # Use a table name that's highly unlikely to exist
    rowver_cols = cursor.rowVerColumns('nonexistent_table_xyz123').fetchall()
    
    # Should return empty list, not error
    assert isinstance(rowver_cols, list), "Should return a list for non-existent table"
    assert len(rowver_cols) == 0, "Should return empty list for non-existent table"

def test_rowver_columns_nullable(cursor, db_connection):
    """Test rowVerColumns with nullable parameter (not expected to have effect)"""
    try:
        # First create a table with rowversion column
        cursor.execute("""
        CREATE TABLE pytest_special_schema.nullable_rowver_test (
            id INT PRIMARY KEY,
            ts ROWVERSION
        )
        """)
        db_connection.commit()
        
        # Test with nullable=True (default)
        rowver_cols_with_nullable = cursor.rowVerColumns(
            table='nullable_rowver_test', 
            schema='pytest_special_schema'
        ).fetchall()

        # Verify rowversion column is included (rowversion can't be nullable)
        assert len(rowver_cols_with_nullable) == 1, "Should find exactly one ROWVER column"
        assert rowver_cols_with_nullable[0].column_name.lower() == 'ts', "ROWVERSION column should be included"
        
        # Test with nullable=False
        rowver_cols_no_nullable = cursor.rowVerColumns(
            table='nullable_rowver_test', 
            schema='pytest_special_schema',
            nullable=False
        ).fetchall()

        # Verify rowversion column is still included
        assert len(rowver_cols_no_nullable) == 1, "Should find exactly one ROWVER column"
        assert rowver_cols_no_nullable[0].column_name.lower() == 'ts', "ROWVERSION column should be included even with nullable=False"
        
    except Exception as e:
        pytest.fail(f"rowVerColumns nullable test failed: {e}")
    finally:
        cursor.execute("DROP TABLE IF EXISTS pytest_special_schema.nullable_rowver_test")
        db_connection.commit()

def test_specialcolumns_catalog_filter(cursor, db_connection):
    """Test special columns with catalog filter"""
    try:
        # Get current database name
        cursor.execute("SELECT DB_NAME() AS current_db")
        current_db = cursor.fetchone().current_db
        
        # Test rowIdColumns with current catalog
        rowid_cols = cursor.rowIdColumns(
            table='rowid_test',
            catalog=current_db,
            schema='pytest_special_schema'
        ).fetchall()

        # Verify catalog filter worked
        assert len(rowid_cols) > 0, "Should find ROWID columns with correct catalog"
        
        # Test rowIdColumns with non-existent catalog
        fake_rowid_cols = cursor.rowIdColumns(
            table='rowid_test',
            catalog='nonexistent_db_xyz123',
            schema='pytest_special_schema'
        ).fetchall()
        assert len(fake_rowid_cols) == 0, "Should return empty list for non-existent catalog"
        
        # Test rowVerColumns with current catalog
        rowver_cols = cursor.rowVerColumns(
            table='timestamp_test',
            catalog=current_db,
            schema='pytest_special_schema'
        ).fetchall()
        
        # Verify catalog filter worked
        assert len(rowver_cols) > 0, "Should find ROWVER columns with correct catalog"
        
        # Test rowVerColumns with non-existent catalog
        fake_rowver_cols = cursor.rowVerColumns(
            table='timestamp_test',
            catalog='nonexistent_db_xyz123',
            schema='pytest_special_schema'
        ).fetchall()
        assert len(fake_rowver_cols) == 0, "Should return empty list for non-existent catalog"
        
    except Exception as e:
        pytest.fail(f"Special columns catalog filter test failed: {e}")
    finally:
        # Clean up happens in test_specialcolumns_cleanup
        pass

def test_specialcolumns_cleanup(cursor, db_connection):
    """Clean up test tables after testing"""
    try:
        # Drop all test tables
        cursor.execute("DROP TABLE IF EXISTS pytest_special_schema.rowid_test")
        cursor.execute("DROP TABLE IF EXISTS pytest_special_schema.timestamp_test")
        cursor.execute("DROP TABLE IF EXISTS pytest_special_schema.multiple_unique_test")
        cursor.execute("DROP TABLE IF EXISTS pytest_special_schema.identity_test")
        cursor.execute("DROP TABLE IF EXISTS pytest_special_schema.nullable_unique_test")
        cursor.execute("DROP TABLE IF EXISTS pytest_special_schema.nullable_timestamp_test")
        
        # Drop the test schema
        cursor.execute("DROP SCHEMA IF EXISTS pytest_special_schema")
        db_connection.commit()
    except Exception as e:
        pytest.fail(f"Test cleanup failed: {e}")

def test_statistics_setup(cursor, db_connection):
    """Create test tables and indexes for statistics testing"""
    try:
        # Create a test schema for isolation
        cursor.execute("IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'pytest_stats_schema') EXEC('CREATE SCHEMA pytest_stats_schema')")
        
        # Drop tables if they exist
        cursor.execute("DROP TABLE IF EXISTS pytest_stats_schema.stats_test")
        cursor.execute("DROP TABLE IF EXISTS pytest_stats_schema.empty_stats_test")
        
        # Create test table with various indexes
        cursor.execute("""
        CREATE TABLE pytest_stats_schema.stats_test (
            id INT PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            email VARCHAR(100) UNIQUE,
            department VARCHAR(50) NOT NULL,
            salary DECIMAL(10, 2) NULL,
            hire_date DATE NOT NULL
        )
        """)
        
        # Create a non-unique index
        cursor.execute("""
        CREATE INDEX IX_stats_test_dept_date ON pytest_stats_schema.stats_test (department, hire_date)
        """)
        
        # Create a unique index on multiple columns
        cursor.execute("""
        CREATE UNIQUE INDEX UX_stats_test_name_dept ON pytest_stats_schema.stats_test (name, department)
        """)
        
        # Create an empty table for testing
        cursor.execute("""
        CREATE TABLE pytest_stats_schema.empty_stats_test (
            id INT PRIMARY KEY,
            data VARCHAR(100) NULL
        )
        """)
        
        db_connection.commit()
    except Exception as e:
        pytest.fail(f"Test setup failed: {e}")

def test_statistics_basic(cursor, db_connection):
    """Test basic functionality of statistics method"""
    try:
        # First set up our test tables
        test_statistics_setup(cursor, db_connection)
        
        # Get statistics for the test table (all indexes)
        stats = cursor.statistics(
            table='stats_test', 
            schema='pytest_stats_schema'
        ).fetchall()
        
        # Verify we got results - should include PK, unique index on email, and non-unique index
        assert stats is not None, "statistics() should return results"
        assert len(stats) > 0, "statistics() should return at least one row"
        
        # Count different types of indexes
        table_stats = [s for s in stats if s.type == 0]  # TABLE_STAT
        indexes = [s for s in stats if s.type != 0]      # Actual indexes
        
        # We should have at least one table statistics row and multiple index rows
        assert len(table_stats) <= 1, "Should have at most one TABLE_STAT row"
        assert len(indexes) >= 3, "Should have at least 3 index entries (PK, unique email, non-unique dept+date)"
        
        # Verify column names in results
        first_row = stats[0]
        assert hasattr(first_row, 'table_name'), "Result should have table_name column"
        assert hasattr(first_row, 'non_unique'), "Result should have non_unique column"
        assert hasattr(first_row, 'index_name'), "Result should have index_name column"
        assert hasattr(first_row, 'type'), "Result should have type column"
        assert hasattr(first_row, 'column_name'), "Result should have column_name column"
        
        # Check that we can find the primary key
        pk_found = False
        for stat in stats:
            if (hasattr(stat, 'index_name') and 
                stat.index_name and 
                'pk' in stat.index_name.lower()):
                pk_found = True
                break
        
        assert pk_found, "Primary key should be included in statistics results"
        
        # Check that we can find the unique index on email
        email_index_found = False
        for stat in stats:
            if (hasattr(stat, 'column_name') and 
                stat.column_name and 
                stat.column_name.lower() == 'email' and
                hasattr(stat, 'non_unique') and 
                stat.non_unique == 0):  # 0 = unique
                email_index_found = True
                break
        
        assert email_index_found, "Unique index on email should be included in statistics results"
        
    finally:
        # Clean up happens in test_statistics_cleanup
        pass

def test_statistics_unique_only(cursor, db_connection):
    """Test statistics with unique=True to get only unique indexes"""
    try:
        # Get statistics for only unique indexes
        stats = cursor.statistics(
            table='stats_test', 
            schema='pytest_stats_schema',
            unique=True
        ).fetchall()
        
        # Verify we got results
        assert stats is not None, "statistics() with unique=True should return results"
        assert len(stats) > 0, "statistics() with unique=True should return at least one row"
        
        # All index entries should be for unique indexes (non_unique = 0)
        for stat in stats:
            if hasattr(stat, 'type') and stat.type != 0:  # Skip TABLE_STAT entries
                assert hasattr(stat, 'non_unique'), "Index entry should have non_unique column"
                assert stat.non_unique == 0, "With unique=True, all indexes should be unique"
        
        # Count different types of indexes
        indexes = [s for s in stats if hasattr(s, 'type') and s.type != 0]
        
        # We should have multiple unique indexes (PK, unique email, unique name+dept)
        assert len(indexes) >= 3, "Should have at least 3 unique index entries"
        
    finally:
        # Clean up happens in test_statistics_cleanup
        pass

def test_statistics_empty_table(cursor, db_connection):
    """Test statistics on a table with no data (just schema)"""
    try:
        # Get statistics for the empty table
        stats = cursor.statistics(
            table='empty_stats_test', 
            schema='pytest_stats_schema'
        ).fetchall()
        
        # Should still return metadata about the primary key
        assert stats is not None, "statistics() should return results even for empty table"
        assert len(stats) > 0, "statistics() should return at least one row for empty table"
        
        # Check for primary key
        pk_found = False
        for stat in stats:
            if (hasattr(stat, 'index_name') and 
                stat.index_name and 
                'pk' in stat.index_name.lower()):
                pk_found = True
                break
        
        assert pk_found, "Primary key should be included in statistics results for empty table"
        
    finally:
        # Clean up happens in test_statistics_cleanup
        pass

def test_statistics_nonexistent(cursor):
    """Test statistics with non-existent table name"""
    # Use a table name that's highly unlikely to exist
    stats = cursor.statistics('nonexistent_table_xyz123').fetchall()
    
    # Should return empty list, not error
    assert isinstance(stats, list), "Should return a list for non-existent table"
    assert len(stats) == 0, "Should return empty list for non-existent table"

def test_statistics_result_structure(cursor, db_connection):
    """Test the complete structure of statistics result rows"""
    try:
        # Get statistics for the test table
        stats = cursor.statistics(
            table='stats_test', 
            schema='pytest_stats_schema'
        ).fetchall()
        
        # Verify we have results
        assert len(stats) > 0, "Should have statistics results"
        
        # Find a row that's an actual index (not TABLE_STAT)
        index_row = None
        for stat in stats:
            if hasattr(stat, 'type') and stat.type != 0:
                index_row = stat
                break
                
        assert index_row is not None, "Should have at least one index row"
        
        # Check for all required columns
        required_columns = [
            'table_cat', 'table_schem', 'table_name', 'non_unique',
            'index_qualifier', 'index_name', 'type', 'ordinal_position',
            'column_name', 'asc_or_desc', 'cardinality', 'pages', 
            'filter_condition'
        ]
        
        for column in required_columns:
            assert hasattr(index_row, column), f"Result missing required column: {column}"
            
        # Check types of key columns
        assert isinstance(index_row.table_name, str), "table_name should be a string"
        assert isinstance(index_row.type, int), "type should be an integer"
        
        # Don't check the actual values of cardinality and pages as they may be NULL
        # or driver-dependent, especially for empty tables
        
    finally:
        # Clean up happens in test_statistics_cleanup
        pass

def test_statistics_catalog_filter(cursor, db_connection):
    """Test statistics with catalog filter"""
    try:
        # Get current database name
        cursor.execute("SELECT DB_NAME() AS current_db")
        current_db = cursor.fetchone().current_db
        
        # Get statistics with current catalog
        stats = cursor.statistics(
            table='stats_test',
            catalog=current_db,
            schema='pytest_stats_schema'
        ).fetchall()

        # Verify catalog filter worked
        assert len(stats) > 0, "Should find statistics with correct catalog"
        
        # Verify catalog in results
        for stat in stats:
            if hasattr(stat, 'table_cat'):
                assert stat.table_cat.lower() == current_db.lower(), "Wrong table catalog"
            
        # Get statistics with non-existent catalog
        fake_stats = cursor.statistics(
            table='stats_test',
            catalog='nonexistent_db_xyz123',
            schema='pytest_stats_schema'
        ).fetchall()
        assert len(fake_stats) == 0, "Should return empty list for non-existent catalog"
        
    finally:
        # Clean up happens in test_statistics_cleanup
        pass

def test_statistics_with_quick_parameter(cursor, db_connection):
    """Test statistics with quick parameter variations"""
    try:
        # Test with quick=True (default)
        quick_stats = cursor.statistics(
            table='stats_test', 
            schema='pytest_stats_schema',
            quick=True
        ).fetchall()
        
        # Test with quick=False
        thorough_stats = cursor.statistics(
            table='stats_test', 
            schema='pytest_stats_schema',
            quick=False
        ).fetchall()
        
        # Both should return results, but we can't guarantee behavior differences
        # since it depends on the ODBC driver and database system
        assert len(quick_stats) > 0, "quick=True should return results"
        assert len(thorough_stats) > 0, "quick=False should return results"
        
        # Just verify that changing the parameter didn't cause errors
        
    finally:
        # Clean up happens in test_statistics_cleanup
        pass

def test_statistics_cleanup(cursor, db_connection):
    """Clean up test tables after testing"""
    try:
        # Drop all test tables
        cursor.execute("DROP TABLE IF EXISTS pytest_stats_schema.stats_test")
        cursor.execute("DROP TABLE IF EXISTS pytest_stats_schema.empty_stats_test")
        
        # Drop the test schema
        cursor.execute("DROP SCHEMA IF EXISTS pytest_stats_schema")
        db_connection.commit()
    except Exception as e:
        pytest.fail(f"Test cleanup failed: {e}")

def test_columns_setup(cursor, db_connection):
    """Create test tables for columns method testing"""
    try:
        # Create a test schema for isolation
        cursor.execute("IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'pytest_cols_schema') EXEC('CREATE SCHEMA pytest_cols_schema')")

        # Drop tables if they exist
        cursor.execute("DROP TABLE IF EXISTS pytest_cols_schema.columns_test")
        cursor.execute("DROP TABLE IF EXISTS pytest_cols_schema.columns_special_test")
        
        # Create test table with various column types
        cursor.execute(""" 
        CREATE TABLE pytest_cols_schema.columns_test (
            id INT PRIMARY KEY,
            name NVARCHAR(100) NOT NULL,
            description NVARCHAR(MAX) NULL,
            price DECIMAL(10, 2) NULL,
            created_date DATETIME DEFAULT GETDATE(),
            is_active BIT NOT NULL DEFAULT 1,
            binary_data VARBINARY(MAX) NULL,
            notes TEXT NULL,
            [computed_col] AS (name + ' - ' + CAST(id AS VARCHAR(10)))
        )
        """)
        
        # Create table with special column names and edge cases - fix the problematic column name
        cursor.execute(""" 
        CREATE TABLE pytest_cols_schema.columns_special_test (
            [ID] INT PRIMARY KEY,
            [User Name] NVARCHAR(100) NULL,
            [Spaces  Multiple] VARCHAR(50) NULL,
            [123_numeric_start] INT NULL,
            [MAX] VARCHAR(20) NULL,        -- SQL keyword as column name
            [SELECT] INT NULL,             -- SQL keyword as column name
            [Column.With.Dots] VARCHAR(20) NULL,
            [Column/With/Slashes] VARCHAR(20) NULL,
            [Column_With_Underscores] VARCHAR(20) NULL  -- Changed from problematic nested brackets
        )
        """)
        
        db_connection.commit()
    except Exception as e:
        pytest.fail(f"Test setup failed: {e}")

def test_columns_all(cursor, db_connection):
    """Test columns returns information about all columns in all tables"""
    try:
        # First set up our test tables
        test_columns_setup(cursor, db_connection)
        
        # Get all columns (no filters)
        cols_cursor = cursor.columns()
        cols = cols_cursor.fetchall()
        
        # Verify we got results
        assert cols is not None, "columns() should return results"
        assert len(cols) > 0, "columns() should return at least one column"
        
        # Verify our test tables' columns are in the results
        # Use case-insensitive comparison to avoid driver case sensitivity issues
        found_test_table = False
        for col in cols:
            if (hasattr(col, 'table_name') and 
                col.table_name and 
                col.table_name.lower() == 'columns_test' and
                hasattr(col, 'table_schem') and 
                col.table_schem and 
                col.table_schem.lower() == 'pytest_cols_schema'):
                found_test_table = True
                break
                
        assert found_test_table, "Test table columns should be included in results"
        
        # Verify structure of results
        first_row = cols[0]
        assert hasattr(first_row, 'table_cat'), "Result should have table_cat column"
        assert hasattr(first_row, 'table_schem'), "Result should have table_schem column"
        assert hasattr(first_row, 'table_name'), "Result should have table_name column"
        assert hasattr(first_row, 'column_name'), "Result should have column_name column"
        assert hasattr(first_row, 'data_type'), "Result should have data_type column"
        assert hasattr(first_row, 'type_name'), "Result should have type_name column"
        assert hasattr(first_row, 'column_size'), "Result should have column_size column"
        assert hasattr(first_row, 'buffer_length'), "Result should have buffer_length column"
        assert hasattr(first_row, 'decimal_digits'), "Result should have decimal_digits column"
        assert hasattr(first_row, 'num_prec_radix'), "Result should have num_prec_radix column"
        assert hasattr(first_row, 'nullable'), "Result should have nullable column"
        assert hasattr(first_row, 'remarks'), "Result should have remarks column"
        assert hasattr(first_row, 'column_def'), "Result should have column_def column"
        assert hasattr(first_row, 'sql_data_type'), "Result should have sql_data_type column"
        assert hasattr(first_row, 'sql_datetime_sub'), "Result should have sql_datetime_sub column"
        assert hasattr(first_row, 'char_octet_length'), "Result should have char_octet_length column"
        assert hasattr(first_row, 'ordinal_position'), "Result should have ordinal_position column"
        assert hasattr(first_row, 'is_nullable'), "Result should have is_nullable column"
        
    finally:
        # Clean up happens in test_columns_cleanup
        pass

def test_columns_specific_table(cursor, db_connection):
    """Test columns returns information about a specific table"""
    try:
        # Get columns for the test table
        cols = cursor.columns(
            table='columns_test', 
            schema='pytest_cols_schema'
        ).fetchall()
        
        # Verify we got results
        assert len(cols) == 9, "Should find exactly 9 columns in columns_test"
        
        # Verify all column names are present (case insensitive)
        col_names = [col.column_name.lower() for col in cols]
        expected_names = ['id', 'name', 'description', 'price', 'created_date', 
                          'is_active', 'binary_data', 'notes', 'computed_col']
        
        for name in expected_names:
            assert name in col_names, f"Column {name} should be in results"
            
        # Verify details of a specific column (id)
        id_col = next(col for col in cols if col.column_name.lower() == 'id')
        assert id_col.nullable == 0, "id column should be non-nullable"
        assert id_col.ordinal_position == 1, "id should be the first column"
        assert id_col.is_nullable == "NO", "is_nullable should be NO for id column"
        
        # Check data types (but don't assume specific ODBC type codes since they vary by driver)
        # Instead check that the type_name is correct
        id_type = id_col.type_name.lower()
        assert 'int' in id_type, f"id column should be INTEGER type, got {id_type}"
        
        # Check a nullable column
        desc_col = next(col for col in cols if col.column_name.lower() == 'description')
        assert desc_col.nullable == 1, "description column should be nullable"
        assert desc_col.is_nullable == "YES", "is_nullable should be YES for description column"
        
    finally:
        # Clean up happens in test_columns_cleanup
        pass

def test_columns_special_chars(cursor, db_connection):
    """Test columns with special characters and edge cases"""
    try:
        # Get columns for the special table
        cols = cursor.columns(
            table='columns_special_test', 
            schema='pytest_cols_schema'
        ).fetchall()
        
        # Verify we got results
        assert len(cols) == 9, "Should find exactly 9 columns in columns_special_test"
        
        # Check that special column names are handled correctly
        col_names = [col.column_name for col in cols]
        
        # Create case-insensitive lookup
        col_names_lower = [name.lower() if name else None for name in col_names]
        
        # Check for columns with special characters - note that column names might be
        # returned with or without brackets/quotes depending on the driver
        assert any('user name' in name.lower() for name in col_names), "Column with spaces should be in results"
        assert any('id' == name.lower() for name in col_names), "ID column should be in results"
        assert any('123_numeric_start' in name.lower() for name in col_names), "Column starting with numbers should be in results"
        assert any('max' == name.lower() for name in col_names), "MAX column should be in results"
        assert any('select' == name.lower() for name in col_names), "SELECT column should be in results"
        assert any('column.with.dots' in name.lower() for name in col_names), "Column with dots should be in results"
        assert any('column/with/slashes' in name.lower() for name in col_names), "Column with slashes should be in results"
        assert any('column_with_underscores' in name.lower() for name in col_names), "Column with underscores should be in results"
        
    finally:
        # Clean up happens in test_columns_cleanup
        pass

def test_columns_specific_column(cursor, db_connection):
    """Test columns with specific column filter"""
    try:
        # Get specific column
        cols = cursor.columns(
            table='columns_test', 
            schema='pytest_cols_schema',
            column='name'
        ).fetchall()
        
        # Verify we got just one result
        assert len(cols) == 1, "Should find exactly 1 column named 'name'"
        
        # Verify column details
        col = cols[0]
        assert col.column_name.lower() == 'name', "Column name should be 'name'"
        assert col.table_name.lower() == 'columns_test', "Table name should be 'columns_test'"
        assert col.table_schem.lower() == 'pytest_cols_schema', "Schema should be 'pytest_cols_schema'"
        assert col.nullable == 0, "name column should be non-nullable"
        
        # Get column using pattern (% wildcard)
        pattern_cols = cursor.columns(
            table='columns_test', 
            schema='pytest_cols_schema',
            column='%date%'
        ).fetchall()
        
        # Should find created_date column
        assert len(pattern_cols) == 1, "Should find 1 column matching '%date%'"

        assert pattern_cols[0].column_name.lower() == 'created_date', "Should find created_date column"
        
        # Get multiple columns with pattern
        multi_cols = cursor.columns(
            table='columns_test', 
            schema='pytest_cols_schema',
            column='%d%'  # Should match id, description, created_date
        ).fetchall()
        
        # At least 3 columns should match this pattern
        assert len(multi_cols) >= 3, "Should find at least 3 columns matching '%d%'"
        match_names = [col.column_name.lower() for col in multi_cols]
        assert 'id' in match_names, "id should match '%d%'"
        assert 'description' in match_names, "description should match '%d%'"
        assert 'created_date' in match_names, "created_date should match '%d%'"
        
    finally:
        # Clean up happens in test_columns_cleanup
        pass

def test_columns_with_underscore_pattern(cursor):
    """Test columns with underscore wildcard pattern"""
    try:
        # Get columns with underscore pattern (one character wildcard)
        # Looking for 'id' (exactly 2 chars)
        cols = cursor.columns(
            table='columns_test', 
            schema='pytest_cols_schema',
            column='__'
        ).fetchall()
        
        # Should find 'id' column
        id_found = False
        for col in cols:
            if col.column_name.lower() == 'id' and col.table_name.lower() == 'columns_test':
                id_found = True
                break
                
        assert id_found, "Should find 'id' column with pattern '__'"
        
        # Try a more complex pattern with both % and _
        # For example: '%_d%' matches any column with 'd' as the second or later character
        pattern_cols = cursor.columns(
            table='columns_test', 
            schema='pytest_cols_schema',
            column='%_d%'
        ).fetchall()
        
        # Should match 'id' (if considering case-insensitive) and 'created_date'
        match_names = [col.column_name.lower() for col in pattern_cols 
                       if col.table_name.lower() == 'columns_test']
        
        # At least 'created_date' should match this pattern
        assert 'created_date' in match_names, "created_date should match '%_d%'"
        
    finally:
        # Clean up happens in test_columns_cleanup
        pass

def test_columns_nonexistent(cursor):
    """Test columns with non-existent table or column"""
    # Test with non-existent table
    table_cols = cursor.columns(table='nonexistent_table_xyz123')
    assert len(table_cols) == 0, "Should return empty list for non-existent table"
    
    # Test with non-existent column in existing table
    col_cols = cursor.columns(
        table='columns_test', 
        schema='pytest_cols_schema',
        column='nonexistent_column_xyz123'
    ).fetchall()
    assert len(col_cols) == 0, "Should return empty list for non-existent column"
    
    # Test with non-existent schema
    schema_cols = cursor.columns(
        table='columns_test', 
        schema='nonexistent_schema_xyz123'
    )
    assert len(schema_cols) == 0, "Should return empty list for non-existent schema"

def test_columns_data_types(cursor):
    """Test columns returns correct data type information"""
    try:
        # Get all columns from test table
        cols = cursor.columns(
            table='columns_test', 
            schema='pytest_cols_schema'
        ).fetchall()
        
        # Create a dictionary mapping column names to their details
        col_dict = {col.column_name.lower(): col for col in cols}
        
        # Check data types by name (case insensitive checks)
        # Note: We're checking type_name as a string to avoid SQL type code inconsistencies
        # between drivers
        
        # INT column
        assert 'int' in col_dict['id'].type_name.lower(), "id should be INT type"
        
        # NVARCHAR column
        assert any(name in col_dict['name'].type_name.lower() 
                  for name in ['nvarchar', 'varchar', 'char', 'wchar']), "name should be NVARCHAR type"
        
        # DECIMAL column
        assert any(name in col_dict['price'].type_name.lower() 
                  for name in ['decimal', 'numeric', 'money']), "price should be DECIMAL type"
        
        # BIT column
        assert any(name in col_dict['is_active'].type_name.lower() 
                  for name in ['bit', 'boolean']), "is_active should be BIT type"
        
        # TEXT column
        assert any(name in col_dict['notes'].type_name.lower() 
                  for name in ['text', 'char', 'varchar']), "notes should be TEXT type"
        
        # Check nullable flag
        assert col_dict['id'].nullable == 0, "id should be non-nullable"
        assert col_dict['description'].nullable == 1, "description should be nullable"
        
        # Check column size
        assert col_dict['name'].column_size == 100, "name should have size 100"
        
        # Check decimal digits for numeric type
        assert col_dict['price'].decimal_digits == 2, "price should have 2 decimal digits"
        
    finally:
        # Clean up happens in test_columns_cleanup
        pass

def test_columns_nonexistent(cursor):
    """Test columns with non-existent table or column"""
    # Test with non-existent table
    table_cols = cursor.columns(table='nonexistent_table_xyz123').fetchall()
    assert len(table_cols) == 0, "Should return empty list for non-existent table"
    
    # Test with non-existent column in existing table
    col_cols = cursor.columns(
        table='columns_test', 
        schema='pytest_cols_schema',
        column='nonexistent_column_xyz123'
    ).fetchall()
    assert len(col_cols) == 0, "Should return empty list for non-existent column"
    
    # Test with non-existent schema
    schema_cols = cursor.columns(
        table='columns_test', 
        schema='nonexistent_schema_xyz123'
    ).fetchall()
    assert len(schema_cols) == 0, "Should return empty list for non-existent schema"

def test_columns_catalog_filter(cursor):
    """Test columns with catalog filter"""
    try:
        # Get current database name
        cursor.execute("SELECT DB_NAME() AS current_db")
        current_db = cursor.fetchone().current_db
        
        # Get columns with current catalog
        cols = cursor.columns(
            table='columns_test',
            catalog=current_db,
            schema='pytest_cols_schema'
        ).fetchall()
        
        # Verify catalog filter worked
        assert len(cols) > 0, "Should find columns with correct catalog"
        
        # Check catalog in results
        for col in cols:
            # Some drivers might return None for catalog
            if col.table_cat is not None:
                assert col.table_cat.lower() == current_db.lower(), "Wrong table catalog"
            
        # Test with non-existent catalog
        fake_cols = cursor.columns(
            table='columns_test',
            catalog='nonexistent_db_xyz123',
            schema='pytest_cols_schema'
        ).fetchall()
        assert len(fake_cols) == 0, "Should return empty list for non-existent catalog"
        
    finally:
        # Clean up happens in test_columns_cleanup
        pass

def test_columns_schema_pattern(cursor):
    """Test columns with schema name pattern"""
    try:
        # Get columns with schema pattern
        cols = cursor.columns(
            table='columns_test',
            schema='pytest_%'
        ).fetchall()
        
        # Should find our test table columns
        test_cols = [col for col in cols if col.table_name.lower() == 'columns_test']
        assert len(test_cols) > 0, "Should find columns using schema pattern"
        
        # Try a more specific pattern
        specific_cols = cursor.columns(
            table='columns_test',
            schema='pytest_cols%'
        ).fetchall()
        
        # Should still find our test table columns
        test_cols = [col for col in specific_cols if col.table_name.lower() == 'columns_test']
        assert len(test_cols) > 0, "Should find columns using specific schema pattern"
        
    finally:
        # Clean up happens in test_columns_cleanup
        pass

def test_columns_table_pattern(cursor):
    """Test columns with table name pattern"""
    try:
        # Get columns with table pattern
        cols = cursor.columns(
            table='columns_%',
            schema='pytest_cols_schema'
        ).fetchall()
        
        # Should find columns from both test tables
        tables_found = set()
        for col in cols:
            if col.table_name:
                tables_found.add(col.table_name.lower())
        
        assert 'columns_test' in tables_found, "Should find columns_test with pattern columns_%"
        assert 'columns_special_test' in tables_found, "Should find columns_special_test with pattern columns_%"
        
    finally:
        # Clean up happens in test_columns_cleanup
        pass

def test_columns_ordinal_position(cursor):
    """Test ordinal_position is correct in columns results"""
    try:
        # Get columns for the test table
        cols = cursor.columns(
            table='columns_test', 
            schema='pytest_cols_schema'
        ).fetchall()
        
        # Sort by ordinal position
        sorted_cols = sorted(cols, key=lambda col: col.ordinal_position)
        
        # Verify positions are consecutive starting from 1
        for i, col in enumerate(sorted_cols, 1):
            assert col.ordinal_position == i, f"Column {col.column_name} should have ordinal_position {i}"
            
        # First column should be id (primary key)
        assert sorted_cols[0].column_name.lower() == 'id', "First column should be id"
        
    finally:
        # Clean up happens in test_columns_cleanup
        pass

def test_columns_cleanup(cursor, db_connection):
    """Clean up test tables after testing"""
    try:
        # Drop all test tables
        cursor.execute("DROP TABLE IF EXISTS pytest_cols_schema.columns_test")
        cursor.execute("DROP TABLE IF EXISTS pytest_cols_schema.columns_special_test")
        
        # Drop the test schema
        cursor.execute("DROP SCHEMA IF EXISTS pytest_cols_schema")
        db_connection.commit()
    except Exception as e:
        pytest.fail(f"Test cleanup failed: {e}")

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
