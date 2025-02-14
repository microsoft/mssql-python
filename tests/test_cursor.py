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

# Setup test table
TEST_TABLE = """
CREATE TABLE all_data_types (
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
    datetime(2024, 5, 20, 12, 34, 56),
    date(2024, 5, 20),
    1.23456789
)

# Parameterized test data with different primary keys
PARAM_TEST_DATA = [
    TEST_DATA,
    (2, 0, 0, 0, 0, 0, 0.0, "test1", time(0, 0, 0), datetime(2024, 1, 1, 0, 0, 0), date(2024, 1, 1), 0.0),
    (3, 1, 1, 1, 1, 1, 1.1, "test2", time(1, 1, 1), datetime(2024, 2, 2, 1, 1, 1), date(2024, 2, 2), 1.1),
    (4, 0, 127, 32767, 9223372036854775807, 2147483647, 1.23456789, "test3", time(12, 34, 56), datetime(2024, 5, 20, 12, 34, 56), date(2024, 5, 20), 1.23456789)
]

def drop_table_if_exists(cursor, table_name):
    """Drop the table if it exists"""
    try:
        cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
    except Exception as e:
        pytest.fail(f"Failed to drop table {table_name}: {e}")

def test_cursor(cursor):
    """Check if the cursor is created"""
    assert cursor is not None, "Cursor should not be None"

def test_insert_id_column(cursor, db_connection):
    """Test inserting data into the id column"""
    try:
        drop_table_if_exists(cursor, "single_column")
        cursor.execute("CREATE TABLE single_column (id INTEGER PRIMARY KEY)")
        db_connection.commit()
        cursor.execute("INSERT INTO single_column (id) VALUES (?)", [1])
        db_connection.commit()
        cursor.execute("SELECT id FROM single_column")
        row = cursor.fetchone()
        assert row[0] == 1, "ID column insertion failed"
    except Exception as e:
        pytest.fail(f"ID column insertion failed: {e}")
    finally:
        cursor.execute("DROP TABLE single_column")
        db_connection.commit()

def test_insert_bit_column(cursor, db_connection):
    """Test inserting data into the bit_column"""
    try:
        cursor.execute("CREATE TABLE single_column (bit_column BIT)")
        db_connection.commit()
        cursor.execute("INSERT INTO single_column (bit_column) VALUES (?)", [1])
        db_connection.commit()
        cursor.execute("SELECT bit_column FROM single_column")
        row = cursor.fetchone()
        assert row[0] == 1, "Bit column insertion failed"
    except Exception as e:
        pytest.fail(f"Bit column insertion failed: {e}")
    finally:
        cursor.execute("DROP TABLE single_column")
        db_connection.commit()

def test_insert_nvarchar_column(cursor, db_connection):
    """Test inserting data into the nvarchar_column"""
    try:
        cursor.execute("CREATE TABLE single_column (nvarchar_column NVARCHAR(255))")
        db_connection.commit()
        cursor.execute("INSERT INTO single_column (nvarchar_column) VALUES (?)", ["test"])
        db_connection.commit()
        cursor.execute("SELECT nvarchar_column FROM single_column")
        row = cursor.fetchone()
        assert row[0] == "test", "Nvarchar column insertion failed"
    except Exception as e:
        pytest.fail(f"Nvarchar column insertion failed: {e}")
    finally:
        cursor.execute("DROP TABLE single_column")
        db_connection.commit()

# def test_insert_time_column(cursor, db_connection):
#     """Test inserting data into the time_column"""
#     try:
#         cursor.execute("CREATE TABLE single_column (time_column TIME)")
#         db_connection.commit()
#         cursor.execute("INSERT INTO single_column (time_column) VALUES (?)", [time(12, 34, 56)])
#         db_connection.commit()
#         cursor.execute("SELECT time_column FROM single_column")
#         row = cursor.fetchone()
#         assert row[0] == time(12, 34, 56), "Time column insertion failed"
#     except Exception as e:
#         pytest.fail(f"Time column insertion failed: {e}")
#     finally:
#         cursor.execute("DROP TABLE single_column")
#         db_connection.commit()

# def test_insert_datetime_column(cursor, db_connection):
#     """Test inserting data into the datetime_column"""
#     try:
#         cursor.execute("CREATE TABLE single_column (datetime_column DATETIME)")
#         db_connection.commit()
#         cursor.execute("INSERT INTO single_column (datetime_column) VALUES (?)", [datetime(2024, 5, 20, 12, 34, 56)])
#         db_connection.commit()
#         cursor.execute("SELECT datetime_column FROM single_column")
#         row = cursor.fetchone()
#         assert row[0] == datetime(2024, 5, 20, 12, 34, 56), "Datetime column insertion failed"
#     except Exception as e:
#         pytest.fail(f"Datetime column insertion failed: {e}")
#     finally:
#         cursor.execute("DROP TABLE single_column")
#         db_connection.commit()

def test_insert_date_column(cursor, db_connection):
    """Test inserting data into the date_column"""
    try:
        cursor.execute("CREATE TABLE single_column (date_column DATE)")
        db_connection.commit()
        cursor.execute("INSERT INTO single_column (date_column) VALUES (?)", [date(2024, 5, 20)])
        db_connection.commit()
        cursor.execute("SELECT date_column FROM single_column")
        row = cursor.fetchone()
        assert row[0] == (2024, 5, 20), "Date column insertion failed"
    except Exception as e:
        pytest.fail(f"Date column insertion failed: {e}")
    finally:
        cursor.execute("DROP TABLE single_column")
        db_connection.commit()

def test_insert_real_column(cursor, db_connection):
    """Test inserting data into the real_column"""
    try:
        cursor.execute("CREATE TABLE single_column (real_column REAL)")
        db_connection.commit()
        cursor.execute("INSERT INTO single_column (real_column) VALUES (?)", [1.23456789])
        db_connection.commit()
        cursor.execute("SELECT real_column FROM single_column")
        row = cursor.fetchone()
        assert abs(row[0] - 1.23456789) < 1e-8, "Real column insertion failed"
    except Exception as e:
        pytest.fail(f"Real column insertion failed: {e}")
    finally:
        cursor.execute("DROP TABLE single_column")
        db_connection.commit()

# def test_insert_decimal_column(cursor, db_connection):
#     """Test inserting data into the decimal_column"""
#     try:
#         cursor.execute("CREATE TABLE single_column (decimal_column DECIMAL(10, 10))")
#         db_connection.commit()
#         cursor.execute("INSERT INTO single_column (decimal_column) VALUES (?)", [decimal.Decimal("1.23456789")])
#         db_connection.commit()
#         cursor.execute("SELECT decimal_column FROM single_column")
#         row = cursor.fetchone()
#         assert row[0] == decimal.Decimal("1.23456789"), "Decimal column insertion failed"
#     except Exception as e:
#         pytest.fail(f"Decimal column insertion failed: {e}")
#     finally:
#         cursor.execute("DROP TABLE single_column")
#         db_connection.commit()

def test_insert_tinyint_column(cursor, db_connection):
    """Test inserting data into the tinyint_column"""
    try:
        cursor.execute("CREATE TABLE single_column (tinyint_column TINYINT)")
        db_connection.commit()
        cursor.execute("INSERT INTO single_column (tinyint_column) VALUES (?)", [127])
        db_connection.commit()
        cursor.execute("SELECT tinyint_column FROM single_column")
        row = cursor.fetchone()
        assert row[0] == 127, "Tinyint column insertion failed"
    except Exception as e:
        pytest.fail(f"Tinyint column insertion failed: {e}")
    finally:
        cursor.execute("DROP TABLE single_column")
        db_connection.commit()

def test_insert_smallint_column(cursor, db_connection):
    """Test inserting data into the smallint_column"""
    try:
        cursor.execute("CREATE TABLE single_column (smallint_column SMALLINT)")
        db_connection.commit()
        cursor.execute("INSERT INTO single_column (smallint_column) VALUES (?)", [32767])
        db_connection.commit()
        cursor.execute("SELECT smallint_column FROM single_column")
        row = cursor.fetchone()
        assert row[0] == 32767, "Smallint column insertion failed"
    except Exception as e:
        pytest.fail(f"Smallint column insertion failed: {e}")
    finally:
        cursor.execute("DROP TABLE single_column")
        db_connection.commit()

def test_insert_bigint_column(cursor, db_connection):
    """Test inserting data into the bigint_column"""
    try:
        cursor.execute("CREATE TABLE single_column (bigint_column BIGINT)")
        db_connection.commit()
        cursor.execute("INSERT INTO single_column (bigint_column) VALUES (?)", [9223372036854775807])
        db_connection.commit()
        cursor.execute("SELECT bigint_column FROM single_column")
        row = cursor.fetchone()
        assert row[0] == 9223372036854775807, "Bigint column insertion failed"
    except Exception as e:
        pytest.fail(f"Bigint column insertion failed: {e}")
    finally:
        cursor.execute("DROP TABLE single_column")
        db_connection.commit()

def test_insert_integer_column(cursor, db_connection):
    """Test inserting data into the integer_column"""
    try:
        cursor.execute("CREATE TABLE single_column (integer_column INTEGER)")
        db_connection.commit()
        cursor.execute("INSERT INTO single_column (integer_column) VALUES (?)", [2147483647])
        db_connection.commit()
        cursor.execute("SELECT integer_column FROM single_column")
        row = cursor.fetchone()
        assert row[0] == 2147483647, "Integer column insertion failed"
    except Exception as e:
        pytest.fail(f"Integer column insertion failed: {e}")
    finally:
        cursor.execute("DROP TABLE single_column")
        db_connection.commit()

def test_insert_float_column(cursor, db_connection):
    """Test inserting data into the float_column"""
    try:
        cursor.execute("CREATE TABLE single_column (float_column FLOAT)")
        db_connection.commit()
        cursor.execute("INSERT INTO single_column (float_column) VALUES (?)", [1.23456789])
        db_connection.commit()
        cursor.execute("SELECT float_column FROM single_column")
        row = cursor.fetchone()
        assert abs(row[0] - 1.23456789) < 1e-8, "Float column insertion failed"
    except Exception as e:
        pytest.fail(f"Float column insertion failed: {e}")
    finally:
        cursor.execute("DROP TABLE single_column")
        db_connection.commit()

def test_create_table(cursor, db_connection):
    # Drop the table if it exists
    drop_table_if_exists(cursor, "all_data_types")
    
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
            INSERT INTO all_data_types VALUES (
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
        cursor.execute("SELECT * FROM all_data_types WHERE id = 1")
        row = cursor.fetchone()
        assert row[0] == TEST_DATA[0], "Insertion using args failed"
    except Exception as e:
        pytest.fail(f"Parameterized data insertion failed: {e}")    
    finally:
        cursor.execute("DELETE FROM all_data_types")
        db_connection.commit()                   

@pytest.mark.parametrize("data", PARAM_TEST_DATA)
def test_parametrized_insert(cursor, db_connection, data):
    """Test parameterized insert using qmark parameters"""
    try:
        cursor.execute("""
            INSERT INTO all_data_types VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
        """, [None if v is None else v for v in data])
        db_connection.commit()
    except Exception as e:
        pytest.fail(f"Parameterized data insertion failed: {e}")

def test_rowcount(cursor, db_connection):
    """Test rowcount after insert operations"""
    try:
        cursor.execute("CREATE TABLE test_rowcount (id INT IDENTITY(1,1) PRIMARY KEY, name NVARCHAR(100))")
        db_connection.commit()

        cursor.execute("INSERT INTO test_rowcount (name) VALUES ('JohnDoe1');")
        assert cursor.rowcount == 1, "Rowcount should be 1 after first insert"

        cursor.execute("INSERT INTO test_rowcount (name) VALUES ('JohnDoe2');")
        assert cursor.rowcount == 1, "Rowcount should be 1 after second insert"

        cursor.execute("INSERT INTO test_rowcount (name) VALUES ('JohnDoe3');")
        assert cursor.rowcount == 1, "Rowcount should be 1 after third insert"

        cursor.execute("""
            INSERT INTO test_rowcount (name) 
            VALUES 
            ('JohnDoe4'), 
            ('JohnDoe5'), 
            ('JohnDoe6');
        """)
        assert cursor.rowcount == 3, "Rowcount should be 3 after inserting multiple rows"

        cursor.execute("SELECT * FROM test_rowcount;")
        assert cursor.rowcount == -1, "Rowcount should be -1 after a SELECT statement"

        db_connection.commit()
    except Exception as e:
        pytest.fail(f"Rowcount test failed: {e}")
    finally:
        cursor.execute("DROP TABLE test_rowcount")
        db_connection.commit()

def test_rowcount_executemany(cursor, db_connection):
    """Test rowcount after executemany operations"""
    try:
        cursor.execute("CREATE TABLE test_rowcount (id INT IDENTITY(1,1) PRIMARY KEY, name NVARCHAR(100))")
        db_connection.commit()

        data = [
            ('JohnDoe1',),
            ('JohnDoe2',),
            ('JohnDoe3',)
        ]

        cursor.executemany("INSERT INTO test_rowcount (name) VALUES (?)", data)
        assert cursor.rowcount == 3, "Rowcount should be 3 after executemany insert"

        cursor.execute("SELECT * FROM test_rowcount;")
        assert cursor.rowcount == -1, "Rowcount should be -1 after a SELECT statement"

        db_connection.commit()
    except Exception as e:
        pytest.fail(f"Rowcount executemany test failed: {e}")
    finally:
        cursor.execute("DROP TABLE test_rowcount")
        db_connection.commit()

def test_fetchone(cursor):
    """Test fetching a single row"""
    cursor.execute("SELECT * FROM all_data_types WHERE id = 1")
    row = cursor.fetchone()
    assert row is not None, "No row returned"
    assert len(row) == 12, "Incorrect number of columns"

def test_fetchmany(cursor):
    """Test fetching multiple rows"""
    cursor.execute("SELECT * FROM all_data_types")
    rows = cursor.fetchmany(2)
    assert isinstance(rows, list), "fetchmany should return a list"
    assert len(rows) == 2, "Incorrect number of rows returned"

def test_fetchall(cursor):
    """Test fetching all rows"""
    cursor.execute("SELECT * FROM all_data_types")
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

# def test_arraysize(cursor):
#     """Test arraysize"""
#     cursor.arraysize = 10
#     assert cursor.arraysize == 10, "Arraysize mismatch"

# def test_description(cursor):
#     """Test description"""
#     cursor.execute("SELECT * FROM all_data_types WHERE id = 1")
#     desc = cursor.description
#     assert len(desc) == 12, "Description length mismatch"
#     assert desc[0][0] == "id", "Description column name mismatch"

# def test_setinputsizes(cursor):
#     """Test setinputsizes"""
#     sizes = [(mssql_python.ConstantsODBC.SQL_INTEGER, 10), (mssql_python.ConstantsODBC.SQL_VARCHAR, 255)]
#     cursor.setinputsizes(sizes)

# def test_setoutputsize(cursor):
#     """Test setoutputsize"""
#     cursor.setoutputsize(10, mssql_python.ConstantsODBC.SQL_INTEGER)

def test_execute_many(cursor, db_connection):
    """Test executemany"""
    # Start fresh
    cursor.execute("DELETE FROM all_data_types")
    db_connection.commit()
    data = [(i,) for i in range(1, 12)]
    cursor.executemany("INSERT INTO all_data_types (id) VALUES (?)", data)
    cursor.execute("SELECT COUNT(*) FROM all_data_types")
    count = cursor.fetchone()[0]
    assert count == 11, "Executemany failed"

def test_nextset(cursor):
    """Test nextset"""
    cursor.execute("SELECT * FROM all_data_types WHERE id = 1;")
    assert cursor.nextset() is False, "Nextset should return False"
    cursor.execute("SELECT * FROM all_data_types WHERE id = 2; SELECT * FROM all_data_types WHERE id = 3;")
    assert cursor.nextset() is True, "Nextset should return True"

def test_delete_table(cursor, db_connection):
    """Test deleting the table"""
    drop_table_if_exists(cursor, "all_data_types")
    db_connection.commit()

# Setup tables for join operations
CREATE_TABLES_FOR_JOIN = [
    """
    CREATE TABLE employees (
        employee_id INTEGER PRIMARY KEY,
        name NVARCHAR(255),
        department_id INTEGER
    );
    """,
    """
    CREATE TABLE departments (
        department_id INTEGER PRIMARY KEY,
        department_name NVARCHAR(255)
    );
    """,
    """
    CREATE TABLE projects (
        project_id INTEGER PRIMARY KEY,
        project_name NVARCHAR(255),
        employee_id INTEGER
    );
    """
]

# Insert data for join operations
INSERT_DATA_FOR_JOIN = [
    """
    INSERT INTO employees (employee_id, name, department_id) VALUES
    (1, 'Alice', 1),
    (2, 'Bob', 2),
    (3, 'Charlie', 1);
    """,
    """
    INSERT INTO departments (department_id, department_name) VALUES
    (1, 'HR'),
    (2, 'Engineering');
    """,
    """
    INSERT INTO projects (project_id, project_name, employee_id) VALUES
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
            FROM employees e
            JOIN departments d ON e.department_id = d.department_id
            JOIN projects p ON e.employee_id = p.employee_id
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
            FROM employees e
            JOIN departments d ON e.department_id = d.department_id
            JOIN projects p ON e.employee_id = p.employee_id
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
CREATE PROCEDURE GetEmployeeProjects
    @EmployeeID INT
AS
BEGIN
    SELECT e.name, p.project_name
    FROM employees e
    JOIN projects p ON e.employee_id = p.employee_id
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
        cursor.execute("{CALL GetEmployeeProjects(?)}", [1])
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
            EXEC GetEmployeeProjects @EmployeeID
        """)
        rows = cursor.fetchall()
        assert len(rows) == 1, "Stored procedure without parameters returned incorrect number of rows"
        assert rows[0] == ['Bob', 'Project B'], "Stored procedure without parameters returned incorrect data"
    except Exception as e:
        pytest.fail(f"Stored procedure execution without parameters failed: {e}")

def test_drop_stored_procedure(cursor, db_connection):
    """Drop stored procedure"""
    try:
        cursor.execute("DROP PROCEDURE IF EXISTS GetEmployeeProjects")
        db_connection.commit()
    except Exception as e:
        pytest.fail(f"Failed to drop stored procedure: {e}")

def test_drop_tables_for_join(cursor, db_connection):
    """Drop tables for join operations"""
    try:
        cursor.execute("DROP TABLE IF EXISTS employees")
        cursor.execute("DROP TABLE IF EXISTS departments")
        cursor.execute("DROP TABLE IF EXISTS projects")
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

def test_close(cursor):
    """Test closing the cursor"""
    cursor.close()
    with pytest.raises(Exception):
        cursor.execute("SELECT 1")
