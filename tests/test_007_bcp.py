import pytest
import os
import uuid
import datetime
from decimal import Decimal
from typing import List, Any, Optional, Union
# Connection will be handled by conftest.py's db_connection fixture
# from mssql_python.connection import Connection 
from mssql_python.bcp_main import BCPClient
from mssql_python.bcp_options import BCPOptions, ColumnFormat
from mssql_python.constants import BCPControlOptions, ConstantsDDBC # Import ConstantsDDBC

# Configuration for your test database is now handled by conftest.py's conn_str fixture
# TEST_DB_CONNECTION_STRING = os.environ.get("MSSQL_PY_CONNECTION_STRING", "DRIVER={ODBC Driver 18 for SQL Server};Server=tcp:DESKTOP-1A982SC,1433;Database=TestBCP;TrustServerCertificate=yes;Trusted_Connection=yes;")

# Using ConstantsDDBC for C types.
# Note: As per provided constants.py, ConstantsDDBC.SQL_C_CHAR.value is -8.
# Standard ODBC SQL_C_CHAR is typically 1. If -8 is used for SQL_C_CHAR,
# it implies C-level character data might be treated as wide characters.
# This could affect behavior of bulk_mode="char". Tests proceed assuming constants.py is correct for the environment.

# db_connection_module fixture is removed as db_connection from conftest.py will be used.

@pytest.fixture(scope="function")
def bcp_client(db_connection): # Uses db_connection from conftest.py
    return BCPClient(connection=db_connection)

@pytest.fixture(scope="function")
def test_table_manager(db_connection): # Uses db_connection from conftest.py
    created_tables = []
    # Create a new cursor for the test_table_manager's operations
    # to avoid conflicts if the conftest.py cursor is used elsewhere.
    with db_connection.cursor() as cursor_manager: 
        def _create_table(schema: str, name_suffix: Optional[str] = None):
            base_name = f"bcp_test_{str(uuid.uuid4()).replace('-', '')}"
            table_name = f"{base_name}_{name_suffix}" if name_suffix else base_name
            try:
                cursor_manager.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
                db_connection.commit() 
                cursor_manager.execute(schema.format(table_name=table_name))
                db_connection.commit()
                created_tables.append(table_name)
                return table_name
            except Exception as e:
                pytest.fail(f"Failed to create table {table_name}: {e}")
        yield _create_table
        try:
            for table_name in created_tables:
                cursor_manager.execute(f"DROP TABLE IF EXISTS {table_name}")
            db_connection.commit()
        except Exception as e:
            print(f"Warning: Error during test table cleanup: {e}")
        # The cursor_manager is closed automatically by the 'with' statement.

def create_data_file(file_path: str, data: List[List[Any]], delimiter: str = ",", row_terminator: str = "\n", encoding='utf-8'):
    """Helper to create a sample data file with specified delimiter and row terminator."""
    with open(file_path, 'w', encoding=encoding) as f:
        if not data: # Handle empty data case explicitly by creating an empty file
            return
        for row_items in data:
            line = delimiter.join(map(str, row_items))
            f.write(line + row_terminator) # Append row terminator to each line

SCHEMA_SIMPLE = """
CREATE TABLE {table_name} ( id INT PRIMARY KEY, name VARCHAR(100), value DECIMAL(10, 2) )
"""
SCHEMA_IDENTITY = """
CREATE TABLE {table_name} ( id INT IDENTITY(1,1) PRIMARY KEY, data VARCHAR(100) )
"""
SCHEMA_NULLABLE_NAME = """
CREATE TABLE {table_name} ( id INT PRIMARY KEY, name VARCHAR(100) NULL, value DECIMAL(10, 2) )
"""
SCHEMA_ALL_TYPES_FOR_CHAR = """
CREATE TABLE {table_name} (
    c_int INT PRIMARY KEY, c_varchar VARCHAR(50), c_nvarchar NVARCHAR(50),
    c_decimal DECIMAL(18, 5), c_float FLOAT, c_date DATE, c_datetime DATETIME2, c_bit BIT
)
"""


# --- BCPOptions Validation Tests ---
def test_bcp_options_validation_missing_data_error_file(tmp_path):
    with pytest.raises(ValueError, match="data_file must be provided"):
        BCPOptions(direction="in", error_file=str(tmp_path / "err.txt"))
    with pytest.raises(ValueError, match="error_file must be provided"):
        BCPOptions(direction="out", data_file=str(tmp_path / "data.txt"))

def test_bcp_options_validation_invalid_direction():
    with pytest.raises(ValueError, match="BCPOptions.direction 'sideways' is invalid"):
        BCPOptions(direction="sideways", data_file="d.txt", error_file="e.txt")

def test_bcp_options_validation_invalid_bulk_mode():
    with pytest.raises(ValueError, match="BCPOptions.bulk_mode 'exotic' is invalid"):
        BCPOptions(direction="in", data_file="d.txt", error_file="e.txt", bulk_mode="exotic")

def test_bcp_options_validation_negative_numbers():
    with pytest.raises(ValueError, match="batch_size must be non-negative"):
        BCPOptions(direction="in", data_file="d.txt", error_file="e.txt", batch_size=-1)
    with pytest.raises(ValueError, match="first_row cannot be greater than BCPOptions.last_row"):
        BCPOptions(direction="in", data_file="d.txt", error_file="e.txt", first_row=5, last_row=1)

def test_bcp_options_format_file_with_columns(tmp_path):
    col_fmt = ColumnFormat(file_col=1, server_col=1, user_data_type=ConstantsDDBC.SQL_C_CHAR.value, data_len=10)
    with pytest.raises(ValueError, match="Cannot specify both 'columns' .* and 'format_file'"):
        BCPOptions(direction="in", data_file="d.txt", error_file="e.txt",
                   format_file=str(tmp_path / "format.fmt"), columns=[col_fmt])

# --- BCP IN Tests ---
# db_connection_module argument is replaced by db_connection from conftest.py
def test_bcp_in_char_mode(bcp_client, test_table_manager, db_connection, tmp_path):
    table_name = test_table_manager(SCHEMA_SIMPLE, "in_char")
    data_file = tmp_path / "bcp_in_char.csv"
    error_file = tmp_path / "bcp_in_char.err"
    sample_data = [[1, "TestName1", "10.50"], [2, "TestName2", "20.75"]]
    create_data_file(str(data_file), sample_data, row_terminator="\n")

    options = BCPOptions(
        direction='in', data_file=str(data_file), error_file=str(error_file),
        bulk_mode="char",
        columns=[ColumnFormat(field_terminator=b',', row_terminator=b'\n')] 
    )
    bcp_client.sql_bulk_copy(table=table_name, options=options)

    with db_connection.cursor() as cursor_check: # Use a new cursor for verification
        cursor_check.execute(f"SELECT id, name, value FROM {table_name} ORDER BY id")
        rows = cursor_check.fetchall()
    assert rows == [(1, "TestName1", Decimal("10.50")), (2, "TestName2", Decimal("20.75"))]
    assert error_file.stat().st_size == 0

@pytest.mark.skip(reason="C++ BCPWrapper::set_bulk_mode needs to support 'unicode' mode mapping and ensure correct handling of SQL_C_WCHAR.")
def test_bcp_in_unicode_mode(bcp_client, test_table_manager, db_connection, tmp_path):
    table_name = test_table_manager(SCHEMA_SIMPLE, "in_unicode")
    data_file = tmp_path / "bcp_in_unicode.csv"
    error_file = tmp_path / "bcp_in_unicode.err"
    sample_data = [[1, "UnicodeNameĀā", "100.99"], [2, "TestÑame2", "200.01"]]
    create_data_file(str(data_file), sample_data, encoding='utf-16le', row_terminator="\n") 

    options = BCPOptions(
        direction='in', data_file=str(data_file), error_file=str(error_file),
        bulk_mode="unicode", 
        columns=[ColumnFormat(field_terminator=b',', row_terminator=b'\n')]
    )
    bcp_client.sql_bulk_copy(table=table_name, options=options)

    with db_connection.cursor() as cursor_check:
        cursor_check.execute(f"SELECT id, name, value FROM {table_name} WHERE id = 1")
        row = cursor_check.fetchone()
    assert row == (1, "UnicodeNameĀā", Decimal("100.99"))
    assert error_file.stat().st_size == 0

def test_bcp_in_native_mode_cycle(bcp_client, test_table_manager, db_connection, tmp_path):
    table_name_out = test_table_manager(SCHEMA_ALL_TYPES_FOR_CHAR, "native_out_cycle")
    table_name_in = test_table_manager(SCHEMA_ALL_TYPES_FOR_CHAR, "native_in_cycle")
    data_file = tmp_path / "bcp_native.dat"
    error_file_out = tmp_path / "bcp_native_out.err"
    error_file_in = tmp_path / "bcp_native_in.err"

    with db_connection.cursor() as cursor_op:
        dt_val = datetime.datetime(2023, 1, 15, 14, 30, 0)
        date_val = datetime.date(2023, 1, 15)
        cursor_op.execute(f"""
            INSERT INTO {table_name_out} (c_int, c_varchar, c_nvarchar, c_decimal, c_float, c_date, c_datetime, c_bit)
            VALUES (1, 'VarcharS', N'NvarcharŠ', 123.45678, 1.23e4, '{date_val}', '{dt_val}', 1)
        """)
        db_connection.commit()

        options_out = BCPOptions(direction='out', data_file=str(data_file), error_file=str(error_file_out), bulk_mode="native")
        bcp_client.sql_bulk_copy(table=table_name_out, options=options_out)
        assert data_file.exists() and data_file.stat().st_size > 0
        assert error_file_out.stat().st_size == 0

        options_in = BCPOptions(direction='in', data_file=str(data_file), error_file=str(error_file_in), bulk_mode="native")
        bcp_client.sql_bulk_copy(table=table_name_in, options=options_in)
        assert error_file_in.stat().st_size == 0

        cursor_op.execute(f"SELECT c_int, c_varchar, c_nvarchar, c_decimal, c_float, c_date, c_datetime, c_bit FROM {table_name_in} WHERE c_int = 1")
        row = cursor_op.fetchone()
    assert row == (1, "VarcharS", "NvarcharŠ", Decimal("123.45678"), pytest.approx(1.23e4), date_val, dt_val, True)


def test_bcp_in_with_controls(bcp_client, test_table_manager, db_connection, tmp_path):
    table_name = test_table_manager(SCHEMA_SIMPLE, "in_controls")
    data_file = tmp_path / "bcp_in_controls.csv"
    error_file = tmp_path / "bcp_in_controls.err"
    sample_data = [[i, f"Name{i}", f"{i}.00"] for i in range(1, 21)] 
    create_data_file(str(data_file), sample_data, row_terminator="ENDOFLINE\n")

    options = BCPOptions(
        direction='in', data_file=str(data_file), error_file=str(error_file),
        bulk_mode="char",
        batch_size=5, first_row=3, last_row=12, 
        columns=[ColumnFormat(field_terminator=b',', row_terminator=b'ENDOFLINE\n')]
    )
    bcp_client.sql_bulk_copy(table=table_name, options=options)

    with db_connection.cursor() as cursor_check:
        cursor_check.execute(f"SELECT COUNT(*) FROM {table_name}")
        assert cursor_check.fetchone()[0] == 10 
        cursor_check.execute(f"SELECT MIN(id), MAX(id) FROM {table_name}")
        min_id, max_id = cursor_check.fetchone()
    assert min_id == 3
    assert max_id == 12
    assert error_file.stat().st_size == 0

def test_bcp_in_keep_nulls_identity_tablock(bcp_client, test_table_manager, db_connection, tmp_path):
    table_name = test_table_manager(SCHEMA_IDENTITY.replace("VARCHAR(100)", "VARCHAR(100) NULL"), "in_special")
    data_file = tmp_path / "bcp_in_special.csv"
    error_file = tmp_path / "bcp_in_special.err"
    sample_data = [[101, "DataRow101"], [102, ""], [103, "DataRow103"]]
    create_data_file(str(data_file), sample_data)

    options = BCPOptions(
        direction='in', data_file=str(data_file), error_file=str(error_file),
        bulk_mode="char",
        keep_identity=True, keep_nulls=True, hints="TABLOCK", 
        columns=[ColumnFormat(field_terminator=b',', row_terminator=b'\n')]
    )
    bcp_client.sql_bulk_copy(table=table_name, options=options)

    with db_connection.cursor() as cursor_check:
        cursor_check.execute(f"SELECT id, data FROM {table_name} ORDER BY id")
        rows = cursor_check.fetchall()
    assert rows == [(101, "DataRow101"), (102, None), (103, "DataRow103")]
    assert error_file.stat().st_size == 0

def test_bcp_in_with_column_definitions(bcp_client, test_table_manager, db_connection, tmp_path):
    table_name = test_table_manager(SCHEMA_SIMPLE, "in_coldef")
    data_file = tmp_path / "bcp_in_coldef.txt"
    error_file = tmp_path / "bcp_in_coldef.err"
    sample_data = [["DefName1", "1", "15.50"], ["DefName2", "2", "25.75"]]
    create_data_file(str(data_file), sample_data, delimiter="|", row_terminator="\r\n")

    options = BCPOptions(
        direction='in', data_file=str(data_file), error_file=str(error_file),
        bulk_mode="char", 
        columns=[
            ColumnFormat(file_col=1, server_col=2, user_data_type=ConstantsDDBC.SQL_C_CHAR.value, data_len=8, field_terminator=b'|'), 
            ColumnFormat(file_col=2, server_col=1, user_data_type=ConstantsDDBC.SQL_C_CHAR.value, data_len=3, field_terminator=b'|'), 
            ColumnFormat(file_col=3, server_col=3, user_data_type=ConstantsDDBC.SQL_C_CHAR.value, data_len=6, field_terminator=b'\r\n', row_terminator=b'\r\n')
        ]
    )
    bcp_client.sql_bulk_copy(table=table_name, options=options)

    with db_connection.cursor() as cursor_check:
        cursor_check.execute(f"SELECT id, name, value FROM {table_name} ORDER BY id")
        rows = cursor_check.fetchall()
    assert rows == [(1, "DefName1", Decimal("15.50")), (2, "DefName2", Decimal("25.75"))]
    assert error_file.stat().st_size == 0


# --- BCP OUT Tests ---
def test_bcp_out_char_mode(bcp_client, test_table_manager, db_connection, tmp_path):
    table_name = test_table_manager(SCHEMA_SIMPLE, "out_char")
    output_file = tmp_path / "bcp_out_char.csv"
    error_file = tmp_path / "bcp_out_char.err"

    with db_connection.cursor() as cursor_op:
        cursor_op.execute(f"INSERT INTO {table_name} (id, name, value) VALUES (1, 'Export1', 11.22), (2, 'Export2', 33.44)")
        db_connection.commit()

    options = BCPOptions(
        direction='out', data_file=str(output_file), error_file=str(error_file),
        bulk_mode="char",
        columns=[ColumnFormat(field_terminator=b',', row_terminator=b'\r\n')]
    )
    bcp_client.sql_bulk_copy(table=table_name, options=options)

    assert output_file.exists()
    with open(output_file, 'r', encoding='utf-8') as f: content = f.read()
    lines = [line.strip() for line in content.strip().replace('\r\n', '\n').split('\n')]
    assert lines[0].startswith("1,Export1,11.22") 
    assert lines[1].startswith("2,Export2,33.44")
    assert error_file.stat().st_size == 0

# --- Error Handling and Other Tests ---
def test_bcp_in_error_file_logging(bcp_client, test_table_manager, db_connection, tmp_path):
    table_name = test_table_manager(SCHEMA_SIMPLE, "in_errorlog")
    data_file = tmp_path / "bcp_in_errorlog.csv"
    error_file = tmp_path / "bcp_in_errorlog.err" 
    sample_data = [[1, "GoodRow", "10.00"], ["bad_id", "BadRow", "20.00"], [3, "GoodRow2", "30.00"]]
    create_data_file(str(data_file), sample_data)

    options = BCPOptions(
        direction='in', data_file=str(data_file), error_file=str(error_file),
        bulk_mode="char", max_errors=5,
        columns=[ColumnFormat(field_terminator=b',', row_terminator=b'\n')]
    )
    bcp_client.sql_bulk_copy(table=table_name, options=options)

    with db_connection.cursor() as cursor_check:
        cursor_check.execute(f"SELECT COUNT(*) FROM {table_name}")
        assert cursor_check.fetchone()[0] == 2 
    assert error_file.exists() and error_file.stat().st_size > 0
    with open(error_file, 'r', encoding='utf-8', errors='ignore') as f:
        error_content = f.read()
    assert "bad_id" in error_content 

def test_bcp_in_to_non_existent_table(bcp_client, tmp_path):
    data_file = tmp_path / "dummy_data.csv"
    error_file = tmp_path / "dummy_err.txt"
    create_data_file(str(data_file), [[1, "data"]])
    non_existent_table = f"non_existent_table_{str(uuid.uuid4()).replace('-', '')}"

    options = BCPOptions(direction='in', data_file=str(data_file), error_file=str(error_file), bulk_mode="char")
    with pytest.raises(RuntimeError): 
        bcp_client.sql_bulk_copy(table=non_existent_table, options=options)

def test_bcp_in_empty_data_file(bcp_client, test_table_manager, db_connection, tmp_path):
    table_name = test_table_manager(SCHEMA_SIMPLE, "in_empty")
    data_file = tmp_path / "bcp_in_empty.csv"
    error_file = tmp_path / "bcp_in_empty.err"
    create_data_file(str(data_file), []) 

    options = BCPOptions(
        direction='in', data_file=str(data_file), error_file=str(error_file),
        bulk_mode="char",
        columns=[ColumnFormat(field_terminator=b',', row_terminator=b'\n')]
    )
    bcp_client.sql_bulk_copy(table=table_name, options=options)

    with db_connection.cursor() as cursor_check:
        cursor_check.execute(f"SELECT COUNT(*) FROM {table_name}")
        assert cursor_check.fetchone()[0] == 0

def test_bcp_out_empty_table(bcp_client, test_table_manager, db_connection, tmp_path): # Added db_connection
    table_name = test_table_manager(SCHEMA_SIMPLE, "out_empty")
    output_file = tmp_path / "bcp_out_empty.csv"
    error_file = tmp_path / "bcp_out_empty.err"

    options = BCPOptions(
        direction='out', data_file=str(output_file), error_file=str(error_file),
        bulk_mode="char",
        columns=[ColumnFormat(field_terminator=b',', row_terminator=b'\n')]
    )
    bcp_client.sql_bulk_copy(table=table_name, options=options)

    assert output_file.exists()
    assert output_file.stat().st_size == 0 
    assert error_file.stat().st_size == 0
    
DUMMY_XML_FORMAT_FILE_CONTENT = """<?xml version="1.0"?>
<BCPFORMAT xmlns="http://schemas.microsoft.com/sqlserver/2004/bulkload/format" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
 <RECORD>
  <FIELD ID="1" xsi:type="CharTerm" TERMINATOR="," MAX_LENGTH="12"/>
  <FIELD ID="2" xsi:type="CharTerm" TERMINATOR="," MAX_LENGTH="100" COLLATION="SQL_Latin1_General_CP1_CI_AS"/>
  <FIELD ID="3" xsi:type="CharTerm" TERMINATOR="\\n" MAX_LENGTH="24"/>
 </RECORD>
 <ROW>
  <COLUMN SOURCE="1" NAME="id" xsi:type="SQLINT"/>
  <COLUMN SOURCE="2" NAME="name" xsi:type="SQLVARYCHAR"/>
  <COLUMN SOURCE="3" NAME="value" xsi:type="SQLDECIMAL"/>
 </ROW>
</BCPFORMAT>
"""
@pytest.mark.skip(reason="Test requires a valid BCP format file and robust BCPWrapper::read_format_file implementation.")
def test_bcp_in_with_existing_format_file(bcp_client, test_table_manager, db_connection, tmp_path):
    table_name = test_table_manager(SCHEMA_SIMPLE, "in_fmtfile")
    data_file = tmp_path / "bcp_in_fmtfile_data.csv"
    error_file = tmp_path / "bcp_in_fmtfile.err"
    format_file = tmp_path / "mytestformat.fmt"

    sample_data = [[101, "FmtName1", "101.00"], [102, "FmtName2", "102.00"]]
    create_data_file(str(data_file), sample_data, delimiter=",", row_terminator="\n")
    with open(format_file, "w") as f:
        f.write(DUMMY_XML_FORMAT_FILE_CONTENT)

    options = BCPOptions(
        direction='in',
        data_file=str(data_file),
        error_file=str(error_file),
        format_file=str(format_file) 
    )
    bcp_client.sql_bulk_copy(table=table_name, options=options)

    with db_connection.cursor() as cursor_check:
        cursor_check.execute(f"SELECT COUNT(*) FROM {table_name}")
        assert cursor_check.fetchone()[0] == 2
        cursor_check.execute(f"SELECT id, name FROM {table_name} WHERE id = 101")
        assert cursor_check.fetchone() == (101, "FmtName1")
    assert error_file.stat().st_size == 0

@pytest.mark.skip(reason="Queryout support in C++ BCPWrapper and BCPClient needs verification/completion. Constants.SUPPORTED_DIRECTIONS is ('in', 'out').")
def test_bcp_queryout(bcp_client, test_table_manager, db_connection, tmp_path):
    table_name = test_table_manager(SCHEMA_SIMPLE, "queryout_tbl")
    output_file = tmp_path / "bcp_queryout_data.csv"
    error_file = tmp_path / "bcp_queryout.err"

    with db_connection.cursor() as cursor_op:
        cursor_op.execute(f"INSERT INTO {table_name} (id, name, value) VALUES (10, 'QueryA', 1.01), (20, 'QueryB', 2.02), (30, 'QueryC', 3.03)")
        db_connection.commit()

    query = f"SELECT id, name FROM {table_name} WHERE value > 2.0 ORDER BY id"
    
    options = BCPOptions(
        direction='out', 
        data_file=str(output_file),
        error_file=str(error_file),
        bulk_mode="char",
        columns=[ColumnFormat(field_terminator=b';', row_terminator=b'\n')]
    )
    bcp_client.sql_bulk_copy(table=query, options=options) 

    assert output_file.exists()
    with open(output_file, 'r', encoding='utf-8') as f:
        lines = [line.strip() for line in f.readlines()]
    assert lines == ["20;QueryB", "30;QueryC"]
    assert error_file.stat().st_size == 0