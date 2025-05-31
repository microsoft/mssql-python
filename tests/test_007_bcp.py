import pytest
import os
import uuid
from pathlib import Path

# Assuming your project structure allows these imports
from mssql_python import connect as mssql_connect # Alias to avoid conflict
from mssql_python.bcp_options import ColumnFormat, BCPOptions
from mssql_python.bcp_main import BCPClient

# --- Constants for Tests ---
SQL_COPT_SS_BCP = 1214 # BCP connection attribute

# --- Database Connection Details from Environment Variables ---
DB_CONNECTION_STRING = os.getenv("PYTEST_MSSQL_CONN_STR")

# Skip all tests in this file if connection string is not provided
pytestmark = pytest.mark.skipif(
    not DB_CONNECTION_STRING,
    reason="PYTEST_MSSQL_CONN_STR environment variable must be set for BCP integration tests."
)

def get_bcp_test_conn_str():
    """Returns the connection string."""
    if not DB_CONNECTION_STRING:
        # This should ideally not be reached due to pytestmark, but as a safeguard:
        pytest.skip("PYTEST_MSSQL_CONN_STR is not set.")
    return DB_CONNECTION_STRING

@pytest.fixture(scope="function")
def bcp_db_setup_and_teardown():
    """
    Fixture to set up a BCP-enabled connection and a unique test table.
    Yields (connection, table_name).
    Cleans up the table afterwards.
    """
    conn_str = get_bcp_test_conn_str()
    table_name_uuid_part = str(uuid.uuid4()).replace('-', '')[:8]
    table_name = f"dbo.pytest_bcp_table_{table_name_uuid_part}"
    
    conn = None
    cursor = None
    try:
        conn = mssql_connect(conn_str, attrs_before={SQL_COPT_SS_BCP: 1}, autocommit=True)
        cursor = conn.cursor()
        
        cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name};")
        cursor.execute(f"""
            CREATE TABLE {table_name} (
                id INT PRIMARY KEY,
                data_col VARCHAR(255) NULL
            );
        """)
        yield conn, table_name
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception as e:
                print(f"Warning: Error closing cursor during BCP test setup/teardown: {e}")
        if conn:
            cursor_cleanup = None
            try:
                cursor_cleanup = conn.cursor()
                cursor_cleanup.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name};")
            except Exception as e:
                print(f"Warning: Error during BCP test cleanup (dropping table {table_name}): {e}")
            finally:
                if cursor_cleanup:
                    try:
                        cursor_cleanup.close()
                    except Exception as e:
                        print(f"Warning: Error closing cleanup cursor: {e}")
                conn.close()

@pytest.fixture
def temp_file_pair(tmp_path):
    """Provides a pair of temporary file paths for data and errors using pytest's tmp_path."""
    file_uuid_part = str(uuid.uuid4()).replace('-', '')[:8]
    data_file = tmp_path / f"bcp_data_{file_uuid_part}.csv"
    error_file = tmp_path / f"bcp_error_{file_uuid_part}.txt"
    return data_file, error_file

# --- Tests for bcp_options.py (Unit tests, no mocking needed) ---
class TestColumnFormat:
    def test_valid_instantiation_defaults(self):
        cf = ColumnFormat()
        assert cf.prefix_len == 0
        assert cf.data_len == 0
        assert cf.field_terminator is None
        assert cf.row_terminator is None
        assert cf.server_col == 1
        assert cf.file_col == 1
        assert cf.user_data_type == 0
        assert cf.col_name is None

    def test_valid_instantiation_all_params(self):
        cf = ColumnFormat(
            prefix_len=1, data_len=10, field_terminator=b",", row_terminator=b"\n",
            server_col=2, file_col=3, user_data_type=10, col_name="TestCol"
        )
        assert cf.prefix_len == 1
        assert cf.data_len == 10
        assert cf.field_terminator == b","
        assert cf.row_terminator == b"\n"
        assert cf.server_col == 2
        assert cf.file_col == 3
        assert cf.user_data_type == 10
        assert cf.col_name == "TestCol"

    @pytest.mark.parametrize("attr, value", [
        ("prefix_len", -1), ("data_len", -1), ("server_col", 0),
        ("server_col", -1), ("file_col", 0), ("file_col", -1),
    ])
    def test_invalid_numeric_values(self, attr, value):
        with pytest.raises(ValueError):
            ColumnFormat(**{attr: value})

    @pytest.mark.parametrize("attr, value", [
        ("field_terminator", ","), ("row_terminator", "\n"),
    ])
    def test_invalid_terminator_types(self, attr, value):
        with pytest.raises(TypeError):
            ColumnFormat(**{attr: value})

class TestBCPOptions:
    _dummy_data_file = "dummy_data.csv"
    _dummy_error_file = "dummy_error.txt"

    def test_valid_instantiation_in_minimal(self):
        opts = BCPOptions(direction="in", data_file=self._dummy_data_file, error_file=self._dummy_error_file)
        assert opts.direction == "in"
        assert opts.data_file == self._dummy_data_file
        assert opts.error_file == self._dummy_error_file
        assert opts.bulk_mode == "native"

    def test_valid_instantiation_out_full(self):
        cols = [ColumnFormat(file_col=1, server_col=1, field_terminator=b'\t')]
        opts = BCPOptions(
            direction="out", data_file="output.csv", error_file="errors.log",
            bulk_mode="char", batch_size=1000, max_errors=10, first_row=1, last_row=100,
            code_page="ACP", hints="ORDER(id)", columns=cols,
            keep_identity=True, keep_nulls=True
        )
        assert opts.direction == "out"
        assert opts.bulk_mode == "char"
        assert opts.columns == cols
        assert opts.keep_identity is True

    def test_invalid_direction(self):
        with pytest.raises(ValueError, match="BCPOptions.direction 'invalid_dir' is invalid"):
            BCPOptions(direction="invalid_dir", data_file=self._dummy_data_file, error_file=self._dummy_error_file)

    @pytest.mark.parametrize("direction_to_test", ["in", "out"])
    def test_missing_data_file_for_in_out(self, direction_to_test):
        with pytest.raises(ValueError, match=f"BCPOptions.data_file is required for BCP direction '{direction_to_test}'."):
            BCPOptions(direction=direction_to_test, error_file=self._dummy_error_file)

    @pytest.mark.parametrize("direction_to_test", ["in", "out"])
    def test_missing_error_file_for_in_out(self, direction_to_test):
         with pytest.raises(ValueError, match="error_file must be provided and non-empty for 'in' or 'out' directions."):
            BCPOptions(direction=direction_to_test, data_file=self._dummy_data_file)

    def test_columns_and_format_file_conflict(self):
        with pytest.raises(ValueError, match="Cannot specify both 'columns' .* and 'format_file'"):
            BCPOptions(
                direction="in", data_file=self._dummy_data_file, error_file=self._dummy_error_file,
                columns=[ColumnFormat()], format_file="format.fmt"
            )

    def test_invalid_bulk_mode(self):
        with pytest.raises(ValueError, match="BCPOptions.bulk_mode 'invalid_mode' is invalid"):
            BCPOptions(direction="in", data_file=self._dummy_data_file, error_file=self._dummy_error_file, bulk_mode="invalid_mode")

    @pytest.mark.parametrize("attr, value", [
        ("batch_size", -1), ("max_errors", -1), ("first_row", -1), ("last_row", -1),
    ])
    def test_negative_control_values(self, attr, value):
        with pytest.raises(ValueError, match=f"BCPOptions.{attr} must be non-negative"):
            BCPOptions(direction="in", data_file=self._dummy_data_file, error_file=self._dummy_error_file, **{attr: value})

    def test_first_row_greater_than_last_row(self):
        with pytest.raises(ValueError, match="BCPOptions.first_row cannot be greater than BCPOptions.last_row"):
            BCPOptions(direction="in", data_file=self._dummy_data_file, error_file=self._dummy_error_file, first_row=10, last_row=5)

    def test_invalid_codepage_negative_int(self):
        with pytest.raises(ValueError, match="BCPOptions.code_page, if an integer, must be non-negative"):
            BCPOptions(direction="in", data_file=self._dummy_data_file, error_file=self._dummy_error_file, code_page=-1)

# --- Tests for bcp_main.py (Integration Tests) ---
class TestBCPClientIntegration:

    def test_init_success_with_real_connection(self, bcp_db_setup_and_teardown):
        conn, _ = bcp_db_setup_and_teardown
        client = BCPClient(connection=conn)
        assert client.wrapper is not None
        assert isinstance(client.wrapper, CppBCPWrapper), \
            "BCPClient.wrapper is not an instance of the C++ BCPWrapper"

    def test_init_connection_none(self): 
        with pytest.raises(ValueError, match="A valid connection object is required"):
            BCPClient(connection=None)

    def test_init_connection_missing_conn_attr(self): 
        class MockPythonConnectionMissingInternal: 
            pass
        invalid_conn = MockPythonConnectionMissingInternal()
        with pytest.raises(TypeError, match="The Python Connection object is missing the '_conn' attribute"):
            BCPClient(connection=invalid_conn)

    def test_sql_bulk_copy_char_mode_successful_import(self, bcp_db_setup_and_teardown, temp_file_pair):
        conn, table_name = bcp_db_setup_and_teardown
        data_file, error_file = temp_file_pair

        sample_data_list = [[1, "Alice"], [2, "Bob"], [3, "Charlie"]]
        with open(data_file, "w", encoding="utf-8") as f:
            for i, row in enumerate(sample_data_list):
                f.write(f"{row[0]},{row[1]}")
                if i < len(sample_data_list) - 1: 
                    f.write("\n")

        client = BCPClient(connection=conn)
        cols = [
            ColumnFormat(file_col=1, server_col=1, field_terminator=b","),
            ColumnFormat(file_col=2, server_col=2, row_terminator=b"\n")
        ]
        options = BCPOptions(
            direction="in",
            data_file=str(data_file),
            error_file=str(error_file),
            bulk_mode="char",
            columns=cols
        )

        client.sql_bulk_copy(table=table_name, options=options)
        
        cursor = None
        try:
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            assert count == len(sample_data_list)

            cursor.execute(f"SELECT id, data_col FROM {table_name} ORDER BY id")
            db_rows = cursor.fetchall()
            for i, expected_row in enumerate(sample_data_list):
                assert db_rows[i][0] == expected_row[0]
                assert db_rows[i][1] == expected_row[1]
        finally:
            if cursor:
                cursor.close()

        assert not error_file.exists() or error_file.stat().st_size == 0, \
            f"BCP error file {error_file} was created or not empty."

    def test_sql_bulk_copy_in_char_mode_successful_import(self, bcp_db_setup_and_teardown, temp_file_pair):
        conn, table_name = bcp_db_setup_and_teardown
        data_file, error_file = temp_file_pair

        sample_data_list = [[1, "Alice Char"], [2, "Bob Char"], [3, "Charlie Char"]]
        # Standard CSV format
        file_content = "\n".join([f"{row[0]},{row[1]}" for row in sample_data_list]) + "\n"
        with open(data_file, "w", encoding="utf-8") as f:
            f.write(file_content)

        client = BCPClient(connection=conn)
        cols = [
            ColumnFormat(file_col=1, server_col=1, field_terminator=b","),
            ColumnFormat(file_col=2, server_col=2, row_terminator=b"\n") 
        ]
        options = BCPOptions(
            direction="in",
            data_file=str(data_file),
            error_file=str(error_file),
            bulk_mode="char",
            columns=cols
        )

        client.sql_bulk_copy(table=table_name, options=options)
        
        cursor = None
        try:
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            assert count == len(sample_data_list)

            cursor.execute(f"SELECT id, data_col FROM {table_name} ORDER BY id")
            db_rows = cursor.fetchall()
            for i, expected_row in enumerate(sample_data_list):
                assert db_rows[i][0] == expected_row[0]
                assert db_rows[i][1] == expected_row[1]
        finally:
            if cursor:
                cursor.close()

        assert not error_file.exists() or error_file.stat().st_size == 0, \
            f"BCP error file {error_file} was created or not empty for char in."

    def test_sql_bulk_copy_out_char_mode_successful_export(self, bcp_db_setup_and_teardown, temp_file_pair):
        conn, table_name = bcp_db_setup_and_teardown
        data_file, error_file = temp_file_pair

        sample_data_list = [[10, "Export Data One"], [20, "Export Data Two"]]
        cursor = None
        try:
            cursor = conn.cursor()
            for row in sample_data_list:
                cursor.execute(f"INSERT INTO {table_name} (id, data_col) VALUES (?, ?)", row[0], row[1])
            conn.commit() # Ensure data is written before BCP OUT
        finally:
            if cursor:
                cursor.close()

        client = BCPClient(connection=conn)
        cols = [
            ColumnFormat(file_col=1, server_col=1, field_terminator=b","),
            ColumnFormat(file_col=2, server_col=2, row_terminator=b"\r\n") # Using CRLF for variety
        ]
        options = BCPOptions(
            direction="out",
            data_file=str(data_file),
            error_file=str(error_file),
            bulk_mode="char",
            columns=cols
        )

        client.sql_bulk_copy(table=table_name, options=options)

        assert data_file.exists() and data_file.stat().st_size > 0, \
            f"BCP data file {data_file} was not created or is empty for char out."
        
        with open(data_file, "r", encoding="utf-8") as f:
            content = f.read()
            expected_content = ""
            for row in sample_data_list:
                expected_content += f"{row[0]},{row[1]}\r\n"
            assert content == expected_content

        assert not error_file.exists() or error_file.stat().st_size == 0, \
            f"BCP error file {error_file} was created or not empty for char out."

    def test_sql_bulk_copy_in_native_mode(self, bcp_db_setup_and_teardown, temp_file_pair):
        conn, table_name = bcp_db_setup_and_teardown
        native_data_file, error_file_bcp_out = temp_file_pair # Files for intermediate BCP OUT
        _, error_file_bcp_in = temp_file_pair # Error file for the actual BCP IN test

        original_data = [[77, "Native In Data"], [88, "More Native"]]

        # 1. Insert initial data and BCP OUT in native format to create a test file
        cursor = None
        try:
            cursor = conn.cursor()
            for row_data in original_data:
                cursor.execute(f"INSERT INTO {table_name} (id, data_col) VALUES (?, ?)", row_data[0], row_data[1])
            conn.commit()
        finally:
            if cursor:
                cursor.close()
        
        client_out = BCPClient(connection=conn)
        # For native out, column definitions are minimal (no terminators)
        # user_data_type=0, prefix_len=0, data_len=0 usually means native
        native_cols_format = [
            ColumnFormat(file_col=1, server_col=1, user_data_type=0, prefix_len=0, data_len=0),
            ColumnFormat(file_col=2, server_col=2, user_data_type=0, prefix_len=0, data_len=0)
        ]
        options_out = BCPOptions(
            direction="out",
            data_file=str(native_data_file),
            error_file=str(error_file_bcp_out),
            bulk_mode="native",
            columns=native_cols_format 
        )
        client_out.sql_bulk_copy(table=table_name, options=options_out)
        assert native_data_file.exists() and native_data_file.stat().st_size > 0, "Native data file not created by BCP OUT"
        assert not error_file_bcp_out.exists() or error_file_bcp_out.stat().st_size == 0, "Errors during native BCP OUT"


        # 2. Clear the table
        cursor = None
        try:
            cursor = conn.cursor()
            cursor.execute(f"DELETE FROM {table_name}")
            conn.commit()
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            assert cursor.fetchone()[0] == 0, "Table not cleared before BCP IN native"
        finally:
            if cursor:
                cursor.close()

        # 3. BCP IN the native file
        client_in = BCPClient(connection=conn)
        options_in = BCPOptions(
            direction="in",
            data_file=str(native_data_file),
            error_file=str(error_file_bcp_in),
            bulk_mode="native",
            columns=native_cols_format # Same column format for native in
        )
        client_in.sql_bulk_copy(table=table_name, options=options_in)

        # 4. Verify data
        cursor = None
        try:
            cursor = conn.cursor()
            cursor.execute(f"SELECT id, data_col FROM {table_name} ORDER BY id")
            imported_rows = cursor.fetchall()
            assert len(imported_rows) == len(original_data)
            for i, expected_row in enumerate(original_data):
                assert imported_rows[i][0] == expected_row[0]
                assert imported_rows[i][1] == expected_row[1]
        finally:
            if cursor:
                cursor.close()
        
        assert not error_file_bcp_in.exists() or error_file_bcp_in.stat().st_size == 0, \
            f"BCP error file {error_file_bcp_in} created for native BCP IN."


    def test_sql_bulk_copy_out_native_mode(self, bcp_db_setup_and_teardown, temp_file_pair):
        conn, table_name_orig = bcp_db_setup_and_teardown
        native_data_file, error_file_out = temp_file_pair
        _, error_file_in_verify = temp_file_pair # For verification step

        original_data = [[91, "Native Out One"], [92, "Native Out Two"]]
        
        # 1. Insert data into the original table
        cursor = None
        try:
            cursor = conn.cursor()
            for row_data in original_data:
                cursor.execute(f"INSERT INTO {table_name_orig} (id, data_col) VALUES (?, ?)", row_data[0], row_data[1])
            conn.commit()
        finally:
            if cursor:
                cursor.close()

        # 2. BCP OUT in native mode
        client_out = BCPClient(connection=conn)
        native_cols_format = [
            ColumnFormat(file_col=1, server_col=1, user_data_type=0, prefix_len=0, data_len=0),
            ColumnFormat(file_col=2, server_col=2, user_data_type=0, prefix_len=0, data_len=0)
        ]
        options_out = BCPOptions(
            direction="out",
            data_file=str(native_data_file),
            error_file=str(error_file_out),
            bulk_mode="native",
            columns=native_cols_format
        )
        client_out.sql_bulk_copy(table=table_name_orig, options=options_out)

        assert native_data_file.exists() and native_data_file.stat().st_size > 0, \
            "Native data file not created by BCP OUT for verification."
        assert not error_file_out.exists() or error_file_out.stat().st_size == 0, \
            "Errors during BCP OUT native."

        # 3. Verify by BCPing the native file into a new table and comparing data
        table_name_verify = f"{table_name_orig}_verify_native"
        cursor = None
        try:
            cursor = conn.cursor()
            cursor.execute(f"IF OBJECT_ID('{table_name_verify}', 'U') IS NOT NULL DROP TABLE {table_name_verify};")
            cursor.execute(f"CREATE TABLE {table_name_verify} (id INT PRIMARY KEY, data_col VARCHAR(255) NULL);")
            conn.commit()
        finally:
            if cursor:
                cursor.close()

        client_in_verify = BCPClient(connection=conn)
        options_in_verify = BCPOptions(
            direction="in",
            data_file=str(native_data_file),
            error_file=str(error_file_in_verify),
            bulk_mode="native",
            columns=native_cols_format
        )
        client_in_verify.sql_bulk_copy(table=table_name_verify, options=options_in_verify)
        assert not error_file_in_verify.exists() or error_file_in_verify.stat().st_size == 0, \
            "Errors during verification BCP IN of native file."

        cursor = None
        try:
            cursor = conn.cursor()
            cursor.execute(f"SELECT id, data_col FROM {table_name_verify} ORDER BY id")
            verified_rows = cursor.fetchall()
            assert len(verified_rows) == len(original_data)
            for i, expected_row in enumerate(original_data):
                assert verified_rows[i][0] == expected_row[0]
                assert verified_rows[i][1] == expected_row[1]
        finally:
            if cursor:
                cursor.close()
            # Clean up the verification table
            cursor_cleanup_verify = None
            try:
                cursor_cleanup_verify = conn.cursor()
                cursor_cleanup_verify.execute(f"IF OBJECT_ID('{table_name_verify}', 'U') IS NOT NULL DROP TABLE {table_name_verify};")
                conn.commit()
            except Exception as e_cleanup:
                print(f"Warning: Error cleaning up verification table {table_name_verify}: {e_cleanup}")
            finally:
                if cursor_cleanup_verify:
                    cursor_cleanup_verify.close()
    
    def test_sql_bulk_copy_with_errors_and_max_errors(self, bcp_db_setup_and_teardown, temp_file_pair):
        conn, table_name = bcp_db_setup_and_teardown
        data_file, error_file = temp_file_pair

        file_content = "100,GoodData1\nXXX,BadDataForIntColumn\n200,GoodData2\n"
        with open(data_file, "w", encoding="utf-8") as f:
            f.write(file_content)

        client = BCPClient(connection=conn)
        cols = [
            ColumnFormat(file_col=1, server_col=1, field_terminator=b","),
            ColumnFormat(file_col=2, server_col=2, row_terminator=b"\n")
        ]
        options = BCPOptions(
            direction="in",
            data_file=str(data_file),
            error_file=str(error_file),
            bulk_mode="char",
            columns=cols,
            max_errors=2 
        )

        client.sql_bulk_copy(table=table_name, options=options)
        
        cursor = None
        try:
            cursor = conn.cursor()
            cursor.execute(f"SELECT id FROM {table_name} ORDER BY id")
            ids = [r[0] for r in cursor.fetchall()]
            assert 100 in ids
            assert 200 in ids
            assert len(ids) == 2 
        finally:
            if cursor:
                cursor.close()

        assert error_file.exists() and error_file.stat().st_size > 0, \
            f"BCP error file {error_file} was not created or is empty when errors were expected."

    def test_sql_bulk_copy_keep_nulls_option(self, bcp_db_setup_and_teardown, temp_file_pair):
        conn, table_name = bcp_db_setup_and_teardown 
        data_file, error_file = temp_file_pair

        file_content = "301,HasData\n302,\n303,MoreData\n" 
        with open(data_file, "w", encoding="utf-8") as f:
            f.write(file_content)

        client = BCPClient(connection=conn)
        cols = [
            ColumnFormat(file_col=1, server_col=1, field_terminator=b","),
            ColumnFormat(file_col=2, server_col=2, row_terminator=b"\n")
        ]
        options = BCPOptions(
            direction="in",
            data_file=str(data_file),
            error_file=str(error_file),
            bulk_mode="char",
            columns=cols,
            keep_nulls=True 
        )

        client.sql_bulk_copy(table=table_name, options=options)
        
        cursor = None
        try:
            cursor = conn.cursor()
            cursor.execute(f"SELECT id, data_col FROM {table_name} ORDER BY id")
            results = {row[0]: row[1] for row in cursor.fetchall()}
            assert len(results) == 3
            assert results[301] == "HasData"
            assert results[302] is None, "Empty field was not inserted as NULL with KEEPNULLS=True"
            assert results[303] == "MoreData"
        finally:
            if cursor:
                cursor.close()
        
        assert not error_file.exists() or error_file.stat().st_size == 0, \
            f"BCP error file {error_file} created unexpectedly for keep_nulls test."


    def test_sql_bulk_copy_no_table_name(self, bcp_db_setup_and_teardown, temp_file_pair):
        conn, _ = bcp_db_setup_and_teardown
        data_file, error_file = temp_file_pair
        client = BCPClient(connection=conn)
        options = BCPOptions(direction="in", data_file=str(data_file), error_file=str(error_file))
        with pytest.raises(ValueError, match="The 'table' name for BCP must be provided and non-empty."):
            client.sql_bulk_copy(table="", options=options)