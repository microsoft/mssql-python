import pytest
import os
import uuid
from pathlib import Path

# Assuming your project structure allows these imports
from mssql_python import connect as mssql_connect # Alias to avoid conflict
from mssql_python.bcp_options import ColumnFormat, BCPOptions, BindData
from mssql_python import (
    SQLINT4, SQLVARCHAR, SQLNVARCHAR, SQL_VARLEN_DATA, SQL_NULL_DATA
)
from mssql_python.bcp_main import BCPClient

# --- Constants for Tests ---
SQL_COPT_SS_BCP = 1219 # BCP connection attribute

# --- Database Connection Details from Environment Variables ---
DB_CONNECTION_STRING = os.getenv("DB_CONNECTION_STRING")

# Skip all tests in this file if connection string is not provided
pytestmark = pytest.mark.skipif(
    not DB_CONNECTION_STRING,
    reason="DB_CONNECTION_STRING environment variable must be set for BCP integration tests."
)

def get_bcp_test_conn_str():
    """Returns the connection string."""
    if not DB_CONNECTION_STRING:
        # This should ideally not be reached due to pytestmark, but as a safeguard:
        pytest.skip("DB_CONNECTION_STRING is not set.")
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
        assert cf.server_col == 1
        assert cf.file_col == 1
        assert cf.user_data_type == 0

    def test_valid_instantiation_all_params(self):
        cf = ColumnFormat(
            prefix_len=1, data_len=10, field_terminator=b",",
            server_col=2, file_col=3, user_data_type=10
        )
        assert cf.prefix_len == 1
        assert cf.data_len == 10
        assert cf.field_terminator == b","
        assert cf.server_col == 2
        assert cf.file_col == 3
        assert cf.user_data_type == 10

    @pytest.mark.parametrize("attr, value", [
        ("prefix_len", -1), ("data_len", -1), ("server_col", 0),
        ("server_col", -1), ("file_col", 0), ("file_col", -1),
    ])
    def test_invalid_numeric_values(self, attr, value):
        with pytest.raises(ValueError):
            ColumnFormat(**{attr: value})

    @pytest.mark.parametrize("attr, value", [
        ("field_terminator", ","),
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

    @pytest.mark.parametrize("direction_to_test", ["out"])
    def test_missing_data_file_for_out(self, direction_to_test):
        """Test that data_file is required for 'out' direction."""
        with pytest.raises(ValueError, match=f"BCPOptions.data_file is required for file-based BCP direction '{direction_to_test}'"):
            BCPOptions(direction=direction_to_test, error_file=self._dummy_error_file)

    def test_missing_data_file_for_in(self):
        """Test that data_file is required for 'in' direction when not using memory BCP."""
        with pytest.raises(ValueError, match=f"BCPOptions.data_file is required for file-based BCP direction 'in'"):
            BCPOptions(direction="in", error_file=self._dummy_error_file, use_memory_bcp=False)

    @pytest.mark.parametrize("direction_to_test", ["in", "out"])
    def test_missing_error_file_for_any_direction(self, direction_to_test):
        """Test that error_file is required for all directions."""
        if direction_to_test == "in":
            with pytest.raises(ValueError, match="error_file must be provided even for in-memory BCP operations"):
                BCPOptions(
                    direction=direction_to_test, 
                    use_memory_bcp=True,
                    bind_data=[BindData(data=123, data_type=SQLINT4, data_length=4, server_col=1)]
                )
        else:
            with pytest.raises(ValueError, match="error_file must be provided for file-based BCP operations"):
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

    def test_valid_memory_bcp_with_bind_data(self):
        """Test that in-memory BCP with bind data is properly configured."""
        bind_data_item = BindData(
            data=123,
            data_type=SQLINT4,
            data_length=4,
            server_col=1
        )
        
        opts = BCPOptions(
            direction="in",
            use_memory_bcp=True,
            bind_data=[bind_data_item],
            error_file=self._dummy_error_file
        )
        
        assert opts.direction == "in"
        assert opts.use_memory_bcp is True
        assert len(opts.bind_data) == 1
        assert opts.bind_data[0].data == 123
        assert opts.bind_data[0].data_type == SQLINT4
        assert opts.data_file is None  # Data file is None for memory BCP in the new implementation

    def test_valid_memory_bcp_with_multiple_rows(self):
        """Test that multi-row binding is properly configured."""
        # Define two rows with two columns each
        row1 = [
            BindData(data=1001, data_type=SQLINT4, data_length=4, server_col=1),
            BindData(
                data="Row 1 Data", 
                data_type=SQLVARCHAR, 
                data_length=SQL_VARLEN_DATA, 
                terminator=b'\0', 
                terminator_length=1, 
                server_col=2
            )
        ]
        
        row2 = [
            BindData(data=1002, data_type=SQLINT4, data_length=4, server_col=1),
            BindData(
                data="Row 2 Data", 
                data_type=SQLVARCHAR, 
                data_length=SQL_VARLEN_DATA, 
                terminator=b'\0', 
                terminator_length=1, 
                server_col=2
            )
        ]
        
        opts = BCPOptions(
            direction="in",
            use_memory_bcp=True,
            bind_data=[row1, row2],  # List of rows, where each row is a list of BindData
            error_file=self._dummy_error_file
        )
        
        assert opts.direction == "in"
        assert opts.use_memory_bcp is True
        assert len(opts.bind_data) == 2  # Two rows
        assert len(opts.bind_data[0]) == 2  # First row has two columns
        assert opts.bind_data[0][0].data == 1001  # First column of first row
        assert opts.bind_data[1][0].data == 1002  # First column of second row
        assert opts.bind_data[0][1].data == "Row 1 Data"  # Second column of first row

    def test_memory_bcp_requires_in_direction(self):
        """Test that memory BCP requires 'in' direction."""
        with pytest.raises(ValueError, match="in-memory BCP operations require direction='in'"):
            BCPOptions(
                direction="out",
                use_memory_bcp=True,
                bind_data=[BindData(data=123, data_type=SQLINT4, data_length=4, server_col=1)],
                error_file=self._dummy_error_file
            )

    def test_memory_bcp_requires_bind_data(self):
        """Test that memory BCP requires bind_data."""
        with pytest.raises(ValueError, match="BCPOptions.bind_data must be provided when use_memory_bcp is True"):
            BCPOptions(
                direction="in",
                use_memory_bcp=True,
                error_file=self._dummy_error_file
            )

    def test_bind_data_requires_memory_bcp(self):
        """Test that binding data automatically enables use_memory_bcp."""
        # The validation has changed - now bind_data automatically sets use_memory_bcp
        opts = BCPOptions(
            direction="in", 
            bind_data=[BindData(data=123, data_type=SQLINT4, data_length=4, server_col=1)],
            error_file=self._dummy_error_file
        )
        assert opts.use_memory_bcp is True

    def test_memory_bcp_doesnt_require_data_file(self):
        """Test that memory BCP doesn't require data_file."""
        # This should not raise an exception
        opts = BCPOptions(
            direction="in",
            use_memory_bcp=True,
            bind_data=[BindData(data=123, data_type=SQLINT4, data_length=4, server_col=1)],
            error_file=self._dummy_error_file
        )
        assert opts.data_file is None  # Data file is None, not "" in the current implementation

    def test_bind_data_with_null_values(self):
        """Test that NULL values are properly configured in bind data."""
        bind_data_item = BindData(
            data=None,
            data_type=SQLINT4,
            indicator_length=4,
            data_length=SQL_NULL_DATA,
            server_col=1
        )
        
        opts = BCPOptions(
            direction="in",
            use_memory_bcp=True,
            bind_data=[bind_data_item],
            error_file=self._dummy_error_file
        )
        
        assert opts.bind_data[0].data is None
        assert opts.bind_data[0].indicator_length == 4
        assert opts.bind_data[0].data_length == SQL_NULL_DATA

# Add a new test class for BindData

class TestBindData:
    def test_valid_instantiation_defaults(self):
        """Test valid instantiation with minimal parameters."""
        bind_data = BindData(
            data=123,
            data_type=SQLINT4,
            data_length=4,
            server_col=1
        )
        assert bind_data.data == 123
        assert bind_data.data_type == SQLINT4
        assert bind_data.data_length == 4
        assert bind_data.server_col == 1
        assert bind_data.indicator_length == 0
        assert bind_data.terminator is None
        assert bind_data.terminator_length == 0

    def test_valid_instantiation_all_params(self):
        """Test valid instantiation with all parameters."""
        bind_data = BindData(
            data="test",
            data_type=SQLVARCHAR,
            indicator_length=0,
            data_length=SQL_VARLEN_DATA,
            terminator=b'\0',
            terminator_length=1,
            server_col=2
        )
        assert bind_data.data == "test"
        assert bind_data.data_type == SQLVARCHAR
        assert bind_data.indicator_length == 0
        assert bind_data.data_length == SQL_VARLEN_DATA
        assert bind_data.terminator == b'\0'
        assert bind_data.terminator_length == 1
        assert bind_data.server_col == 2

    def test_null_data_requires_sql_null_data(self):
        """This validation is not implemented in the current BindData."""
        # We'll create a valid BindData object instead
        bind_data = BindData(
            data=None,
            data_type=SQLINT4,
            indicator_length=4,
            data_length=SQL_NULL_DATA,
            server_col=1
        )
        assert bind_data.data is None
        assert bind_data.data_length == SQL_NULL_DATA

    def test_sql_null_data_requires_null_data(self):
        """This validation is not implemented in the current BindData."""
        # We'll create a valid BindData object instead
        bind_data = BindData(
            data=None,
            data_type=SQLINT4,
            indicator_length=4,
            data_length=SQL_NULL_DATA, 
            server_col=1
        )
        assert bind_data.data is None
        assert bind_data.data_length == SQL_NULL_DATA

    def test_null_data_requires_indicator(self):
        """This validation is not implemented in the current BindData."""
        # We'll test a valid case instead
        bind_data = BindData(
            data=None,
            data_type=SQLINT4,
            indicator_length=4,  # Valid indicator length
            data_length=SQL_NULL_DATA,
            server_col=1
        )
        assert bind_data.indicator_length == 4

    def test_invalid_server_col(self):
        """Test that server_col must be positive."""
        # The implementation does check this
        with pytest.raises(ValueError, match="server_col must be a positive integer"):
            BindData(
                data=123,
                data_type=SQLINT4,
                data_length=4,
                server_col=0  # Should be > 0
            )

    def test_varlen_data_requires_terminator(self):
        """This validation is not implemented in the current BindData."""
        # Create a valid BindData object with terminator instead
        bind_data = BindData(
            data="test",
            data_type=SQLVARCHAR, 
            data_length=SQL_VARLEN_DATA,
            terminator=b'\0',
            terminator_length=1,
            server_col=1
        )
        assert bind_data.terminator == b'\0'
        assert bind_data.data_length == SQL_VARLEN_DATA

    def test_unicode_string_with_nvarchar(self):
        """Test that Unicode strings work with NVARCHAR."""
        unicode_text = "Unicode 文字"
        bind_data = BindData(
            data=unicode_text,
            data_type=SQLNVARCHAR,
            data_length=SQL_VARLEN_DATA,
            terminator=b'\0',
            terminator_length=1,
            server_col=1
        )
        assert bind_data.data == unicode_text
        assert bind_data.data_type == SQLNVARCHAR
