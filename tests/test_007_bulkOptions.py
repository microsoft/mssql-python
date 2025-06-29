"""Unit tests for mssql_python.bcp_options and BCPOptions/ColumnFormat."""

import os
import uuid
import pytest

# Assuming your project structure allows these imports
from mssql_python import connect as mssql_connect  # Alias to avoid conflict
from mssql_python.bcp_options import ColumnFormat, BCPOptions

# --- Constants for Tests ---
SQL_COPT_SS_BCP = 1219  # BCP connection attribute

# --- Database Connection Details from Environment Variables ---
DB_CONNECTION_STRING = os.getenv("DB_CONNECTION_STRING")

# Skip all tests in this file if connection string is not provided
pytestmark = pytest.mark.skipif(
    not DB_CONNECTION_STRING,
    reason="DB_CONNECTION_STRING environment variable must be set for BCP integration tests.",
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
    table_name_uuid_part = str(uuid.uuid4()).replace("-", "")[:8]
    table_name = f"dbo.pytest_bcp_table_{table_name_uuid_part}"

    conn = None
    cursor = None
    try:
        conn = mssql_connect(
            conn_str, attrs_before={SQL_COPT_SS_BCP: 1}, autocommit=True
        )
        cursor = conn.cursor()

        cursor.execute(
            f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name};"
        )
        cursor.execute(
            f"""
            CREATE TABLE {table_name} (
                id INT PRIMARY KEY,
                data_col VARCHAR(255) NULL
            );
        """
        )
        yield conn, table_name
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception as exc:
                print(
                    f"Warning: Error closing cursor during BCP test setup/teardown: {exc}"
                )
        if conn:
            cursor_cleanup = None
            try:
                cursor_cleanup = conn.cursor()
                cursor_cleanup.execute(
                    f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name};"
                )
            except Exception as exc:
                print(
                    f"Warning: Error during BCP test cleanup (dropping table {table_name}): {exc}"
                )
            finally:
                if cursor_cleanup:
                    try:
                        cursor_cleanup.close()
                    except Exception as exc:
                        print(f"Warning: Error closing cleanup cursor: {exc}")
                conn.close()


@pytest.fixture
def temp_file_pair(tmp_path):
    """Provides a pair of temporary file paths for data and errors using pytest's tmp_path."""
    file_uuid_part = str(uuid.uuid4()).replace("-", "")[:8]
    data_file = tmp_path / f"bcp_data_{file_uuid_part}.csv"
    error_file = tmp_path / f"bcp_error_{file_uuid_part}.txt"
    return data_file, error_file


class TestColumnFormat:
    """Unit tests for the ColumnFormat class."""

    def test_valid_instantiation_defaults(self):
        """Test default instantiation of ColumnFormat."""
        cf = ColumnFormat()
        assert cf.prefix_len == 0
        assert cf.data_len == 0
        assert cf.field_terminator is None
        assert cf.server_col == 1
        assert cf.file_col == 1
        assert cf.user_data_type == 0

    def test_valid_instantiation_all_params(self):
        """Test instantiation of ColumnFormat with all parameters."""
        cf = ColumnFormat(
            prefix_len=1,
            data_len=10,
            field_terminator=b",",
            server_col=2,
            file_col=3,
            user_data_type=10,
        )
        assert cf.prefix_len == 1
        assert cf.data_len == 10
        assert cf.field_terminator == b","
        assert cf.server_col == 2
        assert cf.file_col == 3
        assert cf.user_data_type == 10

    @pytest.mark.parametrize(
        "attr, value",
        [
            ("prefix_len", -1),
            ("data_len", -1),
            ("server_col", 0),
            ("server_col", -1),
            ("file_col", 0),
            ("file_col", -1),
        ],
    )
    def test_invalid_numeric_values(self, attr, value):
        """Test invalid numeric values for ColumnFormat."""
        with pytest.raises(ValueError):
            ColumnFormat(**{attr: value})

    @pytest.mark.parametrize(
        "attr, value",
        [
            ("field_terminator", ","),
        ],
    )
    def test_invalid_terminator_types(self, attr, value):
        """Test invalid field_terminator types for ColumnFormat."""
        with pytest.raises(TypeError):
            ColumnFormat(**{attr: value})


class TestBCPOptions:
    """Unit tests for the BCPOptions class."""

    _dummy_data_file = "dummy_data.csv"
    _dummy_error_file = "dummy_error.txt"

    def test_valid_instantiation_in_minimal(self):
        """Test minimal valid instantiation for BCPOptions (direction in)."""
        opts = BCPOptions(
            direction="in",
            data_file=self._dummy_data_file,
            error_file=self._dummy_error_file,
        )
        assert opts.direction == "in"
        assert opts.data_file == self._dummy_data_file
        assert opts.error_file == self._dummy_error_file
        assert opts.bulk_mode == "native"

    def test_valid_instantiation_out_full(self):
        """Test full valid instantiation for BCPOptions (direction out)."""
        cols = [ColumnFormat(file_col=1, server_col=1, field_terminator=b"\t")]
        opts = BCPOptions(
            direction="out",
            data_file="output.csv",
            error_file="errors.log",
            bulk_mode="char",
            batch_size=1000,
            max_errors=10,
            first_row=1,
            last_row=100,
            code_page="ACP",
            hints="ORDER(id)",
            columns=cols,
            keep_identity=True,
            keep_nulls=True,
        )
        assert opts.direction == "out"
        assert opts.bulk_mode == "char"
        assert opts.columns == cols
        assert opts.keep_identity is True

    def test_invalid_direction(self):
        """Test invalid direction for BCPOptions."""
        with pytest.raises(
            ValueError, match="BCPOptions.direction 'invalid_dir' is invalid"
        ):
            BCPOptions(
                direction="invalid_dir",
                data_file=self._dummy_data_file,
                error_file=self._dummy_error_file,
            )

    @pytest.mark.parametrize("direction_to_test", ["in", "out"])
    def test_missing_data_file_for_in_out(self, direction_to_test):
        """Test missing data_file for in/out directions in BCPOptions."""
        with pytest.raises(
            ValueError,
            match=f"BCPOptions.data_file is required for BCP direction '{direction_to_test}'.",
        ):
            BCPOptions(direction=direction_to_test, error_file=self._dummy_error_file)

    @pytest.mark.parametrize("direction_to_test", ["in", "out"])
    def test_missing_error_file_for_in_out(self, direction_to_test):
        """Test missing error_file for in/out directions in BCPOptions."""
        with pytest.raises(
            ValueError,
            match="error_file must be provided and non-empty for 'in' or 'out' directions.",
        ):
            BCPOptions(direction=direction_to_test, data_file=self._dummy_data_file)

    def test_columns_and_format_file_conflict(self):
        """Test conflict between columns and format_file in BCPOptions."""
        with pytest.raises(
            ValueError, match="Cannot specify both 'columns' .* and 'format_file'"
        ):
            BCPOptions(
                direction="in",
                data_file=self._dummy_data_file,
                error_file=self._dummy_error_file,
                columns=[ColumnFormat()],
                format_file="format.fmt",
            )

    def test_invalid_bulk_mode(self):
        """Test invalid bulk_mode for BCPOptions."""
        with pytest.raises(
            ValueError, match="BCPOptions.bulk_mode 'invalid_mode' is invalid"
        ):
            BCPOptions(
                direction="in",
                data_file=self._dummy_data_file,
                error_file=self._dummy_error_file,
                bulk_mode="invalid_mode",
            )

    @pytest.mark.parametrize(
        "attr, value",
        [
            ("batch_size", -1),
            ("max_errors", -1),
            ("first_row", -1),
            ("last_row", -1),
        ],
    )
    def test_negative_control_values(self, attr, value):
        """Test negative control values for BCPOptions."""
        with pytest.raises(ValueError, match=f"BCPOptions.{attr} must be non-negative"):
            BCPOptions(
                direction="in",
                data_file=self._dummy_data_file,
                error_file=self._dummy_error_file,
                **{attr: value},
            )

    def test_first_row_greater_than_last_row(self):
        """Test first_row greater than last_row in BCPOptions."""
        with pytest.raises(
            ValueError,
            match="BCPOptions.first_row cannot be greater than BCPOptions.last_row",
        ):
            BCPOptions(
                direction="in",
                data_file=self._dummy_data_file,
                error_file=self._dummy_error_file,
                first_row=10,
                last_row=5,
            )

    def test_invalid_codepage_negative_int(self):
        """Test negative integer code_page for BCPOptions."""
        with pytest.raises(
            ValueError,
            match="BCPOptions.code_page, if an integer, must be non-negative",
        ):
            BCPOptions(
                direction="in",
                data_file=self._dummy_data_file,
                error_file=self._dummy_error_file,
                code_page=-1,
            )
