import pytest
import os
import uuid
import tempfile
import time
from pathlib import Path

from mssql_python import connect as mssql_connect
from mssql_python.bcp_options import BCPOptions, ColumnFormat
from mssql_python.bcp_main import BCPClient

# --- Constants for Tests ---
SQL_COPT_SS_BCP = 1219  # BCP connection attribute


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
        pytest.skip("DB_CONNECTION_STRING is not set.")
    return DB_CONNECTION_STRING

@pytest.fixture(scope="function")
def format_test_setup():
    """
    Fixture to set up a BCP-enabled connection and a test table with sample data.
    Creates format files for testing.
    Cleans up all resources afterward.
    """
    conn_str = get_bcp_test_conn_str()
    table_uuid = str(uuid.uuid4()).replace('-', '')[:8]
    table_name = f"dbo.pytest_bcp_format_{table_uuid}"
    
    conn = None
    cursor = None
    temp_dir = tempfile.TemporaryDirectory()
    
    # Define paths for our test files
    format_file_path = Path(temp_dir.name) / "test_format.fmt"
    data_out_path = Path(temp_dir.name) / "data_out.bcp"
    data_in_path = Path(temp_dir.name) / "data_in.bcp"
    error_out_path = Path(temp_dir.name) / "error_out.log"
    error_in_path = Path(temp_dir.name) / "error_in.log"
    
    try:
        # Connect with BCP enabled
        conn = mssql_connect(conn_str, attrs_before={SQL_COPT_SS_BCP: 1}, autocommit=True)
        cursor = conn.cursor()
        
        # Create test table
        cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name};")
        cursor.execute(f"""
            CREATE TABLE {table_name} (
                id INT PRIMARY KEY,
                name NVARCHAR(50) NOT NULL,
                value DECIMAL(10,2) NULL
            );
        """)
        
        # Insert sample data
        cursor.execute(f"""
            INSERT INTO {table_name} (id, name, value)
            VALUES 
                (1, 'Item One', 10.50),
                (2, 'Item Two', 20.75),
                (3, 'Item Three', 30.00),
                (4, 'Item Four', NULL);
        """)
        
        # Create a format file - use a simpler format structure
        # For native format (which is safer for BCP transfers)
        format_content = """14.0
3
1       SQLINT      0       4       ""   1     id               ""
2       SQLNCHAR    0       50      ""   2     name             Latin1_General_CI_AS
3       SQLDECIMAL  0       17      ""   3     value            ""
"""
        
        with open(format_file_path, 'w', encoding='utf-8') as f:
            f.write(format_content)
            
        # Yield resources to test
        yield {
            'conn': conn,
            'table_name': table_name,
            'format_file': str(format_file_path),  # Convert to string to avoid Path object issues
            'data_out': str(data_out_path),
            'data_in': str(data_in_path),
            'error_out': str(error_out_path),
            'error_in': str(error_in_path),
            'temp_dir': temp_dir  # Keep reference to prevent cleanup until fixture is done
        }
        
    finally:
        # Cleanup
        if cursor:
            try:
                cursor.close()
            except Exception as e:
                print(f"Warning: Error closing cursor: {e}")
                
        if conn:
            try:
                cleanup_cursor = conn.cursor()
                cleanup_cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name};")
                cleanup_cursor.close()
            except Exception as e:
                print(f"Warning: Error during cleanup: {e}")
            finally:
                conn.close()


class TestBCPFormatFile:
    def test_bcp_out_with_format_file(self, format_test_setup):
        """Test BCP OUT operation using a format file."""
        # Get resources from setup
        conn = format_test_setup['conn']
        table_name = format_test_setup['table_name']
        format_file = format_test_setup['format_file']
        data_file = format_test_setup['data_out']
        error_file = format_test_setup['error_out']
        
        # Create BCPClient
        bcp_client = BCPClient(conn)
        
        # Create options for BCP OUT
        options = BCPOptions(
            direction="out",
            data_file=data_file,
            error_file=error_file,
            format_file=format_file,
            bulk_mode="native"  # Use native mode for format file
        )
        
        # Execute BCP OUT
        bcp_client.sql_bulk_copy(table=table_name, options=options)
        
        # Verify the operation succeeded
        assert os.path.exists(data_file), "Output data file was not created"
        assert os.path.getsize(data_file) > 0, "Output data file is empty"
        
        # Count rows in table to verify against BCP operation
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]
        
        # Additional verification could be done here
        assert row_count == 4, f"Expected 4 rows in source table, got {row_count}"
    
    # Separate test for BCP IN to reduce complexity and potential issues
    def test_bcp_in_with_format_file(self, format_test_setup):
        """Test BCP IN operation using a format file."""
        # Get resources from setup
        conn = format_test_setup['conn']
        table_name = format_test_setup['table_name']
        format_file = format_test_setup['format_file']
        
        # Create test data file with known values
        # Instead of doing BCP OUT first (which could fail), create a simple data file
        # that matches the expected format for BCP IN
        data_in_file = format_test_setup['data_in']
        error_in_file = format_test_setup['error_in']
        
        # First, create a copy table for import
        new_table = f"{table_name}_copy"
        cursor = conn.cursor()
        cursor.execute(f"IF OBJECT_ID('{new_table}', 'U') IS NOT NULL DROP TABLE {new_table};")
        cursor.execute(f"SELECT TOP 0 * INTO {new_table} FROM {table_name}")
        
        try:
            # Create BCPClient
            bcp_client = BCPClient(conn)
            
            # Get row count before import
            cursor.execute(f"SELECT COUNT(*) FROM {new_table}")
            before_count = cursor.fetchone()[0]
            assert before_count == 0, "Target table should be empty before import"
            
            # First do a BCP OUT to create sample data
            test_out_file = format_test_setup['data_out']
            test_err_file = format_test_setup['error_out']
            
            out_options = BCPOptions(
                direction="out",
                data_file=test_out_file,
                error_file=test_err_file,
                format_file=format_file,
                bulk_mode="native"
            )
            
            # Do BCP OUT first to create input data
            bcp_client.sql_bulk_copy(table=table_name, options=out_options)
            
            # Ensure the file was created
            assert os.path.exists(test_out_file), "BCP OUT file not created"
            assert os.path.getsize(test_out_file) > 0, "BCP OUT file is empty"
            
            # Copy the data file to the input location
            with open(test_out_file, 'rb') as src, open(data_in_file, 'wb') as dst:
                dst.write(src.read())
            
            # Create a new BCP client to avoid any state issues
            bcp_client_in = BCPClient(conn)
            
            # Now do BCP IN
            in_options = BCPOptions(
                direction="in",
                data_file=data_in_file,
                error_file=error_in_file,
                format_file=format_file,
                bulk_mode="native"
            )
            
            # Execute BCP IN
            bcp_client_in.sql_bulk_copy(table=new_table, options=in_options)
            
            # Force a small delay to ensure all operations complete
            time.sleep(0.5)
            
            # Verify data was imported
            cursor.execute(f"SELECT COUNT(*) FROM {new_table}")
            after_count = cursor.fetchone()[0]
            
            # Get expected count
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            expected_count = cursor.fetchone()[0]
            
            assert after_count > 0, "No rows were imported"
            assert after_count == expected_count, f"Expected {expected_count} rows, got {after_count}"
            
        finally:
            # Clean up
            cursor.execute(f"IF OBJECT_ID('{new_table}', 'U') IS NOT NULL DROP TABLE {new_table};")

class TestBCPColumnFmt:
    def test_bcp_colfmt(self, format_test_setup):
        """Test BCP operations with explicit column formatting instead of format files."""
        # Get resources from setup
        conn = format_test_setup['conn']
        table_name = format_test_setup['table_name']
        data_out = format_test_setup['data_out']
        data_in = format_test_setup['data_in']
        error_out = format_test_setup['error_out']
        error_in = format_test_setup['error_in']
        
        # Create a new table for column format testing
        col_fmt_table = f"{table_name}_colfmt"
        cursor = conn.cursor()
        cursor.execute(f"IF OBJECT_ID('{col_fmt_table}', 'U') IS NOT NULL DROP TABLE {col_fmt_table};")
        cursor.execute(f"""
            CREATE TABLE {col_fmt_table} (
                id INT PRIMARY KEY,
                name NVARCHAR(50) NOT NULL,
                value DECIMAL(10,2) NULL
            );
        """)
        
        try:
            # Create BCPClient for export
            bcp_client_out = BCPClient(conn)
            
            # Create options for BCP OUT with column formatting
            out_options = BCPOptions(
                direction="out",
                data_file=data_out,
                error_file=error_out,
                bulk_mode="char"  # Use character mode for column formatting
            )
            
            # Set up column formats for OUT operation by directly creating ColumnFormat objects
            # and adding them to the columns list
            col1 = ColumnFormat(
                prefix_len=0,
                data_len=8,
                field_terminator=b"\r\t",
                terminator_len=2,  # Length of the terminator
                server_col=1,
                file_col=1,
                user_data_type= 47  # SQLINT
            )
            
            col2 = ColumnFormat(
                prefix_len=0,
                data_len=50,
                field_terminator=b"\r\t", 
                terminator_len=2,  # Length of the terminator
                server_col=2,
                file_col=2,
                user_data_type= 47  # SQLNCHAR would be different, using placeholder
            )
            
            col3 = ColumnFormat(
                prefix_len=0,
                data_len=12,
                field_terminator=b"\r\n",
                terminator_len=2,  # Length of the terminator
                server_col=3, 
                file_col=3,
                user_data_type= 47  # SQLDECIMAL would be different, using placeholder
            )
            
            # Add columns to the options
            out_options.columns = [col1, col2, col3]
            
            # Execute BCP OUT using column formatting
            bcp_client_out.sql_bulk_copy(table=table_name, options=out_options)
            
            # Verify the operation succeeded
            assert os.path.exists(data_out), "Output data file was not created"
            assert os.path.getsize(data_out) > 0, "Output data file is empty"
            
            # Create BCPClient for import
            bcp_client_in = BCPClient(conn)
            
            # Create options for BCP IN with column formatting
            in_options = BCPOptions(
                direction="in",
                data_file=data_out,  # Use the data we just exported
                error_file=error_in,
                bulk_mode="char"  # Use character mode for column formatting
            )
            
            # Set up column formats for IN operation - must match OUT operation
            in_options.columns = [
                ColumnFormat(
                    prefix_len=0,
                    data_len=8,
                    field_terminator=b"\r\t",
                    terminator_len=2,  # Length of the terminator
                    server_col=1,
                    file_col=1,
                    user_data_type= 47  # SQLINT
                ),
                ColumnFormat(
                    prefix_len=0,
                    data_len=50,
                    field_terminator=b"\r\t",
                    terminator_len=2,  # Length of the terminator
                    server_col=2,
                    file_col=2,
                    user_data_type= 47 # SQLNCHAR placeholder
                ),
                ColumnFormat(
                    prefix_len=0,
                    data_len=12,
                    field_terminator=b"\r\n",
                    terminator_len=2,  # Length of the terminator
                    server_col=3,
                    file_col=3,
                    user_data_type= 47  # SQLDECIMAL placeholder
                )
            ]
            
            # Execute BCP IN using column formatting
            bcp_client_in.sql_bulk_copy(table=col_fmt_table, options=in_options)
            
            # Verify data was imported correctly
            cursor.execute(f"SELECT COUNT(*) FROM {col_fmt_table}")
            imported_count = cursor.fetchone()[0]
            
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            original_count = cursor.fetchone()[0]
            
            assert imported_count == original_count, f"Expected {original_count} rows, got {imported_count}"
            
            # Compare actual data to ensure it was imported correctly
            cursor.execute(f"SELECT id, name, value FROM {table_name} ORDER BY id")
            original_rows = cursor.fetchall()
            
            cursor.execute(f"SELECT id, name, value FROM {col_fmt_table} ORDER BY id")
            imported_rows = cursor.fetchall()
            
            assert len(original_rows) == len(imported_rows), "Row count mismatch in detailed comparison"
            
            for orig, imp in zip(original_rows, imported_rows):
                assert orig == imp, f"Data mismatch: original={orig}, imported={imp}"
            
        finally:
            # Clean up
            cursor.execute(f"IF OBJECT_ID('{col_fmt_table}', 'U') IS NOT NULL DROP TABLE {col_fmt_table};")
            cursor.close()

    def test_bcp_colfmt_with_row_terminator(self, format_test_setup):
        """Test BCP operations with column formatting and explicit row terminator."""
        # Get resources from setup
        conn = format_test_setup['conn']
        table_name = format_test_setup['table_name']
        data_out = format_test_setup['data_out'] + ".row"
        data_in = format_test_setup['data_in'] + ".row"
        error_out = format_test_setup['error_out']
        error_in = format_test_setup['error_in']
        
        # Create a new table for column format testing
        row_term_table = f"{table_name}_rowterm"
        cursor = conn.cursor()
        cursor.execute(f"IF OBJECT_ID('{row_term_table}', 'U') IS NOT NULL DROP TABLE {row_term_table};")
        cursor.execute(f"SELECT * INTO {row_term_table} FROM {table_name} WHERE 1=0")
        
        try:
            # Create BCPClient for export
            bcp_client_out = BCPClient(conn)
            
            # Create options for BCP OUT with column formatting
            # Note: we'll specify row terminator in the last column's row_terminator field
            out_options = BCPOptions(
                direction="out",
                data_file=data_out,
                error_file=error_out,
                bulk_mode="char"  # Use character mode for column formatting
            )
            
            # Set up column formats for OUT operation with row terminator in the last column
            out_options.columns = [
                ColumnFormat(
                    prefix_len=0,
                    data_len=8,
                    field_terminator=b",",
                    terminator_len=1,  # Length of the terminator
                    server_col=1,
                    file_col=1,
                    user_data_type=47  # SQLINT
                ),
                ColumnFormat(
                    prefix_len=0,
                    data_len=50,
                    field_terminator=b",",
                    terminator_len=1,  # Length of the terminator
                    server_col=2,
                    file_col=2,
                    user_data_type=47  # SQLNCHAR placeholder
                ),
                ColumnFormat(
                    prefix_len=0,
                    data_len=12,
                    field_terminator=b"\r\n",  # Use row terminator on last column
                    terminator_len=2,  # Length of the terminator
                    server_col=3,
                    file_col=3,
                    user_data_type=47 # SQLDECIMAL placeholder
                )
            ]
            
            # Execute BCP OUT using column formatting
            bcp_client_out.sql_bulk_copy(table=table_name, options=out_options)
            
            # Verify the operation succeeded
            assert os.path.exists(data_out), "Output data file was not created"
            assert os.path.getsize(data_out) > 0, "Output data file is empty"
            
            # Create BCPClient for import
            bcp_client_in = BCPClient(conn)
            
            # Create options for BCP IN with column formatting
            in_options = BCPOptions(
                direction="in",
                data_file=data_out,  # Use the data we just exported
                error_file=error_in,
                bulk_mode="char"  # Use character mode for column formatting
            )
            
            # Set up column formats for IN operation - must match OUT operation
            in_options.columns = [
                ColumnFormat(
                    prefix_len=0,
                    data_len=8,
                    field_terminator=b",",
                    terminator_len=1,  # Length of the terminator
                    server_col=1,
                    file_col=1,
                    user_data_type=47 # SQLINT
                ),
                ColumnFormat(
                    prefix_len=0,
                    data_len=50,
                    field_terminator=b",",
                    terminator_len=1,  # Length of the terminator
                    server_col=2,
                    file_col=2,
                    user_data_type=47  # SQLNCHAR placeholder
                ),
                ColumnFormat(
                    prefix_len=0,
                    data_len=12,
                    field_terminator=b"\r\n",  # Use row terminator on last column
                    terminator_len=2,  # Length of the terminator
                    server_col=3,
                    file_col=3,
                    user_data_type=47  # SQLDECIMAL placeholder
                )
            ]
            
            # Execute BCP IN using column formatting
            bcp_client_in.sql_bulk_copy(table=row_term_table, options=in_options)
            
            # Verify data was imported correctly
            cursor.execute(f"SELECT COUNT(*) FROM {row_term_table}")
            imported_count = cursor.fetchone()[0]
            
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            original_count = cursor.fetchone()[0]
            
            assert imported_count == original_count, f"Expected {original_count} rows, got {imported_count}"
            
        finally:
            # Clean up
            cursor.execute(f"IF OBJECT_ID('{row_term_table}', 'U') IS NOT NULL DROP TABLE {row_term_table};")
            cursor.close()

class TestBCPQueryOut:
    def test_bcp_query_out_using_formatfile(self, format_test_setup):
        """Test BCP OUT operation using a query and a format file."""
        # Get resources from setup
        conn = format_test_setup['conn']
        table_name = format_test_setup['table_name']
        format_file = format_test_setup['format_file']
        data_file = format_test_setup['data_out']
        error_file = format_test_setup['error_out']
        
        # Create BCPClient
        bcp_client = BCPClient(conn)
        
        # Create options for BCP OUT with query
        options = BCPOptions(
            direction="queryout",
            data_file=data_file,
            error_file=error_file,
            format_file=format_file,
            query=f"SELECT * FROM {table_name}"  # Use a query instead of a table name
        )
        
        # Ensure the data file is clean before running BCP OUT
        if os.path.exists(data_file):
            os.remove(data_file)
        
        # Check if table exists and has data
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        if count == 0:
            pytest.skip(f"No data in table {table_name} to export with BCP OUT.")
        # Log the number of rows to export
        print(f"Rows to export from {table_name}: {count}")
        
        # Execute BCP OUT
        bcp_client.sql_bulk_copy(options=options)
        
        # Verify the operation succeeded
        assert os.path.exists(data_file), "Output data file was not created"
        assert os.path.getsize(data_file) > 0, "Output data file is empty"
        
        # Count rows in table to verify against BCP operation
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]
        
        # Additional verification could be done here
        assert row_count == 4, f"Expected 4 rows in source table, got {row_count}"

    def test_bcp_query_out_with_columns(self, format_test_setup):
        """Test BCP OUT operation using a query and explicit column formatting."""
        # Get resources from setup
        conn = format_test_setup['conn']
        table_name = format_test_setup['table_name']
        data_file = format_test_setup['data_out']
        error_file = format_test_setup['error_out']
        
        # Create BCPClient
        bcp_client = BCPClient(conn)
        
        # Create options for BCP OUT with query and columns
        options = BCPOptions(
            direction="queryout",
            data_file=data_file,
            error_file=error_file,
            query=f"SELECT id, name FROM {table_name}",  # Select specific columns
            columns=[
                ColumnFormat(
                    prefix_len=0,
                    data_len=8,
                    field_terminator=b",",
                    terminator_len=1,  # Length of the terminator
                    server_col=1,
                    file_col=1,
                    user_data_type=47  # SQLINT
                ),
                ColumnFormat(
                    prefix_len=0,
                    data_len=50,
                    field_terminator=b"\r\n",  # Use row terminator on last column
                    terminator_len=2,  # Length of the terminator
                    server_col=2,
                    file_col=2,
                    user_data_type=47  # SQLNCHAR placeholder
                )
            ]
        )
        
        # Ensure the data file is clean before running BCP OUT
        if os.path.exists(data_file):
            os.remove(data_file)
        
        # Check if table exists and has data
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        if count == 0:
            pytest.skip(f"No data in table {table_name} to export with BCP OUT.")
        
        # Log the number of rows to export
        print(f"Rows to export from {table_name}: {count}")
        
        # Execute BCP OUT
        bcp_client.sql_bulk_copy(options=options)
        
        # Verify the operation succeeded
        assert os.path.exists(data_file), "Output data file was not created"
        assert os.path.getsize(data_file) > 0, "Output data file is empty"
        
        # Count rows in table to verify against BCP operation
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]

        # Additional verification could be done here
        assert row_count == 4, f"Expected 4 rows in source table, got {row_count}"