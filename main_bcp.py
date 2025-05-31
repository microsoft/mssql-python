import os
import sys
import uuid
from pathlib import Path
import logging
from mssql_python import connect, setup_logging # Assuming these are available
from mssql_python.bcp_main import BCPClient      # Assuming these are available
from mssql_python.bcp_options import BCPOptions, ColumnFormat # Assuming these are available
from mssql_python.exceptions import Error as MssqlError     # Assuming this is available

SQL_COPT_SS_BCP = 1214

# Configure logging for this script
script_logger = logging.getLogger("main_bcp_script") # Logger for this script's messages
# The main setup will be done in main() to allow dynamic level setting

def create_minimal_data_file(file_path: Path, data: list):
    """Creates a simple CSV file."""
    script_logger.info(f"Creating data file: {file_path}")
    with open(file_path, 'w', newline='', encoding='utf-8') as f:
        for row in data:
            f.write(",".join(map(str, row)) + "\n")
    script_logger.info(f"Data file created: {file_path}")

def main():
    # Initial logging setup
    try:
        # Adjust if your setup_logging has a different signature or doesn't exist
        if callable(setup_logging):
            setup_logging(mode="stdout", log_level=logging.DEBUG)
            script_logger.info("Library's setup_logging(mode='stdout', log_level=logging.DEBUG) called.")
        else:
            raise TypeError("setup_logging not callable as expected")
    except (TypeError, NameError) as te: # Catch NameError if setup_logging isn't imported
        script_logger.warning(f"Problem with library's setup_logging: {te}. "
                            f"Falling back to basicConfig for root logger.")
        logging.basicConfig(level=logging.DEBUG, stream=sys.stdout,
                            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s (%(filename)s:%(lineno)d)')
        script_logger.info("Fallback basicConfig(level=DEBUG) applied to root logger.")

    # Ensure root logger is also configured for console output at DEBUG if not already
    root_logger = logging.getLogger()
    if root_logger.getEffectiveLevel() > logging.DEBUG: # Ensure root is at least DEBUG
        root_logger.setLevel(logging.DEBUG)
        script_logger.info(f"Root logger level set to DEBUG.")

    has_root_stdout_handler = any(
        isinstance(h, logging.StreamHandler) and h.stream == sys.stdout
        for h in root_logger.handlers
    )
    if not root_logger.handlers or not has_root_stdout_handler : # Add handler if none or no stdout
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s (%(filename)s:%(lineno)d)')
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)
        script_logger.info("Added StreamHandler to root logger for console output at DEBUG level.")
    else: # Ensure existing stdout handlers on root are also at DEBUG
        for h in root_logger.handlers:
            if isinstance(h, logging.StreamHandler) and h.stream == sys.stdout and h.level > logging.DEBUG:
                h.setLevel(logging.DEBUG)
                script_logger.info(f"Set existing root stdout handler '{h.name if h.name else h}' level to DEBUG.")


    
    conn_str = os.getenv("DB_CONNECTION_STRING", None)
    script_logger.warning(
        f"DB_CONNECTION_STRING environment variable is set to: {conn_str if conn_str else 'None'}"
    )
    
    if not conn_str: 
        script_logger.error("Connection string is not available. Exiting.") 
        return

    table_name = f"dbo.bcp_script_test_{str(uuid.uuid4()).replace('-', '')[:8]}"
    data_file_path = Path(f"./{table_name.split('.')[-1]}_data.csv")
    # Ensure C:\Temp directory exists and is writable by the user running the script
    error_file_dir = Path("C:/Temp")
    if not error_file_dir.exists():
        try:
            error_file_dir.mkdir(parents=True, exist_ok=True)
            script_logger.info(f"Created directory for error file: {error_file_dir}")
        except Exception as e_mkdir:
            script_logger.error(f"Could not create directory {error_file_dir} for BCP error file: {e_mkdir}. Using current directory for errors.")
            error_file_path = Path(f"./{table_name.split('.')[-1]}_errors.txt")
    else:
        error_file_path = error_file_dir / f"{table_name.split('.')[-1]}_errors.txt"
    
    script_logger.info(f"BCP Error file will be: {error_file_path}")


    conn = None
    cursor = None 
    try:
        script_logger.info("Connecting to database...") 
        conn = connect(conn_str, attrs_before={SQL_COPT_SS_BCP: 1}, autocommit=True) 
        script_logger.info("Connected.") 

        cursor = conn.cursor()

        # 1. Create a simple test table
        script_logger.info(f"Creating table: {table_name}") 
        cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
        cursor.execute(f"""
            CREATE TABLE {table_name} (
                id INT PRIMARY KEY,
                name VARCHAR(50)
            )
        """)
        script_logger.info(f"Table {table_name} created.") 

        # 2. Prepare sample data
        sample_data = [
            [1, "Alice"],
            [2, "Bob"],
            [3, "Charlie"],
            [4, "David"], # Added one more for variety
            [5, "Eve"]
        ]
        create_minimal_data_file(data_file_path, sample_data)

        # 3. Perform BCP IN
        script_logger.info(f"Starting BCP IN for {table_name} from {data_file_path}") 
        
        bcp_client = BCPClient(connection=conn)
        
        bcp_options = BCPOptions(
            direction='in',
            data_file=str(data_file_path),
            error_file=str(error_file_path),
            bulk_mode="char", 
            columns=[
                ColumnFormat(
                    file_col=1,
                    server_col=1,
                    field_terminator=b",", 
                    user_data_type=0 
                ),
                ColumnFormat(
                    file_col=2,
                    server_col=2,
                    row_terminator=b"\n", 
                    user_data_type=0 
                )
            ]
        )
        
        # Execute BCP
        bcp_client.sql_bulk_copy(table=table_name, options=bcp_options)
        script_logger.info("BCP IN API call completed.") 

        # Check error file
        if error_file_path.exists() and error_file_path.stat().st_size > 0:
            script_logger.warning(f"BCP IN errors reported in: {error_file_path}") 
            with open(error_file_path, "r", encoding='utf-8', errors='replace') as err_f:
                script_logger.error(f"--- BCP Errors from {error_file_path} ---\n{err_f.read()}\n--------------------") 
        else:
            script_logger.info(f"BCP error file '{error_file_path}' is empty or was not created (which can be normal if no errors).")

        # 4. Verify data
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        script_logger.info(f"Table '{table_name}' now contains {count} rows.") 
        
        if count == len(sample_data):
            script_logger.info("BCP data verification successful: Row count matches.")
        else:
            script_logger.error(f"Row count mismatch: Expected {len(sample_data)}, Got {count}. BCP operation likely had issues.")
            # The assert is commented out to allow the script to finish and show logs/error files
            # assert count == len(sample_data), f"Row count mismatch after BCP IN: Expected {len(sample_data)}, Got {count}"

    except MssqlError as e_lib:
        script_logger.error(f"A mssql_python library error occurred: {e_lib}", exc_info=True) 
    except Exception as e_main:
        script_logger.error(f"An unexpected error occurred in main execution: {e_main}", exc_info=True) 
    finally:
        if cursor: 
            try:
                cursor.close()
                script_logger.info("Main cursor closed.")
            except Exception as e_cursor_close:
                script_logger.error(f"Error closing main cursor: {e_cursor_close}", exc_info=True)
        
        if conn:
            # Cleanup: Attempt to drop the test table
            # You might want to keep the table for inspection if BCP fails
            # For now, it's not dropped to allow inspection as per previous interactions.
            cursor_cleanup = None
            try:
                cursor_cleanup = conn.cursor()
                script_logger.info(f"Cleanup: Table '{table_name}' will NOT be dropped for inspection.") 
                # script_logger.info(f"Attempting to drop table: {table_name}")
                # cursor_cleanup.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
                # script_logger.info(f"Table {table_name} dropped (if it existed).")
            except Exception as e_cleanup:
                script_logger.error(f"Error during table cleanup (drop attempt): {e_cleanup}", exc_info=True) 
            finally:
                if cursor_cleanup:
                    try:
                        cursor_cleanup.close()
                        script_logger.info("Cleanup cursor closed.")
                    except Exception as e_cur_clean_close:
                        script_logger.error(f"Error closing cleanup cursor: {e_cur_clean_close}", exc_info=True)
            
            script_logger.info("Closing database connection.") 
            conn.close()

        # Keep both data and error files for inspection (as per previous requests)
        if data_file_path.exists():
            script_logger.info(f"Data file kept for inspection: {data_file_path}")
        else:
            script_logger.info(f"Data file was not created or already removed: {data_file_path}")
        
        if error_file_path.exists():
            script_logger.info(f"BCP error file kept for inspection: {error_file_path}")
        else:
            script_logger.info(f"BCP error file was not created or already removed: {error_file_path}")
        
    script_logger.info("Basic BCP test script finished.") 

if __name__ == "__main__":
    main()
