import uuid
from pathlib import Path

from mssql_python import connect, setup_logging
from mssql_python.bcp_main import BCPClient
from mssql_python.bcp_options import BCPOptions, ColumnFormat
from mssql_python.exceptions import Error as MssqlError


def create_minimal_data_file(file_path: Path, data: list):
    """Creates a simple CSV file."""
    print(f"Creating data file: {file_path}")
    with open(file_path, 'w', newline='', encoding='utf-8') as f:
        for row in data:
            f.write(",".join(map(str, row)) + "\n")
    print(f"Data file created: {file_path}")

def main():
    conn_str = os.getenv("DB_CONNECTION_STRING")
    if not conn_str:
        print("DB_CONNECTION_STRING environment variable not set.")
        print("Error: DB_CONNECTION_STRING environment variable not set.")
        return

    table_name = f"dbo.bcp_basic_test_{str(uuid.uuid4()).replace('-', '')[:8]}"
    data_file_path = Path(f"./{table_name.split('.')[-1]}_data.csv")
    error_file_path = Path(f"./{table_name.split('.')[-1]}_errors.txt")

    conn = None
    try:
        print("Connecting to database...")
        conn = connect(conn_str)
        print("Connected.")

        cursor = conn.cursor()
        # 1. Create a simple test table
        print(f"Creating table: {table_name}")
        cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
        cursor.execute(f"""
            CREATE TABLE {table_name} (
                id INT PRIMARY KEY,
                name VARCHAR(50)
            )
        """)
        conn.commit()
        print(f"Table {table_name} created.")

        # 2. Prepare sample data
        sample_data = [
            [1, "Alice"],
            [2, "Bob"],
            [3, "Charlie"]
        ]
        create_minimal_data_file(data_file_path, sample_data)

        # 3. Perform BCP IN
        print(f"Starting BCP IN for {table_name} from {data_file_path}")
        try:
            bcp_client = BCPClient(connection=conn)
            
            bcp_options = BCPOptions(
                direction='in',
                data_file=str(data_file_path),
                error_file=str(error_file_path),
                bulk_mode="char",
                columns=[ColumnFormat(field_terminator=b",", row_terminator=b"\n")]
            )
            bcp_client.sql_bulk_copy(table=table_name, options=bcp_options)
            print("BCP IN operation completed.")

            if error_file_path.exists() and error_file_path.stat().st_size > 0:
                print(f"BCP IN errors reported in: {error_file_path}")
                with open(error_file_path, "r") as err_f:
                    print(f"--- BCP IN Errors ---\n{err_f.read()}\n--------------------")
            else:
                print("BCP IN operation reported no errors.")

            # 4. (Optional) Verify data
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            print(f"Table {table_name} now contains {count} rows.")
            print(f"Table {table_name} contains {count} rows after BCP IN.")
            assert count == len(sample_data), "Row count mismatch after BCP IN"
            print("BCP data verification successful.")

        except MssqlError as bcp_err:
            print(f"Error during BCP operation or verification: {bcp_err}")
            print(f"Error during BCP operation or verification: {bcp_err}")
        except Exception as general_bcp_err:
            print(f"An unexpected error occurred during BCP operation or verification: {general_bcp_err}")
            print(f"An unexpected error occurred during BCP operation or verification: {general_bcp_err}")

    except MssqlError as e:
        print(f"A mssql_python library error occurred: {e}")
        print(f"Database operation failed: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        print(f"An error occurred: {e}")
    finally:
        if conn:
            try:
                with conn.cursor() as cursor:
                    print(f"Cleaning up: Dropping table {table_name}")
                    cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
                    conn.commit()
                    print(f"Table {table_name} dropped.")
            except Exception as e_cleanup:
                print(f"Error during table cleanup: {e_cleanup}")
            
            print("Closing database connection.")
            conn.close()

        # Clean up temporary files
        for f_path in [data_file_path, error_file_path]:
            if f_path.exists():
                try:
                    f_path.unlink()
                    print(f"Deleted: {f_path}")
                except Exception as e_file_del:
                    print(f"Error deleting file {f_path}: {e_file_del}")
        
    print("Basic BCP test script finished.")

if __name__ == "__main__":
    main()
