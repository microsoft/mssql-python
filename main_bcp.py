from mssql_python import connect
from mssql_python.bcp_main import BCPClient
from mssql_python.bcp_options import BCPOptions
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    # BCP connection attribute
    SQL_COPT_SS_BCP = 1214
    
    # Connection string - same as your C++ version
    conn_str = "Driver={ODBC Driver 18 for SQL Server};Server=tcp:DESKTOP-1A982SC,1433;Database=TestBCP;TrustServerCertificate=yes;Trusted_Connection=yes;"
    
    try:
        # Connect with BCP enabled
        logger.info("Connecting to database...")
        conn = connect(conn_str, attrs_before={SQL_COPT_SS_BCP: 1})
        logger.info("Connected successfully")
        
        # Define BCP parameters
        table_name = "[TestBCP].[dbo].[EmployeeFullNames]"
        data_file = "C:\\Users\\jathakkar\\OneDrive - Microsoft\\Documents\\Github_mssql_python\\mssql-python\\EmployeeFullNames.bcp"
        error_file = "C:\\Users\\jathakkar\\OneDrive - Microsoft\\Documents\\Github_mssql_python\\mssql-python\\bcp_wide_error.txt"
        format_file = "C:\\Users\\jathakkar\\OneDrive - Microsoft\\Documents\\Github_mssql_python\\mssql-python\\EmployeeFullNames.fmt"
        
        # Initialize BCP client
        bcp_client = BCPClient(conn)
        
        # Set BCP options for reading from format file
        bcp_options = BCPOptions(
            direction='in',
            data_file=data_file,
            error_file=error_file,
            format_file=format_file
        )
        
        # Execute BCP
        logger.info(f"Starting BCP IN for {table_name}")
        bcp_client.sql_bulk_copy(table=table_name, options=bcp_options)
        logger.info("BCP IN operation completed")
        
        # Verify data
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        logger.info(f"Rows copied: {count}")
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return 1
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            logger.info("Connection closed")
    
    logger.info("BCP completed")
    return 0

if __name__ == "__main__":
    main()