import ctypes
import ddbc_bindings
import os

# Constants
SQL_HANDLE_ENV = 1
SQL_HANDLE_DBC = 2
SQL_HANDLE_STMT = 3
SQL_ATTR_DDBC_VERSION = 200
SQL_OV_DDBC3_80 = 380
SQL_DRIVER_NOPROMPT = 0
SQL_NTS = -3  # SQL_NULL_TERMINATED for indicating string length in SQLDriverConnect
SQL_NO_DATA = 100  # This is the value to indicate that there is no more data

# Allocate environment handle
env_handle = ctypes.c_void_p()
print("Blank Pointer:", env_handle)
print("Blank Pointer Value:", env_handle.value)
result = ddbc_bindings.SQLAllocHandle(SQL_HANDLE_ENV, 0, ctypes.cast(ctypes.pointer(env_handle), ctypes.c_void_p).value)
if result < 0:  # SQL_SUCCESS is typically 0
    print("Error:", ddbc_bindings.CheckError(SQL_HANDLE_ENV, env_handle.value, result))
    raise RuntimeError(f"Failed to allocate SQL environment handle. Error code: {result}")

# Set the DDBC version environment attribute
result = ddbc_bindings.SQLSetEnvAttr(env_handle.value, SQL_ATTR_DDBC_VERSION, SQL_OV_DDBC3_80, 0)
if result < 0:
    print("Error:", ddbc_bindings.CheckError(SQL_HANDLE_ENV, env_handle.value, result))
    raise RuntimeError(f"Failed to set DDBC version attribute. Error code: {result}")

# Allocate connection handle
dbc_handle = ctypes.c_void_p()
result = ddbc_bindings.SQLAllocHandle(SQL_HANDLE_DBC, env_handle.value, ctypes.cast(ctypes.pointer(dbc_handle), ctypes.c_void_p).value)
if result < 0:
    print("Error:", ddbc_bindings.CheckError(SQL_HANDLE_DBC, dbc_handle.value, result))
    raise RuntimeError(f"Failed to allocate SQL connection handle. Error code: {result}")

# Fetch the connection string from environment variables
connection_string = os.getenv("DB_CONNECTION_STRING")
if not connection_string:
    raise EnvironmentError("Environment variable 'DB_CONNECTION_STRING' is not set or is empty.")

print("Connecting!")
# Call SQLDriverConnect to establish the connection
result = ddbc_bindings.SQLDriverConnect(
    dbc_handle.value, 
    0,
    connection_string
)

# Check for errors after calling SQLDriverConnect
if result < 0:
    print("Error:", ddbc_bindings.CheckError(SQL_HANDLE_DBC, dbc_handle.value, result))
    raise RuntimeError(f"SQLDriverConnect failed. Error code: {result}")

print("Connection successful!")

# Allocate connection statement handle
stmt_handle = ctypes.c_void_p()
result = ddbc_bindings.SQLAllocHandle(SQL_HANDLE_STMT, dbc_handle.value, ctypes.cast(ctypes.pointer(stmt_handle), ctypes.c_void_p).value)
if result < 0:
    print("Error:", ddbc_bindings.CheckError(SQL_HANDLE_STMT, stmt_handle.value, result))
    raise RuntimeError(f"Failed to allocate SQL Statement handle. Error code: {result}")

# Prepare and execute a SQL statement
sql_query = "SELECT * FROM customers;"
result = ddbc_bindings.SQLExecDirect(stmt_handle.value, sql_query)
if result < 0:
    print("Error:", ddbc_bindings.CheckError(SQL_HANDLE_STMT, stmt_handle.value, result))
    raise RuntimeError(f"Failed to execute query. Error code: {result}")

print("Fetching Data!")
while result != SQL_NO_DATA:
    print("Fetching resultset")
    column_names = []  # Initialize an empty list to pass as a reference
    retcode = ddbc_bindings.SQLDescribeCol(stmt_handle.value, column_names)
    # Create a ctypes integer for the column count
    column_count = ddbc_bindings.SQLNumResultCols(stmt_handle.value)
    # Fetch rows
    print(column_names)
    rows = []
    while ddbc_bindings.SQLFetch(stmt_handle.value) == 0:
        # Assume 4 columns in the result set
        row = ddbc_bindings.SQLGetData(stmt_handle.value, column_count)
        rows.append(row)

    # Print the results
    for row in rows:
        print(row)
    # Call SQLMoreResults
    result = ddbc_bindings.SQLMoreResults(stmt_handle.value)
    print(result)

# Free the statement handle
result = ddbc_bindings.SQLFreeHandle(SQL_HANDLE_STMT, stmt_handle.value)
if result < 0:
    print("Error:", ddbc_bindings.CheckError(SQL_HANDLE_STMT, stmt_handle.value, result))
    raise RuntimeError(f"Failed to free SQL Statement handle. Error code: {result}")
# Disconnect from the data source
result = ddbc_bindings.SQLDisconnect(dbc_handle.value)
if result < 0:
    print("Error:", ddbc_bindings.CheckError(SQL_HANDLE_DBC, dbc_handle.value, result))
    raise RuntimeError(f"Failed to disconnect from the data source. Error code: {result}")

# Free the connection handle
result = ddbc_bindings.SQLFreeHandle(SQL_HANDLE_DBC, dbc_handle.value)
if result < 0:
    print("Error:", ddbc_bindings.CheckError(SQL_HANDLE_DBC, dbc_handle.value, result))
    raise RuntimeError(f"Failed to free SQL connection handle. Error code: {result}")

# Free the environment handle
result = ddbc_bindings.SQLFreeHandle(SQL_HANDLE_ENV, env_handle.value)
if result < 0:
    print("Error:", ddbc_bindings.CheckError(SQL_HANDLE_ENV, env_handle.value, result))
    raise RuntimeError(f"Failed to free SQL environment handle. Error code: {result}")

print("Done!")
