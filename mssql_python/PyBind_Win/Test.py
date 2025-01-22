import ctypes
import ddbc_bindings
import os
import time

# Constants
SQL_HANDLE_ENV = 1
SQL_HANDLE_DBC = 2
SQL_HANDLE_STMT = 3
SQL_ATTR_DDBC_VERSION = 200
SQL_OV_DDBC3_80 = 380
SQL_DRIVER_NOPROMPT = 0
SQL_NTS = -3  # SQL_NULL_TERMINATED for indicating string length in SQLDriverConnect
SQL_ATTR_ASYNC_DBC_FUNCTIONS_ENABLE = 117  # This is just an example; use the correct value for your environment
SQL_ASYNC_DBC_ENABLE_ON = 1  # This is the value to enable asynchronous functions
SQL_STILL_EXECUTING = 2  # This is the value to indicate that the statement is still executing
SQL_ATTR_ASYNC_STMT_EVENT = 29  # This is just an example; use the correct value for your environment
SQL_ASYNC_ENABLE_ON = 1  # This is the value to enable asynchronous functions
SQL_ATTR_ASYNC_ENABLE = 4  # This is just an example; use the correct value for your environment
SQL_NO_DATA = 100  # This is the value to indicate that there is no more data
SQL_ATTR_AUTOCOMMIT = 102  # This is the value to get the autocommit attribute



# Allocate environment handle
env_handle = ctypes.c_void_p()
result = ddbc_bindings.DDBCSQLAllocHandle(SQL_HANDLE_ENV, 0, ctypes.cast(ctypes.pointer(env_handle), ctypes.c_void_p).value)
if result < 0:  # SQL_SUCCESS is typically 0
    # TODO: if env_handle is not populated in SQLAllocHandle, next line will give error
    # due to None type parameter to CheckError
    print("Error:", ddbc_bindings.DDBCSQLCheckError(SQL_HANDLE_ENV, env_handle.value, result))
    raise RuntimeError(f"Failed to allocate SQL environment handle. Error code: {result}")

# Set the DDBC version environment attribute
result = ddbc_bindings.DDBCSQLSetEnvAttr(env_handle.value, SQL_ATTR_DDBC_VERSION, SQL_OV_DDBC3_80, 0)
if result < 0:
    print("Error:", ddbc_bindings.DDBCSQLCheckError(SQL_HANDLE_ENV, env_handle.value, result))
    raise RuntimeError(f"Failed to set DDBC version attribute. Error code: {result}")

# Allocate connection handle
dbc_handle = ctypes.c_void_p()
result = ddbc_bindings.DDBCSQLAllocHandle(SQL_HANDLE_DBC, env_handle.value, ctypes.cast(ctypes.pointer(dbc_handle), ctypes.c_void_p).value)
if result < 0:
    print("Error:", ddbc_bindings.DDBCSQLCheckError(SQL_HANDLE_DBC, dbc_handle.value, result))
    raise RuntimeError(f"Failed to allocate SQL connection handle. Error code: {result}")

# Set the connection attribute to enable async functions
result = ddbc_bindings.DDBCSQLSetConnectAttr(
    dbc_handle.value,                             # Connection handle (SQLHDBC)
    SQL_ATTR_ASYNC_DBC_FUNCTIONS_ENABLE,          # Attribute for async support
    ctypes.c_void_p(SQL_ASYNC_DBC_ENABLE_ON).value,  # Enable async (cast to SQLPOINTER)
    0                                             # String length, not needed for this attribute
)

# Check if the setting was successful
if result < 0:
    print("Error:", ddbc_bindings.DDBCSQLCheckError(SQL_HANDLE_DBC, dbc_handle.value, result))
    raise RuntimeError(f"Failed to set async connection attribute. Error code: {result}")

# Fetch the connection string from environment variables
connection_string = os.getenv("DB_CONNECTION_STRING")
if not connection_string:
    raise EnvironmentError("Environment variable 'DB_CONNECTION_STRING' is not set or is empty.")

print("Connecting!")
# Call SQLDriverConnect to establish the connection
result = ddbc_bindings.DDBCSQLDriverConnect(
    dbc_handle.value, 
    0,
    connection_string
)
print(result)
#poll for connection to be established
if result == SQL_STILL_EXECUTING:
    print("Still executing, waiting for connection to be established...")
    while result == SQL_STILL_EXECUTING:
        result = ddbc_bindings.DDBCSQLDriverConnect(
            dbc_handle.value, 
            0,
            connection_string
        )
        #wait for 1 sec
        time.sleep(0.2)


# Check for errors after calling SQLDriverConnect
if result < 0:
    print("Error:", ddbc_bindings.DDBCSQLCheckError(SQL_HANDLE_DBC, dbc_handle.value, result))
    raise RuntimeError(f"SQLDriverConnect failed. Error code: {result}")

print("Connection successful!")

#get connection attibuite for 102-SQL_ATTR_AUTOCOMMIT attribute
autocommit = ddbc_bindings.DDBCSQLGetConnectionAttr(dbc_handle.value, SQL_ATTR_AUTOCOMMIT)
print("Autocommit value: ", autocommit)

# Allocate connection statement handle
stmt_handle = ctypes.c_void_p()
result = ddbc_bindings.DDBCSQLAllocHandle(SQL_HANDLE_STMT, dbc_handle.value, ctypes.cast(ctypes.pointer(stmt_handle), ctypes.c_void_p).value)
if result < 0:
    print("Error:", ddbc_bindings.DDBCSQLCheckError(SQL_HANDLE_STMT, stmt_handle.value, result))
    raise RuntimeError(f"Failed to allocate SQL Statement handle. Error code: {result}")

# Set the statement attribute to enable async operations
result = ddbc_bindings.DDBCSQLSetStmtAttr(
    stmt_handle.value,                          # Statement handle (SQLHSTMT)
    SQL_ATTR_ASYNC_ENABLE,                      # Attribute for async enable
    ctypes.c_void_p(SQL_ASYNC_ENABLE_ON).value, # Enable async (cast to SQLPOINTER)
    0                                           # String length (not needed for this attribute)
)

# Check if setting the statement attribute was successful
if result < 0:
    print("Error:", ddbc_bindings.DDBCSQLCheckError(SQL_HANDLE_STMT, stmt_handle.value, result))
    raise RuntimeError(f"Failed to set async enable attribute. Error code: {result}")

sql_query = "SELECT * from Employee"
result = ddbc_bindings.DDBCSQLExecDirect(stmt_handle.value, sql_query)

if result < 0:
    print("Error:", ddbc_bindings.DDBCSQLCheckError(SQL_HANDLE_STMT, stmt_handle.value, result))
    raise RuntimeError(f"Failed to execute query. Error code: {result}")

#poll for statement to be
while result == SQL_STILL_EXECUTING:
    result = ddbc_bindings.DDBCSQLExecDirect(stmt_handle.value, sql_query)
    #wait for 1 sec
    time.sleep(0.2)

print("Fetching Data!")
while result != SQL_NO_DATA:
    rows = []
    result = ddbc_bindings.DDBCSQLFetchOne(stmt_handle.value, rows)
    print(rows)


# ------------------------ Test DDBCSQLExecute ---------------------------------------
# Prepare and execute a SQL statement
print("Test DDBCSQLExecute")
sql_query = "SELECT name, type_desc FROM sys.tables; SELECT spid, loginame, hostname, program_name FROM master.dbo.sysprocesses WHERE program_name LIKE '%MSSQL-Python%';"
params = ['just', 'checking', 'the', 'interface']
# params = ['just', 50, 60.5]
mappedParams = []
for param in params:
    paramInfo = ddbc_bindings.ParamInfo()
    paramInfo.paramCType = 1 # string type
    paramInfo.paramSQLType = 12 # string type
    mappedParams.append(paramType)
# result = ddbc_bindings.SQLExecute(stmt_handle.value, sql_query, params, mappedParams, True)
result = ddbc_bindings.DDBCSQLExecute(stmt_handle.value, "SELECT * FROM sys.tables", [], [], True)
if result < 0:
    print("Error:", ddbc_bindings.DDBCSQLCheckError(SQL_HANDLE_STMT, stmt_handle.value, result))
    raise RuntimeError(f"Failed to execute query. Error code: {result}")


print("Fetching Data for DDBCSQLExecute!")
while result != SQL_NO_DATA:
    print("Fetching resultset")
    column_names = []  # Initialize an empty list to pass as a reference
    retcode = ddbc_bindings.DDBCSQLDescribeCol(stmt_handle.value, column_names)
    # TODO: Check the above retcode
    # Create a ctypes integer for the column count
    column_count = ddbc_bindings.DDBCSQLNumResultCols(stmt_handle.value)
    # Fetch rows
    print(column_names)
    rows = []
    while ddbc_bindings.DDBCSQLFetch(stmt_handle.value) == 0:
        # Assume 4 columns in the result set
        row = ddbc_bindings.DDBCSQLGetData(stmt_handle.value, column_count)
        rows.append(row)

    # Print the results
    for row in rows:
        print(row)
    # Call SQLMoreResults
    result = ddbc_bindings.DDBCSQLMoreResults(stmt_handle.value)
    print(result)
# ------------------------ Test DDBCSQLExecute end ---------------------------------------

# Prepare and execute a SQL statement
sql_query = "SELECT name, type_desc FROM sys.tables; SELECT spid, loginame, hostname, program_name FROM master.dbo.sysprocesses WHERE program_name LIKE '%MSSQL-Python%';"
result = ddbc_bindings.DDBCSQLExecDirect(stmt_handle.value, sql_query)
if result < 0:
    print("Error:", ddbc_bindings.DDBCSQLCheckError(SQL_HANDLE_STMT, stmt_handle.value, result))
    raise RuntimeError(f"Failed to execute query. Error code: {result}")

#poll for connection to be established
if result == SQL_STILL_EXECUTING:
    print("Still executing, waiting for query to be executed...")
    while result == SQL_STILL_EXECUTING:
        result = ddbc_bindings.DDBCSQLExecDirect(stmt_handle.value, sql_query)
        #wait for 1 sec
        time.sleep(0.2)

print("Fetching Data!")
while result != SQL_NO_DATA:
    print("Fetching resultset")
    column_names = []  # Initialize an empty list to pass as a reference
    retcode = ddbc_bindings.DDBCSQLDescribeCol(stmt_handle.value, column_names)
    # TODO: Check the above retcode
    # Create a ctypes integer for the column count
    column_count = ddbc_bindings.DDBCSQLNumResultCols(stmt_handle.value)
    # Fetch rows
    print(column_names)
    rows = []
    while ddbc_bindings.DDBCSQLFetch(stmt_handle.value) == 0:
        # Assume 4 columns in the result set
        row = ddbc_bindings.DDBCSQLGetData(stmt_handle.value, column_count)
        rows.append(row)

    # Print the results
    for row in rows:
        print(row)
    # Call SQLMoreResults
    result = ddbc_bindings.DDBCSQLMoreResults(stmt_handle.value)
    print(result)






# Free the statement handle
result = ddbc_bindings.DDBCSQLFreeHandle(SQL_HANDLE_STMT, stmt_handle.value)
if result < 0:
    print("Error:", ddbc_bindings.DDBCSQLCheckError(SQL_HANDLE_STMT, stmt_handle.value, result))
    raise RuntimeError(f"Failed to free SQL Statement handle. Error code: {result}")
# Disconnect from the data source
result = ddbc_bindings.DDBCSQLDisconnect(dbc_handle.value)
if result < 0:
    print("Error:", ddbc_bindings.DDBCSQLCheckError(SQL_HANDLE_DBC, dbc_handle.value, result))
    raise RuntimeError(f"Failed to disconnect from the data source. Error code: {result}")

# Free the connection handle
result = ddbc_bindings.DDBCSQLFreeHandle(SQL_HANDLE_DBC, dbc_handle.value)
if result < 0:
    print("Error:", ddbc_bindings.DDBCSQLCheckError(SQL_HANDLE_DBC, dbc_handle.value, result))
    raise RuntimeError(f"Failed to free SQL connection handle. Error code: {result}")

# Free the environment handle
result = ddbc_bindings.DDBCSQLFreeHandle(SQL_HANDLE_ENV, env_handle.value)
if result < 0:
    print("Error:", ddbc_bindings.DDBCSQLCheckError(SQL_HANDLE_ENV, env_handle.value, result))
    raise RuntimeError(f"Failed to free SQL environment handle. Error code: {result}")

print("Done!")



