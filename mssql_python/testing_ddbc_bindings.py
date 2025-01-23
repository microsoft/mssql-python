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

def alloc_handle(handle_type, input_handle):
    handle = ctypes.c_void_p()
    result = ddbc_bindings.DDBCSQLAllocHandle(handle_type, input_handle, ctypes.cast(ctypes.pointer(handle), ctypes.c_void_p).value)
    if result < 0:
        print("Error:", ddbc_bindings.DDBCSQLCheckError(handle_type, handle.value, result))
        raise RuntimeError(f"Failed to allocate handle. Error code: {result}")
    return handle

def free_handle(handle_type, handle):
    result = ddbc_bindings.DDBCSQLFreeHandle(handle_type, handle.value)
    if result < 0:
        print("Error:", ddbc_bindings.DDBCSQLCheckError(handle_type, handle.value, result))
        raise RuntimeError(f"Failed to free handle. Error code: {result}")

def ddbc_sql_execute(stmt_handle, query, params, param_info_list, use_prepare=True):
    result = ddbc_bindings.DDBCSQLExecute(stmt_handle.value, query, params, param_info_list, use_prepare)
    if result < 0:
        print("Error:", ddbc_bindings.DDBCSQLCheckError(SQL_HANDLE_STMT, stmt_handle.value, result))
        raise RuntimeError(f"Failed to execute query. Error code: {result}")
    return result

def fetch_data(stmt_handle):
    rows = []
    column_count = ddbc_bindings.DDBCSQLNumResultCols(stmt_handle.value)
    while True:
        result = ddbc_bindings.DDBCSQLFetch(stmt_handle.value)
        if result == SQL_NO_DATA:
            break
        elif result < 0:
            print("Error:", ddbc_bindings.DDBCSQLCheckError(SQL_HANDLE_STMT, stmt_handle.value, result))
            raise RuntimeError(f"Failed to fetch data. Error code: {result}")
        if column_count > 0:
            row = []
            result = ddbc_bindings.DDBCSQLGetData(stmt_handle.value, column_count, row)
            if result < 0:
                print("Error:", ddbc_bindings.DDBCSQLCheckError(SQL_HANDLE_STMT, stmt_handle.value, result))
                raise RuntimeError(f"Failed to get data. Error code: {result}")
            rows.append(row)
    return rows

def describe_columns(stmt_handle):
    column_names = []
    result = ddbc_bindings.DDBCSQLDescribeCol(stmt_handle.value, column_names)
    if result < 0:
        print("Error:", ddbc_bindings.DDBCSQLCheckError(SQL_HANDLE_STMT, stmt_handle.value, result))
        raise RuntimeError(f"Failed to describe columns. Error code: {result}")
    return column_names

def connect_to_db(dbc_handle, connection_string):
    result = ddbc_bindings.DDBCSQLDriverConnect(dbc_handle.value, 0, connection_string)
    if result < 0:
        print("Error:", ddbc_bindings.DDBCSQLCheckError(SQL_HANDLE_DBC, dbc_handle.value, result))
        raise RuntimeError(f"SQLDriverConnect failed. Error code: {result}")

if __name__ == "__main__":
    # Allocate environment handle
    env_handle = alloc_handle(SQL_HANDLE_ENV, 0)

    # Set the DDBC version environment attribute
    result = ddbc_bindings.DDBCSQLSetEnvAttr(env_handle.value, SQL_ATTR_DDBC_VERSION, SQL_OV_DDBC3_80, 0)
    if result < 0:
        print("Error:", ddbc_bindings.DDBCSQLCheckError(SQL_HANDLE_ENV, env_handle.value, result))
        raise RuntimeError(f"Failed to set DDBC version attribute. Error code: {result}")

    # Allocate connection handle
    dbc_handle = alloc_handle(SQL_HANDLE_DBC, env_handle.value)

    # Fetch the connection string from environment variables
    connection_string = os.getenv("DB_CONNECTION_STRING")
    if not connection_string:
        raise EnvironmentError("Environment variable 'DB_CONNECTION_STRING' is not set or is empty.")

    print("Connecting!")
    connect_to_db(dbc_handle, connection_string)
    print("Connection successful!")

    # Allocate connection statement handle
    stmt_handle = alloc_handle(SQL_HANDLE_STMT, dbc_handle.value)

    ParamInfo = ddbc_bindings.ParamInfo
    '''
    Table schema:
    CREATE TABLE customers (
        id INT IDENTITY(1,1) PRIMARY KEY,
        name NVARCHAR(100),
        email NVARCHAR(100)
    );
    '''
    # Test DDBCSQLExecute for INSERT query
    print("Test DDBCSQLExecute insert")
    insert_sql_query = "INSERT INTO customers (name, email) VALUES (?, ?);"
    params = ['gaurav', 'gaurav@gaurav.com']
    param_info_list = []
    for i in params:
        paraminfo = ParamInfo()
        paraminfo.paramCType = 1
        paraminfo.paramSQLType = 12
        param_info_list.append(paraminfo)
    result = ddbc_sql_execute(stmt_handle, insert_sql_query, params, param_info_list, True)
    print("DDBCSQLExecute result:", result)

    # Test DDBCSQLExecute for SELECT query
    print("Test DDBCSQLExecute select")
    select_sql_query = "SELECT * FROM customers;"
    params = []
    param_info_list = []
    result = ddbc_sql_execute(stmt_handle, select_sql_query, params, param_info_list, False)
    print("DDBCSQLExecute result:", result)

    print("Fetching Data for DDBCSQLExecute!")
    column_names = describe_columns(stmt_handle)
    print(column_names)
    if column_names:
        rows = fetch_data(stmt_handle)
        for row in rows:
            print(row)
    else:
        print("No columns to fetch data from.")

    # Free the statement handle
    free_handle(SQL_HANDLE_STMT, stmt_handle)
    # Disconnect from the data source
    result = ddbc_bindings.DDBCSQLDisconnect(dbc_handle.value)
    if result < 0:
        print("Error:", ddbc_bindings.DDBCSQLCheckError(SQL_HANDLE_DBC, dbc_handle.value, result))
        raise RuntimeError(f"Failed to disconnect from the data source. Error code: {result}")

    # Free the connection handle
    free_handle(SQL_HANDLE_DBC, dbc_handle)

    # Free the environment handle
    free_handle(SQL_HANDLE_ENV, env_handle)

    print("Done!")
