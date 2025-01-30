import ctypes
import datetime
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

def fetch_data_onebyone(stmt_handle):
    rows = []
    ret = 1
    while ret != SQL_NO_DATA:
        row = []
        ret = ddbc_bindings.DDBCSQLFetchOne(stmt_handle.value, row)
        if ret < 0:
            print("Error:", ddbc_bindings.DDBCSQLCheckError(SQL_HANDLE_STMT, stmt_handle.value, ret))
            raise RuntimeError(f"Failed to fetch data. Error code: {ret}")
        print(row)
        rows.append(row)
    return rows

def fetch_data_many(stmt_handle):
    rows = []
    ret = 1
    while ret != SQL_NO_DATA:
        ret = ddbc_bindings.DDBCSQLFetchMany(stmt_handle.value, rows, 10)
        if ret < 0:
            print("Error:", ddbc_bindings.DDBCSQLCheckError(SQL_HANDLE_STMT, stmt_handle.value, ret))
            raise RuntimeError(f"Failed to fetch data. Error code: {ret}")
    return rows

def fetch_data_all(stmt_handle):
    rows = []
    ret = 1
    ret = ddbc_bindings.DDBCSQLFetchAll(stmt_handle.value, rows)
    if ret != SQL_NO_DATA:
        print("Error:", ddbc_bindings.DDBCSQLCheckError(SQL_HANDLE_STMT, stmt_handle.value, ret))
        raise RuntimeError(f"Failed to fetch data. Error code: {ret}")
    return rows

def fetch_data(stmt_handle):
    rows = []
    column_count = ddbc_bindings.DDBCSQLNumResultCols(stmt_handle.value)
    print("Number of columns = " + str(column_count))
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

def add_string_param(params, paramInfos, data_string):
    params.append(data_string)
    paramInfo = ddbc_bindings.ParamInfo()
    paramInfo.paramCType = 1 # SQL_C_CHAR
    paramInfo.paramSQLType = 12 # SQL_VARCHAR
    paramInfo.columnSize = len(data_string)
    paramInfo.inputOutputType = 1 # SQL_PARAM_INPUT
    paramInfos.append(paramInfo)

def add_wstring_param(params, paramInfos, wide_string):
    params.append(wide_string)
    paramInfo = ddbc_bindings.ParamInfo()
    paramInfo.paramCType = -8 # SQL_C_WCHAR
    paramInfo.paramSQLType = -9 # SQL_WVARCHAR
    paramInfo.columnSize = len(wide_string)
    paramInfo.inputOutputType = 1 # SQL_PARAM_INPUT
    paramInfos.append(paramInfo)

def add_date_param(params, paramInfos):
    date_obj = datetime.date(2025, 1, 28) # 28th Jan 2025
    params.append(date_obj)
    paramInfo = ddbc_bindings.ParamInfo()
    paramInfo.paramCType = 91 # SQL_C_TYPE_DATE
    paramInfo.paramSQLType = 91 # SQL_TYPE_DATE
    paramInfo.inputOutputType = 1 # SQL_PARAM_INPUT
    paramInfos.append(paramInfo)

def add_time_param(params, paramInfos):
    time_obj = datetime.time(5, 15, 30) # 5:15 AM + 30 secs
    params.append(time_obj)
    paramInfo = ddbc_bindings.ParamInfo()
    paramInfo.paramCType = 92 # SQL_C_TYPE_TIME
    paramInfo.paramSQLType = 92 # SQL_TYPE_TIME
    paramInfo.inputOutputType = 1 # SQL_PARAM_INPUT
    paramInfos.append(paramInfo)

def add_datetime_param(params, paramInfos, addNone):
    paramInfo = ddbc_bindings.ParamInfo()
    if addNone:
        params.append(None)
        paramInfo.paramCType = 99 # SQL_C_DEFAULT
    else:
        datetime_obj = datetime.datetime(2025, 1, 28, 5, 15, 30)
        params.append(datetime_obj)
        paramInfo.paramCType = 93 # SQL_C_TYPE_TIMESTAMP
    paramInfo.paramSQLType = 93 # SQL_TYPE_TIMESTAMP
    paramInfo.inputOutputType = 1 # SQL_PARAM_INPUT
    paramInfos.append(paramInfo)

def add_bool_param(params, paramInfos, bool_val):
    params.append(bool_val)
    paramInfo = ddbc_bindings.ParamInfo()
    paramInfo.paramCType = -7 # SQL_C_BIT
    paramInfo.paramSQLType = -7 # SQL_BIT
    paramInfo.inputOutputType = 1 # SQL_PARAM_INPUT
    paramInfos.append(paramInfo)

def add_tinyint_param(params, paramInfos, val):
    params.append(val)
    paramInfo = ddbc_bindings.ParamInfo()
    paramInfo.paramCType = -6 # SQL_C_TINYINT
    paramInfo.paramSQLType = -6 # SQL_TINYINT
    paramInfo.inputOutputType = 1 # SQL_PARAM_INPUT
    paramInfos.append(paramInfo)

def add_bigint_param(params, paramInfos, val):
    params.append(val)
    paramInfo = ddbc_bindings.ParamInfo()
    paramInfo.paramCType = -25 # SQL_C_SBIGINT
    paramInfo.paramSQLType = -5 # SQL_BIGINT
    paramInfo.inputOutputType = 1 # SQL_PARAM_INPUT
    paramInfos.append(paramInfo)

def add_float_param(params, paramInfos, val):
    params.append(val)
    paramInfo = ddbc_bindings.ParamInfo()
    paramInfo.paramCType = 7 # SQL_C_FLOAT
    paramInfo.paramSQLType = 7 # SQL_REAL
    paramInfo.inputOutputType = 1 # SQL_PARAM_INPUT
    paramInfo.columnSize = 15 # Precision
    paramInfos.append(paramInfo)

def add_double_param(params, paramInfos, val):
    params.append(val)
    paramInfo = ddbc_bindings.ParamInfo()
    paramInfo.paramCType = 8 # SQL_C_DOUBLE
    paramInfo.paramSQLType = 8 # SQL_DOUBLE
    paramInfo.inputOutputType = 1 # SQL_PARAM_INPUT
    paramInfo.columnSize = 15 # Precision
    paramInfos.append(paramInfo)

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
    '''
    insert_sql_query = """
        ALTER TABLE [Employees].[dbo].[EmployeeFullNames]
        ADD date_ DATE,
            time_ TIME,
            datetime_ TIMESTAMP,
            wchar_ NVARCHAR(10),
            bool_ BIT,
            tinyint_ TINYINT,
            bigint_ BIGINT,
            float_ FLOAT(10),
            double_ DOUBLE PRECISION;

    """
    '''
    insert_sql_query = "INSERT INTO [Employees].[dbo].[EmployeeFullNames] (FirstName, LastName, date_, time_, wchar_, bool_, tinyint_, bigint_, float_, double_) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"
    params = []
    param_info_list = []
    add_string_param(params, param_info_list, 'Harry')
    add_string_param(params, param_info_list, 'Potter2')
    add_date_param(params, param_info_list)
    add_time_param(params, param_info_list)
    # add_datetime_param(params, param_info_list, addNone=True) - Cannot insert an explicit value into a timestamp column. Use INSERT with a column list to exclude the timestamp column, or insert a DEFAULT into the timestamp column. Traceback (most recent call last):
    add_wstring_param(params, param_info_list, u"Wide str3")
    add_bool_param(params, param_info_list, True)
    add_tinyint_param(params, param_info_list, 127)
    add_bigint_param(params, param_info_list, 123456789)
    add_float_param(params, param_info_list, 12.34)
    add_double_param(params, param_info_list, 12.34)
    result = ddbc_sql_execute(stmt_handle, insert_sql_query, params, param_info_list, True)
    print("DDBCSQLExecute result:", result)

    # Test DDBCSQLExecute for SELECT query
    print("Test DDBCSQLExecute select")
    # select_sql_query = "SELECT * FROM customers;"
    select_sql_query = "SELECT bool_, float_, wchar_, date_, time_, datetime_, wchar_, FirstName, LastName  FROM [Employees].[dbo].[EmployeeFullNames];"
    params = []
    param_info_list = []
    result = ddbc_sql_execute(stmt_handle, select_sql_query, params, param_info_list, False)
    print("DDBCSQLExecute result:", result)

    print("Fetching Data for DDBCSQLExecute!")
    column_names = describe_columns(stmt_handle)
    print(column_names)
    ret = 1
    while ret != SQL_NO_DATA:
        if column_names:
            rows = fetch_data_all(stmt_handle)
            for row in rows:
                print(row)
        else:
            print("No columns to fetch data from.")
        ret = ddbc_bindings.DDBCSQLMoreResults(stmt_handle.value)

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
