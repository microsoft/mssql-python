"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.
This module provides functions to test DDBC bindings.
"""
import ctypes
import datetime
import os
from mssql_python import ddbc_bindings
from mssql_python.logging_config import setup_logging

setup_logging()

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
    """
    Allocate a handle for the given handle type and input handle.
    """
    result_alloc, handle = ddbc_bindings.DDBCSQLAllocHandle(
        handle_type,
        input_handle
    )
    if result_alloc < 0:
        print(
            "Error:", ddbc_bindings.DDBCSQLCheckError(handle_type, handle, result_alloc)
        )
        raise RuntimeError(f"Failed to allocate handle. Error code: {result_alloc}")
    return handle


def free_handle(handle_type, handle):
    """
    Free the handle for the given handle type and handle.
    """
    result_free = ddbc_bindings.DDBCSQLFreeHandle(handle_type, handle)
    if result_free < 0:
        print(
            "Error:", ddbc_bindings.DDBCSQLCheckError(handle_type, handle, result_free)
        )
        raise RuntimeError(f"Failed to free handle. Error code: {result_free}")


def ddbc_sql_execute(
    stmt_handle, query, params, param_info_list, is_stmt_prepared, use_prepare=True
):
    """
    Execute an SQL statement using DDBC bindings.
    """
    result_execute = ddbc_bindings.DDBCSQLExecute(
        stmt_handle, query, params, param_info_list, is_stmt_prepared, use_prepare
    )
    if result_execute < 0:
        print(
            "Error: ",
            ddbc_bindings.DDBCSQLCheckError(SQL_HANDLE_STMT, stmt_handle, result_execute),
        )
        raise RuntimeError(f"Failed to execute query. Error code: {result_execute}")
    return result_execute


def fetch_data_onebyone(stmt_handle):
    """
    Fetch data one by one using DDBC bindings.
    """
    rows = []
    ret_fetch = 1
    while ret_fetch != SQL_NO_DATA:
        row = []
        ret_fetch = ddbc_bindings.DDBCSQLFetchOne(stmt_handle, row)
        if ret_fetch < 0:
            print(
                "Error: ",
                ddbc_bindings.DDBCSQLCheckError(
                    SQL_HANDLE_STMT, stmt_handle, ret_fetch
                ),
            )
            raise RuntimeError(f"Failed to fetch data. Error code: {ret_fetch}")
        print(row)
        rows.append(row)
    return rows


def fetch_data_many(stmt_handle):
    """
    Fetch data in batches using DDBC bindings.
    """
    rows = []
    ret_fetch = 1
    while ret_fetch != SQL_NO_DATA:
        ret_fetch = ddbc_bindings.DDBCSQLFetchMany(stmt_handle, rows, 10)
        if ret_fetch < 0:
            print(
                "Error: ",
                ddbc_bindings.DDBCSQLCheckError(
                    SQL_HANDLE_STMT, stmt_handle, ret_fetch
                ),
            )
            raise RuntimeError(f"Failed to fetch data. Error code: {ret_fetch}")
    return rows


def fetch_data_all(stmt_handle):
    """
    Fetch all data using DDBC bindings.
    """
    rows = []
    ret_fetch = ddbc_bindings.DDBCSQLFetchAll(stmt_handle, rows)
    if ret_fetch != SQL_NO_DATA:
        print(
            "Error: ",
            ddbc_bindings.DDBCSQLCheckError(SQL_HANDLE_STMT, stmt_handle, ret_fetch),
        )
        raise RuntimeError(f"Failed to fetch data. Error code: {ret_fetch}")
    return rows


def fetch_data(stmt_handle):
    """
    Fetch data using DDBC bindings.
    """
    rows = []
    column_count = ddbc_bindings.DDBCSQLNumResultCols(stmt_handle)
    print("Number of columns = " + str(column_count))
    while True:
        result_fetch = ddbc_bindings.DDBCSQLFetch(stmt_handle)
        if result_fetch == SQL_NO_DATA:
            break
        if result_fetch < 0:
            print(
                "Error: ",
                ddbc_bindings.DDBCSQLCheckError(
                    SQL_HANDLE_STMT, stmt_handle, result_fetch
                ),
            )
            raise RuntimeError(f"Failed to fetch data. Error code: {result_fetch}")
        if column_count > 0:
            row = []
            result_get_data = ddbc_bindings.DDBCSQLGetData(stmt_handle, column_count, row)
            if result_get_data < 0:
                print(
                    "Error: ",
                    ddbc_bindings.DDBCSQLCheckError(
                        SQL_HANDLE_STMT, stmt_handle, result_get_data
                    ),
                )
                raise RuntimeError(f"Failed to get data. Error code: {result_get_data}")
            rows.append(row)
    return rows


def describe_columns(stmt_handle):
    """
    Describe columns using DDBC bindings.
    """
    column_names = []
    result_describe = ddbc_bindings.DDBCSQLDescribeCol(stmt_handle, column_names)
    if result_describe < 0:
        print(
            "Error: ",
            ddbc_bindings.DDBCSQLCheckError(SQL_HANDLE_STMT, stmt_handle, result_describe),
        )
        raise RuntimeError(f"Failed to describe columns. Error code: {result_describe}")
    return column_names


def connect_to_db(dbc_handle, connection_string):
    """
    Connect to the database using DDBC bindings.
    """
    result_connect = ddbc_bindings.DDBCSQLDriverConnect(dbc_handle, 0, connection_string)
    if result_connect < 0:
        print(
            "Error: ",
            ddbc_bindings.DDBCSQLCheckError(SQL_HANDLE_DBC, dbc_handle, result_connect),
        )
        raise RuntimeError(f"SQLDriverConnect failed. Error code: {result_connect}")


def add_string_param(params, param_infos, data_string):
    """
    Add a string parameter to the parameter list.
    """
    params.append(data_string)
    param_info = ddbc_bindings.ParamInfo()
    param_info.paramCType = 1  # SQL_C_CHAR
    param_info.paramSQLType = 12  # SQL_VARCHAR
    param_info.columnSize = len(data_string)
    param_info.inputOutputType = 1  # SQL_PARAM_INPUT
    param_infos.append(param_info)


def add_wstring_param(params, param_infos, wide_string):
    """
    Add a wide string parameter to the parameter list.
    """
    params.append(wide_string)
    param_info = ddbc_bindings.ParamInfo()
    param_info.paramCType = -8  # SQL_C_WCHAR
    param_info.paramSQLType = -9  # SQL_WVARCHAR
    param_info.columnSize = len(wide_string)
    param_info.inputOutputType = 1  # SQL_PARAM_INPUT
    param_infos.append(param_info)


def add_date_param(params, param_infos):
    """
    Add a date parameter to the parameter list.
    """
    date_obj = datetime.date(2025, 1, 28)  # 28th Jan 2025
    params.append(date_obj)
    param_info = ddbc_bindings.ParamInfo()
    param_info.paramCType = 91  # SQL_C_TYPE_DATE
    param_info.paramSQLType = 91  # SQL_TYPE_DATE
    param_info.inputOutputType = 1  # SQL_PARAM_INPUT
    param_infos.append(param_info)


def add_time_param(params, param_infos):
    """
    Add a time parameter to the parameter list.
    """
    time_obj = datetime.time(5, 15, 30)  # 5:15 AM + 30 secs
    params.append(time_obj)
    param_info = ddbc_bindings.ParamInfo()
    param_info.paramCType = 92  # SQL_C_TYPE_TIME
    param_info.paramSQLType = 92  # SQL_TYPE_TIME
    param_info.inputOutputType = 1  # SQL_PARAM_INPUT
    param_infos.append(param_info)


def add_datetime_param(params, param_infos, add_none):
    """
    Add a datetime parameter to the parameter list.
    """
    param_info = ddbc_bindings.ParamInfo()
    if add_none:
        params.append(None)
        param_info.paramCType = 99  # SQL_C_DEFAULT
    else:
        datetime_obj = datetime.datetime(2025, 1, 28, 5, 15, 30)
        params.append(datetime_obj)
        param_info.paramCType = 93  # SQL_C_TYPE_TIMESTAMP
    param_info.paramSQLType = 93  # SQL_TYPE_TIMESTAMP
    param_info.inputOutputType = 1  # SQL_PARAM_INPUT
    param_infos.append(param_info)


def add_bool_param(params, param_infos, bool_val):
    """
    Add a boolean parameter to the parameter list.
    """
    params.append(bool_val)
    param_info = ddbc_bindings.ParamInfo()
    param_info.paramCType = -7  # SQL_C_BIT
    param_info.paramSQLType = -7  # SQL_BIT
    param_info.inputOutputType = 1  # SQL_PARAM_INPUT
    param_infos.append(param_info)


def add_tinyint_param(params, param_infos, val):
    """
    Add a tinyint parameter to the parameter list.
    """
    params.append(val)
    param_info = ddbc_bindings.ParamInfo()
    param_info.paramCType = -6  # SQL_C_TINYINT
    param_info.paramSQLType = -6  # SQL_TINYINT
    param_info.inputOutputType = 1  # SQL_PARAM_INPUT
    param_infos.append(param_info)


def add_bigint_param(params, param_infos, val):
    """
    Add a bigint parameter to the parameter list.
    """
    params.append(val)
    param_info = ddbc_bindings.ParamInfo()
    param_info.paramCType = -25  # SQL_C_SBIGINT
    param_info.paramSQLType = -5  # SQL_BIGINT
    param_info.inputOutputType = 1  # SQL_PARAM_INPUT
    param_infos.append(param_info)


def add_float_param(params, param_infos, val):
    """
    Add a float parameter to the parameter list.
    """
    params.append(val)
    param_info = ddbc_bindings.ParamInfo()
    param_info.paramCType = 7  # SQL_C_FLOAT
    param_info.paramSQLType = 7  # SQL_REAL
    param_info.inputOutputType = 1  # SQL_PARAM_INPUT
    param_info.columnSize = 15  # Precision
    param_infos.append(param_info)


def add_double_param(params, param_infos, val):
    """
    Add a double parameter to the parameter list.
    """
    params.append(val)
    param_info = ddbc_bindings.ParamInfo()
    param_info.paramCType = 8  # SQL_C_DOUBLE
    param_info.paramSQLType = 8  # SQL_DOUBLE
    param_info.inputOutputType = 1  # SQL_PARAM_INPUT
    param_info.columnSize = 15  # Precision
    param_infos.append(param_info)


def add_numeric_param(params, param_infos, param):
    """
    Add a numeric parameter to the parameter list.
    """
    numeric_data = ddbc_bindings.NumericData()
    numeric_data.precision = len(param.as_tuple().digits)
    numeric_data.scale = param.as_tuple().exponent * -1
    numeric_data.sign = param.as_tuple().sign
    numeric_data.val = str(param)
    print(
        type(numeric_data.precision),
        type(numeric_data.scale),
        type(numeric_data.sign),
        type(numeric_data.val),
        type(numeric_data),
    )
    params.append(numeric_data)

    param_info = ddbc_bindings.ParamInfo()
    param_info.paramCType = 2  # SQL_C_NUMERIC
    param_info.paramSQLType = 2  # SQL_NUMERIC
    param_info.inputOutputType = 1  # SQL_PARAM_INPUT
    param_info.columnSize = 10  # Precision
    param_infos.append(param_info)


if __name__ == "__main__":
    # Allocate environment handle
    env_handle = alloc_handle(SQL_HANDLE_ENV, None)
    
    # Set the DDBC version environment attribute
    result_set_env = ddbc_bindings.DDBCSQLSetEnvAttr(
        env_handle, SQL_ATTR_DDBC_VERSION, SQL_OV_DDBC3_80, 0
    )
    if result_set_env < 0:
        print(
            "Error: ",
            ddbc_bindings.DDBCSQLCheckError(SQL_HANDLE_ENV, env_handle, result_set_env),
        )
        raise RuntimeError(
            f"Failed to set DDBC version attribute. Error code: {result_set_env}"
        )

    # Allocate connection handle
    dbc_handle = alloc_handle(SQL_HANDLE_DBC, env_handle)

    # Fetch the connection string from environment variables
    connection_string = os.getenv("DB_CONNECTION_STRING")

    if not connection_string:
        raise EnvironmentError(
            "Environment variable 'DB_CONNECTION_STRING' is not set or is empty."
        )

    print("Connecting!")
    connect_to_db(dbc_handle, connection_string)
    print("Connection successful!")

    # Allocate connection statement handle
    stmt_handle = alloc_handle(SQL_HANDLE_STMT, dbc_handle)

    ParamInfo = ddbc_bindings.ParamInfo
    """
    Table schema:
    CREATE TABLE customers (
        id INT IDENTITY(1,1) PRIMARY KEY,
        name NVARCHAR(100),
        email NVARCHAR(100)
    );
    """
    # Test DDBCSQLExecute for INSERT query
    print("Test DDBCSQLExecute insert")
    insert_sql_query = (
        "INSERT INTO [Employees].[dbo].[EmployeeFullNames] "
        "(FirstName, LastName, date_, time_, wchar_, bool_, tinyint_, bigint_, float_, double_) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"
    )
    params_insert = []
    param_info_list_insert = []
    add_string_param(params_insert, param_info_list_insert, "test")
    add_string_param(params_insert, param_info_list_insert, "inner file")
    add_date_param(params_insert, param_info_list_insert)
    add_time_param(params_insert, param_info_list_insert)
    # add_datetime_param(params_insert, param_info_list_insert, addNone=True) - Cannot insert an explicit value into a timestamp column. Use INSERT with a column list to exclude the timestamp column, or insert a DEFAULT into the timestamp column. Traceback (most recent call last):
    add_wstring_param(params_insert, param_info_list_insert, "Wide str3")
    add_bool_param(params_insert, param_info_list_insert, True)
    add_tinyint_param(params_insert, param_info_list_insert, 127)
    add_bigint_param(params_insert, param_info_list_insert, 123456789)
    add_float_param(params_insert, param_info_list_insert, 12.34)
    add_double_param(params_insert, param_info_list_insert, 12.34)
    # add_numeric_param(params_insert, param_info_list_insert, decimal.Decimal('12'))
    is_stmt_prepared_insert = [False]
    result_insert = ddbc_sql_execute(
        stmt_handle, insert_sql_query, params_insert, param_info_list_insert, is_stmt_prepared_insert, True
    )
    print("DDBCSQLExecute result:", result_insert)

    # Test DDBCSQLExecute for SELECT query
    print("Test DDBCSQLExecute select")
    is_stmt_prepared_select = [False]
    select_sql_query = (
        "SELECT bool_, float_, wchar_, date_, time_, datetime_, wchar_, FirstName, LastName "
        "FROM [Employees].[dbo].[EmployeeFullNames];"
    )
    params_select = []
    param_info_list_select = []
    result_select = ddbc_sql_execute(
        stmt_handle, select_sql_query, params_select, param_info_list_select, is_stmt_prepared_select, False
    )
    print("DDBCSQLExecute result:", result_select)

    print("Fetching Data for DDBCSQLExecute!")
    column_names = describe_columns(stmt_handle)
    print(column_names)
    ret_fetch = 1
    while ret_fetch != SQL_NO_DATA:
        if column_names:
            rows = fetch_data_all(stmt_handle)
            for row in rows:
                print(row)
        else:
            print("No columns to fetch data from.")
        ret_fetch = ddbc_bindings.DDBCSQLMoreResults(stmt_handle)

    # Free the statement handle
    free_handle(SQL_HANDLE_STMT, stmt_handle)
    # Disconnect from the data source
    result_disconnect = ddbc_bindings.DDBCSQLDisconnect(dbc_handle)
    if result_disconnect < 0:
        print(
            "Error: ",
            ddbc_bindings.DDBCSQLCheckError(SQL_HANDLE_DBC, dbc_handle, result_disconnect),
        )
        raise RuntimeError(
            f"Failed to disconnect from the data source. Error code: {result_disconnect}"
        )

    # Free the connection handle
    free_handle(SQL_HANDLE_DBC, dbc_handle)

    # Free the environment handle
    free_handle(SQL_HANDLE_ENV, env_handle)

    print("Done!")
