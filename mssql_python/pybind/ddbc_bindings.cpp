#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/complex.h>
#include<pybind11/functional.h>
#include <pybind11/chrono.h>
#include <windows.h>
#include <sqlext.h>
#include <sql.h>
#include <iostream>
#include <string>
#include <iomanip> // For std::setw and std::setfill
using namespace std;

namespace py = pybind11;

// Struct to hold parameter information for binding. Used by SQLBindParameter.
struct ParamInfo {
    SQLSMALLINT paramCType;
    SQLSMALLINT paramSQLType;
};

// TODO: Handle both UTF-8 and UTF-16 strings
#ifndef NDEBUG
#define DEBUG_LOG(formatString, ...) \
	do { \
		printf(formatString, __VA_ARGS__); \
		printf("\n"); \
	} while (0)
#else
#define DEBUG_LOG(x) (void)0
#endif

// Function pointer typedefs for the Handle APIs
typedef SQLRETURN (*SQLAllocHandleFunc)(SQLSMALLINT, SQLHANDLE, SQLHANDLE*);
typedef SQLRETURN (*SQLSetEnvAttrFunc)(SQLHANDLE, SQLINTEGER, SQLPOINTER, SQLINTEGER);
typedef SQLRETURN (*SQLSetConnectAttrFunc)(SQLHDBC, SQLINTEGER, SQLPOINTER, SQLINTEGER);
typedef SQLRETURN (*SQLSetStmtAttrFunc)(SQLHSTMT, SQLINTEGER, SQLPOINTER, SQLINTEGER);
typedef SQLRETURN (*SQLGetConnectAttrFunc)(SQLHDBC, SQLINTEGER, SQLPOINTER, SQLINTEGER, SQLINTEGER*);

// Connection and statement function typedefs
typedef SQLRETURN (*SQLDriverConnectFunc)(SQLHANDLE, SQLHWND, SQLWCHAR*, SQLSMALLINT, SQLWCHAR*, SQLSMALLINT, SQLSMALLINT*, SQLUSMALLINT);
typedef SQLRETURN (*SQLExecDirectFunc)(SQLHANDLE, SQLWCHAR*, SQLINTEGER);
typedef SQLRETURN (*SQLPrepareFunc)(SQLHANDLE, SQLWCHAR*, SQLINTEGER);
typedef SQLRETURN (*SQLBindParameterFunc)(SQLHANDLE, SQLUSMALLINT, SQLSMALLINT, SQLSMALLINT,
    SQLSMALLINT, SQLULEN, SQLSMALLINT, SQLPOINTER, SQLLEN, SQLLEN*);
typedef SQLRETURN (*SQLExecuteFunc)(SQLHANDLE);

// Fetch and data retrieval function typedefs
typedef SQLRETURN (*SQLFetchFunc)(SQLHANDLE);
typedef SQLRETURN (*SQLGetDataFunc)(SQLHANDLE, SQLUSMALLINT, SQLSMALLINT, SQLPOINTER, SQLLEN, SQLLEN*);
typedef SQLRETURN (*SQLNumResultColsFunc)(SQLHSTMT, SQLSMALLINT*);
typedef SQLRETURN (*SQLBindColFunc)(SQLHSTMT, SQLUSMALLINT, SQLSMALLINT, SQLPOINTER, SQLLEN, SQLLEN*);
typedef SQLRETURN (*SQLDescribeColFunc)(SQLHSTMT, SQLUSMALLINT, SQLWCHAR*, SQLSMALLINT, SQLSMALLINT*, SQLSMALLINT*, SQLULEN*, SQLSMALLINT*, SQLSMALLINT*);
typedef SQLRETURN (*SQLMoreResultsFunc)(SQLHSTMT);
typedef SQLRETURN (*SQLColAttributeFunc)(SQLHSTMT, SQLUSMALLINT, SQLUSMALLINT, SQLPOINTER, SQLSMALLINT, SQLSMALLINT*, SQLPOINTER);

// Transaction function typedefs
typedef SQLRETURN (*SQLEndTranFunc)(SQLSMALLINT, SQLHANDLE, SQLSMALLINT);

// Free handles, disconnect and free function typedefs
typedef SQLRETURN (*SQLFreeHandleFunc)(SQLSMALLINT, SQLHANDLE);
typedef SQLRETURN (*SQLDisconnectFunc)(SQLHDBC);


// Diagnostic record function typedef
typedef SQLRETURN (*SQLGetDiagRecFunc)(SQLSMALLINT, SQLHANDLE, SQLSMALLINT, SQLWCHAR*, SQLINTEGER*, SQLWCHAR*, SQLSMALLINT, SQLSMALLINT*);

// Function pointers for the handles functions pointers
SQLAllocHandleFunc SQLAllocHandle_ptr = nullptr;
SQLSetEnvAttrFunc SQLSetEnvAttr_ptr = nullptr;
SQLSetConnectAttrFunc SQLSetConnectAttr_ptr = nullptr;
SQLSetStmtAttrFunc SQLSetStmtAttr_ptr = nullptr;
SQLGetConnectAttrFunc SQLGetConnectAttr_ptr = nullptr;

// Connection and statement function pointer
SQLDriverConnectFunc SQLDriverConnect_ptr = nullptr;
SQLExecDirectFunc SQLExecDirect_ptr = nullptr;
SQLPrepareFunc SQLPrepare_ptr = nullptr;
SQLBindParameterFunc SQLBindParameter_ptr = nullptr;
SQLExecuteFunc SQLExecute_ptr = nullptr;

// Fetch and data retrieval function pointer
SQLFetchFunc SQLFetch_ptr = nullptr;
SQLGetDataFunc SQLGetData_ptr = nullptr;
SQLNumResultColsFunc SQLNumResultCols_ptr = nullptr;
SQLBindColFunc SQLBindCol_ptr = nullptr;
SQLDescribeColFunc SQLDescribeCol_ptr = nullptr;
SQLMoreResultsFunc SQLMoreResults_ptr = nullptr;
SQLColAttributeFunc SQLColAttribute_ptr = nullptr;

// Transaction function pointers
SQLEndTranFunc SQLEndTran_ptr = nullptr;

// Free handles, disconnect and free function pointer
SQLFreeHandleFunc SQLFreeHandle_ptr = nullptr;
SQLDisconnectFunc SQLDisconnect_ptr = nullptr;

// Diagnostic record function pointer
SQLGetDiagRecFunc SQLGetDiagRec_ptr = nullptr;

// Helper to load the driver
bool LoadDriver() {
    // Get the DLL directory to the current directory
    wchar_t currentDir[MAX_PATH];
    GetCurrentDirectoryW(MAX_PATH, currentDir);
    std::wstring dllDir = std::wstring(currentDir) + L"\\DLLs\\msodbcsql18.dll";

    // Load the DLL from the specified path
    HMODULE hModule = LoadLibraryW(dllDir.c_str());

    if (!hModule) {
        std::cerr << "Failed to load driver." << std::endl;
        return false;
    }
    
    // Environment and Handle function Loading
    SQLAllocHandle_ptr = (SQLAllocHandleFunc)GetProcAddress(hModule, "SQLAllocHandle");
    SQLSetEnvAttr_ptr = (SQLSetEnvAttrFunc)GetProcAddress(hModule, "SQLSetEnvAttr");
    SQLSetConnectAttr_ptr = (SQLSetConnectAttrFunc)GetProcAddress(hModule, "SQLSetConnectAttrW");
    SQLSetStmtAttr_ptr = (SQLSetStmtAttrFunc)GetProcAddress(hModule, "SQLSetStmtAttrW");
    SQLGetConnectAttr_ptr = (SQLGetConnectAttrFunc)GetProcAddress(hModule, "SQLGetConnectAttrW");

    // Connection and statement function Loading
    SQLDriverConnect_ptr = (SQLDriverConnectFunc)GetProcAddress(hModule, "SQLDriverConnectW");
    SQLExecDirect_ptr = (SQLExecDirectFunc)GetProcAddress(hModule, "SQLExecDirectW");
    SQLPrepare_ptr = (SQLPrepareFunc)GetProcAddress(hModule, "SQLPrepareW");
    SQLBindParameter_ptr = (SQLBindParameterFunc)GetProcAddress(hModule, "SQLBindParameter");
    SQLExecute_ptr = (SQLExecuteFunc)GetProcAddress(hModule, "SQLExecute");

    // Fetch and data retrieval function Loading
    SQLFetch_ptr = (SQLFetchFunc)GetProcAddress(hModule, "SQLFetch");
    SQLGetData_ptr = (SQLGetDataFunc)GetProcAddress(hModule, "SQLGetData");
    SQLNumResultCols_ptr = (SQLNumResultColsFunc)GetProcAddress(hModule, "SQLNumResultCols");
    SQLBindCol_ptr = (SQLBindColFunc)GetProcAddress(hModule, "SQLBindCol");
    SQLDescribeCol_ptr = (SQLDescribeColFunc)GetProcAddress(hModule, "SQLDescribeColW");
    SQLMoreResults_ptr = (SQLMoreResultsFunc)GetProcAddress(hModule, "SQLMoreResults");
    SQLColAttribute_ptr = (SQLColAttributeFunc)GetProcAddress(hModule, "SQLColAttributeW");

    // Transaction functions loading
    SQLEndTran_ptr = (SQLEndTranFunc)GetProcAddress(hModule, "SQLEndTran");

    // Free handles, disconnect and free function Loading
    SQLFreeHandle_ptr = (SQLFreeHandleFunc)GetProcAddress(hModule, "SQLFreeHandle");
    SQLDisconnect_ptr = (SQLDisconnectFunc)GetProcAddress(hModule, "SQLDisconnect");

    // Diagnostic record function Loading
    SQLGetDiagRec_ptr = (SQLGetDiagRecFunc)GetProcAddress(hModule, "SQLGetDiagRecW");

    #ifdef _DEBUG
        cout << "Driver loaded successfully." << std::endl;
    #endif

    return SQLAllocHandle_ptr && SQLSetEnvAttr_ptr && SQLSetConnectAttr_ptr && SQLSetStmtAttr_ptr && SQLGetConnectAttr_ptr
    && SQLDriverConnect_ptr && SQLExecDirect_ptr && SQLPrepare_ptr && SQLBindParameter_ptr && SQLExecute_ptr
    && SQLFetch_ptr && SQLGetData_ptr && SQLNumResultCols_ptr && SQLBindCol_ptr && SQLDescribeCol_ptr && SQLMoreResults_ptr
    && SQLColAttribute_ptr && SQLEndTran_ptr && SQLFreeHandle_ptr && SQLDisconnect_ptr
    && SQLGetDiagRec_ptr;
}



// Wrap SQLAllocHandle
SQLRETURN SQLAllocHandle_wrap(SQLSMALLINT HandleType, intptr_t InputHandle, intptr_t OutputHandle) {
    if (!SQLAllocHandle_ptr && !LoadDriver()) {
        return SQL_ERROR;
    }

    #ifdef _DEBUG
        cout << "Allocate SQL Handle" << std::endl;
    #endif

    SQLHANDLE* pOutputHandle = reinterpret_cast<SQLHANDLE*>(OutputHandle);
    return SQLAllocHandle_ptr(HandleType, reinterpret_cast<SQLHANDLE>(InputHandle), pOutputHandle);
}

// Wrap SQLSetEnvAttr
SQLRETURN SQLSetEnvAttr_wrap(intptr_t EnvHandle, SQLINTEGER Attribute, intptr_t ValuePtr, SQLINTEGER StringLength) {
    if (!SQLSetEnvAttr_ptr && !LoadDriver()) {
        return SQL_ERROR;
    }

    #ifdef _DEBUG
        cout << "Set SQL environment Attribute" << std::endl;
    #endif

    return SQLSetEnvAttr_ptr(reinterpret_cast<SQLHANDLE>(EnvHandle), Attribute, reinterpret_cast<SQLPOINTER>(ValuePtr), StringLength);
}

// Wrap SQLSetConnectAttr
SQLRETURN SQLSetConnectAttr_wrap(intptr_t ConnectionHandle, SQLINTEGER Attribute, intptr_t ValuePtr, SQLINTEGER StringLength) {
    if (!SQLSetConnectAttr_ptr && !LoadDriver()) {
        return SQL_ERROR;
    }

    #ifdef _DEBUG
        cout << "Set SQL Connection Attribute" << std::endl;
    #endif

    return SQLSetConnectAttr_ptr(reinterpret_cast<SQLHDBC>(ConnectionHandle), Attribute, reinterpret_cast<SQLPOINTER>(ValuePtr), StringLength);
}

// Wrap SQLSetStmtAttr
SQLRETURN SQLSetStmtAttr_wrap(intptr_t ConnectionHandle, SQLINTEGER Attribute, intptr_t ValuePtr, SQLINTEGER StringLength) {
    if (!SQLSetConnectAttr_ptr && !LoadDriver()) {
        return SQL_ERROR;
    }

    #ifdef _DEBUG
        cout << "Set SQL Statement Attribute" << std::endl;
    #endif
    return SQLSetStmtAttr_ptr(reinterpret_cast<SQLHSTMT>(ConnectionHandle), Attribute, reinterpret_cast<SQLPOINTER>(ValuePtr), StringLength);
}

SQLINTEGER SQLGetConnectionAttr_wrap(intptr_t ConnectionHandle, SQLINTEGER attribute) {
    if (!SQLGetConnectAttr_ptr && !LoadDriver()) {
        return -1;
    }

    SQLINTEGER stringLength;
    SQLINTEGER intValue;

    #ifdef _DEBUG
        cout << "Set SQL Statement Attribute" << std::endl;
    #endif

    // Try to get the attribute as an integer
   SQLGetConnectAttr_ptr(
        reinterpret_cast<SQLHANDLE>(ConnectionHandle),
        attribute, 
        &intValue, 
        sizeof(SQLINTEGER),
        &stringLength
    );
    return intValue;
}

// Helper function to check for driver errors
std::wstring SQLCheckError_Wrap(SQLSMALLINT handleType, intptr_t handle, SQLRETURN retcode) {
    // TODO: Add check for when handle is a nullptr0?
    if (!SQL_SUCCEEDED(retcode)) {
        if (!SQLGetDiagRec_ptr && !LoadDriver()) {
            std::cerr << "Failed to load SQLGetDiagRecW function." << std::endl;
            exit(-1);
        }

        SQLWCHAR sqlState[6], message[SQL_MAX_MESSAGE_LENGTH];
        SQLINTEGER nativeError;
        SQLSMALLINT messageLen;

        SQLRETURN diagReturn = SQLGetDiagRec_ptr(handleType, reinterpret_cast<SQLHANDLE>(handle), 1, sqlState, &nativeError, message, SQL_MAX_MESSAGE_LENGTH, &messageLen);

        if (SQL_SUCCEEDED(diagReturn)) {
            return std::wstring(message); // Return by value, not by reference
        }
    }
    return L""; // Return an empty string if no error
}

// Wrap SQLDriverConnect
SQLRETURN SQLDriverConnect_wrap(
    intptr_t ConnectionHandle, 
    intptr_t WindowHandle, 
    const std::wstring& ConnectionString
) 
{

    #ifdef _DEBUG
        std::cout << "Driver Connect to MSSQL" << std::endl;
    #endif

    if (!SQLDriverConnect_ptr && !LoadDriver()) {
        return SQL_ERROR;
    }
    return SQLDriverConnect_ptr(
        reinterpret_cast<SQLHANDLE>(ConnectionHandle),
        reinterpret_cast<SQLHWND>(WindowHandle),
        const_cast<SQLWCHAR*>(ConnectionString.c_str()),
        SQL_NTS, nullptr, 0, nullptr, SQL_DRIVER_NOPROMPT);
}

// Wrap SQLExecDirect
SQLRETURN SQLExecDirect_wrap(intptr_t StatementHandle, const std::wstring& Query) {
     if (!SQLExecDirect_ptr && !LoadDriver()) {
        return SQL_ERROR;
    }

    #ifdef _DEBUG
        cout << "Execute SQL Query" << std::endl;
    #endif

    return SQLExecDirect_ptr(reinterpret_cast<SQLHANDLE>(StatementHandle),
                              const_cast<SQLWCHAR*>(Query.c_str()), SQL_NTS);
}

// Executes the provided query. If the query is parametrized, it prepares the statement and
// binds the parameters. Otherwise, it executes the query directly.
// usePrepare parameter can be used to disable the prepare step for queries that might already
// be prepared in a previous call.
SQLRETURN SQLExecute_wrap(const intptr_t statementHandle, const std::wstring& query,
    const py::list& params, const std::vector<ParamInfo>& paramInfo,
    const bool usePrepare = true)
{
    if (!SQLPrepare_ptr && !LoadDriver()) {
		// TODO: Error needs to be relayed to application via exception
		std::cout << "DDBCSQLExecute: Could not load ODBC library" << std::endl;
        return SQL_ERROR;
    }
    assert(SQLPrepare_ptr && SQLBindParameter_ptr && SQLExecute_ptr && SQLExecDirect_ptr);

	if (params.size() != paramInfo.size()) {
		// TODO: Error needs to be relayed to application via exception
		std::cout << "DDBCSQLExecute: Number of parameters and paramInfo do not match." << std::endl;
		return SQL_ERROR;
	}

	DEBUG_LOG("DDBCSQLExecute: Executing SQL Query - %ls", query.c_str());
 
    RETCODE rc;
    SQLHANDLE hStmt = reinterpret_cast<SQLHANDLE>(statementHandle);
    SQLWCHAR* queryPtr = const_cast<SQLWCHAR*>(query.c_str());
    if (params.size() == 0) {
        // Execute statement directly if the statement is not parametrized. This is the
        // fastest way to submit a SQL statement for one-time execution according to
        // ODBC documentation - https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlexecdirect-function?view=sql-server-ver16
        rc = SQLExecDirect_ptr(hStmt, queryPtr, SQL_NTS);
        if (!SQL_SUCCEEDED(rc)) {
			DEBUG_LOG("DDBCSQLExecute: Error during direct execution of the statement");
        }
		return rc;
    }
    else {
        if (usePrepare) {
            rc = SQLPrepare_ptr(hStmt, queryPtr, SQL_NTS);
            if (!SQL_SUCCEEDED(rc)) {
				DEBUG_LOG("DDBCSQLExecute: Error while preparing the statement");
                return rc;
            }
        }

		// This vector manages the heap memory allocated for parameter buffers
		std::vector<std::shared_ptr<void>> paramBuffers;
        for (int paramIndex = 0; paramIndex < params.size(); paramIndex++) {
			const auto& param = params[paramIndex];
			SQLULEN columnSize = 0;
            void* dataPtr = nullptr;
            // TODO: Add more data types like wide string, date time, TVPs etc.
            if (py::isinstance<py::str>(param) || py::isinstance<py::bytearray>(param) ||
                py::isinstance<py::bytes>(param)) {
                // TODO: Use wide string?
                paramBuffers.push_back(std::shared_ptr<void>(new std::string(param.cast<std::string>()),
                    std::default_delete<std::string>()));
                const string& strParam = *static_cast<std::string*>(paramBuffers.back().get());
                dataPtr = const_cast<void*>(static_cast<const void*>(strParam.c_str()));
                columnSize = strParam.size();
            }
            else if (py::isinstance<py::int_>(param)) {
				// TODO: Integers in python are mostly longs. Avoid narrowing conversions here
				paramBuffers.push_back(std::shared_ptr<void>(new int(param.cast<int>()),
                                                             std::default_delete<int>()));
				dataPtr = paramBuffers.back().get();
			}
			else if (py::isinstance<py::float_>(param)) {
				paramBuffers.push_back(std::shared_ptr<void>(new float(param.cast<float>()),
                                                             std::default_delete<float>()));
				dataPtr = paramBuffers.back().get();
			}
			else if (py::isinstance<py::bool_>(param)) {
				paramBuffers.push_back(std::shared_ptr<void>(new bool(param.cast<bool>()),
                                                             std::default_delete<bool>()));
				dataPtr = paramBuffers.back().get();
			}
            else {
				// TODO: Error needs to be relayed to application via exception
                std::cout << "DDBCSQLExecute: Unsupported parameter type for parameter - " << paramIndex << std::endl;
            }
            rc = SQLBindParameter_ptr(hStmt, paramIndex + 1 /* 1-based indexing */,
                     SQL_PARAM_INPUT /* TODO: Handle other types of parameters */,
                     paramInfo[paramIndex].paramCType,
                     paramInfo[paramIndex].paramSQLType,
                     columnSize /* Column size */,
                     0, dataPtr, 0, nullptr);
            if (!SQL_SUCCEEDED(rc)) {
				DEBUG_LOG("DDBCSQLExecute: Error when binding parameter - %d", paramIndex);
				return rc;
			}
        }

        rc = SQLExecute_ptr(hStmt);
        if (!SQL_SUCCEEDED(rc)) {
			DEBUG_LOG("DDBCSQLExecute: Error during execution of the statement");
        }
		return rc;
    }
}

// Wrap SQLNumResultCols
SQLSMALLINT SQLNumResultCols_wrap(intptr_t StatementHandle) {
    if (!SQLNumResultCols_ptr && !LoadDriver()) {
        return SQL_ERROR;
    }

    #ifdef _DEBUG
        cout << "Get Number of Columns" << std::endl;
    #endif

    SQLSMALLINT* ColumnCount = new SQLSMALLINT;
    SQLNumResultCols_ptr(reinterpret_cast<SQLHSTMT>(StatementHandle), ColumnCount);
    return *ColumnCount;
}

// Wrap SQLDescribeCol
SQLRETURN SQLDescribeCol_wrap(
    intptr_t StatementHandle, 
    py::list& ColumnNames // Accept a Python list reference
) 
{
    if (!SQLDescribeCol_ptr && !LoadDriver()) {
        return SQL_ERROR;
    }

    // Get the number of columns in the result set
    SQLSMALLINT ColumnCount;
    SQLRETURN retcode = SQLNumResultCols_ptr(reinterpret_cast<SQLHSTMT>(StatementHandle), &ColumnCount);
    if (retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO) {
        return retcode;
    }

    #ifdef _DEBUG
        cout << "Get Columns name\n";
    #endif

    for (SQLUSMALLINT i = 1; i <= ColumnCount; ++i) {
        SQLWCHAR ColumnName[256];
        SQLSMALLINT NameLength;
        SQLSMALLINT DataType;
        SQLULEN ColumnSize;
        SQLSMALLINT DecimalDigits;
        SQLSMALLINT Nullable;

        retcode = SQLDescribeCol_ptr(
            reinterpret_cast<SQLHSTMT>(StatementHandle),
            i, ColumnName, sizeof(ColumnName) / sizeof(SQLWCHAR),
            &NameLength, &DataType, &ColumnSize, &DecimalDigits, &Nullable);

        if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO) {
            ColumnNames.append(py::cast(std::wstring(ColumnName)));
        } else {
            return retcode;
        }
    }
    return SQL_SUCCESS;
}

// Wrap SQLFetch to retrieve rows
SQLRETURN SQLFetch_wrap(intptr_t StatementHandle) {
    if (!SQLFetch_ptr && !LoadDriver()) {
        return SQL_ERROR;
    }

    #ifdef _DEBUG
        cout << "Fetch next row\n";
    #endif

    return SQLFetch_ptr(reinterpret_cast<SQLHANDLE>(StatementHandle));
}

// Helper function to retrieve column data
SQLRETURN SQLGetData_wrap(intptr_t StatementHandle, SQLUSMALLINT colCount, py::list& row) {
    if (!SQLGetData_ptr && !LoadDriver()) {
        return SQL_ERROR;
    }
    #ifdef _DEBUG
        cout << "Get data from columns\n";
    #endif

    SQLRETURN ret;

    SQLHSTMT hStmt = reinterpret_cast<SQLHSTMT>(StatementHandle);
    for (SQLSMALLINT i = 1; i <= colCount; ++i) {
            SQLWCHAR columnName[256];
            SQLSMALLINT columnNameLen;
            SQLSMALLINT dataType;
            SQLULEN columnSize;
            SQLSMALLINT decimalDigits;
            SQLSMALLINT nullable;

            ret = SQLDescribeCol_ptr(hStmt, i, columnName, sizeof(columnName) / sizeof(SQLWCHAR), &columnNameLen, &dataType, &columnSize, &decimalDigits, &nullable);
            if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
                std::wcerr << L"Error retrieving data for column: " << columnName << L" Type: " << dataType << L" Error code: " << ret << std::endl;
                row.append(py::none());
                continue;
            }


            switch (dataType) {
                case SQL_WCHAR:    // NVARCHAR
                case SQL_WVARCHAR: // VARCHAR
                case SQL_CHAR:     // CHAR
                case SQL_VARCHAR: {
                    SQLWCHAR dataBuffer[256];
                    SQLLEN dataLen;
                    ret = SQLGetData_ptr(hStmt, i, SQL_C_WCHAR, dataBuffer, sizeof(dataBuffer), &dataLen);
                    
                    if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
                        if (dataLen > 0 && dataLen < sizeof(dataBuffer)) {
                            dataBuffer[dataLen / sizeof(SQLWCHAR)] = L'\0'; // Null-terminate
                        }
                        row.append(std::wstring(dataBuffer)); // Append to row
                    } else {
                        row.append(py::none());
                    }
                    break;
                }
                case SQL_INTEGER: {  // For integer types
                    SQLINTEGER intValue;
                    ret = SQLGetData_ptr(hStmt, i, SQL_C_LONG, &intValue, 0, NULL);
                    if (ret == SQL_SUCCESS) {
                        row.append(static_cast<int>(intValue));
                    } else {
                        row.append(py::none());
                    }
                    break;
                }
                case SQL_SMALLINT: {  // For small integer types
                    SQLSMALLINT smallIntValue;
                    ret = SQLGetData_ptr(hStmt, i, SQL_C_SHORT, &smallIntValue, 0, NULL);
                    if (ret == SQL_SUCCESS) {
                        row.append(static_cast<int>(smallIntValue));
                    } else {
                        row.append(py::none());
                    }
                    break;
                }
                case SQL_FLOAT:
                case SQL_REAL:
                case SQL_DECIMAL: {  // For float and real types
                    SQLREAL floatValue;
                    ret = SQLGetData_ptr(hStmt, i, SQL_C_FLOAT, &floatValue, 0, NULL);
                    if (ret == SQL_SUCCESS) {
                        row.append(static_cast<float>(floatValue));
                    } else {
                        row.append(py::none());
                    }
                    break;
                }
                case SQL_DOUBLE: {  // For double types
                    SQLDOUBLE doubleValue;
                    ret = SQLGetData_ptr(hStmt, i, SQL_C_DOUBLE, &doubleValue, 0, NULL);
                    if (ret == SQL_SUCCESS) {
                        row.append(static_cast<double>(doubleValue));
                    } else {
                        row.append(py::none());
                    }
                    break;
                }
                case SQL_DATETIME: {  // For datetime types
                    SQL_TIMESTAMP_STRUCT timestampValue;
                    ret = SQLGetData_ptr(hStmt, i, SQL_C_TYPE_TIMESTAMP, &timestampValue, sizeof(timestampValue), NULL);
                    if (ret == SQL_SUCCESS) {
                        row.append(py::make_tuple(timestampValue.year, timestampValue.month, timestampValue.day,
                                                   timestampValue.hour, timestampValue.minute, timestampValue.second));
                    } else {
                        row.append(py::none());
                    }
                    break;
                }
                case SQL_BIGINT: { // BIGINT
                    SQLBIGINT bigintValue;
                    ret = SQLGetData_ptr(hStmt, i, SQL_C_SBIGINT, &bigintValue, 0, NULL);
                    if (ret == SQL_SUCCESS) {
                        row.append(static_cast<long long>(bigintValue));
                    } else {
                        row.append(py::none());
                    }
                    break;
                }
                case SQL_TYPE_DATE: { // DATE
                    SQL_DATE_STRUCT dateValue;
                    ret = SQLGetData_ptr(hStmt, i, SQL_C_TYPE_DATE, &dateValue, sizeof(dateValue), NULL);
                    if (ret == SQL_SUCCESS) {
                        row.append(py::make_tuple(dateValue.year, dateValue.month, dateValue.day));
                    } else {
                        row.append(py::none());
                    }
                    break;
                }
                case SQL_TIME: { // TIME
                    SQL_TIME_STRUCT timeValue;
                    ret = SQLGetData_ptr(hStmt, i, SQL_C_TYPE_TIME, &timeValue, sizeof(timeValue), NULL);
                    if (ret == SQL_SUCCESS) {
                        row.append(py::make_tuple(timeValue.hour, timeValue.minute, timeValue.second));
                    } else {
                        row.append(py::none());
                    }
                    break;
                }
                case SQL_TIMESTAMP: { // TIMESTAMP
                    SQL_TIMESTAMP_STRUCT timestampValue;
                    ret = SQLGetData_ptr(hStmt, i, SQL_C_TYPE_TIMESTAMP, &timestampValue, sizeof(timestampValue), NULL);
                    if (ret == SQL_SUCCESS) {
                        row.append(py::make_tuple(timestampValue.year, timestampValue.month, timestampValue.day,
                                                   timestampValue.hour, timestampValue.minute, timestampValue.second));
                    } else {
                        row.append(py::none());
                    }
                    break;
                }
                case SQL_LONGVARCHAR: { // LONGVARCHAR
                    SQLWCHAR *dataBuffer = new SQLWCHAR[columnSize / sizeof(SQLWCHAR) + 1];
                    SQLLEN dataLen;
                    ret = SQLGetData_ptr(hStmt, i, SQL_C_WCHAR, dataBuffer, columnSize, &dataLen);
                    
                    if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
                        dataBuffer[dataLen / sizeof(SQLWCHAR)] = L'\0'; // Null-terminate
                        row.append(std::wstring(dataBuffer)); // Append to row
                    } else {
                        row.append(py::none());
                    }
                    delete[] dataBuffer;
                    break;
                }
                case SQL_BINARY: { // BINARY
                    SQLCHAR *dataBuffer = new SQLCHAR[columnSize + 1];
                    SQLLEN dataLen;
                    ret = SQLGetData_ptr(hStmt, i, SQL_C_BINARY, dataBuffer, columnSize, &dataLen);
                    
                    if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
                        dataBuffer[dataLen] = '\0'; // Null-terminate
                        // Handle binary data as needed (e.g., converting to a hex string)
                        row.append(py::bytes(reinterpret_cast<const char*>(dataBuffer), dataLen));
                    } else {
                        row.append(py::none());
                    }
                    delete[] dataBuffer;
                    break;
                }
                case SQL_VARBINARY: { // VARBINARY
                    SQLCHAR *dataBuffer = new SQLCHAR[columnSize + 1];
                    SQLLEN dataLen;
                    ret = SQLGetData_ptr(hStmt, i, SQL_C_BINARY, dataBuffer, columnSize, &dataLen);
                    
                    if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
                        dataBuffer[dataLen] = '\0'; // Null-terminate
                        row.append(py::bytes(reinterpret_cast<const char*>(dataBuffer), dataLen));
                    } else {
                        row.append(py::none());
                    }
                    delete[] dataBuffer;
                    break;
                }
                case SQL_LONGVARBINARY: { // LONGVARBINARY
                    SQLCHAR *dataBuffer = new SQLCHAR[columnSize + 1];
                    SQLLEN dataLen;
                    ret = SQLGetData_ptr(hStmt, i, SQL_C_BINARY, dataBuffer, columnSize, &dataLen);
                    
                    if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
                        dataBuffer[dataLen] = '\0'; // Null-terminate
                        row.append(py::bytes(reinterpret_cast<const char*>(dataBuffer), dataLen));
                    } else {
                        row.append(py::none());
                    }
                    delete[] dataBuffer;
                    break;
                }
                case SQL_TINYINT: { // TINYINT
                    SQLCHAR tinyIntValue;
                    ret = SQLGetData_ptr(hStmt, i, SQL_C_STINYINT, &tinyIntValue, 0, NULL);
                    if (ret == SQL_SUCCESS) {
                        row.append(static_cast<int>(tinyIntValue));
                    } else {
                        row.append(py::none());
                    }
                    break;
                }
                case SQL_BIT: { // BIT
                    SQLCHAR bitValue;
                    ret = SQLGetData_ptr(hStmt, i, SQL_C_BIT, &bitValue, 0, NULL);
                    if (ret == SQL_SUCCESS) {
                        row.append(static_cast<bool>(bitValue));
                    } else {
                        row.append(py::none());
                    }
                    break;
                }
#if (ODBCVER >= 0x0350)
                case SQL_GUID: { // GUID
                    SQLGUID guidValue;
                    ret = SQLGetData_ptr(hStmt, i, SQL_C_GUID, &guidValue, sizeof(guidValue), NULL);
                    if (ret == SQL_SUCCESS) {
                        std::ostringstream oss;
                        oss << std::hex << std::setfill('0')
                            << std::setw(8) << guidValue.Data1 << '-'
                            << std::setw(4) << guidValue.Data2 << '-'
                            << std::setw(4) << guidValue.Data3 << '-'
                            << std::setw(2) << static_cast<int>(guidValue.Data4[0]) << std::setw(2) << static_cast<int>(guidValue.Data4[1]) << '-'
                            << std::hex << std::setw(2) << static_cast<int>(guidValue.Data4[2]) << std::setw(2) << static_cast<int>(guidValue.Data4[3])
                            << std::setw(2) << static_cast<int>(guidValue.Data4[4]) << std::setw(2) << static_cast<int>(guidValue.Data4[5])
                            << std::setw(2) << static_cast<int>(guidValue.Data4[6]) << std::setw(2) << static_cast<int>(guidValue.Data4[7]);
                        row.append(oss.str()); // Append GUID as a string
                    } else {
                        row.append(py::none());
                    }
                    break;
                }
#endif
                default:
                    std::wcerr << L"Unsupported data type for column: " << columnName << L" Type: " << dataType << std::endl;
                    row.append(py::none());  // Append None for unsupported types
                    break;
            }
        }
        return ret;
}

SQLRETURN FetchOne_wrap(intptr_t StatementHandle, py::list& row) {
    SQLRETURN ret;
    SQLHSTMT hStmt = reinterpret_cast<SQLHSTMT>(StatementHandle);

    // Assume hStmt is already allocated and a query has been executed
    ret = SQLFetch_ptr(hStmt);
    if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
        // Retrieve column count
        SQLSMALLINT colCount =  SQLNumResultCols_wrap(StatementHandle);
        ret = SQLGetData_wrap(StatementHandle, colCount, row);
        return ret;
    } else if (ret == SQL_NO_DATA) {
        return SQL_NO_DATA;  // Return empty list if no more rows
    } else {
        throw std::runtime_error("Error fetching data.");
    }
}

// Wrap SQLMoreResults
SQLRETURN SQLMoreResults_wrap(intptr_t StatementHandle) {
    if (!SQLMoreResults_ptr && !LoadDriver()) {
        return SQL_ERROR;
    }

    #ifdef _DEBUG
        cout << "Check for more results\n";
    #endif

    return SQLMoreResults_ptr(reinterpret_cast<SQLHANDLE>(StatementHandle));
}

// Wrap SQLEndTran
SQLRETURN SQLEndTran_wrap(SQLSMALLINT HandleType, intptr_t Handle, SQLSMALLINT CompletionType) {
    if (!SQLEndTran_ptr && !LoadDriver()) {
        return SQL_ERROR;
    }

#ifdef _DEBUG
    cout << "End SQL Transaction" << std::endl;
#endif

    return SQLEndTran_ptr(HandleType, reinterpret_cast<SQLHANDLE>(Handle), CompletionType);
}

// Wrap SQLFreeHandle
SQLRETURN SQLFreeHandle_wrap(SQLSMALLINT HandleType, intptr_t Handle) {
    if (!SQLAllocHandle_ptr && !LoadDriver()) { // SQLAllocHandle_ptr ensures driver is loaded
        return SQL_ERROR;
    }

    #ifdef _DEBUG
        cout << "Free SQL Handle\n";
    #endif
    
    return SQLFreeHandle_ptr(HandleType, reinterpret_cast<SQLHANDLE>(Handle));
}

// Wrap SQLDisconnect
SQLRETURN SQLDisconnect_wrap(intptr_t ConnectionHandle) {
    if (!SQLDriverConnect_ptr && !LoadDriver()) { // Ensure the driver is loaded
        return SQL_ERROR;
    }

    #ifdef _DEBUG
        cout << "Disconnect from MSSQL\n";
    #endif
    return SQLDisconnect_ptr(reinterpret_cast<SQLHDBC>(ConnectionHandle));
}

// Bind the functions to the module
PYBIND11_MODULE(ddbc_bindings, m) {
    m.doc() = "msodbcsql driver api bindings for Python"; // optional module docstring

    py::class_<ParamInfo>(m, "ParamInfo")
        .def(py::init<>())
        .def_readwrite("paramCType", &ParamInfo::paramCType)
        .def_readwrite("paramSQLType", &ParamInfo::paramSQLType);

    m.def("DDBCSQLAllocHandle", &SQLAllocHandle_wrap, "Allocate an environment, connection, statement, or descriptor handle");
    m.def("DDBCSQLSetEnvAttr", &SQLSetEnvAttr_wrap, "Set an attribute that governs aspects of environments");
    m.def("DDBCSQLSetConnectAttr", &SQLSetConnectAttr_wrap, "Set an attribute that governs aspects of connections");
    m.def("DDBCSQLSetStmtAttr", &SQLSetStmtAttr_wrap, "Set an attribute that governs aspects of statements");
    m.def("DDBCSQLGetConnectionAttr", &SQLGetConnectionAttr_wrap, "Get an attribute that governs aspects of connections");
    m.def("DDBCSQLDriverConnect", &SQLDriverConnect_wrap, "Connect to a data source with a connection string");
    m.def("DDBCSQLExecDirect", &SQLExecDirect_wrap, "Execute a SQL query directly");
    m.def("DDBCSQLExecute", &SQLExecute_wrap, "Prepare and execute T-SQL statements");
    m.def("DDBCSQLFetch", &SQLFetch_wrap, "Fetch the next row from the result set");
    m.def("DDBCSQLNumResultCols", &SQLNumResultCols_wrap, "Get the number of columns in the result set");
    m.def("DDBCSQLDescribeCol", &SQLDescribeCol_wrap, "Get information about a column in the result set");
    m.def("DDBCSQLGetData", &SQLGetData_wrap, "Retrieve data from the result set");
    m.def("DDBCSQLMoreResults", &SQLMoreResults_wrap, "Check for more results in the result set");
    m.def("DDBCSQLFetchOne", &FetchOne_wrap, "Fetch one row from the result set");
    m.def("DDBCSQLEndTran", &SQLEndTran_wrap, "End a transaction");
    m.def("DDBCSQLFreeHandle", &SQLFreeHandle_wrap, "Free a handle");
    m.def("DDBCSQLDisconnect", &SQLDisconnect_wrap, "Disconnect from a data source");
    m.def("DDBCSQLCheckError", &SQLCheckError_Wrap, "Check for driver errors");
}
