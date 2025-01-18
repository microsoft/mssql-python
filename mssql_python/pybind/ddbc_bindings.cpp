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


// Function pointer typedefs for the Handle APIs
typedef SQLRETURN (*SQLAllocHandleFunc)(SQLSMALLINT, SQLHANDLE, SQLHANDLE*);
typedef SQLRETURN (*SQLSetEnvAttrFunc)(SQLHANDLE, SQLINTEGER, SQLPOINTER, SQLINTEGER);
typedef SQLRETURN (*SQLSetConnectAttrFunc)(SQLHDBC, SQLINTEGER, SQLPOINTER, SQLINTEGER);
typedef SQLRETURN (*SQLSetStmtAttrFunc)(SQLHSTMT, SQLINTEGER, SQLPOINTER, SQLINTEGER);

// Connection and statement function typedefs
typedef SQLRETURN (*SQLDriverConnectFunc)(SQLHANDLE, SQLHWND, SQLWCHAR*, SQLSMALLINT, SQLWCHAR*, SQLSMALLINT, SQLSMALLINT*, SQLUSMALLINT);
typedef SQLRETURN (*SQLExecDirectFunc)(SQLHANDLE, SQLWCHAR*, SQLINTEGER);

// Fetch and data retrieval function typedefs
typedef SQLRETURN (*SQLFetchFunc)(SQLHANDLE);
typedef SQLRETURN (*SQLGetDataFunc)(SQLHANDLE, SQLUSMALLINT, SQLSMALLINT, SQLPOINTER, SQLLEN, SQLLEN*);
typedef SQLRETURN (*SQLNumResultColsFunc)(SQLHSTMT, SQLSMALLINT*);
typedef SQLRETURN (*SQLDescribeColFunc)(SQLHSTMT, SQLUSMALLINT, SQLWCHAR*, SQLSMALLINT, SQLSMALLINT*, SQLSMALLINT*, SQLULEN*, SQLSMALLINT*, SQLSMALLINT*);
typedef SQLRETURN (*SQLMoreResultsFunc)(SQLHSTMT);
typedef SQLRETURN (*SQLColAttributeFunc)(SQLHSTMT, SQLUSMALLINT, SQLUSMALLINT, SQLPOINTER, SQLSMALLINT, SQLSMALLINT*, SQLPOINTER);

// Free handles, disconnect and free function typedefs
typedef SQLRETURN (*SQLFreeHandleFunc)(SQLSMALLINT, SQLHANDLE);
typedef SQLRETURN (*SQLDisconnectFunc)(SQLHDBC);

// Diagnostic record function typedef
typedef SQLRETURN (*SQLGetDiagRecFunc)(SQLSMALLINT, SQLHANDLE, SQLSMALLINT, SQLWCHAR*, SQLINTEGER*, SQLWCHAR*, SQLSMALLINT, SQLSMALLINT*);

// Additional function typedefs
typedef SQLRETURN (*SQLPrepareFunc)(SQLHSTMT, SQLWCHAR*, SQLINTEGER);
typedef SQLRETURN (*SQLExecuteFunc)(SQLHSTMT);
typedef SQLRETURN (*SQLEndTranFunc)(SQLSMALLINT, SQLHANDLE, SQLSMALLINT);
typedef SQLRETURN (*SQLBindColFunc)(SQLHSTMT, SQLUSMALLINT, SQLSMALLINT, SQLPOINTER, SQLLEN, SQLLEN*);
typedef SQLRETURN (*SQLBindParameterFunc)(SQLHSTMT, SQLUSMALLINT, SQLSMALLINT, SQLSMALLINT, SQLSMALLINT, SQLULEN, SQLSMALLINT, SQLPOINTER, SQLLEN, SQLLEN*);

// Function pointers for the handles functions pointers
SQLAllocHandleFunc SQLAllocHandle_ptr = nullptr;
SQLSetEnvAttrFunc SQLSetEnvAttr_ptr = nullptr;
SQLSetConnectAttrFunc SQLSetConnectAttr_ptr = nullptr;
SQLSetStmtAttrFunc SQLSetStmtAttr_ptr = nullptr;

// Connection and statement function pointer
SQLDriverConnectFunc SQLDriverConnect_ptr = nullptr;
SQLExecDirectFunc SQLExecDirect_ptr = nullptr;

// Fetch and data retrieval function pointer
SQLFetchFunc SQLFetch_ptr = nullptr;
SQLGetDataFunc SQLGetData_ptr = nullptr;
SQLNumResultColsFunc SQLNumResultCols_ptr = nullptr;
SQLDescribeColFunc SQLDescribeCol_ptr = nullptr;
SQLMoreResultsFunc SQLMoreResults_ptr = nullptr;
SQLColAttributeFunc SQLColAttribute_ptr = nullptr;

// Free handles, disconnect and free function pointer
SQLFreeHandleFunc SQLFreeHandle_ptr = nullptr;
SQLDisconnectFunc SQLDisconnect_ptr = nullptr;

// Diagnostic record function pointer
SQLGetDiagRecFunc SQLGetDiagRec_ptr = nullptr;

// Additional function pointers
SQLPrepareFunc SQLPrepare_ptr = nullptr;
SQLExecuteFunc SQLExecute_ptr = nullptr;
SQLEndTranFunc SQLEndTran_ptr = nullptr;
SQLBindColFunc SQLBindCol_ptr = nullptr;
SQLBindParameterFunc SQLBindParameter_ptr = nullptr;

// Helper to load the driver
bool LoadDriver() {
    // Set the DLL directory to the current directory
    wchar_t currentDir[MAX_PATH];
    GetCurrentDirectoryW(MAX_PATH, currentDir);
    std::wstring dllDir = std::wstring(currentDir) + L"\\DLLs";
    SetDllDirectoryW(dllDir.c_str());

    // Load the DLL from the specified path
    HMODULE hModule = LoadLibraryW(L"msodbcsql18.dll");

    if (!hModule) {
        std::cerr << "Failed to load driver." << std::endl;
        return false;
    }
    
    // Environment and Handle function Loading
    SQLAllocHandle_ptr = (SQLAllocHandleFunc)GetProcAddress(hModule, "SQLAllocHandle");
    SQLSetEnvAttr_ptr = (SQLSetEnvAttrFunc)GetProcAddress(hModule, "SQLSetEnvAttr");
    SQLSetConnectAttr_ptr = (SQLSetConnectAttrFunc)GetProcAddress(hModule, "SQLSetConnectAttrW");
    SQLSetStmtAttr_ptr = (SQLSetStmtAttrFunc)GetProcAddress(hModule, "SQLSetStmtAttrW");

    // Connection and statement function Loading
    SQLDriverConnect_ptr = (SQLDriverConnectFunc)GetProcAddress(hModule, "SQLDriverConnectW");
    SQLExecDirect_ptr = (SQLExecDirectFunc)GetProcAddress(hModule, "SQLExecDirectW");

    // Fetch and data retrieval function Loading
    SQLFetch_ptr = (SQLFetchFunc)GetProcAddress(hModule, "SQLFetch");
    SQLGetData_ptr = (SQLGetDataFunc)GetProcAddress(hModule, "SQLGetData");
    SQLNumResultCols_ptr = (SQLNumResultColsFunc)GetProcAddress(hModule, "SQLNumResultCols");
    SQLDescribeCol_ptr = (SQLDescribeColFunc)GetProcAddress(hModule, "SQLDescribeColW");
    SQLMoreResults_ptr = (SQLMoreResultsFunc)GetProcAddress(hModule, "SQLMoreResults");
    SQLColAttribute_ptr = (SQLColAttributeFunc)GetProcAddress(hModule, "SQLColAttributeW");

    // Free handles, disconnect and free function Loading
    SQLFreeHandle_ptr = (SQLFreeHandleFunc)GetProcAddress(hModule, "SQLFreeHandle");
    SQLDisconnect_ptr = (SQLDisconnectFunc)GetProcAddress(hModule, "SQLDisconnect");

    // Diagnostic record function Loading
    SQLGetDiagRec_ptr = (SQLGetDiagRecFunc)GetProcAddress(hModule, "SQLGetDiagRecW");

    // Load additional functions
    SQLPrepare_ptr = (SQLPrepareFunc)GetProcAddress(hModule, "SQLPrepareW");
    SQLExecute_ptr = (SQLExecuteFunc)GetProcAddress(hModule, "SQLExecute");
    SQLEndTran_ptr = (SQLEndTranFunc)GetProcAddress(hModule, "SQLEndTran");
    SQLBindCol_ptr = (SQLBindColFunc)GetProcAddress(hModule, "SQLBindCol");
    SQLBindParameter_ptr = (SQLBindParameterFunc)GetProcAddress(hModule, "SQLBindParameter");

    #ifdef _DEBUG
        cout << "Driver loaded successfully." << std::endl;
    #endif

    return SQLAllocHandle_ptr && SQLSetEnvAttr_ptr && SQLSetConnectAttr_ptr && SQLSetStmtAttr_ptr
    && SQLDriverConnect_ptr && SQLExecDirect_ptr 
    && SQLFetch_ptr && SQLGetData_ptr && SQLNumResultCols_ptr && SQLDescribeCol_ptr && SQLMoreResults_ptr && SQLColAttribute_ptr
    && SQLFreeHandle_ptr && SQLDisconnect_ptr
    && SQLGetDiagRec_ptr
    && SQLPrepare_ptr && SQLExecute_ptr && SQLEndTran_ptr && SQLBindCol_ptr && SQLBindParameter_ptr;
}



// Wrap SQLAllocHandle
SQLRETURN SQLAllocHandle_wrap(SQLSMALLINT HandleType, intptr_t InputHandle, intptr_t OutputHandle) {
    if (!SQLAllocHandle_ptr && !LoadDriver()) {
        return SQL_ERROR;
    }

    #ifdef _DEBUG
        // cout << "Allocate SQL Handle" << std::endl;
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
        // cout << "Set SQL environment Attribute" << std::endl;
    #endif

    return SQLSetEnvAttr_ptr(reinterpret_cast<SQLHANDLE>(EnvHandle), Attribute, reinterpret_cast<SQLPOINTER>(ValuePtr), StringLength);
}

// Wrap SQLSetConnectAttr
SQLRETURN SQLSetConnectAttr_wrap(intptr_t ConnectionHandle, SQLINTEGER Attribute, intptr_t ValuePtr, SQLINTEGER StringLength) {
    if (!SQLSetConnectAttr_ptr && !LoadDriver()) {
        return SQL_ERROR;
    }

    #ifdef _DEBUG
        // cout << "Set SQL Connection Attribute" << std::endl;
    #endif

    return SQLSetConnectAttr_ptr(reinterpret_cast<SQLHDBC>(ConnectionHandle), Attribute, reinterpret_cast<SQLPOINTER>(ValuePtr), StringLength);
}

// Wrap SQLSetStmtAttr
SQLRETURN SQLSetStmtAttr_wrap(intptr_t ConnectionHandle, SQLINTEGER Attribute, intptr_t ValuePtr, SQLINTEGER StringLength) {
    if (!SQLSetConnectAttr_ptr && !LoadDriver()) {
        return SQL_ERROR;
    }

    #ifdef _DEBUG
        // cout << "Set SQL Statement Attribute" << std::endl;
    #endif
    return SQLSetStmtAttr_ptr(reinterpret_cast<SQLHSTMT>(ConnectionHandle), Attribute, reinterpret_cast<SQLPOINTER>(ValuePtr), StringLength);
}

// Helper function to check for driver errors
std::wstring CheckError(SQLSMALLINT handleType, intptr_t handle, SQLRETURN retcode) {
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
        // cout << "Execute SQL Query" << std::endl;
    #endif

    return SQLExecDirect_ptr(reinterpret_cast<SQLHANDLE>(StatementHandle),
                              const_cast<SQLWCHAR*>(Query.c_str()), SQL_NTS);
}

// Wrap SQLNumResultCols
SQLSMALLINT SQLNumResultCols_wrap(intptr_t StatementHandle) {
    if (!SQLNumResultCols_ptr && !LoadDriver()) {
        return SQL_ERROR;
    }

    #ifdef _DEBUG
        // cout << "Get Number of Columns" << std::endl;
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
        // cout << "Get Columns name\n";
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
        // cout << "Fetch next row\n";
    #endif

    return SQLFetch_ptr(reinterpret_cast<SQLHANDLE>(StatementHandle));
}

// Helper function to retrieve column data
SQLRETURN SQLGetData_wrap(intptr_t StatementHandle, SQLUSMALLINT colCount, py::list& row) {
    if (!SQLGetData_ptr && !LoadDriver()) {
        return SQL_ERROR;
    }
    #ifdef _DEBUG
        // cout << "Get data from columns\n";
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
                        std::wstring wstrData(dataBuffer);
                        // std::string strData(wstrData.begin(), wstrData.end());
                        // cout << "Data (WCHAR/WVARCHAR/CHAR/VARCHAR): " << strData << endl;
                        row.append(py::cast(wstrData)); // Correct conversion
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

// Bind the functions to the module
PYBIND11_MODULE(ddbc_bindings, m) {
    m.doc() = "msodbcsql driver api bindings for Python"; // optional module docstring

    m.def("SQLAllocHandle", &SQLAllocHandle_wrap, "Allocate an environment, connection, statement, or descriptor handle");
    m.def("SQLSetEnvAttr", &SQLSetEnvAttr_wrap, "Set an attribute that governs aspects of environments");
    m.def("SQLSetConnectAttr", &SQLSetConnectAttr_wrap, "Set an attribute that governs aspects of connections");
    m.def("SQLSetStmtAttr", &SQLSetStmtAttr_wrap, "Set an attribute that governs aspects of statements");
    m.def("SQLDriverConnect", &SQLDriverConnect_wrap, "Connect to a data source with a connection string");
    m.def("SQLExecDirect", &SQLExecDirect_wrap, "Execute a SQL query directly");
    m.def("SQLFetch", &SQLFetch_wrap, "Fetch the next row from the result set");
    m.def("SQLNumResultCols", &SQLNumResultCols_wrap, "Get the number of columns in the result set");
    m.def("SQLDescribeCol", &SQLDescribeCol_wrap, "Get information about a column in the result set");
    m.def("SQLGetData", &SQLGetData_wrap, "Retrieve data from the result set");
    m.def("SQLMoreResults", &SQLMoreResults_wrap, "Check for more results in the result set");
    m.def("FetchOne", &FetchOne_wrap, "Fetch one row from the result set");
    m.def("SQLFreeHandle", &SQLFreeHandle_wrap, "Free a handle");
    m.def("SQLDisconnect", &SQLDisconnect_wrap, "Disconnect from a data source");
    m.def("CheckError", &CheckError, "Check for driver errors");
    m.def("SQLEndTran", &SQLEndTran_wrap, "End a transaction on a connection or environment");
}