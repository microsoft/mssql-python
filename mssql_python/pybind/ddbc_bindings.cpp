// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/complex.h>
#include <pybind11/functional.h>
#include <pybind11/chrono.h>
#include <windows.h>
#include <sqlext.h>
#include <sql.h>
#include <iostream>
#include <cstdint>
#include <string>
#include <iomanip> // For std::setw and std::setfill

namespace py = pybind11;
using namespace pybind11::literals;

// Forward declarations
#define SQL_SS_TIME2                        (-154)

// Log data to stdout only in debug builds
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


// Struct to hold parameter information for binding. Used by SQLBindParameter.
struct ParamInfo {
    SQLSMALLINT inputOutputType;
    SQLSMALLINT paramCType;
    SQLSMALLINT paramSQLType;
    SQLULEN columnSize;
    SQLSMALLINT decimalDigits;
    // py::object* dataPtr; // Stores pointer to the python object
                         // that holds parameter value
                         // TODO: See if we can reuse python buffer in some cases
};

// Mirrors the SQL_NUMERIC_STRUCT. But redefined to replace val char array
// with std::string, because pybind doesn't allow binding char array
struct NumericData {
    SQLCHAR precision;
    SQLSCHAR scale;
    SQLCHAR	sign; /* 1=pos 0=neg */
    std::string val;

    // Default constructor
    NumericData() : precision(0), scale(0), sign(0), val("") {}

    NumericData(SQLCHAR precision, SQLSCHAR scale, SQLCHAR sign, const std::string& value)
        : precision(precision), scale(scale), sign(sign), val(value) {}

    // Method to convert to a Python numeric type
    double to_double() const {
        double result = 0.0;

        // Iterate over each byte in the value string
        for (size_t i = 0; i < val.size(); ++i) {
            // Convert each byte to an unsigned char and add it to the result
            // Multiply the current result by 256 (2^8) before adding the new byte
            result = result * 256 + static_cast<unsigned char>(val[i]);
        }

        // Adjust the result by dividing it by 10 raised to the power of the scale
        // This accounts for the decimal places in the numeric value
        result /= pow(10, scale);
        
        // If the sign is 0, the number is negative, so negate the result
        if (sign == 0) {
            result = -result;
        }

        return result;
    }
};

// Struct to hold data buffers and indicators for each column
struct ColumnBuffers {
    std::vector<std::vector<SQLCHAR>> charBuffers;
    std::vector<std::vector<SQLWCHAR>> wcharBuffers;
    std::vector<std::vector<SQLINTEGER>> intBuffers;
    std::vector<std::vector<SQLSMALLINT>> smallIntBuffers;
    std::vector<std::vector<SQLFLOAT>> floatBuffers;
    std::vector<std::vector<SQLDOUBLE>> doubleBuffers;
    std::vector<std::vector<SQL_NUMERIC_STRUCT>> numericBuffers;
    std::vector<std::vector<SQL_TIMESTAMP_STRUCT>> timestampBuffers;
    std::vector<std::vector<SQLBIGINT>> bigIntBuffers;
    std::vector<std::vector<SQL_DATE_STRUCT>> dateBuffers;
    std::vector<std::vector<SQL_TIME_STRUCT>> timeBuffers;
    std::vector<std::vector<SQLGUID>> guidBuffers;
    std::vector<std::vector<SQLLEN>> indicators;

    ColumnBuffers(SQLSMALLINT numCols, int fetchSize)
        : charBuffers(numCols),
          wcharBuffers(numCols),
          intBuffers(numCols),
          smallIntBuffers(numCols),
          floatBuffers(numCols),
          doubleBuffers(numCols),
          numericBuffers(numCols),
          timestampBuffers(numCols),
          bigIntBuffers(numCols),
          dateBuffers(numCols),
          timeBuffers(numCols),
          guidBuffers(numCols),
          indicators(numCols, std::vector<SQLLEN>(fetchSize)) {}
};

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
typedef SQLRETURN (*SQLFetchScrollFunc)(SQLHANDLE, SQLSMALLINT, SQLLEN);
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
typedef SQLRETURN (*SQLFreeStmtFunc)(SQLHSTMT, SQLUSMALLINT);


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
SQLFetchScrollFunc SQLFetchScroll_ptr = nullptr;
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
SQLFreeStmtFunc SQLFreeStmt_ptr = nullptr;

// Diagnostic record function pointer
SQLGetDiagRecFunc SQLGetDiagRec_ptr = nullptr;

// Helper to load the driver
bool LoadDriver() {
    // Get the DLL directory to the current directory
    wchar_t currentDir[MAX_PATH];
    GetCurrentDirectoryW(MAX_PATH, currentDir);
    std::wstring dllDir = std::wstring(currentDir) + L"\\libs\\win\\msodbcsql18.dll";

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
    SQLFetchScroll_ptr = (SQLFetchScrollFunc)GetProcAddress(hModule, "SQLFetchScroll");
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
    SQLFreeStmt_ptr  = (SQLFreeStmtFunc)GetProcAddress(hModule, "SQLFreeStmt");

    // Diagnostic record function Loading
    SQLGetDiagRec_ptr = (SQLGetDiagRecFunc)GetProcAddress(hModule, "SQLGetDiagRecW");

    #ifdef _DEBUG
        std::cout << "Driver loaded successfully." << std::endl;
    #endif

    return SQLAllocHandle_ptr && SQLSetEnvAttr_ptr && SQLSetConnectAttr_ptr && SQLSetStmtAttr_ptr && SQLGetConnectAttr_ptr
    && SQLDriverConnect_ptr && SQLExecDirect_ptr && SQLPrepare_ptr && SQLBindParameter_ptr && SQLExecute_ptr
    && SQLFetch_ptr && SQLFetchScroll_ptr && SQLGetData_ptr && SQLNumResultCols_ptr && SQLBindCol_ptr && SQLDescribeCol_ptr && SQLMoreResults_ptr
    && SQLColAttribute_ptr && SQLColAttribute_ptr && SQLEndTran_ptr && SQLFreeHandle_ptr && SQLDisconnect_ptr && SQLFreeStmt_ptr
    && SQLGetDiagRec_ptr;
}

// TODO: Add more nuanced exception classes
void ThrowStdException(const std::string& message) {
    throw std::runtime_error(message);
}

std::string MakeParamMismatchErrorStr(const std::string& cType, const int paramIndex) {
    std::string errorString = "Parameter's object type does not match parameter's C type. C type - " +
                              cType + ", paramIndex - " + std::to_string(paramIndex);
    return errorString;
}

// Wrap SQLAllocHandle
SQLRETURN SQLAllocHandle_wrap(SQLSMALLINT HandleType, intptr_t InputHandle, intptr_t OutputHandle) {
    if (!SQLAllocHandle_ptr && !LoadDriver()) {
        return SQL_ERROR;
    }

    #ifdef _DEBUG
        std::cout << "Allocate SQL Handle" << std::endl;
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
        std::cout << "Set SQL environment Attribute" << std::endl;
    #endif

    return SQLSetEnvAttr_ptr(reinterpret_cast<SQLHANDLE>(EnvHandle), Attribute, reinterpret_cast<SQLPOINTER>(ValuePtr), StringLength);
}

// Wrap SQLSetConnectAttr
SQLRETURN SQLSetConnectAttr_wrap(intptr_t ConnectionHandle, SQLINTEGER Attribute, intptr_t ValuePtr, SQLINTEGER StringLength) {
    if (!SQLSetConnectAttr_ptr && !LoadDriver()) {
        return SQL_ERROR;
    }

    #ifdef _DEBUG
        std::cout << "Set SQL Connection Attribute" << std::endl;
    #endif

    return SQLSetConnectAttr_ptr(reinterpret_cast<SQLHDBC>(ConnectionHandle), Attribute, reinterpret_cast<SQLPOINTER>(ValuePtr), StringLength);
}

// Wrap SQLSetStmtAttr
SQLRETURN SQLSetStmtAttr_wrap(intptr_t ConnectionHandle, SQLINTEGER Attribute, intptr_t ValuePtr, SQLINTEGER StringLength) {
    if (!SQLSetConnectAttr_ptr && !LoadDriver()) {
        return SQL_ERROR;
    }

    #ifdef _DEBUG
        std::cout << "Set SQL Statement Attribute" << std::endl;
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
        std::cout << "Set SQL Statement Attribute" << std::endl;
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
        std::cout << "Execute SQL Query" << std::endl;
    #endif

    return SQLExecDirect_ptr(reinterpret_cast<SQLHANDLE>(StatementHandle),
                              const_cast<SQLWCHAR*>(Query.c_str()), SQL_NTS);
}

// Executes the provided query. If the query is parametrized, it prepares the statement and
// binds the parameters. Otherwise, it executes the query directly.
// usePrepare parameter can be used to disable the prepare step for queries that might already
// be prepared in a previous call.
SQLRETURN SQLExecute_wrap(const intptr_t statementHandle, const std::wstring& query /* TODO: Use SQLTCHAR? */,
    const py::list& params, const std::vector<ParamInfo>& paramInfos,
    const bool usePrepare = true)
{
    if (!SQLPrepare_ptr && !LoadDriver()) {
		// TODO: Error needs to be relayed to application via exception
		std::cout << "DDBCSQLExecute: Could not load ODBC library" << std::endl;
        return SQL_ERROR;
    }
    assert(SQLPrepare_ptr && SQLBindParameter_ptr && SQLExecute_ptr && SQLExecDirect_ptr);

	if (params.size() != paramInfos.size()) {
		// TODO: Error needs to be relayed to application via exception
		std::cout << "DDBCSQLExecute: Number of parameters and paramInfos do not match." << std::endl;
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
        if (!SQL_SUCCEEDED(rc) && rc != SQL_NO_DATA) {
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
			const ParamInfo& paramInfo = paramInfos[paramIndex];
            void* dataPtr = nullptr;
            SQLLEN bufferLength = 0;
            SQLLEN* strLenOrIndPtr = new SQLLEN();
            paramBuffers.push_back(std::shared_ptr<void>(strLenOrIndPtr,
                                                         std::default_delete<SQLLEN>()));

            // TODO: Add more data types like money, guid, interval, TVPs etc.
            switch (paramInfo.paramCType) {
                case SQL_C_CHAR:
                case SQL_C_BINARY:
                {
                    if (!py::isinstance<py::str>(param) && !py::isinstance<py::bytearray>(param) &&
                        !py::isinstance<py::bytes>(param)) {
                        ThrowStdException(MakeParamMismatchErrorStr("SQL_C_CHAR", paramIndex));
                    }
                    paramBuffers.push_back(std::shared_ptr<void>( new std::string(param.cast<std::string>()),
                                                                  std::default_delete<std::string>()));
                    std::string* strParam = static_cast<std::string*>(paramBuffers.back().get());
                    if (strParam->size() > 8192 /* TODO: Fix max length */) {
                        // TODO: throw error to python code. It should have done this check (to avoid copying huge data in C++)
                        // Python code must give user error that streaming is not yet supported
                    }
                    dataPtr = const_cast<void*>(static_cast<const void*>(strParam->c_str()));
                    bufferLength = strParam->size() + 1 /* null terminator */;
                    *strLenOrIndPtr = SQL_NTS;
                    break;
                }
                case SQL_C_WCHAR:
                {
                    if (!py::isinstance<py::str>(param) && !py::isinstance<py::bytearray>(param) &&
                        !py::isinstance<py::bytes>(param)) {
                        ThrowStdException(MakeParamMismatchErrorStr("SQL_C_WCHAR", paramIndex));
                    }
                    paramBuffers.push_back(std::shared_ptr<void>(new std::wstring(param.cast<std::wstring>()),
                                                                 std::default_delete<std::wstring>()));
                    std::wstring* strParam = static_cast<std::wstring*>(paramBuffers.back().get());
                    if (strParam->size() > 8192 /* TODO: Fix max length */) {
                        // TODO: throw error to python code. It should have done this check (to avoid copying huge data in C++)
                        // Python code must give user error that streaming is not yet supported
                    }
                    dataPtr = const_cast<void*>(static_cast<const void*>(strParam->c_str()));
                    bufferLength = (strParam->size() + 1 /* null terminator */) * sizeof(wchar_t);
                    *strLenOrIndPtr = SQL_NTS;
                    break;
                }
                case SQL_C_BIT:
                {
                    if (!py::isinstance<py::bool_>(param)) {
                        ThrowStdException(MakeParamMismatchErrorStr("SQL_C_BIT", paramIndex));
                    }
                    paramBuffers.push_back(std::shared_ptr<void>(new bool(param.cast<bool>()),
                        std::default_delete<bool>()));
                    dataPtr = paramBuffers.back().get();
                    break;
                }
                case SQL_C_DEFAULT:
                {
                    if (!py::isinstance<py::none>(param)) {
                        ThrowStdException(MakeParamMismatchErrorStr("SQL_C_DEFAULT", paramIndex));
                    }
                    dataPtr = nullptr;
                    *strLenOrIndPtr = SQL_NULL_DATA;
                    break;
                }
                case SQL_C_STINYINT:
                case SQL_C_TINYINT:
                case SQL_C_SSHORT:
                case SQL_C_SHORT:
                {
                    if (!py::isinstance<py::int_>(param)) {
                        ThrowStdException(MakeParamMismatchErrorStr("SQL_C_TINY/SHORTINT", paramIndex));
                    }
    				paramBuffers.push_back(std::shared_ptr<void>(new int(param.cast<int>()),
                                                                 std::default_delete<int>()));
    				dataPtr = paramBuffers.back().get();
                    break;
                }
                case SQL_C_UTINYINT:
                case SQL_C_USHORT:
                { 
                    if (!py::isinstance<py::int_>(param)) {
                        ThrowStdException(MakeParamMismatchErrorStr("SQL_C_UTINY/USHORTINT", paramIndex));
                    }
    				paramBuffers.push_back(std::shared_ptr<void>(new unsigned int(param.cast<unsigned int>()),
                                                                 std::default_delete<unsigned int>()));
    				dataPtr = paramBuffers.back().get();
                    break;
                }
    			case SQL_C_SBIGINT:
                case SQL_C_SLONG:
                case SQL_C_LONG:
                {
                    if (!py::isinstance<py::int_>(param)) {
                        ThrowStdException(MakeParamMismatchErrorStr("SQL_C_BIG/LONGINT", paramIndex));
                    }
    				paramBuffers.push_back(std::shared_ptr<void>(new int64_t(param.cast<int64_t>()),
                                                                 std::default_delete<int64_t>()));
    				dataPtr = paramBuffers.back().get();
                    break;
                }
                case SQL_C_UBIGINT:
                case SQL_C_ULONG:
                {
                    if (!py::isinstance<py::int_>(param)) {
                        ThrowStdException(MakeParamMismatchErrorStr("SQL_C_UBIG/ULONGINT", paramIndex));
                    }
    				paramBuffers.push_back(std::shared_ptr<void>(new uint64_t(param.cast<uint64_t>()),
                                                                 std::default_delete<uint64_t>()));
    				dataPtr = paramBuffers.back().get();
                    break;
                }
                case SQL_C_FLOAT:
                {
                    if (!py::isinstance<py::float_>(param)) {
                        ThrowStdException(MakeParamMismatchErrorStr("SQL_C_FLOAT", paramIndex));
                    }
                    paramBuffers.push_back(std::shared_ptr<void>(new float(param.cast<float>()),
                                                                 std::default_delete<float>()));
                    dataPtr = paramBuffers.back().get();
                    break;
                }
                case SQL_C_DOUBLE:
                {
                    if (!py::isinstance<py::float_>(param)) {
                        ThrowStdException(MakeParamMismatchErrorStr("SQL_C_DOUBLE", paramIndex));
                    }
    				paramBuffers.push_back(std::shared_ptr<void>(new double(param.cast<double>()),
                                                                 std::default_delete<double>()));
    				dataPtr = paramBuffers.back().get();
                    break;
                }
                case SQL_C_TYPE_DATE:
                {
                    py::object dateType = py::module_::import("datetime").attr("date");
                    if (!py::isinstance(param, dateType)) {
                        ThrowStdException(MakeParamMismatchErrorStr("SQL_C_TYPE_DATE", paramIndex));
                    }
                    // TODO: can be moved to python by registering SQL_DATE_STRUCT in pybind
                    paramBuffers.push_back(std::shared_ptr<void>(new SQL_DATE_STRUCT(),
                                                                 std::default_delete<SQL_DATE_STRUCT>()));
    				dataPtr = paramBuffers.back().get();
                    SQL_DATE_STRUCT* sqlDatePtr = static_cast<SQL_DATE_STRUCT*>(dataPtr);
                    sqlDatePtr->year = param.attr("year").cast<int>();
                    sqlDatePtr->month = param.attr("month").cast<int>();
                    sqlDatePtr->day = param.attr("day").cast<int>();
                    break;
                }
                case SQL_C_TYPE_TIME:
                {
                    py::object timeType = py::module_::import("datetime").attr("time");
                    if (!py::isinstance(param, timeType)) {
                        ThrowStdException(MakeParamMismatchErrorStr("SQL_C_TYPE_TIME", paramIndex));
                    }
                    // TODO: can be moved to python by registering SQL_TIME_STRUCT in pybind
                    paramBuffers.push_back(std::shared_ptr<void>(new SQL_TIME_STRUCT(),
                                                                 std::default_delete<SQL_TIME_STRUCT>()));
    				dataPtr = paramBuffers.back().get();
                    SQL_TIME_STRUCT* sqlTimePtr = static_cast<SQL_TIME_STRUCT*>(dataPtr);
                    sqlTimePtr->hour = param.attr("hour").cast<int>();
                    sqlTimePtr->minute = param.attr("minute").cast<int>();
                    sqlTimePtr->second = param.attr("second").cast<int>();
                    break;
                }
                case SQL_C_TYPE_TIMESTAMP:
                {
                    py::object datetimeType = py::module_::import("datetime").attr("datetime");
                    if (!py::isinstance(param, datetimeType)) {
                        ThrowStdException(MakeParamMismatchErrorStr("SQL_C_TYPE_TIMESTAMP", paramIndex));
                    }
                    paramBuffers.push_back(std::shared_ptr<void>(new SQL_TIMESTAMP_STRUCT(),
                                                                 std::default_delete<SQL_TIMESTAMP_STRUCT>()));
    				dataPtr = paramBuffers.back().get();
                    SQL_TIMESTAMP_STRUCT* sqlTimestampPtr = static_cast<SQL_TIMESTAMP_STRUCT*>(dataPtr);
                    sqlTimestampPtr->year = param.attr("year").cast<int>();
                    sqlTimestampPtr->month = param.attr("month").cast<int>();
                    sqlTimestampPtr->day = param.attr("day").cast<int>();
                    sqlTimestampPtr->hour = param.attr("hour").cast<int>();
                    sqlTimestampPtr->minute = param.attr("minute").cast<int>();
                    sqlTimestampPtr->second = param.attr("second").cast<int>();
                    // TODO: timestamp.fraction field seems to involve some computation.
                    // Handle this in python and pass result to pybind module?
                    sqlTimestampPtr->fraction = 0;
                    break;
                }
                case SQL_C_NUMERIC:
               {
                    if (!py::isinstance<NumericData>(param)) {
                        ThrowStdException(MakeParamMismatchErrorStr("SQL_C_NUMERIC", paramIndex));
                    }
                    NumericData decimalParam = param.cast<NumericData>();
                    paramBuffers.push_back(std::shared_ptr<void>(new SQL_NUMERIC_STRUCT(),
                                                                 std::default_delete<SQL_NUMERIC_STRUCT>()));
                    dataPtr = paramBuffers.back().get();
                    SQL_NUMERIC_STRUCT* decimalPtr = static_cast<SQL_NUMERIC_STRUCT*>(dataPtr);
                    decimalPtr->precision = decimalParam.precision;
                    decimalPtr->scale = decimalParam.scale;
                    decimalPtr->sign = decimalParam.sign;
                    if (decimalParam.val.size() != SQL_MAX_NUMERIC_LEN) {
                        // TODO: Throw error. Val must be a 16 byte integer
                    }
                    std::memcpy(static_cast<void*>(decimalPtr->val), decimalParam.val.c_str(), decimalParam.val.size());
                    break;
                }
                case SQL_C_GUID:
                {
                    // TODO
                    break;
                }
                default:
                {
                    std::ostringstream errorString;
                    errorString << "Unsupported parameter type - " << paramInfo.paramCType << " for parameter - " << paramIndex;
                    ThrowStdException(errorString.str());
                }
            }

            rc = SQLBindParameter_ptr(hStmt, paramIndex + 1 /* 1-based indexing */,
                paramInfo.inputOutputType,
                paramInfo.paramCType,
                paramInfo.paramSQLType,
                paramInfo.columnSize,
                paramInfo.decimalDigits,
                dataPtr, bufferLength, strLenOrIndPtr);
            if (!SQL_SUCCEEDED(rc)) {
				DEBUG_LOG("DDBCSQLExecute: Error when binding parameter - %d", paramIndex);
				return rc;
			}
        }

        rc = SQLExecute_ptr(hStmt);
        if (!SQL_SUCCEEDED(rc) && rc != SQL_NO_DATA) {
			DEBUG_LOG("DDBCSQLExecute: Error during execution of the statement");
            return rc;
        }
        // TODO: Handle huge input parameters by checking rc == SQL_NEED_DATA

        // Unbind the bound buffers for all parameters coz the buffers' memory will
        // be freed when this function exits (parambuffers goes out of scope)
        rc = SQLFreeStmt_ptr(hStmt, SQL_RESET_PARAMS);

		return rc;
    }
}

// Wrap SQLNumResultCols
SQLSMALLINT SQLNumResultCols_wrap(intptr_t StatementHandle) {
    if (!SQLNumResultCols_ptr && !LoadDriver()) {
        return SQL_ERROR;
    }

    #ifdef _DEBUG
        std::cout << "Get Number of Columns" << std::endl;
    #endif

    SQLSMALLINT* ColumnCount = new SQLSMALLINT;
    SQLNumResultCols_ptr(reinterpret_cast<SQLHSTMT>(StatementHandle), ColumnCount);
    return *ColumnCount;
}

// Wrap SQLDescribeCol
SQLRETURN SQLDescribeCol_wrap(
    intptr_t StatementHandle, 
    py::list& ColumnMetadata // Accept a Python list reference
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
        std::cout << "Get Columns name\n";
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

        if (SQL_SUCCEEDED(retcode)) {
            // Append a named py::dict to ColumnMetadata
            ColumnMetadata.append(py::dict(
                "ColumnName"_a = std::wstring(ColumnName),
                "DataType"_a = DataType,
                "ColumnSize"_a = ColumnSize,
                "DecimalDigits"_a = DecimalDigits,
                "Nullable"_a = Nullable
            ));
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
        std::cout << "Fetch next row\n";
    #endif

    return SQLFetch_ptr(reinterpret_cast<SQLHANDLE>(StatementHandle));
}

// Helper function to retrieve column data
SQLRETURN SQLGetData_wrap(intptr_t StatementHandle, SQLUSMALLINT colCount, py::list& row) {
    if (!SQLGetData_ptr && !LoadDriver()) {
        return SQL_ERROR;
    }
    #ifdef _DEBUG
        std::cout << "Get data from columns\n";
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
            if (!SQL_SUCCEEDED(ret)) {
                std::wcerr << L"Error retrieving data for column: " << columnName << L" Type: " << dataType << L" Error code: " << ret << std::endl;
                row.append(py::none());
                continue;
            }

            switch (dataType) {
                case SQL_CHAR:
                case SQL_VARCHAR:
                case SQL_LONGVARCHAR: {
                    std::vector<SQLCHAR> dataBuffer(columnSize / sizeof(SQLCHAR) + 1);
                    SQLLEN dataLen;
                    ret = SQLGetData_ptr(hStmt, i, SQL_C_CHAR, dataBuffer.data(), dataBuffer.size()-1, &dataLen);

                    if (SQL_SUCCEEDED(ret)) {
                        if (dataLen > 0 && dataLen < dataBuffer.size()) {
                            dataBuffer[dataLen / sizeof(SQLCHAR)] = '\0'; // Null-terminate
                        }
                        row.append(std::string(reinterpret_cast<char*>(dataBuffer.data()))); // Append to row
                    } else {
                        row.append(py::none());
                    }
                    break;
                }
                case SQL_WCHAR:
                case SQL_WVARCHAR:
                case SQL_WLONGVARCHAR: {
                    std::vector<SQLWCHAR> dataBuffer(columnSize / sizeof(SQLWCHAR) + 1);
                    SQLLEN dataLen;
                    ret = SQLGetData_ptr(hStmt, i, SQL_C_WCHAR, dataBuffer.data(), (dataBuffer.size()-1)*sizeof(SQLWCHAR), &dataLen);

                    if (SQL_SUCCEEDED(ret)) {
                        if (dataLen > 0 && dataLen < dataBuffer.size()*sizeof(SQLWCHAR)) {
                            dataBuffer[dataLen / sizeof(SQLWCHAR)] = L'\0'; // Null-terminate
                        }
                        row.append(std::wstring(dataBuffer.data())); // Append to row
                    } else {
                        row.append(py::none());
                    }
                    break;
                }
                case SQL_INTEGER: {  // For integer types
                    SQLINTEGER intValue;
                    ret = SQLGetData_ptr(hStmt, i, SQL_C_LONG, &intValue, 0, NULL);
                    if (SQL_SUCCEEDED(ret)) {
                        row.append(static_cast<int>(intValue));
                    } else {
                        row.append(py::none());
                    }
                    break;
                }
                case SQL_SMALLINT: {  // For small integer types
                    SQLSMALLINT smallIntValue;
                    ret = SQLGetData_ptr(hStmt, i, SQL_C_SHORT, &smallIntValue, 0, NULL);
                    if (SQL_SUCCEEDED(ret)) {
                        row.append(static_cast<int>(smallIntValue));
                    } else {
                        row.append(py::none());
                    }
                    break;
                }
                case SQL_FLOAT:
                case SQL_REAL: {  // For float and real types
                    SQLREAL floatValue;
                    ret = SQLGetData_ptr(hStmt, i, SQL_C_FLOAT, &floatValue, 0, NULL);
                    if (SQL_SUCCEEDED(ret)) {
                        row.append(static_cast<float>(floatValue));
                    } else {
                        row.append(py::none());
                    }
                    break;
                }
                case SQL_DECIMAL:
                case SQL_NUMERIC: {  // For decimal and numeric types
                    SQL_NUMERIC_STRUCT numericValue;
                    ret = SQLGetData_ptr(hStmt, i, SQL_C_NUMERIC, &numericValue, sizeof(numericValue), NULL);
                    if (SQL_SUCCEEDED(ret)) {
                        row.append(NumericData(numericValue.precision, numericValue.scale, numericValue.sign, std::string(reinterpret_cast<char*>(numericValue.val), SQL_MAX_NUMERIC_LEN)).to_double());
                    } else {
                        row.append(py::none());
                    }
                    break;
                }
                case SQL_DOUBLE: {  // For double types
                    SQLDOUBLE doubleValue;
                    ret = SQLGetData_ptr(hStmt, i, SQL_C_DOUBLE, &doubleValue, 0, NULL);
                    if (SQL_SUCCEEDED(ret)) {
                        row.append(static_cast<double>(doubleValue));
                    } else {
                        row.append(py::none());
                    }
                    break;
                }
                case SQL_BIGINT: { // BIGINT
                    SQLBIGINT bigintValue;
                    ret = SQLGetData_ptr(hStmt, i, SQL_C_SBIGINT, &bigintValue, 0, NULL);
                    if (SQL_SUCCEEDED(ret)) {
                        row.append(static_cast<long long>(bigintValue));
                    } else {
                        row.append(py::none());
                    }
                    break;
                }
                case SQL_TYPE_DATE: { // DATE
                    SQL_DATE_STRUCT dateValue;
                    ret = SQLGetData_ptr(hStmt, i, SQL_C_TYPE_DATE, &dateValue, sizeof(dateValue), NULL);
                    if (SQL_SUCCEEDED(ret)) {
                        row.append(py::make_tuple(dateValue.year, dateValue.month, dateValue.day));
                    } else {
                        row.append(py::none());
                    }
                    break;
                }
                case SQL_TIME:
                case SQL_TYPE_TIME:
                case SQL_SS_TIME2: { // TIME
                    SQL_TIME_STRUCT timeValue;
                    ret = SQLGetData_ptr(hStmt, i, SQL_C_TYPE_TIME, &timeValue, sizeof(timeValue), NULL);
                    if (SQL_SUCCEEDED(ret)) {
                        row.append(py::make_tuple(timeValue.hour, timeValue.minute, timeValue.second));
                    } else {
                        row.append(py::none());
                    }
                    break;
                }
                case SQL_TIMESTAMP:
                case SQL_TYPE_TIMESTAMP:
                case SQL_DATETIME: { // TIMESTAMP
                    SQL_TIMESTAMP_STRUCT timestampValue;
                    ret = SQLGetData_ptr(hStmt, i, SQL_C_TYPE_TIMESTAMP, &timestampValue, sizeof(timestampValue), NULL);
                    if (SQL_SUCCEEDED(ret)) {
                        row.append(py::make_tuple(timestampValue.year, timestampValue.month, timestampValue.day,
                                                   timestampValue.hour, timestampValue.minute, timestampValue.second));
                    } else {
                        row.append(py::none());
                    }
                    break;
                }
                case SQL_BINARY: { // BINARY
                    // Use raw pointer to manage memory
                    std::unique_ptr<SQLCHAR[]> dataBuffer(new SQLCHAR[columnSize]);
                    SQLLEN dataLen;
                    ret = SQLGetData_ptr(hStmt, i, SQL_C_BINARY, dataBuffer.get(), columnSize, &dataLen);
                    
                    if (SQL_SUCCEEDED(ret)) {
                        // Handle binary data as needed (e.g., converting to a hex string)
                        row.append(py::bytes(reinterpret_cast<const char*>(dataBuffer.get()), dataLen));
                    } else {
                        row.append(py::none());
                    }
                    break;
                }
                case SQL_VARBINARY: { // VARBINARY
                    // Use smart pointer to manage memory
                    std::unique_ptr<SQLCHAR[]> dataBuffer(new SQLCHAR[columnSize]);
                    SQLLEN dataLen;
                    ret = SQLGetData_ptr(hStmt, i, SQL_C_BINARY, dataBuffer.get(), columnSize, &dataLen);
                    
                    if (SQL_SUCCEEDED(ret)) {
                        row.append(py::bytes(reinterpret_cast<const char*>(dataBuffer.get()), dataLen));
                    } else {
                        row.append(py::none());
                    }
                    break;
                }
                case SQL_LONGVARBINARY: { // LONGVARBINARY
                    //use smart pointer to manage memory
                    std::unique_ptr<SQLCHAR[]> dataBuffer(new SQLCHAR[columnSize]);
                    SQLLEN dataLen;
                    ret = SQLGetData_ptr(hStmt, i, SQL_C_BINARY, dataBuffer.get(), columnSize, &dataLen);
                    
                    if (SQL_SUCCEEDED(ret)) {
                        row.append(py::bytes(reinterpret_cast<const char*>(dataBuffer.get()), dataLen));
                    } else {
                        row.append(py::none());
                    }
                    break;
                }
                case SQL_TINYINT: { // TINYINT
                    SQLCHAR tinyIntValue;
                    ret = SQLGetData_ptr(hStmt, i, SQL_C_TINYINT, &tinyIntValue, 0, NULL);
                    if (SQL_SUCCEEDED(ret)) {
                        row.append(static_cast<int>(tinyIntValue));
                    } else {
                        row.append(py::none());
                    }
                    break;
                }
                case SQL_BIT: { // BIT
                    SQLCHAR bitValue;
                    ret = SQLGetData_ptr(hStmt, i, SQL_C_BIT, &bitValue, 0, NULL);
                    if (SQL_SUCCEEDED(ret)) {
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
                    if (SQL_SUCCEEDED(ret)) {
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

SQLRETURN SQLBindColums(SQLHSTMT hStmt, ColumnBuffers& buffers, py::list& columnNames, SQLUSMALLINT numCols, int fetchSize)
{
    SQLRETURN ret = SQL_SUCCESS;
    // Bind columns based on their data types
    for (SQLUSMALLINT col = 1; col <= numCols; col++) {
        auto columnMeta = columnNames[col - 1].cast<py::dict>();
        SQLSMALLINT dataType = columnMeta["DataType"].cast<SQLSMALLINT>();
        SQLULEN columnSize = columnMeta["ColumnSize"].cast<SQLULEN>();

        switch (dataType) {
            case SQL_CHAR:
            case SQL_VARCHAR:
            case SQL_LONGVARCHAR:
                buffers.charBuffers[col - 1].resize(fetchSize * (columnSize));
                ret = SQLBindCol_ptr(hStmt, col, SQL_C_CHAR, buffers.charBuffers[col - 1].data(), (columnSize) * sizeof(SQLCHAR), buffers.indicators[col - 1].data());
                break;
            case SQL_WCHAR:
            case SQL_WVARCHAR:
            case SQL_WLONGVARCHAR:
                buffers.wcharBuffers[col - 1].resize(fetchSize * (columnSize));
                ret = SQLBindCol_ptr(hStmt, col, SQL_C_WCHAR, buffers.wcharBuffers[col - 1].data(), (columnSize ) * sizeof(SQLWCHAR), buffers.indicators[col - 1].data());
                break;
            case SQL_INTEGER:
                buffers.intBuffers[col - 1].resize(fetchSize);
                ret = SQLBindCol_ptr(hStmt, col, SQL_C_SLONG, buffers.intBuffers[col - 1].data(), sizeof(SQLINTEGER), buffers.indicators[col - 1].data());
                break;
            case SQL_SMALLINT:
                buffers.smallIntBuffers[col - 1].resize(fetchSize);
                ret = SQLBindCol_ptr(hStmt, col, SQL_C_SSHORT, buffers.smallIntBuffers[col - 1].data(), sizeof(SQLSMALLINT), buffers.indicators[col - 1].data());
                break;
            case SQL_TINYINT: // TINYINT
                buffers.charBuffers[col - 1].resize(fetchSize);
                ret = SQLBindCol_ptr(hStmt, col, SQL_C_TINYINT, buffers.charBuffers[col - 1].data(), sizeof(SQLCHAR), buffers.indicators[col - 1].data());
                break;
            case SQL_BIT:  // BIT
                buffers.charBuffers[col - 1].resize(fetchSize);
                ret = SQLBindCol_ptr(hStmt, col, SQL_C_BIT, buffers.charBuffers[col - 1].data(), sizeof(SQLCHAR), buffers.indicators[col - 1].data());
                break;
            case SQL_REAL:
            case SQL_FLOAT:
                buffers.floatBuffers[col - 1].resize(fetchSize);
                ret = SQLBindCol_ptr(hStmt, col, SQL_C_FLOAT, buffers.floatBuffers[col - 1].data(), sizeof(SQLREAL), buffers.indicators[col - 1].data());
                break;
            case SQL_DECIMAL:
            case SQL_NUMERIC:
                buffers.numericBuffers[col - 1].resize(fetchSize);
                ret = SQLBindCol_ptr(hStmt, col, SQL_C_NUMERIC, buffers.numericBuffers[col - 1].data(), sizeof(SQL_NUMERIC_STRUCT), buffers.indicators[col - 1].data());
                break;
            case SQL_DOUBLE:
                buffers.doubleBuffers[col - 1].resize(fetchSize);
                ret = SQLBindCol_ptr(hStmt, col, SQL_C_DOUBLE, buffers.doubleBuffers[col - 1].data(), sizeof(SQLDOUBLE), buffers.indicators[col - 1].data());
                break;
            case SQL_TIMESTAMP:
            case SQL_TYPE_TIMESTAMP:
            case SQL_DATETIME:
                buffers.timestampBuffers[col - 1].resize(fetchSize);
                ret = SQLBindCol_ptr(hStmt, col, SQL_C_TYPE_TIMESTAMP, buffers.timestampBuffers[col - 1].data(), sizeof(SQL_TIMESTAMP_STRUCT), buffers.indicators[col - 1].data());
                break;
            case SQL_BIGINT:
                buffers.bigIntBuffers[col - 1].resize(fetchSize);
                ret = SQLBindCol_ptr(hStmt, col, SQL_C_SBIGINT, buffers.bigIntBuffers[col - 1].data(), sizeof(SQLBIGINT), buffers.indicators[col - 1].data());
                break;
            case SQL_TYPE_DATE:
                buffers.dateBuffers[col - 1].resize(fetchSize);
                ret = SQLBindCol_ptr(hStmt, col, SQL_C_TYPE_DATE, buffers.dateBuffers[col - 1].data(), sizeof(SQL_DATE_STRUCT), buffers.indicators[col - 1].data());
                break;
            case SQL_TIME:
            case SQL_TYPE_TIME:
            case SQL_SS_TIME2:
                buffers.timeBuffers[col - 1].resize(fetchSize);
                ret = SQLBindCol_ptr(hStmt, col, SQL_C_TYPE_TIME, buffers.timeBuffers[col - 1].data(), sizeof(SQL_TIME_STRUCT), buffers.indicators[col - 1].data());
                break;
            case SQL_GUID:
                buffers.guidBuffers[col - 1].resize(fetchSize);
                ret = SQLBindCol_ptr(hStmt, col, SQL_C_GUID, buffers.guidBuffers[col - 1].data(), sizeof(SQLGUID), buffers.indicators[col - 1].data());
                break;
            case SQL_BINARY:
            case SQL_VARBINARY:
            case SQL_LONGVARBINARY:
                buffers.charBuffers[col - 1].resize(fetchSize * columnSize);
                ret = SQLBindCol_ptr(hStmt, col, SQL_C_BINARY, buffers.charBuffers[col - 1].data(), columnSize, buffers.indicators[col - 1].data());
                break;
            default:
                std::wcerr << L"Unsupported data type for column: " << col << L" Type: " << dataType << std::endl;
                break;
        }
        if (!SQL_SUCCEEDED(ret)) {
            return ret;
        }
    }
    return ret;
}

SQLRETURN FetchBatchData(SQLHSTMT hStmt, ColumnBuffers& buffers, py::list& columnNames, py::list& rows, SQLUSMALLINT numCols, SQLULEN& numRowsFetched, int fetchSize)
{
    SQLRETURN ret = SQL_SUCCESS;
    // Fetch rows in batches
    if ((ret = SQLFetchScroll_ptr(hStmt, SQL_FETCH_NEXT, 0)) != SQL_NO_DATA) {
        for (SQLULEN i = 0; i < numRowsFetched; i++) {
            py::list row;
            for (SQLUSMALLINT col = 1; col <= numCols; col++) {
                auto columnMeta = columnNames[col - 1].cast<py::dict>();
                SQLSMALLINT dataType = columnMeta["DataType"].cast<SQLSMALLINT>();

                if (buffers.indicators[col - 1][i] == SQL_NULL_DATA) {
                    row.append(py::none());
                    continue;
                }

                switch (dataType) {
                    case SQL_CHAR:
                    case SQL_VARCHAR:
                    case SQL_LONGVARCHAR:
                        {
                            SQLLEN dataLen = buffers.indicators[col - 1][i];
                            if (dataLen <= SQL_NULL_DATA) {
                                row.append(py::none());
                                break;
                            }
                            if (dataLen > 0 && dataLen < columnMeta["ColumnSize"].cast<SQLULEN>()) {
                                buffers.charBuffers[col - 1][i * columnMeta["ColumnSize"].cast<SQLULEN>() + dataLen / sizeof(SQLCHAR)] = '\0'; // Null-terminate
                            }
                            row.append(std::string(reinterpret_cast<char*>(&buffers.charBuffers[col - 1][i * columnMeta["ColumnSize"].cast<SQLULEN>()]), buffers.indicators[col - 1][i]));
                            break;
                        }
                    case SQL_WCHAR:
                    case SQL_WVARCHAR:
                    case SQL_WLONGVARCHAR:
                        {
                            SQLLEN dataLen = buffers.indicators[col - 1][i];
                            if (dataLen <= SQL_NULL_DATA ) {
                                row.append(py::none());
                                break;
                            }
                            if (dataLen > 0 && dataLen < columnMeta["ColumnSize"].cast<SQLULEN>()) {
                                buffers.wcharBuffers[col - 1][i * columnMeta["ColumnSize"].cast<SQLULEN>() + dataLen / sizeof(SQLWCHAR)] = L'\0'; // Null-terminate
                            }
                            row.append(std::wstring(reinterpret_cast<wchar_t*>(&buffers.wcharBuffers[col - 1][i * columnMeta["ColumnSize"].cast<SQLULEN>()]), dataLen / sizeof(SQLWCHAR)));
                            break;
                        }
                    case SQL_INTEGER:
                        row.append(buffers.intBuffers[col - 1][i]);
                        break;
                    case SQL_SMALLINT:
                        row.append(buffers.smallIntBuffers[col - 1][i]);
                        break;
                    case SQL_TINYINT: // TINYINT
                        row.append(buffers.charBuffers[col - 1][i]);
                        break;
                    case SQL_BIT:  // BIT
                        row.append(buffers.charBuffers[col - 1][i]);
                        break;
                    case SQL_REAL:
                    case SQL_FLOAT:
                        row.append(static_cast<float>(buffers.floatBuffers[col - 1][i]));
                        break;
                    case SQL_DECIMAL:
                    case SQL_NUMERIC:
                        row.append(NumericData(buffers.numericBuffers[col - 1][i].precision, buffers.numericBuffers[col - 1][i].scale, buffers.numericBuffers[col - 1][i].sign, std::string(reinterpret_cast<char*>(buffers.numericBuffers[col - 1][i].val), SQL_MAX_NUMERIC_LEN)).to_double());
                        break;
                    case SQL_DOUBLE:
                        row.append(static_cast<double>(buffers.doubleBuffers[col - 1][i]));
                        break;
                    case SQL_TIMESTAMP:
                    case SQL_TYPE_TIMESTAMP:
                    case SQL_DATETIME:
                        row.append(py::make_tuple(
                            buffers.timestampBuffers[col - 1][i].year,
                            buffers.timestampBuffers[col - 1][i].month,
                            buffers.timestampBuffers[col - 1][i].day,
                            buffers.timestampBuffers[col - 1][i].hour,
                            buffers.timestampBuffers[col - 1][i].minute,
                            buffers.timestampBuffers[col - 1][i].second
                        ));
                        break;
                    case SQL_BIGINT:
                        row.append(buffers.bigIntBuffers[col - 1][i]);
                        break;
                    case SQL_TYPE_DATE:
                        row.append(py::make_tuple(buffers.dateBuffers[col - 1][i].year, buffers.dateBuffers[col - 1][i].month, buffers.dateBuffers[col - 1][i].day));
                        break;
                    case SQL_TIME:
                    case SQL_TYPE_TIME:
                    case SQL_SS_TIME2:
                        row.append(py::make_tuple(buffers.timeBuffers[col - 1][i].hour, buffers.timeBuffers[col - 1][i].minute, buffers.timeBuffers[col - 1][i].second));
                        break;
                    case SQL_GUID:
                        row.append(py::bytes(reinterpret_cast<const char*>(&buffers.guidBuffers[col - 1][i]), sizeof(SQLGUID)));
                        break;
                    case SQL_BINARY:
                    case SQL_VARBINARY:
                    case SQL_LONGVARBINARY:
                        row.append(py::bytes(reinterpret_cast<const char*>(&buffers.charBuffers[col - 1][i * columnMeta["ColumnSize"].cast<SQLULEN>()]), buffers.indicators[col - 1][i]));
                        break;
                    default:
                        row.append(py::none());
                        break;
                }
            }
            rows.append(row);
        }
    }

    return ret;
}

size_t calculateRowSize(py::list& columnNames, SQLUSMALLINT numCols) {
    size_t rowSize = 0;
    for (SQLUSMALLINT col = 1; col <= numCols; col++) {
        auto columnMeta = columnNames[col - 1].cast<py::dict>();
        SQLSMALLINT dataType = columnMeta["DataType"].cast<SQLSMALLINT>();
        SQLULEN columnSize = columnMeta["ColumnSize"].cast<SQLULEN>();
        
        switch (dataType) {
            case SQL_CHAR:
            case SQL_VARCHAR:
            case SQL_LONGVARCHAR:
                rowSize += columnSize;
                break;
            case SQL_WCHAR:
            case SQL_WVARCHAR:
            case SQL_WLONGVARCHAR:
                rowSize += columnSize * sizeof(SQLWCHAR);
                break;
            case SQL_INTEGER:
                rowSize += sizeof(SQLINTEGER);
                break;
            case SQL_SMALLINT:
                rowSize += sizeof(SQLSMALLINT);
                break;
            case SQL_REAL:
            case SQL_FLOAT:
            case SQL_DECIMAL:
                rowSize += sizeof(SQLREAL);
                break;
            case SQL_DOUBLE:
                rowSize += sizeof(SQLDOUBLE);
                break;
            case SQL_TIMESTAMP:
            case SQL_TYPE_TIMESTAMP:
            case SQL_DATETIME:
                rowSize += sizeof(SQL_TIMESTAMP_STRUCT);
                break;
            case SQL_BIGINT:
                rowSize += sizeof(SQLBIGINT);
                break;
            case SQL_TYPE_DATE:
                rowSize += sizeof(SQL_DATE_STRUCT);
                break;
            case SQL_TIME:
            case SQL_TYPE_TIME:
            case SQL_SS_TIME2:
                rowSize += sizeof(SQL_TIME_STRUCT);
                break;
            case SQL_GUID:
                rowSize += sizeof(SQLGUID);
                break;
            case SQL_TINYINT:
            case SQL_BIT:
                rowSize += sizeof(SQLCHAR);
                break;
            case SQL_BINARY:
            case SQL_VARBINARY:
            case SQL_LONGVARBINARY:
                rowSize += columnSize;
                break;
            default:
                std::wcerr << L"Unsupported data type for column: " << col << L" Type: " << dataType << std::endl;
                break;
        }
    }
    return rowSize;
}

/**
 * FetchMany_wrap - Fetches multiple rows of data from the result set.
 * 
 * @param StatementHandle: Handle to the statement from which data is to be fetched.
 * @param rows: A Python list that will be populated with the fetched rows of data.
 * @param fetchSize: The number of rows to fetch. Default value is 1.
 * 
 * @return SQLRETURN: SQL_SUCCESS if data is fetched successfully,
 *                    SQL_NO_DATA if there are no more rows to fetch,
 *                    throws a runtime error if there is an error fetching data.
 * 
 * This function assumes that the statement handle (hStmt) is already allocated and a query has been executed.
 * It fetches the specified number of rows from the result set and populates the provided Python list with the row data.
 * If there are no more rows to fetch, it returns SQL_NO_DATA.
 * If an error occurs during fetching, it throws a runtime error.
 */
SQLRETURN FetchMany_wrap(intptr_t StatementHandle, py::list& rows, int fetchSize = 1) {
    SQLRETURN ret;
    SQLHSTMT hStmt = reinterpret_cast<SQLHSTMT>(StatementHandle);
    // Retrieve column count
    SQLSMALLINT numCols =  SQLNumResultCols_wrap(StatementHandle);

    // Retrieve column metadata
    py::list columnNames;
    ret = SQLDescribeCol_wrap(StatementHandle, columnNames);
    if(!SQL_SUCCEEDED(ret)) {
        return ret;
    }

    // Initialize column buffers
    ColumnBuffers buffers(numCols, fetchSize);

    // Bind columns
    ret = SQLBindColums(hStmt, buffers, columnNames, numCols, fetchSize);
    if (!SQL_SUCCEEDED(ret)) {
        std::wcerr << L"Error binding columns: " << SQLCheckError_Wrap(SQL_HANDLE_STMT, StatementHandle, ret) << std::endl;
        return ret;
    }

    SQLULEN numRowsFetched;
    SQLSetStmtAttr_ptr(hStmt, SQL_ATTR_ROW_ARRAY_SIZE, (SQLPOINTER)fetchSize, 0);
    SQLSetStmtAttr_ptr(hStmt, SQL_ATTR_ROWS_FETCHED_PTR, &numRowsFetched, 0);

    ret = FetchBatchData(hStmt, buffers, columnNames, rows, numCols, numRowsFetched, fetchSize);
    if(!SQL_SUCCEEDED(ret) && ret != SQL_NO_DATA) {
        std::wcerr << L"Error fetching data: " << SQLCheckError_Wrap(SQL_HANDLE_STMT, StatementHandle, ret) << std::endl;
        return ret;
    }

    return ret;
}

/*
* FetchAll_wrap - Fetches all rows of data from the result set.
* 
* @param StatementHandle: Handle to the statement from which data is to be fetched.
* @param rows: A Python list that will be populated with the fetched rows of data.
* 
* @return SQLRETURN: SQL_SUCCESS if data is fetched successfully,
*                    SQL_NO_DATA if there are no more rows to fetch,
*                    throws a runtime error if there is an error fetching data.
* 
* This function assumes that the statement handle (hStmt) is already allocated and a query has been executed.
* It fetches all rows from the result set and populates the provided Python list with the row data.
* If there are no more rows to fetch, it returns SQL_NO_DATA.
* If an error occurs during fetching, it throws a runtime error.
*/
SQLRETURN FetchAll_wrap(intptr_t StatementHandle, py::list& rows)
{
    SQLRETURN ret;
    SQLHSTMT hStmt = reinterpret_cast<SQLHSTMT>(StatementHandle);
    // Retrieve column count
    SQLSMALLINT numCols =  SQLNumResultCols_wrap(StatementHandle);

    // Retrieve column metadata
    py::list columnNames;
    ret = SQLDescribeCol_wrap(StatementHandle, columnNames);
    if(!SQL_SUCCEEDED(ret)) {
        return ret;
    }

    // Initialize column buffers
    // Define a memory limit (1 GB)
    const size_t memoryLimit = 1ULL * 1024 * 1024 * 1024; // 1 GB
    size_t totalRowSize = calculateRowSize(columnNames, numCols);

    // Calculate fetch size based on the total row size and memory limit
    int fetchSize = static_cast<int>(memoryLimit / totalRowSize);
    // if a row size is more than GB, fetch only one row at a time or max batch size is 1000.
    if (fetchSize == 0) {
        fetchSize = 1;
    }
    else if( fetchSize > 10  && fetchSize <= 100) {
        fetchSize = 10;
    }
    else if(fetchSize > 100 && fetchSize <= 1000) {
        fetchSize = 100;
    }
    else {
        fetchSize = 1000;
    }

    
    ColumnBuffers buffers(numCols, fetchSize);

    // Bind columns
    ret = SQLBindColums(hStmt, buffers, columnNames, numCols, fetchSize);
    if (!SQL_SUCCEEDED(ret)) {
        std::wcerr << L"Error binding columns: " << SQLCheckError_Wrap(SQL_HANDLE_STMT, StatementHandle, ret) << std::endl;
        return ret;
    }

    SQLULEN numRowsFetched;
    SQLSetStmtAttr_ptr(hStmt, SQL_ATTR_ROW_ARRAY_SIZE, (SQLPOINTER)fetchSize, 0);
    SQLSetStmtAttr_ptr(hStmt, SQL_ATTR_ROWS_FETCHED_PTR, &numRowsFetched, 0);

    while(ret != SQL_NO_DATA) {
        ret = FetchBatchData(hStmt, buffers, columnNames, rows, numCols, numRowsFetched, fetchSize);
        if(!SQL_SUCCEEDED(ret) && ret != SQL_NO_DATA) {
            std::wcerr << L"Error fetching data: " << SQLCheckError_Wrap(SQL_HANDLE_STMT, StatementHandle, ret) << std::endl;
            return ret;
        }
    }

    return ret;
}

/**
 * FetchOne_wrap - Fetches a single row of data from the result set.
 * 
 * @param StatementHandle: Handle to the statement from which data is to be fetched.
 * @param row: A Python list that will be populated with the fetched row data.
 * 
 * @return SQLRETURN: SQL_SUCCESS or SQL_SUCCESS_WITH_INFO if data is fetched successfully,
 *                    SQL_NO_DATA if there are no more rows to fetch,
 *                    throws a runtime error if there is an error fetching data.
 * 
 * This function assumes that the statement handle (hStmt) is already allocated and a query has been executed.
 * It fetches the next row of data from the result set and populates the provided Python list with the row data.
 * If there are no more rows to fetch, it returns SQL_NO_DATA.
 * If an error occurs during fetching, it throws a runtime error.
 */
SQLRETURN FetchOne_wrap(intptr_t StatementHandle, py::list& row) {
    SQLRETURN ret;
    SQLHSTMT hStmt = reinterpret_cast<SQLHSTMT>(StatementHandle);

    // Assume hStmt is already allocated and a query has been executed
    ret = SQLFetch_ptr(hStmt);
    if (SQL_SUCCEEDED(ret)) {
        // Retrieve column count
        SQLSMALLINT colCount =  SQLNumResultCols_wrap(StatementHandle);
        ret = SQLGetData_wrap(StatementHandle, colCount, row);
        return ret;
    } else if (ret == SQL_NO_DATA) {
        return ret;
    } else {
        std::wcerr << L"Error fetching data: " << SQLCheckError_Wrap(SQL_HANDLE_STMT, StatementHandle, ret) << std::endl;
        return ret;
    }
}

// Wrap SQLMoreResults
SQLRETURN SQLMoreResults_wrap(intptr_t StatementHandle) {
    if (!SQLMoreResults_ptr && !LoadDriver()) {
        return SQL_ERROR;
    }

    #ifdef _DEBUG
        std::cout << "Check for more results\n";
    #endif

    return SQLMoreResults_ptr(reinterpret_cast<SQLHANDLE>(StatementHandle));
}

// Wrap SQLEndTran
SQLRETURN SQLEndTran_wrap(SQLSMALLINT HandleType, intptr_t Handle, SQLSMALLINT CompletionType) {
    if (!SQLEndTran_ptr && !LoadDriver()) {
        return SQL_ERROR;
    }

#ifdef _DEBUG
    std::cout << "End SQL Transaction" << std::endl;
#endif

    return SQLEndTran_ptr(HandleType, reinterpret_cast<SQLHANDLE>(Handle), CompletionType);
}

// Wrap SQLFreeHandle
SQLRETURN SQLFreeHandle_wrap(SQLSMALLINT HandleType, intptr_t Handle) {
    if (!SQLAllocHandle_ptr && !LoadDriver()) { // SQLAllocHandle_ptr ensures driver is loaded
        return SQL_ERROR;
    }

    #ifdef _DEBUG
        std::cout << "Free SQL Handle\n";
    #endif
    
    return SQLFreeHandle_ptr(HandleType, reinterpret_cast<SQLHANDLE>(Handle));
}

// Wrap SQLDisconnect
SQLRETURN SQLDisconnect_wrap(intptr_t ConnectionHandle) {
    if (!SQLDriverConnect_ptr && !LoadDriver()) { // Ensure the driver is loaded
        return SQL_ERROR;
    }

    #ifdef _DEBUG
        std::cout << "Disconnect from MSSQL\n";
    #endif
    return SQLDisconnect_ptr(reinterpret_cast<SQLHDBC>(ConnectionHandle));
}

// Bind the functions to the module
PYBIND11_MODULE(ddbc_bindings, m) {
    m.doc() = "msodbcsql driver api bindings for Python"; // optional module docstring
    m.def("ThrowStdException", &ThrowStdException);
    py::class_<ParamInfo>(m, "ParamInfo")
        .def(py::init<>())
        .def_readwrite("inputOutputType", &ParamInfo::inputOutputType)
        .def_readwrite("paramCType", &ParamInfo::paramCType)
        .def_readwrite("paramSQLType", &ParamInfo::paramSQLType)
        .def_readwrite("columnSize", &ParamInfo::columnSize)
        .def_readwrite("decimalDigits", &ParamInfo::decimalDigits);
    py::class_<NumericData>(m, "NumericData")
        .def(py::init<>())
        .def(py::init<SQLCHAR, SQLSCHAR, SQLCHAR, const std::string&>())  // Parameterized constructor
        .def_readwrite("precision", &NumericData::precision)
        .def_readwrite("scale", &NumericData::scale)
        .def_readwrite("sign", &NumericData::sign)
        .def_readwrite("val", &NumericData::val)
        .def("to_double", &NumericData::to_double);  // Expose the to_double method
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
    m.def("DDBCSQLFetchMany", &FetchMany_wrap, py::arg("StatementHandle"), py::arg("rows"), py::arg("fetchSize") = 1,"Fetch many rows from the result set");
    m.def("DDBCSQLFetchAll", &FetchAll_wrap, "Fetch all rows from the result set");
    m.def("DDBCSQLEndTran", &SQLEndTran_wrap, "End a transaction");
    m.def("DDBCSQLFreeHandle", &SQLFreeHandle_wrap, "Free a handle");
    m.def("DDBCSQLDisconnect", &SQLDisconnect_wrap, "Disconnect from a data source");
    m.def("DDBCSQLCheckError", &SQLCheckError_Wrap, "Check for driver errors");
}
