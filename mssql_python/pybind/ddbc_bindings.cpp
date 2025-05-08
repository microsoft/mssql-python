// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

// INFO|TODO - Note that is file is Windows specific right now. Making it arch agnostic will be
//             taken up in beta release

#include <pybind11/pybind11.h> // pybind11.h must be the first include - https://pybind11.readthedocs.io/en/latest/basics.html#header-and-namespace-conventions

#include <cstdint>
#include <iomanip>  // std::setw, std::setfill
#include <iostream>
#include <string>
#include <utility>  // std::forward

#include <pybind11/chrono.h>
#include <pybind11/complex.h>
#include <pybind11/functional.h>
#include <pybind11/pytypes.h>  // Add this line for datetime support
#include <pybind11/stl.h>
#include <windows.h>  // windows.h needs to be included before sql.h
#include <sql.h>
#include <sqlext.h>


namespace py = pybind11;
using namespace pybind11::literals;

//-------------------------------------------------------------------------------------------------
// Macro definitions
//-------------------------------------------------------------------------------------------------

// This constant is not exposed via sql.h, hence define it here
#define SQL_SS_TIME2 (-154)

#define MAX_DIGITS_IN_NUMERIC 64

#define STRINGIFY_FOR_CASE(x) \
    case x:                   \
        return #x

//-------------------------------------------------------------------------------------------------
// Class definitions
//-------------------------------------------------------------------------------------------------

// Struct to hold parameter information for binding. Used by SQLBindParameter.
// This struct is shared between C++ & Python code.
struct ParamInfo {
    SQLSMALLINT inputOutputType;
    SQLSMALLINT paramCType;
    SQLSMALLINT paramSQLType;
    SQLULEN columnSize;
    SQLSMALLINT decimalDigits;
    // TODO: Reuse python buffer for large data using Python buffer protocol
    // Stores pointer to the python object that holds parameter value
    // py::object* dataPtr;
};

// Mirrors the SQL_NUMERIC_STRUCT. But redefined to replace val char array
// with std::string, because pybind doesn't allow binding char array.
// This struct is shared between C++ & Python code.
struct NumericData {
    SQLCHAR precision;
    SQLSCHAR scale;
    SQLCHAR sign;  // 1=pos, 0=neg
    std::uint64_t val; // 123.45 -> 12345

    NumericData() : precision(0), scale(0), sign(0), val(0) {}

    NumericData(SQLCHAR precision, SQLSCHAR scale, SQLCHAR sign, std::uint64_t value)
        : precision(precision), scale(scale), sign(sign), val(value) {}
};

// Struct to hold data buffers and indicators for each column
struct ColumnBuffers {
    std::vector<std::vector<SQLCHAR>> charBuffers;
    std::vector<std::vector<SQLWCHAR>> wcharBuffers;
    std::vector<std::vector<SQLINTEGER>> intBuffers;
    std::vector<std::vector<SQLSMALLINT>> smallIntBuffers;
    std::vector<std::vector<SQLREAL>> realBuffers;
    std::vector<std::vector<SQLDOUBLE>> doubleBuffers;
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
          realBuffers(numCols),
          doubleBuffers(numCols),
          timestampBuffers(numCols),
          bigIntBuffers(numCols),
          dateBuffers(numCols),
          timeBuffers(numCols),
          guidBuffers(numCols),
          indicators(numCols, std::vector<SQLLEN>(fetchSize)) {}
};

// This struct is used to relay error info obtained from SQLDiagRec API to the Python module
struct ErrorInfo {
    std::wstring sqlState;
    std::wstring ddbcErrorMsg;
};


//-------------------------------------------------------------------------------------------------
// Function pointer typedefs
//-------------------------------------------------------------------------------------------------

// Handle APIs
typedef SQLRETURN (*SQLAllocHandleFunc)(SQLSMALLINT, SQLHANDLE, SQLHANDLE*);
typedef SQLRETURN (*SQLSetEnvAttrFunc)(SQLHANDLE, SQLINTEGER, SQLPOINTER, SQLINTEGER);
typedef SQLRETURN (*SQLSetConnectAttrFunc)(SQLHDBC, SQLINTEGER, SQLPOINTER, SQLINTEGER);
typedef SQLRETURN (*SQLSetStmtAttrFunc)(SQLHSTMT, SQLINTEGER, SQLPOINTER, SQLINTEGER);
typedef SQLRETURN (*SQLGetConnectAttrFunc)(SQLHDBC, SQLINTEGER, SQLPOINTER, SQLINTEGER,
                                           SQLINTEGER*);

// Connection and Execution APIs
typedef SQLRETURN (*SQLDriverConnectFunc)(SQLHANDLE, SQLHWND, SQLWCHAR*, SQLSMALLINT, SQLWCHAR*,
                                          SQLSMALLINT, SQLSMALLINT*, SQLUSMALLINT);
typedef SQLRETURN (*SQLExecDirectFunc)(SQLHANDLE, SQLWCHAR*, SQLINTEGER);
typedef SQLRETURN (*SQLPrepareFunc)(SQLHANDLE, SQLWCHAR*, SQLINTEGER);
typedef SQLRETURN (*SQLBindParameterFunc)(SQLHANDLE, SQLUSMALLINT, SQLSMALLINT, SQLSMALLINT,
                                          SQLSMALLINT, SQLULEN, SQLSMALLINT, SQLPOINTER, SQLLEN,
                                          SQLLEN*);
typedef SQLRETURN (*SQLExecuteFunc)(SQLHANDLE);
typedef SQLRETURN (*SQLRowCountFunc)(SQLHSTMT, SQLLEN*);
typedef SQLRETURN (*SQLSetDescFieldFunc)(SQLHDESC, SQLSMALLINT, SQLSMALLINT, SQLPOINTER, SQLINTEGER);
typedef SQLRETURN (*SQLGetStmtAttrFunc)(SQLHSTMT, SQLINTEGER, SQLPOINTER, SQLINTEGER, SQLINTEGER*);

// Data retrieval APIs
typedef SQLRETURN (*SQLFetchFunc)(SQLHANDLE);
typedef SQLRETURN (*SQLFetchScrollFunc)(SQLHANDLE, SQLSMALLINT, SQLLEN);
typedef SQLRETURN (*SQLGetDataFunc)(SQLHANDLE, SQLUSMALLINT, SQLSMALLINT, SQLPOINTER, SQLLEN,
                                    SQLLEN*);
typedef SQLRETURN (*SQLNumResultColsFunc)(SQLHSTMT, SQLSMALLINT*);
typedef SQLRETURN (*SQLBindColFunc)(SQLHSTMT, SQLUSMALLINT, SQLSMALLINT, SQLPOINTER, SQLLEN,
                                    SQLLEN*);
typedef SQLRETURN (*SQLDescribeColFunc)(SQLHSTMT, SQLUSMALLINT, SQLWCHAR*, SQLSMALLINT,
                                        SQLSMALLINT*, SQLSMALLINT*, SQLULEN*, SQLSMALLINT*,
                                        SQLSMALLINT*);
typedef SQLRETURN (*SQLMoreResultsFunc)(SQLHSTMT);
typedef SQLRETURN (*SQLColAttributeFunc)(SQLHSTMT, SQLUSMALLINT, SQLUSMALLINT, SQLPOINTER,
                                         SQLSMALLINT, SQLSMALLINT*, SQLPOINTER);

// Transaction APIs
typedef SQLRETURN (*SQLEndTranFunc)(SQLSMALLINT, SQLHANDLE, SQLSMALLINT);

// Disconnect/free APIs
typedef SQLRETURN (*SQLFreeHandleFunc)(SQLSMALLINT, SQLHANDLE);
typedef SQLRETURN (*SQLDisconnectFunc)(SQLHDBC);
typedef SQLRETURN (*SQLFreeStmtFunc)(SQLHSTMT, SQLUSMALLINT);

// Diagnostic APIs
typedef SQLRETURN (*SQLGetDiagRecFunc)(SQLSMALLINT, SQLHANDLE, SQLSMALLINT, SQLWCHAR*, SQLINTEGER*,
                                       SQLWCHAR*, SQLSMALLINT, SQLSMALLINT*);

//-------------------------------------------------------------------------------------------------
// Function pointer initialization
//-------------------------------------------------------------------------------------------------

// Handle APIs
SQLAllocHandleFunc SQLAllocHandle_ptr = nullptr;
SQLSetEnvAttrFunc SQLSetEnvAttr_ptr = nullptr;
SQLSetConnectAttrFunc SQLSetConnectAttr_ptr = nullptr;
SQLSetStmtAttrFunc SQLSetStmtAttr_ptr = nullptr;
SQLGetConnectAttrFunc SQLGetConnectAttr_ptr = nullptr;

// Connection and Execution APIs
SQLDriverConnectFunc SQLDriverConnect_ptr = nullptr;
SQLExecDirectFunc SQLExecDirect_ptr = nullptr;
SQLPrepareFunc SQLPrepare_ptr = nullptr;
SQLBindParameterFunc SQLBindParameter_ptr = nullptr;
SQLExecuteFunc SQLExecute_ptr = nullptr;
SQLRowCountFunc SQLRowCount_ptr = nullptr;
SQLGetStmtAttrFunc SQLGetStmtAttr_ptr = nullptr;
SQLSetDescFieldFunc SQLSetDescField_ptr = nullptr;

// Data retrieval APIs
SQLFetchFunc SQLFetch_ptr = nullptr;
SQLFetchScrollFunc SQLFetchScroll_ptr = nullptr;
SQLGetDataFunc SQLGetData_ptr = nullptr;
SQLNumResultColsFunc SQLNumResultCols_ptr = nullptr;
SQLBindColFunc SQLBindCol_ptr = nullptr;
SQLDescribeColFunc SQLDescribeCol_ptr = nullptr;
SQLMoreResultsFunc SQLMoreResults_ptr = nullptr;
SQLColAttributeFunc SQLColAttribute_ptr = nullptr;

// Transaction APIs
SQLEndTranFunc SQLEndTran_ptr = nullptr;

// Disconnect/free APIs
SQLFreeHandleFunc SQLFreeHandle_ptr = nullptr;
SQLDisconnectFunc SQLDisconnect_ptr = nullptr;
SQLFreeStmtFunc SQLFreeStmt_ptr = nullptr;

// Diagnostic APIs
SQLGetDiagRecFunc SQLGetDiagRec_ptr = nullptr;

// Smart wrapper around SQLHANDLE
class SqlHandle {
public:
    SqlHandle(SQLSMALLINT type, SQLHANDLE rawHandle) : _type(type), _handle(rawHandle) {}
    ~SqlHandle() {
        if (_handle) {
            SQLFreeHandle_ptr(_type, _handle);
            _handle = nullptr;
        }
    }
    SQLHANDLE get() const { return _handle; }

private:
    SQLSMALLINT _type;
    SQLHANDLE _handle;
};
using SqlHandlePtr = std::shared_ptr<SqlHandle>;

namespace {

// TODO: Revisit GIL considerations if we're using python's logger
template <typename... Args>
void LOG(const std::string& formatString, Args&&... args) {
    // TODO: Try to do this string concatenation at compile time
    std::string ddbcFormatString = "[DDBC Bindings log] " + formatString;
    static py::object logging = py::module_::import("mssql_python.logging_config")
	                            .attr("get_logger")();
    if (py::isinstance<py::none>(logging)) {
        return;
    }
    py::str message = py::str(ddbcFormatString).format(std::forward<Args>(args)...);
    logging.attr("debug")(message);
}

// TODO: Add more nuanced exception classes
void ThrowStdException(const std::string& message) { throw std::runtime_error(message); }

// Helper to load the driver
// TODO: We don't need to do explicit linking using LoadLibrary. We can just use implicit
//       linking to load this DLL. It will simplify the code a lot.
void LoadDriverOrThrowException() {
    HMODULE hDdbcModule;
    wchar_t ddbcModulePath[MAX_PATH];
    // Get the path to DDBC module:
    // GetModuleHandleExW returns a handle to current shared library (ddbc_bindings.pyd) given a
    // function from the library (LoadDriverOrThrowException). GetModuleFileNameW takes in the
    // library handle (hDdbcModule) & returns the full path to this library (ddbcModulePath)
    if (GetModuleHandleExW(
            GET_MODULE_HANDLE_EX_FLAG_FROM_ADDRESS | GET_MODULE_HANDLE_EX_FLAG_UNCHANGED_REFCOUNT,
            (LPWSTR)&LoadDriverOrThrowException, &hDdbcModule) &&
        GetModuleFileNameW(hDdbcModule, ddbcModulePath, MAX_PATH)) {
        // Look for last occurence of '\' in the path and set it to null
        wchar_t* lastBackSlash = wcsrchr(ddbcModulePath, L'\\');
        if (lastBackSlash == nullptr) {
            LOG("Invalid DDBC module path - %S", ddbcModulePath);
            ThrowStdException("Failed to load driver");
        }
        *lastBackSlash = 0;
    } else {
        LOG("Failed to get DDBC module path. Error code - %d", GetLastError());
        ThrowStdException("Failed to load driver");
    }

    // Look for msodbcsql18.dll in a path relative to DDBC module
    std::wstring dllDir = std::wstring(ddbcModulePath) + L"\\libs\\win\\msodbcsql18.dll";
    HMODULE hModule = LoadLibraryW(dllDir.c_str());
    if (!hModule) {
        LOG("LoadLibraryW failed to load driver from - %S", dllDir.c_str());
        ThrowStdException("Failed to load driver");
    }
    LOG("Driver loaded successfully from - {}", dllDir.c_str());

    // Environment and handle function loading
    SQLAllocHandle_ptr = (SQLAllocHandleFunc)GetProcAddress(hModule, "SQLAllocHandle");
    SQLSetEnvAttr_ptr = (SQLSetEnvAttrFunc)GetProcAddress(hModule, "SQLSetEnvAttr");
    SQLSetConnectAttr_ptr = (SQLSetConnectAttrFunc)GetProcAddress(hModule, "SQLSetConnectAttrW");
    SQLSetStmtAttr_ptr = (SQLSetStmtAttrFunc)GetProcAddress(hModule, "SQLSetStmtAttrW");
    SQLGetConnectAttr_ptr = (SQLGetConnectAttrFunc)GetProcAddress(hModule, "SQLGetConnectAttrW");

    // Connection and statement function loading
    SQLDriverConnect_ptr = (SQLDriverConnectFunc)GetProcAddress(hModule, "SQLDriverConnectW");
    SQLExecDirect_ptr = (SQLExecDirectFunc)GetProcAddress(hModule, "SQLExecDirectW");
    SQLPrepare_ptr = (SQLPrepareFunc)GetProcAddress(hModule, "SQLPrepareW");
    SQLBindParameter_ptr = (SQLBindParameterFunc)GetProcAddress(hModule, "SQLBindParameter");
    SQLExecute_ptr = (SQLExecuteFunc)GetProcAddress(hModule, "SQLExecute");
    SQLRowCount_ptr = (SQLRowCountFunc)GetProcAddress(hModule, "SQLRowCount");
    SQLGetStmtAttr_ptr = (SQLGetStmtAttrFunc)GetProcAddress(hModule, "SQLGetStmtAttrW");
    SQLSetDescField_ptr = (SQLSetDescFieldFunc)GetProcAddress(hModule, "SQLSetDescFieldW");

    // Fetch and data retrieval function loading
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

    // Disconnect and free functions loading
    SQLFreeHandle_ptr = (SQLFreeHandleFunc)GetProcAddress(hModule, "SQLFreeHandle");
    SQLDisconnect_ptr = (SQLDisconnectFunc)GetProcAddress(hModule, "SQLDisconnect");
    SQLFreeStmt_ptr = (SQLFreeStmtFunc)GetProcAddress(hModule, "SQLFreeStmt");

    // Diagnostic record function Loading
    SQLGetDiagRec_ptr = (SQLGetDiagRecFunc)GetProcAddress(hModule, "SQLGetDiagRecW");

    bool success = SQLAllocHandle_ptr && SQLSetEnvAttr_ptr && SQLSetConnectAttr_ptr &&
                   SQLSetStmtAttr_ptr && SQLGetConnectAttr_ptr && SQLDriverConnect_ptr &&
                   SQLExecDirect_ptr && SQLPrepare_ptr && SQLBindParameter_ptr && SQLExecute_ptr &&
                   SQLRowCount_ptr && SQLGetStmtAttr_ptr && SQLSetDescField_ptr && SQLFetch_ptr &&
		   SQLFetchScroll_ptr && SQLGetData_ptr && SQLNumResultCols_ptr &&
		   SQLBindCol_ptr && SQLDescribeCol_ptr && SQLMoreResults_ptr &&
		   SQLColAttribute_ptr && SQLEndTran_ptr && SQLFreeHandle_ptr &&
		   SQLDisconnect_ptr && SQLFreeStmt_ptr && SQLGetDiagRec_ptr;

    if (!success) {
        LOG("Failed to load required function pointers from driver - %S", dllDir.c_str());
        ThrowStdException("Failed to load required function pointers from driver");
    }
    LOG("Sucessfully loaded function pointers from driver");
}

const char* GetSqlCTypeAsString(const SQLSMALLINT cType) {
    switch (cType) {
        STRINGIFY_FOR_CASE(SQL_C_CHAR);
        STRINGIFY_FOR_CASE(SQL_C_WCHAR);
        STRINGIFY_FOR_CASE(SQL_C_SSHORT);
        STRINGIFY_FOR_CASE(SQL_C_USHORT);
        STRINGIFY_FOR_CASE(SQL_C_SHORT);
        STRINGIFY_FOR_CASE(SQL_C_SLONG);
        STRINGIFY_FOR_CASE(SQL_C_ULONG);
        STRINGIFY_FOR_CASE(SQL_C_LONG);
        STRINGIFY_FOR_CASE(SQL_C_STINYINT);
        STRINGIFY_FOR_CASE(SQL_C_UTINYINT);
        STRINGIFY_FOR_CASE(SQL_C_TINYINT);
        STRINGIFY_FOR_CASE(SQL_C_SBIGINT);
        STRINGIFY_FOR_CASE(SQL_C_UBIGINT);
        STRINGIFY_FOR_CASE(SQL_C_FLOAT);
        STRINGIFY_FOR_CASE(SQL_C_DOUBLE);
        STRINGIFY_FOR_CASE(SQL_C_BIT);
        STRINGIFY_FOR_CASE(SQL_C_BINARY);
        STRINGIFY_FOR_CASE(SQL_C_TYPE_DATE);
        STRINGIFY_FOR_CASE(SQL_C_TYPE_TIME);
        STRINGIFY_FOR_CASE(SQL_C_TYPE_TIMESTAMP);
        STRINGIFY_FOR_CASE(SQL_C_NUMERIC);
        STRINGIFY_FOR_CASE(SQL_C_GUID);
        STRINGIFY_FOR_CASE(SQL_C_DEFAULT);
        default:
            return "Unkown";
    }
}

std::string MakeParamMismatchErrorStr(const SQLSMALLINT cType, const int paramIndex) {
    std::string errorString =
        "Parameter's object type does not match parameter's C type. paramIndex - " +
        std::to_string(paramIndex) + ", C type - " + GetSqlCTypeAsString(cType);
    return errorString;
}

// This function allocates a buffer of ParamType, stores it as a void* in paramBuffers for
// book-keeping and then returns a ParamType* to the allocated memory.
// ctorArgs are the arguments to ParamType's constructor used while creating/allocating ParamType
template <typename ParamType, typename... CtorArgs>
ParamType* AllocateParamBuffer(std::vector<std::shared_ptr<void>>& paramBuffers,
                               CtorArgs&&... ctorArgs) {
    paramBuffers.emplace_back(new ParamType(std::forward<CtorArgs>(ctorArgs)...),
                              std::default_delete<ParamType>());
    return static_cast<ParamType*>(paramBuffers.back().get());
}

// Given a list of parameters and their ParamInfo, calls SQLBindParameter on each of them with
// appropriate arguments
SQLRETURN BindParameters(SQLHANDLE hStmt, const py::list& params,
                         const std::vector<ParamInfo>& paramInfos,
                         std::vector<std::shared_ptr<void>>& paramBuffers) {
    for (int paramIndex = 0; paramIndex < params.size(); paramIndex++) {
        const auto& param = params[paramIndex];
        const ParamInfo& paramInfo = paramInfos[paramIndex];
        void* dataPtr = nullptr;
        SQLLEN bufferLength = 0;
        SQLLEN* strLenOrIndPtr = nullptr;

        // TODO: Add more data types like money, guid, interval, TVPs etc.
        switch (paramInfo.paramCType) {
            case SQL_C_CHAR:
            case SQL_C_BINARY: {
                if (!py::isinstance<py::str>(param) && !py::isinstance<py::bytearray>(param) &&
                    !py::isinstance<py::bytes>(param)) {
                    ThrowStdException(MakeParamMismatchErrorStr(paramInfo.paramCType, paramIndex));
                }
                std::string* strParam =
                    AllocateParamBuffer<std::string>(paramBuffers, param.cast<std::string>());
                if (strParam->size() > 8192 /* TODO: Fix max length */) {
                    ThrowStdException(
                        "Streaming parameters is not yet supported. Parameter size"
                        " must be less than 8192 bytes");
                }
                dataPtr = const_cast<void*>(static_cast<const void*>(strParam->c_str()));
                bufferLength = strParam->size() + 1 /* null terminator */;
                strLenOrIndPtr = AllocateParamBuffer<SQLLEN>(paramBuffers);
                *strLenOrIndPtr = SQL_NTS;
                break;
            }
            case SQL_C_WCHAR: {
                if (!py::isinstance<py::str>(param) && !py::isinstance<py::bytearray>(param) &&
                    !py::isinstance<py::bytes>(param)) {
                    ThrowStdException(MakeParamMismatchErrorStr(paramInfo.paramCType, paramIndex));
                }
                std::wstring* strParam =
                    AllocateParamBuffer<std::wstring>(paramBuffers, param.cast<std::wstring>());
                if (strParam->size() > 4096 /* TODO: Fix max length */) {
                    ThrowStdException(
                        "Streaming parameters is not yet supported. Parameter size"
                        " must be less than 8192 bytes");
                }
                dataPtr = const_cast<void*>(static_cast<const void*>(strParam->c_str()));
                bufferLength = (strParam->size() + 1 /* null terminator */) * sizeof(wchar_t);
                strLenOrIndPtr = AllocateParamBuffer<SQLLEN>(paramBuffers);
                *strLenOrIndPtr = SQL_NTS;
                break;
            }
            case SQL_C_BIT: {
                if (!py::isinstance<py::bool_>(param)) {
                    ThrowStdException(MakeParamMismatchErrorStr(paramInfo.paramCType, paramIndex));
                }
                dataPtr =
                    static_cast<void*>(AllocateParamBuffer<bool>(paramBuffers, param.cast<bool>()));
                break;
            }
            case SQL_C_DEFAULT: {
                if (!py::isinstance<py::none>(param)) {
                    ThrowStdException(MakeParamMismatchErrorStr(paramInfo.paramCType, paramIndex));
                }
                // TODO: This wont work for None values added to BINARY/VARBINARY columns. None values
                //       of binary columns need to have C type = SQL_C_BINARY & SQL type = SQL_BINARY
                dataPtr = nullptr;
                strLenOrIndPtr = AllocateParamBuffer<SQLLEN>(paramBuffers);
                *strLenOrIndPtr = SQL_NULL_DATA;
                break;
            }
            case SQL_C_STINYINT:
            case SQL_C_TINYINT:
            case SQL_C_SSHORT:
            case SQL_C_SHORT: {
                if (!py::isinstance<py::int_>(param)) {
                    ThrowStdException(MakeParamMismatchErrorStr(paramInfo.paramCType, paramIndex));
                }
                dataPtr =
                    static_cast<void*>(AllocateParamBuffer<int>(paramBuffers, param.cast<int>()));
                break;
            }
            case SQL_C_UTINYINT:
            case SQL_C_USHORT: {
                if (!py::isinstance<py::int_>(param)) {
                    ThrowStdException(MakeParamMismatchErrorStr(paramInfo.paramCType, paramIndex));
                }
                dataPtr = static_cast<void*>(
                    AllocateParamBuffer<unsigned int>(paramBuffers, param.cast<unsigned int>()));
                break;
            }
            case SQL_C_SBIGINT:
            case SQL_C_SLONG:
            case SQL_C_LONG: {
                if (!py::isinstance<py::int_>(param)) {
                    ThrowStdException(MakeParamMismatchErrorStr(paramInfo.paramCType, paramIndex));
                }
                dataPtr = static_cast<void*>(
                    AllocateParamBuffer<int64_t>(paramBuffers, param.cast<int64_t>()));
                break;
            }
            case SQL_C_UBIGINT:
            case SQL_C_ULONG: {
                if (!py::isinstance<py::int_>(param)) {
                    ThrowStdException(MakeParamMismatchErrorStr(paramInfo.paramCType, paramIndex));
                }
                dataPtr = static_cast<void*>(
                    AllocateParamBuffer<uint64_t>(paramBuffers, param.cast<uint64_t>()));
                break;
            }
            case SQL_C_FLOAT: {
                if (!py::isinstance<py::float_>(param)) {
                    ThrowStdException(MakeParamMismatchErrorStr(paramInfo.paramCType, paramIndex));
                }
                dataPtr = static_cast<void*>(
                    AllocateParamBuffer<float>(paramBuffers, param.cast<float>()));
                break;
            }
            case SQL_C_DOUBLE: {
                if (!py::isinstance<py::float_>(param)) {
                    ThrowStdException(MakeParamMismatchErrorStr(paramInfo.paramCType, paramIndex));
                }
                dataPtr = static_cast<void*>(
                    AllocateParamBuffer<double>(paramBuffers, param.cast<double>()));
                break;
            }
            case SQL_C_TYPE_DATE: {
                py::object dateType = py::module_::import("datetime").attr("date");
                if (!py::isinstance(param, dateType)) {
                    ThrowStdException(MakeParamMismatchErrorStr(paramInfo.paramCType, paramIndex));
                }
                // TODO: can be moved to python by registering SQL_DATE_STRUCT in pybind
                SQL_DATE_STRUCT* sqlDatePtr = AllocateParamBuffer<SQL_DATE_STRUCT>(paramBuffers);
                sqlDatePtr->year = param.attr("year").cast<int>();
                sqlDatePtr->month = param.attr("month").cast<int>();
                sqlDatePtr->day = param.attr("day").cast<int>();
                dataPtr = static_cast<void*>(sqlDatePtr);
                break;
            }
            case SQL_C_TYPE_TIME: {
                py::object timeType = py::module_::import("datetime").attr("time");
                if (!py::isinstance(param, timeType)) {
                    ThrowStdException(MakeParamMismatchErrorStr(paramInfo.paramCType, paramIndex));
                }
                // TODO: can be moved to python by registering SQL_TIME_STRUCT in pybind
                SQL_TIME_STRUCT* sqlTimePtr = AllocateParamBuffer<SQL_TIME_STRUCT>(paramBuffers);
                sqlTimePtr->hour = param.attr("hour").cast<int>();
                sqlTimePtr->minute = param.attr("minute").cast<int>();
                sqlTimePtr->second = param.attr("second").cast<int>();
                dataPtr = static_cast<void*>(sqlTimePtr);
                break;
            }
            case SQL_C_TYPE_TIMESTAMP: {
                py::object datetimeType = py::module_::import("datetime").attr("datetime");
                if (!py::isinstance(param, datetimeType)) {
                    ThrowStdException(MakeParamMismatchErrorStr(paramInfo.paramCType, paramIndex));
                }
                SQL_TIMESTAMP_STRUCT* sqlTimestampPtr =
                    AllocateParamBuffer<SQL_TIMESTAMP_STRUCT>(paramBuffers);
                sqlTimestampPtr->year = param.attr("year").cast<int>();
                sqlTimestampPtr->month = param.attr("month").cast<int>();
                sqlTimestampPtr->day = param.attr("day").cast<int>();
                sqlTimestampPtr->hour = param.attr("hour").cast<int>();
                sqlTimestampPtr->minute = param.attr("minute").cast<int>();
                sqlTimestampPtr->second = param.attr("second").cast<int>();
                // SQL server supports in ns, but python datetime supports in µs
                sqlTimestampPtr->fraction = static_cast<SQLUINTEGER>(
                    param.attr("microsecond").cast<int>() * 1000);  // Convert µs to ns
                dataPtr = static_cast<void*>(sqlTimestampPtr);
                break;
            }
            case SQL_C_NUMERIC: {
                if (!py::isinstance<NumericData>(param)) {
                    ThrowStdException(MakeParamMismatchErrorStr(paramInfo.paramCType, paramIndex));
                }
                NumericData decimalParam = param.cast<NumericData>();
                LOG("Received numeric parameter: precision - {}, scale- {}, sign - {}, value - {}",
                    decimalParam.precision, decimalParam.scale, decimalParam.sign,
                    decimalParam.val);
                SQL_NUMERIC_STRUCT* decimalPtr =
                    AllocateParamBuffer<SQL_NUMERIC_STRUCT>(paramBuffers);
                decimalPtr->precision = decimalParam.precision;
                decimalPtr->scale = decimalParam.scale;
                decimalPtr->sign = decimalParam.sign;
                // Convert the integer decimalParam.val to char array
                std:memset(static_cast<void*>(decimalPtr->val), 0, sizeof(decimalPtr->val));
                std::memcpy(static_cast<void*>(decimalPtr->val),
			    reinterpret_cast<char*>(&decimalParam.val),
                            sizeof(decimalParam.val));
                dataPtr = static_cast<void*>(decimalPtr);
                // TODO: Remove these lines
                //strLenOrIndPtr = AllocateParamBuffer<SQLLEN>(paramBuffers);
                //*strLenOrIndPtr = sizeof(SQL_NUMERIC_STRUCT);
                break;
            }
            case SQL_C_GUID: {
                // TODO
            }
            default: {
                std::ostringstream errorString;
                errorString << "Unsupported parameter type - " << paramInfo.paramCType
                            << " for parameter - " << paramIndex;
                ThrowStdException(errorString.str());
            }
        }
        assert(SQLBindParameter_ptr && SQLGetStmtAttr_ptr && SQLSetDescField_ptr);

        RETCODE rc = SQLBindParameter_ptr(
            hStmt, paramIndex + 1 /* 1-based indexing */, paramInfo.inputOutputType,
            paramInfo.paramCType, paramInfo.paramSQLType, paramInfo.columnSize,
            paramInfo.decimalDigits, dataPtr, bufferLength, strLenOrIndPtr);
        if (!SQL_SUCCEEDED(rc)) {
            LOG("Error when binding parameter - {}", paramIndex);
            return rc;
        }
	// Special handling for Numeric type -
	// https://learn.microsoft.com/en-us/sql/odbc/reference/appendixes/retrieve-numeric-data-sql-numeric-struct-kb222831?view=sql-server-ver16#sql_c_numeric-overview
        if (paramInfo.paramCType == SQL_C_NUMERIC) {
            SQLHDESC hDesc = nullptr;
            RETCODE rc = SQLGetStmtAttr_ptr(hStmt, SQL_ATTR_APP_PARAM_DESC, &hDesc, 0, NULL);
            if(!SQL_SUCCEEDED(rc)) {
                LOG("Error when getting statement attribute - {}", paramIndex);
                return rc;
            }
            rc = SQLSetDescField_ptr(hDesc, 1, SQL_DESC_TYPE, (SQLPOINTER) SQL_C_NUMERIC, 0);
            if(!SQL_SUCCEEDED(rc)) {
                LOG("Error when setting descriptor field SQL_DESC_TYPE - {}", paramIndex);
                return rc;
            }
            SQL_NUMERIC_STRUCT* numericPtr = reinterpret_cast<SQL_NUMERIC_STRUCT*>(dataPtr);
            rc = SQLSetDescField_ptr(hDesc, 1, SQL_DESC_PRECISION,
			             (SQLPOINTER) numericPtr->precision, 0);
            if(!SQL_SUCCEEDED(rc)) {
                LOG("Error when setting descriptor field SQL_DESC_PRECISION - {}", paramIndex);
                return rc;
            }

            rc = SQLSetDescField_ptr(hDesc, 1, SQL_DESC_SCALE,
			             (SQLPOINTER) numericPtr->scale, 0);
            if(!SQL_SUCCEEDED(rc)) {
                LOG("Error when setting descriptor field SQL_DESC_SCALE - {}", paramIndex);
                return rc;
            }

            rc = SQLSetDescField_ptr(hDesc, 1, SQL_DESC_DATA_PTR, (SQLPOINTER) numericPtr, 0);
            if(!SQL_SUCCEEDED(rc)) {
                LOG("Error when setting descriptor field SQL_DESC_DATA_PTR - {}", paramIndex);
                return rc;
            }
        }
    }
    return SQL_SUCCESS;
}

// This is temporary hack to avoid crash when SQLDescribeCol returns 0 as columnSize
// for NVARCHAR(MAX) & similar types. Variable length data needs more nuanced handling.
// TODO: Fix this in beta
// This function sets the buffer allocated to fetch NVARCHAR(MAX) & similar types to
// 4096 chars. So we'll retrieve data upto 4096. Anything greater then that will throw
// error
void HandleZeroColumnSizeAtFetch(SQLULEN& columnSize) {
    if (columnSize == 0) {
        columnSize = 4096;
    }
}

}  // namespace

// Wrap SQLAllocHandle
SQLRETURN SQLAllocHandle_wrap(SQLSMALLINT HandleType, SqlHandlePtr InputHandle, SqlHandlePtr& OutputHandle) {
    LOG("Allocate SQL Handle");
    if (!SQLAllocHandle_ptr) {
        LoadDriverOrThrowException();
    }

    SQLHANDLE rawOutputHandle = nullptr;
    SQLRETURN ret = SQLAllocHandle_ptr(HandleType, InputHandle ? InputHandle->get() : nullptr, &rawOutputHandle);
    if (!SQL_SUCCEEDED(ret)) {
        LOG("Failed to allocate handle");
        return ret;
    }
    OutputHandle = std::make_shared<SqlHandle>(HandleType, rawOutputHandle);
    return ret;
}

// Wrap SQLSetEnvAttr
SQLRETURN SQLSetEnvAttr_wrap(SqlHandlePtr EnvHandle, SQLINTEGER Attribute, intptr_t ValuePtr,
                             SQLINTEGER StringLength) {
    LOG("Set SQL environment Attribute");
    if (!SQLSetEnvAttr_ptr) {
        LoadDriverOrThrowException();
    }

    // TODO: Does ValuePtr need to be converted from Python to C++ object?
    SQLRETURN ret =  SQLSetEnvAttr_ptr(EnvHandle->get(), Attribute, reinterpret_cast<SQLPOINTER>(ValuePtr), StringLength);
    if (!SQL_SUCCEEDED(ret)) {
        LOG("Failed to set environment attribute");
    }
    return ret;
}

// Wrap SQLSetConnectAttr
SQLRETURN SQLSetConnectAttr_wrap(SqlHandlePtr ConnectionHandle, SQLINTEGER Attribute, intptr_t ValuePtr,
                                 SQLINTEGER StringLength) {
    LOG("Set SQL Connection Attribute");
    if (!SQLSetConnectAttr_ptr) {
        LoadDriverOrThrowException();
    }

    // TODO: Does ValuePtr need to be converted from Python to C++ object?
    SQLRETURN ret =  SQLSetConnectAttr_ptr(ConnectionHandle->get(), Attribute, reinterpret_cast<SQLPOINTER>(ValuePtr), StringLength);
    if (!SQL_SUCCEEDED(ret)) {
        LOG("Failed to set Connection attribute");
    }
    return ret;
}

// Wrap SQLSetStmtAttr
SQLRETURN SQLSetStmtAttr_wrap(SqlHandlePtr StatementHandle, SQLINTEGER Attribute, intptr_t ValuePtr,
                              SQLINTEGER StringLength) {
    LOG("Set SQL Statement Attribute");
    if (!SQLSetConnectAttr_ptr) {
        LoadDriverOrThrowException();
    }

    // TODO: Does ValuePtr need to be converted from Python to C++ object?
    SQLRETURN ret = SQLSetStmtAttr_ptr(StatementHandle->get(), Attribute, reinterpret_cast<SQLPOINTER>(ValuePtr), StringLength);
    if (!SQL_SUCCEEDED(ret)) {
        LOG("Failed to set Statement attribute");
    }
    return ret;
}

// Wrap SQLGetConnectionAttrA
// Currently only supports retrieval of int-valued attributes
// TODO: add support to retrieve all types of attributes
SQLINTEGER SQLGetConnectionAttr_wrap(SqlHandlePtr ConnectionHandle, SQLINTEGER attribute) {
    LOG("Get SQL COnnection Attribute");
    if (!SQLGetConnectAttr_ptr) {
        LoadDriverOrThrowException();
    }

    SQLINTEGER stringLength;
    SQLINTEGER intValue;

    // Try to get the attribute as an integer
    SQLGetConnectAttr_ptr(ConnectionHandle->get(), attribute, &intValue,
                          sizeof(SQLINTEGER), &stringLength);
    return intValue;
}

// Helper function to check for driver errors
ErrorInfo SQLCheckError_Wrap(SQLSMALLINT handleType, SqlHandlePtr handle, SQLRETURN retcode) {
    LOG("Checking errors for retcode - {}" , retcode);
    ErrorInfo errorInfo;
    if (retcode == SQL_INVALID_HANDLE) {
        LOG("Invalid handle received");
        errorInfo.ddbcErrorMsg = std::wstring( L"Invalid handle!");
        return errorInfo;
    }
    assert(handle != 0);
    SQLHANDLE rawHandle = handle->get();
    if (!SQL_SUCCEEDED(retcode)) {
        if (!SQLGetDiagRec_ptr) {
            LoadDriverOrThrowException();
        }

        SQLWCHAR sqlState[6], message[SQL_MAX_MESSAGE_LENGTH];
        SQLINTEGER nativeError;
        SQLSMALLINT messageLen;

        SQLRETURN diagReturn =
            SQLGetDiagRec_ptr(handleType, rawHandle, 1, sqlState,
                              &nativeError, message, SQL_MAX_MESSAGE_LENGTH, &messageLen);

        if (SQL_SUCCEEDED(diagReturn)) {
            errorInfo.sqlState = std::wstring(sqlState);
            errorInfo.ddbcErrorMsg = std::wstring(message);
        }
    }
    return errorInfo;
}

// Wrap SQLDriverConnect
SQLRETURN SQLDriverConnect_wrap(SqlHandlePtr ConnectionHandle, intptr_t WindowHandle, const std::wstring& ConnectionString) {
    LOG("Driver Connect to MSSQL");
    if (!SQLDriverConnect_ptr) {
        LoadDriverOrThrowException();
    }
    SQLRETURN ret = SQLDriverConnect_ptr(ConnectionHandle->get(),
                                reinterpret_cast<SQLHWND>(WindowHandle),
                                const_cast<SQLWCHAR*>(ConnectionString.c_str()), SQL_NTS, nullptr,
                                0, nullptr, SQL_DRIVER_NOPROMPT);
    if (!SQL_SUCCEEDED(ret)) {
        LOG("Failed to connect to DB");
    }
    return ret;
}

// Wrap SQLExecDirect
SQLRETURN SQLExecDirect_wrap(SqlHandlePtr StatementHandle, const std::wstring& Query) {
    LOG("Execute SQL query directly - {}", Query.c_str());
    if (!SQLExecDirect_ptr) {
        LoadDriverOrThrowException();
    }

    SQLRETURN ret = SQLExecDirect_ptr(StatementHandle->get(), const_cast<SQLWCHAR*>(Query.c_str()), SQL_NTS);
    if (!SQL_SUCCEEDED(ret)) {
        LOG("Failed to execute query directly");
    }
    return ret;
}

// Executes the provided query. If the query is parametrized, it prepares the statement and
// binds the parameters. Otherwise, it executes the query directly.
// 'usePrepare' parameter can be used to disable the prepare step for queries that might already
// be prepared in a previous call.
SQLRETURN SQLExecute_wrap(const SqlHandlePtr statementHandle,
                          const std::wstring& query /* TODO: Use SQLTCHAR? */,
                          const py::list& params, const std::vector<ParamInfo>& paramInfos,
                          py::list& isStmtPrepared, const bool usePrepare = true) {
    LOG("Execute SQL Query - {}", query.c_str());
    if (!SQLPrepare_ptr) {
        LoadDriverOrThrowException();
    }
    assert(SQLPrepare_ptr && SQLBindParameter_ptr && SQLExecute_ptr && SQLExecDirect_ptr);

    if (params.size() != paramInfos.size()) {
        // TODO: This should be a special internal exception, that python wont relay to users as is
        ThrowStdException("Number of parameters and paramInfos do not match");
    }

    RETCODE rc;
    SQLHANDLE hStmt = statementHandle->get();
    if (!statementHandle || !statementHandle->get()) {
        LOG("Statement handle is null or empty");
    }
    SQLWCHAR* queryPtr = const_cast<SQLWCHAR*>(query.c_str());
    if (params.size() == 0) {
        // Execute statement directly if the statement is not parametrized. This is the
        // fastest way to submit a SQL statement for one-time execution according to
        // DDBC documentation -
        // https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlexecdirect-function?view=sql-server-ver16
        rc = SQLExecDirect_ptr(hStmt, queryPtr, SQL_NTS);
        if (!SQL_SUCCEEDED(rc) && rc != SQL_NO_DATA) {
            LOG("Error during direct execution of the statement");
        }
        return rc;
    } else {
        // isStmtPrepared is a list instead of a bool coz bools in Python are immutable.
        // Hence, we can't pass around bools by reference & modify them. Therefore, isStmtPrepared
        // must be a list with exactly one bool element
        assert(isStmtPrepared.size() == 1);
        if (usePrepare) {
            rc = SQLPrepare_ptr(hStmt, queryPtr, SQL_NTS);
            if (!SQL_SUCCEEDED(rc)) {
                LOG("Error while preparing the statement");
                return rc;
            }
            isStmtPrepared[0] = py::cast(true);
        } else {
            // Make sure the statement has been prepared earlier if we're not preparing now
            bool isStmtPreparedAsBool = isStmtPrepared[0].cast<bool>();
            if (!isStmtPreparedAsBool) {
                // TODO: Print the query
                ThrowStdException("Cannot execute unprepared statement");
            }
        }

        // This vector manages the heap memory allocated for parameter buffers.
        // It must be in scope until SQLExecute is done.
        std::vector<std::shared_ptr<void>> paramBuffers;
        rc = BindParameters(hStmt, params, paramInfos, paramBuffers);
        if (!SQL_SUCCEEDED(rc)) {
            return rc;
        }

        rc = SQLExecute_ptr(hStmt);
        if (!SQL_SUCCEEDED(rc) && rc != SQL_NO_DATA) {
            LOG("DDBCSQLExecute: Error during execution of the statement");
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
SQLSMALLINT SQLNumResultCols_wrap(SqlHandlePtr statementHandle) {
    LOG("Get number of columns in result set");
    if (!SQLNumResultCols_ptr) {
        LoadDriverOrThrowException();
    }

    SQLSMALLINT columnCount;
    // TODO: Handle the return code
    SQLNumResultCols_ptr(statementHandle->get(), &columnCount);
    return columnCount;
}

// Wrap SQLDescribeCol
SQLRETURN SQLDescribeCol_wrap(SqlHandlePtr StatementHandle, py::list& ColumnMetadata) {
    LOG("Get column description");
    if (!SQLDescribeCol_ptr) {
        LoadDriverOrThrowException();
    }

    SQLSMALLINT ColumnCount;
    SQLRETURN retcode =
        SQLNumResultCols_ptr(StatementHandle->get(), &ColumnCount);
    if (!SQL_SUCCEEDED(retcode)) {
        LOG("Failed to get number of columns");
        return retcode;
    }

    for (SQLUSMALLINT i = 1; i <= ColumnCount; ++i) {
        SQLWCHAR ColumnName[256];
        SQLSMALLINT NameLength;
        SQLSMALLINT DataType;
        SQLULEN ColumnSize;
        SQLSMALLINT DecimalDigits;
        SQLSMALLINT Nullable;

        retcode = SQLDescribeCol_ptr(StatementHandle->get(), i, ColumnName,
                                     sizeof(ColumnName) / sizeof(SQLWCHAR), &NameLength, &DataType,
                                     &ColumnSize, &DecimalDigits, &Nullable);

        if (SQL_SUCCEEDED(retcode)) {
            // Append a named py::dict to ColumnMetadata
            // TODO: Should we define a struct for this task instead of dict?
            ColumnMetadata.append(py::dict("ColumnName"_a = std::wstring(ColumnName),
                                           "DataType"_a = DataType, "ColumnSize"_a = ColumnSize,
                                           "DecimalDigits"_a = DecimalDigits,
                                           "Nullable"_a = Nullable));
        } else {
            return retcode;
        }
    }
    return SQL_SUCCESS;
}

// Wrap SQLFetch to retrieve rows
SQLRETURN SQLFetch_wrap(SqlHandlePtr StatementHandle) {
    LOG("Fetch next row");
    if (!SQLFetch_ptr) {
        LoadDriverOrThrowException();
    }

    return SQLFetch_ptr(StatementHandle->get());
}

// Helper function to retrieve column data
// TODO: Handle variable length data correctly
SQLRETURN SQLGetData_wrap(SqlHandlePtr StatementHandle, SQLUSMALLINT colCount, py::list& row) {
    LOG("Get data from columns");
    if (!SQLGetData_ptr) {
        LoadDriverOrThrowException();
    }

    SQLRETURN ret;
    SQLHSTMT hStmt = StatementHandle->get();
    for (SQLSMALLINT i = 1; i <= colCount; ++i) {
        SQLWCHAR columnName[256];
        SQLSMALLINT columnNameLen;
        SQLSMALLINT dataType;
        SQLULEN columnSize;
        SQLSMALLINT decimalDigits;
        SQLSMALLINT nullable;

        ret = SQLDescribeCol_ptr(hStmt, i, columnName, sizeof(columnName) / sizeof(SQLWCHAR),
                                 &columnNameLen, &dataType, &columnSize, &decimalDigits, &nullable);
        if (!SQL_SUCCEEDED(ret)) {
            LOG("Error retrieving data for column - {}, SQLDescribeCol return code - {}", i, ret);
            row.append(py::none());
            // TODO: Do we want to continue in this case or return?
            continue;
        }

        switch (dataType) {
            case SQL_CHAR:
            case SQL_VARCHAR:
            case SQL_LONGVARCHAR: {
                // TODO: revisit
                HandleZeroColumnSizeAtFetch(columnSize);
		uint64_t fetchBufferSize = columnSize + 1 /* null-termination */;
                std::vector<SQLCHAR> dataBuffer(fetchBufferSize);
                SQLLEN dataLen;
                // TODO: Handle the return code better
                ret = SQLGetData_ptr(hStmt, i, SQL_C_CHAR, dataBuffer.data(), dataBuffer.size(),
                                     &dataLen);

                if (SQL_SUCCEEDED(ret)) {
                    // TODO: Refactor these if's across other switches to avoid code duplication
                    // columnSize is in chars, dataLen is in bytes
                    if (dataLen > 0) {
                        uint64_t numCharsInData = dataLen / sizeof(SQLCHAR);
                        // NOTE: dataBuffer.size() includes null-terminator, dataLen doesn't. Hence use '<'.
						if (numCharsInData < dataBuffer.size()) {
                            // SQLGetData will null-terminate the data
                            row.append(std::string(reinterpret_cast<char*>(dataBuffer.data())));
						} else {
                            // In this case, buffer size is smaller, and data to be retrieved is longer
                            // TODO: Revisit
                            std::ostringstream oss;
                            oss << "Buffer length for fetch (" << dataBuffer.size()-1 << ") is smaller, & data "
                                << "to be retrieved is longer (" << numCharsInData << "). ColumnID - "
                                << i << ", datatype - " << dataType;
                            ThrowStdException(oss.str());
                        }
				    } else if (dataLen == SQL_NULL_DATA) {
					    row.append(py::none());
                    } else {
                        assert(dataLen == SQL_NO_TOTAL);
                        LOG("SQLGetData couldn't determine the length of the data. "
                            "Returning NULL value instead. Column ID - {}", i);
					    row.append(py::none());
                    }
				} else {
					LOG("Error retrieving data for column - {}, data type - {}, SQLGetData return "
						"code - {}. Returning NULL value instead",
						i, dataType, ret);
					row.append(py::none());
				}
                break;
            }
            case SQL_WCHAR:
            case SQL_WVARCHAR:
			case SQL_WLONGVARCHAR: {
                // TODO: revisit
                HandleZeroColumnSizeAtFetch(columnSize);
		uint64_t fetchBufferSize = columnSize + 1 /* null-termination */;
                std::vector<SQLWCHAR> dataBuffer(fetchBufferSize);
                SQLLEN dataLen;
                ret = SQLGetData_ptr(hStmt, i, SQL_C_WCHAR, dataBuffer.data(),
                                     dataBuffer.size() * sizeof(SQLWCHAR), &dataLen);

                if (SQL_SUCCEEDED(ret)) {
                    // TODO: Refactor these if's across other switches to avoid code duplication
                    if (dataLen > 0) {
                        uint64_t numCharsInData = dataLen / sizeof(SQLWCHAR);
						if (numCharsInData < dataBuffer.size()) {
                            // SQLGetData will null-terminate the data
                            row.append(std::wstring(dataBuffer.data()));
						} else {
                            // In this case, buffer size is smaller, and data to be retrieved is longer
                            // TODO: Revisit
                            std::ostringstream oss;
                            oss << "Buffer length for fetch (" << dataBuffer.size()-1 << ") is smaller, & data "
                                << "to be retrieved is longer (" << numCharsInData << "). ColumnID - "
                                << i << ", datatype - " << dataType;
                            ThrowStdException(oss.str());
                        }
				    } else if (dataLen == SQL_NULL_DATA) {
					    row.append(py::none());
                    } else {
                        assert(dataLen == SQL_NO_TOTAL);
                        LOG("SQLGetData couldn't determine the length of the data. "
                            "Returning NULL value instead. Column ID - {}", i);
					    row.append(py::none());
                    }
				} else {
					LOG("Error retrieving data for column - {}, data type - {}, SQLGetData return "
						"code - {}. Returning NULL value instead",
						i, dataType, ret);
					row.append(py::none());
				}
                break;
            }
            case SQL_INTEGER: {
                SQLINTEGER intValue;
                ret = SQLGetData_ptr(hStmt, i, SQL_C_LONG, &intValue, 0, NULL);
                if (SQL_SUCCEEDED(ret)) {
                    row.append(static_cast<int>(intValue));
                } else {
                    row.append(py::none());
                }
                break;
            }
            case SQL_SMALLINT: {
                SQLSMALLINT smallIntValue;
                ret = SQLGetData_ptr(hStmt, i, SQL_C_SHORT, &smallIntValue, 0, NULL);
                if (SQL_SUCCEEDED(ret)) {
                    row.append(static_cast<int>(smallIntValue));
                } else {
                    LOG("Error retrieving data for column - {}, data type - {}, SQLGetData return "
                        "code - {}. Returning NULL value instead",
                        i, dataType, ret);
                    row.append(py::none());
                }
                break;
            }
            case SQL_REAL: {
                SQLREAL realValue;
                ret = SQLGetData_ptr(hStmt, i, SQL_C_FLOAT, &realValue, 0, NULL);
                if (SQL_SUCCEEDED(ret)) {
                    row.append(realValue);
                } else {
                    LOG("Error retrieving data for column - {}, data type - {}, SQLGetData return "
                        "code - {}. Returning NULL value instead",
                        i, dataType, ret);
                    row.append(py::none());
                }
                break;
            }
            case SQL_DECIMAL:
            case SQL_NUMERIC: {
                SQLCHAR numericStr[MAX_DIGITS_IN_NUMERIC] = {0};
                SQLLEN indicator;
                ret = SQLGetData_ptr(hStmt, i, SQL_C_CHAR, numericStr, sizeof(numericStr), &indicator);

                if (SQL_SUCCEEDED(ret)) {
                    try{
                    // Convert numericStr to py::decimal.Decimal and append to row
                    row.append(py::module_::import("decimal").attr("Decimal")(
                        std::string(reinterpret_cast<const char*>(numericStr), indicator)));
                    } catch (const py::error_already_set& e) {
                        // If the conversion fails, append None
                        LOG("Error converting to decimal: {}", e.what());
                        row.append(py::none());
                    }
                }
                else {
                    LOG("Error retrieving data for column - {}, data type - {}, SQLGetData return "
                        "code - {}. Returning NULL value instead",
                        i, dataType, ret);
                    row.append(py::none());
                }
                break;
            }
            case SQL_DOUBLE:
            case SQL_FLOAT: {
                SQLDOUBLE doubleValue;
                ret = SQLGetData_ptr(hStmt, i, SQL_C_DOUBLE, &doubleValue, 0, NULL);
                if (SQL_SUCCEEDED(ret)) {
                    row.append(doubleValue);
                } else {
                    LOG("Error retrieving data for column - {}, data type - {}, SQLGetData return "
                        "code - {}. Returning NULL value instead",
                        i, dataType, ret);
                    row.append(py::none());
                }
                break;
            }
            case SQL_BIGINT: {
                SQLBIGINT bigintValue;
                ret = SQLGetData_ptr(hStmt, i, SQL_C_SBIGINT, &bigintValue, 0, NULL);
                if (SQL_SUCCEEDED(ret)) {
                    row.append(static_cast<long long>(bigintValue));
                } else {
                    LOG("Error retrieving data for column - {}, data type - {}, SQLGetData return "
                        "code - {}. Returning NULL value instead",
                        i, dataType, ret);
                    row.append(py::none());
                }
                break;
            }
            case SQL_TYPE_DATE: {
                SQL_DATE_STRUCT dateValue;
                ret =
                    SQLGetData_ptr(hStmt, i, SQL_C_TYPE_DATE, &dateValue, sizeof(dateValue), NULL);
                if (SQL_SUCCEEDED(ret)) {
                    row.append(
                        py::module_::import("datetime").attr("date")(
                            dateValue.year,
                            dateValue.month,
                            dateValue.day
                        )
                    );
                } else {
                    LOG("Error retrieving data for column - {}, data type - {}, SQLGetData return "
                        "code - {}. Returning NULL value instead",
                        i, dataType, ret);
                    row.append(py::none());
                }
                break;
            }
            case SQL_TIME:
            case SQL_TYPE_TIME:
            case SQL_SS_TIME2: {
                SQL_TIME_STRUCT timeValue;
                ret =
                    SQLGetData_ptr(hStmt, i, SQL_C_TYPE_TIME, &timeValue, sizeof(timeValue), NULL);
                if (SQL_SUCCEEDED(ret)) {
                    row.append(
                        py::module_::import("datetime").attr("time")(
                            timeValue.hour,
                            timeValue.minute,
                            timeValue.second
                        )
                    );
                } else {
                    LOG("Error retrieving data for column - {}, data type - {}, SQLGetData return "
                        "code - {}. Returning NULL value instead",
                        i, dataType, ret);
                    row.append(py::none());
                }
                break;
            }
            case SQL_TIMESTAMP:
            case SQL_TYPE_TIMESTAMP:
            case SQL_DATETIME: {
                SQL_TIMESTAMP_STRUCT timestampValue;
                ret = SQLGetData_ptr(hStmt, i, SQL_C_TYPE_TIMESTAMP, &timestampValue,
                                     sizeof(timestampValue), NULL);
                if (SQL_SUCCEEDED(ret)) {
                    row.append(
                        py::module_::import("datetime").attr("datetime")(
                            timestampValue.year,
                            timestampValue.month,
                            timestampValue.day,
                            timestampValue.hour,
                            timestampValue.minute,
                            timestampValue.second,
                            timestampValue.fraction / 1000  // Convert back ns to µs
                        )
                    );
                } else {
                    LOG("Error retrieving data for column - {}, data type - {}, SQLGetData return "
                        "code - {}. Returning NULL value instead",
                        i, dataType, ret);
                    row.append(py::none());
                }
                break;
            }
            case SQL_BINARY:
            case SQL_VARBINARY:
            case SQL_LONGVARBINARY: {
                // TODO: revisit
                HandleZeroColumnSizeAtFetch(columnSize);
                std::unique_ptr<SQLCHAR[]> dataBuffer(new SQLCHAR[columnSize]);
                SQLLEN dataLen;
                ret = SQLGetData_ptr(hStmt, i, SQL_C_BINARY, dataBuffer.get(), columnSize, &dataLen);

                if (SQL_SUCCEEDED(ret)) {
                    // TODO: Refactor these if's across other switches to avoid code duplication
                    if (dataLen > 0) {
						if (dataLen <= columnSize) {
                            row.append(py::bytes(reinterpret_cast<const char*>(
                                dataBuffer.get()), dataLen));
						} else {
                            // In this case, buffer size is smaller, and data to be retrieved is longer
                            // TODO: Revisit
                            std::ostringstream oss;
                            oss << "Buffer length for fetch (" << columnSize << ") is smaller, & data "
                                << "to be retrieved is longer (" << dataLen << "). ColumnID - "
                                << i << ", datatype - " << dataType;
                            ThrowStdException(oss.str());
                        }
				    } else if (dataLen == SQL_NULL_DATA) {
					    row.append(py::none());
                    } else {
                        assert(dataLen == SQL_NO_TOTAL);
                        LOG("SQLGetData couldn't determine the length of the data. "
                            "Returning NULL value instead. Column ID - {}", i);
					    row.append(py::none());
                    }
				} else {
					LOG("Error retrieving data for column - {}, data type - {}, SQLGetData return "
						"code - {}. Returning NULL value instead",
						i, dataType, ret);
					row.append(py::none());
				}
                break;
            }
            case SQL_TINYINT: {
                SQLCHAR tinyIntValue;
                ret = SQLGetData_ptr(hStmt, i, SQL_C_TINYINT, &tinyIntValue, 0, NULL);
                if (SQL_SUCCEEDED(ret)) {
                    row.append(static_cast<int>(tinyIntValue));
                } else {
                    LOG("Error retrieving data for column - {}, data type - {}, SQLGetData return "
                        "code - {}. Returning NULL value instead",
                        i, dataType, ret);
                    row.append(py::none());
                }
                break;
            }
            case SQL_BIT: {
                SQLCHAR bitValue;
                ret = SQLGetData_ptr(hStmt, i, SQL_C_BIT, &bitValue, 0, NULL);
                if (SQL_SUCCEEDED(ret)) {
                    row.append(static_cast<bool>(bitValue));
                } else {
                    LOG("Error retrieving data for column - {}, data type - {}, SQLGetData return "
                        "code - {}. Returning NULL value instead",
                        i, dataType, ret);
                    row.append(py::none());
                }
                break;
            }
#if (ODBCVER >= 0x0350)
            case SQL_GUID: {
                SQLGUID guidValue;
                ret = SQLGetData_ptr(hStmt, i, SQL_C_GUID, &guidValue, sizeof(guidValue), NULL);
                if (SQL_SUCCEEDED(ret)) {
                    std::ostringstream oss;
                    oss << std::hex << std::setfill('0') << std::setw(8) << guidValue.Data1 << '-'
                        << std::setw(4) << guidValue.Data2 << '-' << std::setw(4) << guidValue.Data3
                        << '-' << std::setw(2) << static_cast<int>(guidValue.Data4[0])
                        << std::setw(2) << static_cast<int>(guidValue.Data4[1]) << '-' << std::hex
                        << std::setw(2) << static_cast<int>(guidValue.Data4[2]) << std::setw(2)
                        << static_cast<int>(guidValue.Data4[3]) << std::setw(2)
                        << static_cast<int>(guidValue.Data4[4]) << std::setw(2)
                        << static_cast<int>(guidValue.Data4[5]) << std::setw(2)
                        << static_cast<int>(guidValue.Data4[6]) << std::setw(2)
                        << static_cast<int>(guidValue.Data4[7]);
                    row.append(oss.str());  // Append GUID as a string
                } else {
                    LOG("Error retrieving data for column - {}, data type - {}, SQLGetData return "
                        "code - {}. Returning NULL value instead",
                        i, dataType, ret);
                    row.append(py::none());
                }
                break;
            }
#endif
            default:
                std::ostringstream errorString;
                errorString << "Unsupported data type for column - " << columnName << ", Type - "
                            << dataType << ", column ID - " << i;
                LOG(errorString.str());
                ThrowStdException(errorString.str());
                break;
        }
    }
    return ret;
}

// For column in the result set, binds a buffer to retrieve column data
// TODO: Move to anonymous namespace, since it is not used outside this file
SQLRETURN SQLBindColums(SQLHSTMT hStmt, ColumnBuffers& buffers, py::list& columnNames,
                        SQLUSMALLINT numCols, int fetchSize) {
    SQLRETURN ret = SQL_SUCCESS;
    // Bind columns based on their data types
    for (SQLUSMALLINT col = 1; col <= numCols; col++) {
        auto columnMeta = columnNames[col - 1].cast<py::dict>();
        SQLSMALLINT dataType = columnMeta["DataType"].cast<SQLSMALLINT>();
        SQLULEN columnSize = columnMeta["ColumnSize"].cast<SQLULEN>();

        switch (dataType) {
            case SQL_CHAR:
            case SQL_VARCHAR:
            case SQL_LONGVARCHAR: {
                // TODO: handle variable length data correctly. This logic wont suffice
                HandleZeroColumnSizeAtFetch(columnSize);
                uint64_t fetchBufferSize = columnSize + 1 /*null-terminator*/;
		// TODO: For LONGVARCHAR/BINARY types, columnSize is returned as 2GB-1 by
		// SQLDescribeCol. So fetchBufferSize = 2GB. fetchSize=1 if columnSize>1GB.
		// So we'll allocate a vector of size 2GB. If a query fetches multiple (say N)
		// LONG... columns, we will have allocated multiple (N) 2GB sized vectors. This
		// will make driver very slow. And if the N is high enough, we could hit the OS
		// limit for heap memory that we can allocate, & hence get a std::bad_alloc. The
		// process could also be killed by OS for consuming too much memory.
		// Hence this will be revisited in beta to not allocate 2GB+ memory,
		// & use streaming instead
                buffers.charBuffers[col - 1].resize(fetchSize * fetchBufferSize);
                ret = SQLBindCol_ptr(hStmt, col, SQL_C_CHAR, buffers.charBuffers[col - 1].data(),
                                     fetchBufferSize * sizeof(SQLCHAR),
                                     buffers.indicators[col - 1].data());
                break;
            }
            case SQL_WCHAR:
            case SQL_WVARCHAR:
            case SQL_WLONGVARCHAR: {
                // TODO: handle variable length data correctly. This logic wont suffice
                HandleZeroColumnSizeAtFetch(columnSize);
                uint64_t fetchBufferSize = columnSize + 1 /*null-terminator*/;
                buffers.wcharBuffers[col - 1].resize(fetchSize * fetchBufferSize);
                ret = SQLBindCol_ptr(hStmt, col, SQL_C_WCHAR, buffers.wcharBuffers[col - 1].data(),
                                     fetchBufferSize * sizeof(SQLWCHAR),
                                     buffers.indicators[col - 1].data());
                break;
            }
            case SQL_INTEGER:
                buffers.intBuffers[col - 1].resize(fetchSize);
                ret = SQLBindCol_ptr(hStmt, col, SQL_C_SLONG, buffers.intBuffers[col - 1].data(),
                                     sizeof(SQLINTEGER), buffers.indicators[col - 1].data());
                break;
            case SQL_SMALLINT:
                buffers.smallIntBuffers[col - 1].resize(fetchSize);
                ret = SQLBindCol_ptr(hStmt, col, SQL_C_SSHORT,
                                     buffers.smallIntBuffers[col - 1].data(), sizeof(SQLSMALLINT),
                                     buffers.indicators[col - 1].data());
                break;
            case SQL_TINYINT:
                buffers.charBuffers[col - 1].resize(fetchSize);
                ret = SQLBindCol_ptr(hStmt, col, SQL_C_TINYINT, buffers.charBuffers[col - 1].data(),
                                     sizeof(SQLCHAR), buffers.indicators[col - 1].data());
                break;
            case SQL_BIT:
                buffers.charBuffers[col - 1].resize(fetchSize);
                ret = SQLBindCol_ptr(hStmt, col, SQL_C_BIT, buffers.charBuffers[col - 1].data(),
                                     sizeof(SQLCHAR), buffers.indicators[col - 1].data());
                break;
            case SQL_REAL:
                buffers.realBuffers[col - 1].resize(fetchSize);
                ret = SQLBindCol_ptr(hStmt, col, SQL_C_FLOAT, buffers.realBuffers[col - 1].data(),
                                     sizeof(SQLREAL), buffers.indicators[col - 1].data());
                break;
            case SQL_DECIMAL:
            case SQL_NUMERIC:
                buffers.charBuffers[col - 1].resize(fetchSize * MAX_DIGITS_IN_NUMERIC);
                ret = SQLBindCol_ptr(hStmt, col, SQL_C_CHAR, buffers.charBuffers[col - 1].data(),
                                     MAX_DIGITS_IN_NUMERIC * sizeof(SQLCHAR),
                                     buffers.indicators[col - 1].data());
                break;
            case SQL_DOUBLE:
            case SQL_FLOAT:
                buffers.doubleBuffers[col - 1].resize(fetchSize);
                ret =
                    SQLBindCol_ptr(hStmt, col, SQL_C_DOUBLE, buffers.doubleBuffers[col - 1].data(),
                                   sizeof(SQLDOUBLE), buffers.indicators[col - 1].data());
                break;
            case SQL_TIMESTAMP:
            case SQL_TYPE_TIMESTAMP:
            case SQL_DATETIME:
                buffers.timestampBuffers[col - 1].resize(fetchSize);
                ret = SQLBindCol_ptr(
                    hStmt, col, SQL_C_TYPE_TIMESTAMP, buffers.timestampBuffers[col - 1].data(),
                    sizeof(SQL_TIMESTAMP_STRUCT), buffers.indicators[col - 1].data());
                break;
            case SQL_BIGINT:
                buffers.bigIntBuffers[col - 1].resize(fetchSize);
                ret =
                    SQLBindCol_ptr(hStmt, col, SQL_C_SBIGINT, buffers.bigIntBuffers[col - 1].data(),
                                   sizeof(SQLBIGINT), buffers.indicators[col - 1].data());
                break;
            case SQL_TYPE_DATE:
                buffers.dateBuffers[col - 1].resize(fetchSize);
                ret =
                    SQLBindCol_ptr(hStmt, col, SQL_C_TYPE_DATE, buffers.dateBuffers[col - 1].data(),
                                   sizeof(SQL_DATE_STRUCT), buffers.indicators[col - 1].data());
                break;
            case SQL_TIME:
            case SQL_TYPE_TIME:
            case SQL_SS_TIME2:
                buffers.timeBuffers[col - 1].resize(fetchSize);
                ret =
                    SQLBindCol_ptr(hStmt, col, SQL_C_TYPE_TIME, buffers.timeBuffers[col - 1].data(),
                                   sizeof(SQL_TIME_STRUCT), buffers.indicators[col - 1].data());
                break;
            case SQL_GUID:
                buffers.guidBuffers[col - 1].resize(fetchSize);
                ret = SQLBindCol_ptr(hStmt, col, SQL_C_GUID, buffers.guidBuffers[col - 1].data(),
                                     sizeof(SQLGUID), buffers.indicators[col - 1].data());
                break;
            case SQL_BINARY:
            case SQL_VARBINARY:
            case SQL_LONGVARBINARY:
                // TODO: handle variable length data correctly. This logic wont suffice
                HandleZeroColumnSizeAtFetch(columnSize);
                buffers.charBuffers[col - 1].resize(fetchSize * columnSize);
                ret = SQLBindCol_ptr(hStmt, col, SQL_C_BINARY, buffers.charBuffers[col - 1].data(),
                                     columnSize, buffers.indicators[col - 1].data());
                break;
            default:
                std::wstring columnName = columnMeta["ColumnName"].cast<std::wstring>();
                std::ostringstream errorString;
                errorString << "Unsupported data type for column - " << columnName.c_str()
                            << ", Type - " << dataType << ", column ID - " << col;
                LOG(errorString.str());
                ThrowStdException(errorString.str());
                break;
        }
        if (!SQL_SUCCEEDED(ret)) {
            std::wstring columnName = columnMeta["ColumnName"].cast<std::wstring>();
            std::ostringstream errorString;
            errorString << "Failed to bind column - " << columnName.c_str() << ", Type - "
                        << dataType << ", column ID - " << col;
            LOG(errorString.str());
            ThrowStdException(errorString.str());
            return ret;
        }
    }
    return ret;
}

// Fetch rows in batches
// TODO: Move to anonymous namespace, since it is not used outside this file
SQLRETURN FetchBatchData(SQLHSTMT hStmt, ColumnBuffers& buffers, py::list& columnNames,
                         py::list& rows, SQLUSMALLINT numCols, SQLULEN& numRowsFetched) {
    LOG("Fetching data in batches");
    SQLRETURN ret = SQLFetchScroll_ptr(hStmt, SQL_FETCH_NEXT, 0);
    if (ret == SQL_NO_DATA) {
        LOG("No data to fetch");
        return ret;
    }
    if (!SQL_SUCCEEDED(ret)) {
        LOG("Error while fetching rows in batches");
        return ret;
    }
    // numRowsFetched is the SQL_ATTR_ROWS_FETCHED_PTR attribute. It'll be populated by
    // SQLFetchScroll
    for (SQLULEN i = 0; i < numRowsFetched; i++) {
        py::list row;
        for (SQLUSMALLINT col = 1; col <= numCols; col++) {
            auto columnMeta = columnNames[col - 1].cast<py::dict>();
            SQLSMALLINT dataType = columnMeta["DataType"].cast<SQLSMALLINT>();
            SQLLEN dataLen = buffers.indicators[col - 1][i];

            if (dataLen == SQL_NULL_DATA) {
                row.append(py::none());
                continue;
            }
            // TODO: variable length data needs special handling, this logic wont suffice
            // This value indicates that the driver cannot determine the length of the data
            if (dataLen == SQL_NO_TOTAL) {
                LOG("Cannot determine the length of the data. Returning NULL value instead."
                    "Column ID - {}", col);
                row.append(py::none());
                continue;
            }
            assert(dataLen > 0 && "Must be > 0 since SQL_NULL_DATA & SQL_NO_DATA is already handled");

            switch (dataType) {
                case SQL_CHAR:
                case SQL_VARCHAR:
                case SQL_LONGVARCHAR: {
                    // TODO: variable length data needs special handling, this logic wont suffice
                    SQLULEN columnSize = columnMeta["ColumnSize"].cast<SQLULEN>();
                    HandleZeroColumnSizeAtFetch(columnSize);
                    uint64_t fetchBufferSize = columnSize + 1 /*null-terminator*/;
					uint64_t numCharsInData = dataLen / sizeof(SQLCHAR);
					// fetchBufferSize includes null-terminator, numCharsInData doesn't. Hence '<'
                    if (numCharsInData < fetchBufferSize) {
                        // SQLFetch will nullterminate the data
                        row.append(std::string(
                            reinterpret_cast<char*>(&buffers.charBuffers[col - 1][i * fetchBufferSize]),
                            numCharsInData));
                    } else {
                        // In this case, buffer size is smaller, and data to be retrieved is longer
                        // TODO: Revisit
                        std::ostringstream oss;
                        oss << "Buffer length for fetch (" << columnSize << ") is smaller, & data "
                            << "to be retrieved is longer (" << numCharsInData << "). ColumnID - "
                            << col << ", datatype - " << dataType;
                        ThrowStdException(oss.str());
                    }
                    break;
                }
                case SQL_WCHAR:
                case SQL_WVARCHAR:
                case SQL_WLONGVARCHAR: {
                    // TODO: variable length data needs special handling, this logic wont suffice
                    SQLULEN columnSize = columnMeta["ColumnSize"].cast<SQLULEN>();
                    HandleZeroColumnSizeAtFetch(columnSize);
                    uint64_t fetchBufferSize = columnSize + 1 /*null-terminator*/;
					uint64_t numCharsInData = dataLen / sizeof(SQLWCHAR);
					// fetchBufferSize includes null-terminator, numCharsInData doesn't. Hence '<'
                    if (numCharsInData < fetchBufferSize) {
                        // SQLFetch will nullterminate the data
                        row.append(std::wstring(
                            reinterpret_cast<wchar_t*>(&buffers.wcharBuffers[col - 1][i * fetchBufferSize]),
                            numCharsInData));
                    } else {
                        // In this case, buffer size is smaller, and data to be retrieved is longer
                        // TODO: Revisit
                        std::ostringstream oss;
                        oss << "Buffer length for fetch (" << columnSize << ") is smaller, & data "
                            << "to be retrieved is longer (" << numCharsInData << "). ColumnID - "
                            << col << ", datatype - " << dataType;
                        ThrowStdException(oss.str());
                    }
                    break;
                }
                case SQL_INTEGER: {
                    row.append(buffers.intBuffers[col - 1][i]);
                    break;
                }
                case SQL_SMALLINT: {
                    row.append(buffers.smallIntBuffers[col - 1][i]);
                    break;
                }
                case SQL_TINYINT: {
                    row.append(buffers.charBuffers[col - 1][i]);
                    break;
                }
                case SQL_BIT: {
                    row.append(static_cast<bool>(buffers.charBuffers[col - 1][i]));
                    break;
                }
                case SQL_REAL: {
                    row.append(buffers.realBuffers[col - 1][i]);
                    break;
                }
                case SQL_DECIMAL:
                case SQL_NUMERIC: {
                    try {
                        // Convert numericStr to py::decimal.Decimal and append to row
                        row.append(py::module_::import("decimal").attr("Decimal")(std::string(
                            reinterpret_cast<const char*>(
                                &buffers.charBuffers[col - 1][i * MAX_DIGITS_IN_NUMERIC]),
                            buffers.indicators[col - 1][i])));
                    } catch (const py::error_already_set& e) {
                        // Handle the exception, e.g., log the error and append py::none()
                        LOG("Error converting to decimal: {}", e.what());
                        row.append(py::none());
                    }
                    break;
                }
                case SQL_DOUBLE:
                case SQL_FLOAT: {
                    row.append(buffers.doubleBuffers[col - 1][i]);
                    break;
                }
                case SQL_TIMESTAMP:
                case SQL_TYPE_TIMESTAMP:
                case SQL_DATETIME: {
                    row.append(py::module_::import("datetime")
                                   .attr("datetime")(buffers.timestampBuffers[col - 1][i].year,
                                                     buffers.timestampBuffers[col - 1][i].month,
                                                     buffers.timestampBuffers[col - 1][i].day,
                                                     buffers.timestampBuffers[col - 1][i].hour,
                                                     buffers.timestampBuffers[col - 1][i].minute,
                                                     buffers.timestampBuffers[col - 1][i].second,
						                             buffers.timestampBuffers[col - 1][i].fraction / 1000  /* Convert back ns to µs */));
                    break;
                }
                case SQL_BIGINT: {
                    row.append(buffers.bigIntBuffers[col - 1][i]);
                    break;
                }
                case SQL_TYPE_DATE: {
                    row.append(py::module_::import("datetime")
                                   .attr("date")(buffers.dateBuffers[col - 1][i].year,
                                                 buffers.dateBuffers[col - 1][i].month,
                                                 buffers.dateBuffers[col - 1][i].day));
                    break;
                }
                case SQL_TIME:
                case SQL_TYPE_TIME:
                case SQL_SS_TIME2: {
                    row.append(py::module_::import("datetime")
                                   .attr("time")(buffers.timeBuffers[col - 1][i].hour,
                                                 buffers.timeBuffers[col - 1][i].minute,
                                                 buffers.timeBuffers[col - 1][i].second));
                    break;
                }
                case SQL_GUID: {
                    row.append(
                        py::bytes(reinterpret_cast<const char*>(&buffers.guidBuffers[col - 1][i]),
                                  sizeof(SQLGUID)));
                    break;
                }
                case SQL_BINARY:
                case SQL_VARBINARY:
                case SQL_LONGVARBINARY: {
                    // TODO: variable length data needs special handling, this logic wont suffice
                    SQLULEN columnSize = columnMeta["ColumnSize"].cast<SQLULEN>();
                    HandleZeroColumnSizeAtFetch(columnSize);
                    if (dataLen <= columnSize) {
                        row.append(py::bytes(reinterpret_cast<const char*>(
                                                 &buffers.charBuffers[col - 1][i * columnSize]),
                                             dataLen));
                    } else {
                        // In this case, buffer size is smaller, and data to be retrieved is longer
                        // TODO: Revisit
                        std::ostringstream oss;
                        oss << "Buffer length for fetch (" << columnSize << ") is smaller, & data "
                            << "to be retrieved is longer (" << dataLen << "). ColumnID - "
                            << col << ", datatype - " << dataType;
                        ThrowStdException(oss.str());
                    }
                    break;
                }
                default: {
                    std::wstring columnName = columnMeta["ColumnName"].cast<std::wstring>();
                    std::ostringstream errorString;
                    errorString << "Unsupported data type for column - " << columnName.c_str()
                                << ", Type - " << dataType << ", column ID - " << col;
                    LOG(errorString.str());
                    ThrowStdException(errorString.str());
                    break;
                }
            }
        }
        rows.append(row);
    }
    return ret;
}

// Given a list of columns that are a part of single row in the result set, calculates
// the max size of the row
// TODO: Move to anonymous namespace, since it is not used outside this file
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
                rowSize += sizeof(SQLREAL);
                break;
            case SQL_FLOAT:
                rowSize += sizeof(SQLFLOAT);
                break;
            case SQL_DOUBLE:
                rowSize += sizeof(SQLDOUBLE);
                break;
            case SQL_DECIMAL:
            case SQL_NUMERIC:
                rowSize += MAX_DIGITS_IN_NUMERIC;
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
                std::wstring columnName = columnMeta["ColumnName"].cast<std::wstring>();
                std::ostringstream errorString;
                errorString << "Unsupported data type for column - " << columnName.c_str()
                            << ", Type - " << dataType << ", column ID - " << col;
                LOG(errorString.str());
                ThrowStdException(errorString.str());
                break;
        }
    }
    return rowSize;
}

// FetchMany_wrap - Fetches multiple rows of data from the result set.
//
// @param StatementHandle: Handle to the statement from which data is to be fetched.
// @param rows: A Python list that will be populated with the fetched rows of data.
// @param fetchSize: The number of rows to fetch. Default value is 1.
//
// @return SQLRETURN: SQL_SUCCESS if data is fetched successfully,
//                    SQL_NO_DATA if there are no more rows to fetch,
//                    throws a runtime error if there is an error fetching data.
//
// This function assumes that the statement handle (hStmt) is already allocated and a query has been
// executed. It fetches the specified number of rows from the result set and populates the provided
// Python list with the row data. If there are no more rows to fetch, it returns SQL_NO_DATA. If an
// error occurs during fetching, it throws a runtime error.
SQLRETURN FetchMany_wrap(SqlHandlePtr StatementHandle, py::list& rows, int fetchSize = 1) {
    SQLRETURN ret;
    SQLHSTMT hStmt = StatementHandle->get();
    // Retrieve column count
    SQLSMALLINT numCols = SQLNumResultCols_wrap(StatementHandle);

    // Retrieve column metadata
    py::list columnNames;
    ret = SQLDescribeCol_wrap(StatementHandle, columnNames);
    if (!SQL_SUCCEEDED(ret)) {
        LOG("Failed to get column descriptions");
        return ret;
    }

    // Initialize column buffers
    ColumnBuffers buffers(numCols, fetchSize);

    // Bind columns
    ret = SQLBindColums(hStmt, buffers, columnNames, numCols, fetchSize);
    if (!SQL_SUCCEEDED(ret)) {
        LOG("Error when binding columns");
        return ret;
    }

    SQLULEN numRowsFetched;
    SQLSetStmtAttr_ptr(hStmt, SQL_ATTR_ROW_ARRAY_SIZE, (SQLPOINTER)fetchSize, 0);
    SQLSetStmtAttr_ptr(hStmt, SQL_ATTR_ROWS_FETCHED_PTR, &numRowsFetched, 0);

    ret = FetchBatchData(hStmt, buffers, columnNames, rows, numCols, numRowsFetched);
    if (!SQL_SUCCEEDED(ret) && ret != SQL_NO_DATA) {
        LOG("Error when fetching data");
        return ret;
    }

    return ret;
}

// FetchAll_wrap - Fetches all rows of data from the result set.
//
// @param StatementHandle: Handle to the statement from which data is to be fetched.
// @param rows: A Python list that will be populated with the fetched rows of data.
//
// @return SQLRETURN: SQL_SUCCESS if data is fetched successfully,
//                    SQL_NO_DATA if there are no more rows to fetch,
//                    throws a runtime error if there is an error fetching data.
//
// This function assumes that the statement handle (hStmt) is already allocated and a query has been
// executed. It fetches all rows from the result set and populates the provided Python list with the
// row data. If there are no more rows to fetch, it returns SQL_NO_DATA. If an error occurs during
// fetching, it throws a runtime error.
SQLRETURN FetchAll_wrap(SqlHandlePtr StatementHandle, py::list& rows) {
    SQLRETURN ret;
    SQLHSTMT hStmt = StatementHandle->get();
    // Retrieve column count
    SQLSMALLINT numCols = SQLNumResultCols_wrap(StatementHandle);

    // Retrieve column metadata
    py::list columnNames;
    ret = SQLDescribeCol_wrap(StatementHandle, columnNames);
    if (!SQL_SUCCEEDED(ret)) {
        LOG("Failed to get column descriptions");
        return ret;
    }

    // Define a memory limit (1 GB)
    const size_t memoryLimit = 1ULL * 1024 * 1024 * 1024;  // 1 GB
    size_t totalRowSize = calculateRowSize(columnNames, numCols);

    // Calculate fetch size based on the total row size and memory limit
    size_t numRowsInMemLimit;
    if (totalRowSize > 0) {
        numRowsInMemLimit = static_cast<size_t>(memoryLimit / totalRowSize);
    } else {
        // Handle case where totalRowSize is 0 to avoid division by zero.
        // This can happen for NVARCHAR(MAX) cols. SQLDescribeCol returns 0
        // for column size of such columns.
        // TODO: Find why NVARCHAR(MAX) returns columnsize 0
        // TODO: What if a row has 2 cols, an int & NVARCHAR(MAX)?
        //       totalRowSize will be 4+0 = 4. It wont take NVARCHAR(MAX)
        //       into account. So, we will end up fetching 1000 rows at a time.
        numRowsInMemLimit = 1;  // fetchsize will be 10
    }
    // TODO: Revisit this logic. Eventhough we're fetching fetchSize rows at a time,
    // fetchall will keep all rows in memory anyway. So what are we gaining by fetching
    // fetchSize rows at a time?
    // Also, say the table has only 10 rows, each row size if 100 bytes. Here, we'll have
    // fetchSize = 1000, so we'll allocate memory for 1000 rows inside SQLBindCol_wrap, while
    // actually only need to retrieve 10 rows
    int fetchSize;
    if (numRowsInMemLimit == 0) {
        // If the row size is larger than the memory limit, fetch one row at a time
        fetchSize = 1;
    } else if (numRowsInMemLimit > 0 && numRowsInMemLimit <= 100) {
        // If between 1-100 rows fit in memoryLimit, fetch 10 rows at a time
        fetchSize = 10;
    } else if (numRowsInMemLimit > 100 && numRowsInMemLimit <= 1000) {
        // If between 100-1000 rows fit in memoryLimit, fetch 100 rows at a time
        fetchSize = 100;
    } else {
        fetchSize = 1000;
    }
    LOG("Fetching data in batch sizes of {}", fetchSize);

    ColumnBuffers buffers(numCols, fetchSize);

    // Bind columns
    ret = SQLBindColums(hStmt, buffers, columnNames, numCols, fetchSize);
    if (!SQL_SUCCEEDED(ret)) {
        LOG("Error when binding columns");
        return ret;
    }

    SQLULEN numRowsFetched;
    SQLSetStmtAttr_ptr(hStmt, SQL_ATTR_ROW_ARRAY_SIZE, (SQLPOINTER)fetchSize, 0);
    SQLSetStmtAttr_ptr(hStmt, SQL_ATTR_ROWS_FETCHED_PTR, &numRowsFetched, 0);

    while (ret != SQL_NO_DATA) {
        ret = FetchBatchData(hStmt, buffers, columnNames, rows, numCols, numRowsFetched);
        if (!SQL_SUCCEEDED(ret) && ret != SQL_NO_DATA) {
            LOG("Error when fetching data");
            return ret;
        }
    }

    return ret;
}

// FetchOne_wrap - Fetches a single row of data from the result set.
//
// @param StatementHandle: Handle to the statement from which data is to be fetched.
// @param row: A Python list that will be populated with the fetched row data.
//
// @return SQLRETURN: SQL_SUCCESS or SQL_SUCCESS_WITH_INFO if data is fetched successfully,
//                    SQL_NO_DATA if there are no more rows to fetch,
//                    throws a runtime error if there is an error fetching data.
//
// This function assumes that the statement handle (hStmt) is already allocated and a query has been
// executed. It fetches the next row of data from the result set and populates the provided Python
// list with the row data. If there are no more rows to fetch, it returns SQL_NO_DATA. If an error
// occurs during fetching, it throws a runtime error.
SQLRETURN FetchOne_wrap(SqlHandlePtr StatementHandle, py::list& row) {
    SQLRETURN ret;
    SQLHSTMT hStmt = StatementHandle->get();

    // Assume hStmt is already allocated and a query has been executed
    ret = SQLFetch_ptr(hStmt);
    if (SQL_SUCCEEDED(ret)) {
        // Retrieve column count
        SQLSMALLINT colCount = SQLNumResultCols_wrap(StatementHandle);
        ret = SQLGetData_wrap(StatementHandle, colCount, row);
    } else if (ret != SQL_NO_DATA) {
        LOG("Error when fetching data");
    }
    return ret;
}

// Wrap SQLMoreResults
SQLRETURN SQLMoreResults_wrap(SqlHandlePtr StatementHandle) {
    LOG("Check for more results");
    if (!SQLMoreResults_ptr) {
        LoadDriverOrThrowException();
    }

    return SQLMoreResults_ptr(StatementHandle->get());
}

// Wrap SQLEndTran
SQLRETURN SQLEndTran_wrap(SQLSMALLINT HandleType, SqlHandlePtr Handle, SQLSMALLINT CompletionType) {
    LOG("End SQL Transaction");
    if (!SQLEndTran_ptr) {
        LoadDriverOrThrowException();
    }

    return SQLEndTran_ptr(HandleType, Handle->get(), CompletionType);
}

// Wrap SQLFreeHandle
SQLRETURN SQLFreeHandle_wrap(SQLSMALLINT HandleType, SqlHandlePtr Handle) {
    LOG("Free SQL handle");
    if (!SQLAllocHandle_ptr) {
        LoadDriverOrThrowException();
    }

    SQLRETURN ret = SQLFreeHandle_ptr(HandleType, Handle->get());
    if (!SQL_SUCCEEDED(ret)) {
        LOG("SQLFreeHandle failed with error code - {}", ret);
    }
    return ret;
}

// Wrap SQLDisconnect
SQLRETURN SQLDisconnect_wrap(SqlHandlePtr ConnectionHandle) {
    LOG("Disconnect from MSSQL");
    if (!SQLDisconnect_ptr) {
        LoadDriverOrThrowException();
    }

    return SQLDisconnect_ptr(ConnectionHandle->get());
}

// Wrap SQLRowCount
SQLLEN SQLRowCount_wrap(SqlHandlePtr StatementHandle) {
    LOG("Get number of row affected by last execute");
    if (!SQLRowCount_ptr) {
        LoadDriverOrThrowException();
    }

    SQLLEN rowCount;
    SQLRETURN ret = SQLRowCount_ptr(StatementHandle->get(), &rowCount);
    if (!SQL_SUCCEEDED(ret)) {
        LOG("SQLRowCount failed with error code - {}", ret);
        return ret;
    }
    LOG("SQLRowCount returned {}", rowCount);
    return rowCount;
}

// Functions/data to be exposed to Python as a part of ddbc_bindings module
PYBIND11_MODULE(ddbc_bindings, m) {
    m.doc() = "msodbcsql driver api bindings for Python";
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
        .def(py::init<SQLCHAR, SQLSCHAR, SQLCHAR, std::uint64_t>())
        .def_readwrite("precision", &NumericData::precision)
        .def_readwrite("scale", &NumericData::scale)
        .def_readwrite("sign", &NumericData::sign)
        .def_readwrite("val", &NumericData::val);
    py::class_<ErrorInfo>(m, "ErrorInfo")
        .def_readwrite("sqlState", &ErrorInfo::sqlState)
        .def_readwrite("ddbcErrorMsg", &ErrorInfo::ddbcErrorMsg);
    py::class_<SqlHandle, SqlHandlePtr>(m, "SqlHandle");
    m.def("DDBCSQLAllocHandle", [](SQLSMALLINT HandleType, SqlHandlePtr InputHandle = nullptr) {
            SqlHandlePtr OutputHandle;
            SQLRETURN rc = SQLAllocHandle_wrap(HandleType, InputHandle, OutputHandle);
            return py::make_tuple(rc, OutputHandle);
        }, "Allocate an environment, connection, statement, or descriptor handle");
    m.def("DDBCSQLSetEnvAttr", &SQLSetEnvAttr_wrap,
          "Set an attribute that governs aspects of environments");
    m.def("DDBCSQLSetConnectAttr", &SQLSetConnectAttr_wrap,
          "Set an attribute that governs aspects of connections");
    m.def("DDBCSQLSetStmtAttr", &SQLSetStmtAttr_wrap,
          "Set an attribute that governs aspects of statements");
    m.def("DDBCSQLGetConnectionAttr", &SQLGetConnectionAttr_wrap,
          "Get an attribute that governs aspects of connections");
    m.def("DDBCSQLDriverConnect", &SQLDriverConnect_wrap,
          "Connect to a data source with a connection string");
    m.def("DDBCSQLExecDirect", &SQLExecDirect_wrap, "Execute a SQL query directly");
    m.def("DDBCSQLExecute", &SQLExecute_wrap, "Prepare and execute T-SQL statements");
    m.def("DDBCSQLRowCount", &SQLRowCount_wrap,
          "Get the number of rows affected by the last statement");
    m.def("DDBCSQLFetch", &SQLFetch_wrap, "Fetch the next row from the result set");
    m.def("DDBCSQLNumResultCols", &SQLNumResultCols_wrap,
          "Get the number of columns in the result set");
    m.def("DDBCSQLDescribeCol", &SQLDescribeCol_wrap,
          "Get information about a column in the result set");
    m.def("DDBCSQLGetData", &SQLGetData_wrap, "Retrieve data from the result set");
    m.def("DDBCSQLMoreResults", &SQLMoreResults_wrap, "Check for more results in the result set");
    m.def("DDBCSQLFetchOne", &FetchOne_wrap, "Fetch one row from the result set");
    m.def("DDBCSQLFetchMany", &FetchMany_wrap, py::arg("StatementHandle"), py::arg("rows"),
          py::arg("fetchSize") = 1, "Fetch many rows from the result set");
    m.def("DDBCSQLFetchAll", &FetchAll_wrap, "Fetch all rows from the result set");
    m.def("DDBCSQLEndTran", &SQLEndTran_wrap, "End a transaction");
    m.def("DDBCSQLFreeHandle", &SQLFreeHandle_wrap, "Free a handle");
    m.def("DDBCSQLDisconnect", &SQLDisconnect_wrap, "Disconnect from a data source");
    m.def("DDBCSQLCheckError", &SQLCheckError_Wrap, "Check for driver errors");
}
