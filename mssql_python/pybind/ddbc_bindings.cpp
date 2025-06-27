// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

// INFO|TODO - Note that is file is Windows specific right now. Making it arch agnostic will be
//             taken up in beta release
#include "ddbc_bindings.h"
#include "connection/connection.h"
#include "connection/connection_pool.h"

#include <cstdint>
#include <iomanip>  // std::setw, std::setfill
#include <iostream>
#include <utility>  // std::forward
#include <filesystem>

//-------------------------------------------------------------------------------------------------
// Macro definitions
//-------------------------------------------------------------------------------------------------

// This constant is not exposed via sql.h, hence define it here
#define SQL_SS_TIME2 (-154)

#define MAX_DIGITS_IN_NUMERIC 64

#define STRINGIFY_FOR_CASE(x) \
    case x:                   \
        return #x

// Architecture-specific defines
#ifndef ARCHITECTURE
#define ARCHITECTURE "win64"  // Default to win64 if not defined during compilation
#endif

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

namespace {

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
            return "Unknown";
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

std::string DescribeChar(unsigned char ch) {
    if (ch >= 32 && ch <= 126) {
        return std::string("'") + static_cast<char>(ch) + "'";
    } else {
        char buffer[16];
        snprintf(buffer, sizeof(buffer), "U+%04X", ch);
        return std::string(buffer);
    }
}

// Given a list of parameters and their ParamInfo, calls SQLBindParameter on each of them with
// appropriate arguments
SQLRETURN BindParameters(SQLHANDLE hStmt, const py::list& params,
                         const std::vector<ParamInfo>& paramInfos,
                         std::vector<std::shared_ptr<void>>& paramBuffers) {
    LOG("Starting parameter binding. Number of parameters: {}", params.size());
    for (int paramIndex = 0; paramIndex < params.size(); paramIndex++) {
        const auto& param = params[paramIndex];
        const ParamInfo& paramInfo = paramInfos[paramIndex];
        LOG("Binding parameter {} - C Type: {}, SQL Type: {}", paramIndex, paramInfo.paramCType, paramInfo.paramSQLType);
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
                
                // Log detailed parameter information
                LOG("SQL_C_WCHAR Parameter[{}]: Length={}, Content='{}'",
                    paramIndex,
                    strParam->size(),
                    (strParam->size() <= 100
                        ? WideToUTF8(std::wstring(strParam->begin(), strParam->end()))
                        : WideToUTF8(std::wstring(strParam->begin(), strParam->begin() + 100)) + "..."));

                // Log each character's code point for debugging
                if (strParam->size() <= 20) {
                    for (size_t i = 0; i < strParam->size(); i++) {
                        unsigned char ch = static_cast<unsigned char>((*strParam)[i]);
                        LOG("  char[{}] = {} ({})", i, static_cast<int>(ch), DescribeChar(ch));
                    }
                }
#if defined(__APPLE__)
                // On macOS, we need special handling for wide characters
                // Create a properly encoded SQLWCHAR buffer for the parameter
                std::vector<SQLWCHAR>* sqlwcharBuffer =
                    AllocateParamBuffer<std::vector<SQLWCHAR>>(paramBuffers);

                // Reserve space and convert from wstring to SQLWCHAR array
                sqlwcharBuffer->resize(strParam->size() + 1, 0); // +1 for null terminator

                // Convert each wchar_t (4 bytes on macOS) to SQLWCHAR (2 bytes)
                for (size_t i = 0; i < strParam->size(); i++) {
                    (*sqlwcharBuffer)[i] = static_cast<SQLWCHAR>((*strParam)[i]);
                }

                // Use the SQLWCHAR buffer instead of the wstring directly
                dataPtr = sqlwcharBuffer->data();
                bufferLength = (strParam->size() + 1) * sizeof(SQLWCHAR);
                LOG("macOS: Created SQLWCHAR buffer for parameter with size: {} bytes", bufferLength);
#else
                // On Windows, wchar_t and SQLWCHAR are the same size, so direct cast works
                dataPtr = const_cast<void*>(static_cast<const void*>(strParam->c_str()));
                bufferLength = (strParam->size() + 1 /* null terminator */) * sizeof(wchar_t);
#endif
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
                int value = param.cast<int>();
                // Range validation for signed 16-bit integer
                if (value < std::numeric_limits<short>::min() || value > std::numeric_limits<short>::max()) {
                    ThrowStdException("Signed short integer parameter out of range at paramIndex " + std::to_string(paramIndex));
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
                unsigned int value = param.cast<unsigned int>();
                if (value > std::numeric_limits<unsigned short>::max()) {
                    ThrowStdException("Unsigned short integer parameter out of range at paramIndex " + std::to_string(paramIndex));
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
                int64_t value = param.cast<int64_t>();
                // Range validation for signed 64-bit integer
                if (value < std::numeric_limits<int64_t>::min() || value > std::numeric_limits<int64_t>::max()) {
                    ThrowStdException("Signed 64-bit integer parameter out of range at paramIndex " + std::to_string(paramIndex));
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
                uint64_t value = param.cast<uint64_t>();
                // Range validation for unsigned 64-bit integer
                if (value > std::numeric_limits<uint64_t>::max()) {
                    ThrowStdException("Unsigned 64-bit integer parameter out of range at paramIndex " + std::to_string(paramIndex));
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
                int year = param.attr("year").cast<int>();
                if (year < 1753 || year > 9999) {
                    ThrowStdException("Date out of range for SQL Server (1753-9999) at paramIndex " + std::to_string(paramIndex));
                }
                // TODO: can be moved to python by registering SQL_DATE_STRUCT in pybind
                SQL_DATE_STRUCT* sqlDatePtr = AllocateParamBuffer<SQL_DATE_STRUCT>(paramBuffers);
                sqlDatePtr->year = static_cast<SQLSMALLINT>(param.attr("year").cast<int>());
                sqlDatePtr->month = static_cast<SQLUSMALLINT>(param.attr("month").cast<int>());
                sqlDatePtr->day = static_cast<SQLUSMALLINT>(param.attr("day").cast<int>());
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
                sqlTimePtr->hour = static_cast<SQLUSMALLINT>(param.attr("hour").cast<int>());
                sqlTimePtr->minute = static_cast<SQLUSMALLINT>(param.attr("minute").cast<int>());
                sqlTimePtr->second = static_cast<SQLUSMALLINT>(param.attr("second").cast<int>());
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
                sqlTimestampPtr->year = static_cast<SQLSMALLINT>(param.attr("year").cast<int>());
                sqlTimestampPtr->month = static_cast<SQLUSMALLINT>(param.attr("month").cast<int>());
                sqlTimestampPtr->day = static_cast<SQLUSMALLINT>(param.attr("day").cast<int>());
                sqlTimestampPtr->hour = static_cast<SQLUSMALLINT>(param.attr("hour").cast<int>());
                sqlTimestampPtr->minute = static_cast<SQLUSMALLINT>(param.attr("minute").cast<int>());
                sqlTimestampPtr->second = static_cast<SQLUSMALLINT>(param.attr("second").cast<int>());
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
                std::memset(static_cast<void*>(decimalPtr->val), 0, sizeof(decimalPtr->val));
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
            hStmt,
            static_cast<SQLUSMALLINT>(paramIndex + 1),  /* 1-based indexing */
            static_cast<SQLUSMALLINT>(paramInfo.inputOutputType),
            static_cast<SQLSMALLINT>(paramInfo.paramCType),
            static_cast<SQLSMALLINT>(paramInfo.paramSQLType), paramInfo.columnSize,
            paramInfo.decimalDigits, dataPtr, bufferLength, strLenOrIndPtr);
        if (!SQL_SUCCEEDED(rc)) {
            LOG("Error when binding parameter - {}", paramIndex);
            return rc;
        }
	// Special handling for Numeric type -
	// https://learn.microsoft.com/en-us/sql/odbc/reference/appendixes/retrieve-numeric-data-sql-numeric-struct-kb222831?view=sql-server-ver16#sql_c_numeric-overview
        if (paramInfo.paramCType == SQL_C_NUMERIC) {
            SQLHDESC hDesc = nullptr;
            rc = SQLGetStmtAttr_ptr(hStmt, SQL_ATTR_APP_PARAM_DESC, &hDesc, 0, NULL);
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
    LOG("Finished parameter binding. Number of parameters: {}", params.size());
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

std::string GetModuleDirectory() {
    py::object module = py::module::import("mssql_python");
    py::object module_path = module.attr("__file__");
    std::string module_file = module_path.cast<std::string>();
    
#ifdef _WIN32
    // Windows-specific path handling
    char path[MAX_PATH];
    errno_t err = strncpy_s(path, MAX_PATH, module_file.c_str(), module_file.length());
    if (err != 0) {
        LOG("strncpy_s failed with error code: {}", err);
        return {};
    }
    PathRemoveFileSpecA(path);
    return std::string(path);
#else
    // macOS/Unix path handling without using std::filesystem
    std::string::size_type pos = module_file.find_last_of('/');
    if (pos != std::string::npos) {
        std::string dir = module_file.substr(0, pos);
        return dir;
    }
    LOG("DEBUG: Could not extract directory from path: {}", module_file);
    return module_file;
#endif
}

// Platform-agnostic function to load the driver dynamic library
DriverHandle LoadDriverLibrary(const std::string& driverPath) {
    LOG("Loading driver from path: {}", driverPath);
#ifdef _WIN32
    // Windows: Convert string to wide string for LoadLibraryW
    std::wstring widePath(driverPath.begin(), driverPath.end());
    HMODULE handle = LoadLibraryW(widePath.c_str());
    if (!handle) {
        LOG("LoadLibraryW failed.");
    }
    return handle;
#else
    // macOS/Unix: Use dlopen
    void* handle = dlopen(driverPath.c_str(), RTLD_LAZY);
    if (!handle) {
        LOG("dlopen failed.");
    }
    return handle;
#endif
}

// Platform-agnostic function to get last error message
std::string GetLastErrorMessage() {
#ifdef _WIN32
    // Windows: Use FormatMessageA
    DWORD error = GetLastError();
    char* messageBuffer = nullptr;
    size_t size = FormatMessageA(
        FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
        NULL,
        error,
        MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
        (LPSTR)&messageBuffer,
        0,
        NULL
    );
    std::string errorMessage = messageBuffer ? std::string(messageBuffer, size) : "Unknown error";
    LocalFree(messageBuffer);
    return "Error code: " + std::to_string(error) + " - " + errorMessage;
#else
    // macOS/Unix: Use dlerror
    const char* error = dlerror();
    return error ? std::string(error) : "Unknown error";
#endif
}

// Helper to load the driver
// TODO: We don't need to do explicit linking using LoadLibrary. We can just use implicit
//       linking to load this DLL. It will simplify the code a lot.
DriverHandle LoadDriverOrThrowException() {
    namespace fs = std::filesystem;
    std::string moduleDir = GetModuleDirectory();
    LOG("Module directory: {}", moduleDir);
    std::string archStr = ARCHITECTURE;
    std::string archDir =
        (archStr == "win64" || archStr == "amd64" || archStr == "x64") ? "x64" :
        (archStr == "arm64") ? "arm64" :
        "x86";

    fs::path driverPath;
#ifdef _WIN32
    fs::path dllDir = fs::path(moduleDir) / "libs" / archDir;

    // Optionally load mssql-auth.dll if it exists
    fs::path authDllPath = dllDir / "mssql-auth.dll";
    if (fs::exists(authDllPath)) {
        HMODULE hAuth = LoadLibraryW(authDllPath.wstring().c_str());
        if (hAuth) {
            LOG("Authentication DLL loaded: {}", authDllPath.string());
        } else {
            LOG("Failed to load mssql-auth.dll: {}", GetLastErrorMessage());
        }
    } else {
        LOG("Note: mssql-auth.dll not found. This is OK if Entra ID is not in use.");
    }

    driverPath = dllDir / "msodbcsql18.dll";
#else // macOS
    std::string runtimeArch =
    #if defined(__arm64__) || defined(__aarch64__)
        "arm64";
    #else
        "x86_64";
    #endif
    fs::path primaryPath = fs::path(moduleDir) / "libs" / "macos" / runtimeArch / "lib" / "libmsodbcsql.18.dylib";
    if (fs::exists(primaryPath)) {
        driverPath = primaryPath;
        LOG("macOS driver found at: {}", driverPath.string());
    } else {
        driverPath = fs::path(moduleDir) / "libs" / archDir /  "macos/libmsodbcsql.18.dylib";
        LOG("Using fallback macOS driver path: {}", driverPath.string());
    }
#endif
    if (!fs::exists(driverPath)) {
        ThrowStdException("ODBC driver not found at: " + driverPath.string());
    }
    DriverHandle handle = LoadDriverLibrary(driverPath.string());
    if (!handle) {
        LOG("Failed to load driver: {}", GetLastErrorMessage());
        ThrowStdException("Failed to load ODBC driver. Please check installation.");
    }
    LOG("Driver library successfully loaded.");

    // Load function pointers using helper
    SQLAllocHandle_ptr = GetFunctionPointer<SQLAllocHandleFunc>(handle, "SQLAllocHandle");
    SQLSetEnvAttr_ptr = GetFunctionPointer<SQLSetEnvAttrFunc>(handle, "SQLSetEnvAttr");
    SQLSetConnectAttr_ptr = GetFunctionPointer<SQLSetConnectAttrFunc>(handle, "SQLSetConnectAttrW");
    SQLSetStmtAttr_ptr = GetFunctionPointer<SQLSetStmtAttrFunc>(handle, "SQLSetStmtAttrW");
    SQLGetConnectAttr_ptr = GetFunctionPointer<SQLGetConnectAttrFunc>(handle, "SQLGetConnectAttrW");

    SQLDriverConnect_ptr = GetFunctionPointer<SQLDriverConnectFunc>(handle, "SQLDriverConnectW");
    SQLExecDirect_ptr = GetFunctionPointer<SQLExecDirectFunc>(handle, "SQLExecDirectW");
    SQLPrepare_ptr = GetFunctionPointer<SQLPrepareFunc>(handle, "SQLPrepareW");
    SQLBindParameter_ptr = GetFunctionPointer<SQLBindParameterFunc>(handle, "SQLBindParameter");
    SQLExecute_ptr = GetFunctionPointer<SQLExecuteFunc>(handle, "SQLExecute");
    SQLRowCount_ptr = GetFunctionPointer<SQLRowCountFunc>(handle, "SQLRowCount");
    SQLGetStmtAttr_ptr = GetFunctionPointer<SQLGetStmtAttrFunc>(handle, "SQLGetStmtAttrW");
    SQLSetDescField_ptr = GetFunctionPointer<SQLSetDescFieldFunc>(handle, "SQLSetDescFieldW");

    SQLFetch_ptr = GetFunctionPointer<SQLFetchFunc>(handle, "SQLFetch");
    SQLFetchScroll_ptr = GetFunctionPointer<SQLFetchScrollFunc>(handle, "SQLFetchScroll");
    SQLGetData_ptr = GetFunctionPointer<SQLGetDataFunc>(handle, "SQLGetData");
    SQLNumResultCols_ptr = GetFunctionPointer<SQLNumResultColsFunc>(handle, "SQLNumResultCols");
    SQLBindCol_ptr = GetFunctionPointer<SQLBindColFunc>(handle, "SQLBindCol");
    SQLDescribeCol_ptr = GetFunctionPointer<SQLDescribeColFunc>(handle, "SQLDescribeColW");
    SQLMoreResults_ptr = GetFunctionPointer<SQLMoreResultsFunc>(handle, "SQLMoreResults");
    SQLColAttribute_ptr = GetFunctionPointer<SQLColAttributeFunc>(handle, "SQLColAttributeW");

    SQLEndTran_ptr = GetFunctionPointer<SQLEndTranFunc>(handle, "SQLEndTran");
    SQLDisconnect_ptr = GetFunctionPointer<SQLDisconnectFunc>(handle, "SQLDisconnect");
    SQLFreeHandle_ptr = GetFunctionPointer<SQLFreeHandleFunc>(handle, "SQLFreeHandle");
    SQLFreeStmt_ptr = GetFunctionPointer<SQLFreeStmtFunc>(handle, "SQLFreeStmt");

    SQLGetDiagRec_ptr = GetFunctionPointer<SQLGetDiagRecFunc>(handle, "SQLGetDiagRecW");

    bool success =
        SQLAllocHandle_ptr && SQLSetEnvAttr_ptr && SQLSetConnectAttr_ptr &&
        SQLSetStmtAttr_ptr && SQLGetConnectAttr_ptr && SQLDriverConnect_ptr &&
        SQLExecDirect_ptr && SQLPrepare_ptr && SQLBindParameter_ptr &&
        SQLExecute_ptr && SQLRowCount_ptr && SQLGetStmtAttr_ptr &&
        SQLSetDescField_ptr && SQLFetch_ptr && SQLFetchScroll_ptr &&
        SQLGetData_ptr && SQLNumResultCols_ptr && SQLBindCol_ptr &&
        SQLDescribeCol_ptr && SQLMoreResults_ptr && SQLColAttribute_ptr &&
        SQLEndTran_ptr && SQLDisconnect_ptr && SQLFreeHandle_ptr &&
        SQLFreeStmt_ptr && SQLGetDiagRec_ptr;

    if (!success) {
        ThrowStdException("Failed to load required function pointers from driver.");
    }
    LOG("All driver function pointers successfully loaded.");
    return handle;
}

// DriverLoader definition 
DriverLoader::DriverLoader() : m_driverLoaded(false) {}

DriverLoader& DriverLoader::getInstance() {
    static DriverLoader instance;
    return instance;
}

void DriverLoader::loadDriver() {
    std::call_once(m_onceFlag, [this]() {
        LoadDriverOrThrowException();
        m_driverLoaded = true;
    });
}

// SqlHandle definition
SqlHandle::SqlHandle(SQLSMALLINT type, SQLHANDLE rawHandle)
    : _type(type), _handle(rawHandle) {}

SqlHandle::~SqlHandle() {
    if (_handle) {
        free();
    }
}

SQLHANDLE SqlHandle::get() const {
    return _handle;
}

SQLSMALLINT SqlHandle::type() const {
    return _type;
}

void SqlHandle::free() {
    if (_handle && SQLFreeHandle_ptr) {
        const char* type_str = nullptr;
        switch (_type) {
            case SQL_HANDLE_ENV:  type_str = "ENV"; break;
            case SQL_HANDLE_DBC:  type_str = "DBC"; break;
            case SQL_HANDLE_STMT: type_str = "STMT"; break;
            case SQL_HANDLE_DESC: type_str = "DESC"; break;
            default:              type_str = "UNKNOWN"; break;
        }
        SQLFreeHandle_ptr(_type, _handle);
        _handle = nullptr;
        std::stringstream ss;
        ss << "Freed SQL Handle of type: " << type_str;
        LOG(ss.str());
    }
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
            LOG("Function pointer not initialized. Loading the driver.");
            DriverLoader::getInstance().loadDriver();  // Load the driver
        }

        SQLWCHAR sqlState[6], message[SQL_MAX_MESSAGE_LENGTH];
        SQLINTEGER nativeError;
        SQLSMALLINT messageLen;

        SQLRETURN diagReturn =
            SQLGetDiagRec_ptr(handleType, rawHandle, 1, sqlState,
                              &nativeError, message, SQL_MAX_MESSAGE_LENGTH, &messageLen);

        if (SQL_SUCCEEDED(diagReturn)) {
#if defined(_WIN32)
            // On Windows, SQLWCHAR and wchar_t are compatible
            errorInfo.sqlState = std::wstring(sqlState);
            errorInfo.ddbcErrorMsg = std::wstring(message);
#else
            // On macOS/Linux, need to convert SQLWCHAR (usually unsigned short) to wchar_t
            errorInfo.sqlState = SQLWCHARToWString(sqlState);
            errorInfo.ddbcErrorMsg = SQLWCHARToWString(message, messageLen);
#endif
        }
    }
    return errorInfo;
}

// Wrap SQLExecDirect
SQLRETURN SQLExecDirect_wrap(SqlHandlePtr StatementHandle, const std::wstring& Query) {
    LOG("Execute SQL query directly - {}", Query.c_str());
    if (!SQLExecDirect_ptr) {
        LOG("Function pointer not initialized. Loading the driver.");
        DriverLoader::getInstance().loadDriver();  // Load the driver
    }

    SQLWCHAR* queryPtr;
#if defined(__APPLE__)
    std::vector<SQLWCHAR> queryBuffer = WStringToSQLWCHAR(Query);
    queryPtr = queryBuffer.data();
#else
    queryPtr = const_cast<SQLWCHAR*>(Query.c_str());
#endif
    SQLRETURN ret = SQLExecDirect_ptr(StatementHandle->get(), queryPtr, SQL_NTS);
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
        LOG("Function pointer not initialized. Loading the driver.");
        DriverLoader::getInstance().loadDriver();  // Load the driver
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
    SQLWCHAR* queryPtr;
#if defined(__APPLE__)
    std::vector<SQLWCHAR> queryBuffer = WStringToSQLWCHAR(query);
    queryPtr = queryBuffer.data();
#else
    queryPtr = const_cast<SQLWCHAR*>(query.c_str());
#endif
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
        LOG("Function pointer not initialized. Loading the driver.");
        DriverLoader::getInstance().loadDriver();  // Load the driver
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
        LOG("Function pointer not initialized. Loading the driver.");
        DriverLoader::getInstance().loadDriver();  // Load the driver
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
#if defined(__APPLE__)
            ColumnMetadata.append(py::dict("ColumnName"_a = SQLWCHARToWString(ColumnName, SQL_NTS),
#else
            ColumnMetadata.append(py::dict("ColumnName"_a = std::wstring(ColumnName),
#endif
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
        LOG("Function pointer not initialized. Loading the driver.");
        DriverLoader::getInstance().loadDriver();  // Load the driver
    }

    return SQLFetch_ptr(StatementHandle->get());
}

// Helper function to retrieve column data
// TODO: Handle variable length data correctly
SQLRETURN SQLGetData_wrap(SqlHandlePtr StatementHandle, SQLUSMALLINT colCount, py::list& row) {
    LOG("Get data from columns");
    if (!SQLGetData_ptr) {
        LOG("Function pointer not initialized. Loading the driver.");
        DriverLoader::getInstance().loadDriver();  // Load the driver
    }

    SQLRETURN ret = SQL_SUCCESS;
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
#if defined(__APPLE__)
                            std::string fullStr(reinterpret_cast<char*>(dataBuffer.data()));
                            row.append(fullStr);
                            LOG("macOS: Appended CHAR string of length {} to result row", fullStr.length());
#else
                            row.append(std::string(reinterpret_cast<char*>(dataBuffer.data())));
#endif
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
#if defined(__APPLE__)
                            row.append(SQLWCHARToWString(dataBuffer.data(), SQL_NTS));
#else
                            row.append(std::wstring(dataBuffer.data()));
#endif
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
						if (static_cast<size_t>(dataLen) <= columnSize) {
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
#if defined(__APPLE__)
                        // Use macOS-specific conversion to handle the wchar_t/SQLWCHAR size difference
                        SQLWCHAR* wcharData = &buffers.wcharBuffers[col - 1][i * fetchBufferSize];
                        std::wstring wstr = SQLWCHARToWString(wcharData, numCharsInData);
                        row.append(wstr);
#else
                        // On Windows, wchar_t and SQLWCHAR are both 2 bytes, so direct cast works
                        row.append(std::wstring(
                            reinterpret_cast<wchar_t*>(&buffers.wcharBuffers[col - 1][i * fetchBufferSize]),
                            numCharsInData));
#endif
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
                    if (static_cast<size_t>(dataLen) <= columnSize) {
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
    SQLSetStmtAttr_ptr(hStmt, SQL_ATTR_ROW_ARRAY_SIZE, (SQLPOINTER)(intptr_t)fetchSize, 0);
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
    SQLSetStmtAttr_ptr(hStmt, SQL_ATTR_ROW_ARRAY_SIZE, (SQLPOINTER)(intptr_t)fetchSize, 0);
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
        LOG("Function pointer not initialized. Loading the driver.");
        DriverLoader::getInstance().loadDriver();  // Load the driver
    }

    return SQLMoreResults_ptr(StatementHandle->get());
}

// Wrap SQLFreeHandle
SQLRETURN SQLFreeHandle_wrap(SQLSMALLINT HandleType, SqlHandlePtr Handle) {
    LOG("Free SQL handle");
    if (!SQLAllocHandle_ptr) {
        LOG("Function pointer not initialized. Loading the driver.");
        DriverLoader::getInstance().loadDriver();  // Load the driver
    }

    SQLRETURN ret = SQLFreeHandle_ptr(HandleType, Handle->get());
    if (!SQL_SUCCEEDED(ret)) {
        LOG("SQLFreeHandle failed with error code - {}", ret);
    }
    return ret;
}

// Wrap SQLRowCount
SQLLEN SQLRowCount_wrap(SqlHandlePtr StatementHandle) {
    LOG("Get number of row affected by last execute");
    if (!SQLRowCount_ptr) {
        LOG("Function pointer not initialized. Loading the driver.");
        DriverLoader::getInstance().loadDriver();  // Load the driver
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

static std::once_flag pooling_init_flag;
void enable_pooling(int maxSize, int idleTimeout) {
    std::call_once(pooling_init_flag, [&]() {
        ConnectionPoolManager::getInstance().configure(maxSize, idleTimeout);
    });
}

// Architecture-specific defines
#ifndef ARCHITECTURE
#define ARCHITECTURE "win64"  // Default to win64 if not defined during compilation
#endif

// Functions/data to be exposed to Python as a part of ddbc_bindings module
PYBIND11_MODULE(ddbc_bindings, m) {
    m.doc() = "msodbcsql driver api bindings for Python";

    // Add architecture information as module attribute
    m.attr("__architecture__") = ARCHITECTURE;

    // Expose architecture-specific constants
    m.attr("ARCHITECTURE") = ARCHITECTURE;
    
    // Expose the C++ functions to Python
    m.def("ThrowStdException", &ThrowStdException);

    // Define parameter info class
    py::class_<ParamInfo>(m, "ParamInfo")
        .def(py::init<>())
        .def_readwrite("inputOutputType", &ParamInfo::inputOutputType)
        .def_readwrite("paramCType", &ParamInfo::paramCType)
        .def_readwrite("paramSQLType", &ParamInfo::paramSQLType)
        .def_readwrite("columnSize", &ParamInfo::columnSize)
        .def_readwrite("decimalDigits", &ParamInfo::decimalDigits);
    
    // Define numeric data class
    py::class_<NumericData>(m, "NumericData")
        .def(py::init<>())
        .def(py::init<SQLCHAR, SQLSCHAR, SQLCHAR, std::uint64_t>())
        .def_readwrite("precision", &NumericData::precision)
        .def_readwrite("scale", &NumericData::scale)
        .def_readwrite("sign", &NumericData::sign)
        .def_readwrite("val", &NumericData::val);

    // Define error info class
    py::class_<ErrorInfo>(m, "ErrorInfo")
        .def_readwrite("sqlState", &ErrorInfo::sqlState)
        .def_readwrite("ddbcErrorMsg", &ErrorInfo::ddbcErrorMsg);
        
    py::class_<SqlHandle, SqlHandlePtr>(m, "SqlHandle")
        .def("free", &SqlHandle::free, "Free the handle");
  
    py::class_<ConnectionHandle>(m, "Connection")
        .def(py::init<const std::string&, bool, const py::dict&>(), py::arg("conn_str"), py::arg("use_pool"), py::arg("attrs_before") = py::dict())
        .def("close", &ConnectionHandle::close, "Close the connection")
        .def("commit", &ConnectionHandle::commit, "Commit the current transaction")
        .def("rollback", &ConnectionHandle::rollback, "Rollback the current transaction")
        .def("set_autocommit", &ConnectionHandle::setAutocommit)
        .def("get_autocommit", &ConnectionHandle::getAutocommit)
        .def("alloc_statement_handle", &ConnectionHandle::allocStatementHandle);
    m.def("enable_pooling", &enable_pooling, "Enable global connection pooling");
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
    m.def("DDBCSQLFreeHandle", &SQLFreeHandle_wrap, "Free a handle");
    m.def("DDBCSQLCheckError", &SQLCheckError_Wrap, "Check for driver errors");

    // Add a version attribute
    m.attr("__version__") = "1.0.0";
    
    try {
        // Try loading the ODBC driver when the module is imported
        LOG("Loading ODBC driver");
        DriverLoader::getInstance().loadDriver();  // Load the driver
    } catch (const std::exception& e) {
        // Log the error but don't throw - let the error happen when functions are called
        LOG("Failed to load ODBC driver during module initialization: {}", e.what());
    }
}
