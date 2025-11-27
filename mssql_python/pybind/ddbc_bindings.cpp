// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

// INFO|TODO - Note that is file is Windows specific right now. Making it arch
// agnostic will be
//             taken up in beta release
#include "ddbc_bindings.h"
#include "connection/connection.h"
#include "connection/connection_pool.h"
#include "logger_bridge.hpp"

#include <cstdint>
#include <cstring>  // For std::memcpy
#include <filesystem>
#include <iomanip>  // std::setw, std::setfill
#include <iostream>
#include <utility>  // std::forward

//-------------------------------------------------------------------------------------------------
// Macro definitions
//-------------------------------------------------------------------------------------------------

// This constant is not exposed via sql.h, hence define it here
#define SQL_SS_TIME2 (-154)
#define SQL_SS_TIMESTAMPOFFSET (-155)
#define SQL_C_SS_TIMESTAMPOFFSET (0x4001)
#define MAX_DIGITS_IN_NUMERIC 64
#define SQL_MAX_NUMERIC_LEN 16
#define SQL_SS_XML (-152)

#define STRINGIFY_FOR_CASE(x)                                                                      \
    case x:                                                                                        \
        return #x

// Architecture-specific defines
#ifndef ARCHITECTURE
#define ARCHITECTURE "win64"  // Default to win64 if not defined during compilation
#endif
#define DAE_CHUNK_SIZE 8192
#define SQL_MAX_LOB_SIZE 8000

//-------------------------------------------------------------------------------------------------
//-------------------------------------------------------------------------------------------------
// Logging Infrastructure:
// - LOG() macro: All diagnostic/debug logging at DEBUG level (single level)
// - LOG_INFO/WARNING/ERROR: Higher-level messages for production
// Uses printf-style formatting: LOG("Value: %d", x) -- __FILE__/__LINE__
// embedded in macro
//-------------------------------------------------------------------------------------------------
namespace PythonObjectCache {
static py::object datetime_class;
static py::object date_class;
static py::object time_class;
static py::object decimal_class;
static py::object uuid_class;
static bool cache_initialized = false;

void initialize() {
    if (!cache_initialized) {
        auto datetime_module = py::module_::import("datetime");
        datetime_class = datetime_module.attr("datetime");
        date_class = datetime_module.attr("date");
        time_class = datetime_module.attr("time");

        auto decimal_module = py::module_::import("decimal");
        decimal_class = decimal_module.attr("Decimal");

        auto uuid_module = py::module_::import("uuid");
        uuid_class = uuid_module.attr("UUID");

        cache_initialized = true;
    }
}

py::object get_datetime_class() {
    if (cache_initialized && datetime_class) {
        return datetime_class;
    }
    return py::module_::import("datetime").attr("datetime");
}

py::object get_date_class() {
    if (cache_initialized && date_class) {
        return date_class;
    }
    return py::module_::import("datetime").attr("date");
}

py::object get_time_class() {
    if (cache_initialized && time_class) {
        return time_class;
    }
    return py::module_::import("datetime").attr("time");
}

py::object get_decimal_class() {
    if (cache_initialized && decimal_class) {
        return decimal_class;
    }
    return py::module_::import("decimal").attr("Decimal");
}

py::object get_uuid_class() {
    if (cache_initialized && uuid_class) {
        return uuid_class;
    }
    return py::module_::import("uuid").attr("UUID");
}
}  // namespace PythonObjectCache

//-------------------------------------------------------------------------------------------------
// Class definitions
//-------------------------------------------------------------------------------------------------

// Struct to hold parameter information for binding. Used by SQLBindParameter.
// This struct is shared between C++ & Python code.
#ifdef __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wattributes"
#endif
struct ParamInfo {
    SQLSMALLINT inputOutputType;
    SQLSMALLINT paramCType;
    SQLSMALLINT paramSQLType;
    SQLULEN columnSize;
    SQLSMALLINT decimalDigits;
    SQLLEN strLenOrInd = 0;  // Required for DAE
    bool isDAE = false;      // Indicates if we need to stream
    py::object dataPtr;
};
#ifdef __GNUC__
#pragma GCC diagnostic pop
#endif

// Mirrors the SQL_NUMERIC_STRUCT. But redefined to replace val char array
// with std::string, because pybind doesn't allow binding char array.
// This struct is shared between C++ & Python code.
struct NumericData {
    SQLCHAR precision;
    SQLSCHAR scale;
    SQLCHAR sign;     // 1=pos, 0=neg
    std::string val;  // 123.45 -> 12345

    NumericData() : precision(0), scale(0), sign(0), val(SQL_MAX_NUMERIC_LEN, '\0') {}

    NumericData(SQLCHAR precision, SQLSCHAR scale, SQLCHAR sign, const std::string& valueBytes)
        : precision(precision), scale(scale), sign(sign), val(SQL_MAX_NUMERIC_LEN, '\0') {
        if (valueBytes.size() > SQL_MAX_NUMERIC_LEN) {
            throw std::runtime_error(
                "NumericData valueBytes size exceeds SQL_MAX_NUMERIC_LEN (16)");
        }
        // Copy binary data to buffer, remaining bytes stay zero-padded
        std::memcpy(&val[0], valueBytes.data(), valueBytes.size());
    }
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
SQLGetTypeInfoFunc SQLGetTypeInfo_ptr = nullptr;
SQLProceduresFunc SQLProcedures_ptr = nullptr;
SQLForeignKeysFunc SQLForeignKeys_ptr = nullptr;
SQLPrimaryKeysFunc SQLPrimaryKeys_ptr = nullptr;
SQLSpecialColumnsFunc SQLSpecialColumns_ptr = nullptr;
SQLStatisticsFunc SQLStatistics_ptr = nullptr;
SQLColumnsFunc SQLColumns_ptr = nullptr;
SQLGetInfoFunc SQLGetInfo_ptr = nullptr;

// Transaction APIs
SQLEndTranFunc SQLEndTran_ptr = nullptr;

// Disconnect/free APIs
SQLFreeHandleFunc SQLFreeHandle_ptr = nullptr;
SQLDisconnectFunc SQLDisconnect_ptr = nullptr;
SQLFreeStmtFunc SQLFreeStmt_ptr = nullptr;

// Diagnostic APIs
SQLGetDiagRecFunc SQLGetDiagRec_ptr = nullptr;

// DAE APIs
SQLParamDataFunc SQLParamData_ptr = nullptr;
SQLPutDataFunc SQLPutData_ptr = nullptr;
SQLTablesFunc SQLTables_ptr = nullptr;

SQLDescribeParamFunc SQLDescribeParam_ptr = nullptr;

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
    std::string errorString = "Parameter's object type does not match "
                              "parameter's C type. paramIndex - " +
                              std::to_string(paramIndex) + ", C type - " +
                              GetSqlCTypeAsString(cType);
    return errorString;
}

// This function allocates a buffer of ParamType, stores it as a void* in
// paramBuffers for book-keeping and then returns a ParamType* to the allocated
// memory. ctorArgs are the arguments to ParamType's constructor used while
// creating/allocating ParamType
template <typename ParamType, typename... CtorArgs>
ParamType* AllocateParamBuffer(std::vector<std::shared_ptr<void>>& paramBuffers,
                               CtorArgs&&... ctorArgs) {
    paramBuffers.emplace_back(new ParamType(std::forward<CtorArgs>(ctorArgs)...),
                              std::default_delete<ParamType>());
    return static_cast<ParamType*>(paramBuffers.back().get());
}

template <typename ParamType>
ParamType* AllocateParamBufferArray(std::vector<std::shared_ptr<void>>& paramBuffers,
                                    size_t count) {
    std::shared_ptr<ParamType> buffer(new ParamType[count], std::default_delete<ParamType[]>());
    ParamType* raw = buffer.get();
    paramBuffers.push_back(buffer);
    return raw;
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

// Given a list of parameters and their ParamInfo, calls SQLBindParameter on
// each of them with appropriate arguments
SQLRETURN BindParameters(SQLHANDLE hStmt, const py::list& params,
                         std::vector<ParamInfo>& paramInfos,
                         std::vector<std::shared_ptr<void>>& paramBuffers) {
    LOG("BindParameters: Starting parameter binding for statement handle %p "
        "with %zu parameters",
        (void*)hStmt, params.size());
    for (int paramIndex = 0; paramIndex < params.size(); paramIndex++) {
        const auto& param = params[paramIndex];
        ParamInfo& paramInfo = paramInfos[paramIndex];
        LOG("BindParameters: Processing param[%d] - C_Type=%d, SQL_Type=%d, "
            "ColumnSize=%lu, DecimalDigits=%d, InputOutputType=%d",
            paramIndex, paramInfo.paramCType, paramInfo.paramSQLType,
            (unsigned long)paramInfo.columnSize, paramInfo.decimalDigits,
            paramInfo.inputOutputType);
        void* dataPtr = nullptr;
        SQLLEN bufferLength = 0;
        SQLLEN* strLenOrIndPtr = nullptr;

        // TODO: Add more data types like money, guid, interval, TVPs etc.
        switch (paramInfo.paramCType) {
            case SQL_C_CHAR: {
                if (!py::isinstance<py::str>(param) && !py::isinstance<py::bytearray>(param) &&
                    !py::isinstance<py::bytes>(param)) {
                    ThrowStdException(MakeParamMismatchErrorStr(paramInfo.paramCType, paramIndex));
                }
                if (paramInfo.isDAE) {
                    LOG("BindParameters: param[%d] SQL_C_CHAR - Using DAE "
                        "(Data-At-Execution) for large string streaming",
                        paramIndex);
                    dataPtr =
                        const_cast<void*>(reinterpret_cast<const void*>(&paramInfos[paramIndex]));
                    strLenOrIndPtr = AllocateParamBuffer<SQLLEN>(paramBuffers);
                    *strLenOrIndPtr = SQL_LEN_DATA_AT_EXEC(0);
                    bufferLength = 0;
                } else {
                    std::string* strParam =
                        AllocateParamBuffer<std::string>(paramBuffers, param.cast<std::string>());
                    dataPtr = const_cast<void*>(static_cast<const void*>(strParam->c_str()));
                    bufferLength = strParam->size() + 1;
                    strLenOrIndPtr = AllocateParamBuffer<SQLLEN>(paramBuffers);
                    *strLenOrIndPtr = SQL_NTS;
                }
                break;
            }
            case SQL_C_BINARY: {
                if (!py::isinstance<py::str>(param) && !py::isinstance<py::bytearray>(param) &&
                    !py::isinstance<py::bytes>(param)) {
                    ThrowStdException(MakeParamMismatchErrorStr(paramInfo.paramCType, paramIndex));
                }
                if (paramInfo.isDAE) {
                    // Deferred execution for VARBINARY(MAX)
                    LOG("BindParameters: param[%d] SQL_C_BINARY - Using DAE "
                        "for VARBINARY(MAX) streaming",
                        paramIndex);
                    dataPtr =
                        const_cast<void*>(reinterpret_cast<const void*>(&paramInfos[paramIndex]));
                    strLenOrIndPtr = AllocateParamBuffer<SQLLEN>(paramBuffers);
                    *strLenOrIndPtr = SQL_LEN_DATA_AT_EXEC(0);
                    bufferLength = 0;
                } else {
                    // small binary
                    std::string binData;
                    if (py::isinstance<py::bytes>(param)) {
                        binData = param.cast<std::string>();
                    } else {
                        // bytearray
                        binData = std::string(
                            reinterpret_cast<const char*>(PyByteArray_AsString(param.ptr())),
                            PyByteArray_Size(param.ptr()));
                    }
                    std::string* binBuffer =
                        AllocateParamBuffer<std::string>(paramBuffers, binData);
                    dataPtr = const_cast<void*>(static_cast<const void*>(binBuffer->data()));
                    bufferLength = static_cast<SQLLEN>(binBuffer->size());
                    strLenOrIndPtr = AllocateParamBuffer<SQLLEN>(paramBuffers);
                    *strLenOrIndPtr = bufferLength;
                }
                break;
            }
            case SQL_C_WCHAR: {
                if (!py::isinstance<py::str>(param) && !py::isinstance<py::bytearray>(param) &&
                    !py::isinstance<py::bytes>(param)) {
                    ThrowStdException(MakeParamMismatchErrorStr(paramInfo.paramCType, paramIndex));
                }
                if (paramInfo.isDAE) {
                    // deferred execution
                    LOG("BindParameters: param[%d] SQL_C_WCHAR - Using DAE for "
                        "NVARCHAR(MAX) streaming",
                        paramIndex);
                    dataPtr =
                        const_cast<void*>(reinterpret_cast<const void*>(&paramInfos[paramIndex]));
                    strLenOrIndPtr = AllocateParamBuffer<SQLLEN>(paramBuffers);
                    *strLenOrIndPtr = SQL_LEN_DATA_AT_EXEC(0);
                    bufferLength = 0;
                } else {
                    // Normal small-string case
                    std::wstring* strParam =
                        AllocateParamBuffer<std::wstring>(paramBuffers, param.cast<std::wstring>());
                    LOG("BindParameters: param[%d] SQL_C_WCHAR - String "
                        "length=%zu characters, buffer=%zu bytes",
                        paramIndex, strParam->size(), strParam->size() * sizeof(SQLWCHAR));
                    std::vector<SQLWCHAR>* sqlwcharBuffer =
                        AllocateParamBuffer<std::vector<SQLWCHAR>>(paramBuffers,
                                                                   WStringToSQLWCHAR(*strParam));
                    dataPtr = sqlwcharBuffer->data();
                    bufferLength = sqlwcharBuffer->size() * sizeof(SQLWCHAR);
                    strLenOrIndPtr = AllocateParamBuffer<SQLLEN>(paramBuffers);
                    *strLenOrIndPtr = SQL_NTS;
                }
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
                SQLSMALLINT sqlType = paramInfo.paramSQLType;
                SQLULEN columnSize = paramInfo.columnSize;
                SQLSMALLINT decimalDigits = paramInfo.decimalDigits;
                if (sqlType == SQL_UNKNOWN_TYPE) {
                    SQLSMALLINT describedType;
                    SQLULEN describedSize;
                    SQLSMALLINT describedDigits;
                    SQLSMALLINT nullable;
                    RETCODE rc = SQLDescribeParam_ptr(
                        hStmt, static_cast<SQLUSMALLINT>(paramIndex + 1), &describedType,
                        &describedSize, &describedDigits, &nullable);
                    if (!SQL_SUCCEEDED(rc)) {
                        LOG("BindParameters: SQLDescribeParam failed for "
                            "param[%d] (NULL parameter) - SQLRETURN=%d",
                            paramIndex, rc);
                        return rc;
                    }
                    sqlType = describedType;
                    columnSize = describedSize;
                    decimalDigits = describedDigits;
                }
                dataPtr = nullptr;
                strLenOrIndPtr = AllocateParamBuffer<SQLLEN>(paramBuffers);
                *strLenOrIndPtr = SQL_NULL_DATA;
                bufferLength = 0;
                paramInfo.paramSQLType = sqlType;
                paramInfo.columnSize = columnSize;
                paramInfo.decimalDigits = decimalDigits;
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
                if (value < std::numeric_limits<short>::min() ||
                    value > std::numeric_limits<short>::max()) {
                    ThrowStdException("Signed short integer parameter out of "
                                      "range at paramIndex " +
                                      std::to_string(paramIndex));
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
                    ThrowStdException("Unsigned short integer parameter out of "
                                      "range at paramIndex " +
                                      std::to_string(paramIndex));
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
                if (value < std::numeric_limits<int64_t>::min() ||
                    value > std::numeric_limits<int64_t>::max()) {
                    ThrowStdException("Signed 64-bit integer parameter out of "
                                      "range at paramIndex " +
                                      std::to_string(paramIndex));
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
                    ThrowStdException("Unsigned 64-bit integer parameter out "
                                      "of range at paramIndex " +
                                      std::to_string(paramIndex));
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
                py::object dateType = PythonObjectCache::get_date_class();
                if (!py::isinstance(param, dateType)) {
                    ThrowStdException(MakeParamMismatchErrorStr(paramInfo.paramCType, paramIndex));
                }
                int year = param.attr("year").cast<int>();
                if (year < 1753 || year > 9999) {
                    ThrowStdException("Date out of range for SQL Server "
                                      "(1753-9999) at paramIndex " +
                                      std::to_string(paramIndex));
                }
                // TODO: can be moved to python by registering SQL_DATE_STRUCT
                // in pybind
                SQL_DATE_STRUCT* sqlDatePtr = AllocateParamBuffer<SQL_DATE_STRUCT>(paramBuffers);
                sqlDatePtr->year = static_cast<SQLSMALLINT>(param.attr("year").cast<int>());
                sqlDatePtr->month = static_cast<SQLUSMALLINT>(param.attr("month").cast<int>());
                sqlDatePtr->day = static_cast<SQLUSMALLINT>(param.attr("day").cast<int>());
                dataPtr = static_cast<void*>(sqlDatePtr);
                break;
            }
            case SQL_C_TYPE_TIME: {
                py::object timeType = PythonObjectCache::get_time_class();
                if (!py::isinstance(param, timeType)) {
                    ThrowStdException(MakeParamMismatchErrorStr(paramInfo.paramCType, paramIndex));
                }
                // TODO: can be moved to python by registering SQL_TIME_STRUCT
                // in pybind
                SQL_TIME_STRUCT* sqlTimePtr = AllocateParamBuffer<SQL_TIME_STRUCT>(paramBuffers);
                sqlTimePtr->hour = static_cast<SQLUSMALLINT>(param.attr("hour").cast<int>());
                sqlTimePtr->minute = static_cast<SQLUSMALLINT>(param.attr("minute").cast<int>());
                sqlTimePtr->second = static_cast<SQLUSMALLINT>(param.attr("second").cast<int>());
                dataPtr = static_cast<void*>(sqlTimePtr);
                break;
            }
            case SQL_C_SS_TIMESTAMPOFFSET: {
                py::object datetimeType = PythonObjectCache::get_datetime_class();
                if (!py::isinstance(param, datetimeType)) {
                    ThrowStdException(MakeParamMismatchErrorStr(paramInfo.paramCType, paramIndex));
                }
                // Checking if the object has a timezone
                py::object tzinfo = param.attr("tzinfo");
                if (tzinfo.is_none()) {
                    ThrowStdException("Datetime object must have tzinfo for "
                                      "SQL_C_SS_TIMESTAMPOFFSET at paramIndex " +
                                      std::to_string(paramIndex));
                }

                DateTimeOffset* dtoPtr = AllocateParamBuffer<DateTimeOffset>(paramBuffers);

                dtoPtr->year = static_cast<SQLSMALLINT>(param.attr("year").cast<int>());
                dtoPtr->month = static_cast<SQLUSMALLINT>(param.attr("month").cast<int>());
                dtoPtr->day = static_cast<SQLUSMALLINT>(param.attr("day").cast<int>());
                dtoPtr->hour = static_cast<SQLUSMALLINT>(param.attr("hour").cast<int>());
                dtoPtr->minute = static_cast<SQLUSMALLINT>(param.attr("minute").cast<int>());
                dtoPtr->second = static_cast<SQLUSMALLINT>(param.attr("second").cast<int>());
                // SQL server supports in ns, but python datetime supports in µs
                dtoPtr->fraction =
                    static_cast<SQLUINTEGER>(param.attr("microsecond").cast<int>() * 1000);

                py::object utcoffset = tzinfo.attr("utcoffset")(param);
                if (utcoffset.is_none()) {
                    ThrowStdException("Datetime object's tzinfo.utcoffset() "
                                      "returned None at paramIndex " +
                                      std::to_string(paramIndex));
                }

                int total_seconds =
                    static_cast<int>(utcoffset.attr("total_seconds")().cast<double>());
                const int MAX_OFFSET = 14 * 3600;
                const int MIN_OFFSET = -14 * 3600;

                if (total_seconds > MAX_OFFSET || total_seconds < MIN_OFFSET) {
                    ThrowStdException("Datetimeoffset tz offset out of SQL Server range "
                                      "(-14h to +14h) at paramIndex " +
                                      std::to_string(paramIndex));
                }
                std::div_t div_result = std::div(total_seconds, 3600);
                dtoPtr->timezone_hour = static_cast<SQLSMALLINT>(div_result.quot);
                dtoPtr->timezone_minute = static_cast<SQLSMALLINT>(div(div_result.rem, 60).quot);

                dataPtr = static_cast<void*>(dtoPtr);
                bufferLength = sizeof(DateTimeOffset);
                strLenOrIndPtr = AllocateParamBuffer<SQLLEN>(paramBuffers);
                *strLenOrIndPtr = bufferLength;
                break;
            }
            case SQL_C_TYPE_TIMESTAMP: {
                py::object datetimeType = PythonObjectCache::get_datetime_class();
                if (!py::isinstance(param, datetimeType)) {
                    ThrowStdException(MakeParamMismatchErrorStr(paramInfo.paramCType, paramIndex));
                }
                SQL_TIMESTAMP_STRUCT* sqlTimestampPtr =
                    AllocateParamBuffer<SQL_TIMESTAMP_STRUCT>(paramBuffers);
                sqlTimestampPtr->year = static_cast<SQLSMALLINT>(param.attr("year").cast<int>());
                sqlTimestampPtr->month = static_cast<SQLUSMALLINT>(param.attr("month").cast<int>());
                sqlTimestampPtr->day = static_cast<SQLUSMALLINT>(param.attr("day").cast<int>());
                sqlTimestampPtr->hour = static_cast<SQLUSMALLINT>(param.attr("hour").cast<int>());
                sqlTimestampPtr->minute =
                    static_cast<SQLUSMALLINT>(param.attr("minute").cast<int>());
                sqlTimestampPtr->second =
                    static_cast<SQLUSMALLINT>(param.attr("second").cast<int>());
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
                LOG("BindParameters: param[%d] SQL_C_NUMERIC - precision=%d, "
                    "scale=%d, sign=%d, value_bytes=%zu",
                    paramIndex, decimalParam.precision, decimalParam.scale, decimalParam.sign,
                    decimalParam.val.size());
                SQL_NUMERIC_STRUCT* decimalPtr =
                    AllocateParamBuffer<SQL_NUMERIC_STRUCT>(paramBuffers);
                decimalPtr->precision = decimalParam.precision;
                decimalPtr->scale = decimalParam.scale;
                decimalPtr->sign = decimalParam.sign;
                // Convert the integer decimalParam.val to char array
                std::memset(static_cast<void*>(decimalPtr->val), 0, sizeof(decimalPtr->val));
                size_t copyLen = std::min(decimalParam.val.size(), sizeof(decimalPtr->val));
                if (copyLen > 0) {
                    std::memcpy(decimalPtr->val, decimalParam.val.data(), copyLen);
                }
                dataPtr = static_cast<void*>(decimalPtr);
                break;
            }
            case SQL_C_GUID: {
                if (!py::isinstance<py::bytes>(param)) {
                    ThrowStdException(MakeParamMismatchErrorStr(paramInfo.paramCType, paramIndex));
                }
                py::bytes uuid_bytes = param.cast<py::bytes>();
                const unsigned char* uuid_data =
                    reinterpret_cast<const unsigned char*>(PyBytes_AS_STRING(uuid_bytes.ptr()));
                if (PyBytes_GET_SIZE(uuid_bytes.ptr()) != 16) {
                    LOG("BindParameters: param[%d] SQL_C_GUID - Invalid UUID "
                        "length: expected 16 bytes, got %ld bytes",
                        paramIndex, PyBytes_GET_SIZE(uuid_bytes.ptr()));
                    ThrowStdException("UUID binary data must be exactly 16 bytes long.");
                }
                SQLGUID* guid_data_ptr = AllocateParamBuffer<SQLGUID>(paramBuffers);
                guid_data_ptr->Data1 = (static_cast<uint32_t>(uuid_data[3]) << 24) |
                                       (static_cast<uint32_t>(uuid_data[2]) << 16) |
                                       (static_cast<uint32_t>(uuid_data[1]) << 8) |
                                       (static_cast<uint32_t>(uuid_data[0]));
                guid_data_ptr->Data2 = (static_cast<uint16_t>(uuid_data[5]) << 8) |
                                       (static_cast<uint16_t>(uuid_data[4]));
                guid_data_ptr->Data3 = (static_cast<uint16_t>(uuid_data[7]) << 8) |
                                       (static_cast<uint16_t>(uuid_data[6]));
                std::memcpy(guid_data_ptr->Data4, &uuid_data[8], 8);
                dataPtr = static_cast<void*>(guid_data_ptr);
                bufferLength = sizeof(SQLGUID);
                strLenOrIndPtr = AllocateParamBuffer<SQLLEN>(paramBuffers);
                *strLenOrIndPtr = sizeof(SQLGUID);
                break;
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
            hStmt, static_cast<SQLUSMALLINT>(paramIndex + 1), /* 1-based indexing */
            static_cast<SQLUSMALLINT>(paramInfo.inputOutputType),
            static_cast<SQLSMALLINT>(paramInfo.paramCType),
            static_cast<SQLSMALLINT>(paramInfo.paramSQLType), paramInfo.columnSize,
            paramInfo.decimalDigits, dataPtr, bufferLength, strLenOrIndPtr);
        if (!SQL_SUCCEEDED(rc)) {
            LOG("BindParameters: SQLBindParameter failed for param[%d] - "
                "SQLRETURN=%d, C_Type=%d, SQL_Type=%d",
                paramIndex, rc, paramInfo.paramCType, paramInfo.paramSQLType);
            return rc;
        }
        // Special handling for Numeric type -
        // https://learn.microsoft.com/en-us/sql/odbc/reference/appendixes/retrieve-numeric-data-sql-numeric-struct-kb222831?view=sql-server-ver16#sql_c_numeric-overview
        if (paramInfo.paramCType == SQL_C_NUMERIC) {
            SQLHDESC hDesc = nullptr;
            rc = SQLGetStmtAttr_ptr(hStmt, SQL_ATTR_APP_PARAM_DESC, &hDesc, 0, NULL);
            if (!SQL_SUCCEEDED(rc)) {
                LOG("BindParameters: SQLGetStmtAttr(SQL_ATTR_APP_PARAM_DESC) "
                    "failed for param[%d] - SQLRETURN=%d",
                    paramIndex, rc);
                return rc;
            }
            rc = SQLSetDescField_ptr(hDesc, 1, SQL_DESC_TYPE, (SQLPOINTER)SQL_C_NUMERIC, 0);
            if (!SQL_SUCCEEDED(rc)) {
                LOG("BindParameters: SQLSetDescField(SQL_DESC_TYPE) failed for "
                    "param[%d] - SQLRETURN=%d",
                    paramIndex, rc);
                return rc;
            }
            SQL_NUMERIC_STRUCT* numericPtr = reinterpret_cast<SQL_NUMERIC_STRUCT*>(dataPtr);
            rc = SQLSetDescField_ptr(hDesc, 1, SQL_DESC_PRECISION,
                                     reinterpret_cast<SQLPOINTER>(static_cast<uintptr_t>(numericPtr->precision)), 0);
            if (!SQL_SUCCEEDED(rc)) {
                LOG("BindParameters: SQLSetDescField(SQL_DESC_PRECISION) "
                    "failed for param[%d] - SQLRETURN=%d",
                    paramIndex, rc);
                return rc;
            }

            rc = SQLSetDescField_ptr(hDesc, 1, SQL_DESC_SCALE, reinterpret_cast<SQLPOINTER>(static_cast<uintptr_t>(numericPtr->scale)), 0);
            if (!SQL_SUCCEEDED(rc)) {
                LOG("BindParameters: SQLSetDescField(SQL_DESC_SCALE) failed "
                    "for param[%d] - SQLRETURN=%d",
                    paramIndex, rc);
                return rc;
            }

            rc = SQLSetDescField_ptr(hDesc, 1, SQL_DESC_DATA_PTR, reinterpret_cast<SQLPOINTER>(numericPtr), 0);
            if (!SQL_SUCCEEDED(rc)) {
                LOG("BindParameters: SQLSetDescField(SQL_DESC_DATA_PTR) failed "
                    "for param[%d] - SQLRETURN=%d",
                    paramIndex, rc);
                return rc;
            }
        }
    }
    LOG("BindParameters: Completed parameter binding for statement handle %p - "
        "%zu parameters bound successfully",
        (void*)hStmt, params.size());
    return SQL_SUCCESS;
}

// This is temporary hack to avoid crash when SQLDescribeCol returns 0 as
// columnSize for NVARCHAR(MAX) & similar types. Variable length data needs more
// nuanced handling.
// TODO: Fix this in beta
// This function sets the buffer allocated to fetch NVARCHAR(MAX) & similar
// types to 4096 chars. So we'll retrieve data upto 4096. Anything greater then
// that will throw error
void HandleZeroColumnSizeAtFetch(SQLULEN& columnSize) {
    if (columnSize == 0) {
        columnSize = 4096;
    }
}

}  // namespace

// Helper function to check if Python is shutting down or finalizing
// This centralizes the shutdown detection logic to avoid code duplication
static bool is_python_finalizing() {
    try {
        if (Py_IsInitialized() == 0) {
            return true;  // Python is already shut down
        }

        py::gil_scoped_acquire gil;
        py::object sys_module = py::module_::import("sys");
        if (!sys_module.is_none()) {
            // Check if the attribute exists before accessing it (for Python
            // version compatibility)
            if (py::hasattr(sys_module, "_is_finalizing")) {
                py::object finalizing_func = sys_module.attr("_is_finalizing");
                if (!finalizing_func.is_none() && finalizing_func().cast<bool>()) {
                    return true;  // Python is finalizing
                }
            }
        }
        return false;
    } catch (...) {
        std::cerr << "Error occurred while checking Python finalization state." << std::endl;
        // Be conservative - don't assume shutdown on any exception
        // Only return true if we're absolutely certain Python is shutting down
        return false;
    }
}

// TODO: Add more nuanced exception classes
void ThrowStdException(const std::string& message) {
    throw std::runtime_error(message);
}
std::string GetLastErrorMessage();

// TODO: Move this to Python
std::string GetModuleDirectory() {
    py::object module = py::module::import("mssql_python");
    py::object module_path = module.attr("__file__");
    std::string module_file = module_path.cast<std::string>();

#ifdef _WIN32
    // Windows-specific path handling
    char path[MAX_PATH];
    errno_t err = strncpy_s(path, MAX_PATH, module_file.c_str(), module_file.length());
    if (err != 0) {
        LOG("GetModuleDirectory: strncpy_s failed copying path - "
            "error_code=%d, path_length=%zu",
            err, module_file.length());
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
    LOG("GetModuleDirectory: Could not extract directory from module path - "
        "path='%s'",
        module_file.c_str());
    return module_file;
#endif
}

// Platform-agnostic function to load the driver dynamic library
DriverHandle LoadDriverLibrary(const std::string& driverPath) {
    LOG("LoadDriverLibrary: Attempting to load ODBC driver from path='%s'", driverPath.c_str());

#ifdef _WIN32
    // Windows: Convert string to wide string for LoadLibraryW
    std::wstring widePath(driverPath.begin(), driverPath.end());
    HMODULE handle = LoadLibraryW(widePath.c_str());
    if (!handle) {
        LOG("LoadDriverLibrary: LoadLibraryW failed for path='%s' - %s", driverPath.c_str(),
            GetLastErrorMessage().c_str());
        ThrowStdException("Failed to load library: " + driverPath);
    }
    return handle;
#else
    // macOS/Unix: Use dlopen
    void* handle = dlopen(driverPath.c_str(), RTLD_LAZY);
    if (!handle) {
        LOG("LoadDriverLibrary: dlopen failed for path='%s' - %s", driverPath.c_str(),
            dlerror() ? dlerror() : "unknown error");
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
        NULL, error, MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), (LPSTR)&messageBuffer, 0, NULL);
    std::string errorMessage = messageBuffer ? std::string(messageBuffer, size) : "Unknown error";
    LocalFree(messageBuffer);
    return "Error code: " + std::to_string(error) + " - " + errorMessage;
#else
    // macOS/Unix: Use dlerror
    const char* error = dlerror();
    return error ? std::string(error) : "Unknown error";
#endif
}

/*
 * Resolve ODBC driver path in C++ to avoid circular import issues on Alpine.
 *
 * Background:
 * On Alpine Linux, calling into Python during module initialization (via
 * pybind11) causes a circular import due to musl's stricter dynamic loader
 * behavior.
 *
 * Specifically, importing Python helpers from C++ triggered a re-import of the
 * partially-initialized native module, which works on glibc (Ubuntu/macOS) but
 * fails on musl-based systems like Alpine.
 *
 * By moving driver path resolution entirely into C++, we avoid any Python-layer
 * dependencies during critical initialization, ensuring compatibility across
 * all supported platforms.
 */
std::string GetDriverPathCpp(const std::string& moduleDir) {
    namespace fs = std::filesystem;
    fs::path basePath(moduleDir);

    std::string platform;
    std::string arch;

// Detect architecture
#if defined(__aarch64__) || defined(_M_ARM64)
    arch = "arm64";
#elif defined(__x86_64__) || defined(_M_X64) || defined(_M_AMD64)
    arch = "x86_64";  // maps to "x64" on Windows
#else
    throw std::runtime_error("Unsupported architecture");
#endif

// Detect platform and set path
#ifdef __linux__
    if (fs::exists("/etc/alpine-release")) {
        platform = "alpine";
    } else if (fs::exists("/etc/redhat-release") || fs::exists("/etc/centos-release")) {
        platform = "rhel";
    } else if (fs::exists("/etc/SuSE-release") || fs::exists("/etc/SUSE-brand")) {
        platform = "suse";
    } else {
        platform = "debian_ubuntu";  // Default to debian_ubuntu for other distros
    }

    fs::path driverPath =
        basePath / "libs" / "linux" / platform / arch / "lib" / "libmsodbcsql-18.5.so.1.1";
    return driverPath.string();

#elif defined(__APPLE__)
    platform = "macos";
    fs::path driverPath = basePath / "libs" / platform / arch / "lib" / "libmsodbcsql.18.dylib";
    return driverPath.string();

#elif defined(_WIN32)
    platform = "windows";
    // Normalize x86_64 to x64 for Windows naming
    if (arch == "x86_64")
        arch = "x64";
    fs::path driverPath = basePath / "libs" / platform / arch / "msodbcsql18.dll";
    return driverPath.string();

#else
    throw std::runtime_error("Unsupported platform");
#endif
}

DriverHandle LoadDriverOrThrowException() {
    namespace fs = std::filesystem;

    std::string moduleDir = GetModuleDirectory();
    LOG("LoadDriverOrThrowException: Module directory resolved to '%s'", moduleDir.c_str());

    std::string archStr = ARCHITECTURE;
    LOG("LoadDriverOrThrowException: Architecture detected as '%s'", archStr.c_str());

    // Use only C++ function for driver path resolution
    // Not using Python function since it causes circular import issues on
    // Alpine Linux and other platforms with strict module loading rules.
    std::string driverPathStr = GetDriverPathCpp(moduleDir);

    fs::path driverPath(driverPathStr);

    LOG("LoadDriverOrThrowException: ODBC driver path determined - path='%s'",
        driverPath.string().c_str());

#ifdef _WIN32
    // On Windows, optionally load mssql-auth.dll if it exists
    std::string archDir = (archStr == "win64" || archStr == "amd64" || archStr == "x64") ? "x64"
                          : (archStr == "arm64")                                         ? "arm64"
                                                                                         : "x86";

    fs::path dllDir = fs::path(moduleDir) / "libs" / "windows" / archDir;
    fs::path authDllPath = dllDir / "mssql-auth.dll";
    if (fs::exists(authDllPath)) {
        HMODULE hAuth = LoadLibraryW(
            std::wstring(authDllPath.native().begin(), authDllPath.native().end()).c_str());
        if (hAuth) {
            LOG("LoadDriverOrThrowException: mssql-auth.dll loaded "
                "successfully from '%s'",
                authDllPath.string().c_str());
        } else {
            LOG("LoadDriverOrThrowException: Failed to load mssql-auth.dll "
                "from '%s' - %s",
                authDllPath.string().c_str(), GetLastErrorMessage().c_str());
            ThrowStdException("Failed to load mssql-auth.dll. Please ensure it "
                              "is present in the expected directory.");
        }
    } else {
        LOG("LoadDriverOrThrowException: mssql-auth.dll not found at '%s' - "
            "Entra ID authentication will not be available",
            authDllPath.string().c_str());
        ThrowStdException("mssql-auth.dll not found. If you are using Entra "
                          "ID, please ensure it is present.");
    }
#endif

    if (!fs::exists(driverPath)) {
        ThrowStdException("ODBC driver not found at: " + driverPath.string());
    }

    DriverHandle handle = LoadDriverLibrary(driverPath.string());
    if (!handle) {
        LOG("LoadDriverOrThrowException: Failed to load ODBC driver - "
            "path='%s', error='%s'",
            driverPath.string().c_str(), GetLastErrorMessage().c_str());
        ThrowStdException("Failed to load the driver. Please read the documentation "
                          "(https://github.com/microsoft/mssql-python#installation) to "
                          "install the required dependencies.");
    }
    LOG("LoadDriverOrThrowException: ODBC driver library loaded successfully "
        "from '%s'",
        driverPath.string().c_str());

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
    SQLGetTypeInfo_ptr = GetFunctionPointer<SQLGetTypeInfoFunc>(handle, "SQLGetTypeInfoW");
    SQLProcedures_ptr = GetFunctionPointer<SQLProceduresFunc>(handle, "SQLProceduresW");
    SQLForeignKeys_ptr = GetFunctionPointer<SQLForeignKeysFunc>(handle, "SQLForeignKeysW");
    SQLPrimaryKeys_ptr = GetFunctionPointer<SQLPrimaryKeysFunc>(handle, "SQLPrimaryKeysW");
    SQLSpecialColumns_ptr = GetFunctionPointer<SQLSpecialColumnsFunc>(handle, "SQLSpecialColumnsW");
    SQLStatistics_ptr = GetFunctionPointer<SQLStatisticsFunc>(handle, "SQLStatisticsW");
    SQLColumns_ptr = GetFunctionPointer<SQLColumnsFunc>(handle, "SQLColumnsW");
    SQLGetInfo_ptr = GetFunctionPointer<SQLGetInfoFunc>(handle, "SQLGetInfoW");

    SQLEndTran_ptr = GetFunctionPointer<SQLEndTranFunc>(handle, "SQLEndTran");
    SQLDisconnect_ptr = GetFunctionPointer<SQLDisconnectFunc>(handle, "SQLDisconnect");
    SQLFreeHandle_ptr = GetFunctionPointer<SQLFreeHandleFunc>(handle, "SQLFreeHandle");
    SQLFreeStmt_ptr = GetFunctionPointer<SQLFreeStmtFunc>(handle, "SQLFreeStmt");

    SQLGetDiagRec_ptr = GetFunctionPointer<SQLGetDiagRecFunc>(handle, "SQLGetDiagRecW");

    SQLParamData_ptr = GetFunctionPointer<SQLParamDataFunc>(handle, "SQLParamData");
    SQLPutData_ptr = GetFunctionPointer<SQLPutDataFunc>(handle, "SQLPutData");
    SQLTables_ptr = GetFunctionPointer<SQLTablesFunc>(handle, "SQLTablesW");

    SQLDescribeParam_ptr = GetFunctionPointer<SQLDescribeParamFunc>(handle, "SQLDescribeParam");

    bool success = SQLAllocHandle_ptr && SQLSetEnvAttr_ptr && SQLSetConnectAttr_ptr &&
                   SQLSetStmtAttr_ptr && SQLGetConnectAttr_ptr && SQLDriverConnect_ptr &&
                   SQLExecDirect_ptr && SQLPrepare_ptr && SQLBindParameter_ptr && SQLExecute_ptr &&
                   SQLRowCount_ptr && SQLGetStmtAttr_ptr && SQLSetDescField_ptr && SQLFetch_ptr &&
                   SQLFetchScroll_ptr && SQLGetData_ptr && SQLNumResultCols_ptr && SQLBindCol_ptr &&
                   SQLDescribeCol_ptr && SQLMoreResults_ptr && SQLColAttribute_ptr &&
                   SQLEndTran_ptr && SQLDisconnect_ptr && SQLFreeHandle_ptr && SQLFreeStmt_ptr &&
                   SQLGetDiagRec_ptr && SQLGetInfo_ptr && SQLParamData_ptr && SQLPutData_ptr &&
                   SQLTables_ptr && SQLDescribeParam_ptr && SQLGetTypeInfo_ptr &&
                   SQLProcedures_ptr && SQLForeignKeys_ptr && SQLPrimaryKeys_ptr &&
                   SQLSpecialColumns_ptr && SQLStatistics_ptr && SQLColumns_ptr;

    if (!success) {
        ThrowStdException("Failed to load required function pointers from driver.");
    }
    LOG("LoadDriverOrThrowException: All %d ODBC function pointers loaded "
        "successfully",
        44);
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
SqlHandle::SqlHandle(SQLSMALLINT type, SQLHANDLE rawHandle) : _type(type), _handle(rawHandle) {}

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

/*
 * IMPORTANT: Never log in destructors - it causes segfaults.
 * During program exit, C++ destructors may run AFTER Python shuts down.
 * LOG() tries to acquire Python GIL and call Python functions, which crashes
 * if Python is already gone. Keep destructors simple - just free resources.
 * If you need destruction logs, use explicit close() methods instead.
 */
void SqlHandle::free() {
    if (_handle && SQLFreeHandle_ptr) {
        // Check if Python is shutting down using centralized helper function
        bool pythonShuttingDown = is_python_finalizing();

        // CRITICAL FIX: During Python shutdown, don't free STMT handles as
        // their parent DBC may already be freed This prevents segfault when
        // handles are freed in wrong order during interpreter shutdown Type 3 =
        // SQL_HANDLE_STMT, Type 2 = SQL_HANDLE_DBC, Type 1 = SQL_HANDLE_ENV
        if (pythonShuttingDown && _type == 3) {
            _handle = nullptr;  // Mark as freed to prevent double-free attempts
            return;
        }

        // Always clean up ODBC resources, regardless of Python state
        SQLFreeHandle_ptr(_type, _handle);
        _handle = nullptr;

        // Only log if Python is not shutting down (to avoid segfault)
        if (!pythonShuttingDown) {
            // Don't log during destruction - even in normal cases it can be
            // problematic If logging is needed, use explicit close() methods
            // instead
        }
    }
}

SQLRETURN SQLGetTypeInfo_Wrapper(SqlHandlePtr StatementHandle, SQLSMALLINT DataType) {
    if (!SQLGetTypeInfo_ptr) {
        ThrowStdException("SQLGetTypeInfo function not loaded");
    }

    return SQLGetTypeInfo_ptr(StatementHandle->get(), DataType);
}

SQLRETURN SQLProcedures_wrap(SqlHandlePtr StatementHandle, const py::object& catalogObj,
                             const py::object& schemaObj, const py::object& procedureObj) {
    if (!SQLProcedures_ptr) {
        ThrowStdException("SQLProcedures function not loaded");
    }

    std::wstring catalog =
        py::isinstance<py::none>(catalogObj) ? L"" : catalogObj.cast<std::wstring>();
    std::wstring schema =
        py::isinstance<py::none>(schemaObj) ? L"" : schemaObj.cast<std::wstring>();
    std::wstring procedure =
        py::isinstance<py::none>(procedureObj) ? L"" : procedureObj.cast<std::wstring>();

#if defined(__APPLE__) || defined(__linux__)
    // Unix implementation
    std::vector<SQLWCHAR> catalogBuf = WStringToSQLWCHAR(catalog);
    std::vector<SQLWCHAR> schemaBuf = WStringToSQLWCHAR(schema);
    std::vector<SQLWCHAR> procedureBuf = WStringToSQLWCHAR(procedure);

    return SQLProcedures_ptr(
        StatementHandle->get(), catalog.empty() ? nullptr : catalogBuf.data(),
        catalog.empty() ? 0 : SQL_NTS, schema.empty() ? nullptr : schemaBuf.data(),
        schema.empty() ? 0 : SQL_NTS, procedure.empty() ? nullptr : procedureBuf.data(),
        procedure.empty() ? 0 : SQL_NTS);
#else
    // Windows implementation
    return SQLProcedures_ptr(
        StatementHandle->get(), catalog.empty() ? nullptr : (SQLWCHAR*)catalog.c_str(),
        catalog.empty() ? 0 : SQL_NTS, schema.empty() ? nullptr : (SQLWCHAR*)schema.c_str(),
        schema.empty() ? 0 : SQL_NTS, procedure.empty() ? nullptr : (SQLWCHAR*)procedure.c_str(),
        procedure.empty() ? 0 : SQL_NTS);
#endif
}

SQLRETURN SQLForeignKeys_wrap(SqlHandlePtr StatementHandle, const py::object& pkCatalogObj,
                              const py::object& pkSchemaObj, const py::object& pkTableObj,
                              const py::object& fkCatalogObj, const py::object& fkSchemaObj,
                              const py::object& fkTableObj) {
    if (!SQLForeignKeys_ptr) {
        ThrowStdException("SQLForeignKeys function not loaded");
    }

    std::wstring pkCatalog =
        py::isinstance<py::none>(pkCatalogObj) ? L"" : pkCatalogObj.cast<std::wstring>();
    std::wstring pkSchema =
        py::isinstance<py::none>(pkSchemaObj) ? L"" : pkSchemaObj.cast<std::wstring>();
    std::wstring pkTable =
        py::isinstance<py::none>(pkTableObj) ? L"" : pkTableObj.cast<std::wstring>();
    std::wstring fkCatalog =
        py::isinstance<py::none>(fkCatalogObj) ? L"" : fkCatalogObj.cast<std::wstring>();
    std::wstring fkSchema =
        py::isinstance<py::none>(fkSchemaObj) ? L"" : fkSchemaObj.cast<std::wstring>();
    std::wstring fkTable =
        py::isinstance<py::none>(fkTableObj) ? L"" : fkTableObj.cast<std::wstring>();

#if defined(__APPLE__) || defined(__linux__)
    // Unix implementation
    std::vector<SQLWCHAR> pkCatalogBuf = WStringToSQLWCHAR(pkCatalog);
    std::vector<SQLWCHAR> pkSchemaBuf = WStringToSQLWCHAR(pkSchema);
    std::vector<SQLWCHAR> pkTableBuf = WStringToSQLWCHAR(pkTable);
    std::vector<SQLWCHAR> fkCatalogBuf = WStringToSQLWCHAR(fkCatalog);
    std::vector<SQLWCHAR> fkSchemaBuf = WStringToSQLWCHAR(fkSchema);
    std::vector<SQLWCHAR> fkTableBuf = WStringToSQLWCHAR(fkTable);

    return SQLForeignKeys_ptr(
        StatementHandle->get(), pkCatalog.empty() ? nullptr : pkCatalogBuf.data(),
        pkCatalog.empty() ? 0 : SQL_NTS, pkSchema.empty() ? nullptr : pkSchemaBuf.data(),
        pkSchema.empty() ? 0 : SQL_NTS, pkTable.empty() ? nullptr : pkTableBuf.data(),
        pkTable.empty() ? 0 : SQL_NTS, fkCatalog.empty() ? nullptr : fkCatalogBuf.data(),
        fkCatalog.empty() ? 0 : SQL_NTS, fkSchema.empty() ? nullptr : fkSchemaBuf.data(),
        fkSchema.empty() ? 0 : SQL_NTS, fkTable.empty() ? nullptr : fkTableBuf.data(),
        fkTable.empty() ? 0 : SQL_NTS);
#else
    // Windows implementation
    return SQLForeignKeys_ptr(
        StatementHandle->get(), pkCatalog.empty() ? nullptr : (SQLWCHAR*)pkCatalog.c_str(),
        pkCatalog.empty() ? 0 : SQL_NTS, pkSchema.empty() ? nullptr : (SQLWCHAR*)pkSchema.c_str(),
        pkSchema.empty() ? 0 : SQL_NTS, pkTable.empty() ? nullptr : (SQLWCHAR*)pkTable.c_str(),
        pkTable.empty() ? 0 : SQL_NTS, fkCatalog.empty() ? nullptr : (SQLWCHAR*)fkCatalog.c_str(),
        fkCatalog.empty() ? 0 : SQL_NTS, fkSchema.empty() ? nullptr : (SQLWCHAR*)fkSchema.c_str(),
        fkSchema.empty() ? 0 : SQL_NTS, fkTable.empty() ? nullptr : (SQLWCHAR*)fkTable.c_str(),
        fkTable.empty() ? 0 : SQL_NTS);
#endif
}

SQLRETURN SQLPrimaryKeys_wrap(SqlHandlePtr StatementHandle, const py::object& catalogObj,
                              const py::object& schemaObj, const std::wstring& table) {
    if (!SQLPrimaryKeys_ptr) {
        ThrowStdException("SQLPrimaryKeys function not loaded");
    }

    // Convert py::object to std::wstring, treating None as empty string
    std::wstring catalog = catalogObj.is_none() ? L"" : catalogObj.cast<std::wstring>();
    std::wstring schema = schemaObj.is_none() ? L"" : schemaObj.cast<std::wstring>();

#if defined(__APPLE__) || defined(__linux__)
    // Unix implementation
    std::vector<SQLWCHAR> catalogBuf = WStringToSQLWCHAR(catalog);
    std::vector<SQLWCHAR> schemaBuf = WStringToSQLWCHAR(schema);
    std::vector<SQLWCHAR> tableBuf = WStringToSQLWCHAR(table);

    return SQLPrimaryKeys_ptr(
        StatementHandle->get(), catalog.empty() ? nullptr : catalogBuf.data(),
        catalog.empty() ? 0 : SQL_NTS, schema.empty() ? nullptr : schemaBuf.data(),
        schema.empty() ? 0 : SQL_NTS, table.empty() ? nullptr : tableBuf.data(),
        table.empty() ? 0 : SQL_NTS);
#else
    // Windows implementation
    return SQLPrimaryKeys_ptr(
        StatementHandle->get(), catalog.empty() ? nullptr : (SQLWCHAR*)catalog.c_str(),
        catalog.empty() ? 0 : SQL_NTS, schema.empty() ? nullptr : (SQLWCHAR*)schema.c_str(),
        schema.empty() ? 0 : SQL_NTS, table.empty() ? nullptr : (SQLWCHAR*)table.c_str(),
        table.empty() ? 0 : SQL_NTS);
#endif
}

SQLRETURN SQLStatistics_wrap(SqlHandlePtr StatementHandle, const py::object& catalogObj,
                             const py::object& schemaObj, const std::wstring& table,
                             SQLUSMALLINT unique, SQLUSMALLINT reserved) {
    if (!SQLStatistics_ptr) {
        ThrowStdException("SQLStatistics function not loaded");
    }

    // Convert py::object to std::wstring, treating None as empty string
    std::wstring catalog = catalogObj.is_none() ? L"" : catalogObj.cast<std::wstring>();
    std::wstring schema = schemaObj.is_none() ? L"" : schemaObj.cast<std::wstring>();

#if defined(__APPLE__) || defined(__linux__)
    // Unix implementation
    std::vector<SQLWCHAR> catalogBuf = WStringToSQLWCHAR(catalog);
    std::vector<SQLWCHAR> schemaBuf = WStringToSQLWCHAR(schema);
    std::vector<SQLWCHAR> tableBuf = WStringToSQLWCHAR(table);

    return SQLStatistics_ptr(
        StatementHandle->get(), catalog.empty() ? nullptr : catalogBuf.data(),
        catalog.empty() ? 0 : SQL_NTS, schema.empty() ? nullptr : schemaBuf.data(),
        schema.empty() ? 0 : SQL_NTS, table.empty() ? nullptr : tableBuf.data(),
        table.empty() ? 0 : SQL_NTS, unique, reserved);
#else
    // Windows implementation
    return SQLStatistics_ptr(
        StatementHandle->get(), catalog.empty() ? nullptr : (SQLWCHAR*)catalog.c_str(),
        catalog.empty() ? 0 : SQL_NTS, schema.empty() ? nullptr : (SQLWCHAR*)schema.c_str(),
        schema.empty() ? 0 : SQL_NTS, table.empty() ? nullptr : (SQLWCHAR*)table.c_str(),
        table.empty() ? 0 : SQL_NTS, unique, reserved);
#endif
}

SQLRETURN SQLColumns_wrap(SqlHandlePtr StatementHandle, const py::object& catalogObj,
                          const py::object& schemaObj, const py::object& tableObj,
                          const py::object& columnObj) {
    if (!SQLColumns_ptr) {
        ThrowStdException("SQLColumns function not loaded");
    }

    // Convert py::object to std::wstring, treating None as empty string
    std::wstring catalogStr = catalogObj.is_none() ? L"" : catalogObj.cast<std::wstring>();
    std::wstring schemaStr = schemaObj.is_none() ? L"" : schemaObj.cast<std::wstring>();
    std::wstring tableStr = tableObj.is_none() ? L"" : tableObj.cast<std::wstring>();
    std::wstring columnStr = columnObj.is_none() ? L"" : columnObj.cast<std::wstring>();

#if defined(__APPLE__) || defined(__linux__)
    // Unix implementation
    std::vector<SQLWCHAR> catalogBuf = WStringToSQLWCHAR(catalogStr);
    std::vector<SQLWCHAR> schemaBuf = WStringToSQLWCHAR(schemaStr);
    std::vector<SQLWCHAR> tableBuf = WStringToSQLWCHAR(tableStr);
    std::vector<SQLWCHAR> columnBuf = WStringToSQLWCHAR(columnStr);

    return SQLColumns_ptr(
        StatementHandle->get(), catalogStr.empty() ? nullptr : catalogBuf.data(),
        catalogStr.empty() ? 0 : SQL_NTS, schemaStr.empty() ? nullptr : schemaBuf.data(),
        schemaStr.empty() ? 0 : SQL_NTS, tableStr.empty() ? nullptr : tableBuf.data(),
        tableStr.empty() ? 0 : SQL_NTS, columnStr.empty() ? nullptr : columnBuf.data(),
        columnStr.empty() ? 0 : SQL_NTS);
#else
    // Windows implementation
    return SQLColumns_ptr(
        StatementHandle->get(), catalogStr.empty() ? nullptr : (SQLWCHAR*)catalogStr.c_str(),
        catalogStr.empty() ? 0 : SQL_NTS,
        schemaStr.empty() ? nullptr : (SQLWCHAR*)schemaStr.c_str(), schemaStr.empty() ? 0 : SQL_NTS,
        tableStr.empty() ? nullptr : (SQLWCHAR*)tableStr.c_str(), tableStr.empty() ? 0 : SQL_NTS,
        columnStr.empty() ? nullptr : (SQLWCHAR*)columnStr.c_str(),
        columnStr.empty() ? 0 : SQL_NTS);
#endif
}

// Helper function to check for driver errors
ErrorInfo SQLCheckError_Wrap(SQLSMALLINT handleType, SqlHandlePtr handle, SQLRETURN retcode) {
    LOG("SQLCheckError: Checking ODBC errors - handleType=%d, retcode=%d", handleType, retcode);
    ErrorInfo errorInfo;
    if (retcode == SQL_INVALID_HANDLE) {
        LOG("SQLCheckError: SQL_INVALID_HANDLE detected - handle is invalid");
        errorInfo.ddbcErrorMsg = std::wstring(L"Invalid handle!");
        return errorInfo;
    }
    assert(handle != 0);
    SQLHANDLE rawHandle = handle->get();
    if (!SQL_SUCCEEDED(retcode)) {
        if (!SQLGetDiagRec_ptr) {
            LOG("SQLCheckError: SQLGetDiagRec function pointer not "
                "initialized, loading driver");
            DriverLoader::getInstance().loadDriver();  // Load the driver
        }

        SQLWCHAR sqlState[6], message[SQL_MAX_MESSAGE_LENGTH];
        SQLINTEGER nativeError;
        SQLSMALLINT messageLen;

        SQLRETURN diagReturn = SQLGetDiagRec_ptr(handleType, rawHandle, 1, sqlState, &nativeError,
                                                 message, SQL_MAX_MESSAGE_LENGTH, &messageLen);

        if (SQL_SUCCEEDED(diagReturn)) {
#if defined(_WIN32)
            // On Windows, SQLWCHAR and wchar_t are compatible
            errorInfo.sqlState = std::wstring(sqlState);
            errorInfo.ddbcErrorMsg = std::wstring(message);
#else
            // On macOS/Linux, need to convert SQLWCHAR (usually unsigned short)
            // to wchar_t
            errorInfo.sqlState = SQLWCHARToWString(sqlState);
            errorInfo.ddbcErrorMsg = SQLWCHARToWString(message, messageLen);
#endif
        }
    }
    return errorInfo;
}

py::list SQLGetAllDiagRecords(SqlHandlePtr handle) {
    LOG("SQLGetAllDiagRecords: Retrieving all diagnostic records for handle "
        "%p, handleType=%d",
        (void*)handle->get(), handle->type());
    if (!SQLGetDiagRec_ptr) {
        LOG("SQLGetAllDiagRecords: SQLGetDiagRec function pointer not "
            "initialized, loading driver");
        DriverLoader::getInstance().loadDriver();
    }

    py::list records;
    SQLHANDLE rawHandle = handle->get();
    SQLSMALLINT handleType = handle->type();

    // Iterate through all available diagnostic records
    for (SQLSMALLINT recNumber = 1;; recNumber++) {
        SQLWCHAR sqlState[6] = {0};
        SQLWCHAR message[SQL_MAX_MESSAGE_LENGTH] = {0};
        SQLINTEGER nativeError = 0;
        SQLSMALLINT messageLen = 0;

        SQLRETURN diagReturn =
            SQLGetDiagRec_ptr(handleType, rawHandle, recNumber, sqlState, &nativeError, message,
                              SQL_MAX_MESSAGE_LENGTH, &messageLen);

        if (diagReturn == SQL_NO_DATA || !SQL_SUCCEEDED(diagReturn))
            break;

#if defined(_WIN32)
        // On Windows, create a formatted UTF-8 string for state+error

        // Convert SQLWCHAR sqlState to UTF-8
        int stateSize = WideCharToMultiByte(CP_UTF8, 0, sqlState, -1, NULL, 0, NULL, NULL);
        std::vector<char> stateBuffer(stateSize);
        WideCharToMultiByte(CP_UTF8, 0, sqlState, -1, stateBuffer.data(), stateSize, NULL, NULL);

        // Format the state with error code
        std::string stateWithError =
            "[" + std::string(stateBuffer.data()) + "] (" + std::to_string(nativeError) + ")";

        // Convert wide string message to UTF-8
        int msgSize = WideCharToMultiByte(CP_UTF8, 0, message, -1, NULL, 0, NULL, NULL);
        std::vector<char> msgBuffer(msgSize);
        WideCharToMultiByte(CP_UTF8, 0, message, -1, msgBuffer.data(), msgSize, NULL, NULL);

        // Create the tuple with converted strings
        records.append(py::make_tuple(py::str(stateWithError), py::str(msgBuffer.data())));
#else
        // On Unix, use the SQLWCHARToWString utility and then convert to UTF-8
        std::string stateStr = WideToUTF8(SQLWCHARToWString(sqlState));
        std::string msgStr = WideToUTF8(SQLWCHARToWString(message, messageLen));

        // Format the state string
        std::string stateWithError = "[" + stateStr + "] (" + std::to_string(nativeError) + ")";

        // Create the tuple with converted strings
        records.append(py::make_tuple(py::str(stateWithError), py::str(msgStr)));
#endif
    }

    return records;
}

// Wrap SQLExecDirect
SQLRETURN SQLExecDirect_wrap(SqlHandlePtr StatementHandle, const std::wstring& Query) {
    std::string queryUtf8 = WideToUTF8(Query);
    LOG("SQLExecDirect: Executing query directly - statement_handle=%p, "
        "query_length=%zu chars",
        (void*)StatementHandle->get(), Query.length());
    if (!SQLExecDirect_ptr) {
        LOG("SQLExecDirect: Function pointer not initialized, loading driver");
        DriverLoader::getInstance().loadDriver();  // Load the driver
    }

    // Configure forward-only cursor
    if (SQLSetStmtAttr_ptr && StatementHandle && StatementHandle->get()) {
        SQLSetStmtAttr_ptr(StatementHandle->get(), SQL_ATTR_CURSOR_TYPE,
                           (SQLPOINTER)SQL_CURSOR_FORWARD_ONLY, 0);
        SQLSetStmtAttr_ptr(StatementHandle->get(), SQL_ATTR_CONCURRENCY,
                           (SQLPOINTER)SQL_CONCUR_READ_ONLY, 0);
    }

    SQLWCHAR* queryPtr;
#if defined(__APPLE__) || defined(__linux__)
    std::vector<SQLWCHAR> queryBuffer = WStringToSQLWCHAR(Query);
    queryPtr = queryBuffer.data();
#else
    queryPtr = const_cast<SQLWCHAR*>(Query.c_str());
#endif
    SQLRETURN ret = SQLExecDirect_ptr(StatementHandle->get(), queryPtr, SQL_NTS);
    if (!SQL_SUCCEEDED(ret)) {
        LOG("SQLExecDirect: Query execution failed - SQLRETURN=%d", ret);
    }
    return ret;
}

// Wrapper for SQLTables
SQLRETURN SQLTables_wrap(SqlHandlePtr StatementHandle, const std::wstring& catalog,
                         const std::wstring& schema, const std::wstring& table,
                         const std::wstring& tableType) {
    if (!SQLTables_ptr) {
        LOG("SQLTables: Function pointer not initialized, loading driver");
        DriverLoader::getInstance().loadDriver();
    }

    SQLWCHAR* catalogPtr = nullptr;
    SQLWCHAR* schemaPtr = nullptr;
    SQLWCHAR* tablePtr = nullptr;
    SQLWCHAR* tableTypePtr = nullptr;
    SQLSMALLINT catalogLen = 0;
    SQLSMALLINT schemaLen = 0;
    SQLSMALLINT tableLen = 0;
    SQLSMALLINT tableTypeLen = 0;

    std::vector<SQLWCHAR> catalogBuffer;
    std::vector<SQLWCHAR> schemaBuffer;
    std::vector<SQLWCHAR> tableBuffer;
    std::vector<SQLWCHAR> tableTypeBuffer;

#if defined(__APPLE__) || defined(__linux__)
    // On Unix platforms, convert wstring to SQLWCHAR array
    if (!catalog.empty()) {
        catalogBuffer = WStringToSQLWCHAR(catalog);
        catalogPtr = catalogBuffer.data();
        catalogLen = SQL_NTS;
    }
    if (!schema.empty()) {
        schemaBuffer = WStringToSQLWCHAR(schema);
        schemaPtr = schemaBuffer.data();
        schemaLen = SQL_NTS;
    }
    if (!table.empty()) {
        tableBuffer = WStringToSQLWCHAR(table);
        tablePtr = tableBuffer.data();
        tableLen = SQL_NTS;
    }
    if (!tableType.empty()) {
        tableTypeBuffer = WStringToSQLWCHAR(tableType);
        tableTypePtr = tableTypeBuffer.data();
        tableTypeLen = SQL_NTS;
    }
#else
    // On Windows, direct assignment works
    if (!catalog.empty()) {
        catalogPtr = const_cast<SQLWCHAR*>(catalog.c_str());
        catalogLen = SQL_NTS;
    }
    if (!schema.empty()) {
        schemaPtr = const_cast<SQLWCHAR*>(schema.c_str());
        schemaLen = SQL_NTS;
    }
    if (!table.empty()) {
        tablePtr = const_cast<SQLWCHAR*>(table.c_str());
        tableLen = SQL_NTS;
    }
    if (!tableType.empty()) {
        tableTypePtr = const_cast<SQLWCHAR*>(tableType.c_str());
        tableTypeLen = SQL_NTS;
    }
#endif

    SQLRETURN ret = SQLTables_ptr(StatementHandle->get(), catalogPtr, catalogLen, schemaPtr,
                                  schemaLen, tablePtr, tableLen, tableTypePtr, tableTypeLen);

    LOG("SQLTables: Catalog metadata query %s - SQLRETURN=%d",
        SQL_SUCCEEDED(ret) ? "succeeded" : "failed", ret);

    return ret;
}

// Executes the provided query. If the query is parametrized, it prepares the
// statement and binds the parameters. Otherwise, it executes the query
// directly. 'usePrepare' parameter can be used to disable the prepare step for
// queries that might already be prepared in a previous call.
SQLRETURN SQLExecute_wrap(const SqlHandlePtr statementHandle,
                          const std::wstring& query /* TODO: Use SQLTCHAR? */,
                          const py::list& params, std::vector<ParamInfo>& paramInfos,
                          py::list& isStmtPrepared, const bool usePrepare = true) {
    LOG("SQLExecute: Executing %s query - statement_handle=%p, "
        "param_count=%zu, query_length=%zu chars",
        (params.size() > 0 ? "parameterized" : "direct"), (void*)statementHandle->get(),
        params.size(), query.length());
    if (!SQLPrepare_ptr) {
        LOG("SQLExecute: Function pointer not initialized, loading driver");
        DriverLoader::getInstance().loadDriver();  // Load the driver
    }
    assert(SQLPrepare_ptr && SQLBindParameter_ptr && SQLExecute_ptr && SQLExecDirect_ptr);

    if (params.size() != paramInfos.size()) {
        // TODO: This should be a special internal exception, that python wont
        // relay to users as is
        ThrowStdException("Number of parameters and paramInfos do not match");
    }

    RETCODE rc;
    SQLHANDLE hStmt = statementHandle->get();
    if (!statementHandle || !statementHandle->get()) {
        LOG("SQLExecute: Statement handle is null or invalid");
    }

    // Configure forward-only cursor
    if (SQLSetStmtAttr_ptr && hStmt) {
        SQLSetStmtAttr_ptr(hStmt, SQL_ATTR_CURSOR_TYPE, (SQLPOINTER)SQL_CURSOR_FORWARD_ONLY, 0);
        SQLSetStmtAttr_ptr(hStmt, SQL_ATTR_CONCURRENCY, (SQLPOINTER)SQL_CONCUR_READ_ONLY, 0);
    }

    SQLWCHAR* queryPtr;
#if defined(__APPLE__) || defined(__linux__)
    std::vector<SQLWCHAR> queryBuffer = WStringToSQLWCHAR(query);
    queryPtr = queryBuffer.data();
#else
    queryPtr = const_cast<SQLWCHAR*>(query.c_str());
#endif
    if (params.size() == 0) {
        // Execute statement directly if the statement is not parametrized. This
        // is the fastest way to submit a SQL statement for one-time execution
        // according to DDBC documentation -
        // https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlexecdirect-function?view=sql-server-ver16
        rc = SQLExecDirect_ptr(hStmt, queryPtr, SQL_NTS);
        if (!SQL_SUCCEEDED(rc) && rc != SQL_NO_DATA) {
            LOG("SQLExecute: Direct execution failed (non-parameterized query) "
                "- SQLRETURN=%d",
                rc);
        }
        return rc;
    } else {
        // isStmtPrepared is a list instead of a bool coz bools in Python are
        // immutable. Hence, we can't pass around bools by reference & modify
        // them. Therefore, isStmtPrepared must be a list with exactly one bool
        // element
        assert(isStmtPrepared.size() == 1);
        if (usePrepare) {
            rc = SQLPrepare_ptr(hStmt, queryPtr, SQL_NTS);
            if (!SQL_SUCCEEDED(rc)) {
                LOG("SQLExecute: SQLPrepare failed - SQLRETURN=%d, "
                    "statement_handle=%p",
                    rc, (void*)hStmt);
                return rc;
            }
            isStmtPrepared[0] = py::cast(true);
        } else {
            // Make sure the statement has been prepared earlier if we're not
            // preparing now
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
        if (rc == SQL_NEED_DATA) {
            LOG("SQLExecute: SQL_NEED_DATA received - Starting DAE "
                "(Data-At-Execution) loop for large parameter streaming");
            SQLPOINTER paramToken = nullptr;
            while ((rc = SQLParamData_ptr(hStmt, &paramToken)) == SQL_NEED_DATA) {
                // Finding the paramInfo that matches the returned token
                const ParamInfo* matchedInfo = nullptr;
                for (auto& info : paramInfos) {
                    if (reinterpret_cast<SQLPOINTER>(const_cast<ParamInfo*>(&info)) == paramToken) {
                        matchedInfo = &info;
                        break;
                    }
                }
                if (!matchedInfo) {
                    ThrowStdException("Unrecognized paramToken returned by SQLParamData");
                }
                const py::object& pyObj = matchedInfo->dataPtr;
                if (pyObj.is_none()) {
                    SQLPutData_ptr(hStmt, nullptr, 0);
                    continue;
                }
                if (py::isinstance<py::str>(pyObj)) {
                    if (matchedInfo->paramCType == SQL_C_WCHAR) {
                        std::wstring wstr = pyObj.cast<std::wstring>();
                        const SQLWCHAR* dataPtr = nullptr;
                        size_t totalChars = 0;
#if defined(__APPLE__) || defined(__linux__)
                        std::vector<SQLWCHAR> sqlwStr = WStringToSQLWCHAR(wstr);
                        totalChars = sqlwStr.size() - 1;
                        dataPtr = sqlwStr.data();
#else
                        dataPtr = wstr.c_str();
                        totalChars = wstr.size();
#endif
                        size_t offset = 0;
                        size_t chunkChars = DAE_CHUNK_SIZE / sizeof(SQLWCHAR);
                        while (offset < totalChars) {
                            size_t len = std::min(chunkChars, totalChars - offset);
                            size_t lenBytes = len * sizeof(SQLWCHAR);
                            if (lenBytes >
                                static_cast<size_t>(std::numeric_limits<SQLLEN>::max())) {
                                ThrowStdException("Chunk size exceeds maximum "
                                                  "allowed by SQLLEN");
                            }
                            rc = SQLPutData_ptr(hStmt, (SQLPOINTER)(dataPtr + offset),
                                                static_cast<SQLLEN>(lenBytes));
                            if (!SQL_SUCCEEDED(rc)) {
                                LOG("SQLExecute: SQLPutData failed for "
                                    "SQL_C_WCHAR chunk - offset=%zu",
                                    offset, totalChars, lenBytes, rc);
                                return rc;
                            }
                            offset += len;
                        }
                    } else if (matchedInfo->paramCType == SQL_C_CHAR) {
                        std::string s = pyObj.cast<std::string>();
                        size_t totalBytes = s.size();
                        const char* dataPtr = s.data();
                        size_t offset = 0;
                        size_t chunkBytes = DAE_CHUNK_SIZE;
                        while (offset < totalBytes) {
                            size_t len = std::min(chunkBytes, totalBytes - offset);

                            rc = SQLPutData_ptr(hStmt, (SQLPOINTER)(dataPtr + offset),
                                                static_cast<SQLLEN>(len));
                            if (!SQL_SUCCEEDED(rc)) {
                                LOG("SQLExecute: SQLPutData failed for "
                                    "SQL_C_CHAR chunk - offset=%zu",
                                    offset, totalBytes, len, rc);
                                return rc;
                            }
                            offset += len;
                        }
                    } else {
                        ThrowStdException("Unsupported C type for str in DAE");
                    }
                } else if (py::isinstance<py::bytes>(pyObj) ||
                           py::isinstance<py::bytearray>(pyObj)) {
                    py::bytes b = pyObj.cast<py::bytes>();
                    std::string s = b;
                    const char* dataPtr = s.data();
                    size_t totalBytes = s.size();
                    const size_t chunkSize = DAE_CHUNK_SIZE;
                    for (size_t offset = 0; offset < totalBytes; offset += chunkSize) {
                        size_t len = std::min(chunkSize, totalBytes - offset);
                        rc = SQLPutData_ptr(hStmt, (SQLPOINTER)(dataPtr + offset),
                                            static_cast<SQLLEN>(len));
                        if (!SQL_SUCCEEDED(rc)) {
                            LOG("SQLExecute: SQLPutData failed for "
                                "binary/bytes chunk - offset=%zu",
                                offset, totalBytes, len, rc);
                            return rc;
                        }
                    }
                } else {
                    ThrowStdException("DAE only supported for str or bytes");
                }
            }
            if (!SQL_SUCCEEDED(rc)) {
                LOG("SQLExecute: SQLParamData final call %s - SQLRETURN=%d",
                    (rc == SQL_NO_DATA ? "completed with no data" : "failed"), rc);
                return rc;
            }
            LOG("SQLExecute: DAE streaming completed successfully, SQLExecute "
                "resumed");
        }
        if (!SQL_SUCCEEDED(rc) && rc != SQL_NO_DATA) {
            LOG("SQLExecute: Statement execution failed - SQLRETURN=%d, "
                "statement_handle=%p",
                rc, (void*)hStmt);
            return rc;
        }

        // Unbind the bound buffers for all parameters coz the buffers' memory
        // will be freed when this function exits (parambuffers goes out of
        // scope)
        rc = SQLFreeStmt_ptr(hStmt, SQL_RESET_PARAMS);
        return rc;
    }
}

SQLRETURN BindParameterArray(SQLHANDLE hStmt, const py::list& columnwise_params,
                             const std::vector<ParamInfo>& paramInfos, size_t paramSetSize,
                             std::vector<std::shared_ptr<void>>& paramBuffers) {
    LOG("BindParameterArray: Starting column-wise array binding - "
        "param_count=%zu, param_set_size=%zu",
        columnwise_params.size(), paramSetSize);

    std::vector<std::shared_ptr<void>> tempBuffers;

    try {
        for (int paramIndex = 0; paramIndex < columnwise_params.size(); ++paramIndex) {
            const py::list& columnValues = columnwise_params[paramIndex].cast<py::list>();
            const ParamInfo& info = paramInfos[paramIndex];
            LOG("BindParameterArray: Processing param_index=%d, C_type=%d, "
                "SQL_type=%d, column_size=%zu, decimal_digits=%d",
                paramIndex, info.paramCType, info.paramSQLType, info.columnSize,
                info.decimalDigits);
            if (columnValues.size() != paramSetSize) {
                LOG("BindParameterArray: Size mismatch - param_index=%d, "
                    "expected=%zu, actual=%zu",
                    paramIndex, paramSetSize, columnValues.size());
                ThrowStdException("Column " + std::to_string(paramIndex) + " has mismatched size.");
            }
            void* dataPtr = nullptr;
            SQLLEN* strLenOrIndArray = nullptr;
            SQLLEN bufferLength = 0;
            switch (info.paramCType) {
                case SQL_C_LONG: {
                    LOG("BindParameterArray: Binding SQL_C_LONG array - "
                        "param_index=%d, count=%zu",
                        paramIndex, paramSetSize);
                    int* dataArray = AllocateParamBufferArray<int>(tempBuffers, paramSetSize);
                    for (size_t i = 0; i < paramSetSize; ++i) {
                        if (columnValues[i].is_none()) {
                            if (!strLenOrIndArray)
                                strLenOrIndArray =
                                    AllocateParamBufferArray<SQLLEN>(tempBuffers, paramSetSize);
                            dataArray[i] = 0;
                            strLenOrIndArray[i] = SQL_NULL_DATA;
                        } else {
                            dataArray[i] = columnValues[i].cast<int>();
                            if (strLenOrIndArray)
                                strLenOrIndArray[i] = 0;
                        }
                    }
                    LOG("BindParameterArray: SQL_C_LONG bound - param_index=%d", paramIndex);
                    dataPtr = dataArray;
                    break;
                }
                case SQL_C_DOUBLE: {
                    LOG("BindParameterArray: Binding SQL_C_DOUBLE array - "
                        "param_index=%d, count=%zu",
                        paramIndex, paramSetSize);
                    double* dataArray = AllocateParamBufferArray<double>(tempBuffers, paramSetSize);
                    for (size_t i = 0; i < paramSetSize; ++i) {
                        if (columnValues[i].is_none()) {
                            if (!strLenOrIndArray)
                                strLenOrIndArray =
                                    AllocateParamBufferArray<SQLLEN>(tempBuffers, paramSetSize);
                            dataArray[i] = 0;
                            strLenOrIndArray[i] = SQL_NULL_DATA;
                        } else {
                            dataArray[i] = columnValues[i].cast<double>();
                            if (strLenOrIndArray)
                                strLenOrIndArray[i] = 0;
                        }
                    }
                    LOG("BindParameterArray: SQL_C_DOUBLE bound - "
                        "param_index=%d",
                        paramIndex);
                    dataPtr = dataArray;
                    break;
                }
                case SQL_C_WCHAR: {
                    LOG("BindParameterArray: Binding SQL_C_WCHAR array - "
                        "param_index=%d, count=%zu, column_size=%zu",
                        paramIndex, paramSetSize, info.columnSize);
                    SQLWCHAR* wcharArray = AllocateParamBufferArray<SQLWCHAR>(
                        tempBuffers, paramSetSize * (info.columnSize + 1));
                    strLenOrIndArray = AllocateParamBufferArray<SQLLEN>(tempBuffers, paramSetSize);
                    for (size_t i = 0; i < paramSetSize; ++i) {
                        if (columnValues[i].is_none()) {
                            strLenOrIndArray[i] = SQL_NULL_DATA;
                            std::memset(wcharArray + i * (info.columnSize + 1), 0,
                                        (info.columnSize + 1) * sizeof(SQLWCHAR));
                        } else {
                            std::wstring wstr = columnValues[i].cast<std::wstring>();
#if defined(__APPLE__) || defined(__linux__)
                            // Convert to UTF-16 first, then check the actual
                            // UTF-16 length
                            auto utf16Buf = WStringToSQLWCHAR(wstr);
                            size_t utf16_len = utf16Buf.size() > 0 ? utf16Buf.size() - 1 : 0;
                            // Check UTF-16 length (excluding null terminator)
                            // against column size
                            if (utf16Buf.size() > 0 && utf16_len > info.columnSize) {
                                std::string offending = WideToUTF8(wstr);
                                LOG("BindParameterArray: SQL_C_WCHAR string "
                                    "too long - param_index=%d, row=%zu, "
                                    "utf16_length=%zu, max=%zu",
                                    paramIndex, i, utf16_len, info.columnSize);
                                ThrowStdException("Input string UTF-16 length exceeds "
                                                  "allowed column size at parameter index " +
                                                  std::to_string(paramIndex) + ". UTF-16 length: " +
                                                  std::to_string(utf16_len) + ", Column size: " +
                                                  std::to_string(info.columnSize));
                            }
                            // If we reach here, the UTF-16 string fits - copy
                            // it completely
                            std::memcpy(wcharArray + i * (info.columnSize + 1), utf16Buf.data(),
                                        utf16Buf.size() * sizeof(SQLWCHAR));
#else
                            // On Windows, wchar_t is already UTF-16, so the
                            // original check is sufficient
                            if (wstr.length() > info.columnSize) {
                                std::string offending = WideToUTF8(wstr);
                                ThrowStdException("Input string exceeds allowed column size "
                                                  "at parameter index " +
                                                  std::to_string(paramIndex));
                            }
                            std::memcpy(wcharArray + i * (info.columnSize + 1), wstr.c_str(),
                                        (wstr.length() + 1) * sizeof(SQLWCHAR));
#endif
                            strLenOrIndArray[i] = SQL_NTS;
                        }
                    }
                    LOG("BindParameterArray: SQL_C_WCHAR bound - "
                        "param_index=%d",
                        paramIndex);
                    dataPtr = wcharArray;
                    bufferLength = (info.columnSize + 1) * sizeof(SQLWCHAR);
                    break;
                }
                case SQL_C_TINYINT:
                case SQL_C_UTINYINT: {
                    LOG("BindParameterArray: Binding SQL_C_TINYINT/UTINYINT "
                        "array - param_index=%d, count=%zu",
                        paramIndex, paramSetSize);
                    unsigned char* dataArray =
                        AllocateParamBufferArray<unsigned char>(tempBuffers, paramSetSize);
                    for (size_t i = 0; i < paramSetSize; ++i) {
                        if (columnValues[i].is_none()) {
                            if (!strLenOrIndArray)
                                strLenOrIndArray =
                                    AllocateParamBufferArray<SQLLEN>(tempBuffers, paramSetSize);
                            dataArray[i] = 0;
                            strLenOrIndArray[i] = SQL_NULL_DATA;
                        } else {
                            int intVal = columnValues[i].cast<int>();
                            if (intVal < 0 || intVal > 255) {
                                LOG("BindParameterArray: TINYINT value out of "
                                    "range - param_index=%d, row=%zu, value=%d",
                                    paramIndex, i, intVal);
                                ThrowStdException("UTINYINT value out of range at rowIndex " +
                                                  std::to_string(i));
                            }
                            dataArray[i] = static_cast<unsigned char>(intVal);
                            if (strLenOrIndArray)
                                strLenOrIndArray[i] = 0;
                        }
                    }
                    LOG("BindParameterArray: SQL_C_TINYINT bound - "
                        "param_index=%d",
                        paramIndex);
                    dataPtr = dataArray;
                    bufferLength = sizeof(unsigned char);
                    break;
                }
                case SQL_C_SHORT: {
                    LOG("BindParameterArray: Binding SQL_C_SHORT array - "
                        "param_index=%d, count=%zu",
                        paramIndex, paramSetSize);
                    short* dataArray = AllocateParamBufferArray<short>(tempBuffers, paramSetSize);
                    for (size_t i = 0; i < paramSetSize; ++i) {
                        if (columnValues[i].is_none()) {
                            if (!strLenOrIndArray)
                                strLenOrIndArray =
                                    AllocateParamBufferArray<SQLLEN>(tempBuffers, paramSetSize);
                            dataArray[i] = 0;
                            strLenOrIndArray[i] = SQL_NULL_DATA;
                        } else {
                            int intVal = columnValues[i].cast<int>();
                            if (intVal < std::numeric_limits<short>::min() ||
                                intVal > std::numeric_limits<short>::max()) {
                                LOG("BindParameterArray: SHORT value out of "
                                    "range - param_index=%d, row=%zu, value=%d",
                                    paramIndex, i, intVal);
                                ThrowStdException("SHORT value out of range at rowIndex " +
                                                  std::to_string(i));
                            }
                            dataArray[i] = static_cast<short>(intVal);
                            if (strLenOrIndArray)
                                strLenOrIndArray[i] = 0;
                        }
                    }
                    LOG("BindParameterArray: SQL_C_SHORT bound - "
                        "param_index=%d",
                        paramIndex);
                    dataPtr = dataArray;
                    bufferLength = sizeof(short);
                    break;
                }
                case SQL_C_CHAR:
                case SQL_C_BINARY: {
                    LOG("BindParameterArray: Binding SQL_C_CHAR/BINARY array - "
                        "param_index=%d, count=%zu, column_size=%zu",
                        paramIndex, paramSetSize, info.columnSize);
                    char* charArray = AllocateParamBufferArray<char>(
                        tempBuffers, paramSetSize * (info.columnSize + 1));
                    strLenOrIndArray = AllocateParamBufferArray<SQLLEN>(tempBuffers, paramSetSize);
                    for (size_t i = 0; i < paramSetSize; ++i) {
                        if (columnValues[i].is_none()) {
                            strLenOrIndArray[i] = SQL_NULL_DATA;
                            std::memset(charArray + i * (info.columnSize + 1), 0,
                                        info.columnSize + 1);
                        } else {
                            std::string str = columnValues[i].cast<std::string>();
                            if (str.size() > info.columnSize) {
                                LOG("BindParameterArray: String/binary too "
                                    "long - param_index=%d, row=%zu, size=%zu, "
                                    "max=%zu",
                                    paramIndex, i, str.size(), info.columnSize);
                                ThrowStdException("Input exceeds column size at index " +
                                                  std::to_string(i));
                            }
                            std::memcpy(charArray + i * (info.columnSize + 1), str.c_str(),
                                        str.size());
                            strLenOrIndArray[i] = static_cast<SQLLEN>(str.size());
                        }
                    }
                    LOG("BindParameterArray: SQL_C_CHAR/BINARY bound - "
                        "param_index=%d",
                        paramIndex);
                    dataPtr = charArray;
                    bufferLength = info.columnSize + 1;
                    break;
                }
                case SQL_C_BIT: {
                    LOG("BindParameterArray: Binding SQL_C_BIT array - "
                        "param_index=%d, count=%zu",
                        paramIndex, paramSetSize);
                    char* boolArray = AllocateParamBufferArray<char>(tempBuffers, paramSetSize);
                    strLenOrIndArray = AllocateParamBufferArray<SQLLEN>(tempBuffers, paramSetSize);
                    for (size_t i = 0; i < paramSetSize; ++i) {
                        if (columnValues[i].is_none()) {
                            boolArray[i] = 0;
                            strLenOrIndArray[i] = SQL_NULL_DATA;
                        } else {
                            bool val = columnValues[i].cast<bool>();
                            boolArray[i] = val ? 1 : 0;
                            strLenOrIndArray[i] = 0;
                        }
                    }
                    LOG("BindParameterArray: SQL_C_BIT bound - param_index=%d", paramIndex);
                    dataPtr = boolArray;
                    bufferLength = sizeof(char);
                    break;
                }
                case SQL_C_STINYINT:
                case SQL_C_USHORT: {
                    LOG("BindParameterArray: Binding SQL_C_USHORT/STINYINT "
                        "array - param_index=%d, count=%zu",
                        paramIndex, paramSetSize);
                    unsigned short* dataArray =
                        AllocateParamBufferArray<unsigned short>(tempBuffers, paramSetSize);
                    strLenOrIndArray = AllocateParamBufferArray<SQLLEN>(tempBuffers, paramSetSize);
                    for (size_t i = 0; i < paramSetSize; ++i) {
                        if (columnValues[i].is_none()) {
                            strLenOrIndArray[i] = SQL_NULL_DATA;
                            dataArray[i] = 0;
                        } else {
                            dataArray[i] = columnValues[i].cast<unsigned short>();
                            strLenOrIndArray[i] = 0;
                        }
                    }
                    LOG("BindParameterArray: SQL_C_USHORT bound - "
                        "param_index=%d",
                        paramIndex);
                    dataPtr = dataArray;
                    bufferLength = sizeof(unsigned short);
                    break;
                }
                case SQL_C_SBIGINT:
                case SQL_C_SLONG:
                case SQL_C_UBIGINT:
                case SQL_C_ULONG: {
                    LOG("BindParameterArray: Binding SQL_C_BIGINT array - "
                        "param_index=%d, count=%zu",
                        paramIndex, paramSetSize);
                    int64_t* dataArray =
                        AllocateParamBufferArray<int64_t>(tempBuffers, paramSetSize);
                    strLenOrIndArray = AllocateParamBufferArray<SQLLEN>(tempBuffers, paramSetSize);
                    for (size_t i = 0; i < paramSetSize; ++i) {
                        if (columnValues[i].is_none()) {
                            strLenOrIndArray[i] = SQL_NULL_DATA;
                            dataArray[i] = 0;
                        } else {
                            dataArray[i] = columnValues[i].cast<int64_t>();
                            strLenOrIndArray[i] = 0;
                        }
                    }
                    LOG("BindParameterArray: SQL_C_BIGINT bound - "
                        "param_index=%d",
                        paramIndex);
                    dataPtr = dataArray;
                    bufferLength = sizeof(int64_t);
                    break;
                }
                case SQL_C_FLOAT: {
                    LOG("BindParameterArray: Binding SQL_C_FLOAT array - "
                        "param_index=%d, count=%zu",
                        paramIndex, paramSetSize);
                    float* dataArray = AllocateParamBufferArray<float>(tempBuffers, paramSetSize);
                    strLenOrIndArray = AllocateParamBufferArray<SQLLEN>(tempBuffers, paramSetSize);
                    for (size_t i = 0; i < paramSetSize; ++i) {
                        if (columnValues[i].is_none()) {
                            strLenOrIndArray[i] = SQL_NULL_DATA;
                            dataArray[i] = 0.0f;
                        } else {
                            dataArray[i] = columnValues[i].cast<float>();
                            strLenOrIndArray[i] = 0;
                        }
                    }
                    LOG("BindParameterArray: SQL_C_FLOAT bound - "
                        "param_index=%d",
                        paramIndex);
                    dataPtr = dataArray;
                    bufferLength = sizeof(float);
                    break;
                }
                case SQL_C_TYPE_DATE: {
                    LOG("BindParameterArray: Binding SQL_C_TYPE_DATE array - "
                        "param_index=%d, count=%zu",
                        paramIndex, paramSetSize);
                    SQL_DATE_STRUCT* dateArray =
                        AllocateParamBufferArray<SQL_DATE_STRUCT>(tempBuffers, paramSetSize);
                    strLenOrIndArray = AllocateParamBufferArray<SQLLEN>(tempBuffers, paramSetSize);
                    for (size_t i = 0; i < paramSetSize; ++i) {
                        if (columnValues[i].is_none()) {
                            strLenOrIndArray[i] = SQL_NULL_DATA;
                            std::memset(&dateArray[i], 0, sizeof(SQL_DATE_STRUCT));
                        } else {
                            py::object dateObj = columnValues[i];
                            dateArray[i].year = dateObj.attr("year").cast<SQLSMALLINT>();
                            dateArray[i].month = dateObj.attr("month").cast<SQLUSMALLINT>();
                            dateArray[i].day = dateObj.attr("day").cast<SQLUSMALLINT>();
                            strLenOrIndArray[i] = 0;
                        }
                    }
                    LOG("BindParameterArray: SQL_C_TYPE_DATE bound - "
                        "param_index=%d",
                        paramIndex);
                    dataPtr = dateArray;
                    bufferLength = sizeof(SQL_DATE_STRUCT);
                    break;
                }
                case SQL_C_TYPE_TIME: {
                    LOG("BindParameterArray: Binding SQL_C_TYPE_TIME array - "
                        "param_index=%d, count=%zu",
                        paramIndex, paramSetSize);
                    SQL_TIME_STRUCT* timeArray =
                        AllocateParamBufferArray<SQL_TIME_STRUCT>(tempBuffers, paramSetSize);
                    strLenOrIndArray = AllocateParamBufferArray<SQLLEN>(tempBuffers, paramSetSize);
                    for (size_t i = 0; i < paramSetSize; ++i) {
                        if (columnValues[i].is_none()) {
                            strLenOrIndArray[i] = SQL_NULL_DATA;
                            std::memset(&timeArray[i], 0, sizeof(SQL_TIME_STRUCT));
                        } else {
                            py::object timeObj = columnValues[i];
                            timeArray[i].hour = timeObj.attr("hour").cast<SQLUSMALLINT>();
                            timeArray[i].minute = timeObj.attr("minute").cast<SQLUSMALLINT>();
                            timeArray[i].second = timeObj.attr("second").cast<SQLUSMALLINT>();
                            strLenOrIndArray[i] = 0;
                        }
                    }
                    LOG("BindParameterArray: SQL_C_TYPE_TIME bound - "
                        "param_index=%d",
                        paramIndex);
                    dataPtr = timeArray;
                    bufferLength = sizeof(SQL_TIME_STRUCT);
                    break;
                }
                case SQL_C_TYPE_TIMESTAMP: {
                    LOG("BindParameterArray: Binding SQL_C_TYPE_TIMESTAMP "
                        "array - param_index=%d, count=%zu",
                        paramIndex, paramSetSize);
                    SQL_TIMESTAMP_STRUCT* tsArray =
                        AllocateParamBufferArray<SQL_TIMESTAMP_STRUCT>(tempBuffers, paramSetSize);
                    strLenOrIndArray = AllocateParamBufferArray<SQLLEN>(tempBuffers, paramSetSize);
                    for (size_t i = 0; i < paramSetSize; ++i) {
                        if (columnValues[i].is_none()) {
                            strLenOrIndArray[i] = SQL_NULL_DATA;
                            std::memset(&tsArray[i], 0, sizeof(SQL_TIMESTAMP_STRUCT));
                        } else {
                            py::object dtObj = columnValues[i];
                            tsArray[i].year = dtObj.attr("year").cast<SQLSMALLINT>();
                            tsArray[i].month = dtObj.attr("month").cast<SQLUSMALLINT>();
                            tsArray[i].day = dtObj.attr("day").cast<SQLUSMALLINT>();
                            tsArray[i].hour = dtObj.attr("hour").cast<SQLUSMALLINT>();
                            tsArray[i].minute = dtObj.attr("minute").cast<SQLUSMALLINT>();
                            tsArray[i].second = dtObj.attr("second").cast<SQLUSMALLINT>();
                            tsArray[i].fraction = static_cast<SQLUINTEGER>(
                                dtObj.attr("microsecond").cast<int>() * 1000);  // µs to ns
                            strLenOrIndArray[i] = 0;
                        }
                    }
                    LOG("BindParameterArray: SQL_C_TYPE_TIMESTAMP bound - "
                        "param_index=%d",
                        paramIndex);
                    dataPtr = tsArray;
                    bufferLength = sizeof(SQL_TIMESTAMP_STRUCT);
                    break;
                }
                case SQL_C_SS_TIMESTAMPOFFSET: {
                    LOG("BindParameterArray: Binding SQL_C_SS_TIMESTAMPOFFSET "
                        "array - param_index=%d, count=%zu",
                        paramIndex, paramSetSize);
                    DateTimeOffset* dtoArray =
                        AllocateParamBufferArray<DateTimeOffset>(tempBuffers, paramSetSize);
                    strLenOrIndArray = AllocateParamBufferArray<SQLLEN>(tempBuffers, paramSetSize);

                    py::object datetimeType = PythonObjectCache::get_datetime_class();

                    for (size_t i = 0; i < paramSetSize; ++i) {
                        const py::handle& param = columnValues[i];

                        if (param.is_none()) {
                            std::memset(&dtoArray[i], 0, sizeof(DateTimeOffset));
                            strLenOrIndArray[i] = SQL_NULL_DATA;
                        } else {
                            if (!py::isinstance(param, datetimeType)) {
                                ThrowStdException(
                                    MakeParamMismatchErrorStr(info.paramCType, paramIndex));
                            }

                            py::object tzinfo = param.attr("tzinfo");
                            if (tzinfo.is_none()) {
                                ThrowStdException("Datetime object must have tzinfo for "
                                                  "SQL_C_SS_TIMESTAMPOFFSET at paramIndex " +
                                                  std::to_string(paramIndex));
                            }

                            // Populate the C++ struct directly from the Python
                            // datetime object.
                            dtoArray[i].year =
                                static_cast<SQLSMALLINT>(param.attr("year").cast<int>());
                            dtoArray[i].month =
                                static_cast<SQLUSMALLINT>(param.attr("month").cast<int>());
                            dtoArray[i].day =
                                static_cast<SQLUSMALLINT>(param.attr("day").cast<int>());
                            dtoArray[i].hour =
                                static_cast<SQLUSMALLINT>(param.attr("hour").cast<int>());
                            dtoArray[i].minute =
                                static_cast<SQLUSMALLINT>(param.attr("minute").cast<int>());
                            dtoArray[i].second =
                                static_cast<SQLUSMALLINT>(param.attr("second").cast<int>());
                            // SQL server supports in ns, but python datetime
                            // supports in µs
                            dtoArray[i].fraction = static_cast<SQLUINTEGER>(
                                param.attr("microsecond").cast<int>() * 1000);

                            // Compute and preserve the original UTC offset.
                            py::object utcoffset = tzinfo.attr("utcoffset")(param);
                            int total_seconds =
                                static_cast<int>(utcoffset.attr("total_seconds")().cast<double>());
                            std::div_t div_result = std::div(total_seconds, 3600);
                            dtoArray[i].timezone_hour = static_cast<SQLSMALLINT>(div_result.quot);
                            dtoArray[i].timezone_minute =
                                static_cast<SQLSMALLINT>(div(div_result.rem, 60).quot);

                            strLenOrIndArray[i] = sizeof(DateTimeOffset);
                        }
                    }
                    LOG("BindParameterArray: SQL_C_SS_TIMESTAMPOFFSET bound - "
                        "param_index=%d",
                        paramIndex);
                    dataPtr = dtoArray;
                    bufferLength = sizeof(DateTimeOffset);
                    break;
                }
                case SQL_C_NUMERIC: {
                    LOG("BindParameterArray: Binding SQL_C_NUMERIC array - "
                        "param_index=%d, count=%zu",
                        paramIndex, paramSetSize);
                    SQL_NUMERIC_STRUCT* numericArray =
                        AllocateParamBufferArray<SQL_NUMERIC_STRUCT>(tempBuffers, paramSetSize);
                    strLenOrIndArray = AllocateParamBufferArray<SQLLEN>(tempBuffers, paramSetSize);
                    for (size_t i = 0; i < paramSetSize; ++i) {
                        const py::handle& element = columnValues[i];
                        if (element.is_none()) {
                            strLenOrIndArray[i] = SQL_NULL_DATA;
                            std::memset(&numericArray[i], 0, sizeof(SQL_NUMERIC_STRUCT));
                            continue;
                        }
                        if (!py::isinstance<NumericData>(element)) {
                            LOG("BindParameterArray: NUMERIC type mismatch - "
                                "param_index=%d, row=%zu",
                                paramIndex, i);
                            throw std::runtime_error(
                                MakeParamMismatchErrorStr(info.paramCType, paramIndex));
                        }
                        NumericData decimalParam = element.cast<NumericData>();
                        LOG("BindParameterArray: NUMERIC value - "
                            "param_index=%d, row=%zu, precision=%d, scale=%d, "
                            "sign=%d",
                            paramIndex, i, decimalParam.precision, decimalParam.scale,
                            decimalParam.sign);
                        SQL_NUMERIC_STRUCT& target = numericArray[i];
                        std::memset(&target, 0, sizeof(SQL_NUMERIC_STRUCT));
                        target.precision = decimalParam.precision;
                        target.scale = decimalParam.scale;
                        target.sign = decimalParam.sign;
                        size_t copyLen = std::min(decimalParam.val.size(), sizeof(target.val));
                        if (copyLen > 0) {
                            std::memcpy(target.val, decimalParam.val.data(), copyLen);
                        }
                        strLenOrIndArray[i] = sizeof(SQL_NUMERIC_STRUCT);
                    }
                    LOG("BindParameterArray: SQL_C_NUMERIC bound - "
                        "param_index=%d",
                        paramIndex);
                    dataPtr = numericArray;
                    bufferLength = sizeof(SQL_NUMERIC_STRUCT);
                    break;
                }
                case SQL_C_GUID: {
                    LOG("BindParameterArray: Binding SQL_C_GUID array - "
                        "param_index=%d, count=%zu",
                        paramIndex, paramSetSize);
                    SQLGUID* guidArray =
                        AllocateParamBufferArray<SQLGUID>(tempBuffers, paramSetSize);
                    strLenOrIndArray = AllocateParamBufferArray<SQLLEN>(tempBuffers, paramSetSize);

                    // Get cached UUID class from module-level helper
                    // This avoids static object destruction issues during
                    // Python finalization
                    py::object uuid_class = PythonObjectCache::get_uuid_class();
                    // Get cached UUID class

                    for (size_t i = 0; i < paramSetSize; ++i) {
                        const py::handle& element = columnValues[i];
                        std::array<unsigned char, 16> uuid_bytes;
                        if (element.is_none()) {
                            std::memset(&guidArray[i], 0, sizeof(SQLGUID));
                            strLenOrIndArray[i] = SQL_NULL_DATA;
                            continue;
                        } else if (py::isinstance<py::bytes>(element)) {
                            py::bytes b = element.cast<py::bytes>();
                            if (PyBytes_GET_SIZE(b.ptr()) != 16) {
                                LOG("BindParameterArray: GUID bytes wrong "
                                    "length - param_index=%d, row=%zu, "
                                    "length=%d",
                                    paramIndex, i, PyBytes_GET_SIZE(b.ptr()));
                                ThrowStdException("UUID binary data must be "
                                                  "exactly 16 bytes long.");
                            }
                            std::memcpy(uuid_bytes.data(), PyBytes_AS_STRING(b.ptr()), 16);
                        } else if (py::isinstance(element, uuid_class)) {
                            py::bytes b = element.attr("bytes_le").cast<py::bytes>();
                            std::memcpy(uuid_bytes.data(), PyBytes_AS_STRING(b.ptr()), 16);
                        } else {
                            LOG("BindParameterArray: GUID type mismatch - "
                                "param_index=%d, row=%zu",
                                paramIndex, i);
                            ThrowStdException(
                                MakeParamMismatchErrorStr(info.paramCType, paramIndex));
                        }
                        guidArray[i].Data1 = (static_cast<uint32_t>(uuid_bytes[3]) << 24) |
                                             (static_cast<uint32_t>(uuid_bytes[2]) << 16) |
                                             (static_cast<uint32_t>(uuid_bytes[1]) << 8) |
                                             (static_cast<uint32_t>(uuid_bytes[0]));
                        guidArray[i].Data2 = (static_cast<uint16_t>(uuid_bytes[5]) << 8) |
                                             (static_cast<uint16_t>(uuid_bytes[4]));
                        guidArray[i].Data3 = (static_cast<uint16_t>(uuid_bytes[7]) << 8) |
                                             (static_cast<uint16_t>(uuid_bytes[6]));
                        std::memcpy(guidArray[i].Data4, uuid_bytes.data() + 8, 8);
                        strLenOrIndArray[i] = sizeof(SQLGUID);
                    }
                    LOG("BindParameterArray: SQL_C_GUID bound - "
                        "param_index=%d, null=%zu, bytes=%zu, uuid_obj=%zu",
                        paramIndex);
                    dataPtr = guidArray;
                    bufferLength = sizeof(SQLGUID);
                    break;
                }
                default: {
                    LOG("BindParameterArray: Unsupported C type - "
                        "param_index=%d, C_type=%d",
                        paramIndex, info.paramCType);
                    ThrowStdException("BindParameterArray: Unsupported C type: " +
                                      std::to_string(info.paramCType));
                }
            }
            LOG("BindParameterArray: Calling SQLBindParameter - "
                "param_index=%d, buffer_length=%lld",
                paramIndex, static_cast<long long>(bufferLength));
            RETCODE rc =
                SQLBindParameter_ptr(hStmt, static_cast<SQLUSMALLINT>(paramIndex + 1),
                                     static_cast<SQLUSMALLINT>(info.inputOutputType),
                                     static_cast<SQLSMALLINT>(info.paramCType),
                                     static_cast<SQLSMALLINT>(info.paramSQLType), info.columnSize,
                                     info.decimalDigits, dataPtr, bufferLength, strLenOrIndArray);
            if (!SQL_SUCCEEDED(rc)) {
                LOG("BindParameterArray: SQLBindParameter failed - "
                    "param_index=%d, SQLRETURN=%d",
                    paramIndex, rc);
                return rc;
            }
        }
    } catch (...) {
        LOG("BindParameterArray: Exception during binding, cleaning up "
            "buffers");
        throw;
    }
    paramBuffers.insert(paramBuffers.end(), tempBuffers.begin(), tempBuffers.end());
    LOG("BindParameterArray: Successfully bound all parameters - "
        "total_params=%zu, buffer_count=%zu",
        columnwise_params.size(), paramBuffers.size());
    return SQL_SUCCESS;
}

SQLRETURN SQLExecuteMany_wrap(const SqlHandlePtr statementHandle, const std::wstring& query,
                              const py::list& columnwise_params,
                              const std::vector<ParamInfo>& paramInfos, size_t paramSetSize) {
    LOG("SQLExecuteMany: Starting batch execution - param_count=%zu, "
        "param_set_size=%zu",
        columnwise_params.size(), paramSetSize);
    SQLHANDLE hStmt = statementHandle->get();
    SQLWCHAR* queryPtr;

#if defined(__APPLE__) || defined(__linux__)
    std::vector<SQLWCHAR> queryBuffer = WStringToSQLWCHAR(query);
    queryPtr = queryBuffer.data();
    LOG("SQLExecuteMany: Query converted to SQLWCHAR - buffer_size=%zu", queryBuffer.size());
#else
    queryPtr = const_cast<SQLWCHAR*>(query.c_str());
    LOG("SQLExecuteMany: Using wide string query directly");
#endif
    RETCODE rc = SQLPrepare_ptr(hStmt, queryPtr, SQL_NTS);
    if (!SQL_SUCCEEDED(rc)) {
        LOG("SQLExecuteMany: SQLPrepare failed - rc=%d", rc);
        return rc;
    }
    LOG("SQLExecuteMany: Query prepared successfully");

    bool hasDAE = false;
    for (const auto& p : paramInfos) {
        if (p.isDAE) {
            hasDAE = true;
            break;
        }
    }
    LOG("SQLExecuteMany: Parameter analysis - hasDAE=%s", hasDAE ? "true" : "false");
    if (!hasDAE) {
        LOG("SQLExecuteMany: Using array binding (non-DAE) - calling "
            "BindParameterArray");
        std::vector<std::shared_ptr<void>> paramBuffers;
        rc = BindParameterArray(hStmt, columnwise_params, paramInfos, paramSetSize, paramBuffers);
        if (!SQL_SUCCEEDED(rc)) {
            LOG("SQLExecuteMany: BindParameterArray failed - rc=%d", rc);
            return rc;
        }

        rc = SQLSetStmtAttr_ptr(hStmt, SQL_ATTR_PARAMSET_SIZE, (SQLPOINTER)paramSetSize, 0);
        if (!SQL_SUCCEEDED(rc)) {
            LOG("SQLExecuteMany: SQLSetStmtAttr(PARAMSET_SIZE) failed - rc=%d", rc);
            return rc;
        }
        LOG("SQLExecuteMany: PARAMSET_SIZE set to %zu", paramSetSize);

        rc = SQLExecute_ptr(hStmt);
        LOG("SQLExecuteMany: SQLExecute completed - rc=%d", rc);
        return rc;
    } else {
        LOG("SQLExecuteMany: Using DAE (data-at-execution) - row_count=%zu",
            columnwise_params.size());
        size_t rowCount = columnwise_params.size();
        for (size_t rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
            LOG("SQLExecuteMany: Processing DAE row %zu of %zu", rowIndex + 1, rowCount);
            py::list rowParams = columnwise_params[rowIndex];

            std::vector<std::shared_ptr<void>> paramBuffers;
            rc = BindParameters(hStmt, rowParams, const_cast<std::vector<ParamInfo>&>(paramInfos),
                                paramBuffers);
            if (!SQL_SUCCEEDED(rc)) {
                LOG("SQLExecuteMany: BindParameters failed for row %zu - rc=%d", rowIndex, rc);
                return rc;
            }
            LOG("SQLExecuteMany: Parameters bound for row %zu", rowIndex);

            rc = SQLExecute_ptr(hStmt);
            LOG("SQLExecuteMany: SQLExecute for row %zu - initial_rc=%d", rowIndex, rc);
            size_t dae_chunk_count = 0;
            while (rc == SQL_NEED_DATA) {
                SQLPOINTER token;
                rc = SQLParamData_ptr(hStmt, &token);
                LOG("SQLExecuteMany: SQLParamData called - chunk=%zu, rc=%d, "
                    "token=%p",
                    dae_chunk_count, rc, token);
                if (!SQL_SUCCEEDED(rc) && rc != SQL_NEED_DATA) {
                    LOG("SQLExecuteMany: SQLParamData failed - chunk=%zu, "
                        "rc=%d",
                        dae_chunk_count, rc);
                    return rc;
                }

                py::object* py_obj_ptr = reinterpret_cast<py::object*>(token);
                if (!py_obj_ptr) {
                    LOG("SQLExecuteMany: NULL token pointer in DAE - chunk=%zu", dae_chunk_count);
                    return SQL_ERROR;
                }

                if (py::isinstance<py::str>(*py_obj_ptr)) {
                    std::string data = py_obj_ptr->cast<std::string>();
                    SQLLEN data_len = static_cast<SQLLEN>(data.size());
                    LOG("SQLExecuteMany: Sending string DAE data - chunk=%zu, "
                        "length=%lld",
                        dae_chunk_count, static_cast<long long>(data_len));
                    rc = SQLPutData_ptr(hStmt, (SQLPOINTER)data.c_str(), data_len);
                    if (!SQL_SUCCEEDED(rc) && rc != SQL_NEED_DATA) {
                        LOG("SQLExecuteMany: SQLPutData(string) failed - "
                            "chunk=%zu, rc=%d",
                            dae_chunk_count, rc);
                    }
                } else if (py::isinstance<py::bytes>(*py_obj_ptr) ||
                           py::isinstance<py::bytearray>(*py_obj_ptr)) {
                    std::string data = py_obj_ptr->cast<std::string>();
                    SQLLEN data_len = static_cast<SQLLEN>(data.size());
                    LOG("SQLExecuteMany: Sending bytes/bytearray DAE data - "
                        "chunk=%zu, length=%lld",
                        dae_chunk_count, static_cast<long long>(data_len));
                    rc = SQLPutData_ptr(hStmt, (SQLPOINTER)data.c_str(), data_len);
                    if (!SQL_SUCCEEDED(rc) && rc != SQL_NEED_DATA) {
                        LOG("SQLExecuteMany: SQLPutData(bytes) failed - "
                            "chunk=%zu, rc=%d",
                            dae_chunk_count, rc);
                    }
                } else {
                    LOG("SQLExecuteMany: Unsupported DAE data type - chunk=%zu", dae_chunk_count);
                    return SQL_ERROR;
                }
                dae_chunk_count++;
            }
            LOG("SQLExecuteMany: DAE completed for row %zu - total_chunks=%zu, "
                "final_rc=%d",
                rowIndex, dae_chunk_count, rc);

            if (!SQL_SUCCEEDED(rc)) {
                LOG("SQLExecuteMany: DAE row %zu failed - rc=%d", rowIndex, rc);
                return rc;
            }
        }
        LOG("SQLExecuteMany: All DAE rows processed successfully - "
            "total_rows=%zu",
            rowCount);
        return SQL_SUCCESS;
    }
}

// Wrap SQLNumResultCols
SQLSMALLINT SQLNumResultCols_wrap(SqlHandlePtr statementHandle) {
    LOG("SQLNumResultCols: Getting number of columns in result set for "
        "statement_handle=%p",
        (void*)statementHandle->get());
    if (!SQLNumResultCols_ptr) {
        LOG("SQLNumResultCols: Function pointer not initialized, loading "
            "driver");
        DriverLoader::getInstance().loadDriver();  // Load the driver
    }

    SQLSMALLINT columnCount;
    // TODO: Handle the return code
    SQLNumResultCols_ptr(statementHandle->get(), &columnCount);
    return columnCount;
}

// Wrap SQLDescribeCol
SQLRETURN SQLDescribeCol_wrap(SqlHandlePtr StatementHandle, py::list& ColumnMetadata) {
    LOG("SQLDescribeCol: Getting column descriptions for statement_handle=%p",
        (void*)StatementHandle->get());
    if (!SQLDescribeCol_ptr) {
        LOG("SQLDescribeCol: Function pointer not initialized, loading driver");
        DriverLoader::getInstance().loadDriver();  // Load the driver
    }

    SQLSMALLINT ColumnCount;
    SQLRETURN retcode = SQLNumResultCols_ptr(StatementHandle->get(), &ColumnCount);
    if (!SQL_SUCCEEDED(retcode)) {
        LOG("SQLDescribeCol: Failed to get number of columns - SQLRETURN=%d", retcode);
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
#if defined(__APPLE__) || defined(__linux__)
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

SQLRETURN SQLSpecialColumns_wrap(SqlHandlePtr StatementHandle, SQLSMALLINT identifierType,
                                 const py::object& catalogObj, const py::object& schemaObj,
                                 const std::wstring& table, SQLSMALLINT scope,
                                 SQLSMALLINT nullable) {
    if (!SQLSpecialColumns_ptr) {
        ThrowStdException("SQLSpecialColumns function not loaded");
    }

    // Convert py::object to std::wstring, treating None as empty string
    std::wstring catalog = catalogObj.is_none() ? L"" : catalogObj.cast<std::wstring>();
    std::wstring schema = schemaObj.is_none() ? L"" : schemaObj.cast<std::wstring>();

#if defined(__APPLE__) || defined(__linux__)
    // Unix implementation
    std::vector<SQLWCHAR> catalogBuf = WStringToSQLWCHAR(catalog);
    std::vector<SQLWCHAR> schemaBuf = WStringToSQLWCHAR(schema);
    std::vector<SQLWCHAR> tableBuf = WStringToSQLWCHAR(table);

    return SQLSpecialColumns_ptr(
        StatementHandle->get(), identifierType, catalog.empty() ? nullptr : catalogBuf.data(),
        catalog.empty() ? 0 : SQL_NTS, schema.empty() ? nullptr : schemaBuf.data(),
        schema.empty() ? 0 : SQL_NTS, table.empty() ? nullptr : tableBuf.data(),
        table.empty() ? 0 : SQL_NTS, scope, nullable);
#else
    // Windows implementation
    return SQLSpecialColumns_ptr(
        StatementHandle->get(), identifierType,
        catalog.empty() ? nullptr : (SQLWCHAR*)catalog.c_str(), catalog.empty() ? 0 : SQL_NTS,
        schema.empty() ? nullptr : (SQLWCHAR*)schema.c_str(), schema.empty() ? 0 : SQL_NTS,
        table.empty() ? nullptr : (SQLWCHAR*)table.c_str(), table.empty() ? 0 : SQL_NTS, scope,
        nullable);
#endif
}

// Wrap SQLFetch to retrieve rows
SQLRETURN SQLFetch_wrap(SqlHandlePtr StatementHandle) {
    LOG("SQLFetch: Fetching next row for statement_handle=%p", (void*)StatementHandle->get());
    if (!SQLFetch_ptr) {
        LOG("SQLFetch: Function pointer not initialized, loading driver");
        DriverLoader::getInstance().loadDriver();  // Load the driver
    }

    return SQLFetch_ptr(StatementHandle->get());
}

// Non-static so it can be called from inline functions in header
py::object FetchLobColumnData(SQLHSTMT hStmt, SQLUSMALLINT colIndex, SQLSMALLINT cType,
                              bool isWideChar, bool isBinary) {
    std::vector<char> buffer;
    SQLRETURN ret = SQL_SUCCESS_WITH_INFO;
    int loopCount = 0;

    while (true) {
        ++loopCount;
        std::vector<char> chunk(DAE_CHUNK_SIZE, 0);
        SQLLEN actualRead = 0;
        ret = SQLGetData_ptr(hStmt, colIndex, cType, chunk.data(), DAE_CHUNK_SIZE, &actualRead);

        if (ret == SQL_ERROR || !SQL_SUCCEEDED(ret) && ret != SQL_SUCCESS_WITH_INFO) {
            std::ostringstream oss;
            oss << "Error fetching LOB for column " << colIndex << ", cType=" << cType
                << ", loop=" << loopCount << ", SQLGetData return=" << ret;
            LOG("FetchLobColumnData: %s", oss.str().c_str());
            ThrowStdException(oss.str());
        }
        if (actualRead == SQL_NULL_DATA) {
            LOG("FetchLobColumnData: Column %d is NULL at loop %d", colIndex, loopCount);
            return py::none();
        }

        size_t bytesRead = 0;
        if (actualRead >= 0) {
            bytesRead = static_cast<size_t>(actualRead);
            if (bytesRead > DAE_CHUNK_SIZE) {
                bytesRead = DAE_CHUNK_SIZE;
            }
        } else {
            // fallback: use full buffer size if actualRead is unknown
            bytesRead = DAE_CHUNK_SIZE;
        }

        // For character data, trim trailing null terminators
        if (!isBinary && bytesRead > 0) {
            if (!isWideChar) {
                // Narrow characters
                while (bytesRead > 0 && chunk[bytesRead - 1] == '\0') {
                    --bytesRead;
                }
                if (bytesRead < DAE_CHUNK_SIZE) {
                    LOG("FetchLobColumnData: Trimmed null terminator from "
                        "narrow char data - loop=%d",
                        loopCount);
                }
            } else {
                // Wide characters
                size_t wcharSize = sizeof(SQLWCHAR);
                if (bytesRead >= wcharSize && (bytesRead % wcharSize == 0)) {
                    size_t wcharCount = bytesRead / wcharSize;
                    std::vector<SQLWCHAR> alignedBuf(wcharCount);
                    std::memcpy(alignedBuf.data(), chunk.data(), bytesRead);
                    while (wcharCount > 0 && alignedBuf[wcharCount - 1] == 0) {
                        --wcharCount;
                        bytesRead -= wcharSize;
                    }
                    if (bytesRead < DAE_CHUNK_SIZE) {
                        LOG("FetchLobColumnData: Trimmed null terminator from "
                            "wide char data - loop=%d",
                            loopCount);
                    }
                }
            }
        }
        if (bytesRead > 0) {
            buffer.insert(buffer.end(), chunk.begin(), chunk.begin() + bytesRead);
            LOG("FetchLobColumnData: Appended %zu bytes at loop %d", bytesRead, loopCount);
        }
        if (ret == SQL_SUCCESS) {
            LOG("FetchLobColumnData: SQL_SUCCESS - no more data at loop %d", loopCount);
            break;
        }
    }
    LOG("FetchLobColumnData: Total bytes collected=%zu for column %d", buffer.size(), colIndex);

    if (buffer.empty()) {
        if (isBinary) {
            return py::bytes("");
        }
        return py::str("");
    }
    if (isWideChar) {
#if defined(_WIN32)
        size_t wcharCount = buffer.size() / sizeof(wchar_t);
        std::vector<wchar_t> alignedBuf(wcharCount);
        std::memcpy(alignedBuf.data(), buffer.data(), buffer.size());
        std::wstring wstr(alignedBuf.data(), wcharCount);
        std::string utf8str = WideToUTF8(wstr);
        return py::str(utf8str);
#else
        // Linux/macOS handling
        size_t wcharCount = buffer.size() / sizeof(SQLWCHAR);
        std::vector<SQLWCHAR> alignedBuf(wcharCount);
        std::memcpy(alignedBuf.data(), buffer.data(), buffer.size());
        std::wstring wstr = SQLWCHARToWString(alignedBuf.data(), wcharCount);
        std::string utf8str = WideToUTF8(wstr);
        return py::str(utf8str);
#endif
    }
    if (isBinary) {
        LOG("FetchLobColumnData: Returning binary data - %zu bytes for column "
            "%d",
            buffer.size(), colIndex);
        return py::bytes(buffer.data(), buffer.size());
    }
    std::string str(buffer.data(), buffer.size());
    LOG("FetchLobColumnData: Returning narrow string - length=%zu for column "
        "%d",
        str.length(), colIndex);
    return py::str(str);
}

// Helper function to retrieve column data
SQLRETURN SQLGetData_wrap(SqlHandlePtr StatementHandle, SQLUSMALLINT colCount, py::list& row) {
    LOG("SQLGetData: Getting data from %d columns for statement_handle=%p", colCount,
        (void*)StatementHandle->get());
    if (!SQLGetData_ptr) {
        LOG("SQLGetData: Function pointer not initialized, loading driver");
        DriverLoader::getInstance().loadDriver();  // Load the driver
    }

    SQLRETURN ret = SQL_SUCCESS;
    SQLHSTMT hStmt = StatementHandle->get();

    // Cache decimal separator to avoid repeated system calls
    std::string decimalSeparator = GetDecimalSeparator();

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
            LOG("SQLGetData: Error retrieving metadata for column %d - "
                "SQLDescribeCol SQLRETURN=%d",
                i, ret);
            row.append(py::none());
            continue;
        }

        switch (dataType) {
            case SQL_CHAR:
            case SQL_VARCHAR:
            case SQL_LONGVARCHAR: {
                if (columnSize == SQL_NO_TOTAL || columnSize == 0 ||
                    columnSize > SQL_MAX_LOB_SIZE) {
                    LOG("SQLGetData: Streaming LOB for column %d (SQL_C_CHAR) "
                        "- columnSize=%lu",
                        i, (unsigned long)columnSize);
                    row.append(FetchLobColumnData(hStmt, i, SQL_C_CHAR, false, false));
                } else {
                    uint64_t fetchBufferSize = columnSize + 1 /* null-termination */;
                    std::vector<SQLCHAR> dataBuffer(fetchBufferSize);
                    SQLLEN dataLen;
                    ret = SQLGetData_ptr(hStmt, i, SQL_C_CHAR, dataBuffer.data(), dataBuffer.size(),
                                         &dataLen);
                    if (SQL_SUCCEEDED(ret)) {
                        // columnSize is in chars, dataLen is in bytes
                        if (dataLen > 0) {
                            uint64_t numCharsInData = dataLen / sizeof(SQLCHAR);
                            if (numCharsInData < dataBuffer.size()) {
                                // SQLGetData will null-terminate the data
#if defined(__APPLE__) || defined(__linux__)
                                std::string fullStr(reinterpret_cast<char*>(dataBuffer.data()));
                                row.append(fullStr);
#else
                                row.append(std::string(reinterpret_cast<char*>(dataBuffer.data())));
#endif
                            } else {
                                // Buffer too small, fallback to streaming
                                LOG("SQLGetData: CHAR column %d data truncated "
                                    "(buffer_size=%zu), using streaming LOB",
                                    i, dataBuffer.size());
                                row.append(FetchLobColumnData(hStmt, i, SQL_C_CHAR, false, false));
                            }
                        } else if (dataLen == SQL_NULL_DATA) {
                            LOG("SQLGetData: Column %d is NULL (CHAR)", i);
                            row.append(py::none());
                        } else if (dataLen == 0) {
                            row.append(py::str(""));
                        } else if (dataLen == SQL_NO_TOTAL) {
                            LOG("SQLGetData: Cannot determine data length "
                                "(SQL_NO_TOTAL) for column %d (SQL_CHAR), "
                                "returning NULL",
                                i);
                            row.append(py::none());
                        } else if (dataLen < 0) {
                            LOG("SQLGetData: Unexpected negative data length "
                                "for column %d - dataType=%d, dataLen=%ld",
                                i, dataType, (long)dataLen);
                            ThrowStdException("SQLGetData returned an unexpected negative "
                                              "data length");
                        }
                    } else {
                        LOG("SQLGetData: Error retrieving data for column %d "
                            "(SQL_CHAR) - SQLRETURN=%d, returning NULL",
                            i, ret);
                        row.append(py::none());
                    }
                }
                break;
            }
            case SQL_SS_XML: {
                LOG("SQLGetData: Streaming XML for column %d", i);
                row.append(FetchLobColumnData(hStmt, i, SQL_C_WCHAR, true, false));
                break;
            }
            case SQL_WCHAR:
            case SQL_WVARCHAR:
            case SQL_WLONGVARCHAR: {
                if (columnSize == SQL_NO_TOTAL || columnSize > 4000) {
                    LOG("SQLGetData: Streaming LOB for column %d (SQL_C_WCHAR) "
                        "- columnSize=%lu",
                        i, (unsigned long)columnSize);
                    row.append(FetchLobColumnData(hStmt, i, SQL_C_WCHAR, true, false));
                } else {
                    uint64_t fetchBufferSize =
                        (columnSize + 1) * sizeof(SQLWCHAR);  // +1 for null terminator
                    std::vector<SQLWCHAR> dataBuffer(columnSize + 1);
                    SQLLEN dataLen;
                    ret = SQLGetData_ptr(hStmt, i, SQL_C_WCHAR, dataBuffer.data(), fetchBufferSize,
                                         &dataLen);
                    if (SQL_SUCCEEDED(ret)) {
                        if (dataLen > 0) {
                            uint64_t numCharsInData = dataLen / sizeof(SQLWCHAR);
                            if (numCharsInData < dataBuffer.size()) {
#if defined(__APPLE__) || defined(__linux__)
                                std::wstring wstr =
                                    SQLWCHARToWString(dataBuffer.data(), numCharsInData);
                                std::string utf8str = WideToUTF8(wstr);
                                row.append(py::str(utf8str));
#else
                                std::wstring wstr(reinterpret_cast<wchar_t*>(dataBuffer.data()));
                                row.append(py::cast(wstr));
#endif
                                LOG("SQLGetData: Appended NVARCHAR string "
                                    "length=%lu for column %d",
                                    (unsigned long)numCharsInData, i);
                            } else {
                                // Buffer too small, fallback to streaming
                                LOG("SQLGetData: NVARCHAR column %d data "
                                    "truncated, using streaming LOB",
                                    i);
                                row.append(FetchLobColumnData(hStmt, i, SQL_C_WCHAR, true, false));
                            }
                        } else if (dataLen == SQL_NULL_DATA) {
                            LOG("SQLGetData: Column %d is NULL (NVARCHAR)", i);
                            row.append(py::none());
                        } else if (dataLen == 0) {
                            row.append(py::str(""));
                        } else if (dataLen == SQL_NO_TOTAL) {
                            LOG("SQLGetData: Cannot determine NVARCHAR data "
                                "length (SQL_NO_TOTAL) for column %d, "
                                "returning NULL",
                                i);
                            row.append(py::none());
                        } else if (dataLen < 0) {
                            LOG("SQLGetData: Unexpected negative data length "
                                "for column %d (NVARCHAR) - dataLen=%ld",
                                i, (long)dataLen);
                            ThrowStdException("SQLGetData returned an unexpected negative "
                                              "data length");
                        }
                    } else {
                        LOG("SQLGetData: Error retrieving data for column %d "
                            "(NVARCHAR) - SQLRETURN=%d",
                            i, ret);
                        row.append(py::none());
                    }
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
                    LOG("SQLGetData: Error retrieving SQL_SMALLINT for column "
                        "%d - SQLRETURN=%d",
                        i, ret);
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
                    LOG("SQLGetData: Error retrieving SQL_REAL for column %d - "
                        "SQLRETURN=%d",
                        i, ret);
                    row.append(py::none());
                }
                break;
            }
            case SQL_DECIMAL:
            case SQL_NUMERIC: {
                SQLCHAR numericStr[MAX_DIGITS_IN_NUMERIC] = {0};
                SQLLEN indicator = 0;

                ret = SQLGetData_ptr(hStmt, i, SQL_C_CHAR, numericStr, sizeof(numericStr),
                                     &indicator);

                if (SQL_SUCCEEDED(ret)) {
                    try {
                        // Validate 'indicator' to avoid buffer overflow and
                        // fallback to a safe null-terminated read when length
                        // is unknown or out-of-range.
                        const char* cnum = reinterpret_cast<const char*>(numericStr);
                        size_t bufSize = sizeof(numericStr);
                        size_t safeLen = 0;

                        if (indicator > 0 && indicator <= static_cast<SQLLEN>(bufSize)) {
                            // indicator appears valid and within the buffer
                            // size
                            safeLen = static_cast<size_t>(indicator);
                        } else {
                            // indicator is unknown, zero, negative, or too
                            // large; determine length by searching for a
                            // terminating null (safe bounded scan)
                            for (size_t j = 0; j < bufSize; ++j) {
                                if (cnum[j] == '\0') {
                                    safeLen = j;
                                    break;
                                }
                            }
                            // if no null found, use the full buffer size as a
                            // conservative fallback
                            if (safeLen == 0 && bufSize > 0 && cnum[0] != '\0') {
                                safeLen = bufSize;
                            }
                        }
                        // Always use standard decimal point for Python Decimal
                        // parsing The decimal separator only affects display
                        // formatting, not parsing
                        py::object decimalObj =
                            PythonObjectCache::get_decimal_class()(py::str(cnum, safeLen));
                        row.append(decimalObj);
                    } catch (const py::error_already_set& e) {
                        // If conversion fails, append None
                        LOG("SQLGetData: Error converting to decimal for "
                            "column %d - %s",
                            i, e.what());
                        row.append(py::none());
                    }
                } else {
                    LOG("SQLGetData: Error retrieving SQL_NUMERIC/DECIMAL for "
                        "column %d - SQLRETURN=%d",
                        i, ret);
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
                    LOG("SQLGetData: Error retrieving SQL_DOUBLE/FLOAT for "
                        "column %d - SQLRETURN=%d",
                        i, ret);
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
                    LOG("SQLGetData: Error retrieving SQL_BIGINT for column %d "
                        "- SQLRETURN=%d",
                        i, ret);
                    row.append(py::none());
                }
                break;
            }
            case SQL_TYPE_DATE: {
                SQL_DATE_STRUCT dateValue;
                ret =
                    SQLGetData_ptr(hStmt, i, SQL_C_TYPE_DATE, &dateValue, sizeof(dateValue), NULL);
                if (SQL_SUCCEEDED(ret)) {
                    row.append(PythonObjectCache::get_date_class()(dateValue.year, dateValue.month,
                                                                   dateValue.day));
                } else {
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
                    row.append(PythonObjectCache::get_time_class()(timeValue.hour, timeValue.minute,
                                                                   timeValue.second));
                } else {
                    LOG("SQLGetData: Error retrieving SQL_TYPE_TIME for column "
                        "%d - SQLRETURN=%d",
                        i, ret);
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
                    row.append(PythonObjectCache::get_datetime_class()(
                        timestampValue.year, timestampValue.month, timestampValue.day,
                        timestampValue.hour, timestampValue.minute, timestampValue.second,
                        timestampValue.fraction / 1000  // Convert back ns to µs
                        ));
                } else {
                    LOG("SQLGetData: Error retrieving SQL_TYPE_TIMESTAMP for "
                        "column %d - SQLRETURN=%d",
                        i, ret);
                    row.append(py::none());
                }
                break;
            }
            case SQL_SS_TIMESTAMPOFFSET: {
                DateTimeOffset dtoValue;
                SQLLEN indicator;
                ret = SQLGetData_ptr(hStmt, i, SQL_C_SS_TIMESTAMPOFFSET, &dtoValue,
                                     sizeof(dtoValue), &indicator);
                if (SQL_SUCCEEDED(ret) && indicator != SQL_NULL_DATA) {
                    LOG("SQLGetData: Retrieved DATETIMEOFFSET for column %d - "
                        "%d-%d-%d %d:%d:%d, fraction_ns=%u, tz_hour=%d, "
                        "tz_minute=%d",
                        i, dtoValue.year, dtoValue.month, dtoValue.day, dtoValue.hour,
                        dtoValue.minute, dtoValue.second, dtoValue.fraction, dtoValue.timezone_hour,
                        dtoValue.timezone_minute);

                    int totalMinutes = dtoValue.timezone_hour * 60 + dtoValue.timezone_minute;
                    // Validating offset
                    if (totalMinutes < -24 * 60 || totalMinutes > 24 * 60) {
                        std::ostringstream oss;
                        oss << "Invalid timezone offset from "
                               "SQL_SS_TIMESTAMPOFFSET_STRUCT: "
                            << totalMinutes << " minutes for column " << i;
                        ThrowStdException(oss.str());
                    }
                    // Convert fraction from ns to µs
                    int microseconds = dtoValue.fraction / 1000;
                    py::object datetime_module = py::module_::import("datetime");
                    py::object tzinfo = datetime_module.attr("timezone")(
                        datetime_module.attr("timedelta")(py::arg("minutes") = totalMinutes));
                    py::object py_dt = PythonObjectCache::get_datetime_class()(
                        dtoValue.year, dtoValue.month, dtoValue.day, dtoValue.hour, dtoValue.minute,
                        dtoValue.second, microseconds, tzinfo);
                    row.append(py_dt);
                } else {
                    LOG("SQLGetData: Error fetching DATETIMEOFFSET for column "
                        "%d - SQLRETURN=%d, indicator=%ld",
                        i, ret, (long)indicator);
                    row.append(py::none());
                }
                break;
            }
            case SQL_BINARY:
            case SQL_VARBINARY:
            case SQL_LONGVARBINARY: {
                // Use streaming for large VARBINARY (columnSize unknown or >
                // 8000)
                if (columnSize == SQL_NO_TOTAL || columnSize == 0 || columnSize > 8000) {
                    LOG("SQLGetData: Streaming LOB for column %d "
                        "(SQL_C_BINARY) - columnSize=%lu",
                        i, (unsigned long)columnSize);
                    row.append(FetchLobColumnData(hStmt, i, SQL_C_BINARY, false, true));
                } else {
                    // Small VARBINARY, fetch directly
                    std::vector<SQLCHAR> dataBuffer(columnSize);
                    SQLLEN dataLen;
                    ret = SQLGetData_ptr(hStmt, i, SQL_C_BINARY, dataBuffer.data(), columnSize,
                                         &dataLen);

                    if (SQL_SUCCEEDED(ret)) {
                        if (dataLen > 0) {
                            if (static_cast<size_t>(dataLen) <= columnSize) {
                                row.append(py::bytes(
                                    reinterpret_cast<const char*>(dataBuffer.data()), dataLen));
                            } else {
                                row.append(FetchLobColumnData(hStmt, i, SQL_C_BINARY, false, true));
                            }
                        } else if (dataLen == SQL_NULL_DATA) {
                            row.append(py::none());
                        } else if (dataLen == 0) {
                            row.append(py::bytes(""));
                        } else {
                            std::ostringstream oss;
                            oss << "Unexpected negative length (" << dataLen
                                << ") returned by SQLGetData. ColumnID=" << i
                                << ", dataType=" << dataType << ", bufferSize=" << columnSize;
                            LOG("SQLGetData: %s", oss.str().c_str());
                            ThrowStdException(oss.str());
                        }
                    } else {
                        LOG("SQLGetData: Error retrieving VARBINARY data for "
                            "column %d - SQLRETURN=%d",
                            i, ret);
                        row.append(py::none());
                    }
                }
                break;
            }
            case SQL_TINYINT: {
                SQLCHAR tinyIntValue;
                ret = SQLGetData_ptr(hStmt, i, SQL_C_TINYINT, &tinyIntValue, 0, NULL);
                if (SQL_SUCCEEDED(ret)) {
                    row.append(static_cast<int>(tinyIntValue));
                } else {
                    LOG("SQLGetData: Error retrieving SQL_TINYINT for column "
                        "%d - SQLRETURN=%d",
                        i, ret);
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
                    LOG("SQLGetData: Error retrieving SQL_BIT for column %d - "
                        "SQLRETURN=%d",
                        i, ret);
                    row.append(py::none());
                }
                break;
            }
#if (ODBCVER >= 0x0350)
            case SQL_GUID: {
                SQLGUID guidValue;
                SQLLEN indicator;
                ret =
                    SQLGetData_ptr(hStmt, i, SQL_C_GUID, &guidValue, sizeof(guidValue), &indicator);

                if (SQL_SUCCEEDED(ret) && indicator != SQL_NULL_DATA) {
                    std::vector<char> guid_bytes(16);
                    guid_bytes[0] = ((char*)&guidValue.Data1)[3];
                    guid_bytes[1] = ((char*)&guidValue.Data1)[2];
                    guid_bytes[2] = ((char*)&guidValue.Data1)[1];
                    guid_bytes[3] = ((char*)&guidValue.Data1)[0];
                    guid_bytes[4] = ((char*)&guidValue.Data2)[1];
                    guid_bytes[5] = ((char*)&guidValue.Data2)[0];
                    guid_bytes[6] = ((char*)&guidValue.Data3)[1];
                    guid_bytes[7] = ((char*)&guidValue.Data3)[0];
                    std::memcpy(&guid_bytes[8], guidValue.Data4, sizeof(guidValue.Data4));

                    py::bytes py_guid_bytes(guid_bytes.data(), guid_bytes.size());
                    py::object uuid_obj =
                        PythonObjectCache::get_uuid_class()(py::arg("bytes") = py_guid_bytes);
                    row.append(uuid_obj);
                } else if (indicator == SQL_NULL_DATA) {
                    row.append(py::none());
                } else {
                    LOG("SQLGetData: Error retrieving SQL_GUID for column %d - "
                        "SQLRETURN=%d, indicator=%ld",
                        i, ret, (long)indicator);
                    row.append(py::none());
                }
                break;
            }
#endif
            default:
                std::ostringstream errorString;
                errorString << "Unsupported data type for column - " << columnName << ", Type - "
                            << dataType << ", column ID - " << i;
                LOG("SQLGetData: %s", errorString.str().c_str());
                ThrowStdException(errorString.str());
                break;
        }
    }
    return ret;
}

SQLRETURN SQLFetchScroll_wrap(SqlHandlePtr StatementHandle, SQLSMALLINT FetchOrientation,
                              SQLLEN FetchOffset, py::list& row_data) {
    LOG("SQLFetchScroll_wrap: Fetching with scroll orientation=%d, offset=%ld", FetchOrientation,
        (long)FetchOffset);
    if (!SQLFetchScroll_ptr) {
        LOG("SQLFetchScroll_wrap: Function pointer not initialized. Loading "
            "the driver.");
        DriverLoader::getInstance().loadDriver();  // Load the driver
    }

    // Unbind any columns from previous fetch operations to avoid memory
    // corruption
    SQLFreeStmt_ptr(StatementHandle->get(), SQL_UNBIND);

    // Perform scroll operation
    SQLRETURN ret = SQLFetchScroll_ptr(StatementHandle->get(), FetchOrientation, FetchOffset);

    // If successful and caller wants data, retrieve it
    if (SQL_SUCCEEDED(ret) && row_data.size() == 0) {
        // Get column count
        SQLSMALLINT colCount = SQLNumResultCols_wrap(StatementHandle);

        // Get the data in a consistent way with other fetch methods
        ret = SQLGetData_wrap(StatementHandle, colCount, row_data);
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
                // TODO: handle variable length data correctly. This logic wont
                // suffice
                HandleZeroColumnSizeAtFetch(columnSize);
                uint64_t fetchBufferSize = columnSize + 1 /*null-terminator*/;
                // TODO: For LONGVARCHAR/BINARY types, columnSize is returned as
                // 2GB-1 by SQLDescribeCol. So fetchBufferSize = 2GB.
                // fetchSize=1 if columnSize>1GB. So we'll allocate a vector of
                // size 2GB. If a query fetches multiple (say N) LONG...
                // columns, we will have allocated multiple (N) 2GB sized
                // vectors. This will make driver very slow. And if the N is
                // high enough, we could hit the OS limit for heap memory that
                // we can allocate, & hence get a std::bad_alloc. The process
                // could also be killed by OS for consuming too much memory.
                // Hence this will be revisited in beta to not allocate 2GB+
                // memory, & use streaming instead
                buffers.charBuffers[col - 1].resize(fetchSize * fetchBufferSize);
                ret = SQLBindCol_ptr(hStmt, col, SQL_C_CHAR, buffers.charBuffers[col - 1].data(),
                                     fetchBufferSize * sizeof(SQLCHAR),
                                     buffers.indicators[col - 1].data());
                break;
            }
            case SQL_WCHAR:
            case SQL_WVARCHAR:
            case SQL_WLONGVARCHAR: {
                // TODO: handle variable length data correctly. This logic wont
                // suffice
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
                // TODO: handle variable length data correctly. This logic wont
                // suffice
                HandleZeroColumnSizeAtFetch(columnSize);
                buffers.charBuffers[col - 1].resize(fetchSize * columnSize);
                ret = SQLBindCol_ptr(hStmt, col, SQL_C_BINARY, buffers.charBuffers[col - 1].data(),
                                     columnSize, buffers.indicators[col - 1].data());
                break;
            case SQL_SS_TIMESTAMPOFFSET:
                buffers.datetimeoffsetBuffers[col - 1].resize(fetchSize);
                ret = SQLBindCol_ptr(hStmt, col, SQL_C_SS_TIMESTAMPOFFSET,
                                     buffers.datetimeoffsetBuffers[col - 1].data(),
                                     sizeof(DateTimeOffset) * fetchSize,
                                     buffers.indicators[col - 1].data());
                break;
            default:
                std::wstring columnName = columnMeta["ColumnName"].cast<std::wstring>();
                std::ostringstream errorString;
                errorString << "Unsupported data type for column - " << columnName.c_str()
                            << ", Type - " << dataType << ", column ID - " << col;
                LOG("SQLBindColums: %s", errorString.str().c_str());
                ThrowStdException(errorString.str());
                break;
        }
        if (!SQL_SUCCEEDED(ret)) {
            std::wstring columnName = columnMeta["ColumnName"].cast<std::wstring>();
            std::ostringstream errorString;
            errorString << "Failed to bind column - " << columnName.c_str() << ", Type - "
                        << dataType << ", column ID - " << col;
            LOG("SQLBindColums: %s", errorString.str().c_str());
            ThrowStdException(errorString.str());
            return ret;
        }
    }
    return ret;
}

// Fetch rows in batches
// TODO: Move to anonymous namespace, since it is not used outside this file
SQLRETURN FetchBatchData(SQLHSTMT hStmt, ColumnBuffers& buffers, py::list& columnNames,
                         py::list& rows, SQLUSMALLINT numCols, SQLULEN& numRowsFetched,
                         const std::vector<SQLUSMALLINT>& lobColumns) {
    LOG("FetchBatchData: Fetching data in batches");
    SQLRETURN ret = SQLFetchScroll_ptr(hStmt, SQL_FETCH_NEXT, 0);
    if (ret == SQL_NO_DATA) {
        LOG("FetchBatchData: No data to fetch");
        return ret;
    }
    if (!SQL_SUCCEEDED(ret)) {
        LOG("FetchBatchData: Error while fetching rows in batches - "
            "SQLRETURN=%d",
            ret);
        return ret;
    }
    // Pre-cache column metadata to avoid repeated dictionary lookups
    struct ColumnInfo {
        SQLSMALLINT dataType;
        SQLULEN columnSize;
        SQLULEN processedColumnSize;
        uint64_t fetchBufferSize;
        bool isLob;
    };
    std::vector<ColumnInfo> columnInfos(numCols);
    for (SQLUSMALLINT col = 0; col < numCols; col++) {
        const auto& columnMeta = columnNames[col].cast<py::dict>();
        columnInfos[col].dataType = columnMeta["DataType"].cast<SQLSMALLINT>();
        columnInfos[col].columnSize = columnMeta["ColumnSize"].cast<SQLULEN>();
        columnInfos[col].isLob =
            std::find(lobColumns.begin(), lobColumns.end(), col + 1) != lobColumns.end();
        columnInfos[col].processedColumnSize = columnInfos[col].columnSize;
        HandleZeroColumnSizeAtFetch(columnInfos[col].processedColumnSize);
        columnInfos[col].fetchBufferSize =
            columnInfos[col].processedColumnSize + 1;  // +1 for null terminator
    }

    std::string decimalSeparator = GetDecimalSeparator();  // Cache decimal separator

    // Performance: Build function pointer dispatch table (once per batch)
    // This eliminates the switch statement from the hot loop - 10,000 rows × 10
    // cols reduces from 100,000 switch evaluations to just 10 switch
    // evaluations
    std::vector<ColumnProcessor> columnProcessors(numCols);
    std::vector<ColumnInfoExt> columnInfosExt(numCols);

    for (SQLUSMALLINT col = 0; col < numCols; col++) {
        // Populate extended column info for processors that need it
        columnInfosExt[col].dataType = columnInfos[col].dataType;
        columnInfosExt[col].columnSize = columnInfos[col].columnSize;
        columnInfosExt[col].processedColumnSize = columnInfos[col].processedColumnSize;
        columnInfosExt[col].fetchBufferSize = columnInfos[col].fetchBufferSize;
        columnInfosExt[col].isLob = columnInfos[col].isLob;

        // Map data type to processor function (switch executed once per column,
        // not per cell)
        SQLSMALLINT dataType = columnInfos[col].dataType;
        switch (dataType) {
            case SQL_INTEGER:
                columnProcessors[col] = ColumnProcessors::ProcessInteger;
                break;
            case SQL_SMALLINT:
                columnProcessors[col] = ColumnProcessors::ProcessSmallInt;
                break;
            case SQL_BIGINT:
                columnProcessors[col] = ColumnProcessors::ProcessBigInt;
                break;
            case SQL_TINYINT:
                columnProcessors[col] = ColumnProcessors::ProcessTinyInt;
                break;
            case SQL_BIT:
                columnProcessors[col] = ColumnProcessors::ProcessBit;
                break;
            case SQL_REAL:
                columnProcessors[col] = ColumnProcessors::ProcessReal;
                break;
            case SQL_DOUBLE:
            case SQL_FLOAT:
                columnProcessors[col] = ColumnProcessors::ProcessDouble;
                break;
            case SQL_CHAR:
            case SQL_VARCHAR:
            case SQL_LONGVARCHAR:
                columnProcessors[col] = ColumnProcessors::ProcessChar;
                break;
            case SQL_WCHAR:
            case SQL_WVARCHAR:
            case SQL_WLONGVARCHAR:
                columnProcessors[col] = ColumnProcessors::ProcessWChar;
                break;
            case SQL_BINARY:
            case SQL_VARBINARY:
            case SQL_LONGVARBINARY:
                columnProcessors[col] = ColumnProcessors::ProcessBinary;
                break;
            default:
                // For complex types (Decimal, DateTime, Guid, etc.), set to
                // nullptr and handle via fallback switch in the hot loop
                columnProcessors[col] = nullptr;
                break;
        }
    }

    // Performance: Single-phase row creation pattern
    // Create each row, fill it completely, then append to results list
    // This prevents data corruption (no partially-filled rows) and simplifies
    // error handling
    PyObject* rowsList = rows.ptr();

    // RAII wrapper to ensure row cleanup on exception (CRITICAL: prevents
    // memory leak)
    struct RowGuard {
        PyObject* row;
        bool released;
        RowGuard() : row(nullptr), released(false) {}
        ~RowGuard() {
            if (row && !released)
                Py_DECREF(row);
        }
        void release() { released = true; }
    };

    for (SQLULEN i = 0; i < numRowsFetched; i++) {
        // Create row and immediately fill it (atomic operation per row)
        // This eliminates the two-phase pattern that could leave garbage rows
        // on exception
        RowGuard guard;
        guard.row = PyList_New(numCols);
        if (!guard.row) {
            throw std::runtime_error("Failed to allocate row list - memory allocation failure");
        }
        PyObject* row = guard.row;

        for (SQLUSMALLINT col = 1; col <= numCols; col++) {
            // Performance: Centralized NULL checking before calling processor
            // functions This eliminates redundant NULL checks inside each
            // processor and improves CPU branch prediction
            SQLLEN dataLen = buffers.indicators[col - 1][i];

            // Handle NULL and special indicator values first (applies to ALL
            // types)
            if (dataLen == SQL_NULL_DATA) {
                Py_INCREF(Py_None);
                PyList_SET_ITEM(row, col - 1, Py_None);
                continue;
            }
            if (dataLen == SQL_NO_TOTAL) {
                LOG("Cannot determine the length of the data. Returning NULL "
                    "value instead. Column ID - {}",
                    col);
                Py_INCREF(Py_None);
                PyList_SET_ITEM(row, col - 1, Py_None);
                continue;
            }

            // Performance: Use function pointer dispatch for simple types (fast
            // path) This eliminates the switch statement from hot loop -
            // reduces 100,000 switch evaluations (1000 rows × 10 cols × 10
            // types) to just 10 (setup only) Note: Processor functions no
            // longer need to check for NULL since we do it above
            if (columnProcessors[col - 1] != nullptr) {
                columnProcessors[col - 1](row, buffers, &columnInfosExt[col - 1], col, i, hStmt);
                continue;
            }

            // Fallback for complex types (Decimal, DateTime, Guid,
            // DateTimeOffset, etc.) that require pybind11 or special handling
            const ColumnInfoExt& colInfo = columnInfosExt[col - 1];
            SQLSMALLINT dataType = colInfo.dataType;

            // Additional validation for complex types
            if (dataLen == 0) {
                // Handle zero-length (non-NULL) data for complex types
                LOG("Column data length is 0 for complex datatype. Setting "
                    "None to the result row. Column ID - {}",
                    col);
                Py_INCREF(Py_None);
                PyList_SET_ITEM(row, col - 1, Py_None);
                continue;
            } else if (dataLen < 0) {
                // Negative value is unexpected, log column index, SQL type &
                // raise exception
                LOG("FetchBatchData: Unexpected negative data length - "
                    "column=%d, SQL_type=%d, dataLen=%ld",
                    col, dataType, (long)dataLen);
                ThrowStdException("Unexpected negative data length, check logs for details");
            }
            assert(dataLen > 0 && "Data length must be > 0");

            // Handle complex types that couldn't use function pointers
            switch (dataType) {
                case SQL_DECIMAL:
                case SQL_NUMERIC: {
                    try {
                        SQLLEN decimalDataLen = buffers.indicators[col - 1][i];
                        const char* rawData = reinterpret_cast<const char*>(
                            &buffers.charBuffers[col - 1][i * MAX_DIGITS_IN_NUMERIC]);

                        // Always use standard decimal point for Python Decimal
                        // parsing The decimal separator only affects display
                        // formatting, not parsing
                        PyObject* decimalObj =
                            PythonObjectCache::get_decimal_class()(py::str(rawData, decimalDataLen))
                                .release()
                                .ptr();
                        PyList_SET_ITEM(row, col - 1, decimalObj);
                    } catch (const py::error_already_set& e) {
                        // Handle the exception, e.g., log the error and set
                        // py::none()
                        LOG("Error converting to decimal: {}", e.what());
                        Py_INCREF(Py_None);
                        PyList_SET_ITEM(row, col - 1, Py_None);
                    }
                    break;
                }
                case SQL_TIMESTAMP:
                case SQL_TYPE_TIMESTAMP:
                case SQL_DATETIME: {
                    const SQL_TIMESTAMP_STRUCT& ts = buffers.timestampBuffers[col - 1][i];
                    PyObject* datetimeObj = PythonObjectCache::get_datetime_class()(
                                                ts.year, ts.month, ts.day, ts.hour, ts.minute,
                                                ts.second, ts.fraction / 1000)
                                                .release()
                                                .ptr();
                    PyList_SET_ITEM(row, col - 1, datetimeObj);
                    break;
                }
                case SQL_TYPE_DATE: {
                    PyObject* dateObj =
                        PythonObjectCache::get_date_class()(buffers.dateBuffers[col - 1][i].year,
                                                            buffers.dateBuffers[col - 1][i].month,
                                                            buffers.dateBuffers[col - 1][i].day)
                            .release()
                            .ptr();
                    PyList_SET_ITEM(row, col - 1, dateObj);
                    break;
                }
                case SQL_TIME:
                case SQL_TYPE_TIME:
                case SQL_SS_TIME2: {
                    PyObject* timeObj =
                        PythonObjectCache::get_time_class()(buffers.timeBuffers[col - 1][i].hour,
                                                            buffers.timeBuffers[col - 1][i].minute,
                                                            buffers.timeBuffers[col - 1][i].second)
                            .release()
                            .ptr();
                    PyList_SET_ITEM(row, col - 1, timeObj);
                    break;
                }
                case SQL_SS_TIMESTAMPOFFSET: {
                    SQLULEN rowIdx = i;
                    const DateTimeOffset& dtoValue = buffers.datetimeoffsetBuffers[col - 1][rowIdx];
                    SQLLEN indicator = buffers.indicators[col - 1][rowIdx];
                    if (indicator != SQL_NULL_DATA) {
                        int totalMinutes = dtoValue.timezone_hour * 60 + dtoValue.timezone_minute;
                        py::object datetime_module = py::module_::import("datetime");
                        py::object tzinfo = datetime_module.attr("timezone")(
                            datetime_module.attr("timedelta")(py::arg("minutes") = totalMinutes));
                        py::object py_dt = PythonObjectCache::get_datetime_class()(
                            dtoValue.year, dtoValue.month, dtoValue.day, dtoValue.hour,
                            dtoValue.minute, dtoValue.second,
                            dtoValue.fraction / 1000,  // ns → µs
                            tzinfo);
                        PyList_SET_ITEM(row, col - 1, py_dt.release().ptr());
                    } else {
                        Py_INCREF(Py_None);
                        PyList_SET_ITEM(row, col - 1, Py_None);
                    }
                    break;
                }
                case SQL_GUID: {
                    SQLLEN indicator = buffers.indicators[col - 1][i];
                    if (indicator == SQL_NULL_DATA) {
                        Py_INCREF(Py_None);
                        PyList_SET_ITEM(row, col - 1, Py_None);
                        break;
                    }
                    SQLGUID* guidValue = &buffers.guidBuffers[col - 1][i];
                    uint8_t reordered[16];
                    reordered[0] = ((char*)&guidValue->Data1)[3];
                    reordered[1] = ((char*)&guidValue->Data1)[2];
                    reordered[2] = ((char*)&guidValue->Data1)[1];
                    reordered[3] = ((char*)&guidValue->Data1)[0];
                    reordered[4] = ((char*)&guidValue->Data2)[1];
                    reordered[5] = ((char*)&guidValue->Data2)[0];
                    reordered[6] = ((char*)&guidValue->Data3)[1];
                    reordered[7] = ((char*)&guidValue->Data3)[0];
                    std::memcpy(reordered + 8, guidValue->Data4, 8);

                    py::bytes py_guid_bytes(reinterpret_cast<char*>(reordered), 16);
                    py::dict kwargs;
                    kwargs["bytes"] = py_guid_bytes;
                    py::object uuid_obj = PythonObjectCache::get_uuid_class()(**kwargs);
                    PyList_SET_ITEM(row, col - 1, uuid_obj.release().ptr());
                    break;
                }
                default: {
                    const auto& columnMeta = columnNames[col - 1].cast<py::dict>();
                    std::wstring columnName = columnMeta["ColumnName"].cast<std::wstring>();
                    std::ostringstream errorString;
                    errorString << "Unsupported data type for column - " << columnName.c_str()
                                << ", Type - " << dataType << ", column ID - " << col;
                    LOG("FetchBatchData: %s", errorString.str().c_str());
                    ThrowStdException(errorString.str());
                    break;
                }
            }
        }

        // Row is now fully populated - add it to results list atomically
        // This ensures no partially-filled rows exist in the list on exception
        if (PyList_Append(rowsList, row) < 0) {
            // RowGuard will clean up row automatically
            throw std::runtime_error("Failed to append row to results list - "
                                     "memory allocation failure");
        }
        // PyList_Append increments refcount, so we can release our reference
        // Mark guard as released so destructor doesn't double-free
        guard.release();
        Py_DECREF(row);
    }
    return ret;
}

// Given a list of columns that are a part of single row in the result set,
// calculates the max size of the row
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
            case SQL_SS_XML:
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
            case SQL_SS_TIMESTAMPOFFSET:
                rowSize += sizeof(DateTimeOffset);
                break;
            default:
                std::wstring columnName = columnMeta["ColumnName"].cast<std::wstring>();
                std::ostringstream errorString;
                errorString << "Unsupported data type for column - " << columnName.c_str()
                            << ", Type - " << dataType << ", column ID - " << col;
                LOG("calculateRowSize: %s", errorString.str().c_str());
                ThrowStdException(errorString.str());
                break;
        }
    }
    return rowSize;
}

// FetchMany_wrap - Fetches multiple rows of data from the result set.
//
// @param StatementHandle: Handle to the statement from which data is to be
// fetched.
// @param rows: A Python list that will be populated with the fetched rows of
// data.
// @param fetchSize: The number of rows to fetch. Default value is 1.
//
// @return SQLRETURN: SQL_SUCCESS if data is fetched successfully,
//                    SQL_NO_DATA if there are no more rows to fetch,
//                    throws a runtime error if there is an error fetching data.
//
// This function assumes that the statement handle (hStmt) is already allocated
// and a query has been executed. It fetches the specified number of rows from
// the result set and populates the provided Python list with the row data. If
// there are no more rows to fetch, it returns SQL_NO_DATA. If an error occurs
// during fetching, it throws a runtime error.
SQLRETURN FetchMany_wrap(SqlHandlePtr StatementHandle, py::list& rows, int fetchSize = 1) {
    SQLRETURN ret;
    SQLHSTMT hStmt = StatementHandle->get();
    // Retrieve column count
    SQLSMALLINT numCols = SQLNumResultCols_wrap(StatementHandle);

    // Retrieve column metadata
    py::list columnNames;
    ret = SQLDescribeCol_wrap(StatementHandle, columnNames);
    if (!SQL_SUCCEEDED(ret)) {
        LOG("FetchMany_wrap: Failed to get column descriptions - SQLRETURN=%d", ret);
        return ret;
    }

    std::vector<SQLUSMALLINT> lobColumns;
    for (SQLSMALLINT i = 0; i < numCols; i++) {
        auto colMeta = columnNames[i].cast<py::dict>();
        SQLSMALLINT dataType = colMeta["DataType"].cast<SQLSMALLINT>();
        SQLULEN columnSize = colMeta["ColumnSize"].cast<SQLULEN>();

        if ((dataType == SQL_WVARCHAR || dataType == SQL_WLONGVARCHAR || dataType == SQL_VARCHAR ||
             dataType == SQL_LONGVARCHAR || dataType == SQL_VARBINARY ||
             dataType == SQL_LONGVARBINARY || dataType == SQL_SS_XML) &&
            (columnSize == 0 || columnSize == SQL_NO_TOTAL || columnSize > SQL_MAX_LOB_SIZE)) {
            lobColumns.push_back(i + 1);  // 1-based
        }
    }

    // If we have LOBs → fall back to row-by-row fetch + SQLGetData_wrap
    if (!lobColumns.empty()) {
        LOG("FetchMany_wrap: LOB columns detected (%zu columns), using per-row "
            "SQLGetData path",
            lobColumns.size());
        while (true) {
            ret = SQLFetch_ptr(hStmt);
            if (ret == SQL_NO_DATA)
                break;
            if (!SQL_SUCCEEDED(ret))
                return ret;

            py::list row;
            SQLGetData_wrap(StatementHandle, numCols,
                            row);  // <-- streams LOBs correctly
            rows.append(row);
        }
        return SQL_SUCCESS;
    }

    // Initialize column buffers
    ColumnBuffers buffers(numCols, fetchSize);

    // Bind columns
    ret = SQLBindColums(hStmt, buffers, columnNames, numCols, fetchSize);
    if (!SQL_SUCCEEDED(ret)) {
        LOG("FetchMany_wrap: Error when binding columns - SQLRETURN=%d", ret);
        return ret;
    }

    SQLULEN numRowsFetched;
    SQLSetStmtAttr_ptr(hStmt, SQL_ATTR_ROW_ARRAY_SIZE, (SQLPOINTER)(intptr_t)fetchSize, 0);
    SQLSetStmtAttr_ptr(hStmt, SQL_ATTR_ROWS_FETCHED_PTR, &numRowsFetched, 0);

    ret = FetchBatchData(hStmt, buffers, columnNames, rows, numCols, numRowsFetched, lobColumns);
    if (!SQL_SUCCEEDED(ret) && ret != SQL_NO_DATA) {
        LOG("FetchMany_wrap: Error when fetching data - SQLRETURN=%d", ret);
        return ret;
    }

    // Reset attributes before returning to avoid using stack pointers later
    SQLSetStmtAttr_ptr(hStmt, SQL_ATTR_ROW_ARRAY_SIZE, (SQLPOINTER)1, 0);
    SQLSetStmtAttr_ptr(hStmt, SQL_ATTR_ROWS_FETCHED_PTR, NULL, 0);
    return ret;
}

// FetchAll_wrap - Fetches all rows of data from the result set.
//
// @param StatementHandle: Handle to the statement from which data is to be
// fetched.
// @param rows: A Python list that will be populated with the fetched rows of
// data.
//
// @return SQLRETURN: SQL_SUCCESS if data is fetched successfully,
//                    SQL_NO_DATA if there are no more rows to fetch,
//                    throws a runtime error if there is an error fetching data.
//
// This function assumes that the statement handle (hStmt) is already allocated
// and a query has been executed. It fetches all rows from the result set and
// populates the provided Python list with the row data. If there are no more
// rows to fetch, it returns SQL_NO_DATA. If an error occurs during fetching, it
// throws a runtime error.
SQLRETURN FetchAll_wrap(SqlHandlePtr StatementHandle, py::list& rows) {
    SQLRETURN ret;
    SQLHSTMT hStmt = StatementHandle->get();
    // Retrieve column count
    SQLSMALLINT numCols = SQLNumResultCols_wrap(StatementHandle);

    // Retrieve column metadata
    py::list columnNames;
    ret = SQLDescribeCol_wrap(StatementHandle, columnNames);
    if (!SQL_SUCCEEDED(ret)) {
        LOG("FetchAll_wrap: Failed to get column descriptions - SQLRETURN=%d", ret);
        return ret;
    }

    // Define a memory limit (1 GB)
    const size_t memoryLimit = 1ULL * 1024 * 1024 * 1024;
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
    // TODO: Revisit this logic. Eventhough we're fetching fetchSize rows at a
    // time, fetchall will keep all rows in memory anyway. So what are we
    // gaining by fetching fetchSize rows at a time? Also, say the table has
    // only 10 rows, each row size if 100 bytes. Here, we'll have fetchSize =
    // 1000, so we'll allocate memory for 1000 rows inside SQLBindCol_wrap,
    // while actually only need to retrieve 10 rows
    int fetchSize;
    if (numRowsInMemLimit == 0) {
        // If the row size is larger than the memory limit, fetch one row at a
        // time
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
    LOG("FetchAll_wrap: Fetching data in batch sizes of %d", fetchSize);

    std::vector<SQLUSMALLINT> lobColumns;
    for (SQLSMALLINT i = 0; i < numCols; i++) {
        auto colMeta = columnNames[i].cast<py::dict>();
        SQLSMALLINT dataType = colMeta["DataType"].cast<SQLSMALLINT>();
        SQLULEN columnSize = colMeta["ColumnSize"].cast<SQLULEN>();

        if ((dataType == SQL_WVARCHAR || dataType == SQL_WLONGVARCHAR || dataType == SQL_VARCHAR ||
             dataType == SQL_LONGVARCHAR || dataType == SQL_VARBINARY ||
             dataType == SQL_LONGVARBINARY || dataType == SQL_SS_XML) &&
            (columnSize == 0 || columnSize == SQL_NO_TOTAL || columnSize > SQL_MAX_LOB_SIZE)) {
            lobColumns.push_back(i + 1);  // 1-based
        }
    }

    // If we have LOBs → fall back to row-by-row fetch + SQLGetData_wrap
    if (!lobColumns.empty()) {
        LOG("FetchAll_wrap: LOB columns detected (%zu columns), using per-row "
            "SQLGetData path",
            lobColumns.size());
        while (true) {
            ret = SQLFetch_ptr(hStmt);
            if (ret == SQL_NO_DATA)
                break;
            if (!SQL_SUCCEEDED(ret))
                return ret;

            py::list row;
            SQLGetData_wrap(StatementHandle, numCols,
                            row);  // <-- streams LOBs correctly
            rows.append(row);
        }
        return SQL_SUCCESS;
    }

    ColumnBuffers buffers(numCols, fetchSize);

    // Bind columns
    ret = SQLBindColums(hStmt, buffers, columnNames, numCols, fetchSize);
    if (!SQL_SUCCEEDED(ret)) {
        LOG("FetchAll_wrap: Error when binding columns - SQLRETURN=%d", ret);
        return ret;
    }

    SQLULEN numRowsFetched;
    SQLSetStmtAttr_ptr(hStmt, SQL_ATTR_ROW_ARRAY_SIZE, (SQLPOINTER)(intptr_t)fetchSize, 0);
    SQLSetStmtAttr_ptr(hStmt, SQL_ATTR_ROWS_FETCHED_PTR, &numRowsFetched, 0);

    while (ret != SQL_NO_DATA) {
        ret =
            FetchBatchData(hStmt, buffers, columnNames, rows, numCols, numRowsFetched, lobColumns);
        if (!SQL_SUCCEEDED(ret) && ret != SQL_NO_DATA) {
            LOG("FetchAll_wrap: Error when fetching data - SQLRETURN=%d", ret);
            return ret;
        }
    }

    // Reset attributes before returning to avoid using stack pointers later
    SQLSetStmtAttr_ptr(hStmt, SQL_ATTR_ROW_ARRAY_SIZE, (SQLPOINTER)1, 0);
    SQLSetStmtAttr_ptr(hStmt, SQL_ATTR_ROWS_FETCHED_PTR, NULL, 0);

    return ret;
}

// FetchOne_wrap - Fetches a single row of data from the result set.
//
// @param StatementHandle: Handle to the statement from which data is to be
// fetched.
// @param row: A Python list that will be populated with the fetched row data.
//
// @return SQLRETURN: SQL_SUCCESS or SQL_SUCCESS_WITH_INFO if data is fetched
// successfully,
//                    SQL_NO_DATA if there are no more rows to fetch,
//                    throws a runtime error if there is an error fetching data.
//
// This function assumes that the statement handle (hStmt) is already allocated
// and a query has been executed. It fetches the next row of data from the
// result set and populates the provided Python list with the row data. If there
// are no more rows to fetch, it returns SQL_NO_DATA. If an error occurs during
// fetching, it throws a runtime error.
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
        LOG("FetchOne_wrap: Error when fetching data - SQLRETURN=%d", ret);
    }
    return ret;
}

// Wrap SQLMoreResults
SQLRETURN SQLMoreResults_wrap(SqlHandlePtr StatementHandle) {
    LOG("SQLMoreResults_wrap: Check for more results");
    if (!SQLMoreResults_ptr) {
        LOG("SQLMoreResults_wrap: Function pointer not initialized. Loading "
            "the driver.");
        DriverLoader::getInstance().loadDriver();  // Load the driver
    }

    return SQLMoreResults_ptr(StatementHandle->get());
}

// Wrap SQLFreeHandle
SQLRETURN SQLFreeHandle_wrap(SQLSMALLINT HandleType, SqlHandlePtr Handle) {
    LOG("SQLFreeHandle_wrap: Free SQL handle type=%d", HandleType);
    if (!SQLAllocHandle_ptr) {
        LOG("SQLFreeHandle_wrap: Function pointer not initialized. Loading the "
            "driver.");
        DriverLoader::getInstance().loadDriver();  // Load the driver
    }

    SQLRETURN ret = SQLFreeHandle_ptr(HandleType, Handle->get());
    if (!SQL_SUCCEEDED(ret)) {
        LOG("SQLFreeHandle_wrap: SQLFreeHandle failed with error code - %d", ret);
        return ret;
    }
    return ret;
}

// Wrap SQLRowCount
SQLLEN SQLRowCount_wrap(SqlHandlePtr StatementHandle) {
    LOG("SQLRowCount_wrap: Get number of rows affected by last execute");
    if (!SQLRowCount_ptr) {
        LOG("SQLRowCount_wrap: Function pointer not initialized. Loading the "
            "driver.");
        DriverLoader::getInstance().loadDriver();  // Load the driver
    }

    SQLLEN rowCount;
    SQLRETURN ret = SQLRowCount_ptr(StatementHandle->get(), &rowCount);
    if (!SQL_SUCCEEDED(ret)) {
        LOG("SQLRowCount_wrap: SQLRowCount failed with error code - %d", ret);
        return ret;
    }
    LOG("SQLRowCount_wrap: SQLRowCount returned %ld", (long)rowCount);
    return rowCount;
}

static std::once_flag pooling_init_flag;
void enable_pooling(int maxSize, int idleTimeout) {
    std::call_once(pooling_init_flag,
                   [&]() { ConnectionPoolManager::getInstance().configure(maxSize, idleTimeout); });
}

// Thread-safe decimal separator setting
ThreadSafeDecimalSeparator g_decimalSeparator;

void DDBCSetDecimalSeparator(const std::string& separator) {
    SetDecimalSeparator(separator);
}

// Architecture-specific defines
#ifndef ARCHITECTURE
#define ARCHITECTURE "win64"  // Default to win64 if not defined during compilation
#endif

// Functions/data to be exposed to Python as a part of ddbc_bindings module
PYBIND11_MODULE(ddbc_bindings, m) {
    m.doc() = "msodbcsql driver api bindings for Python";

    PythonObjectCache::initialize();

    // Add architecture information as module attribute
    m.attr("__architecture__") = ARCHITECTURE;

    // Expose architecture-specific constants
    m.attr("ARCHITECTURE") = ARCHITECTURE;

    // Expose the C++ functions to Python
    m.def("ThrowStdException", &ThrowStdException);
    m.def("GetDriverPathCpp", &GetDriverPathCpp, "Get the path to the ODBC driver");

    // Define parameter info class
    py::class_<ParamInfo>(m, "ParamInfo")
        .def(py::init<>())
        .def_readwrite("inputOutputType", &ParamInfo::inputOutputType)
        .def_readwrite("paramCType", &ParamInfo::paramCType)
        .def_readwrite("paramSQLType", &ParamInfo::paramSQLType)
        .def_readwrite("columnSize", &ParamInfo::columnSize)
        .def_readwrite("decimalDigits", &ParamInfo::decimalDigits)
        .def_readwrite("strLenOrInd", &ParamInfo::strLenOrInd)
        .def_readwrite("dataPtr", &ParamInfo::dataPtr)
        .def_readwrite("isDAE", &ParamInfo::isDAE);

    // Define numeric data class
    py::class_<NumericData>(m, "NumericData")
        .def(py::init<>())
        .def(py::init<SQLCHAR, SQLSCHAR, SQLCHAR, const std::string&>())
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
        .def(py::init<const std::string&, bool, const py::dict&>(), py::arg("conn_str"),
             py::arg("use_pool"), py::arg("attrs_before") = py::dict())
        .def("close", &ConnectionHandle::close, "Close the connection")
        .def("commit", &ConnectionHandle::commit, "Commit the current transaction")
        .def("rollback", &ConnectionHandle::rollback, "Rollback the current transaction")
        .def("set_autocommit", &ConnectionHandle::setAutocommit)
        .def("get_autocommit", &ConnectionHandle::getAutocommit)
        .def("set_attr", &ConnectionHandle::setAttr, py::arg("attribute"), py::arg("value"),
             "Set connection attribute")
        .def("alloc_statement_handle", &ConnectionHandle::allocStatementHandle)
        .def("get_info", &ConnectionHandle::getInfo, py::arg("info_type"));
    m.def("enable_pooling", &enable_pooling, "Enable global connection pooling");
    m.def("close_pooling", []() { ConnectionPoolManager::getInstance().closePools(); });
    m.def("DDBCSQLExecDirect", &SQLExecDirect_wrap, "Execute a SQL query directly");
    m.def("DDBCSQLExecute", &SQLExecute_wrap, "Prepare and execute T-SQL statements");
    m.def("SQLExecuteMany", &SQLExecuteMany_wrap, "Execute statement with multiple parameter sets");
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
    m.def("DDBCSQLGetAllDiagRecords", &SQLGetAllDiagRecords,
          "Get all diagnostic records for a handle", py::arg("handle"));
    m.def("DDBCSQLTables", &SQLTables_wrap, "Get table information using ODBC SQLTables",
          py::arg("StatementHandle"), py::arg("catalog") = std::wstring(),
          py::arg("schema") = std::wstring(), py::arg("table") = std::wstring(),
          py::arg("tableType") = std::wstring());
    m.def("DDBCSQLFetchScroll", &SQLFetchScroll_wrap,
          "Scroll to a specific position in the result set and optionally "
          "fetch data");
    m.def("DDBCSetDecimalSeparator", &DDBCSetDecimalSeparator,
          "Set the decimal separator character");
    m.def(
        "DDBCSQLSetStmtAttr",
        [](SqlHandlePtr stmt, SQLINTEGER attr, SQLPOINTER value) {
            return SQLSetStmtAttr_ptr(stmt->get(), attr, value, 0);
        },
        "Set statement attributes");
    m.def("DDBCSQLGetTypeInfo", &SQLGetTypeInfo_Wrapper,
          "Returns information about the data types that are supported by the "
          "data source",
          py::arg("StatementHandle"), py::arg("DataType"));
    m.def("DDBCSQLProcedures", [](SqlHandlePtr StatementHandle, const py::object& catalog,
                                  const py::object& schema, const py::object& procedure) {
        return SQLProcedures_wrap(StatementHandle, catalog, schema, procedure);
    });

    m.def("DDBCSQLForeignKeys",
          [](SqlHandlePtr StatementHandle, const py::object& pkCatalog, const py::object& pkSchema,
             const py::object& pkTable, const py::object& fkCatalog, const py::object& fkSchema,
             const py::object& fkTable) {
              return SQLForeignKeys_wrap(StatementHandle, pkCatalog, pkSchema, pkTable, fkCatalog,
                                         fkSchema, fkTable);
          });
    m.def("DDBCSQLPrimaryKeys", [](SqlHandlePtr StatementHandle, const py::object& catalog,
                                   const py::object& schema, const std::wstring& table) {
        return SQLPrimaryKeys_wrap(StatementHandle, catalog, schema, table);
    });
    m.def("DDBCSQLSpecialColumns",
          [](SqlHandlePtr StatementHandle, SQLSMALLINT identifierType, const py::object& catalog,
             const py::object& schema, const std::wstring& table, SQLSMALLINT scope,
             SQLSMALLINT nullable) {
              return SQLSpecialColumns_wrap(StatementHandle, identifierType, catalog, schema, table,
                                            scope, nullable);
          });
    m.def("DDBCSQLStatistics",
          [](SqlHandlePtr StatementHandle, const py::object& catalog, const py::object& schema,
             const std::wstring& table, SQLUSMALLINT unique, SQLUSMALLINT reserved) {
              return SQLStatistics_wrap(StatementHandle, catalog, schema, table, unique, reserved);
          });
    m.def("DDBCSQLColumns",
          [](SqlHandlePtr StatementHandle, const py::object& catalog, const py::object& schema,
             const py::object& table, const py::object& column) {
              return SQLColumns_wrap(StatementHandle, catalog, schema, table, column);
          });

    // Add a version attribute
    m.attr("__version__") = "1.0.0";

    // Expose logger bridge function to Python
    m.def("update_log_level", &mssql_python::logging::LoggerBridge::updateLevel,
          "Update the cached log level in C++ bridge");

    // Initialize the logger bridge
    try {
        mssql_python::logging::LoggerBridge::initialize();
    } catch (const std::exception& e) {
        // Log initialization failure but don't throw
        // Use std::cerr instead of fprintf for type-safe output
        std::cerr << "Logger bridge initialization failed: " << e.what() << std::endl;
    }

    try {
        // Try loading the ODBC driver when the module is imported
        LOG("Module initialization: Loading ODBC driver");
        DriverLoader::getInstance().loadDriver();  // Load the driver
    } catch (const std::exception& e) {
        // Log the error but don't throw - let the error happen when functions
        // are called
        LOG("Module initialization: Failed to load ODBC driver - %s", e.what());
    }
}
