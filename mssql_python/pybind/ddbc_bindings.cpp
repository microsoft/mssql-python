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
#define DAE_CHUNK_SIZE 8192
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
    SQLLEN strLenOrInd = 0;  // Required for DAE
    bool isDAE = false;      // Indicates if we need to stream
    py::object dataPtr; 
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

// Given a list of parameters and their ParamInfo, calls SQLBindParameter on each of them with
// appropriate arguments
SQLRETURN BindParameters(SQLHANDLE hStmt, const py::list& params,
                         std::vector<ParamInfo>& paramInfos,
                         std::vector<std::shared_ptr<void>>& paramBuffers) {
    LOG("Starting parameter binding. Number of parameters: {}", params.size());
    for (int paramIndex = 0; paramIndex < params.size(); paramIndex++) {
        const auto& param = params[paramIndex];
        ParamInfo& paramInfo = paramInfos[paramIndex];
        LOG("Binding parameter {} - C Type: {}, SQL Type: {}", paramIndex, paramInfo.paramCType, paramInfo.paramSQLType);
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
                    LOG("Parameter[{}] is marked for DAE streaming", paramIndex);
                    dataPtr = const_cast<void*>(reinterpret_cast<const void*>(&paramInfos[paramIndex]));
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
                if (!py::isinstance<py::bytes>(param) && !py::isinstance<py::bytearray>(param)) {
                    ThrowStdException(MakeParamMismatchErrorStr(paramInfo.paramCType, paramIndex));
                }
                if (paramInfo.isDAE) {
                    // Deferred execution for VARBINARY(MAX)
                    LOG("Parameter[{}] is marked for DAE streaming (VARBINARY(MAX))", paramIndex);
                    dataPtr = const_cast<void*>(reinterpret_cast<const void*>(&paramInfos[paramIndex]));
                    strLenOrIndPtr = AllocateParamBuffer<SQLLEN>(paramBuffers);
                    *strLenOrIndPtr = SQL_LEN_DATA_AT_EXEC(0);
                    bufferLength = 0;
                } else {
                    std::cout<<"I'm here"<<std::endl;
                    // small binary
                    std::string binData;
                    if (py::isinstance<py::bytes>(param)) {
                        binData = param.cast<std::string>();
                    } else {
                        // bytearray
                        binData = std::string(reinterpret_cast<const char*>(PyByteArray_AsString(param.ptr())),
                                            PyByteArray_Size(param.ptr()));
                    }
                    std::string* binBuffer = AllocateParamBuffer<std::string>(paramBuffers, binData);
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
                    LOG("Parameter[{}] is marked for DAE streaming", paramIndex);
                    dataPtr = const_cast<void*>(reinterpret_cast<const void*>(&paramInfos[paramIndex]));
                    strLenOrIndPtr = AllocateParamBuffer<SQLLEN>(paramBuffers);
                    *strLenOrIndPtr = SQL_LEN_DATA_AT_EXEC(0);
                    bufferLength = 0;
                } else {
                    // Normal small-string case
                    std::wstring* strParam =
                        AllocateParamBuffer<std::wstring>(paramBuffers, param.cast<std::wstring>());
                    LOG("SQL_C_WCHAR Parameter[{}]: Length={}, isDAE={}", paramIndex, strParam->size(), paramInfo.isDAE);
                    std::vector<SQLWCHAR>* sqlwcharBuffer =
                        AllocateParamBuffer<std::vector<SQLWCHAR>>(paramBuffers, WStringToSQLWCHAR(*strParam));
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
            // case SQL_C_DEFAULT: {
            //     if (!py::isinstance<py::none>(param)) {
            //         ThrowStdException(MakeParamMismatchErrorStr(paramInfo.paramCType, paramIndex));
            //     }
            //     // TODO: This wont work for None values added to BINARY/VARBINARY columns. None values
            //     //       of binary columns need to have C type = SQL_C_BINARY & SQL type = SQL_BINARY
            //     dataPtr = nullptr;
            //     strLenOrIndPtr = AllocateParamBuffer<SQLLEN>(paramBuffers);
            //     *strLenOrIndPtr = SQL_NULL_DATA;
            //     bufferLength = 0;
            //     break;
            // }
            case SQL_C_DEFAULT: {
                if (!py::isinstance<py::none>(param)) {
                    ThrowStdException(MakeParamMismatchErrorStr(paramInfo.paramCType, paramIndex));
                }
                SQLSMALLINT sqlType       = paramInfo.paramSQLType;
                SQLULEN     columnSize    = paramInfo.columnSize;
                SQLSMALLINT decimalDigits = paramInfo.decimalDigits;
                if (sqlType == SQL_UNKNOWN_TYPE) {
                    SQLSMALLINT describedType;
                    SQLULEN     describedSize;
                    SQLSMALLINT describedDigits;
                    SQLSMALLINT nullable;
                    RETCODE rc = SQLDescribeParam_ptr(
                        hStmt,
                        static_cast<SQLUSMALLINT>(paramIndex + 1),
                        &describedType,
                        &describedSize,
                        &describedDigits,
                        &nullable
                    );
                    if (!SQL_SUCCEEDED(rc)) {
                        LOG("SQLDescribeParam failed for parameter {} with error code {}", paramIndex, rc);
                        return rc;
                    }
                    sqlType       = describedType;
                    columnSize    = describedSize;
                    decimalDigits = describedDigits;
                }
                dataPtr = nullptr;
                strLenOrIndPtr = AllocateParamBuffer<SQLLEN>(paramBuffers);
                *strLenOrIndPtr = SQL_NULL_DATA;
                bufferLength = 0;
                paramInfo.paramSQLType   = sqlType;
                paramInfo.columnSize     = columnSize;
                paramInfo.decimalDigits  = decimalDigits;
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
    py::gil_scoped_acquire gil;  // <---- this ensures safe Python API usage

    py::object logger = py::module_::import("mssql_python.logging_config").attr("get_logger")();
    if (py::isinstance<py::none>(logger)) return;

    try {
        std::string ddbcFormatString = "[DDBC Bindings log] " + formatString;
        if constexpr (sizeof...(args) == 0) {
            logger.attr("debug")(py::str(ddbcFormatString));
        } else {
            py::str message = py::str(ddbcFormatString).format(std::forward<Args>(args)...);
            logger.attr("debug")(message);
        }
    } catch (const std::exception& e) {
        std::cerr << "Logging error: " << e.what() << std::endl;
    }
}

// TODO: Add more nuanced exception classes
void ThrowStdException(const std::string& message) { throw std::runtime_error(message); }
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
        LOG("Failed to load library: {}. Error: {}", driverPath, GetLastErrorMessage());
        ThrowStdException("Failed to load library: " + driverPath);
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


/*
 * Resolve ODBC driver path in C++ to avoid circular import issues on Alpine.
 *
 * Background:
 * On Alpine Linux, calling into Python during module initialization (via pybind11)
 * causes a circular import due to musl's stricter dynamic loader behavior.
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
            platform = "debian_ubuntu"; // Default to debian_ubuntu for other distros
        }

        fs::path driverPath = basePath / "libs" / "linux" / platform / arch / "lib" / "libmsodbcsql-18.5.so.1.1";
        return driverPath.string();

    #elif defined(__APPLE__)
        platform = "macos";
        fs::path driverPath = basePath / "libs" / platform / arch / "lib" / "libmsodbcsql.18.dylib";
        return driverPath.string();

    #elif defined(_WIN32)
        platform = "windows";
        // Normalize x86_64 to x64 for Windows naming
        if (arch == "x86_64") arch = "x64";
        fs::path driverPath = basePath / "libs" / platform / arch / "msodbcsql18.dll";
        return driverPath.string();

    #else
        throw std::runtime_error("Unsupported platform");
    #endif
}

DriverHandle LoadDriverOrThrowException() {
    namespace fs = std::filesystem;

    std::string moduleDir = GetModuleDirectory();
    LOG("Module directory: {}", moduleDir);

    std::string archStr = ARCHITECTURE;
    LOG("Architecture: {}", archStr);

    // Use only C++ function for driver path resolution
    // Not using Python function since it causes circular import issues on Alpine Linux
    // and other platforms with strict module loading rules.
    std::string driverPathStr = GetDriverPathCpp(moduleDir);
    
    fs::path driverPath(driverPathStr);
    
    LOG("Driver path determined: {}", driverPath.string());

    #ifdef _WIN32
        // On Windows, optionally load mssql-auth.dll if it exists
        std::string archDir =
            (archStr == "win64" || archStr == "amd64" || archStr == "x64") ? "x64" :
            (archStr == "arm64") ? "arm64" :
            "x86";
        
        fs::path dllDir = fs::path(moduleDir) / "libs" / "windows" / archDir;
        fs::path authDllPath = dllDir / "mssql-auth.dll";
        if (fs::exists(authDllPath)) {
            HMODULE hAuth = LoadLibraryW(std::wstring(authDllPath.native().begin(), authDllPath.native().end()).c_str());
            if (hAuth) {
                LOG("mssql-auth.dll loaded: {}", authDllPath.string());
            } else {
                LOG("Failed to load mssql-auth.dll: {}", GetLastErrorMessage());
                ThrowStdException("Failed to load mssql-auth.dll. Please ensure it is present in the expected directory.");
            }
        } else {
            LOG("Note: mssql-auth.dll not found. This is OK if Entra ID is not in use.");
            ThrowStdException("mssql-auth.dll not found. If you are using Entra ID, please ensure it is present.");
        }
    #endif

    if (!fs::exists(driverPath)) {
        ThrowStdException("ODBC driver not found at: " + driverPath.string());
    }

    DriverHandle handle = LoadDriverLibrary(driverPath.string());
    if (!handle) {
        LOG("Failed to load driver: {}", GetLastErrorMessage());
        ThrowStdException("Failed to load the driver. Please read the documentation (https://github.com/microsoft/mssql-python#installation) to install the required dependencies.");
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

    SQLParamData_ptr = GetFunctionPointer<SQLParamDataFunc>(handle, "SQLParamData");
    SQLPutData_ptr = GetFunctionPointer<SQLPutDataFunc>(handle, "SQLPutData");
    SQLTables_ptr = GetFunctionPointer<SQLTablesFunc>(handle, "SQLTablesW");

    SQLDescribeParam_ptr = GetFunctionPointer<SQLDescribeParamFunc>(handle, "SQLDescribeParam");

    bool success =
        SQLAllocHandle_ptr && SQLSetEnvAttr_ptr && SQLSetConnectAttr_ptr &&
        SQLSetStmtAttr_ptr && SQLGetConnectAttr_ptr && SQLDriverConnect_ptr &&
        SQLExecDirect_ptr && SQLPrepare_ptr && SQLBindParameter_ptr &&
        SQLExecute_ptr && SQLRowCount_ptr && SQLGetStmtAttr_ptr &&
        SQLSetDescField_ptr && SQLFetch_ptr && SQLFetchScroll_ptr &&
        SQLGetData_ptr && SQLNumResultCols_ptr && SQLBindCol_ptr &&
        SQLDescribeCol_ptr && SQLMoreResults_ptr && SQLColAttribute_ptr &&
        SQLEndTran_ptr && SQLDisconnect_ptr && SQLFreeHandle_ptr &&
        SQLFreeStmt_ptr && SQLGetDiagRec_ptr && SQLParamData_ptr &&
        SQLPutData_ptr && SQLTables_ptr &&
        SQLDescribeParam_ptr;

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

/*
 * IMPORTANT: Never log in destructors - it causes segfaults.
 * During program exit, C++ destructors may run AFTER Python shuts down.
 * LOG() tries to acquire Python GIL and call Python functions, which crashes
 * if Python is already gone. Keep destructors simple - just free resources.
 * If you need destruction logs, use explicit close() methods instead.
 */
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
        // Don't log during destruction - it can cause segfaults during Python shutdown
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

py::list SQLGetAllDiagRecords(SqlHandlePtr handle) {
    LOG("Retrieving all diagnostic records");
    if (!SQLGetDiagRec_ptr) {
        LOG("Function pointer not initialized. Loading the driver.");
        DriverLoader::getInstance().loadDriver();
    }
    
    py::list records;
    SQLHANDLE rawHandle = handle->get();
    SQLSMALLINT handleType = handle->type();
    
    // Iterate through all available diagnostic records
    for (SQLSMALLINT recNumber = 1; ; recNumber++) {
        SQLWCHAR sqlState[6] = {0};
        SQLWCHAR message[SQL_MAX_MESSAGE_LENGTH] = {0};
        SQLINTEGER nativeError = 0;
        SQLSMALLINT messageLen = 0;
        
        SQLRETURN diagReturn = SQLGetDiagRec_ptr(
            handleType, rawHandle, recNumber, sqlState, &nativeError, 
            message, SQL_MAX_MESSAGE_LENGTH, &messageLen);
            
        if (diagReturn == SQL_NO_DATA || !SQL_SUCCEEDED(diagReturn))
            break;
        
#if defined(_WIN32)
        // On Windows, create a formatted UTF-8 string for state+error
        
        // Convert SQLWCHAR sqlState to UTF-8
        int stateSize = WideCharToMultiByte(CP_UTF8, 0, sqlState, -1, NULL, 0, NULL, NULL);
        std::vector<char> stateBuffer(stateSize);
        WideCharToMultiByte(CP_UTF8, 0, sqlState, -1, stateBuffer.data(), stateSize, NULL, NULL);
        
        // Format the state with error code
        std::string stateWithError = "[" + std::string(stateBuffer.data()) + "] (" + std::to_string(nativeError) + ")";
        
        // Convert wide string message to UTF-8
        int msgSize = WideCharToMultiByte(CP_UTF8, 0, message, -1, NULL, 0, NULL, NULL);
        std::vector<char> msgBuffer(msgSize);
        WideCharToMultiByte(CP_UTF8, 0, message, -1, msgBuffer.data(), msgSize, NULL, NULL);
        
        // Create the tuple with converted strings
        records.append(py::make_tuple(
            py::str(stateWithError),
            py::str(msgBuffer.data())
        ));
#else
        // On Unix, use the SQLWCHARToWString utility and then convert to UTF-8
        std::string stateStr = WideToUTF8(SQLWCHARToWString(sqlState));
        std::string msgStr = WideToUTF8(SQLWCHARToWString(message, messageLen));
        
        // Format the state string
        std::string stateWithError = "[" + stateStr + "] (" + std::to_string(nativeError) + ")";
        
        // Create the tuple with converted strings
        records.append(py::make_tuple(
            py::str(stateWithError),
            py::str(msgStr)
        ));
#endif
    }
    
    return records;
}

// Wrap SQLExecDirect
SQLRETURN SQLExecDirect_wrap(SqlHandlePtr StatementHandle, const std::wstring& Query) {
    LOG("Execute SQL query directly - {}", Query.c_str());
    if (!SQLExecDirect_ptr) {
        LOG("Function pointer not initialized. Loading the driver.");
        DriverLoader::getInstance().loadDriver();  // Load the driver
    }

    // Ensure statement is scrollable BEFORE executing
    if (SQLSetStmtAttr_ptr && StatementHandle && StatementHandle->get()) {
        SQLSetStmtAttr_ptr(StatementHandle->get(),
                           SQL_ATTR_CURSOR_TYPE,
                           (SQLPOINTER)SQL_CURSOR_STATIC,
                           0);
        SQLSetStmtAttr_ptr(StatementHandle->get(),
                           SQL_ATTR_CONCURRENCY,
                           (SQLPOINTER)SQL_CONCUR_READ_ONLY,
                           0);
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
        LOG("Failed to execute query directly");
    }
    return ret;
}

// Wrapper for SQLTables
SQLRETURN SQLTables_wrap(SqlHandlePtr StatementHandle, 
                         const std::wstring& catalog,
                         const std::wstring& schema, 
                         const std::wstring& table,
                         const std::wstring& tableType) {
    
    if (!SQLTables_ptr) {
        LOG("Function pointer not initialized. Loading the driver.");
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

    SQLRETURN ret = SQLTables_ptr(
        StatementHandle->get(),
        catalogPtr, catalogLen,
        schemaPtr, schemaLen,
        tablePtr, tableLen,
        tableTypePtr, tableTypeLen
    );

    if (!SQL_SUCCEEDED(ret)) {
        LOG("SQLTables failed with return code: {}", ret);
    } else {
        LOG("SQLTables succeeded");
    }

    return ret;
}

// Executes the provided query. If the query is parametrized, it prepares the statement and
// binds the parameters. Otherwise, it executes the query directly.
// 'usePrepare' parameter can be used to disable the prepare step for queries that might already
// be prepared in a previous call.
SQLRETURN SQLExecute_wrap(const SqlHandlePtr statementHandle,
                          const std::wstring& query /* TODO: Use SQLTCHAR? */,
                          const py::list& params, std::vector<ParamInfo>& paramInfos,
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

    // Ensure statement is scrollable BEFORE executing
    if (SQLSetStmtAttr_ptr && hStmt) {
        SQLSetStmtAttr_ptr(hStmt,
                           SQL_ATTR_CURSOR_TYPE,
                           (SQLPOINTER)SQL_CURSOR_STATIC,
                           0);
        SQLSetStmtAttr_ptr(hStmt,
                           SQL_ATTR_CONCURRENCY,
                           (SQLPOINTER)SQL_CONCUR_READ_ONLY,
                           0);
    }

    SQLWCHAR* queryPtr;
#if defined(__APPLE__) || defined(__linux__)
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
        if (rc == SQL_NEED_DATA) {
            LOG("Beginning SQLParamData/SQLPutData loop for DAE.");
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
                            if (lenBytes > static_cast<size_t>(std::numeric_limits<SQLLEN>::max())) {
                                ThrowStdException("Chunk size exceeds maximum allowed by SQLLEN");
                            }
                            rc = SQLPutData_ptr(hStmt, (SQLPOINTER)(dataPtr + offset), static_cast<SQLLEN>(lenBytes));
                            if (!SQL_SUCCEEDED(rc)) {
                                LOG("SQLPutData failed at offset {} of {}", offset, totalChars);
                                return rc;
                            }
                            offset += len;
                        }
                    } 
                    else if (matchedInfo->paramCType == SQL_C_CHAR) {
                        std::string s = pyObj.cast<std::string>();
                        const char* dataPtr = s.data();
                        size_t totalBytes = s.size();
                        const char* dataPtr = s.data();
                        size_t offset = 0;
                        size_t chunkBytes = DAE_CHUNK_SIZE;
                        while (offset < totalBytes) {
                            size_t len = std::min(chunkBytes, totalBytes - offset);

                            rc = SQLPutData_ptr(hStmt, (SQLPOINTER)(dataPtr + offset), static_cast<SQLLEN>(len));
                            if (!SQL_SUCCEEDED(rc)) {
                                LOG("SQLPutData failed at offset {} of {}", offset, totalBytes);
                                return rc;
                            }
                            offset += len;
                        }
                    } else {
                        ThrowStdException("Unsupported C type for str in DAE");
                    }
                } else if (py::isinstance<py::bytes>(pyObj) || py::isinstance<py::bytearray>(pyObj)) {
                    py::bytes b = pyObj.cast<py::bytes>();
                    std::string s = b;
                    const char* dataPtr = s.data();
                    size_t totalBytes = s.size();
                    const size_t chunkSize = DAE_CHUNK_SIZE;
                    for (size_t offset = 0; offset < totalBytes; offset += chunkSize) {
                        size_t len = std::min(chunkSize, totalBytes - offset);
                        rc = SQLPutData_ptr(hStmt, (SQLPOINTER)(dataPtr + offset), static_cast<SQLLEN>(len));
                        if (!SQL_SUCCEEDED(rc)) {
                            LOG("SQLPutData failed at offset {} of {}", offset, totalBytes);
                            return rc;
                        }
                    }
                } else {
                    ThrowStdException("DAE only supported for str or bytes");
                }
            }
            if (!SQL_SUCCEEDED(rc)) {
                LOG("SQLParamData final rc: {}", rc);
                return rc;
            }
            LOG("DAE complete, SQLExecute resumed internally.");
        }
        if (!SQL_SUCCEEDED(rc) && rc != SQL_NO_DATA) {
            LOG("DDBCSQLExecute: Error during execution of the statement");
            return rc;
        }

        // Unbind the bound buffers for all parameters coz the buffers' memory will
        // be freed when this function exits (parambuffers goes out of scope)
        rc = SQLFreeStmt_ptr(hStmt, SQL_RESET_PARAMS);
        return rc;
    }
}

SQLRETURN BindParameterArray(SQLHANDLE hStmt,
                             const py::list& columnwise_params,
                             const std::vector<ParamInfo>& paramInfos,
                             size_t paramSetSize,
                             std::vector<std::shared_ptr<void>>& paramBuffers) {
    LOG("Starting column-wise parameter array binding. paramSetSize: {}, paramCount: {}", paramSetSize, columnwise_params.size());

    std::vector<std::shared_ptr<void>> tempBuffers;

    try {
        for (int paramIndex = 0; paramIndex < columnwise_params.size(); ++paramIndex) {
            const py::list& columnValues = columnwise_params[paramIndex].cast<py::list>();
            const ParamInfo& info = paramInfos[paramIndex];
            if (columnValues.size() != paramSetSize) {
                ThrowStdException("Column " + std::to_string(paramIndex) + " has mismatched size.");
            }
            void* dataPtr = nullptr;
            SQLLEN* strLenOrIndArray = nullptr;
            SQLLEN bufferLength = 0;
            switch (info.paramCType) {
                case SQL_C_LONG: {
                    int* dataArray = AllocateParamBufferArray<int>(tempBuffers, paramSetSize);
                    for (size_t i = 0; i < paramSetSize; ++i) {
                        if (columnValues[i].is_none()) {
                            if (!strLenOrIndArray)
                                strLenOrIndArray = AllocateParamBufferArray<SQLLEN>(tempBuffers, paramSetSize);
                            dataArray[i] = 0;
                            strLenOrIndArray[i] = SQL_NULL_DATA;
                        } else {
                            dataArray[i] = columnValues[i].cast<int>();
                            if (strLenOrIndArray) strLenOrIndArray[i] = 0;
                        }
                    }
                    dataPtr = dataArray;
                    break;
                }
                case SQL_C_DOUBLE: {
                    double* dataArray = AllocateParamBufferArray<double>(tempBuffers, paramSetSize);
                    for (size_t i = 0; i < paramSetSize; ++i) {
                        if (columnValues[i].is_none()) {
                            if (!strLenOrIndArray)
                                strLenOrIndArray = AllocateParamBufferArray<SQLLEN>(tempBuffers, paramSetSize);
                            dataArray[i] = 0;
                            strLenOrIndArray[i] = SQL_NULL_DATA;
                        } else {
                            dataArray[i] = columnValues[i].cast<double>();
                            if (strLenOrIndArray) strLenOrIndArray[i] = 0;
                        }
                    }
                    dataPtr = dataArray;
                    break;
                }
                case SQL_C_WCHAR: {
                    SQLWCHAR* wcharArray = AllocateParamBufferArray<SQLWCHAR>(tempBuffers, paramSetSize * (info.columnSize + 1));
                    strLenOrIndArray = AllocateParamBufferArray<SQLLEN>(tempBuffers, paramSetSize);
                    for (size_t i = 0; i < paramSetSize; ++i) {
                        if (columnValues[i].is_none()) {
                            strLenOrIndArray[i] = SQL_NULL_DATA;
                            std::memset(wcharArray + i * (info.columnSize + 1), 0, (info.columnSize + 1) * sizeof(SQLWCHAR));
                        } else {
                            std::wstring wstr = columnValues[i].cast<std::wstring>();
                            if (wstr.length() > info.columnSize) {
                                std::string offending = WideToUTF8(wstr);
                                ThrowStdException("Input string exceeds allowed column size at parameter index " + std::to_string(paramIndex));
                            }
                            std::memcpy(wcharArray + i * (info.columnSize + 1), wstr.c_str(), (wstr.length() + 1) * sizeof(SQLWCHAR));
                            strLenOrIndArray[i] = SQL_NTS;
                        }
                    }
                    dataPtr = wcharArray;
                    bufferLength = (info.columnSize + 1) * sizeof(SQLWCHAR);
                    break;
                }
                case SQL_C_TINYINT:
                case SQL_C_UTINYINT: {
                    unsigned char* dataArray = AllocateParamBufferArray<unsigned char>(tempBuffers, paramSetSize);
                    for (size_t i = 0; i < paramSetSize; ++i) {
                        if (columnValues[i].is_none()) {
                            if (!strLenOrIndArray)
                                strLenOrIndArray = AllocateParamBufferArray<SQLLEN>(tempBuffers, paramSetSize);
                            dataArray[i] = 0;
                            strLenOrIndArray[i] = SQL_NULL_DATA;
                        } else {
                            int intVal = columnValues[i].cast<int>();
                            if (intVal < 0 || intVal > 255) {
                                ThrowStdException("UTINYINT value out of range at rowIndex " + std::to_string(i));
                            }
                            dataArray[i] = static_cast<unsigned char>(intVal);
                            if (strLenOrIndArray) strLenOrIndArray[i] = 0;
                        }
                    }
                    dataPtr = dataArray;
                    bufferLength = sizeof(unsigned char);
                    break;
                }
                case SQL_C_SHORT: {
                    short* dataArray = AllocateParamBufferArray<short>(tempBuffers, paramSetSize);
                    for (size_t i = 0; i < paramSetSize; ++i) {
                        if (columnValues[i].is_none()) {
                            if (!strLenOrIndArray)
                                strLenOrIndArray = AllocateParamBufferArray<SQLLEN>(tempBuffers, paramSetSize);
                            dataArray[i] = 0;
                            strLenOrIndArray[i] = SQL_NULL_DATA;
                        } else {
                            int intVal = columnValues[i].cast<int>();
                            if (intVal < std::numeric_limits<short>::min() ||
                                intVal > std::numeric_limits<short>::max()) {
                                ThrowStdException("SHORT value out of range at rowIndex " + std::to_string(i));
                            }
                            dataArray[i] = static_cast<short>(intVal);
                            if (strLenOrIndArray) strLenOrIndArray[i] = 0;
                        }
                    }
                    dataPtr = dataArray;
                    bufferLength = sizeof(short);
                    break;
                }
                case SQL_C_CHAR:
                case SQL_C_BINARY: {
                    char* charArray = AllocateParamBufferArray<char>(tempBuffers, paramSetSize * (info.columnSize + 1));
                    strLenOrIndArray = AllocateParamBufferArray<SQLLEN>(tempBuffers, paramSetSize);
                    for (size_t i = 0; i < paramSetSize; ++i) {
                        if (columnValues[i].is_none()) {
                            strLenOrIndArray[i] = SQL_NULL_DATA;
                            std::memset(charArray + i * (info.columnSize + 1), 0, info.columnSize + 1);
                        } else {
                            std::string str = columnValues[i].cast<std::string>();
                            if (str.size() > info.columnSize)
                                ThrowStdException("Input exceeds column size at index " + std::to_string(i));
                            std::memcpy(charArray + i * (info.columnSize + 1), str.c_str(), str.size());
                            strLenOrIndArray[i] = static_cast<SQLLEN>(str.size());
                        }
                    }
                    dataPtr = charArray;
                    bufferLength = info.columnSize + 1;
                    break;
                }
                case SQL_C_BIT: {
                    char* boolArray = AllocateParamBufferArray<char>(tempBuffers, paramSetSize);
                    strLenOrIndArray = AllocateParamBufferArray<SQLLEN>(tempBuffers, paramSetSize);
                    for (size_t i = 0; i < paramSetSize; ++i) {
                        if (columnValues[i].is_none()) {
                            boolArray[i] = 0;
                            strLenOrIndArray[i] = SQL_NULL_DATA;
                        } else {
                            boolArray[i] = columnValues[i].cast<bool>() ? 1 : 0;
                            strLenOrIndArray[i] = 0;
                        }
                    }
                    dataPtr = boolArray;
                    bufferLength = sizeof(char);
                    break;
                }
                case SQL_C_STINYINT:
                case SQL_C_USHORT: {
                    unsigned short* dataArray = AllocateParamBufferArray<unsigned short>(tempBuffers, paramSetSize);
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
                    dataPtr = dataArray;
                    bufferLength = sizeof(unsigned short);
                    break;
                }
                case SQL_C_SBIGINT:
                case SQL_C_SLONG:
                case SQL_C_UBIGINT:
                case SQL_C_ULONG: {
                    int64_t* dataArray = AllocateParamBufferArray<int64_t>(tempBuffers, paramSetSize);
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
                    dataPtr = dataArray;
                    bufferLength = sizeof(int64_t);
                    break;
                }
                case SQL_C_FLOAT: {
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
                    dataPtr = dataArray;
                    bufferLength = sizeof(float);
                    break;
                }
                case SQL_C_TYPE_DATE: {
                    SQL_DATE_STRUCT* dateArray = AllocateParamBufferArray<SQL_DATE_STRUCT>(tempBuffers, paramSetSize);
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
                    dataPtr = dateArray;
                    bufferLength = sizeof(SQL_DATE_STRUCT);
                    break;
                }
                case SQL_C_TYPE_TIME: {
                    SQL_TIME_STRUCT* timeArray = AllocateParamBufferArray<SQL_TIME_STRUCT>(tempBuffers, paramSetSize);
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
                    dataPtr = timeArray;
                    bufferLength = sizeof(SQL_TIME_STRUCT);
                    break;
                }
                case SQL_C_TYPE_TIMESTAMP: {
                    SQL_TIMESTAMP_STRUCT* tsArray = AllocateParamBufferArray<SQL_TIMESTAMP_STRUCT>(tempBuffers, paramSetSize);
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
                            tsArray[i].fraction = static_cast<SQLUINTEGER>(dtObj.attr("microsecond").cast<int>() * 1000);  // µs to ns
                            strLenOrIndArray[i] = 0;
                        }
                    }
                    dataPtr = tsArray;
                    bufferLength = sizeof(SQL_TIMESTAMP_STRUCT);
                    break;
                }
                case SQL_C_NUMERIC: {
                    SQL_NUMERIC_STRUCT* numericArray = AllocateParamBufferArray<SQL_NUMERIC_STRUCT>(tempBuffers, paramSetSize);
                    strLenOrIndArray = AllocateParamBufferArray<SQLLEN>(tempBuffers, paramSetSize);
                    for (size_t i = 0; i < paramSetSize; ++i) {
                        const py::handle& element = columnValues[i];
                        if (element.is_none()) {
                            strLenOrIndArray[i] = SQL_NULL_DATA;
                            std::memset(&numericArray[i], 0, sizeof(SQL_NUMERIC_STRUCT));
                            continue;
                        }
                        if (!py::isinstance<NumericData>(element)) {
                            throw std::runtime_error(MakeParamMismatchErrorStr(info.paramCType, paramIndex));
                        }
                        NumericData decimalParam = element.cast<NumericData>();
                        LOG("Received numeric parameter at [%zu]: precision=%d, scale=%d, sign=%d, val=%lld",
                            i, decimalParam.precision, decimalParam.scale, decimalParam.sign, decimalParam.val);
                        numericArray[i].precision = decimalParam.precision;
                        numericArray[i].scale = decimalParam.scale;
                        numericArray[i].sign = decimalParam.sign;
                        std::memset(numericArray[i].val, 0, sizeof(numericArray[i].val));
                        std::memcpy(numericArray[i].val,
                                    reinterpret_cast<const char*>(&decimalParam.val),
                                    std::min(sizeof(decimalParam.val), sizeof(numericArray[i].val)));
                        strLenOrIndArray[i] = sizeof(SQL_NUMERIC_STRUCT);
                    }
                    dataPtr = numericArray;
                    bufferLength = sizeof(SQL_NUMERIC_STRUCT);
                    break;
                }
                default: {
                    ThrowStdException("BindParameterArray: Unsupported C type: " + std::to_string(info.paramCType));
                }
            }
            RETCODE rc = SQLBindParameter_ptr(
                hStmt,
                static_cast<SQLUSMALLINT>(paramIndex + 1),
                static_cast<SQLUSMALLINT>(info.inputOutputType),
                static_cast<SQLSMALLINT>(info.paramCType),
                static_cast<SQLSMALLINT>(info.paramSQLType),
                info.columnSize,
                info.decimalDigits,
                dataPtr,
                bufferLength,
                strLenOrIndArray
            );
            if (!SQL_SUCCEEDED(rc)) {
                LOG("Failed to bind array param {}", paramIndex);
                return rc;
            }
        }
    } catch (...) {
        LOG("Exception occurred during parameter array binding. Cleaning up.");
        throw;
    }
    paramBuffers.insert(paramBuffers.end(), tempBuffers.begin(), tempBuffers.end());
    LOG("Finished column-wise parameter array binding.");
    return SQL_SUCCESS;
}

SQLRETURN SQLExecuteMany_wrap(const SqlHandlePtr statementHandle,
                              const std::wstring& query,
                              const py::list& columnwise_params,
                              const std::vector<ParamInfo>& paramInfos,
                              size_t paramSetSize) {
    SQLHANDLE hStmt = statementHandle->get();
    SQLWCHAR* queryPtr;
#if defined(__APPLE__) || defined(__linux__)
    std::vector<SQLWCHAR> queryBuffer = WStringToSQLWCHAR(query);
    queryPtr = queryBuffer.data();
#else
    queryPtr = const_cast<SQLWCHAR*>(query.c_str());
#endif
    RETCODE rc = SQLPrepare_ptr(hStmt, queryPtr, SQL_NTS);
    if (!SQL_SUCCEEDED(rc)) return rc;
    std::vector<std::shared_ptr<void>> paramBuffers;
    rc = BindParameterArray(hStmt, columnwise_params, paramInfos, paramSetSize, paramBuffers);
    if (!SQL_SUCCEEDED(rc)) return rc;
    rc = SQLSetStmtAttr_ptr(hStmt, SQL_ATTR_PARAMSET_SIZE, (SQLPOINTER)paramSetSize, 0);
    if (!SQL_SUCCEEDED(rc)) return rc;
    rc = SQLExecute_ptr(hStmt);
    return rc;
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
#if defined(__APPLE__) || defined(__linux__)
                            std::string fullStr(reinterpret_cast<char*>(dataBuffer.data()));
                            row.append(fullStr);
                            LOG("macOS/Linux: Appended CHAR string of length {} to result row", fullStr.length());
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
#if defined(__APPLE__) || defined(__linux__)
                            auto raw_bytes = reinterpret_cast<const char*>(dataBuffer.data());
                            size_t actualBufferSize = dataBuffer.size() * sizeof(SQLWCHAR);
                            if (dataLen < 0 || static_cast<size_t>(dataLen) > actualBufferSize) {
                                LOG("Error: py::bytes creation request exceeds buffer size. dataLen={} buffer={}",
                                    dataLen, actualBufferSize);
                                ThrowStdException("Invalid buffer length for py::bytes");
                            }
                            py::bytes py_bytes(raw_bytes, dataLen);
                            py::str decoded = py_bytes.attr("decode")("utf-16-le");
                            row.append(decoded);
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

SQLRETURN SQLFetchScroll_wrap(SqlHandlePtr StatementHandle, SQLSMALLINT FetchOrientation, SQLLEN FetchOffset, py::list& row_data) {
    LOG("Fetching with scroll: orientation={}, offset={}", FetchOrientation, FetchOffset);
    if (!SQLFetchScroll_ptr) {
        LOG("Function pointer not initialized. Loading the driver.");
        DriverLoader::getInstance().loadDriver();  // Load the driver
    }

    // Unbind any columns from previous fetch operations to avoid memory corruption
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
#if defined(__APPLE__) || defined(__linux__)
                        // Use unix-specific conversion to handle the wchar_t/SQLWCHAR size difference
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

    // Reset attributes before returning to avoid using stack pointers later
    SQLSetStmtAttr_ptr(hStmt, SQL_ATTR_ROW_ARRAY_SIZE, (SQLPOINTER)1, 0);
    SQLSetStmtAttr_ptr(hStmt, SQL_ATTR_ROWS_FETCHED_PTR, NULL, 0);

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
    
    // Reset attributes before returning to avoid using stack pointers later
    SQLSetStmtAttr_ptr(hStmt, SQL_ATTR_ROW_ARRAY_SIZE, (SQLPOINTER)1, 0);
    SQLSetStmtAttr_ptr(hStmt, SQL_ATTR_ROWS_FETCHED_PTR, NULL, 0);

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
        return ret;
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
    m.def("close_pooling", []() {ConnectionPoolManager::getInstance().closePools();});
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
          "Get all diagnostic records for a handle",
          py::arg("handle"));
    m.def("DDBCSQLTables", &SQLTables_wrap, 
          "Get table information using ODBC SQLTables",
          py::arg("StatementHandle"), py::arg("catalog") = std::wstring(), 
          py::arg("schema") = std::wstring(), py::arg("table") = std::wstring(), 
          py::arg("tableType") = std::wstring());
    m.def("DDBCSQLFetchScroll", &SQLFetchScroll_wrap,
          "Scroll to a specific position in the result set and optionally fetch data");

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
