// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

// INFO|TODO - Note that is file is Windows specific right now. Making it
//             arch agnostic will be taken up in future.

#pragma once

// pybind11.h must be the first include
#include <memory>
#include <pybind11/chrono.h>
#include <pybind11/complex.h>
#include <pybind11/functional.h>
#include <pybind11/pybind11.h>
#include <pybind11/pytypes.h>  // Add this line for datetime support
#include <pybind11/stl.h>
#include <cstring>
#include <simdutf.h>
#include <string>
#include <vector>

namespace py = pybind11;
using py::literals::operator""_a;

#ifdef _WIN32
// Windows-specific headers
#include <Windows.h>  // windows.h needs to be included before sql.h
#include <shlwapi.h>
#pragma comment(lib, "shlwapi.lib")
#define IS_WINDOWS 1
#else
#define IS_WINDOWS 0
#endif

#include <sql.h>
#include <sqlext.h>

// Include logger bridge for LOG macros
#include "logger_bridge.hpp"

#if defined(__APPLE__) || defined(__linux__)
#include <dlfcn.h>
#endif

inline std::string utf16LeToUtf8Alloc(const std::u16string& utf16) {
    if (utf16.empty()) {
        return {};
    }

    simdutf::result utf8Length =
        simdutf::utf8_length_from_utf16le_with_replacement(utf16.data(), utf16.size());
    std::string utf8(utf8Length.count, '\0');
    utf8.resize(
        simdutf::convert_utf16le_to_utf8_with_replacement(utf16.data(), utf16.size(), utf8.data()));
    return utf8;
}

inline std::u16string dupeSqlWCharAsUtf16Le(const SQLWCHAR* value, size_t length) {
    std::u16string utf16(length, u'\0');
    static_assert(sizeof(SQLWCHAR) == sizeof(char16_t), "SQLWCHAR must be 16-bit");

    if (length > 0) {
        std::memcpy(utf16.data(), value, length * sizeof(SQLWCHAR));
    }
    return utf16;
}

inline SQLWCHAR* reinterpretU16stringAsSqlWChar(const std::u16string& utf16) {
    static_assert(sizeof(std::u16string::value_type) == sizeof(SQLWCHAR),
        "SQLWCHAR must same as u16string");
    static_assert(alignof(std::u16string::value_type) == alignof(SQLWCHAR),
        "SQLWCHAR must same as u16string");
    return const_cast<SQLWCHAR*>(reinterpret_cast<const SQLWCHAR*>(utf16.c_str()));
}

//-------------------------------------------------------------------------------------------------
// Function pointer typedefs
//-------------------------------------------------------------------------------------------------

// Handle APIs
typedef SQLRETURN(SQL_API* SQLAllocHandleFunc)(SQLSMALLINT, SQLHANDLE, SQLHANDLE*);
typedef SQLRETURN(SQL_API* SQLSetEnvAttrFunc)(SQLHANDLE, SQLINTEGER, SQLPOINTER, SQLINTEGER);
typedef SQLRETURN(SQL_API* SQLSetConnectAttrFunc)(SQLHDBC, SQLINTEGER, SQLPOINTER, SQLINTEGER);
typedef SQLRETURN(SQL_API* SQLSetStmtAttrFunc)(SQLHSTMT, SQLINTEGER, SQLPOINTER, SQLINTEGER);
typedef SQLRETURN(SQL_API* SQLGetConnectAttrFunc)(SQLHDBC, SQLINTEGER, SQLPOINTER, SQLINTEGER,
                                                  SQLINTEGER*);

// Connection and Execution APIs
typedef SQLRETURN(SQL_API* SQLDriverConnectFunc)(SQLHANDLE, SQLHWND, SQLWCHAR*, SQLSMALLINT,
                                                 SQLWCHAR*, SQLSMALLINT, SQLSMALLINT*,
                                                 SQLUSMALLINT);
typedef SQLRETURN(SQL_API* SQLExecDirectFunc)(SQLHANDLE, SQLWCHAR*, SQLINTEGER);
typedef SQLRETURN(SQL_API* SQLPrepareFunc)(SQLHANDLE, SQLWCHAR*, SQLINTEGER);
typedef SQLRETURN(SQL_API* SQLBindParameterFunc)(SQLHANDLE, SQLUSMALLINT, SQLSMALLINT, SQLSMALLINT,
                                                 SQLSMALLINT, SQLULEN, SQLSMALLINT, SQLPOINTER,
                                                 SQLLEN, SQLLEN*);
typedef SQLRETURN(SQL_API* SQLExecuteFunc)(SQLHANDLE);
typedef SQLRETURN(SQL_API* SQLRowCountFunc)(SQLHSTMT, SQLLEN*);
typedef SQLRETURN(SQL_API* SQLSetDescFieldFunc)(SQLHDESC, SQLSMALLINT, SQLSMALLINT, SQLPOINTER,
                                                SQLINTEGER);
typedef SQLRETURN(SQL_API* SQLGetStmtAttrFunc)(SQLHSTMT, SQLINTEGER, SQLPOINTER, SQLINTEGER,
                                               SQLINTEGER*);

// Data retrieval APIs
typedef SQLRETURN(SQL_API* SQLFetchFunc)(SQLHANDLE);
typedef SQLRETURN(SQL_API* SQLFetchScrollFunc)(SQLHANDLE, SQLSMALLINT, SQLLEN);
typedef SQLRETURN(SQL_API* SQLGetDataFunc)(SQLHANDLE, SQLUSMALLINT, SQLSMALLINT, SQLPOINTER, SQLLEN,
                                           SQLLEN*);
typedef SQLRETURN(SQL_API* SQLNumResultColsFunc)(SQLHSTMT, SQLSMALLINT*);
typedef SQLRETURN(SQL_API* SQLBindColFunc)(SQLHSTMT, SQLUSMALLINT, SQLSMALLINT, SQLPOINTER, SQLLEN,
                                           SQLLEN*);
typedef SQLRETURN(SQL_API* SQLDescribeColFunc)(SQLHSTMT, SQLUSMALLINT, SQLWCHAR*, SQLSMALLINT,
                                               SQLSMALLINT*, SQLSMALLINT*, SQLULEN*, SQLSMALLINT*,
                                               SQLSMALLINT*);
typedef SQLRETURN(SQL_API* SQLMoreResultsFunc)(SQLHSTMT);
typedef SQLRETURN(SQL_API* SQLColAttributeFunc)(SQLHSTMT, SQLUSMALLINT, SQLUSMALLINT, SQLPOINTER,
                                                SQLSMALLINT, SQLSMALLINT*, SQLPOINTER);
typedef SQLRETURN (*SQLTablesFunc)(SQLHSTMT StatementHandle, SQLWCHAR* CatalogName,
                                   SQLSMALLINT NameLength1, SQLWCHAR* SchemaName,
                                   SQLSMALLINT NameLength2, SQLWCHAR* TableName,
                                   SQLSMALLINT NameLength3, SQLWCHAR* TableType,
                                   SQLSMALLINT NameLength4);
typedef SQLRETURN(SQL_API* SQLGetTypeInfoFunc)(SQLHSTMT, SQLSMALLINT);
typedef SQLRETURN(SQL_API* SQLProceduresFunc)(SQLHSTMT, SQLWCHAR*, SQLSMALLINT, SQLWCHAR*,
                                              SQLSMALLINT, SQLWCHAR*, SQLSMALLINT);
typedef SQLRETURN(SQL_API* SQLForeignKeysFunc)(SQLHSTMT, SQLWCHAR*, SQLSMALLINT, SQLWCHAR*,
                                               SQLSMALLINT, SQLWCHAR*, SQLSMALLINT, SQLWCHAR*,
                                               SQLSMALLINT, SQLWCHAR*, SQLSMALLINT, SQLWCHAR*,
                                               SQLSMALLINT);
typedef SQLRETURN(SQL_API* SQLPrimaryKeysFunc)(SQLHSTMT, SQLWCHAR*, SQLSMALLINT, SQLWCHAR*,
                                               SQLSMALLINT, SQLWCHAR*, SQLSMALLINT);
typedef SQLRETURN(SQL_API* SQLSpecialColumnsFunc)(SQLHSTMT, SQLUSMALLINT, SQLWCHAR*, SQLSMALLINT,
                                                  SQLWCHAR*, SQLSMALLINT, SQLWCHAR*, SQLSMALLINT,
                                                  SQLUSMALLINT, SQLUSMALLINT);
typedef SQLRETURN(SQL_API* SQLStatisticsFunc)(SQLHSTMT, SQLWCHAR*, SQLSMALLINT, SQLWCHAR*,
                                              SQLSMALLINT, SQLWCHAR*, SQLSMALLINT, SQLUSMALLINT,
                                              SQLUSMALLINT);
typedef SQLRETURN(SQL_API* SQLColumnsFunc)(SQLHSTMT, SQLWCHAR*, SQLSMALLINT, SQLWCHAR*, SQLSMALLINT,
                                           SQLWCHAR*, SQLSMALLINT, SQLWCHAR*, SQLSMALLINT);
typedef SQLRETURN(SQL_API* SQLGetInfoFunc)(SQLHDBC, SQLUSMALLINT, SQLPOINTER, SQLSMALLINT,
                                           SQLSMALLINT*);

// Transaction APIs
typedef SQLRETURN(SQL_API* SQLEndTranFunc)(SQLSMALLINT, SQLHANDLE, SQLSMALLINT);

// Disconnect/free APIs
typedef SQLRETURN(SQL_API* SQLFreeHandleFunc)(SQLSMALLINT, SQLHANDLE);
typedef SQLRETURN(SQL_API* SQLDisconnectFunc)(SQLHDBC);
typedef SQLRETURN(SQL_API* SQLFreeStmtFunc)(SQLHSTMT, SQLUSMALLINT);

// Diagnostic APIs
typedef SQLRETURN(SQL_API* SQLGetDiagRecFunc)(SQLSMALLINT, SQLHANDLE, SQLSMALLINT, SQLWCHAR*,
                                              SQLINTEGER*, SQLWCHAR*, SQLSMALLINT, SQLSMALLINT*);

typedef SQLRETURN(SQL_API* SQLDescribeParamFunc)(SQLHSTMT, SQLUSMALLINT, SQLSMALLINT*, SQLULEN*,
                                                 SQLSMALLINT*, SQLSMALLINT*);

// DAE APIs
typedef SQLRETURN(SQL_API* SQLParamDataFunc)(SQLHSTMT, SQLPOINTER*);
typedef SQLRETURN(SQL_API* SQLPutDataFunc)(SQLHSTMT, SQLPOINTER, SQLLEN);
//-------------------------------------------------------------------------------------------------
// Extern function pointer declarations (defined in ddbc_bindings.cpp)
//-------------------------------------------------------------------------------------------------

// Handle APIs
extern SQLAllocHandleFunc SQLAllocHandle_ptr;
extern SQLSetEnvAttrFunc SQLSetEnvAttr_ptr;
extern SQLSetConnectAttrFunc SQLSetConnectAttr_ptr;
extern SQLSetStmtAttrFunc SQLSetStmtAttr_ptr;
extern SQLGetConnectAttrFunc SQLGetConnectAttr_ptr;

// Connection and Execution APIs
extern SQLDriverConnectFunc SQLDriverConnect_ptr;
extern SQLExecDirectFunc SQLExecDirect_ptr;
extern SQLPrepareFunc SQLPrepare_ptr;
extern SQLBindParameterFunc SQLBindParameter_ptr;
extern SQLExecuteFunc SQLExecute_ptr;
extern SQLRowCountFunc SQLRowCount_ptr;
extern SQLSetDescFieldFunc SQLSetDescField_ptr;
extern SQLGetStmtAttrFunc SQLGetStmtAttr_ptr;

// Data retrieval APIs
extern SQLFetchFunc SQLFetch_ptr;
extern SQLFetchScrollFunc SQLFetchScroll_ptr;
extern SQLGetDataFunc SQLGetData_ptr;
extern SQLNumResultColsFunc SQLNumResultCols_ptr;
extern SQLBindColFunc SQLBindCol_ptr;
extern SQLDescribeColFunc SQLDescribeCol_ptr;
extern SQLMoreResultsFunc SQLMoreResults_ptr;
extern SQLColAttributeFunc SQLColAttribute_ptr;
extern SQLTablesFunc SQLTables_ptr;
extern SQLGetTypeInfoFunc SQLGetTypeInfo_ptr;
extern SQLProceduresFunc SQLProcedures_ptr;
extern SQLForeignKeysFunc SQLForeignKeys_ptr;
extern SQLPrimaryKeysFunc SQLPrimaryKeys_ptr;
extern SQLSpecialColumnsFunc SQLSpecialColumns_ptr;
extern SQLStatisticsFunc SQLStatistics_ptr;
extern SQLColumnsFunc SQLColumns_ptr;
extern SQLGetInfoFunc SQLGetInfo_ptr;

// Transaction APIs
extern SQLEndTranFunc SQLEndTran_ptr;

// Disconnect/free APIs
extern SQLFreeHandleFunc SQLFreeHandle_ptr;
extern SQLDisconnectFunc SQLDisconnect_ptr;
extern SQLFreeStmtFunc SQLFreeStmt_ptr;

// Diagnostic APIs
extern SQLGetDiagRecFunc SQLGetDiagRec_ptr;

extern SQLDescribeParamFunc SQLDescribeParam_ptr;

// DAE APIs
extern SQLParamDataFunc SQLParamData_ptr;
extern SQLPutDataFunc SQLPutData_ptr;

// Throws a std::runtime_error with the given message
void ThrowStdException(const std::string& message);

// Define a platform-agnostic type for the driver handle
#ifdef _WIN32
typedef HMODULE DriverHandle;
#else
typedef void* DriverHandle;
#endif

// Platform-agnostic function to get a function pointer from the loaded library
template <typename T>
T GetFunctionPointer(DriverHandle handle, const char* functionName) {
#ifdef _WIN32
    // Windows: Use GetProcAddress
    return reinterpret_cast<T>(GetProcAddress(handle, functionName));
#else
    // macOS/Unix: Use dlsym
    return reinterpret_cast<T>(dlsym(handle, functionName));
#endif
}

//-------------------------------------------------------------------------------------------------
// Loads the ODBC driver and resolves function pointers.
// Throws if loading or resolution fails.
//-------------------------------------------------------------------------------------------------
DriverHandle LoadDriverOrThrowException();

//-------------------------------------------------------------------------------------------------
// DriverLoader (Singleton)
//
// Ensures the ODBC driver and all function pointers are loaded exactly once
// across the process.
// This avoids redundant work and ensures thread-safe, centralized
// initialization.
//
// Not copyable or assignable.
//-------------------------------------------------------------------------------------------------
class DriverLoader {
  public:
    static DriverLoader& getInstance();
    void loadDriver();

  private:
    DriverLoader();
    DriverLoader(const DriverLoader&) = delete;
    DriverLoader& operator=(const DriverLoader&) = delete;

    bool m_driverLoaded;
    std::once_flag m_onceFlag;
};

//-------------------------------------------------------------------------------------------------
// SqlHandle
//
// RAII wrapper around ODBC handles (ENV, DBC, STMT).
// Use `std::shared_ptr<SqlHandle>` (alias: SqlHandlePtr) for shared ownership.
//-------------------------------------------------------------------------------------------------
class SqlHandle {
  public:
    SqlHandle(SQLSMALLINT type, SQLHANDLE rawHandle);
    ~SqlHandle();
    SQLHANDLE get() const;
    SQLSMALLINT type() const;
    void free();
    void close_cursor();
    bool isImplicitlyFreed() const { return _implicitly_freed; }

    // Mark this handle as implicitly freed (freed by parent handle)
    // This prevents double-free attempts when the ODBC driver automatically
    // frees child handles (e.g., STMT handles when DBC handle is freed)
    //
    // SAFETY CONSTRAINTS:
    // - ONLY call this on SQL_HANDLE_STMT handles
    // - ONLY call this when the parent DBC handle is about to be freed
    // - Calling on other handle types (ENV, DBC, DESC) will cause HANDLE LEAKS
    // - The ODBC spec only guarantees automatic freeing of STMT handles by DBC parents
    //
    // Current usage: Connection::disconnect() marks all tracked STMT handles
    // before freeing the DBC handle.
    void markImplicitlyFreed();

  private:
    SQLSMALLINT _type;
    SQLHANDLE _handle;
    bool _implicitly_freed = false;  // Tracks if handle was freed by parent
};
using SqlHandlePtr = std::shared_ptr<SqlHandle>;

// This struct is used to relay error info obtained from SQLDiagRec API to the
// Python module
struct ErrorInfo {
    std::string sqlState;
    std::string ddbcErrorMsg;
};
ErrorInfo SQLCheckError_Wrap(SQLSMALLINT handleType, SqlHandlePtr handle, SQLRETURN retcode);

// Thread-safe decimal separator accessor class
class ThreadSafeDecimalSeparator {
  private:
    std::string value;
    mutable std::mutex mutex;

  public:
    // Constructor with default value
    ThreadSafeDecimalSeparator() : value(".") {}

    // Set the decimal separator with thread safety
    void set(const std::string& separator) {
        std::lock_guard<std::mutex> lock(mutex);
        value = separator;
    }

    // Get the decimal separator with thread safety
    std::string get() const {
        std::lock_guard<std::mutex> lock(mutex);
        return value;
    }

    // Returns whether the current separator is different from the default "."
    bool isCustomSeparator() const {
        std::lock_guard<std::mutex> lock(mutex);
        return value != ".";
    }
};

// Global instance
extern ThreadSafeDecimalSeparator g_decimalSeparator;

// Helper functions to replace direct access
inline void SetDecimalSeparator(const std::string& separator) {
    g_decimalSeparator.set(separator);
}

inline std::string GetDecimalSeparator() {
    return g_decimalSeparator.get();
}

// Function to set the decimal separator
void DDBCSetDecimalSeparator(const std::string& separator);

//-------------------------------------------------------------------------------------------------
// INTERNAL: Performance Optimization Helpers for Fetch Path
// (Used internally by ddbc_bindings.cpp - not part of public API)
//-------------------------------------------------------------------------------------------------

// Struct to hold the SQL Server TIME2 structure (SQL_C_SS_TIME2)
struct SQL_SS_TIME2_STRUCT {
    SQLUSMALLINT hour;
    SQLUSMALLINT minute;
    SQLUSMALLINT second;
    SQLUINTEGER fraction;  // Nanoseconds
};

// Struct to hold the DateTimeOffset structure
struct DateTimeOffset {
    SQLSMALLINT year;
    SQLUSMALLINT month;
    SQLUSMALLINT day;
    SQLUSMALLINT hour;
    SQLUSMALLINT minute;
    SQLUSMALLINT second;
    SQLUINTEGER fraction;         // Nanoseconds
    SQLSMALLINT timezone_hour;    // Offset hours from UTC
    SQLSMALLINT timezone_minute;  // Offset minutes from UTC
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
    std::vector<std::vector<SQL_SS_TIME2_STRUCT>> timeBuffers;
    std::vector<std::vector<SQLGUID>> guidBuffers;
    std::vector<std::vector<SQLLEN>> indicators;
    std::vector<std::vector<DateTimeOffset>> datetimeoffsetBuffers;

    ColumnBuffers(SQLSMALLINT numCols, int fetchSize)
        : charBuffers(numCols), wcharBuffers(numCols), intBuffers(numCols),
          smallIntBuffers(numCols), realBuffers(numCols), doubleBuffers(numCols),
          timestampBuffers(numCols), bigIntBuffers(numCols), dateBuffers(numCols),
          timeBuffers(numCols), guidBuffers(numCols), datetimeoffsetBuffers(numCols),
          indicators(numCols, std::vector<SQLLEN>(fetchSize)) {}
};

// Performance: Column processor function type for fast type conversion
// Using function pointers eliminates switch statement overhead in the hot loop
typedef void (*ColumnProcessor)(PyObject* row, ColumnBuffers& buffers, const void* colInfo,
                                SQLUSMALLINT col, SQLULEN rowIdx, SQLHSTMT hStmt);

// Extended column info struct for processor functions
struct ColumnInfoExt {
    SQLSMALLINT dataType;
    SQLULEN columnSize;
    SQLULEN processedColumnSize;
    uint64_t fetchBufferSize;
    bool isLob;
    bool isUtf8;               // Pre-computed from charEncoding (avoids string compare per cell)
    std::string charEncoding;  // Effective decoding encoding for SQL_C_CHAR data
};

// Forward declare FetchLobColumnData (defined in ddbc_bindings.cpp) - MUST be
// outside namespace
py::object FetchLobColumnData(SQLHSTMT hStmt, SQLUSMALLINT col, SQLSMALLINT cType, bool isWideChar,
                              bool isBinary, const std::string& charEncoding = "utf-8");

// Specialized column processors for each data type (eliminates switch in hot
// loop)
namespace ColumnProcessors {

// Process SQL INTEGER (4-byte int) column into Python int
// SAFETY: PyList_SET_ITEM is safe here because row is freshly allocated with
// PyList_New()
//         and each slot is filled exactly once (NULL -> value)
// Performance: NULL check removed - handled centrally before processor is
// called
inline void ProcessInteger(PyObject* row, ColumnBuffers& buffers, const void*, SQLUSMALLINT col,
                           SQLULEN rowIdx, SQLHSTMT) {
    // Performance: Direct Python C API call (bypasses pybind11 overhead)
    PyObject* pyInt = PyLong_FromLong(buffers.intBuffers[col - 1][rowIdx]);
    if (!pyInt) {  // Handle memory allocation failure
        Py_INCREF(Py_None);
        PyList_SET_ITEM(row, col - 1, Py_None);
        return;
    }
    PyList_SET_ITEM(row, col - 1, pyInt);  // Transfer ownership to list
}

// Process SQL SMALLINT (2-byte int) column into Python int
// Performance: NULL check removed - handled centrally before processor is
// called
inline void ProcessSmallInt(PyObject* row, ColumnBuffers& buffers, const void*, SQLUSMALLINT col,
                            SQLULEN rowIdx, SQLHSTMT) {
    // Performance: Direct Python C API call
    PyObject* pyInt = PyLong_FromLong(buffers.smallIntBuffers[col - 1][rowIdx]);
    if (!pyInt) {  // Handle memory allocation failure
        Py_INCREF(Py_None);
        PyList_SET_ITEM(row, col - 1, Py_None);
        return;
    }
    PyList_SET_ITEM(row, col - 1, pyInt);
}

// Process SQL BIGINT (8-byte int) column into Python int
// Performance: NULL check removed - handled centrally before processor is
// called
inline void ProcessBigInt(PyObject* row, ColumnBuffers& buffers, const void*, SQLUSMALLINT col,
                          SQLULEN rowIdx, SQLHSTMT) {
    // Performance: Direct Python C API call
    PyObject* pyInt = PyLong_FromLongLong(buffers.bigIntBuffers[col - 1][rowIdx]);
    if (!pyInt) {  // Handle memory allocation failure
        Py_INCREF(Py_None);
        PyList_SET_ITEM(row, col - 1, Py_None);
        return;
    }
    PyList_SET_ITEM(row, col - 1, pyInt);
}

// Process SQL TINYINT (1-byte unsigned int) column into Python int
// Performance: NULL check removed - handled centrally before processor is
// called
inline void ProcessTinyInt(PyObject* row, ColumnBuffers& buffers, const void*, SQLUSMALLINT col,
                           SQLULEN rowIdx, SQLHSTMT) {
    // Performance: Direct Python C API call
    PyObject* pyInt = PyLong_FromLong(buffers.charBuffers[col - 1][rowIdx]);
    if (!pyInt) {  // Handle memory allocation failure
        Py_INCREF(Py_None);
        PyList_SET_ITEM(row, col - 1, Py_None);
        return;
    }
    PyList_SET_ITEM(row, col - 1, pyInt);
}

// Process SQL BIT column into Python bool
// Performance: NULL check removed - handled centrally before processor is
// called
inline void ProcessBit(PyObject* row, ColumnBuffers& buffers, const void*, SQLUSMALLINT col,
                       SQLULEN rowIdx, SQLHSTMT) {
    // Performance: Direct Python C API call (converts 0/1 to True/False)
    PyObject* pyBool = PyBool_FromLong(buffers.charBuffers[col - 1][rowIdx]);
    if (!pyBool) {  // Handle memory allocation failure
        Py_INCREF(Py_None);
        PyList_SET_ITEM(row, col - 1, Py_None);
        return;
    }
    PyList_SET_ITEM(row, col - 1, pyBool);
}

// Process SQL REAL (4-byte float) column into Python float
// Performance: NULL check removed - handled centrally before processor is
// called
inline void ProcessReal(PyObject* row, ColumnBuffers& buffers, const void*, SQLUSMALLINT col,
                        SQLULEN rowIdx, SQLHSTMT) {
    // Performance: Direct Python C API call
    PyObject* pyFloat = PyFloat_FromDouble(buffers.realBuffers[col - 1][rowIdx]);
    if (!pyFloat) {  // Handle memory allocation failure
        Py_INCREF(Py_None);
        PyList_SET_ITEM(row, col - 1, Py_None);
        return;
    }
    PyList_SET_ITEM(row, col - 1, pyFloat);
}

// Process SQL DOUBLE/FLOAT (8-byte float) column into Python float
// Performance: NULL check removed - handled centrally before processor is
// called
inline void ProcessDouble(PyObject* row, ColumnBuffers& buffers, const void*, SQLUSMALLINT col,
                          SQLULEN rowIdx, SQLHSTMT) {
    // Performance: Direct Python C API call
    PyObject* pyFloat = PyFloat_FromDouble(buffers.doubleBuffers[col - 1][rowIdx]);
    if (!pyFloat) {  // Handle memory allocation failure
        Py_INCREF(Py_None);
        PyList_SET_ITEM(row, col - 1, Py_None);
        return;
    }
    PyList_SET_ITEM(row, col - 1, pyFloat);
}

// Process SQL CHAR/VARCHAR (single-byte string) column into Python str
// Performance: NULL/NO_TOTAL checks removed - handled centrally before
// processor is called
inline void ProcessChar(PyObject* row, ColumnBuffers& buffers, const void* colInfoPtr,
                        SQLUSMALLINT col, SQLULEN rowIdx, SQLHSTMT hStmt) {
    const ColumnInfoExt* colInfo = static_cast<const ColumnInfoExt*>(colInfoPtr);
    SQLLEN dataLen = buffers.indicators[col - 1][rowIdx];

    // Handle empty strings
    if (dataLen == 0) {
        PyObject* emptyStr = PyUnicode_FromStringAndSize("", 0);
        if (!emptyStr) {
            Py_INCREF(Py_None);
            PyList_SET_ITEM(row, col - 1, Py_None);
        } else {
            PyList_SET_ITEM(row, col - 1, emptyStr);
        }
        return;
    }

    uint64_t numCharsInData = dataLen / sizeof(SQLCHAR);
    // Fast path: Data fits in buffer (not LOB or truncated)
    // fetchBufferSize includes null-terminator, numCharsInData doesn't. Hence
    // '<'
    if (!colInfo->isLob && numCharsInData < colInfo->fetchBufferSize) {
        const char* dataPtr = reinterpret_cast<char*>(
            &buffers.charBuffers[col - 1][rowIdx * colInfo->fetchBufferSize]);
        PyObject* pyStr = nullptr;
#if defined(__APPLE__) || defined(__linux__)
        // On Linux/macOS, ODBC driver returns UTF-8 — PyUnicode_FromStringAndSize
        // expects UTF-8, so this is correct and fast.
        pyStr = PyUnicode_FromStringAndSize(dataPtr, numCharsInData);
#else
        // On Windows, ODBC driver returns bytes in the server's native encoding.
        // For UTF-8, use the direct C API (PyUnicode_FromStringAndSize) which
        // bypasses the codec registry for maximum reliability. For non-UTF-8
        // encodings (e.g., CP1252), use PyUnicode_Decode with the codec registry.
        if (colInfo->isUtf8) {
            pyStr = PyUnicode_FromStringAndSize(dataPtr, numCharsInData);
        } else {
            pyStr =
                PyUnicode_Decode(dataPtr, numCharsInData, colInfo->charEncoding.c_str(), "strict");
        }
#endif
        if (!pyStr) {
            // Decode failed — fall back to returning raw bytes (consistent with
            // FetchLobColumnData and SQLGetData_wrap which also return raw bytes
            // on decode failure instead of silently converting to None).
            PyErr_Clear();
            PyObject* pyBytes = PyBytes_FromStringAndSize(dataPtr, numCharsInData);
            if (pyBytes) {
                PyList_SET_ITEM(row, col - 1, pyBytes);
            } else {
                PyErr_Clear();
                Py_INCREF(Py_None);
                PyList_SET_ITEM(row, col - 1, Py_None);
            }
        } else {
            PyList_SET_ITEM(row, col - 1, pyStr);
        }
    } else {
        // Slow path: LOB data requires separate fetch call
        PyList_SET_ITEM(
            row, col - 1,
            FetchLobColumnData(hStmt, col, SQL_C_CHAR, false, false, colInfo->charEncoding)
                .release()
                .ptr());
    }
}

// Process SQL NCHAR/NVARCHAR (wide/Unicode string) column into Python str
// Performance: NULL/NO_TOTAL checks removed - handled centrally before
// processor is called
inline void ProcessWChar(PyObject* row, ColumnBuffers& buffers, const void* colInfoPtr,
                         SQLUSMALLINT col, SQLULEN rowIdx, SQLHSTMT hStmt) {
    const ColumnInfoExt* colInfo = static_cast<const ColumnInfoExt*>(colInfoPtr);
    SQLLEN dataLen = buffers.indicators[col - 1][rowIdx];

    // Handle empty strings
    if (dataLen == 0) {
        PyObject* emptyStr = PyUnicode_FromStringAndSize("", 0);
        if (!emptyStr) {
            Py_INCREF(Py_None);
            PyList_SET_ITEM(row, col - 1, Py_None);
        } else {
            PyList_SET_ITEM(row, col - 1, emptyStr);
        }
        return;
    }

    uint64_t numCharsInData = dataLen / sizeof(SQLWCHAR);
    // Fast path: Data fits in buffer (not LOB or truncated)
    // fetchBufferSize includes null-terminator, numCharsInData doesn't. Hence
    // '<'
    if (!colInfo->isLob && numCharsInData < colInfo->fetchBufferSize) {
#if defined(__APPLE__) || defined(__linux__)
        // Performance: Direct UTF-16 decode (SQLWCHAR is 2 bytes on
        // Linux/macOS)
        SQLWCHAR* wcharData = &buffers.wcharBuffers[col - 1][rowIdx * colInfo->fetchBufferSize];
        PyObject* pyStr = PyUnicode_DecodeUTF16(reinterpret_cast<const char*>(wcharData),
                                                numCharsInData * sizeof(SQLWCHAR),
                                                NULL,  // errors (use default strict)
                                                NULL   // byteorder (auto-detect)
        );
        if (pyStr) {
            PyList_SET_ITEM(row, col - 1, pyStr);
        } else {
            PyErr_Clear();  // Ignore decode error, return empty string
            PyObject* emptyStr = PyUnicode_FromStringAndSize("", 0);
            if (!emptyStr) {
                Py_INCREF(Py_None);
                PyList_SET_ITEM(row, col - 1, Py_None);
            } else {
                PyList_SET_ITEM(row, col - 1, emptyStr);
            }
        }
#else
        // Performance: Direct Python C API call (Windows where SQLWCHAR ==
        // wchar_t)
        PyObject* pyStr = PyUnicode_FromWideChar(
            reinterpret_cast<wchar_t*>(
                &buffers.wcharBuffers[col - 1][rowIdx * colInfo->fetchBufferSize]),
            numCharsInData);
        if (!pyStr) {
            Py_INCREF(Py_None);
            PyList_SET_ITEM(row, col - 1, Py_None);
        } else {
            PyList_SET_ITEM(row, col - 1, pyStr);
        }
#endif
    } else {
        // Slow path: LOB data requires separate fetch call
        PyList_SET_ITEM(row, col - 1,
                        FetchLobColumnData(hStmt, col, SQL_C_WCHAR, true, false).release().ptr());
    }
}

// Process SQL BINARY/VARBINARY (binary data) column into Python bytes
// Performance: NULL/NO_TOTAL checks removed - handled centrally before
// processor is called
inline void ProcessBinary(PyObject* row, ColumnBuffers& buffers, const void* colInfoPtr,
                          SQLUSMALLINT col, SQLULEN rowIdx, SQLHSTMT hStmt) {
    const ColumnInfoExt* colInfo = static_cast<const ColumnInfoExt*>(colInfoPtr);
    SQLLEN dataLen = buffers.indicators[col - 1][rowIdx];

    // Handle empty binary data
    if (dataLen == 0) {
        PyObject* emptyBytes = PyBytes_FromStringAndSize("", 0);
        if (!emptyBytes) {
            Py_INCREF(Py_None);
            PyList_SET_ITEM(row, col - 1, Py_None);
        } else {
            PyList_SET_ITEM(row, col - 1, emptyBytes);
        }
        return;
    }

    // Fast path: Data fits in buffer (not LOB or truncated)
    if (!colInfo->isLob && static_cast<size_t>(dataLen) <= colInfo->processedColumnSize) {
        // Performance: Direct Python C API call - create bytes from buffer
        PyObject* pyBytes = PyBytes_FromStringAndSize(
            reinterpret_cast<const char*>(
                &buffers.charBuffers[col - 1][rowIdx * colInfo->processedColumnSize]),
            dataLen);
        if (!pyBytes) {
            Py_INCREF(Py_None);
            PyList_SET_ITEM(row, col - 1, Py_None);
        } else {
            PyList_SET_ITEM(row, col - 1, pyBytes);
        }
    } else {
        // Slow path: LOB data requires separate fetch call
        PyList_SET_ITEM(
            row, col - 1,
            FetchLobColumnData(hStmt, col, SQL_C_BINARY, false, true, "").release().ptr());
    }
}

}  // namespace ColumnProcessors
