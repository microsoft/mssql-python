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

#if defined(_WIN32)
inline std::vector<SQLWCHAR> WStringToSQLWCHAR(const std::wstring& str) {
    std::vector<SQLWCHAR> result(str.begin(), str.end());
    result.push_back(0);
    return result;
}

inline std::wstring SQLWCHARToWString(const SQLWCHAR* sqlwStr, size_t length = SQL_NTS) {
    if (!sqlwStr)
        return std::wstring();

    if (length == SQL_NTS) {
        size_t i = 0;
        while (sqlwStr[i] != 0)
            ++i;
        length = i;
    }
    return std::wstring(reinterpret_cast<const wchar_t*>(sqlwStr), length);
}

#endif

#if defined(__APPLE__) || defined(__linux__)
#include <dlfcn.h>

// Unicode constants for surrogate ranges and max scalar value
constexpr uint32_t UNICODE_SURROGATE_HIGH_START = 0xD800;
constexpr uint32_t UNICODE_SURROGATE_HIGH_END = 0xDBFF;
constexpr uint32_t UNICODE_SURROGATE_LOW_START = 0xDC00;
constexpr uint32_t UNICODE_SURROGATE_LOW_END = 0xDFFF;
constexpr uint32_t UNICODE_MAX_CODEPOINT = 0x10FFFF;
constexpr uint32_t UNICODE_REPLACEMENT_CHAR = 0xFFFD;

// Validate whether a code point is a legal Unicode scalar value
// (excludes surrogate halves and values beyond U+10FFFF)
inline bool IsValidUnicodeScalar(uint32_t cp) {
    return cp <= UNICODE_MAX_CODEPOINT &&
           !(cp >= UNICODE_SURROGATE_HIGH_START && cp <= UNICODE_SURROGATE_LOW_END);
}

inline std::wstring SQLWCHARToWString(const SQLWCHAR* sqlwStr, size_t length = SQL_NTS) {
    if (!sqlwStr)
        return std::wstring();
    if (length == SQL_NTS) {
        size_t i = 0;
        while (sqlwStr[i] != 0)
            ++i;
        length = i;
    }
    std::wstring result;
    result.reserve(length);
    if constexpr (sizeof(SQLWCHAR) == 2) {
        // Use a manual increment to handle skipping
        for (size_t i = 0; i < length;) {
            uint16_t wc = static_cast<uint16_t>(sqlwStr[i]);
            // Check for high surrogate and valid low surrogate
            if (wc >= UNICODE_SURROGATE_HIGH_START && wc <= UNICODE_SURROGATE_HIGH_END &&
                (i + 1 < length)) {
                uint16_t low = static_cast<uint16_t>(sqlwStr[i + 1]);
                if (low >= UNICODE_SURROGATE_LOW_START && low <= UNICODE_SURROGATE_LOW_END) {
                    // Combine into a single code point
                    uint32_t cp = (((wc - UNICODE_SURROGATE_HIGH_START) << 10) |
                                   (low - UNICODE_SURROGATE_LOW_START)) +
                                  0x10000;
                    result.push_back(static_cast<wchar_t>(cp));
                    i += 2;  // Move past both surrogates
                    continue;
                }
            }
            // If we reach here, it's not a valid surrogate pair or is a BMP
            // character. Check if it's a valid scalar and append, otherwise
            // append replacement char.
            if (IsValidUnicodeScalar(wc)) {
                result.push_back(static_cast<wchar_t>(wc));
            } else {
                result.push_back(static_cast<wchar_t>(UNICODE_REPLACEMENT_CHAR));
            }
            ++i;  // Move to the next code unit
        }
    } else {
        // SQLWCHAR is UTF-32, so just copy with validation
        for (size_t i = 0; i < length; ++i) {
            uint32_t cp = static_cast<uint32_t>(sqlwStr[i]);
            if (IsValidUnicodeScalar(cp)) {
                result.push_back(static_cast<wchar_t>(cp));
            } else {
                result.push_back(static_cast<wchar_t>(UNICODE_REPLACEMENT_CHAR));
            }
        }
    }
    return result;
}

inline std::vector<SQLWCHAR> WStringToSQLWCHAR(const std::wstring& str) {
    std::vector<SQLWCHAR> result;
    result.reserve(str.size() + 2);
    if constexpr (sizeof(SQLWCHAR) == 2) {
        // Encode UTF-32 to UTF-16
        for (wchar_t wc : str) {
            uint32_t cp = static_cast<uint32_t>(wc);
            if (!IsValidUnicodeScalar(cp)) {
                cp = UNICODE_REPLACEMENT_CHAR;
            }
            if (cp <= 0xFFFF) {
                // Fits in a single UTF-16 code unit
                result.push_back(static_cast<SQLWCHAR>(cp));
            } else {
                // Encode as surrogate pair
                cp -= 0x10000;
                SQLWCHAR high = static_cast<SQLWCHAR>((cp >> 10) + UNICODE_SURROGATE_HIGH_START);
                SQLWCHAR low = static_cast<SQLWCHAR>((cp & 0x3FF) + UNICODE_SURROGATE_LOW_START);
                result.push_back(high);
                result.push_back(low);
            }
        }
    } else {
        // Encode UTF-32 directly
        for (wchar_t wc : str) {
            uint32_t cp = static_cast<uint32_t>(wc);
            if (IsValidUnicodeScalar(cp)) {
                result.push_back(static_cast<SQLWCHAR>(cp));
            } else {
                result.push_back(static_cast<SQLWCHAR>(UNICODE_REPLACEMENT_CHAR));
            }
        }
    }
    result.push_back(0);  // null terminator
    return result;
}
#endif

#if defined(__APPLE__) || defined(__linux__)
#include "unix_utils.h"  // Unix-specific fixes
#endif

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

  private:
    SQLSMALLINT _type;
    SQLHANDLE _handle;
};
using SqlHandlePtr = std::shared_ptr<SqlHandle>;

// This struct is used to relay error info obtained from SQLDiagRec API to the
// Python module
struct ErrorInfo {
    std::wstring sqlState;
    std::wstring ddbcErrorMsg;
};
ErrorInfo SQLCheckError_Wrap(SQLSMALLINT handleType, SqlHandlePtr handle, SQLRETURN retcode);

inline std::string WideToUTF8(const std::wstring& wstr) {
    if (wstr.empty())
        return {};

#if defined(_WIN32)
    int size_needed = WideCharToMultiByte(CP_UTF8, 0, wstr.data(), static_cast<int>(wstr.size()),
                                          nullptr, 0, nullptr, nullptr);
    if (size_needed == 0)
        return {};
    std::string result(size_needed, 0);
    int converted = WideCharToMultiByte(CP_UTF8, 0, wstr.data(), static_cast<int>(wstr.size()),
                                        result.data(), size_needed, nullptr, nullptr);
    if (converted == 0)
        return {};
    return result;
#else
    // Manual UTF-32 to UTF-8 conversion for macOS/Linux
    std::string utf8_string;
    // Reserve enough space for worst case (4 bytes per character)
    utf8_string.reserve(wstr.size() * 4);

    for (wchar_t wc : wstr) {
        uint32_t code_point = static_cast<uint32_t>(wc);

        if (code_point <= 0x7F) {
            // 1-byte UTF-8 sequence for ASCII characters
            utf8_string += static_cast<char>(code_point);
        } else if (code_point <= 0x7FF) {
            // 2-byte UTF-8 sequence
            utf8_string += static_cast<char>(0xC0 | ((code_point >> 6) & 0x1F));
            utf8_string += static_cast<char>(0x80 | (code_point & 0x3F));
        } else if (code_point <= 0xFFFF) {
            // 3-byte UTF-8 sequence
            utf8_string += static_cast<char>(0xE0 | ((code_point >> 12) & 0x0F));
            utf8_string += static_cast<char>(0x80 | ((code_point >> 6) & 0x3F));
            utf8_string += static_cast<char>(0x80 | (code_point & 0x3F));
        } else if (code_point <= 0x10FFFF) {
            // 4-byte UTF-8 sequence for characters like emojis (e.g., U+1F604)
            utf8_string += static_cast<char>(0xF0 | ((code_point >> 18) & 0x07));
            utf8_string += static_cast<char>(0x80 | ((code_point >> 12) & 0x3F));
            utf8_string += static_cast<char>(0x80 | ((code_point >> 6) & 0x3F));
            utf8_string += static_cast<char>(0x80 | (code_point & 0x3F));
        }
    }
    return utf8_string;
#endif
}

inline std::wstring Utf8ToWString(const std::string& str) {
    if (str.empty())
        return {};
#if defined(_WIN32)
    int size_needed =
        MultiByteToWideChar(CP_UTF8, 0, str.data(), static_cast<int>(str.size()), nullptr, 0);
    if (size_needed == 0) {
        LOG_ERROR("MultiByteToWideChar failed for UTF8 to wide string conversion");
        return {};
    }
    std::wstring result(size_needed, 0);
    int converted = MultiByteToWideChar(CP_UTF8, 0, str.data(), static_cast<int>(str.size()),
                                        result.data(), size_needed);
    if (converted == 0)
        return {};
    return result;
#else
    // Optimized UTF-8 to UTF-32 conversion (wstring on Unix)
    if (str.empty())
        return {};

    // Lambda to decode UTF-8 multi-byte sequences
    constexpr auto decodeUtf8 = [](const unsigned char* data, size_t& i, size_t len) -> wchar_t {
        unsigned char byte = data[i];

        // 1-byte sequence (ASCII): 0xxxxxxx
        if (byte <= 0x7F) {
            ++i;
            return static_cast<wchar_t>(byte);
        }
        // 2-byte sequence: 110xxxxx 10xxxxxx
        if ((byte & 0xE0) == 0xC0 && i + 1 < len) {
            uint32_t cp = ((static_cast<uint32_t>(byte & 0x1F) << 6) | (data[i + 1] & 0x3F));
            i += 2;
            return static_cast<wchar_t>(cp);
        }
        // 3-byte sequence: 1110xxxx 10xxxxxx 10xxxxxx
        if ((byte & 0xF0) == 0xE0 && i + 2 < len) {
            uint32_t cp = ((static_cast<uint32_t>(byte & 0x0F) << 12) |
                           ((data[i + 1] & 0x3F) << 6) | (data[i + 2] & 0x3F));
            i += 3;
            return static_cast<wchar_t>(cp);
        }
        // 4-byte sequence: 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
        if ((byte & 0xF8) == 0xF0 && i + 3 < len) {
            uint32_t cp =
                ((static_cast<uint32_t>(byte & 0x07) << 18) | ((data[i + 1] & 0x3F) << 12) |
                 ((data[i + 2] & 0x3F) << 6) | (data[i + 3] & 0x3F));
            i += 4;
            return static_cast<wchar_t>(cp);
        }
        // Invalid sequence - skip byte
        ++i;
        return 0xFFFD;  // Unicode replacement character
    };

    std::wstring result;
    result.reserve(str.size());  // Reserve assuming mostly ASCII

    const unsigned char* data = reinterpret_cast<const unsigned char*>(str.data());
    const size_t len = str.size();
    size_t i = 0;

    // Fast path for ASCII-only prefix (most common case)
    while (i < len && data[i] <= 0x7F) {
        result.push_back(static_cast<wchar_t>(data[i]));
        ++i;
    }

    // Handle remaining multi-byte sequences
    while (i < len) {
        wchar_t wc = decodeUtf8(data, i, len);
        if (wc != 0xFFFD || data[i - 1] >= 0x80) {  // Skip invalid sequences
            result.push_back(wc);
        }
    }

    return result;
#endif
}

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
    std::vector<std::vector<SQL_TIME_STRUCT>> timeBuffers;
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
};

// Forward declare FetchLobColumnData (defined in ddbc_bindings.cpp) - MUST be
// outside namespace
py::object FetchLobColumnData(SQLHSTMT hStmt, SQLUSMALLINT col, SQLSMALLINT cType, bool isWideChar,
                              bool isBinary);

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
        // Performance: Direct Python C API call - create string from buffer
        PyObject* pyStr = PyUnicode_FromStringAndSize(
            reinterpret_cast<char*>(
                &buffers.charBuffers[col - 1][rowIdx * colInfo->fetchBufferSize]),
            numCharsInData);
        if (!pyStr) {
            Py_INCREF(Py_None);
            PyList_SET_ITEM(row, col - 1, Py_None);
        } else {
            PyList_SET_ITEM(row, col - 1, pyStr);
        }
    } else {
        // Slow path: LOB data requires separate fetch call
        PyList_SET_ITEM(row, col - 1,
                        FetchLobColumnData(hStmt, col, SQL_C_CHAR, false, false).release().ptr());
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
        PyList_SET_ITEM(row, col - 1,
                        FetchLobColumnData(hStmt, col, SQL_C_BINARY, false, true).release().ptr());
    }
}

}  // namespace ColumnProcessors
