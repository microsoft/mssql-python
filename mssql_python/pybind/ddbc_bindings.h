// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

// INFO|TODO - Note that is file is Windows specific right now. Making it
//             arch agnostic will be taken up in future.

#pragma once

// pybind11.h must be the first include
#include <pybind11/pybind11.h>
#include <pybind11/chrono.h>
#include <pybind11/complex.h>
#include <pybind11/functional.h>
#include <pybind11/pytypes.h>  // Add this line for datetime support
#include <pybind11/stl.h>
#include <memory>
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

#if defined(_WIN32)
inline std::vector<SQLWCHAR> WStringToSQLWCHAR(const std::wstring& str) {
    std::vector<SQLWCHAR> result(str.begin(), str.end());
    result.push_back(0);
    return result;
}

inline std::wstring SQLWCHARToWString(const SQLWCHAR* sqlwStr,
                                      size_t length = SQL_NTS) {
    if (!sqlwStr) return std::wstring();

    if (length == SQL_NTS) {
        size_t i = 0;
        while (sqlwStr[i] != 0) ++i;
        length = i;
    }
    return std::wstring(reinterpret_cast<const wchar_t*>(sqlwStr), length);
}

#endif

#if defined(__APPLE__) || defined(__linux__)
#include <dlfcn.h>

// Unicode constants for surrogate ranges and max scalar value
constexpr uint32_t UNICODE_SURROGATE_HIGH_START = 0xD800;
constexpr uint32_t UNICODE_SURROGATE_HIGH_END   = 0xDBFF;
constexpr uint32_t UNICODE_SURROGATE_LOW_START  = 0xDC00;
constexpr uint32_t UNICODE_SURROGATE_LOW_END    = 0xDFFF;
constexpr uint32_t UNICODE_MAX_CODEPOINT        = 0x10FFFF;
constexpr uint32_t UNICODE_REPLACEMENT_CHAR     = 0xFFFD;

// Validate whether a code point is a legal Unicode scalar value
// (excludes surrogate halves and values beyond U+10FFFF)
inline bool IsValidUnicodeScalar(uint32_t cp) {
    return cp <= UNICODE_MAX_CODEPOINT &&
           !(cp >= UNICODE_SURROGATE_HIGH_START &&
             cp <= UNICODE_SURROGATE_LOW_END);
}

inline std::wstring SQLWCHARToWString(const SQLWCHAR* sqlwStr,
                                      size_t length = SQL_NTS) {
    if (!sqlwStr) return std::wstring();
    if (length == SQL_NTS) {
        size_t i = 0;
        while (sqlwStr[i] != 0) ++i;
        length = i;
    }
    std::wstring result;
    result.reserve(length);
    if constexpr (sizeof(SQLWCHAR) == 2) {
        // Use a manual increment to handle skipping
        for (size_t i = 0; i < length; ) {
            uint16_t wc = static_cast<uint16_t>(sqlwStr[i]);
            // Check for high surrogate and valid low surrogate
            if (wc >= UNICODE_SURROGATE_HIGH_START &&
                wc <= UNICODE_SURROGATE_HIGH_END && (i + 1 < length)) {
                uint16_t low = static_cast<uint16_t>(sqlwStr[i + 1]);
                if (low >= UNICODE_SURROGATE_LOW_START &&
                    low <= UNICODE_SURROGATE_LOW_END) {
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
                result.push_back(
                    static_cast<wchar_t>(UNICODE_REPLACEMENT_CHAR));
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
                result.push_back(
                    static_cast<wchar_t>(UNICODE_REPLACEMENT_CHAR));
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
                SQLWCHAR high = static_cast<SQLWCHAR>(
                    (cp >> 10) + UNICODE_SURROGATE_HIGH_START);
                SQLWCHAR low = static_cast<SQLWCHAR>(
                    (cp & 0x3FF) + UNICODE_SURROGATE_LOW_START);
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
                result.push_back(
                    static_cast<SQLWCHAR>(UNICODE_REPLACEMENT_CHAR));
            }
        }
    }
    result.push_back(0);  // null terminator
    return result;
}
#endif

#if defined(__APPLE__) || defined(__linux__)
#include "unix_utils.h"    // Unix-specific fixes
#include "unix_buffers.h"  // Unix-specific buffers
#endif

//-------------------------------------------------------------------------------------------------
// Function pointer typedefs
//-------------------------------------------------------------------------------------------------

// Handle APIs
typedef SQLRETURN (SQL_API* SQLAllocHandleFunc)(SQLSMALLINT, SQLHANDLE,
                                                 SQLHANDLE*);
typedef SQLRETURN (SQL_API* SQLSetEnvAttrFunc)(SQLHANDLE, SQLINTEGER,
                                                SQLPOINTER, SQLINTEGER);
typedef SQLRETURN (SQL_API* SQLSetConnectAttrFunc)(SQLHDBC, SQLINTEGER,
                                                    SQLPOINTER, SQLINTEGER);
typedef SQLRETURN (SQL_API* SQLSetStmtAttrFunc)(SQLHSTMT, SQLINTEGER,
                                                 SQLPOINTER, SQLINTEGER);
typedef SQLRETURN (SQL_API* SQLGetConnectAttrFunc)(SQLHDBC, SQLINTEGER,
                                                    SQLPOINTER, SQLINTEGER,
                                                    SQLINTEGER*);

// Connection and Execution APIs
typedef SQLRETURN (SQL_API* SQLDriverConnectFunc)(SQLHANDLE, SQLHWND,
                                                   SQLWCHAR*, SQLSMALLINT,
                                                   SQLWCHAR*, SQLSMALLINT,
                                                   SQLSMALLINT*,
                                                   SQLUSMALLINT);
typedef SQLRETURN (SQL_API* SQLExecDirectFunc)(SQLHANDLE, SQLWCHAR*,
                                                SQLINTEGER);
typedef SQLRETURN (SQL_API* SQLPrepareFunc)(SQLHANDLE, SQLWCHAR*,
                                             SQLINTEGER);
typedef SQLRETURN (SQL_API* SQLBindParameterFunc)(SQLHANDLE, SQLUSMALLINT,
                                                   SQLSMALLINT, SQLSMALLINT,
                                                   SQLSMALLINT, SQLULEN,
                                                   SQLSMALLINT, SQLPOINTER,
                                                   SQLLEN, SQLLEN*);
typedef SQLRETURN (SQL_API* SQLExecuteFunc)(SQLHANDLE);
typedef SQLRETURN (SQL_API* SQLRowCountFunc)(SQLHSTMT, SQLLEN*);
typedef SQLRETURN (SQL_API* SQLSetDescFieldFunc)(SQLHDESC, SQLSMALLINT,
                                                 SQLSMALLINT, SQLPOINTER,
                                                 SQLINTEGER);
typedef SQLRETURN (SQL_API* SQLGetStmtAttrFunc)(SQLHSTMT, SQLINTEGER,
                                                SQLPOINTER, SQLINTEGER,
                                                SQLINTEGER*);

// Data retrieval APIs
typedef SQLRETURN (SQL_API* SQLFetchFunc)(SQLHANDLE);
typedef SQLRETURN (SQL_API* SQLFetchScrollFunc)(SQLHANDLE, SQLSMALLINT,
                                                SQLLEN);
typedef SQLRETURN (SQL_API* SQLGetDataFunc)(SQLHANDLE, SQLUSMALLINT,
                                            SQLSMALLINT, SQLPOINTER,
                                            SQLLEN, SQLLEN*);
typedef SQLRETURN (SQL_API* SQLNumResultColsFunc)(SQLHSTMT, SQLSMALLINT*);
typedef SQLRETURN (SQL_API* SQLBindColFunc)(SQLHSTMT, SQLUSMALLINT,
                                            SQLSMALLINT, SQLPOINTER,
                                            SQLLEN, SQLLEN*);
typedef SQLRETURN (SQL_API* SQLDescribeColFunc)(SQLHSTMT, SQLUSMALLINT,
                                                SQLWCHAR*, SQLSMALLINT,
                                                SQLSMALLINT*, SQLSMALLINT*,
                                                SQLULEN*, SQLSMALLINT*,
                                                SQLSMALLINT*);
typedef SQLRETURN (SQL_API* SQLMoreResultsFunc)(SQLHSTMT);
typedef SQLRETURN (SQL_API* SQLColAttributeFunc)(SQLHSTMT, SQLUSMALLINT,
                                                 SQLUSMALLINT, SQLPOINTER,
                                                 SQLSMALLINT, SQLSMALLINT*,
                                                 SQLPOINTER);
typedef SQLRETURN (*SQLTablesFunc)(
    SQLHSTMT       StatementHandle,
    SQLWCHAR*      CatalogName,
    SQLSMALLINT    NameLength1,
    SQLWCHAR*      SchemaName,
    SQLSMALLINT    NameLength2,
    SQLWCHAR*      TableName,
    SQLSMALLINT    NameLength3,
    SQLWCHAR*      TableType,
    SQLSMALLINT    NameLength4
);
typedef SQLRETURN (SQL_API* SQLGetTypeInfoFunc)(SQLHSTMT, SQLSMALLINT);
typedef SQLRETURN (SQL_API* SQLProceduresFunc)(SQLHSTMT, SQLWCHAR*,
                                               SQLSMALLINT, SQLWCHAR*,
                                               SQLSMALLINT, SQLWCHAR*,
                                               SQLSMALLINT);
typedef SQLRETURN (SQL_API* SQLForeignKeysFunc)(SQLHSTMT, SQLWCHAR*,
                                                SQLSMALLINT, SQLWCHAR*,
                                                SQLSMALLINT, SQLWCHAR*,
                                                SQLSMALLINT, SQLWCHAR*,
                                                SQLSMALLINT, SQLWCHAR*,
                                                SQLSMALLINT, SQLWCHAR*,
                                                SQLSMALLINT);
typedef SQLRETURN (SQL_API* SQLPrimaryKeysFunc)(SQLHSTMT, SQLWCHAR*,
                                                SQLSMALLINT, SQLWCHAR*,
                                                SQLSMALLINT, SQLWCHAR*,
                                                SQLSMALLINT);
typedef SQLRETURN (SQL_API* SQLSpecialColumnsFunc)(SQLHSTMT, SQLUSMALLINT,
                                                   SQLWCHAR*, SQLSMALLINT,
                                                   SQLWCHAR*, SQLSMALLINT,
                                                   SQLWCHAR*, SQLSMALLINT,
                                                   SQLUSMALLINT,
                                                   SQLUSMALLINT);
typedef SQLRETURN (SQL_API* SQLStatisticsFunc)(SQLHSTMT, SQLWCHAR*,
                                               SQLSMALLINT, SQLWCHAR*,
                                               SQLSMALLINT, SQLWCHAR*,
                                               SQLSMALLINT, SQLUSMALLINT,
                                               SQLUSMALLINT);
typedef SQLRETURN (SQL_API* SQLColumnsFunc)(SQLHSTMT, SQLWCHAR*,
                                            SQLSMALLINT, SQLWCHAR*,
                                            SQLSMALLINT, SQLWCHAR*,
                                            SQLSMALLINT, SQLWCHAR*,
                                            SQLSMALLINT);
typedef SQLRETURN (SQL_API* SQLGetInfoFunc)(SQLHDBC, SQLUSMALLINT,
                                            SQLPOINTER, SQLSMALLINT,
                                            SQLSMALLINT*);

// Transaction APIs
typedef SQLRETURN (SQL_API* SQLEndTranFunc)(SQLSMALLINT, SQLHANDLE,
                                            SQLSMALLINT);

// Disconnect/free APIs
typedef SQLRETURN (SQL_API* SQLFreeHandleFunc)(SQLSMALLINT, SQLHANDLE);
typedef SQLRETURN (SQL_API* SQLDisconnectFunc)(SQLHDBC);
typedef SQLRETURN (SQL_API* SQLFreeStmtFunc)(SQLHSTMT, SQLUSMALLINT);

// Diagnostic APIs
typedef SQLRETURN (SQL_API* SQLGetDiagRecFunc)(SQLSMALLINT, SQLHANDLE,
                                               SQLSMALLINT, SQLWCHAR*,
                                               SQLINTEGER*, SQLWCHAR*,
                                               SQLSMALLINT, SQLSMALLINT*);

typedef SQLRETURN (SQL_API* SQLDescribeParamFunc)(SQLHSTMT, SQLUSMALLINT,
                                                  SQLSMALLINT*, SQLULEN*,
                                                  SQLSMALLINT*,
                                                  SQLSMALLINT*);

// DAE APIs
typedef SQLRETURN (SQL_API* SQLParamDataFunc)(SQLHSTMT, SQLPOINTER*);
typedef SQLRETURN (SQL_API* SQLPutDataFunc)(SQLHSTMT, SQLPOINTER, SQLLEN);
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

// Logging utility
template <typename... Args>
void LOG(const std::string& formatString, Args&&... args);

// Throws a std::runtime_error with the given message
void ThrowStdException(const std::string& message);

// Define a platform-agnostic type for the driver handle
#ifdef _WIN32
typedef HMODULE DriverHandle;
#else
typedef void* DriverHandle;
#endif

// Platform-agnostic function to get a function pointer from the loaded library
template<typename T>
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
#if defined(_WIN32)
    std::wstring sqlState;
    std::wstring ddbcErrorMsg;
#else
    // On Unix, store as UTF-8 std::string to avoid wstring conversion issues
    std::string sqlState_utf8;
    std::string ddbcErrorMsg_utf8;
    
    // Provide accessor methods for Python bindings
    std::string get_sqlState() const { return sqlState_utf8; }
    std::string get_ddbcErrorMsg() const { return ddbcErrorMsg_utf8; }
#endif
};
ErrorInfo SQLCheckError_Wrap(SQLSMALLINT handleType, SqlHandlePtr handle,
                              SQLRETURN retcode);

inline std::string WideToUTF8(const std::wstring& wstr) {
    if (wstr.empty()) return {};

#if defined(_WIN32)
    int size_needed = WideCharToMultiByte(CP_UTF8, 0, wstr.data(),
                                          static_cast<int>(wstr.size()),
                                          nullptr, 0, nullptr, nullptr);
    if (size_needed == 0) return {};
    std::string result(size_needed, 0);
    int converted = WideCharToMultiByte(CP_UTF8, 0, wstr.data(),
                                        static_cast<int>(wstr.size()),
                                        result.data(), size_needed,
                                        nullptr, nullptr);
    if (converted == 0) return {};
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
            utf8_string += static_cast<char>(0xE0 |
                                            ((code_point >> 12) & 0x0F));
            utf8_string += static_cast<char>(0x80 |
                                            ((code_point >> 6) & 0x3F));
            utf8_string += static_cast<char>(0x80 | (code_point & 0x3F));
        } else if (code_point <= 0x10FFFF) {
            // 4-byte UTF-8 sequence for characters like emojis (e.g., U+1F604)
            utf8_string += static_cast<char>(0xF0 |
                                            ((code_point >> 18) & 0x07));
            utf8_string += static_cast<char>(0x80 |
                                            ((code_point >> 12) & 0x3F));
            utf8_string += static_cast<char>(0x80 |
                                            ((code_point >> 6) & 0x3F));
            utf8_string += static_cast<char>(0x80 | (code_point & 0x3F));
        }
    }
    return utf8_string;
#endif
}

inline std::wstring Utf8ToWString(const std::string& str) {
    if (str.empty()) return {};
#if defined(_WIN32)
    int size_needed = MultiByteToWideChar(CP_UTF8, 0, str.data(),
                                          static_cast<int>(str.size()),
                                          nullptr, 0);
    if (size_needed == 0) {
        LOG("MultiByteToWideChar failed.");
        return {};
    }
    std::wstring result(size_needed, 0);
    int converted = MultiByteToWideChar(CP_UTF8, 0, str.data(),
                                        static_cast<int>(str.size()),
                                        result.data(), size_needed);
    if (converted == 0) return {};
    return result;
#else
    std::wstring_convert<std::codecvt_utf8_utf16<wchar_t>> converter;
    return converter.from_bytes(str);
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
