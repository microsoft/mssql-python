// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

// INFO|TODO - Note that is file is Windows specific right now. Making it arch agnostic will be
//             taken up in future.

#pragma once

#include <pybind11/pybind11.h> // pybind11.h must be the first include - https://pybind11.readthedocs.io/en/latest/basics.html#header-and-namespace-conventions

#include <Windows.h>
#include <string>
#include <sql.h>
#include <sqlext.h>
#include <memory>
#include <mutex>
#include <odbcss.h>

#include <pybind11/chrono.h>
#include <pybind11/complex.h>
#include <pybind11/functional.h>
#include <pybind11/pytypes.h>  // Add this line for datetime support
#include <pybind11/stl.h>
namespace py = pybind11;
using namespace pybind11::literals;

#include <string>
#include <memory>
#include <mutex>

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

#if defined(__APPLE__)
    // macOS-specific headers
    #include <dlfcn.h>

    inline std::wstring SQLWCHARToWString(const SQLWCHAR* sqlwStr, size_t length = SQL_NTS) {
        if (!sqlwStr) return std::wstring();

        if (length == SQL_NTS) {
            size_t i = 0;
            while (sqlwStr[i] != 0) ++i;
            length = i;
        }

        std::wstring result;
        result.reserve(length);
        for (size_t i = 0; i < length; ++i) {
            result.push_back(static_cast<wchar_t>(sqlwStr[i]));
        }
        return result;
    }

    inline std::vector<SQLWCHAR> WStringToSQLWCHAR(const std::wstring& str) {
        std::vector<SQLWCHAR> result(str.size() + 1, 0);  // +1 for null terminator
        for (size_t i = 0; i < str.size(); ++i) {
            result[i] = static_cast<SQLWCHAR>(str[i]);
        }
        return result;
    }
#endif

#if defined(__APPLE__)
#include "mac_utils.h"  // For macOS-specific Unicode encoding fixes
#include "mac_buffers.h"  // For macOS-specific buffer handling
#endif

//-------------------------------------------------------------------------------------------------
// Function pointer typedefs
//-------------------------------------------------------------------------------------------------

// Handle APIs
typedef SQLRETURN (SQL_API* SQLAllocHandleFunc)(SQLSMALLINT, SQLHANDLE, SQLHANDLE*);
typedef SQLRETURN (SQL_API* SQLSetEnvAttrFunc)(SQLHANDLE, SQLINTEGER, SQLPOINTER, SQLINTEGER);
typedef SQLRETURN (SQL_API* SQLSetConnectAttrFunc)(SQLHDBC, SQLINTEGER, SQLPOINTER, SQLINTEGER);
typedef SQLRETURN (SQL_API* SQLSetStmtAttrFunc)(SQLHSTMT, SQLINTEGER, SQLPOINTER, SQLINTEGER);
typedef SQLRETURN (SQL_API* SQLGetConnectAttrFunc)(SQLHDBC, SQLINTEGER, SQLPOINTER, SQLINTEGER, SQLINTEGER*);

// Connection and Execution APIs
typedef SQLRETURN (SQL_API* SQLDriverConnectFunc)(SQLHANDLE, SQLHWND, SQLWCHAR*, SQLSMALLINT, SQLWCHAR*,
                                          SQLSMALLINT, SQLSMALLINT*, SQLUSMALLINT);
typedef SQLRETURN (SQL_API* SQLExecDirectFunc)(SQLHANDLE, SQLWCHAR*, SQLINTEGER);
typedef SQLRETURN (SQL_API* SQLPrepareFunc)(SQLHANDLE, SQLWCHAR*, SQLINTEGER);
typedef SQLRETURN (SQL_API* SQLBindParameterFunc)(SQLHANDLE, SQLUSMALLINT, SQLSMALLINT, SQLSMALLINT,
                                          SQLSMALLINT, SQLULEN, SQLSMALLINT, SQLPOINTER, SQLLEN,
                                          SQLLEN*);
typedef SQLRETURN (SQL_API* SQLExecuteFunc)(SQLHANDLE);
typedef SQLRETURN (SQL_API* SQLRowCountFunc)(SQLHSTMT, SQLLEN*);
typedef SQLRETURN (SQL_API* SQLSetDescFieldFunc)(SQLHDESC, SQLSMALLINT, SQLSMALLINT, SQLPOINTER, SQLINTEGER);
typedef SQLRETURN (SQL_API* SQLGetStmtAttrFunc)(SQLHSTMT, SQLINTEGER, SQLPOINTER, SQLINTEGER, SQLINTEGER*);

// BCP APIs (Bulk Copy Program)
// Typedefs (ensure these match the function signatures in odbcss.h)
typedef SQLRETURN (SQL_API* BCPInitWFunc)(SQLHDBC, LPCWSTR, LPCWSTR, LPCWSTR, INT);
typedef SQLRETURN (SQL_API* BCPControlWFunc)(SQLHDBC, INT, LPVOID);
// typedef SQLRETURN (SQL_API* BCPControlAFunc)(SQLHDBC, INT, LPVOID); 
typedef SQLRETURN (SQL_API* BCPReadFmtWFunc)(SQLHDBC, LPCWSTR);
typedef SQLRETURN (SQL_API* BCPColumnsFunc)(SQLHDBC, INT);
typedef SQLRETURN (SQL_API* BCPColFmtWFunc)(SQLHDBC, INT, INT, INT, DBINT, LPCBYTE, INT, INT);
typedef SQLRETURN  (SQL_API* BCPExecFunc)(SQLHDBC, DBINT*); 
typedef SQLRETURN (SQL_API* BCPDoneFunc)(SQLHDBC);

// Data retrieval APIs
typedef SQLRETURN (SQL_API* SQLFetchFunc)(SQLHANDLE);
typedef SQLRETURN (SQL_API* SQLFetchScrollFunc)(SQLHANDLE, SQLSMALLINT, SQLLEN);
typedef SQLRETURN (SQL_API* SQLGetDataFunc)(SQLHANDLE, SQLUSMALLINT, SQLSMALLINT, SQLPOINTER, SQLLEN,
                                    SQLLEN*);
typedef SQLRETURN (SQL_API* SQLNumResultColsFunc)(SQLHSTMT, SQLSMALLINT*);
typedef SQLRETURN (SQL_API* SQLBindColFunc)(SQLHSTMT, SQLUSMALLINT, SQLSMALLINT, SQLPOINTER, SQLLEN,
                                    SQLLEN*);
typedef SQLRETURN (SQL_API* SQLDescribeColFunc)(SQLHSTMT, SQLUSMALLINT, SQLWCHAR*, SQLSMALLINT,
                                        SQLSMALLINT*, SQLSMALLINT*, SQLULEN*, SQLSMALLINT*,
                                        SQLSMALLINT*);
typedef SQLRETURN (SQL_API* SQLMoreResultsFunc)(SQLHSTMT);
typedef SQLRETURN (SQL_API* SQLColAttributeFunc)(SQLHSTMT, SQLUSMALLINT, SQLUSMALLINT, SQLPOINTER,
                                         SQLSMALLINT, SQLSMALLINT*, SQLPOINTER);

// Transaction APIs
typedef SQLRETURN (SQL_API* SQLEndTranFunc)(SQLSMALLINT, SQLHANDLE, SQLSMALLINT);

// Disconnect/free APIs
typedef SQLRETURN (SQL_API* SQLFreeHandleFunc)(SQLSMALLINT, SQLHANDLE);
typedef SQLRETURN (SQL_API* SQLDisconnectFunc)(SQLHDBC);
typedef SQLRETURN (SQL_API* SQLFreeStmtFunc)(SQLHSTMT, SQLUSMALLINT);

// Diagnostic APIs
typedef SQLRETURN (SQL_API* SQLGetDiagRecFunc)(SQLSMALLINT, SQLHANDLE, SQLSMALLINT, SQLWCHAR*, SQLINTEGER*,
                                       SQLWCHAR*, SQLSMALLINT, SQLSMALLINT*);

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

// Transaction APIs
extern SQLEndTranFunc SQLEndTran_ptr;

// Disconnect/free APIs
extern SQLFreeHandleFunc SQLFreeHandle_ptr;
extern SQLDisconnectFunc SQLDisconnect_ptr;
extern SQLFreeStmtFunc SQLFreeStmt_ptr;

// Diagnostic APIs
extern SQLGetDiagRecFunc SQLGetDiagRec_ptr;

// BCP APIs (Bulk Copy Program)
// Extern function pointer declarations for BCP APIs
extern BCPInitWFunc BCPInitW_ptr;
extern BCPControlWFunc BCPControlW_ptr;
extern BCPReadFmtWFunc BCPReadFmtW_ptr;
extern BCPColumnsFunc BCPColumns_ptr;
extern BCPColFmtWFunc BCPColFmtW_ptr;
extern BCPExecFunc BCPExec_ptr;
extern BCPDoneFunc BCPDone_ptr;

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
// Ensures the ODBC driver and all function pointers are loaded exactly once across the process.
// This avoids redundant work and ensures thread-safe, centralized initialization.
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

// This struct is used to relay error info obtained from SQLDiagRec API to the Python module
struct ErrorInfo {
    std::wstring sqlState;
    std::wstring ddbcErrorMsg;
};
ErrorInfo SQLCheckError_Wrap(SQLSMALLINT handleType, SqlHandlePtr handle, SQLRETURN retcode);

inline std::string WideToUTF8(const std::wstring& wstr) {
    if (wstr.empty()) return {};
#if defined(_WIN32)
    int size_needed = WideCharToMultiByte(CP_UTF8, 0, wstr.data(), static_cast<int>(wstr.size()), nullptr, 0, nullptr, nullptr);
    if (size_needed == 0) return {};
    std::string result(size_needed, 0);
    int converted = WideCharToMultiByte(CP_UTF8, 0, wstr.data(), static_cast<int>(wstr.size()), result.data(), size_needed, nullptr, nullptr);
    if (converted == 0) return {};
    return result;
#else
    std::wstring_convert<std::codecvt_utf8_utf16<wchar_t>> converter;
    return converter.to_bytes(wstr);
#endif
}

inline std::wstring Utf8ToWString(const std::string& str) {
    if (str.empty()) return {};
#if defined(_WIN32)
    int size_needed = MultiByteToWideChar(CP_UTF8, 0, str.data(), static_cast<int>(str.size()), nullptr, 0);
    if (size_needed == 0) {
        LOG("MultiByteToWideChar failed.");
        return {};
    }
    std::wstring result(size_needed, 0);
    int converted = MultiByteToWideChar(CP_UTF8, 0, str.data(), static_cast<int>(str.size()), result.data(), size_needed);
    if (converted == 0) return {};
    return result;
#else
    std::wstring_convert<std::codecvt_utf8_utf16<wchar_t>> converter;
    return converter.from_bytes(str);
#endif
}
