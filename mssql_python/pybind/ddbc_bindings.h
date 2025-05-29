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
#include <odbcss.h>

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

// BCP APIs (Bulk Copy Program)
// Typedefs (ensure these match the function signatures in odbcss.h)
typedef SQLRETURN (SQL_API* BCPInitWFunc)(SQLHDBC, LPCWSTR, LPCWSTR, LPCWSTR, INT);
typedef SQLRETURN (SQL_API* BCPControlWFunc)(SQLHDBC, INT, LPVOID);
typedef SQLRETURN (SQL_API* BCPControlAFunc)(SQLHDBC, INT, LPVOID); 
typedef SQLRETURN (SQL_API* BCPReadFmtWFunc)(SQLHDBC, LPCWSTR);
typedef SQLRETURN (SQL_API* BCPColumnsFunc)(SQLHDBC, INT);
typedef SQLRETURN (SQL_API* BCPColFmtWFunc)(SQLHDBC, INT, INT, INT, DBINT, LPCBYTE, INT, INT);
typedef SQLRETURN  (SQL_API* BCPExecFunc)(SQLHDBC, DBINT*); 
typedef SQLRETURN (SQL_API* BCPSetBulkModeFunc)(SQLHDBC, INT, LPCBYTE, INT, LPCBYTE, INT); 
typedef SQLRETURN (SQL_API* BCPDoneFunc)(SQLHDBC);

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
extern BCPControlAFunc BCPControlA_ptr;
extern BCPReadFmtWFunc BCPReadFmtW_ptr;
extern BCPColumnsFunc BCPColumns_ptr;
extern BCPColFmtWFunc BCPColFmtW_ptr;
extern BCPExecFunc BCPExec_ptr;
extern BCPDoneFunc BCPDone_ptr;
extern BCPSetBulkModeFunc BCPSetBulkMode_ptr;

// -- Logging utility --
template <typename... Args>
void LOG(const std::string& formatString, Args&&... args);


// Throws a std::runtime_error with the given message
void ThrowStdException(const std::string& message);

//-------------------------------------------------------------------------------------------------
// Loads the ODBC driver and resolves function pointers.
// Throws if loading or resolution fails.
//-------------------------------------------------------------------------------------------------
std::wstring LoadDriverOrThrowException();

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
