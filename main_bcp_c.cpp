#include <windows.h>
#include <sql.h>
#include <sqlext.h>
#include <odbcss.h>
#include <iostream>
#include <locale>
#include <codecvt>

void checkODBCError(RETCODE retcode, HDBC hdbc, const std::wstring& functionName) {
    if (retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO) {
        SQLWCHAR sqlState[6], message[256];
        SQLINTEGER nativeError;
        SQLSMALLINT textLength;
        SQLGetDiagRecW(SQL_HANDLE_DBC, hdbc, 1, sqlState, &nativeError, message, sizeof(message)/sizeof(SQLWCHAR), &textLength);
        std::wcerr << functionName << L" failed: " << message << std::endl;
        exit(EXIT_FAILURE);
    }
}

int main() {
    SQLHENV henv = SQL_NULL_HENV;
    SQLHDBC hdbc = SQL_NULL_HDBC;
    RETCODE retcode;

    // Allocate and set environment
    retcode = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
    retcode = SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0);
    retcode = SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);

    // Enable BCP
    retcode = SQLSetConnectAttr(hdbc, SQL_COPT_SS_BCP, (SQLPOINTER)SQL_BCP_ON, SQL_IS_INTEGER);
    checkODBCError(retcode, hdbc, L"SQLSetConnectAttr");

    // Connect
    SQLWCHAR connStr[] = L"";
    retcode = SQLDriverConnectW(hdbc, NULL, connStr, SQL_NTS, NULL, 0, NULL, SQL_DRIVER_NOPROMPT);
    checkODBCError(retcode, hdbc, L"SQLDriverConnect");

    std::wcout << L"Connected successfully." << std::endl;

    // Create table
    //SQLHSTMT hstmt;
    //retcode = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
    //checkODBCError(retcode, hdbc, L"SQLAllocHandle");

    //const wchar_t* createSQL =
    //    L"IF OBJECT_ID(N'dbo.bcp_wide_test', 'U') IS NOT NULL DROP TABLE dbo.bcp_wide_test;"
    //    L"CREATE TABLE dbo.bcp_wide_test (id VARCHAR(100), names VARCHAR(100));";
    //retcode = SQLExecDirectW(hstmt, (SQLWCHAR*)createSQL, SQL_NTS);
    //SQLFreeHandle(SQL_HANDLE_STMT, hstmt);

    //std::wcout << L"Table created." << std::endl;

    //// Prepare file paths
    //LPCWSTR table = L"[Employees].[dbo].[bcp_wide_test]";
    //LPCWSTR datafile = L"data_unicode.txt";         // UTF-16 LE encoded
    //LPCWSTR errorfile = L"bcp_wide_error.txt";

    //// Prepare file paths
    LPCWSTR table =  L"[TestBCP].[dbo].[EmployeeFullNames]";
    LPCWSTR datafile =  L"EmployeeFullNames.bcp";
    LPCWSTR errorfile = L"bcp_wide_error.txt";
    // Initialize BCP
    if (bcp_initW(hdbc, table, datafile, errorfile, DB_IN) == FAIL) {
        std::wcerr << L"bcp_initW failed" << std::endl;
        return 1;
    }

    // // Generate formate file
    // if (bcp_writefmtW(hdbc, L"TestBCP.fmt") == FAIL) {
    //     std::wcerr << L"bcp_writefmtW failed" << std::endl;
    //     return 1;
    // }
    // std::wcout << L"BCP format file created." << std::endl;

    // Read format file
    if (bcp_readfmtW(hdbc,  L"EmployeeFullNames.fmt") == FAIL) {
        std::wcerr << L"bcp_readfmtW failed" << std::endl;
        return 1;
    }


    // // Set batch size
    // DBINT batchSize = 1000;
    // bcp_control(hdbc, BCPBATCH, (void*)&batchSize);

    // // 2 columns: id, greeting
    // if (bcp_columns(hdbc, 2) == FAIL) {
    //     std::wcerr << L"bcp_columns failed" << std::endl;
    //     return 1;
    // }



    // // Define formats

    // // Column 1: ID (INT as string), delimiter ","
    // if (bcp_colfmt(hdbc, 1, SQLCHARACTER, 0, 4, (LPCBYTE)L" ", 2, 1) == FAIL) {
    //     std::wcerr << L"bcp_colfmt (id) failed" << std::endl;
    //     return 1;
    // }

    // // Column 2: Greeting (NVARCHAR), delimiter "\r\n"
    // if (bcp_colfmt(hdbc, 2, SQLCHARACTER, 0, 200, (LPCBYTE)L"\r\n", 4, 2) == FAIL) {
    //     std::wcerr << L"bcp_colfmt (greeting) failed" << std::endl;
    //     return 1;
    // }

    // Execute BCP
    DBINT rowsCopied = 0;
    if (bcp_exec(hdbc, &rowsCopied) == FAIL) {
        std::wcerr << L"bcp_exec failed" << std::endl;
        return 1;
    }

    std::wcout << L"Rows copied: " << rowsCopied << std::endl;

    // Done
    bcp_done(hdbc);
    SQLDisconnect(hdbc);
    SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
    SQLFreeHandle(SQL_HANDLE_ENV, henv);

    std::wcout << L"BCP completed using bcp_initW." << std::endl;
    return 0;
}

 