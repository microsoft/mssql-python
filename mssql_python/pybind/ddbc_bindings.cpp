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
// Include cstring for strnlen
#include <cstring>
// Replace std::filesystem usage with Windows-specific headers
#include <shlwapi.h>
#pragma comment(lib, "shlwapi.lib")
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

// Architecture-specific defines
#ifndef ARCHITECTURE
  #if defined(_WIN64)
    #define ARCHITECTURE "x64"
  #else
    #define ARCHITECTURE "x86"
  #endif
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

// Move GetModuleDirectory outside namespace to resolve ambiguity
std::string GetModuleDirectory() {
    py::object module = py::module::import("mssql_python");
    py::object module_path = module.attr("__file__");
    std::string module_file = module_path.cast<std::string>();
    
    char path[MAX_PATH];
    strncpy_s(path, MAX_PATH, module_file.c_str(), module_file.length());
    PathRemoveFileSpecA(path);
    return std::string(path);
}

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
std::wstring LoadDriverOrThrowException(const std::wstring& modulePath = L"") {
    std::cout << "[DDBC] Entered LoadDriverOrThrowException" << std::endl;
    std::wstring ddbcModulePath = modulePath;
    if (ddbcModulePath.empty()) {
        std::cout << "[DDBC] Getting module directory..." << std::endl;
        std::string path = GetModuleDirectory();
        std::cout << "[DDBC] Module directory: " << path << std::endl;
        ddbcModulePath = std::wstring(path.begin(), path.end());
    }

    std::wstring dllDir = ddbcModulePath;
    dllDir += L"\\libs\\";
    std::cout << "[DDBC] dllDir after libs: " << std::string(dllDir.begin(), dllDir.end()) << std::endl;
    std::wstring archStr(ARCHITECTURE, ARCHITECTURE + strlen(ARCHITECTURE));
    std::cout << "[DDBC] ARCHITECTURE macro at runtime: " << std::string(archStr.begin(), archStr.end()) << std::endl;
    std::wstring archDir;
    if (archStr == L"x64" || archStr == L"win64" || archStr == L"amd64") {
        archDir = L"x64";
    } else if (archStr == L"x86" || archStr == L"win32" || archStr == L"i386") {
        archDir = L"x86";
    } else if (archStr == L"arm64") {
        archDir = L"arm64";
    } else {
        archDir = archStr;
    }
    dllDir += archDir;
    dllDir += L"\\msodbcsql18.dll";
    std::string dllDirStr(dllDir.begin(), dllDir.end());
    std::cout << "[DDBC] Attempting to load driver from: " << dllDirStr << std::endl;
    HMODULE hModule = LoadLibraryW(dllDir.c_str());
    if (!hModule) {
        std::cout << "[DDBC] LoadLibraryW failed!" << std::endl;
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
        std::cout << "[DDBC] Failed to load the driver with error code: " << error << " - " << errorMessage << std::endl;
        ThrowStdException("Failed to load the ODBC driver. Please check that it is installed correctly.");
    }
    std::cout << "[DDBC] DLL loaded successfully!" << std::endl;
    // If we got here, we've successfully loaded the DLL. Now get the function pointers.
    // Environment and handle function loading
    SQLAllocHandle_ptr = (SQLAllocHandleFunc)GetProcAddress(hModule, "SQLAllocHandle");
    std::cout << "[DDBC] SQLAllocHandle_ptr: " << (void*)SQLAllocHandle_ptr << std::endl;
    SQLSetEnvAttr_ptr = (SQLSetEnvAttrFunc)GetProcAddress(hModule, "SQLSetEnvAttr");
    std::cout << "[DDBC] SQLSetEnvAttr_ptr: " << (void*)SQLSetEnvAttr_ptr << std::endl;
    SQLSetConnectAttr_ptr = (SQLSetConnectAttrFunc)GetProcAddress(hModule, "SQLSetConnectAttrW");
    std::cout << "[DDBC] SQLSetConnectAttr_ptr: " << (void*)SQLSetConnectAttr_ptr << std::endl;
    SQLSetStmtAttr_ptr = (SQLSetStmtAttrFunc)GetProcAddress(hModule, "SQLSetStmtAttrW");
    std::cout << "[DDBC] SQLSetStmtAttr_ptr: " << (void*)SQLSetStmtAttr_ptr << std::endl;
    SQLGetConnectAttr_ptr = (SQLGetConnectAttrFunc)GetProcAddress(hModule, "SQLGetConnectAttrW");
    std::cout << "[DDBC] SQLGetConnectAttr_ptr: " << (void*)SQLGetConnectAttr_ptr << std::endl;

    // Connection and statement function loading
    SQLDriverConnect_ptr = (SQLDriverConnectFunc)GetProcAddress(hModule, "SQLDriverConnectW");
    std::cout << "[DDBC] SQLDriverConnect_ptr: " << (void*)SQLDriverConnect_ptr << std::endl;
    SQLExecDirect_ptr = (SQLExecDirectFunc)GetProcAddress(hModule, "SQLExecDirectW");
    std::cout << "[DDBC] SQLExecDirect_ptr: " << (void*)SQLExecDirect_ptr << std::endl;
    SQLPrepare_ptr = (SQLPrepareFunc)GetProcAddress(hModule, "SQLPrepareW");
    std::cout << "[DDBC] SQLPrepare_ptr: " << (void*)SQLPrepare_ptr << std::endl;
    SQLBindParameter_ptr = (SQLBindParameterFunc)GetProcAddress(hModule, "SQLBindParameter");
    std::cout << "[DDBC] SQLBindParameter_ptr: " << (void*)SQLBindParameter_ptr << std::endl;
    SQLExecute_ptr = (SQLExecuteFunc)GetProcAddress(hModule, "SQLExecute");
    std::cout << "[DDBC] SQLExecute_ptr: " << (void*)SQLExecute_ptr << std::endl;
    SQLRowCount_ptr = (SQLRowCountFunc)GetProcAddress(hModule, "SQLRowCount");
    std::cout << "[DDBC] SQLRowCount_ptr: " << (void*)SQLRowCount_ptr << std::endl;
    SQLGetStmtAttr_ptr = (SQLGetStmtAttrFunc)GetProcAddress(hModule, "SQLGetStmtAttrW");
    std::cout << "[DDBC] SQLGetStmtAttr_ptr: " << (void*)SQLGetStmtAttr_ptr << std::endl;
    SQLSetDescField_ptr = (SQLSetDescFieldFunc)GetProcAddress(hModule, "SQLSetDescFieldW");
    std::cout << "[DDBC] SQLSetDescField_ptr: " << (void*)SQLSetDescField_ptr << std::endl;

    // Fetch and data retrieval function loading
    SQLFetch_ptr = (SQLFetchFunc)GetProcAddress(hModule, "SQLFetch");
    std::cout << "[DDBC] SQLFetch_ptr: " << (void*)SQLFetch_ptr << std::endl;
    SQLFetchScroll_ptr = (SQLFetchScrollFunc)GetProcAddress(hModule, "SQLFetchScroll");
    std::cout << "[DDBC] SQLFetchScroll_ptr: " << (void*)SQLFetchScroll_ptr << std::endl;
    SQLGetData_ptr = (SQLGetDataFunc)GetProcAddress(hModule, "SQLGetData");
    std::cout << "[DDBC] SQLGetData_ptr: " << (void*)SQLGetData_ptr << std::endl;
    SQLNumResultCols_ptr = (SQLNumResultColsFunc)GetProcAddress(hModule, "SQLNumResultCols");
    std::cout << "[DDBC] SQLNumResultCols_ptr: " << (void*)SQLNumResultCols_ptr << std::endl;
    SQLBindCol_ptr = (SQLBindColFunc)GetProcAddress(hModule, "SQLBindCol");
    std::cout << "[DDBC] SQLBindCol_ptr: " << (void*)SQLBindCol_ptr << std::endl;
    SQLDescribeCol_ptr = (SQLDescribeColFunc)GetProcAddress(hModule, "SQLDescribeColW");
    std::cout << "[DDBC] SQLDescribeCol_ptr: " << (void*)SQLDescribeCol_ptr << std::endl;
    SQLMoreResults_ptr = (SQLMoreResultsFunc)GetProcAddress(hModule, "SQLMoreResults");
    std::cout << "[DDBC] SQLMoreResults_ptr: " << (void*)SQLMoreResults_ptr << std::endl;
    SQLColAttribute_ptr = (SQLColAttributeFunc)GetProcAddress(hModule, "SQLColAttributeW");
    std::cout << "[DDBC] SQLColAttribute_ptr: " << (void*)SQLColAttribute_ptr << std::endl;

    // Transaction functions loading
    SQLEndTran_ptr = (SQLEndTranFunc)GetProcAddress(hModule, "SQLEndTran");
    std::cout << "[DDBC] SQLEndTran_ptr: " << (void*)SQLEndTran_ptr << std::endl;

    // Disconnect and free functions loading
    SQLFreeHandle_ptr = (SQLFreeHandleFunc)GetProcAddress(hModule, "SQLFreeHandle");
    std::cout << "[DDBC] SQLFreeHandle_ptr: " << (void*)SQLFreeHandle_ptr << std::endl;
    SQLDisconnect_ptr = (SQLDisconnectFunc)GetProcAddress(hModule, "SQLDisconnect");
    std::cout << "[DDBC] SQLDisconnect_ptr: " << (void*)SQLDisconnect_ptr << std::endl;
    SQLFreeStmt_ptr = (SQLFreeStmtFunc)GetProcAddress(hModule, "SQLFreeStmt");
    std::cout << "[DDBC] SQLFreeStmt_ptr: " << (void*)SQLFreeStmt_ptr << std::endl;

    // Diagnostic record function Loading
    SQLGetDiagRec_ptr = (SQLGetDiagRecFunc)GetProcAddress(hModule, "SQLGetDiagRecW");
    std::cout << "[DDBC] SQLGetDiagRec_ptr: " << (void*)SQLGetDiagRec_ptr << std::endl;

    std::cout << "[DDBC] All function pointers loaded. Checking success..." << std::endl;
    bool success = SQLAllocHandle_ptr && SQLSetEnvAttr_ptr && SQLSetConnectAttr_ptr &&
                   SQLSetStmtAttr_ptr && SQLGetConnectAttr_ptr && SQLDriverConnect_ptr &&
                   SQLExecDirect_ptr && SQLPrepare_ptr && SQLBindParameter_ptr && SQLExecute_ptr &&
                   SQLRowCount_ptr && SQLGetStmtAttr_ptr && SQLSetDescField_ptr && SQLFetch_ptr &&
                   SQLFetchScroll_ptr && SQLGetData_ptr && SQLNumResultCols_ptr &&
                   SQLBindCol_ptr && SQLDescribeCol_ptr && SQLMoreResults_ptr &&
                   SQLColAttribute_ptr && SQLEndTran_ptr && SQLFreeHandle_ptr &&
                   SQLDisconnect_ptr && SQLFreeStmt_ptr && SQLGetDiagRec_ptr;
    std::cout << "[DDBC] Success value: " << success << std::endl;
    if (!success) {
        std::cout << "[DDBC] Failed to load required function pointers from driver!" << std::endl;
        LOG("Failed to load required function pointers from driver - {}", dllDirStr);
        ThrowStdException("Failed to load required function pointers from driver");
    }
    std::cout << "[DDBC] Successfully loaded function pointers from driver!" << std::endl;
    LOG("Successfully loaded function pointers from driver");
    return dllDir;
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
SQLRETURN SQLAllocHandle_wrap(SQLSMALLINT HandleType, intptr_t InputHandle, intptr_t OutputHandle) {
    std::cout << "[DDBC] Entered SQLAllocHandle_wrap" << std::endl;
    std::cout << "[DDBC] HandleType: " << HandleType << std::endl;
    std::cout << "[DDBC] InputHandle: " << InputHandle << std::endl;
    std::cout << "[DDBC] OutputHandle: " << OutputHandle << std::endl;
    std::cout << "[DDBC] About to call LOG for Allocate SQL Handle" << std::endl;
    LOG("Allocate SQL Handle");
    std::cout << "[DDBC] After LOG, checking SQLAllocHandle_ptr" << std::endl;
    if (!SQLAllocHandle_ptr) {
        std::cout << "[DDBC] SQLAllocHandle_ptr is null, calling LoadDriverOrThrowException" << std::endl;
        LoadDriverOrThrowException();
        std::cout << "[DDBC] Returned from LoadDriverOrThrowException" << std::endl;
    }
    std::cout << "[DDBC] About to reinterpret_cast OutputHandle" << std::endl;
    SQLHANDLE* pOutputHandle = reinterpret_cast<SQLHANDLE*>(OutputHandle);
    std::cout << "[DDBC] About to call SQLAllocHandle_ptr" << std::endl;
    SQLRETURN ret = SQLAllocHandle_ptr(HandleType, reinterpret_cast<SQLHANDLE>(InputHandle), pOutputHandle);
    std::cout << "[DDBC] SQLAllocHandle_ptr returned: " << ret << std::endl;
    return ret;
}

// Wrap SQLSetEnvAttr
SQLRETURN SQLSetEnvAttr_wrap(intptr_t EnvHandle, SQLINTEGER Attribute, intptr_t ValuePtr,
                             SQLINTEGER StringLength) {
    std::cout << "[DDBC] Entered SQLSetEnvAttr_wrap" << std::endl;
    std::cout << "[DDBC] EnvHandle: " << EnvHandle << std::endl;
    std::cout << "[DDBC] Attribute: " << Attribute << std::endl;
    std::cout << "[DDBC] ValuePtr: " << ValuePtr << std::endl;
    std::cout << "[DDBC] StringLength: " << StringLength << std::endl;
    std::cout << "[DDBC] About to call LOG for Set SQL environment Attribute" << std::endl;
    LOG("Set SQL environment Attribute");
    std::cout << "[DDBC] After LOG, checking SQLSetEnvAttr_ptr" << std::endl;
    if (!SQLSetEnvAttr_ptr) {
        std::cout << "[DDBC] SQLSetEnvAttr_ptr is null, calling LoadDriverOrThrowException" << std::endl;
        LoadDriverOrThrowException();
        std::cout << "[DDBC] Returned from LoadDriverOrThrowException" << std::endl;
    }
    std::cout << "[DDBC] About to call SQLSetEnvAttr_ptr" << std::endl;
    SQLRETURN ret = SQLSetEnvAttr_ptr(reinterpret_cast<SQLHANDLE>(EnvHandle), Attribute,
                             reinterpret_cast<SQLPOINTER>(ValuePtr), StringLength);
    std::cout << "[DDBC] SQLSetEnvAttr_ptr returned: " << ret << std::endl;
    return ret;
}

// Wrap SQLSetConnectAttr
SQLRETURN SQLSetConnectAttr_wrap(intptr_t ConnectionHandle, SQLINTEGER Attribute, intptr_t ValuePtr,
                                 SQLINTEGER StringLength) {
    std::cout << "[DDBC] Entered SQLSetConnectAttr_wrap" << std::endl;
    std::cout << "[DDBC] ConnectionHandle: " << ConnectionHandle << std::endl;
    std::cout << "[DDBC] Attribute: " << Attribute << std::endl;
    std::cout << "[DDBC] ValuePtr: " << ValuePtr << std::endl;
    std::cout << "[DDBC] StringLength: " << StringLength << std::endl;
    std::cout << "[DDBC] About to call LOG for Set SQL Connection Attribute" << std::endl;
    LOG("Set SQL Connection Attribute");
    std::cout << "[DDBC] After LOG, checking SQLSetConnectAttr_ptr" << std::endl;
    if (!SQLSetConnectAttr_ptr) {
        std::cout << "[DDBC] SQLSetConnectAttr_ptr is null, calling LoadDriverOrThrowException" << std::endl;
        LoadDriverOrThrowException();
        std::cout << "[DDBC] Returned from LoadDriverOrThrowException" << std::endl;
    }
    std::cout << "[DDBC] About to call SQLSetConnectAttr_ptr" << std::endl;
    SQLRETURN ret = SQLSetConnectAttr_ptr(reinterpret_cast<SQLHDBC>(ConnectionHandle), Attribute,
                                 reinterpret_cast<SQLPOINTER>(ValuePtr), StringLength);
    std::cout << "[DDBC] SQLSetConnectAttr_ptr returned: " << ret << std::endl;
    return ret;
}

// Wrap SQLSetStmtAttr
SQLRETURN SQLSetStmtAttr_wrap(intptr_t StatementHandle, SQLINTEGER Attribute, intptr_t ValuePtr,
                              SQLINTEGER StringLength) {
    std::cout << "[DDBC] Entered SQLSetStmtAttr_wrap" << std::endl;
    std::cout << "[DDBC] StatementHandle: " << StatementHandle << std::endl;
    std::cout << "[DDBC] Attribute: " << Attribute << std::endl;
    std::cout << "[DDBC] ValuePtr: " << ValuePtr << std::endl;
    std::cout << "[DDBC] StringLength: " << StringLength << std::endl;
    std::cout << "[DDBC] About to call LOG for Set SQL Statement Attribute" << std::endl;
    LOG("Set SQL Statement Attribute");
    std::cout << "[DDBC] After LOG, checking SQLSetStmtAttr_ptr" << std::endl;
    if (!SQLSetStmtAttr_ptr) {
        std::cout << "[DDBC] SQLSetStmtAttr_ptr is null, calling LoadDriverOrThrowException" << std::endl;
        LoadDriverOrThrowException();
        std::cout << "[DDBC] Returned from LoadDriverOrThrowException" << std::endl;
    }
    std::cout << "[DDBC] About to call SQLSetStmtAttr_ptr" << std::endl;
    SQLRETURN ret = SQLSetStmtAttr_ptr(reinterpret_cast<SQLHSTMT>(StatementHandle), Attribute,
                              reinterpret_cast<SQLPOINTER>(ValuePtr), StringLength);
    std::cout << "[DDBC] SQLSetStmtAttr_ptr returned: " << ret << std::endl;
    return ret;
}

// Wrap SQLGetConnectionAttrA
// Currently only supports retrieval of int-valued attributes
// TODO: add support to retrieve all types of attributes
SQLINTEGER SQLGetConnectionAttr_wrap(intptr_t ConnectionHandle, SQLINTEGER attribute) {
    LOG("Get SQL COnnection Attribute");
    if (!SQLGetConnectAttr_ptr) {
        LoadDriverOrThrowException();
    }

    SQLINTEGER stringLength;
    SQLINTEGER intValue;

    // Try to get the attribute as an integer
    SQLGetConnectAttr_ptr(reinterpret_cast<SQLHDBC>(ConnectionHandle), attribute, &intValue,
                          sizeof(SQLINTEGER), &stringLength);
    return intValue;
}

// Helper function to check for driver errors
ErrorInfo SQLCheckError_Wrap(SQLSMALLINT handleType, intptr_t handle, SQLRETURN retcode) {
    LOG("Checking errors for retcode - {}" , retcode);
    ErrorInfo errorInfo;
    if (retcode == SQL_INVALID_HANDLE) {
        LOG("Invalid handle received");
        errorInfo.ddbcErrorMsg = std::wstring( L"Invalid handle!");
        return errorInfo;
    }
    assert(handle != 0);
    if (!SQL_SUCCEEDED(retcode)) {
        if (!SQLGetDiagRec_ptr) {
            LoadDriverOrThrowException();
        }

        SQLWCHAR sqlState[6], message[SQL_MAX_MESSAGE_LENGTH];
        SQLINTEGER nativeError;
        SQLSMALLINT messageLen;

        SQLRETURN diagReturn =
            SQLGetDiagRec_ptr(handleType, reinterpret_cast<SQLHANDLE>(handle), 1, sqlState,
                              &nativeError, message, SQL_MAX_MESSAGE_LENGTH, &messageLen);

        if (SQL_SUCCEEDED(diagReturn)) {
            errorInfo.sqlState = std::wstring(sqlState);
            errorInfo.ddbcErrorMsg = std::wstring(message);
        }
    }
    return errorInfo;
}

// Wrap SQLDriverConnect
SQLRETURN SQLDriverConnect_wrap(intptr_t ConnectionHandle, intptr_t WindowHandle,
                                const std::wstring& ConnectionString) {
    std::cout << "[DDBC] Entered SQLDriverConnect_wrap" << std::endl;
    std::cout << "[DDBC] ConnectionHandle: " << ConnectionHandle << std::endl;
    std::cout << "[DDBC] WindowHandle: " << WindowHandle << std::endl;
    std::wcout << L"[DDBC] ConnectionString: " << ConnectionString << std::endl;
    std::cout << "[DDBC] About to call LOG for Driver Connect to MSSQL" << std::endl;
    LOG("Driver Connect to MSSQL");
    std::cout << "[DDBC] After LOG, checking SQLDriverConnect_ptr" << std::endl;
    if (!SQLDriverConnect_ptr) {
        std::cout << "[DDBC] SQLDriverConnect_ptr is null, calling LoadDriverOrThrowException" << std::endl;
        LoadDriverOrThrowException();
        std::cout << "[DDBC] Returned from LoadDriverOrThrowException" << std::endl;
    }
    std::cout << "[DDBC] About to call SQLDriverConnect_ptr" << std::endl;
    SQLRETURN ret = SQLDriverConnect_ptr(reinterpret_cast<SQLHANDLE>(ConnectionHandle),
                                reinterpret_cast<SQLHWND>(WindowHandle),
                                const_cast<SQLWCHAR*>(ConnectionString.c_str()), SQL_NTS, nullptr,
                                0, nullptr, SQL_DRIVER_NOPROMPT);
    std::cout << "[DDBC] SQLDriverConnect_ptr returned: " << ret << std::endl;
    return ret;
}

// Wrap SQLExecDirect
SQLRETURN SQLExecDirect_wrap(intptr_t StatementHandle, const std::wstring& Query) {
    std::cout << "[DDBC] Entered SQLExecDirect_wrap" << std::endl;
    std::cout << "[DDBC] StatementHandle: " << StatementHandle << std::endl;
    std::wcout << L"[DDBC] Query: " << Query << std::endl;
    std::cout << "[DDBC] About to call LOG for Execute SQL query directly" << std::endl;
    LOG("Execute SQL query directly - {}", Query.c_str());
    std::cout << "[DDBC] After LOG, checking SQLExecDirect_ptr" << std::endl;
    if (!SQLExecDirect_ptr) {
        std::cout << "[DDBC] SQLExecDirect_ptr is null, calling LoadDriverOrThrowException" << std::endl;
        LoadDriverOrThrowException();
        std::cout << "[DDBC] Returned from LoadDriverOrThrowException" << std::endl;
    }
    std::cout << "[DDBC] About to call SQLExecDirect_ptr" << std::endl;
    SQLRETURN ret = SQLExecDirect_ptr(reinterpret_cast<SQLHANDLE>(StatementHandle),
                             const_cast<SQLWCHAR*>(Query.c_str()), SQL_NTS);
    std::cout << "[DDBC] SQLExecDirect_ptr returned: " << ret << std::endl;
    return ret;
}

// Executes the provided query. If the query is parametrized, it prepares the statement and
// binds the parameters. Otherwise, it executes the query directly.
// 'usePrepare' parameter can be used to disable the prepare step for queries that might already
// be prepared in a previous call.
SQLRETURN SQLExecute_wrap(const intptr_t statementHandle,
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
    SQLHANDLE hStmt = reinterpret_cast<SQLHANDLE>(statementHandle);
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
SQLSMALLINT SQLNumResultCols_wrap(intptr_t statementHandle) {
    std::cout << "[DDBC] Entered SQLNumResultCols_wrap" << std::endl;
    std::cout << "[DDBC] statementHandle: " << statementHandle << std::endl;
    
    if (statementHandle == 0) {
        std::cout << "[DDBC ERROR] Statement handle is NULL in SQLNumResultCols_wrap" << std::endl;
        ThrowStdException("Statement handle is NULL");
        return 0;  // Never reaches here, but keeps compiler happy
    }
    
    LOG("Get number of columns in result set");
    if (!SQLNumResultCols_ptr) {
        std::cout << "[DDBC] SQLNumResultCols_ptr is null! Loading driver..." << std::endl;
        LoadDriverOrThrowException();
    }

    SQLSMALLINT columnCount = 0;
    std::cout << "[DDBC] About to call SQLNumResultCols_ptr" << std::endl;
    
    // Use a try-catch block around the ODBC call
    try {
        SQLHSTMT hStmt = reinterpret_cast<SQLHSTMT>(statementHandle);
        SQLRETURN ret = SQLNumResultCols_ptr(hStmt, &columnCount);
        std::cout << "[DDBC] SQLNumResultCols_ptr returned: " << ret << ", columnCount: " << columnCount << std::endl;
        
        if (!SQL_SUCCEEDED(ret)) {
            std::cout << "[DDBC ERROR] SQLNumResultCols_ptr failed with code: " << ret << std::endl;
            columnCount = 0;
        }
    }
    catch (const std::exception& e) {
        std::cout << "[DDBC ERROR] Exception in SQLNumResultCols_ptr: " << e.what() << std::endl;
        columnCount = 0;
    }
    catch (...) {
        std::cout << "[DDBC ERROR] Unknown exception in SQLNumResultCols_ptr" << std::endl;
        columnCount = 0;
    }
    
    return columnCount;
}

// Wrap SQLDescribeCol
SQLRETURN SQLDescribeCol_wrap(intptr_t StatementHandle, py::list& ColumnMetadata) {
    std::cout << "[DDBC] Entered SQLDescribeCol_wrap" << std::endl;
    std::cout << "[DDBC] StatementHandle: " << StatementHandle << std::endl;
    
    if (StatementHandle == 0) {
        std::cout << "[DDBC ERROR] Statement handle is NULL in SQLDescribeCol_wrap" << std::endl;
        return SQL_ERROR;
    }
    
    LOG("Get column description");
    if (!SQLDescribeCol_ptr || !SQLNumResultCols_ptr) {
        std::cout << "[DDBC] SQLDescribeCol_ptr or SQLNumResultCols_ptr is null! Loading driver..." << std::endl;
        LoadDriverOrThrowException();
    }

    SQLSMALLINT ColumnCount = 0;
    SQLRETURN retcode = SQL_ERROR;
    
    try {
        SQLHSTMT hStmt = reinterpret_cast<SQLHSTMT>(StatementHandle);
        std::cout << "[DDBC] About to call SQLNumResultCols_ptr in SQLDescribeCol_wrap" << std::endl;
        retcode = SQLNumResultCols_ptr(hStmt, &ColumnCount);
        std::cout << "[DDBC] SQLNumResultCols_ptr in SQLDescribeCol_wrap returned: " << retcode << ", ColumnCount: " << ColumnCount << std::endl;
        
        if (!SQL_SUCCEEDED(retcode)) {
            std::cout << "[DDBC ERROR] Failed to get number of columns in SQLDescribeCol_wrap" << std::endl;
            LOG("Failed to get number of columns");
            return retcode;
        }
        
        for (SQLUSMALLINT i = 1; i <= ColumnCount; ++i) {
            try {
                SQLWCHAR ColumnName[256];
                SQLSMALLINT NameLength;
                SQLSMALLINT DataType;
                SQLULEN ColumnSize;
                SQLSMALLINT DecimalDigits;
                SQLSMALLINT Nullable;
                
                std::cout << "[DDBC] About to call SQLDescribeCol_ptr for column: " << i << std::endl;
                retcode = SQLDescribeCol_ptr(hStmt, i, ColumnName, sizeof(ColumnName) / sizeof(SQLWCHAR),
                                         &NameLength, &DataType, &ColumnSize, &DecimalDigits, &Nullable);
                std::cout << "[DDBC] SQLDescribeCol_ptr returned: " << retcode << " for column: " << i << std::endl;

                if (SQL_SUCCEEDED(retcode)) {
                    // Append a named py::dict to ColumnMetadata
                    // TODO: Should we define a struct for this task instead of dict?
                    ColumnMetadata.append(py::dict("ColumnName"_a = std::wstring(ColumnName),
                                               "DataType"_a = DataType, "ColumnSize"_a = ColumnSize,
                                               "DecimalDigits"_a = DecimalDigits,
                                               "Nullable"_a = Nullable));
                } else {
                    std::cout << "[DDBC ERROR] SQLDescribeCol_ptr failed for column: " << i << std::endl;
                    return retcode;
                }
            } catch (const std::exception& e) {
                std::cout << "[DDBC ERROR] Exception describing column " << i << ": " << e.what() << std::endl;
                return SQL_ERROR;
            } catch (...) {
                std::cout << "[DDBC ERROR] Unknown exception describing column " << i << std::endl;
                return SQL_ERROR;
            }
        }
    } catch (const std::exception& e) {
        std::cout << "[DDBC ERROR] Exception in SQLDescribeCol_wrap: " << e.what() << std::endl;
        return SQL_ERROR;
    } catch (...) {
        std::cout << "[DDBC ERROR] Unknown exception in SQLDescribeCol_wrap" << std::endl;
        return SQL_ERROR;
    }
    
    std::cout << "[DDBC] SQLDescribeCol_wrap completed successfully" << std::endl;
    return SQL_SUCCESS;
}

// Wrap SQLFetch to retrieve rows
SQLRETURN SQLFetch_wrap(intptr_t StatementHandle) {
    std::cout << "[DDBC] Entered SQLFetch_wrap" << std::endl;
    std::cout << "[DDBC] StatementHandle: " << StatementHandle << std::endl;
    if (!SQLFetch_ptr) {
        LoadDriverOrThrowException();
    }
    std::cout << "[DDBC] About to call SQLFetch_ptr" << std::endl;
    SQLRETURN ret = SQLFetch_ptr(reinterpret_cast<SQLHANDLE>(StatementHandle));
    std::cout << "[DDBC] SQLFetch_ptr returned: " << ret << std::endl;
    return ret;
}

// Simplified approach to FetchOne_wrap to avoid complex buffer handling and be more 32-bit compatible
SQLRETURN FetchOne_wrap(intptr_t StatementHandle, py::list& row) {
    // Print directly to stdout without buffering to ensure it's immediately visible
    printf("[DDBC] ENTERING FetchOne_wrap with handle: %p\n", (void*)StatementHandle);
    fflush(stdout);
    std::cout << "[DDBC] ENTERING FetchOne_wrap" << std::endl;
    std::cout << "[DDBC] Entered FetchOne_wrap with simplification for 32-bit" << std::endl;
    std::cout << "[DDBC] StatementHandle: " << StatementHandle << std::endl;
    std::cout.flush();
    
    // Use try/catch for all operations
    try {
        if (StatementHandle == 0) {
            std::cout << "[DDBC ERROR] Statement handle is NULL in FetchOne_wrap" << std::endl;
            return SQL_ERROR;
        }
        
        // Special handling for 32-bit platforms
        #if defined(_WIN32) && !defined(_WIN64)
            printf("[DDBC] Using 32-bit specific handle conversion\n");
            fflush(stdout);
            // On 32-bit, use careful casting to avoid pointer truncation
            SQLHSTMT hStmt = (SQLHSTMT)(DWORD_PTR)StatementHandle;
        #else
            // Standard 64-bit handling
            SQLHSTMT hStmt = reinterpret_cast<SQLHSTMT>(StatementHandle);
        #endif
        
        printf("[DDBC] Handle conversion complete, hStmt: %p\n", (void*)hStmt);
        fflush(stdout);
        
        // First check if SQLFetch_ptr is valid
        if (!SQLFetch_ptr || !SQLGetData_ptr || !SQLNumResultCols_ptr) {
            std::cout << "[DDBC] Some function pointers are null, loading driver..." << std::endl;
            LoadDriverOrThrowException();
        }
        
        // Safely fetch next row
        std::cout << "[DDBC] About to call SQLFetch_ptr with hStmt: " << hStmt << std::endl;
        SQLRETURN ret = SQLFetch_ptr(hStmt);
        std::cout << "[DDBC] SQLFetch_ptr returned: " << ret << std::endl;
        
        if (ret == SQL_NO_DATA) {
            std::cout << "[DDBC] No more rows" << std::endl;
            return SQL_NO_DATA;
        }
        
        if (!SQL_SUCCEEDED(ret)) {
            std::cout << "[DDBC ERROR] SQLFetch_ptr failed with return code: " << ret << std::endl;
            return ret;
        }
        
        // Get number of columns
        std::cout << "[DDBC] Fetching column count" << std::endl;
        SQLSMALLINT colCount = 0;
        ret = SQLNumResultCols_ptr(hStmt, &colCount);
        std::cout << "[DDBC] SQLNumResultCols_ptr returned: " << ret << ", colCount: " << colCount << std::endl;
        
        if (!SQL_SUCCEEDED(ret)) {
            std::cout << "[DDBC ERROR] SQLNumResultCols_ptr failed with return code: " << ret << std::endl;
            return ret;
        }
        
        // Get data for each column
        std::cout << "[DDBC] Processing " << colCount << " columns" << std::endl;
        for (SQLSMALLINT i = 1; i <= colCount; i++) {
            std::cout << "[DDBC] Processing column: " << i << std::endl;
            SQLCHAR buffer[1024] = {0};
            SQLLEN indicator = 0;
            // Always use SQL_C_CHAR type for simplicity
            std::cout << "[DDBC] About to call SQLGetData_ptr for column: " << i << std::endl;
            ret = SQLGetData_ptr(hStmt, i, SQL_C_CHAR, buffer, sizeof(buffer), &indicator);
            std::cout << "[DDBC] SQLGetData_ptr returned: " << ret << " for column: " << i << ", indicator: " << indicator << std::endl;
            
            if (SQL_SUCCEEDED(ret)) {
                if (indicator == SQL_NULL_DATA) {
                    std::cout << "[DDBC] Column " << i << " is NULL" << std::endl;
                    row.append(py::none());
                } else {
                    // Only use the valid bytes from buffer up to indicator to avoid garbage
                    const char* charBuf = reinterpret_cast<const char*>(buffer);
                    size_t dataLen = 0;
                    if (indicator > 0 && indicator < static_cast<SQLLEN>(sizeof(buffer))) {
                        dataLen = static_cast<size_t>(indicator);
                    } else {
                        dataLen = strnlen(charBuf, sizeof(buffer) - 1);
                    }
                    std::string dataStr(charBuf, dataLen);
                    std::cout << "[DDBC] Column " << i << " data: '" << dataStr << "'" << std::endl;
                    row.append(py::str(dataStr));
                }
            } else {
                std::cout << "[DDBC ERROR] Failed to get data for column " << i << std::endl;
                // Add an empty string rather than fail the whole operation
                row.append(py::str(""));
            }
        }
        
        std::cout << "[DDBC] Row processing complete" << std::endl;
        return SQL_SUCCESS;
    }
    catch(const std::exception& e) {
        std::cerr << "[DDBC ERROR] Exception in FetchOne_wrap: " << e.what() << std::endl;
        return SQL_ERROR;
    }
    catch(...) {
        std::cerr << "[DDBC ERROR] Unknown exception in FetchOne_wrap" << std::endl;
        return SQL_ERROR;
    }
}

// Helper function to fetch multiple rows
SQLRETURN FetchMany_wrap(intptr_t StatementHandle, py::list& rows, int fetchSize) {
    // Print directly to stdout without buffering to ensure it's immediately visible
    printf("[DDBC] ENTERING FetchMany_wrap with handle: %p\n", (void*)StatementHandle);
    fflush(stdout);
    std::cout << "[DDBC] Entered FetchMany_wrap" << std::endl;
    std::cout << "[DDBC] StatementHandle: " << StatementHandle << std::endl;
    std::cout << "[DDBC] fetchSize: " << fetchSize << std::endl;
    std::cout.flush();
    
    if (StatementHandle == 0) {
        std::cout << "[DDBC ERROR] StatementHandle is NULL in FetchMany_wrap" << std::endl;
        return SQL_ERROR;
    }
      try {
        // Special handling for 32-bit platforms isn't needed here since we're using FetchOne_wrap
        // which already has the 32-bit handle conversion
        
        // Fetch the specified number of rows or until there are no more rows
        int rowsRetrieved = 0;
        while (rowsRetrieved < fetchSize) {
            py::list row;
            SQLRETURN ret = FetchOne_wrap(StatementHandle, row);
            
            if (ret == SQL_NO_DATA) {
                std::cout << "[DDBC] No more rows to fetch in FetchMany_wrap" << std::endl;
                break;
            }
            
            if (!SQL_SUCCEEDED(ret)) {
                std::cout << "[DDBC ERROR] Error fetching row in FetchMany_wrap: " << ret << std::endl;
                return ret;
            }
            
            rows.append(row);
            rowsRetrieved++;
        }
        
        std::cout << "[DDBC] FetchMany_wrap retrieved " << rowsRetrieved << " rows" << std::endl;
        return SQL_SUCCESS;
    } catch (const std::exception& e) {
        std::cout << "[DDBC ERROR] Exception in FetchMany_wrap: " << e.what() << std::endl;
        return SQL_ERROR;
    } catch (...) {
        std::cout << "[DDBC ERROR] Unknown exception in FetchMany_wrap" << std::endl;
        return SQL_ERROR;
    }
}

// Helper function to fetch all remaining rows with more defensive programming
SQLRETURN FetchAll_wrap(intptr_t StatementHandle, py::list& rows) {
    printf("[DDBC] ENTERING FetchAll_wrap with handle: %p\n", (void*)StatementHandle);
    fflush(stdout);
    
    if (StatementHandle == 0) {
        printf("[DDBC ERROR] Statement handle is NULL in FetchAll_wrap\n");
        fflush(stdout);
        return SQL_ERROR;
    }

    // Clear the rows list to start fresh
    rows.attr("clear")();
    
    try {
        // Special handling for 32-bit platforms
        #if defined(_WIN32) && !defined(_WIN64)
            printf("[DDBC] Using 32-bit specific handle conversion in FetchAll_wrap\n");
            fflush(stdout);
            // On 32-bit, use careful casting to avoid pointer truncation
            SQLHSTMT hStmt = (SQLHSTMT)(DWORD_PTR)StatementHandle;
        #else
            // Standard 64-bit handling
            SQLHSTMT hStmt = reinterpret_cast<SQLHSTMT>(StatementHandle);
        #endif
        
        printf("[DDBC] FetchAll_wrap handle conversion complete, hStmt: %p\n", (void*)hStmt);
        fflush(stdout);
        
        // Check if function pointers are valid
        if (!SQLFetch_ptr || !SQLNumResultCols_ptr || !SQLGetData_ptr) {
            printf("[DDBC] Loading driver in FetchAll_wrap\n");
            fflush(stdout);
            LoadDriverOrThrowException();
        }
        
        // Get column count once
        SQLSMALLINT numCols = 0;
        SQLRETURN colRet = SQLNumResultCols_ptr(hStmt, &numCols);
        printf("[DDBC] FetchAll_wrap got column count: %d (ret=%d)\n", numCols, colRet);
        fflush(stdout);
        
        if (!SQL_SUCCEEDED(colRet) || numCols <= 0) {
            printf("[DDBC ERROR] Failed to get column count or no columns in result set\n");
            fflush(stdout);
            return colRet != SQL_SUCCESS ? colRet : SQL_ERROR;
        }
        
        // Fetch all rows directly
        SQLRETURN ret;
        int rowCount = 0;
        while (true) {
            // Fetch next row
            printf("[DDBC] FetchAll_wrap calling SQLFetch_ptr\n");
            fflush(stdout);
            ret = SQLFetch_ptr(hStmt);
            printf("[DDBC] FetchAll_wrap SQLFetch_ptr returned: %d\n", ret);
            fflush(stdout);

            if (ret == SQL_NO_DATA) {
                printf("[DDBC] No more rows to fetch in FetchAll_wrap\n");
                fflush(stdout);
                break;
            }

            if (!SQL_SUCCEEDED(ret)) {
                printf("[DDBC ERROR] Error in SQLFetch_ptr: %d\n", ret);
                fflush(stdout);
                return ret;
            }

            // Create a new row
            py::list row;

            // Get data for each column
            for (SQLSMALLINT i = 1; i <= numCols; i++) {
                char buffer[4096] = {0};
                SQLLEN indicator = 0;

                SQLRETURN dataRet = SQLGetData_ptr(hStmt, i, SQL_C_CHAR, buffer, sizeof(buffer), &indicator);
                printf("[DDBC] Column %d SQLGetData_ptr returned: %d (indicator=%ld)\n", i, dataRet, (long)indicator);
                fflush(stdout);

                if (SQL_SUCCEEDED(dataRet)) {
                    if (indicator == SQL_NULL_DATA) {
                        row.append(py::none());
                        printf("[DDBC] Column %d is NULL\n", i);
                        fflush(stdout);
                    } else {
                        // Only use the valid bytes from buffer up to indicator to avoid garbage
                        size_t dataLen = 0;
                        if (indicator > 0 && indicator < static_cast<SQLLEN>(sizeof(buffer))) {
                            dataLen = static_cast<size_t>(indicator);
                        } else {
                            dataLen = strnlen(buffer, sizeof(buffer) - 1);
                        }
                        std::string dataStr(buffer, dataLen);
                        printf("[DDBC] Column %d data: '%s'\n", i, dataStr.c_str());
                        fflush(stdout);
                        row.append(py::str(dataStr));
                    }
                } else {
                    printf("[DDBC ERROR] Failed to get column %d data\n", i);
                    fflush(stdout);
                    row.append(py::none());
                }
            }

            // Validate row before appending
            if (py::len(row) == numCols) {
                rows.append(row);
                rowCount++;
                printf("[DDBC] Added row %d, row size: %d\n", rowCount, (int)py::len(row));
                fflush(stdout);
            } else {
                printf("[DDBC ERROR] Row size mismatch, skipping row\n");
                fflush(stdout);
            }

            // Debug: Print the current state of rows
            printf("[DDBC DEBUG] Current rows list:\n");
            for (auto& r : rows) {
                printf("[DDBC DEBUG] Row: %s\n", py::str(r).cast<std::string>().c_str());
            }
        }

        printf("[DDBC] FetchAll_wrap successfully retrieved all rows: %d\n", (int)py::len(rows));
        fflush(stdout);
        return SQL_SUCCESS;
    } catch (const std::exception& e) {
        printf("[DDBC ERROR] Exception in FetchAll_wrap: %s\n", e.what());
        fflush(stdout);
        return SQL_ERROR;
    } catch (...) {
        printf("[DDBC ERROR] Unknown exception in FetchAll_wrap\n");
        fflush(stdout);
        return SQL_ERROR;
    }
}

// Helper function to retrieve column data
// TODO: Handle variable length data correctly
SQLRETURN SQLGetData_wrap(intptr_t StatementHandle, SQLUSMALLINT colCount, py::list& row) {
    std::cout << "[DDBC] Entered SQLGetData_wrap" << std::endl;
    std::cout << "[DDBC] StatementHandle: " << StatementHandle << std::endl;
    std::cout << "[DDBC] colCount: " << colCount << std::endl;
    
    if (!SQLGetData_ptr) {
        std::cout << "[DDBC] SQLGetData_ptr is null! Loading driver..." << std::endl;
        LoadDriverOrThrowException();
    }
    
    SQLRETURN ret = SQL_SUCCESS;
    SQLHSTMT hStmt = reinterpret_cast<SQLHSTMT>(StatementHandle);
    
    for (SQLSMALLINT i = 1; i <= colCount; i++) {
        try {
            std::cout << "[DDBC] Processing column: " << i << std::endl;
            SQLCHAR buffer[1024] = {0};
            SQLLEN indicator = 0;
              // Use direct SQL_C_CHAR for simplicity and compatibility
            std::cout << "[DDBC] About to call SQLGetData_ptr for column: " << i << std::endl;
            ret = SQLGetData_ptr(hStmt, i, SQL_C_CHAR, buffer, sizeof(buffer), &indicator);
            std::cout << "[DDBC] SQLGetData_ptr returned: " << ret << " for column: " << i << ", indicator: " << indicator << std::endl;
            
            if (SQL_SUCCEEDED(ret)) {
                if (indicator == SQL_NULL_DATA) {
                    std::cout << "[DDBC] Column " << i << " is NULL" << std::endl;
                    row.append(py::none());
                } else {
                    // Only use the valid bytes from buffer up to indicator to avoid garbage
                    size_t dataLen = 0;
                    if (indicator == SQL_NO_TOTAL) {
                        // For SQL_NO_TOTAL, find null terminator manually
                        for (dataLen = 0; dataLen < sizeof(buffer); dataLen++) {
                            if (buffer[dataLen] == 0) break;
                        }
                    } else if (indicator > 0) {
                        // Normal case: indicator contains the length
                        dataLen = static_cast<size_t>(indicator);
                        if (dataLen >= sizeof(buffer)) {
                            dataLen = sizeof(buffer) - 1; // Ensure we don't exceed buffer
                        }
                    } else {
                        // Fallback: find first null byte manually
                        for (dataLen = 0; dataLen < sizeof(buffer); dataLen++) {
                            if (buffer[dataLen] == 0) break;
                        }
                    }
                    // Create string using the proper char* cast instead of relying on implicit conversion
                    std::string dataStr(reinterpret_cast<const char*>(buffer), dataLen);
                    std::cout << "[DDBC] Column " << i << " data: '" << dataStr << "'" << std::endl;
                    row.append(py::str(dataStr));
                }
            } else {
                std::cout << "[DDBC] Error retrieving data for column: " << i << std::endl;
                row.append(py::none());
            }
        } catch (const std::exception& e) {
            std::cout << "[DDBC ERROR] Exception in SQLGetData_wrap for column " << i << ": " << e.what() << std::endl;
            row.append(py::none());
        } catch (...) {
            std::cout << "[DDBC ERROR] Unknown exception in SQLGetData_wrap for column " << i << std::endl;
            row.append(py::none());
        }
    }
    
    std::cout << "[DDBC] SQLGetData_wrap finished with ret: " << ret << std::endl;
    return ret;
}

// Wrap SQLMoreResults
SQLRETURN SQLMoreResults_wrap(intptr_t StatementHandle) {
    LOG("Check for more results");
    if (!SQLMoreResults_ptr) {
        LoadDriverOrThrowException();
    }

    return SQLMoreResults_ptr(reinterpret_cast<SQLHANDLE>(StatementHandle));
}

// Wrap SQLEndTran
SQLRETURN SQLEndTran_wrap(SQLSMALLINT HandleType, intptr_t Handle, SQLSMALLINT CompletionType) {
    LOG("End SQL Transaction");
    if (!SQLEndTran_ptr) {
        LoadDriverOrThrowException();
    }

    return SQLEndTran_ptr(HandleType, reinterpret_cast<SQLHANDLE>(Handle), CompletionType);
}

// Wrap SQLFreeHandle
SQLRETURN SQLFreeHandle_wrap(SQLSMALLINT HandleType, intptr_t Handle) {
    LOG("Free SQL handle");
    if (!SQLAllocHandle_ptr) {
        LoadDriverOrThrowException();
    }

    return SQLFreeHandle_ptr(HandleType, reinterpret_cast<SQLHANDLE>(Handle));
}

// Wrap SQLDisconnect
SQLRETURN SQLDisconnect_wrap(intptr_t ConnectionHandle) {
    LOG("Disconnect from MSSQL");
    if (!SQLDisconnect_ptr) {
        LoadDriverOrThrowException();
    }

    return SQLDisconnect_ptr(reinterpret_cast<SQLHDBC>(ConnectionHandle));
}

// Wrap SQLRowCount
SQLLEN SQLRowCount_wrap(intptr_t StatementHandle) {
    std::cout << "[DDBC] Entered SQLRowCount_wrap" << std::endl;
    std::cout << "[DDBC] StatementHandle: " << StatementHandle << std::endl;
    LOG("Get number of row affected by last execute");
    if (!SQLRowCount_ptr) {
        std::cout << "[DDBC] SQLRowCount_ptr is null! Loading driver..." << std::endl;
        LoadDriverOrThrowException();
    }

    SQLLEN rowCount;
    std::cout << "[DDBC] About to call SQLRowCount_ptr" << std::endl;
    SQLRETURN ret = SQLRowCount_ptr(reinterpret_cast<SQLHSTMT>(StatementHandle), &rowCount);
    std::cout << "[DDBC] SQLRowCount_ptr returned: " << ret << ", rowCount: " << rowCount << std::endl;
    if (!SQL_SUCCEEDED(ret)) {
        LOG("Error getting row count");
    }
    LOG("SQLRowCount returned {}", rowCount);
    return rowCount;
}

// Functions/data to be exposed to Python as a part of ddbc_bindings module
PYBIND11_MODULE(ddbc_bindings, m) {
    // Make sure the return value policy is properly set to ensure ownership transfer
    pybind11::return_value_policy rvp = pybind11::return_value_policy::move;
    
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
        .def_readwrite("ddbcErrorMsg", &ErrorInfo::ddbcErrorMsg);    // Expose all the SQL functions with proper error handling and explicit return value policies
    m.def("DDBCSQLAllocHandle", &SQLAllocHandle_wrap,
          "Allocate an environment, connection, statement, or descriptor handle");
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
    // For these fetch functions, use py::return_value_policy::reference to ensure Python references the same list
    m.def("DDBCSQLFetchOne", &FetchOne_wrap, py::arg("StatementHandle"), py::arg("row"),
          "Fetch one row from the result set");
    m.def("DDBCSQLFetchMany", &FetchMany_wrap, py::arg("StatementHandle"), py::arg("rows"),
          py::arg("fetchSize") = 1, "Fetch many rows from the result set");
    m.def("DDBCSQLFetchAll", &FetchAll_wrap, py::arg("StatementHandle"), py::arg("rows"),
          "Fetch all rows from the result set");
    m.def("DDBCSQLEndTran", &SQLEndTran_wrap, "End a transaction");
    m.def("DDBCSQLFreeHandle", &SQLFreeHandle_wrap, "Free a handle");
    m.def("DDBCSQLDisconnect", &SQLDisconnect_wrap, "Disconnect from a data source");
    m.def("DDBCSQLCheckError", &SQLCheckError_Wrap, "Check for driver errors");

    // Add a version attribute
    m.attr("__version__") = "1.0.0";
    
    try {
        // Try loading the ODBC driver when the module is imported
        LoadDriverOrThrowException();
    } catch (const std::exception& e) {
        // Log the error but don't throw - let the error happen when functions are called
        LOG("Failed to load ODBC driver during module initialization: {}", e.what());
    }
}
