#include "bcp_wrapper.h" // Includes ddbc_bindings.h (and thus sql.h, sqlext.h, <string>, <memory>) and connection.h

// Pybind11 headers (needed for py::cast and potentially std::optional if used with pybind types)
#include <pybind11/pybind11.h>
#include <pybind11/stl.h> 
#include <pybind11/pytypes.h> // Ensure this is included for py::bytes if not transitively

// Standard C++ headers (not covered by ddbc_bindings.h)
#include <vector>
#include <stdexcept>
#include <unordered_map>
#include <string> // For std::to_string, std::string
#include <iostream> // Added for std::cout

namespace py = pybind11; // Alias for pybind11 namespace

// Helper to manage BCP properties for bcp_control
enum class BCPCtrlPropType { INT, WSTRING };
struct BCPCtrlPropertyInfo {
    INT option_code;
    BCPCtrlPropType type;
};

// Map property names to their ODBC codes and types
const std::unordered_map<std::wstring, BCPCtrlPropertyInfo> bcp_control_properties = {
    {L"BCPMAXERRS",     {BCPMAXERRS,        BCPCtrlPropType::INT}},
    {L"BCPBATCH",       {BCPBATCH,          BCPCtrlPropType::INT}},
    {L"BCPKEEPNULLS",   {BCPKEEPNULLS,      BCPCtrlPropType::INT}},
    {L"BCPKEEPIDENTITY",{BCPKEEPIDENTITY,   BCPCtrlPropType::INT}},
    {L"BCPHINTS",       {BCPHINTS,          BCPCtrlPropType::WSTRING}},
    {L"BCPFILECP",      {BCPFILECP,         BCPCtrlPropType::INT}},
    // Example if you were to add global terminators via bcp_control:
    // {L"BCPFIELDTERM",   {BCPFIELDTERM_Constant, BCPCtrlPropType::BYTES}}, // Assuming BCPFIELDTERM_Constant is the ODBC int
    // {L"BCPROWTERM",     {BCPROWTERM_Constant,   BCPCtrlPropType::BYTES}}, // Assuming BCPROWTERM_Constant is the ODBC int
};

// Helper for bcp_init direction string
INT get_bcp_direction_code(const std::wstring& direction_str) {
    if (direction_str == L"in" || direction_str == L"IN") return DB_IN;
    if (direction_str == L"out" || direction_str == L"OUT") return DB_OUT;
    throw std::runtime_error("Invalid BCP direction string: " + py::cast(direction_str).cast<std::string>());
}

// Helper to get HDBC, ensuring driver and BCP function pointers are loaded.
// Assumes Connection class has get_hdbc() and is_connected() methods.
SQLHDBC get_valid_hdbc_for_bcp(Connection& conn) { // Changed to Connection&
    if (!conn.is_connected()) { // Use . instead of ->
        std::cout << "BCPWrapper: Connection is not connected." << std::endl;
        throw std::runtime_error("BCPWrapper: Connection is not connected.");
    }

    // Check critical BCP and related function pointers
    // REMOVED BCPSetBulkMode_ptr from this check
    if (!SQLSetConnectAttr_ptr || !BCPInitW_ptr || !BCPControlW_ptr || 
        !BCPReadFmtW_ptr || !BCPColumns_ptr || !BCPColFmtW_ptr || 
        !BCPExec_ptr || !BCPDone_ptr ) { 
         std::cout << "BCPWrapper: Critical ODBC/BCP function pointers not loaded. Attempting to load via DriverLoader." << std::endl;
         DriverLoader::getInstance().loadDriver(); 
         // REMOVED BCPSetBulkMode_ptr from this check
         if (!SQLSetConnectAttr_ptr || !BCPInitW_ptr || !BCPControlW_ptr || 
             !BCPReadFmtW_ptr || !BCPColumns_ptr || !BCPColFmtW_ptr || 
             !BCPExec_ptr || !BCPDone_ptr ) { 
            std::cout << "BCPWrapper Error: ODBC/BCP function pointers still not loaded after attempt." << std::endl;
            throw std::runtime_error("BCPWrapper: ODBC/BCP function pointers not loaded.");
         }
    }

    SQLHDBC hdbc = conn.get_hdbc(); // Use . instead of ->
    std::cout << hdbc << std::endl; // Debugging output
    if (!hdbc) {
        std::cout << "BCPWrapper Error: Connection handle is null." << std::endl;
        throw std::runtime_error("BCPWrapper: Connection handle is null.");
    }
    if (hdbc == SQL_NULL_HDBC) {
        std::cout << "BCPWrapper Error: Failed to get HDBC from Connection object." << std::endl;
        throw std::runtime_error("BCPWrapper: Failed to get HDBC from Connection object.");
    }
    return hdbc;
}

// Helper function (can be a static private method or a lambda in C++11 and later)
// to retrieve ODBC diagnostic messages and populate ErrorInfo.
// This uses SQLGetDiagRec_ptr directly, which is loaded by DriverLoader.
static ErrorInfo get_odbc_diagnostics_for_handle(SQLSMALLINT handle_type, SQLHANDLE handle) {
    ErrorInfo error_info;
    error_info.sqlState = L""; // Initialize
    error_info.ddbcErrorMsg = L""; // Initialize

    if (!SQLGetDiagRec_ptr) {
        std::cout << "get_odbc_diagnostics_for_handle: SQLGetDiagRec_ptr is null." << std::endl;
        error_info.ddbcErrorMsg = L"SQLGetDiagRec_ptr not loaded. Cannot retrieve diagnostics.";
        return error_info;
    }

    SQLWCHAR sql_state_w[6];
    SQLINTEGER native_error;
    SQLWCHAR message_text_w[SQL_MAX_MESSAGE_LENGTH];
    SQLSMALLINT text_length;
    SQLSMALLINT rec_number = 1;
    std::wstring combined_messages;

    while (SQLGetDiagRec_ptr(handle_type, handle, rec_number, sql_state_w, &native_error,
                             message_text_w, SQL_MAX_MESSAGE_LENGTH, &text_length) == SQL_SUCCESS) {
        if (rec_number == 1) {
            error_info.sqlState = std::wstring(sql_state_w);
        }
        if (!combined_messages.empty()) {
            combined_messages += L" | ";
        }
        combined_messages += std::wstring(message_text_w, text_length) + L" (Native: " + std::to_wstring(native_error) + L")";
        rec_number++;
    }
    error_info.ddbcErrorMsg = combined_messages;
    if (combined_messages.empty() && rec_number == 1) { // No records found
         error_info.ddbcErrorMsg = L"No ODBC diagnostic records found for the handle.";
    }
    return error_info;
}


BCPWrapper::BCPWrapper(Connection& conn) // Changed to Connection&
    : _conn(conn), _bcp_initialized(false), _bcp_finished(true) { // Initialize reference
    SQLHDBC hdbc = SQL_NULL_HDBC;
    try {
        hdbc = get_valid_hdbc_for_bcp(_conn); // Pass the reference _conn

    } catch (const std::runtime_error& e) {
        // Re-throw with more context or just let it propagate
        throw std::runtime_error(std::string("BCPWrapper Constructor: Failed to get valid HDBC - ") + e.what());
    }
}

BCPWrapper::~BCPWrapper() {
    std::cout << "BCPWrapper: Destructor called." << std::endl;
    try {
        close(); 

        if (_conn.is_connected() && SQLSetConnectAttr_ptr) { 
            SQLHDBC hdbc = SQL_NULL_HDBC;
            try {
                 hdbc = _conn.get_hdbc(); 
            } catch (const std::exception& e) {
                std::cout << "BCPWrapper Destructor: Exception getting HDBC to disable BCP mode: " << e.what() << std::endl;
            }

            if (hdbc != SQL_NULL_HDBC) {
                std::cout << "BCPWrapper: Disabling BCP mode on connection in destructor." << std::endl;
                SQLSetConnectAttr_ptr(hdbc, SQL_COPT_SS_BCP, (SQLPOINTER)SQL_BCP_OFF, SQL_IS_INTEGER);
            } else {
                 std::cout << "BCPWrapper Destructor: Could not get HDBC, unable to disable BCP mode explicitly." << std::endl;
            }
        } else {
            std::cout << "BCPWrapper Destructor: Connection not connected, or SQLSetConnectAttr_ptr null; cannot disable BCP mode." << std::endl;
        }
    } catch (const std::exception& e) {
        std::cout << "BCPWrapper Error: Exception in destructor: " << e.what() << std::endl;
    } catch (...) {
        std::cout << "BCPWrapper Error: Unknown exception in destructor." << std::endl;
    }
    std::cout << "BCPWrapper: Destructor finished." << std::endl;
}

SQLRETURN BCPWrapper::bcp_initialize_operation(const std::wstring& table,
                                               const std::wstring& data_file,
                                               const std::wstring& error_file,
                                               const std::wstring& direction) {
    if (_bcp_initialized) {
        std::cout << "BCPWrapper Warning: bcp_initialize_operation called but BCP already initialized. Call finish() or close() first." << std::endl;
        return SQL_ERROR; 
    }
    SQLHDBC hdbc = get_valid_hdbc_for_bcp(_conn); 

    INT dir_code = get_bcp_direction_code(direction);
    
    LPCWSTR pTable = table.c_str();
    LPCWSTR pDataFile = data_file.empty() ? nullptr : data_file.c_str();
    LPCWSTR pErrorFile = error_file.empty() ? nullptr : error_file.c_str();

    std::cout << "BCPWrapper: Calling bcp_initW for table '" << py::cast(table).cast<std::string>() 
              << "', data_file '" << (pDataFile ? py::cast(data_file).cast<std::string>() : "nullptr")
              << "', error_file '" << (pErrorFile ? py::cast(error_file).cast<std::string>() : "nullptr")
              << "', direction '" << py::cast(direction).cast<std::string>() << "'." << std::endl;

    std::wcout << "BCPWrapper: Calling bcp_initW for table '" << pTable 
              << "', data_file '" << pDataFile
              << "', error_file '" << pErrorFile
              << "', direction '" << dir_code << "'." << std::endl;


    // Check if SQL_COPT_SS_BCP is set
    if (SQLGetConnectAttr_ptr) {
        SQLUINTEGER bcp_mode = 0;
        SQLINTEGER bcp_mode_len = 0;
        SQLRETURN ret_ga = SQLGetConnectAttr_ptr(hdbc, SQL_COPT_SS_BCP, &bcp_mode, sizeof(bcp_mode), &bcp_mode_len);
        if (SQL_SUCCEEDED(ret_ga)) {
            if (bcp_mode == SQL_BCP_ON) {
                std::cout << "BCPWrapper: SQL_COPT_SS_BCP is set to SQL_BCP_ON." << bcp_mode << std::endl;
            } else {
                std::cout << "BCPWrapper: SQL_COPT_SS_BCP is NOT set to SQL_BCP_ON. Current value: " << bcp_mode << std::endl;
                    // Optionally, you might want to throw an error here if BCP mode is required
                // throw std::runtime_error("BCPWrapper Constructor: SQL_COPT_SS_BCP is not enabled.");
            }
        } else {
            std::cout << "BCPWrapper: Failed to get SQL_COPT_SS_BCP attribute. Return code: " << ret_ga << std::endl;
            ErrorInfo diag = get_odbc_diagnostics_for_handle(SQL_HANDLE_DBC, hdbc);
            std::wcout << L"BCPWrapper: ODBC Diag for SQLGetConnectAttr: SQLState: " << diag.sqlState 
                        << L", Message: " << diag.ddbcErrorMsg << std::endl;
        }
    } else {
        std::cout << "BCPWrapper: SQLGetConnectAttr_ptr is null, cannot check SQL_COPT_SS_BCP." << std::endl;
    }
    std::cout << "BCPWrapper: BCP mode setup attempted on HDBC." << std::endl; // General message


    SQLHSTMT hstmt = SQL_NULL_HSTMT;
    SQLRETURN retcode = SQLAllocHandle_ptr(SQL_HANDLE_STMT, hdbc, &hstmt);
    if (retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO) {
        std::cout << "BCPWrapper Error: Failed to allocate statement handle for insert." << std::endl;
        return SQL_ERROR;
    }

    std::wstring insert_sql = L"INSERT INTO [TestBCP].[dbo].[EmployeeFullNames] (FirstName, LastName) VALUES (?, ?)";
    retcode = SQLPrepare_ptr(hstmt, (SQLWCHAR*)insert_sql.c_str(), SQL_NTS);
    if (retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO) {
        std::cout << "BCPWrapper Error: Failed to prepare insert statement." << std::endl;
        SQLFreeHandle_ptr(SQL_HANDLE_STMT, hstmt);
        return SQL_ERROR;
    }

    // Example data for demonstration
    std::wstring first_name = L"John";
    std::wstring last_name = L"Doe";

    retcode = SQLBindParameter_ptr(hstmt, 1, SQL_PARAM_INPUT, SQL_C_WCHAR, SQL_WVARCHAR, 100, 0, (SQLPOINTER)first_name.c_str(), 0, nullptr);
    if (retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO) {
        std::cout << "BCPWrapper Error: Failed to bind first parameter." << std::endl;
        SQLFreeHandle_ptr(SQL_HANDLE_STMT, hstmt);
        return SQL_ERROR;
    }
    retcode = SQLBindParameter_ptr(hstmt, 2, SQL_PARAM_INPUT, SQL_C_WCHAR, SQL_WVARCHAR, 100, 0, (SQLPOINTER)last_name.c_str(), 0, nullptr);
    if (retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO) {
        std::cout << "BCPWrapper Error: Failed to bind second parameter." << std::endl;
        SQLFreeHandle_ptr(SQL_HANDLE_STMT, hstmt);
        return SQL_ERROR;
    }

    retcode = SQLExecute_ptr(hstmt);
    if (retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO) {
        std::cout << "BCPWrapper Error: Failed to execute insert statement." << std::endl;
        SQLFreeHandle_ptr(SQL_HANDLE_STMT, hstmt);
        return SQL_ERROR;
    }

    SQLFreeHandle_ptr(SQL_HANDLE_STMT, hstmt);

    SQLRETURN ret = BCPInitW_ptr(hdbc, L"[TestBCP].[dbo].[EmployeeFullNames]", L"C:\\Users\\jathakkar\\OneDrive - Microsoft\\Documents\\Github_mssql_python\\mssql-python\\EmployeeFullNames.bcp", L"C:\\Users\\jathakkar\\OneDrive - Microsoft\\Documents\\Github_mssql_python\\mssql-python\\bcp_wide_error.txt", DB_IN);
    std::cout << "BCPWrapper: HELLOOOO " << ret << std::endl;
    

    
    if (ret != FAIL) {
        _bcp_initialized = true;
        _bcp_finished = false;
        std::cout << "BCPWrapper: bcp_initW successful." << std::endl;
    } else {
        std::cout << "BCPWrapper Error: bcp_initW failed. Ret: " << ret << std::endl;
    }
    return ret;
}

SQLRETURN BCPWrapper::bcp_control(const std::wstring& property_name, int value) {
    if (!_bcp_initialized || _bcp_finished) {
        std::cout << "BCPWrapper Warning: bcp_control(int) called in invalid state (not initialized or already finished)." << std::endl;
        // Throw an exception instead of returning SQL_ERROR for better Python-side error handling
        throw std::runtime_error("BCPWrapper: bcp_control(int) called in invalid state.");
    }
    SQLHDBC hdbc = get_valid_hdbc_for_bcp(_conn);

    auto it = bcp_control_properties.find(property_name);
    if (it == bcp_control_properties.end() || it->second.type != BCPCtrlPropType::INT) {
        std::string msg = "BCPWrapper Error: bcp_control(int) - property '" + py::cast(property_name).cast<std::string>() + "' not found or type mismatch.";
        std::cout << msg << std::endl;
        throw std::runtime_error(msg);
    }
    
    std::cout << "BCPWrapper: Calling bcp_controlW for property '" << py::cast(property_name).cast<std::string>() << "' with int value " << value << "." << std::endl;
    // Correctly pass integer values to bcp_control.
    // The third argument (pvValue) for integer options is typically the value itself, cast to LPVOID,
    // or a pointer to the value. For BCP options like BCPMAXERRS, BCPBATCH, BCPKEEPNULLS, BCPKEEPIDENTITY,
    // passing the value directly cast to LPVOID after ensuring it's the correct size (e.g. SQLLEN) is common.
    SQLRETURN ret = BCPControlW_ptr(hdbc, it->second.option_code, (LPVOID)(SQLLEN)value);
    if (ret == FAIL) {
        std::string msg = "BCPWrapper Error: bcp_controlW (int value) failed for property '" + py::cast(property_name).cast<std::string>() + "'. Ret: " + std::to_string(ret);
        ErrorInfo diag = get_odbc_diagnostics_for_handle(SQL_HANDLE_DBC, hdbc);
        msg += " ODBC Diag: SQLState: " + py::cast(diag.sqlState).cast<std::string>() + ", Message: " + py::cast(diag.ddbcErrorMsg).cast<std::string>();
        std::cout << msg << std::endl;
        throw std::runtime_error(msg);
    }
    return ret;
}

SQLRETURN BCPWrapper::bcp_control(const std::wstring& property_name, const std::wstring& value) {
    if (!_bcp_initialized || _bcp_finished) {
        std::cout << "BCPWrapper Warning: bcp_control(wstring) called in invalid state." << std::endl;
        return SQL_ERROR;
    }
    SQLHDBC hdbc = get_valid_hdbc_for_bcp(_conn);

    auto it = bcp_control_properties.find(property_name);
    if (it == bcp_control_properties.end() || it->second.type != BCPCtrlPropType::WSTRING) {
        std::cout << "BCPWrapper Error: bcp_control(wstring) - property '" << py::cast(property_name).cast<std::string>() << "' not found or type mismatch." << std::endl;
        return SQL_ERROR; 
    }
    
    std::cout << "BCPWrapper: Calling bcp_controlW for property '" << py::cast(property_name).cast<std::string>() << "' with wstring value '" << py::cast(value).cast<std::string>() << "'." << std::endl;
    SQLRETURN ret = BCPControlW_ptr(hdbc, it->second.option_code, (LPVOID)value.c_str());
    if (ret == FAIL) {
        std::cout << "BCPWrapper Error: bcp_controlW (wstring value) failed for property '" << py::cast(property_name).cast<std::string>() << "'. Ret: " << ret << std::endl;
    }
    return ret;
}

SQLRETURN BCPWrapper::read_format_file(const std::wstring& file_path) {
    if (!_bcp_initialized || _bcp_finished) {
        std::cout << "BCPWrapper Warning: read_format_file called in invalid state." << std::endl;
        return SQL_ERROR;
    }
    if (file_path.empty()) {
        std::cout << "BCPWrapper Error: read_format_file - file path cannot be empty." << std::endl;
        return SQL_ERROR;
    }
    SQLHDBC hdbc = get_valid_hdbc_for_bcp(_conn);

    std::cout << "BCPWrapper: Calling bcp_readfmtW for file '" << py::cast(file_path).cast<std::string>() << "'." << std::endl;
    SQLRETURN ret = BCPReadFmtW_ptr(hdbc, file_path.c_str());
    if (ret == FAIL) {
        std::cout << "BCPWrapper Error: bcp_readfmtW failed for file '" << py::cast(file_path).cast<std::string>() << "'. Ret: " << ret << std::endl;
    }
    return ret;
}

SQLRETURN BCPWrapper::define_columns(int num_cols) {
    if (!_bcp_initialized || _bcp_finished) {
        std::cout << "BCPWrapper Warning: define_columns called in invalid state." << std::endl;
        return SQL_ERROR;
    }
    if (num_cols <= 0) {
        std::cout << "BCPWrapper Error: define_columns - invalid number of columns: " << num_cols << std::endl;
        return SQL_ERROR;
    }
    SQLHDBC hdbc = get_valid_hdbc_for_bcp(_conn);

    std::cout << "BCPWrapper: Calling bcp_columns with " << num_cols << " columns." << std::endl;
    SQLRETURN ret = BCPColumns_ptr(hdbc, num_cols);
    if (ret == FAIL) {
        std::cout << "BCPWrapper Error: bcp_columns failed for " << num_cols << " columns. Ret: " << ret << std::endl;
    }
    return ret;
}


SQLRETURN BCPWrapper::define_column_format(int file_col_idx,
                                           int user_data_type,
                                           int indicator_length,
                                           long long user_data_length,
                                           const std::optional<py::bytes>& terminator_bytes_py, 
                                           int server_col_idx) {
    if (!_bcp_initialized || _bcp_finished) {
         throw std::runtime_error("BCPWrapper: define_column_format called in invalid state.");
    }
    SQLHDBC hdbc = get_valid_hdbc_for_bcp(_conn);

    const BYTE* pTerminator = nullptr;
    INT cbTerminate = 0;
    std::string terminator_str_holder; 

    if (terminator_bytes_py) {
        terminator_str_holder = terminator_bytes_py->cast<std::string>(); 
        if (!terminator_str_holder.empty()) {
            pTerminator = reinterpret_cast<const BYTE*>(terminator_str_holder.data());
            cbTerminate = static_cast<INT>(terminator_str_holder.length());
        }
    }
    
    DBINT bcp_user_data_len = static_cast<DBINT>(user_data_length); 

    std::cout << "BCPWrapper: Calling bcp_colfmtW for file_col " << file_col_idx
              << ", server_col " << server_col_idx
              << ", user_data_type " << user_data_type
              << ", indicator_len " << indicator_length
              << ", user_data_len " << static_cast<long long>(bcp_user_data_len)
              << ", terminator_len " << cbTerminate 
              << ", terminator_ptr " << (void*)pTerminator
              << "." << std::endl;
    
    SQLRETURN ret = BCPColFmtW_ptr(hdbc,
                                   file_col_idx,
                                   static_cast<BYTE>(user_data_type),
                                   indicator_length,
                                   bcp_user_data_len,
                                   pTerminator, 
                                   cbTerminate, 
                                   server_col_idx
                                   );
    if (ret == FAIL) {
        std::string msg = "BCPWrapper Error: bcp_colfmtW failed for file_col " + std::to_string(file_col_idx)
                  + ", server_col " + std::to_string(server_col_idx)
                  + ". Ret: " + std::to_string(ret);
        ErrorInfo diag = get_odbc_diagnostics_for_handle(SQL_HANDLE_DBC, hdbc);
        msg += " ODBC Diag: SQLState: " + py::cast(diag.sqlState).cast<std::string>() + ", Message: " + py::cast(diag.ddbcErrorMsg).cast<std::string>();
        std::cout << msg << std::endl;
        throw std::runtime_error(msg);
    }
    return ret;
}

SQLRETURN BCPWrapper::exec_bcp() {
    if (!_bcp_initialized || _bcp_finished) {
        throw std::runtime_error("BCPWrapper: exec_bcp called in invalid state.");
    }
    SQLHDBC hdbc = get_valid_hdbc_for_bcp(_conn);

    DBINT rows_copied_in_batch = 0; 
    std::cout << "BCPWrapper: Calling bcp_exec." << std::endl;
    DBINT bcp_ret = BCPExec_ptr(hdbc, &rows_copied_in_batch);
    
    if (bcp_ret == FAIL) { 
        std::string msg = "BCPWrapper Error: bcp_exec failed (returned -1). Rows in this batch (if any before error): " + std::to_string(static_cast<long long>(rows_copied_in_batch));
        ErrorInfo diag = get_odbc_diagnostics_for_handle(SQL_HANDLE_DBC, hdbc);
        msg += " ODBC Diag: SQLState: " + py::cast(diag.sqlState).cast<std::string>() + ", Message: " + py::cast(diag.ddbcErrorMsg).cast<std::string>();
        std::cout << msg << std::endl;
        throw std::runtime_error(msg);
    }
    std::cout << "BCPWrapper: bcp_exec returned " << static_cast<long long>(bcp_ret) << ". Rows parameter output: " << static_cast<long long>(rows_copied_in_batch) << std::endl;
    return SQL_SUCCESS; 
}

SQLRETURN BCPWrapper::finish() {
    if (!_bcp_initialized) {
        std::cout << "BCPWrapper Info: finish called but BCP not initialized. No action taken." << std::endl;
        return SQL_SUCCESS; 
    }
    if (_bcp_finished) {
        std::cout << "BCPWrapper Info: finish called but BCP already finished. No action taken." << std::endl;
        return SQL_SUCCESS;
    }

    SQLHDBC hdbc = get_valid_hdbc_for_bcp(_conn);

    std::cout << "BCPWrapper: Calling bcp_done." << std::endl;
    SQLRETURN ret = BCPDone_ptr(hdbc);
    if (ret != FAIL) {
        _bcp_finished = true;
        std::cout << "BCPWrapper: bcp_done successful." << std::endl;
    } else {
        std::cout << "BCPWrapper Error: bcp_done failed. Ret: " << ret << std::endl;
    }
    return ret;
}

SQLRETURN BCPWrapper::close() {
    std::cout << "BCPWrapper: close() called." << std::endl;
    SQLRETURN ret = SQL_SUCCESS;
    if (_bcp_initialized && !_bcp_finished) {
        std::cout << "BCPWrapper: Active BCP operation found in close(), calling finish()." << std::endl;
        ret = finish();
    }
    return ret;
}