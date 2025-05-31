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

#define BCP_OUT_CHARACTER_MODE      0x01
#define BCP_OUT_WIDE_CHARACTER_MODE 0x02
#define BCP_OUT_NATIVE_TEXT_MODE    0x03
#define BCP_OUT_NATIVE_MODE         0x04

// Helper to manage BCP properties for bcp_control
enum class BCPCtrlPropType { INT, WSTRING };
struct BCPCtrlPropertyInfo {
    INT option_code;
    BCPCtrlPropType type;
};

// Map property names to their ODBC codes and types
const std::unordered_map<std::wstring, BCPCtrlPropertyInfo> bcp_control_properties = {
    {L"BCPMAXERRS",     {BCPMAXERRS,        BCPCtrlPropType::INT}},
    // {L"BCPFIRST",       {BCPFIRSTROW,       BCPCtrlPropType::INT}},
    // {L"BCPLAST",        {BCPLASTROW,        BCPCtrlPropType::INT}},
    {L"BCPBATCH",       {BCPBATCH,          BCPCtrlPropType::INT}},
    {L"BCPKEEPNULLS",   {BCPKEEPNULLS,      BCPCtrlPropType::INT}},
    {L"BCPKEEPIDENTITY",{BCPKEEPIDENTITY,   BCPCtrlPropType::INT}},
    {L"BCPHINTS",       {BCPHINTS,          BCPCtrlPropType::WSTRING}},
    {L"BCPFILECP",      {BCPFILECP,         BCPCtrlPropType::INT}},
    // {L"BCPSETROWTERM",  {BCPROWTERM,        BCPCtrlPropType::WSTRING}},
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
    if (!SQLSetConnectAttr_ptr || !BCPInitW_ptr || !BCPControlW_ptr || 
        !BCPReadFmtW_ptr || !BCPColumns_ptr || !BCPColFmtW_ptr || 
        !BCPExec_ptr || !BCPDone_ptr || !BCPSetBulkMode_ptr) { // Added BCPSetBulkMode_ptr
         std::cout << "BCPWrapper: Critical ODBC/BCP function pointers not loaded. Attempting to load via DriverLoader." << std::endl;
         DriverLoader::getInstance().loadDriver(); 
         if (!SQLSetConnectAttr_ptr || !BCPInitW_ptr || !BCPControlW_ptr || 
             !BCPReadFmtW_ptr || !BCPColumns_ptr || !BCPColFmtW_ptr || 
             !BCPExec_ptr || !BCPDone_ptr || !BCPSetBulkMode_ptr) { // Added BCPSetBulkMode_ptr
            std::cout << "BCPWrapper Error: ODBC/BCP function pointers still not loaded after attempt." << std::endl;
            throw std::runtime_error("BCPWrapper: ODBC/BCP function pointers not loaded.");
         }
    }

    SQLHDBC hdbc = conn.get_hdbc(); // Use . instead of ->
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
        std::cout << "BCPWrapper: BCP mode setup attempted on HDBC." << std::endl; // General message
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
    SQLRETURN ret = BCPInitW_ptr(hdbc, pTable, pDataFile, pErrorFile, dir_code);
    
    if (SQL_SUCCEEDED(ret)) {
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
        return SQL_ERROR;
    }
    SQLHDBC hdbc = get_valid_hdbc_for_bcp(_conn);

    auto it = bcp_control_properties.find(property_name);
    if (it == bcp_control_properties.end() || it->second.type != BCPCtrlPropType::INT) {
        std::cout << "BCPWrapper Error: bcp_control(int) - property '" << py::cast(property_name).cast<std::string>() << "' not found or type mismatch." << std::endl;
        return SQL_ERROR; 
    }
    
    std::cout << "BCPWrapper: Calling bcp_controlW for property '" << py::cast(property_name).cast<std::string>() << "' with int value " << value << "." << std::endl;
    SQLRETURN ret = BCPControlW_ptr(hdbc, it->second.option_code, (LPVOID)&value);
    if (!SQL_SUCCEEDED(ret)) {
        std::cout << "BCPWrapper Error: bcp_controlW (int value) failed for property '" << py::cast(property_name).cast<std::string>() << "'. Ret: " << ret << std::endl;
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
    if (!SQL_SUCCEEDED(ret)) {
        std::cout << "BCPWrapper Error: bcp_controlW (wstring value) failed for property '" << py::cast(property_name).cast<std::string>() << "'. Ret: " << ret << std::endl;
    }
    return ret;
}

SQLRETURN BCPWrapper::set_bulk_mode(const std::wstring& mode,
                                    const std::optional<py::bytes>& field_terminator_py, 
                                    const std::optional<py::bytes>& row_terminator_py) {   
    std::cout << "BCPWrapper: Setting bulk mode to '" << py::cast(mode).cast<std::string>() << "' using bcp_setbulkmode." << std::endl;

    if (!_bcp_initialized || _bcp_finished) {
        std::cout << "BCPWrapper Warning: set_bulk_mode called in invalid state (not initialized or already finished)." << std::endl;
        return SQL_ERROR;
    }

    SQLHDBC hdbc = get_valid_hdbc_for_bcp(_conn);

    INT bcp_property_code;
    if (mode == L"char") {
        bcp_property_code = BCP_OUT_CHARACTER_MODE;
    } else if (mode == L"native") {
        bcp_property_code = BCP_OUT_NATIVE_MODE;
    } else if (mode == L"unicode") { 
        bcp_property_code = BCP_OUT_WIDE_CHARACTER_MODE; 
    }
    else {
        std::cout << "BCPWrapper Error: set_bulk_mode - invalid mode string: '" << py::cast(mode).cast<std::string>() << "'." << std::endl;
        return SQL_ERROR;
    }

    const unsigned char* pField = nullptr;
    INT cbField = 0;
    std::string field_terminator_str; 

    if (field_terminator_py) {
        field_terminator_str = field_terminator_py->cast<std::string>(); 
        if (!field_terminator_str.empty()) {
            pField = reinterpret_cast<const unsigned char*>(field_terminator_str.data());
            cbField = static_cast<INT>(field_terminator_str.length());
            std::cout << "BCPWrapper: Using custom field terminator with length " << cbField << " for bcp_setbulkmode." << std::endl;
        }
    }

    const unsigned char* pRow = nullptr;
    INT cbRow = 0;
    std::string row_terminator_str; 

    if (row_terminator_py) {
        row_terminator_str = row_terminator_py->cast<std::string>(); 
        if (!row_terminator_str.empty()) {
            pRow = reinterpret_cast<const unsigned char*>(row_terminator_str.data());
            cbRow = static_cast<INT>(row_terminator_str.length());
            std::cout << "BCPWrapper: Using custom row terminator with length " << cbRow << " for bcp_setbulkmode." << std::endl;
        }
    }

    std::cout << "BCPWrapper: Calling BCPSetBulkMode_ptr with property code " << bcp_property_code << "." << std::endl;
    SQLRETURN ret = BCPSetBulkMode_ptr(hdbc, bcp_property_code, (LPVOID)pField, cbField, (LPVOID)pRow, cbRow);

    if (!SQL_SUCCEEDED(ret)) {
        std::cout << "BCPWrapper Error: BCPSetBulkMode_ptr failed for mode '" << py::cast(mode).cast<std::string>() << "' (property code " << bcp_property_code << "). Ret: " << ret << std::endl;
    } else {
        std::cout << "BCPWrapper: BCPSetBulkMode_ptr successful for mode '" << py::cast(mode).cast<std::string>() << "'." << std::endl;
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
    if (!SQL_SUCCEEDED(ret)) {
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
    if (!SQL_SUCCEEDED(ret)) {
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
        std::cout << "BCPWrapper Warning: define_column_format called in invalid state." << std::endl;
        return SQL_ERROR;
    }
    SQLHDBC hdbc = get_valid_hdbc_for_bcp(_conn);

    const BYTE* pTerminator = nullptr;
    INT cbTerminate = 0;
    std::string terminator_str; 

    if (terminator_bytes_py) {
        terminator_str = terminator_bytes_py->cast<std::string>(); 
        if (!terminator_str.empty()) {
            pTerminator = reinterpret_cast<const BYTE*>(terminator_str.data());
            cbTerminate = static_cast<INT>(terminator_str.length());
            std::cout << "BCPWrapper: Using custom terminator for file_col " << file_col_idx
                      << " with length " << cbTerminate << "." << std::endl;
        }
    }
    
    DBINT bcp_user_data_len = static_cast<DBINT>(user_data_length); 

    std::cout << "BCPWrapper: Calling bcp_colfmtW for file_col " << file_col_idx
              << ", server_col " << server_col_idx
              << ", user_data_type " << user_data_type
              << ", indicator_len " << indicator_length
              << ", user_data_len " << static_cast<long long>(bcp_user_data_len)
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
    if (!SQL_SUCCEEDED(ret)) {
        std::cout << "BCPWrapper Error: bcp_colfmtW failed for file_col " << file_col_idx
                  << ", server_col " << server_col_idx
                  << ". Ret: " << ret << std::endl;
    }
    return ret;
}

SQLRETURN BCPWrapper::exec_bcp() {
    if (!_bcp_initialized || _bcp_finished) {
        std::cout << "BCPWrapper Warning: exec_bcp called in invalid state." << std::endl;
        return SQL_ERROR;
    }
    SQLHDBC hdbc = get_valid_hdbc_for_bcp(_conn);

    DBINT rows_copied_in_batch = 0; 
    std::cout << "BCPWrapper: Calling bcp_exec." << std::endl;
    DBINT bcp_ret = BCPExec_ptr(hdbc, &rows_copied_in_batch);
    
    if (bcp_ret == -1) { 
        std::cout << "BCPWrapper Error: bcp_exec failed (returned -1). Rows in this batch (if any before error): " << static_cast<long long>(rows_copied_in_batch) << std::endl;
        return SQL_ERROR;
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
    if (SQL_SUCCEEDED(ret)) {
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