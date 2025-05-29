#include "bcp_wrapper.h" // Includes ddbc_bindings.h (and thus sql.h, sqlext.h, <string>, <memory>) and connection.h

// Pybind11 headers (needed for py::cast and potentially std::optional if used with pybind types)
#include <pybind11/pybind11.h>
#include <pybind11/stl.h> 

// Standard C++ headers (not covered by ddbc_bindings.h)
#include <vector>
#include <stdexcept>
#include <unordered_map>

namespace py = pybind11; // Alias for pybind11 namespace

namespace { // Anonymous namespace for internal helpers

// Helper to manage BCP properties for bcp_control
enum class BCPCtrlPropType { INT, WSTRING };
struct BCPCtrlPropertyInfo {
    INT option_code;
    BCPCtrlPropType type;
};

// Map property names to their ODBC codes and types
const std::unordered_map<std::wstring, BCPCtrlPropertyInfo> bcp_control_properties = {
    {L"BCPMAXERRS",     {BCPMAXERRS,        BCPCtrlPropType::INT}},
    {L"BCPFIRST",       {BCPFIRSTROW,       BCPCtrlPropType::INT}},
    {L"BCPLAST",        {BCPLASTROW,        BCPCtrlPropType::INT}},
    {L"BCPBATCH",       {BCPBATCH,          BCPCtrlPropType::INT}},
    {L"BCPKEEPNULLS",   {BCPKEEPNULLS,      BCPCtrlPropType::INT}},
    {L"BCPKEEPIDENTITY",{BCPKEEPIDENTITY,   BCPCtrlPropType::INT}},
    {L"BCPHINTS",       {BCPHINTS,          BCPCtrlPropType::WSTRING}},
    {L"BCPFILECP",      {BCPFILECP,         BCPCtrlPropType::INT}},
    {L"BCPSETROWTERM",  {BCPROWTERM,        BCPCtrlPropType::WSTRING}},
};

// Helper for bcp_init direction string
INT get_bcp_direction_code(const std::wstring& direction_str) {
    if (direction_str == L"in" || direction_str == L"IN") return DB_IN;
    if (direction_str == L"out" || direction_str == L"OUT") return DB_OUT;
    throw std::runtime_error("Invalid BCP direction string: " + py::cast(direction_str).cast<std::string>());
}

// Helper to get HDBC, ensuring driver and BCP function pointers are loaded.
// Assumes Connection class has get_hdbc() and is_connected() methods.
SQLHDBC get_valid_hdbc_for_bcp(const std::shared_ptr<Connection>& conn) {
    if (!conn || !conn->is_connected()) {
        LOG("BCPWrapper: Connection is null or not connected.");
        throw std::runtime_error("BCPWrapper: Connection is null or not connected.");
    }

    // Check critical BCP and related function pointers
    if (!SQLSetConnectAttr_ptr || !BCPInitW_ptr || !BCPControlW_ptr || 
        !BCPReadFmtW_ptr || !BCPColumns_ptr || !BCPColFmtW_ptr || 
        !BCPExec_ptr || !BCPDone_ptr || !BCPSetBulkMode_ptr) { // Added BCPSetBulkMode_ptr
         LOG("BCPWrapper: Critical ODBC/BCP function pointers not loaded. Attempting to load via DriverLoader.");
         DriverLoader::getInstance().loadDriver(); 
         if (!SQLSetConnectAttr_ptr || !BCPInitW_ptr || !BCPControlW_ptr || 
             !BCPReadFmtW_ptr || !BCPColumns_ptr || !BCPColFmtW_ptr || 
             !BCPExec_ptr || !BCPDone_ptr || !BCPSetBulkMode_ptr) { // Added BCPSetBulkMode_ptr
            LOG("BCPWrapper Error: ODBC/BCP function pointers still not loaded after attempt.");
            throw std::runtime_error("BCPWrapper: ODBC/BCP function pointers not loaded.");
         }
    }

    SQLHDBC hdbc = conn->get_hdbc(); 
    if (hdbc == SQL_NULL_HDBC) {
        LOG("BCPWrapper Error: Failed to get HDBC from Connection object.");
        throw std::runtime_error("BCPWrapper: Failed to get HDBC from Connection object.");
    }
    return hdbc;
}

} // anonymous namespace

BCPWrapper::BCPWrapper(std::shared_ptr<Connection> conn)
    : _conn(conn), _bcp_initialized(false), _bcp_finished(true) {
    if (!_conn) {
        LOG("BCPWrapper Error: Connection object provided to constructor is null.");
        throw std::invalid_argument("BCPWrapper: Connection object cannot be null.");
    }
    
    SQLHDBC hdbc = get_valid_hdbc_for_bcp(_conn); // This also checks conn status and loads func ptrs

    LOG("BCPWrapper: Enabling BCP mode on connection.");
    SQLRETURN ret = SQLSetConnectAttr_ptr(hdbc, SQL_COPT_SS_BCP, (SQLPOINTER)SQL_BCP_ON, SQL_IS_INTEGER);
    if (!SQL_SUCCEEDED(ret)) {
        // TODO: Use SQLGetDiagRec_ptr to get detailed error from hdbc
        LOG("BCPWrapper Error: Failed to enable BCP mode on connection (SQLSetConnectAttr SQL_COPT_SS_BCP). Ret: %d", ret);
        throw std::runtime_error("BCPWrapper: Failed to enable BCP mode on connection.");
    }
    LOG("BCPWrapper: BCP mode enabled successfully.");
}

BCPWrapper::~BCPWrapper() {
    LOG("BCPWrapper: Destructor called.");
    try {
        close(); // Ensure any active BCP operation is finished

        if (_conn && _conn->is_connected() && SQLSetConnectAttr_ptr) {
            // Check if get_hdbc() might throw or return null if connection was closed externally
            SQLHDBC hdbc = SQL_NULL_HDBC;
            try {
                 hdbc = _conn->get_hdbc();
            } catch (const std::exception& e) {
                LOG("BCPWrapper Destructor: Exception getting HDBC to disable BCP mode: %s", e.what());
            }

            if (hdbc != SQL_NULL_HDBC) {
                LOG("BCPWrapper: Disabling BCP mode on connection in destructor.");
                SQLSetConnectAttr_ptr(hdbc, SQL_COPT_SS_BCP, (SQLPOINTER)SQL_BCP_OFF, SQL_IS_INTEGER);
                // Ignore return code in destructor, best effort.
            } else {
                 LOG("BCPWrapper Destructor: Could not get HDBC, unable to disable BCP mode explicitly.");
            }
        } else {
            LOG("BCPWrapper Destructor: Connection null, not connected, or SQLSetConnectAttr_ptr null; cannot disable BCP mode.");
        }
    } catch (const std::exception& e) {
        LOG("BCPWrapper Error: Exception in destructor: %s", e.what());
    } catch (...) {
        LOG("BCPWrapper Error: Unknown exception in destructor.");
    }
    LOG("BCPWrapper: Destructor finished.");
}

SQLRETURN BCPWrapper::bcp_initialize_operation(const std::wstring& table,
                                               const std::wstring& data_file,
                                               const std::wstring& error_file,
                                               const std::wstring& direction) {
    if (_bcp_initialized) {
        LOG("BCPWrapper Warning: bcp_initialize_operation called but BCP already initialized. Call finish() or close() first.");
        return SQL_ERROR; 
    }
    SQLHDBC hdbc = get_valid_hdbc_for_bcp(_conn);

    INT dir_code = get_bcp_direction_code(direction);
    
    LPCWSTR pTable = table.c_str();
    LPCWSTR pDataFile = data_file.empty() ? nullptr : data_file.c_str();
    LPCWSTR pErrorFile = error_file.empty() ? nullptr : error_file.c_str();

    LOG("BCPWrapper: Calling bcp_initW for table '%ls', direction '%ls'.", table.c_str(), direction.c_str());
    SQLRETURN ret = BCPInitW_ptr(hdbc, pTable, pDataFile, pErrorFile, dir_code);
    
    if (SQL_SUCCEEDED(ret)) {
        _bcp_initialized = true;
        _bcp_finished = false;
        LOG("BCPWrapper: bcp_initW successful.");
    } else {
        LOG("BCPWrapper Error: bcp_initW failed. Ret: %d", ret);
    }
    return ret;
}

SQLRETURN BCPWrapper::bcp_control(const std::wstring& property_name, int value) {
    if (!_bcp_initialized || _bcp_finished) {
        LOG("BCPWrapper Warning: bcp_control(int) called in invalid state (not initialized or already finished).");
        return SQL_ERROR;
    }
    SQLHDBC hdbc = get_valid_hdbc_for_bcp(_conn);
    // BCPControlW_ptr already checked in get_valid_hdbc_for_bcp

    auto it = bcp_control_properties.find(property_name);
    if (it == bcp_control_properties.end() || it->second.type != BCPCtrlPropType::INT) {
        LOG("BCPWrapper Error: bcp_control(int) - property '%ls' not found or type mismatch.", property_name.c_str());
        return SQL_ERROR; 
    }
    
    LOG("BCPWrapper: Calling bcp_controlW for property '%ls' with int value %d.", property_name.c_str(), value);
    SQLRETURN ret = BCPControlW_ptr(hdbc, it->second.option_code, (LPVOID)&value);
    if (!SQL_SUCCEEDED(ret)) {
        LOG("BCPWrapper Error: bcp_controlW (int value) failed for property '%ls'. Ret: %d", property_name.c_str(), ret);
    }
    return ret;
}

SQLRETURN BCPWrapper::bcp_control(const std::wstring& property_name, const std::wstring& value) {
    if (!_bcp_initialized || _bcp_finished) {
        LOG("BCPWrapper Warning: bcp_control(wstring) called in invalid state.");
        return SQL_ERROR;
    }
    SQLHDBC hdbc = get_valid_hdbc_for_bcp(_conn);
    // BCPControlW_ptr already checked in get_valid_hdbc_for_bcp

    auto it = bcp_control_properties.find(property_name);
    if (it == bcp_control_properties.end() || it->second.type != BCPCtrlPropType::WSTRING) {
        LOG("BCPWrapper Error: bcp_control(wstring) - property '%ls' not found or type mismatch.", property_name.c_str());
        return SQL_ERROR; 
    }
    
    LOG("BCPWrapper: Calling bcp_controlW for property '%ls' with wstring value '%ls'.", property_name.c_str(), value.c_str());
    SQLRETURN ret = BCPControlW_ptr(hdbc, it->second.option_code, (LPVOID)value.c_str());
    if (!SQL_SUCCEEDED(ret)) {
        LOG("BCPWrapper Error: bcp_controlW (wstring value) failed for property '%ls'. Ret: %d", property_name.c_str(), ret);
    }
    return ret;
}

SQLRETURN BCPWrapper::set_bulk_mode(const std::wstring& mode,
                                    const std::optional<std::vector<unsigned char>>& field_terminator,
                                    const std::optional<std::vector<unsigned char>>& row_terminator) {
    LOG("BCPWrapper: Setting bulk mode to '%ls' using bcp_setbulkmode.", mode.c_str());

    if (!_bcp_initialized || _bcp_finished) {
        LOG("BCPWrapper Warning: set_bulk_mode called in invalid state (not initialized or already finished).");
        return SQL_ERROR;
    }

    SQLHDBC hdbc = get_valid_hdbc_for_bcp(_conn); 

    INT bcp_property_code;
    if (mode == L"char") {
        bcp_property_code = BCP_OUT_CHARACTER_MODE;
    } else if (mode == L"native") {
        bcp_property_code = BCP_OUT_NATIVE_MODE;
    } else {
        LOG("BCPWrapper Error: set_bulk_mode - invalid mode string: '%ls'. Expected 'char' or 'native' for bcp_setbulkmode.", mode.c_str());
        return SQL_ERROR;
    }

    const unsigned char* pField = nullptr;
    INT cbField = 0;
    if (field_terminator && !field_terminator->empty()) {
        pField = field_terminator->data();
        cbField = static_cast<INT>(field_terminator->size());
        LOG("BCPWrapper: Using custom field terminator with length %d for bcp_setbulkmode.", cbField);
    }

    const unsigned char* pRow = nullptr;
    INT cbRow = 0;
    if (row_terminator && !row_terminator->empty()) {
        pRow = row_terminator->data();
        cbRow = static_cast<INT>(row_terminator->size());
        LOG("BCPWrapper: Using custom row terminator with length %d for bcp_setbulkmode.", cbRow);
    }

    LOG("BCPWrapper: Calling BCPSetBulkMode_ptr with property code %d.", bcp_property_code);
    // Note: bcp_setbulkmode's pField and pRow are LPVOID, so casting is appropriate.
    SQLRETURN ret = BCPSetBulkMode_ptr(hdbc, bcp_property_code, (LPVOID)pField, cbField, (LPVOID)pRow, cbRow);

    if (!SQL_SUCCEEDED(ret)) {
        LOG("BCPWrapper Error: BCPSetBulkMode_ptr failed for mode '%ls' (property code %d). Ret: %d", mode.c_str(), bcp_property_code, ret);
    } else {
        LOG("BCPWrapper: BCPSetBulkMode_ptr successful for mode '%ls'.", mode.c_str());
    }
    return ret;
}

SQLRETURN BCPWrapper::read_format_file(const std::wstring& file_path) {
    if (!_bcp_initialized || _bcp_finished) {
        LOG("BCPWrapper Warning: read_format_file called in invalid state.");
        return SQL_ERROR;
    }
    if (file_path.empty()) {
        LOG("BCPWrapper Error: read_format_file - file path cannot be empty.");
        return SQL_ERROR;
    }
    SQLHDBC hdbc = get_valid_hdbc_for_bcp(_conn);

    LOG("BCPWrapper: Calling bcp_readfmtW for file '%ls'.", file_path.c_str());
    SQLRETURN ret = BCPReadFmtW_ptr(hdbc, file_path.c_str());
    if (!SQL_SUCCEEDED(ret)) {
        LOG("BCPWrapper Error: bcp_readfmtW failed for file '%ls'. Ret: %d", file_path.c_str(), ret);
    }
    return ret;
}

SQLRETURN BCPWrapper::define_columns(int num_cols) {
    if (!_bcp_initialized || _bcp_finished) {
        LOG("BCPWrapper Warning: define_columns called in invalid state.");
        return SQL_ERROR;
    }
    if (num_cols <= 0) {
        LOG("BCPWrapper Error: define_columns - invalid number of columns: %d", num_cols);
        return SQL_ERROR;
    }
    SQLHDBC hdbc = get_valid_hdbc_for_bcp(_conn);
    // BCPColumns_ptr already checked in get_valid_hdbc_for_bcp

    LOG("BCPWrapper: Calling bcp_columns with %d columns.", num_cols);
    SQLRETURN ret = BCPColumns_ptr(hdbc, num_cols);
    if (!SQL_SUCCEEDED(ret)) {
        LOG("BCPWrapper Error: bcp_columns failed for %d columns. Ret: %d", num_cols, ret);
    }
    return ret;
}


SQLRETURN BCPWrapper::define_column_format(int file_col_idx,
                                           int user_data_type,
                                           int indicator_length,
                                           long long user_data_length,
                                           const std::optional<std::vector<unsigned char>>& terminator_bytes,
                                           int server_col_idx) {
    if (!_bcp_initialized || _bcp_finished) {
        LOG("BCPWrapper Warning: define_column_format called in invalid state.");
        return SQL_ERROR;
    }
    SQLHDBC hdbc = get_valid_hdbc_for_bcp(_conn);
    // BCPColFmtW_ptr (or BCPColFmt_ptr if using non-Unicode) already checked in get_valid_hdbc_for_bcp

    const BYTE* pTerminator = nullptr;
    INT cbTerminate = 0; 
    
    if (terminator_bytes && !terminator_bytes->empty()) {
        pTerminator = reinterpret_cast<const BYTE*>(terminator_bytes->data());
        cbTerminate = static_cast<INT>(terminator_bytes->size());
        LOG("BCPWrapper: Using custom terminator for file_col %d with length %d.", file_col_idx, cbTerminate);
    }
    
    DBINT bcp_user_data_len = static_cast<DBINT>(user_data_length); 

    LOG("BCPWrapper: Calling bcp_colfmtW for file_col %d, server_col %d, user_data_type %d, indicator_len %d, user_data_len %lld.", 
        file_col_idx, server_col_idx, user_data_type, indicator_length, static_cast<long long>(bcp_user_data_len));
    
    // Assuming BCPColFmtW_ptr is the correct function pointer for wide character format files if applicable,
    // or BCPColFmt_ptr for non-Unicode. The parameters match bcp_colfmt.
    SQLRETURN ret = BCPColFmtW_ptr(hdbc,             // HDBC hdbc
                                   file_col_idx,     // INT idxUserDataCol
                                   static_cast<BYTE>(user_data_type), // BYTE eUserDataType
                                   indicator_length, // INT cbIndicator
                                   bcp_user_data_len,// DBINT cbUserData
                                   pTerminator,      // LPCBYTE pUserDataTerm
                                   cbTerminate,      // INT cbUserDataTerm
                                   server_col_idx    // INT idxServerCol
                                   );
    if (!SQL_SUCCEEDED(ret)) {
        LOG("BCPWrapper Error: bcp_colfmtW failed for file_col %d, server_col %d. Ret: %d", file_col_idx, server_col_idx, ret);
    }
    return ret;
}

SQLRETURN BCPWrapper::exec_bcp() {
    if (!_bcp_initialized || _bcp_finished) {
        LOG("BCPWrapper Warning: exec_bcp called in invalid state.");
        return SQL_ERROR;
    }
    SQLHDBC hdbc = get_valid_hdbc_for_bcp(_conn);

    DBINT rows_copied_in_batch = 0; 
    LOG("BCPWrapper: Calling bcp_exec.");
    DBINT bcp_ret = BCPExec_ptr(hdbc, &rows_copied_in_batch);
    
    if (bcp_ret == -1) { 
        LOG("BCPWrapper Error: bcp_exec failed (returned -1). Rows in this batch (if any before error): %lld", static_cast<long long>(rows_copied_in_batch));
        return SQL_ERROR;
    }
    LOG("BCPWrapper: bcp_exec returned %lld. Rows parameter output: %lld", static_cast<long long>(bcp_ret), static_cast<long long>(rows_copied_in_batch));
    return SQL_SUCCESS; 
}

SQLRETURN BCPWrapper::finish() {
    if (!_bcp_initialized) {
        LOG("BCPWrapper Info: finish called but BCP not initialized. No action taken.");
        return SQL_SUCCESS; 
    }
    if (_bcp_finished) {
        LOG("BCPWrapper Info: finish called but BCP already finished. No action taken.");
        return SQL_SUCCESS;
    }

    SQLHDBC hdbc = get_valid_hdbc_for_bcp(_conn);
    // BCPDone_ptr already checked in get_valid_hdbc_for_bcp

    LOG("BCPWrapper: Calling bcp_done.");
    SQLRETURN ret = BCPDone_ptr(hdbc);
    if (SQL_SUCCEEDED(ret)) {
        _bcp_finished = true;
        LOG("BCPWrapper: bcp_done successful.");
    } else {
        LOG("BCPWrapper Error: bcp_done failed. Ret: %d", ret);
    }
    return ret;
}

SQLRETURN BCPWrapper::close() {
    LOG("BCPWrapper: close() called.");
    SQLRETURN ret = SQL_SUCCESS;
    if (_bcp_initialized && !_bcp_finished) {
        LOG("BCPWrapper: Active BCP operation found in close(), calling finish().");
        ret = finish();
    }
    return ret;
}