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
    if (direction_str == L"in") return DB_IN;
    if (direction_str == L"out" || direction_str == L"queryout") return DB_OUT;
    throw std::runtime_error("Invalid BCP direction string: " + py::cast(direction_str).cast<std::string>());
}

// Helper function (can be a static private method or a lambda in C++11 and later)
// to retrieve ODBC diagnostic messages and populate ErrorInfo.
// This uses SQLGetDiagRec_ptr directly, which is loaded by DriverLoader.
static ErrorInfo get_odbc_diagnostics_for_handle(SQLSMALLINT handle_type, SQLHANDLE handle) {
    ErrorInfo error_info;
    error_info.sqlState = L""; // Initialize
    error_info.ddbcErrorMsg = L""; // Initialize

    if (!SQLGetDiagRec_ptr) {
        LOG("get_odbc_diagnostics_for_handle: SQLGetDiagRec_ptr is null.");
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

BCPWrapper::BCPWrapper(ConnectionHandle& conn) // Changed to Connection&
    : _bcp_initialized(false), _bcp_finished(true) { // Initialize reference
    try {
        _hdbc = conn.getConnection()->getDbcHandle()->get();
        if (!_hdbc || _hdbc == SQL_NULL_HDBC) {
            LOG("BCPWrapper Error: Invalid HDBC from Connection object.");
            throw std::runtime_error("BCPWrapper: Invalid HDBC from Connection object.");
        }
    } catch (const std::runtime_error& e) {
        // Re-throw with more context or just let it propagate
        throw std::runtime_error(std::string("BCPWrapper Constructor: Failed to get valid HDBC - ") + e.what());
    }
}

BCPWrapper::~BCPWrapper() {
    LOG("BCPWrapper: Destructor called.");
    // try {
    //     close(); 
    // } catch (const std::exception& e) {
    //     LOG("BCPWrapper Error: Exception in destructor: " + std::string(e.what()));
    // } catch (...) {
    //     LOG("BCPWrapper Error: Unknown exception in destructor.");
    // }
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

    INT dir_code = get_bcp_direction_code(direction);

    LPCWSTR pTable = table.empty() ? nullptr : table.c_str();
    LPCWSTR pDataFile = data_file.empty() ? nullptr : data_file.c_str();
    LPCWSTR pErrorFile = error_file.empty() ? nullptr : error_file.c_str();

    LOG("BCPWrapper: Calling BCPInitW_ptr with HDBC: " + std::to_string(reinterpret_cast<uintptr_t>(_hdbc)) + ", table: " 
        + (pTable ? py::cast(table).cast<std::string>() : "nullptr") 
        + ", data_file: " + (pDataFile ? py::cast(data_file).cast<std::string>() : "nullptr")
        + ", error_file: " + (pErrorFile ? py::cast(error_file).cast<std::string>() : "nullptr")
        + ", direction code: " + std::to_string(dir_code));
    // Call BCPInitW with the correct parameters
    SQLRETURN ret = BCPInitW_ptr(_hdbc, pTable, pDataFile, pErrorFile, dir_code);
    LOG("BCPWrapper: HELLOOOO " + std::to_string(ret));
    
    if (ret != FAIL) {
        _bcp_initialized = true;
        _bcp_finished = false;
        LOG("BCPWrapper: bcp_initW successful.");
    } else {
        LOG("BCPWrapper Error: bcp_initW failed. Ret: " + std::to_string(ret));
    }
    return ret;
}

SQLRETURN BCPWrapper::bcp_control(const std::wstring& property_name, int value) {
    if (!_bcp_initialized || _bcp_finished) {
        LOG("BCPWrapper Warning: bcp_control(int) called in invalid state (not initialized or already finished).");
        // Throw an exception instead of returning SQL_ERROR for better Python-side error handling
        throw std::runtime_error("BCPWrapper: bcp_control(int) called in invalid state.");
    }

    auto it = bcp_control_properties.find(property_name);
    if (it == bcp_control_properties.end() || it->second.type != BCPCtrlPropType::INT) {
        std::string msg = "BCPWrapper Error: bcp_control(int) - property '" + py::cast(property_name).cast<std::string>() + "' not found or type mismatch.";
        LOG(msg);
        throw std::runtime_error(msg);
    }
    
    LOG("BCPWrapper: Calling bcp_controlW for property '" + py::cast(property_name).cast<std::string>() + "' with int value " + std::to_string(value) + ".");
    // Correctly pass integer values to bcp_control.
    SQLRETURN ret = BCPControlW_ptr(_hdbc, it->second.option_code, (LPVOID)(SQLLEN)value);
    if (ret == FAIL) {
        std::string msg = "BCPWrapper Error: bcp_controlW (int value) failed for property '" + py::cast(property_name).cast<std::string>() + "'. Ret: " + std::to_string(ret);
        ErrorInfo diag = get_odbc_diagnostics_for_handle(SQL_HANDLE_DBC, _hdbc);
        msg += " ODBC Diag: SQLState: " + py::cast(diag.sqlState).cast<std::string>() + ", Message: " + py::cast(diag.ddbcErrorMsg).cast<std::string>();
        LOG(msg);
        throw std::runtime_error(msg);
    }
    return ret;
}

SQLRETURN BCPWrapper::bcp_control(const std::wstring& property_name, const std::wstring& value) {
    if (!_bcp_initialized || _bcp_finished) {
        LOG("BCPWrapper Warning: bcp_control(wstring) called in invalid state.");
        return SQL_ERROR;
    }

    auto it = bcp_control_properties.find(property_name);
    LOG("BCPWrapper: bcp_control(wstring) called for property '" + py::cast(property_name).cast<std::string>() + "'.");
    
    // Check if the property exists and is of type WSTRING
    // Note: For WSTRING properties, we expect the value to be a wide string (std::wstring).
    // If the property is not found or is not of type WSTRING, we return an error.
    LOG("BCPWrapper: bcp_control value: '" + py::cast(value).cast<std::string>() + "'.");
    LOG("BCPWrapper: bcp_control property name: '" + py::cast(property_name).cast<std::string>() + "'.");
    
    if (it == bcp_control_properties.end() || it->second.type != BCPCtrlPropType::WSTRING) {
        LOG("BCPWrapper Error: bcp_control(wstring) - property '" + py::cast(property_name).cast<std::string>() + "' not found or type mismatch.");
        return SQL_ERROR; 
    }
    
    LOG("BCPWrapper: Calling bcp_controlW for property '" + py::cast(property_name).cast<std::string>() + "' with wstring value '" + py::cast(value).cast<std::string>() + "'.");
    
    std::string value_utf8 = py::cast(value).cast<std::string>();
    // Fix the BCPHINTS issue: use the wide string directly rather than converting to narrow
    SQLRETURN ret = BCPControlW_ptr(_hdbc, it->second.option_code, (LPVOID)value_utf8.c_str());
    
    if (ret == FAIL) {
        std::string msg = "BCPWrapper Error: bcp_controlW (wstring value) failed for property '" + py::cast(property_name).cast<std::string>() + "'. Ret: " + std::to_string(ret);
        ErrorInfo diag = get_odbc_diagnostics_for_handle(SQL_HANDLE_DBC, _hdbc);
        msg += " ODBC Diag: SQLState: " + py::cast(diag.sqlState).cast<std::string>() + ", Message: " + py::cast(diag.ddbcErrorMsg).cast<std::string>();
        LOG(msg);
        throw std::runtime_error("BCPWrapper: bcp_controlW (wstring value) failed.");
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

    LOG("BCPWrapper: Calling bcp_readfmtW for file '" + py::cast(file_path).cast<std::string>() + "'.");
    SQLRETURN ret = BCPReadFmtW_ptr(_hdbc, file_path.c_str());
    if (ret == FAIL) {
        LOG("BCPWrapper Error: bcp_readfmtW failed for file '" + py::cast(file_path).cast<std::string>() + "'. Ret: " + std::to_string(ret));
    }
    return ret;
}

SQLRETURN BCPWrapper::define_columns(int num_cols) {
    if (!_bcp_initialized || _bcp_finished) {
        LOG("BCPWrapper Warning: define_columns called in invalid state.");
        return SQL_ERROR;
    }
    if (num_cols <= 0) {
        LOG("BCPWrapper Error: define_columns - invalid number of columns: " + std::to_string(num_cols));
        return SQL_ERROR;
    }

    LOG("BCPWrapper: Calling bcp_columns with " + std::to_string(num_cols) + " columns.");
    SQLRETURN ret = BCPColumns_ptr(_hdbc, num_cols);
    if (ret == FAIL) {
        LOG("BCPWrapper Error: bcp_columns failed for " + std::to_string(num_cols) + " columns. Ret: " + std::to_string(ret));
    }
    LOG("BCPWrapper: bcp_columns returned " + std::to_string(ret));
    return ret;
}

SQLRETURN BCPWrapper::define_column_format(int file_col_idx,
                                           int user_data_type,
                                           int indicator_length,
                                           long long user_data_length,
                                           const std::optional<py::bytes>& terminator_bytes_py, 
                                           int terminator_length,
                                           int server_col_idx) {
    if (!_bcp_initialized || _bcp_finished) {
        LOG("BCPWrapper: _bcp_initialized: " + std::to_string(_bcp_initialized) + ", _bcp_finished: " + std::to_string(_bcp_finished));
        throw std::runtime_error("BCPWrapper: define_column_format called in invalid state.");
    }

    const BYTE* pTerminator = nullptr;
    std::string terminator_str_holder; 

    if (terminator_bytes_py) {
        terminator_str_holder = terminator_bytes_py->cast<std::string>(); 
        if (!terminator_str_holder.empty()) {
            LOG("BCPWrapper: Terminator bytes provided: " + py::cast(terminator_str_holder).cast<std::string>());
            pTerminator = reinterpret_cast<const BYTE*>(terminator_str_holder.data());
            LOG("BCPWrapper: Terminator pointer: " + std::to_string(reinterpret_cast<uintptr_t>(pTerminator)));
            LOG("BCPWRapper: Terminator pointer type: " + std::string(typeid(pTerminator).name()));
            
            std::string hex_dump = "Terminator content hex dump: ";
            for (size_t i = 0; i < terminator_str_holder.size(); i++) {
                char hex_buf[8];
                snprintf(hex_buf, sizeof(hex_buf), "%02x ", (unsigned char)terminator_str_holder[i]);
                hex_dump += hex_buf;
            }
            LOG(hex_dump);
        } else {
            LOG("Warning: Terminator string is empty!");
        }
    } else {
        LOG("Warning: No terminator bytes provided!");
    }

    DBINT bcp_user_data_len = static_cast<DBINT>(user_data_length); 

    LOG("BCPWrapper: Calling bcp_colfmtW for file_col " + std::to_string(file_col_idx)
        + ", server_col " + std::to_string(server_col_idx)
        + ", user_data_type " + std::to_string(user_data_type)
        + ", indicator_len " + std::to_string(indicator_length)
        + ", user_data_len " + std::to_string(static_cast<long long>(bcp_user_data_len))
        + ", terminator_len " + std::to_string(terminator_length) 
        + ", terminator_ptr " + std::to_string(reinterpret_cast<uintptr_t>(pTerminator)));

    LOG("BCPWrapper: user_data_type: " + std::to_string(static_cast<BYTE>(user_data_type)));

    SQLRETURN ret = BCPColFmtW_ptr(_hdbc,
                                   file_col_idx,
                                   static_cast<BYTE>(user_data_type),
                                   indicator_length,
                                   bcp_user_data_len,
                                   pTerminator, 
                                   terminator_length, 
                                   server_col_idx
                                   );
    if (ret == FAIL) {
        std::string msg = "BCPWrapper Error: bcp_colfmtW failed for file_col " + std::to_string(file_col_idx)
                  + ", server_col " + std::to_string(server_col_idx)
                  + ". Ret: " + std::to_string(ret);
        ErrorInfo diag = get_odbc_diagnostics_for_handle(SQL_HANDLE_DBC, _hdbc);
        msg += " ODBC Diag: SQLState: " + py::cast(diag.sqlState).cast<std::string>() + ", Message: " + py::cast(diag.ddbcErrorMsg).cast<std::string>();
        LOG(msg);
        throw std::runtime_error(msg);
    }
    return ret;
}

SQLRETURN BCPWrapper::exec_bcp() {
    if (!_bcp_initialized || _bcp_finished) {
        throw std::runtime_error("BCPWrapper: exec_bcp called in invalid state.");
    }

    DBINT rows_copied_in_batch = 0; 
    LOG("BCPWrapper: Calling bcp_exec.");
    DBINT bcp_ret = BCPExec_ptr(_hdbc, &rows_copied_in_batch);
    
    if (bcp_ret == FAIL) { 
        std::string msg = "BCPWrapper Error: bcp_exec failed (returned -1). Rows in this batch (if any before error): " + std::to_string(static_cast<long long>(rows_copied_in_batch));
        ErrorInfo diag = get_odbc_diagnostics_for_handle(SQL_HANDLE_DBC, _hdbc);
        msg += " ODBC Diag: SQLState: " + py::cast(diag.sqlState).cast<std::string>() + ", Message: " + py::cast(diag.ddbcErrorMsg).cast<std::string>();
        LOG(msg);
        throw std::runtime_error(msg);
    }
    LOG("BCPWrapper: bcp_exec returned " + std::to_string(static_cast<long long>(bcp_ret)) + ". Rows parameter output: " + std::to_string(static_cast<long long>(rows_copied_in_batch)));
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

    LOG("BCPWrapper: Calling bcp_done.");
    SQLRETURN ret = BCPDone_ptr(_hdbc);
    if (ret != FAIL) {
        _bcp_finished = true;
        LOG("BCPWrapper: bcp_done successful.");
    } else {
        LOG("BCPWrapper Error: bcp_done failed. Ret: " + std::to_string(ret));
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

// Helper to allocate and copy data for different data types
template <typename T>
T* AllocateAndCopyData(const py::object& data, std::vector<std::shared_ptr<void>>& buffers) {
    // Create a shared pointer to hold our value
    auto buffer = std::shared_ptr<T>(new T());
    
    try {
        // For numeric types, handle possible Python type mismatches
        if constexpr (std::is_same<T, float>::value || std::is_same<T, double>::value) {
            // For float/double, accept integers or floats
            if (py::isinstance<py::int_>(data)) {
                *buffer = static_cast<T>(data.cast<long long>());
            } else if (py::isinstance<py::float_>(data)) {
                *buffer = data.cast<T>();
            } else {
                throw std::runtime_error("Cannot convert Python type to float/double");
            }
        } 
        else if constexpr (std::is_integral<T>::value) {
            // For integer types, accept Python integers
            if (py::isinstance<py::int_>(data)) {
                *buffer = static_cast<T>(data.cast<long long>());
            } else {
                throw std::runtime_error("Cannot convert Python type to integral type");
            }
        } 
        else {
            // Direct cast for other types
            *buffer = data.cast<T>();
        }
    } 
    catch (const py::cast_error& e) {
        std::string msg = "Cast error: ";
        msg += e.what();
        msg += " (Python type: ";
        msg += py::str(py::type::of(data)).cast<std::string>();
        msg += ")";
        throw std::runtime_error(msg);
    }
    
    // Store the shared_ptr in the buffers vector to keep track of it
    buffers.push_back(buffer);
    
    // Return the raw pointer for use with C APIs like bcp_bind
    return buffer.get();
}

SQLRETURN BCPWrapper::bind_column(const py::object& data, 
                                  int indicator_length,
                                  long long data_length,
                                  const std::optional<py::bytes>& terminator,
                                  int terminator_length,
                                  int data_type,
                                  int server_col_idx) {
    if (!_bcp_initialized || _bcp_finished) {
        LOG("BCPWrapper: _bcp_initialized: " + std::to_string(_bcp_initialized) + ", _bcp_finished: " + std::to_string(_bcp_finished));
        // LOG("BCPWrapper Warning: bind_column called in invalid state.");
        throw std::runtime_error("BCPWrapper: _bcp_initialized: " + std::to_string(_bcp_initialized) + ", _bcp_finished: " + std::to_string(_bcp_finished)
                                 + " - bind_column called in invalid state.");
    }
    
    LPCBYTE pData = nullptr;
    LPCBYTE pTerm = nullptr;
    std::string terminator_str_holder;
    
    // Process terminator bytes if provided
    if (terminator) {
        terminator_str_holder = terminator->cast<std::string>();
        if (!terminator_str_holder.empty()) {
            pTerm = reinterpret_cast<const LPCBYTE>(terminator_str_holder.data());
        }
    }
    
    // Handle different data types and convert Python objects to C++ types
    try {
        // Note: The allocated memory will be freed in the destructor
        if (py::isinstance<py::str>(data)) {
            // Handle string data - convert to either narrow or wide string based on data_type
            if (data_type == 239 /* SQLNCHAR */ || 
                data_type == 231 /* SQLNVARCHAR */ || 
                data_type == 99  /* SQLNTEXT */) {
                // For wide string types
                std::wstring wstrValue = data.cast<std::wstring>();
                auto buffer = std::shared_ptr<wchar_t[]>(new wchar_t[wstrValue.length() + 1]);
                wcscpy_s(buffer.get(), wstrValue.length() + 1, wstrValue.c_str());
                _data_buffers.push_back(buffer);
                pData = reinterpret_cast<LPCBYTE>(buffer.get());
            } else {
                // For narrow string types (SQLCHAR, SQLVARCHAR, SQLTEXT, etc.)
                std::string strValue = data.cast<std::string>();
                auto buffer = std::shared_ptr<char[]>(new char[strValue.length() + 1]);
                strcpy_s(buffer.get(), strValue.length() + 1, strValue.c_str());
                _data_buffers.push_back(buffer);
                pData = reinterpret_cast<LPCBYTE>(buffer.get());
            }
        } else if (py::isinstance<py::bytes>(data) || py::isinstance<py::bytearray>(data)) {
            // Handle binary data
            std::string binValue = data.cast<std::string>();
            auto buffer = std::shared_ptr<unsigned char[]>(new unsigned char[binValue.length()]);
            memcpy(buffer.get(), binValue.data(), binValue.length());
            _data_buffers.push_back(buffer);
            pData = reinterpret_cast<LPCBYTE>(buffer.get());
        } else if (py::isinstance<py::int_>(data)) {
            // Handle integer types based on data_type
            switch (data_type) {
                case 48:  // SQLINT1
                case 50:  // SQLBIT
                case 104: // SQLBITN
                    pData = reinterpret_cast<LPCBYTE>(AllocateAndCopyData<char>(data, _data_buffers));
                    break;
                case 52:  // SQLINT2
                    pData = reinterpret_cast<LPCBYTE>(AllocateAndCopyData<short>(data, _data_buffers));
                    break;
                case 56:  // SQLINT4
                    pData = reinterpret_cast<LPCBYTE>(AllocateAndCopyData<int>(data, _data_buffers));
                    break;
                case 127: // SQLINT8
                    pData = reinterpret_cast<LPCBYTE>(AllocateAndCopyData<long long>(data, _data_buffers));
                    break;
                case 38:  // SQLINTN - need to determine size from indicator_length
                    if (indicator_length == 1) {
                        pData = reinterpret_cast<LPCBYTE>(AllocateAndCopyData<char>(data, _data_buffers));
                    } else if (indicator_length == 2) {
                        pData = reinterpret_cast<LPCBYTE>(AllocateAndCopyData<short>(data, _data_buffers));
                    } else if (indicator_length == 4) {
                        pData = reinterpret_cast<LPCBYTE>(AllocateAndCopyData<int>(data, _data_buffers));
                    } else if (indicator_length == 8) {
                        pData = reinterpret_cast<LPCBYTE>(AllocateAndCopyData<long long>(data, _data_buffers));
                    } else {
                        // Default to int
                        pData = reinterpret_cast<LPCBYTE>(AllocateAndCopyData<int>(data, _data_buffers));
                    }
                    break;
                default:
                    pData = reinterpret_cast<LPCBYTE>(AllocateAndCopyData<int>(data, _data_buffers));
            }
        } else if (py::isinstance<py::float_>(data) || py::isinstance<py::int_>(data)) {
            // Handle float types - accept both float and int Python types
            switch (data_type) {
                case 59:  // SQLFLT4
                    pData = reinterpret_cast<LPCBYTE>(AllocateAndCopyData<float>(data, _data_buffers));
                    break;
                case 62:  // SQLFLT8
                case 109: // SQLFLTN
                default:
                    pData = reinterpret_cast<LPCBYTE>(AllocateAndCopyData<double>(data, _data_buffers));
            }
        } else if (py::isinstance<py::none>(data)) {
            // Handle NULL values
            if (indicator_length > 0) {
                // Create indicator buffer with SQL_NULL_DATA
                auto indicator_buffer = std::shared_ptr<SQLLEN>(new SQLLEN(-1)); // SQL_NULL_DATA is -1
                _data_buffers.push_back(indicator_buffer);
                pData = reinterpret_cast<LPCBYTE>(indicator_buffer.get());
            } else {
                pData = nullptr;
            }
        } else {
            // Default: try to convert to string
            LOG("BCPWrapper Warning: Unknown data type, attempting to convert to string");
            std::string strValue = py::str(data).cast<std::string>();
            auto buffer = std::shared_ptr<char[]>(new char[strValue.length() + 1]);
            strcpy_s(buffer.get(), strValue.length() + 1, strValue.c_str());
            _data_buffers.push_back(buffer);
            pData = reinterpret_cast<LPCBYTE>(buffer.get());
        }
    } catch (const std::exception& e) {
        std::string error_msg = "BCPWrapper Error: Failed to convert Python data for binding: ";
        error_msg += e.what();
        LOG(error_msg);
        throw std::runtime_error(error_msg);
    }

    // Call bcp_bind with the prepared data
    LOG("BCPWrapper: Calling bcp_bind for column " + std::to_string(server_col_idx) 
        + ", data_type " + std::to_string(data_type)
        + ", indicator_length " + std::to_string(indicator_length)
        + ", data_length " + std::to_string(data_length));
              
    SQLRETURN ret = BCPBind_ptr(_hdbc, 
                      pData, 
                      indicator_length, 
                      static_cast<DBINT>(data_length),
                      pTerm, 
                      terminator_length, 
                      data_type, 
                      server_col_idx);
                          
    if (ret == FAIL) {
        std::string msg = "BCPWrapper Error: bcp_bind failed for column " + std::to_string(server_col_idx);
        ErrorInfo diag = get_odbc_diagnostics_for_handle(SQL_HANDLE_DBC, _hdbc);
        msg += " ODBC Diag: SQLState: " + py::cast(diag.sqlState).cast<std::string>() + ", Message: " + py::cast(diag.ddbcErrorMsg).cast<std::string>();
        LOG(msg);
        throw std::runtime_error(msg);
    }
    
    LOG("BCPWrapper: bcp_bind successful for column " + std::to_string(server_col_idx));
    
    return ret;
}

SQLRETURN BCPWrapper::send_row() {
    if (!_bcp_initialized || _bcp_finished) {
        LOG("BCPWrapper Warning: send_row called in invalid state.");
        throw std::runtime_error("BCPWrapper: send_row called in invalid state.");
    }
    
    LOG("BCPWrapper: Calling bcp_sendrow");
    SQLRETURN ret = BCPSendRow_ptr(_hdbc);

    LOG("BCPWrapper: bcp_sendrow returned " + std::to_string(ret));

    if (ret == SQL_NO_DATA) {
        LOG("BCPWrapper: bcp_sendrow returned SQL_NO_DATA, indicating no more rows to send.");
        return SQL_NO_DATA; // No more rows to send
    }
    if (ret == SQL_SUCCESS_WITH_INFO) {
        LOG("BCPWrapper: bcp_sendrow returned SQL_SUCCESS_WITH_INFO, indicating a warning occurred.");
        // Handle warnings if needed, but still consider it a success
    }
    
    if (ret == FAIL) {
        std::string msg = "BCPWrapper Error: bcp_sendrow failed";
        ErrorInfo diag = get_odbc_diagnostics_for_handle(SQL_HANDLE_DBC, _hdbc);
        msg += " ODBC Diag: SQLState: " + py::cast(diag.sqlState).cast<std::string>() + ", Message: " + py::cast(diag.ddbcErrorMsg).cast<std::string>();
        LOG(msg);
        throw std::runtime_error(msg);
    }
    LOG("BCPWrapper: bcp_sendrow successful.");
    
    return ret;
}
