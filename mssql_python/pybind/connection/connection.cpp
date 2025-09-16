// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

// INFO|TODO - Note that is file is Windows specific right now. Making it arch agnostic will be
//             taken up in future

#include "connection.h"
#include "connection_pool.h"
#include <vector>
#include <pybind11/pybind11.h>

#define SQL_COPT_SS_ACCESS_TOKEN   1256  // Custom attribute ID for access token
#define SQL_MAX_SMALL_INT 32767  // Maximum value for SQLSMALLINT

static SqlHandlePtr getEnvHandle() {
    static SqlHandlePtr envHandle = []() -> SqlHandlePtr {
        LOG("Allocating ODBC environment handle");
        if (!SQLAllocHandle_ptr) {
            LOG("Function pointers not initialized, loading driver");
            DriverLoader::getInstance().loadDriver();
        }
        SQLHANDLE env = nullptr;
        SQLRETURN ret = SQLAllocHandle_ptr(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
        if (!SQL_SUCCEEDED(ret)) {
            ThrowStdException("Failed to allocate environment handle");
        }
        ret = SQLSetEnvAttr_ptr(env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3_80, 0);
        if (!SQL_SUCCEEDED(ret)) {
            ThrowStdException("Failed to set environment attributes");
        }
        return std::make_shared<SqlHandle>(static_cast<SQLSMALLINT>(SQL_HANDLE_ENV), env);
    }();

    return envHandle;
}

//-------------------------------------------------------------------------------------------------
// Implements the Connection class declared in connection.h.
// This class wraps low-level ODBC operations like connect/disconnect,
// transaction control, and autocommit configuration.
//-------------------------------------------------------------------------------------------------
Connection::Connection(const std::wstring& conn_str, bool use_pool)
    : _connStr(conn_str), _autocommit(false), _fromPool(use_pool) {
    allocateDbcHandle();
}

Connection::~Connection() {
    disconnect();   // fallback if user forgets to disconnect
}

// Allocates connection handle
void Connection::allocateDbcHandle() {
    auto _envHandle = getEnvHandle();
    SQLHANDLE dbc = nullptr;
    LOG("Allocate SQL Connection Handle");
    SQLRETURN ret = SQLAllocHandle_ptr(SQL_HANDLE_DBC, _envHandle->get(), &dbc);
    checkError(ret);
    _dbcHandle = std::make_shared<SqlHandle>(static_cast<SQLSMALLINT>(SQL_HANDLE_DBC), dbc);
}

void Connection::connect(const py::dict& attrs_before) {
    LOG("Connecting to database");
    // Apply access token before connect
    if (!attrs_before.is_none() && py::len(attrs_before) > 0) {
        LOG("Apply attributes before connect");
        applyAttrsBefore(attrs_before);
        if (_autocommit) {
            setAutocommit(_autocommit);
        }
    }
    SQLWCHAR* connStrPtr;
#if defined(__APPLE__) || defined(__linux__) // macOS/Linux specific handling
    LOG("Creating connection string buffer for macOS/Linux");
    std::vector<SQLWCHAR> connStrBuffer = WStringToSQLWCHAR(_connStr);
    // Ensure the buffer is null-terminated
    LOG("Connection string buffer size - {}", connStrBuffer.size());
    connStrPtr = connStrBuffer.data();
    LOG("Connection string buffer created");
#else
    connStrPtr = const_cast<SQLWCHAR*>(_connStr.c_str());
#endif
    SQLRETURN ret = SQLDriverConnect_ptr(
        _dbcHandle->get(), nullptr,
        connStrPtr, SQL_NTS,
        nullptr, 0, nullptr, SQL_DRIVER_NOPROMPT);
    checkError(ret);
    updateLastUsed();
}

void Connection::disconnect() {
    if (_dbcHandle) {
        LOG("Disconnecting from database");
        SQLRETURN ret = SQLDisconnect_ptr(_dbcHandle->get());
        checkError(ret);
        _dbcHandle.reset(); // triggers SQLFreeHandle via destructor, if last owner
    }
    else {
        LOG("No connection handle to disconnect");
    }
}

// TODO: Add an exception class in C++ for error handling, DB spec compliant
void Connection::checkError(SQLRETURN ret) const{
    if (!SQL_SUCCEEDED(ret)) {
        ErrorInfo err = SQLCheckError_Wrap(SQL_HANDLE_DBC, _dbcHandle, ret);
        std::string errorMsg = WideToUTF8(err.ddbcErrorMsg);
        ThrowStdException(errorMsg);
    }
}

void Connection::commit() {
    if (!_dbcHandle) {
        ThrowStdException("Connection handle not allocated");
    }
    updateLastUsed();
    LOG("Committing transaction");
    SQLRETURN ret = SQLEndTran_ptr(SQL_HANDLE_DBC, _dbcHandle->get(), SQL_COMMIT);
    checkError(ret);
}

void Connection::rollback() {
    if (!_dbcHandle) {
        ThrowStdException("Connection handle not allocated");
    }
    updateLastUsed();
    LOG("Rolling back transaction");
    SQLRETURN ret = SQLEndTran_ptr(SQL_HANDLE_DBC, _dbcHandle->get(), SQL_ROLLBACK);
    checkError(ret);
}

void Connection::setAutocommit(bool enable) {
    if (!_dbcHandle) {
        ThrowStdException("Connection handle not allocated");
    }
    SQLINTEGER value = enable ? SQL_AUTOCOMMIT_ON : SQL_AUTOCOMMIT_OFF;
    LOG("Setting SQL Connection Attribute");
    SQLRETURN ret = SQLSetConnectAttr_ptr(_dbcHandle->get(), SQL_ATTR_AUTOCOMMIT, reinterpret_cast<SQLPOINTER>(static_cast<SQLULEN>(value)), 0);
    checkError(ret);
    if(value == SQL_AUTOCOMMIT_ON) {
        LOG("SQL Autocommit set to True");
    } else {
        LOG("SQL Autocommit set to False");
    }
    _autocommit = enable;
}

bool Connection::getAutocommit() const {
    if (!_dbcHandle) {
        ThrowStdException("Connection handle not allocated");
    }
    LOG("Get SQL Connection Attribute");
    SQLINTEGER value;
    SQLINTEGER string_length;
    SQLRETURN ret = SQLGetConnectAttr_ptr(_dbcHandle->get(), SQL_ATTR_AUTOCOMMIT, &value, sizeof(value), &string_length);
    checkError(ret);
    return value == SQL_AUTOCOMMIT_ON;
}

SqlHandlePtr Connection::allocStatementHandle() {
    if (!_dbcHandle) {
        ThrowStdException("Connection handle not allocated");
    }
    updateLastUsed();
    LOG("Allocating statement handle");
    SQLHANDLE stmt = nullptr;
    SQLRETURN ret = SQLAllocHandle_ptr(SQL_HANDLE_STMT, _dbcHandle->get(), &stmt);
    checkError(ret);
    return std::make_shared<SqlHandle>(static_cast<SQLSMALLINT>(SQL_HANDLE_STMT), stmt);
}


SQLRETURN Connection::setAttribute(SQLINTEGER attribute, py::object value) {
    LOG("Setting SQL attribute");
    SQLPOINTER ptr = nullptr;
    SQLINTEGER length = 0;

    if (py::isinstance<py::int_>(value)) {
        int intValue = value.cast<int>();
        ptr = reinterpret_cast<SQLPOINTER>(static_cast<uintptr_t>(intValue));
        length = SQL_IS_INTEGER;
    } else if (py::isinstance<py::bytes>(value) || py::isinstance<py::bytearray>(value)) {
        static std::vector<std::string> buffers;
        buffers.emplace_back(value.cast<std::string>());
        ptr = const_cast<char*>(buffers.back().c_str());
        length = static_cast<SQLINTEGER>(buffers.back().size());
    } else {
        LOG("Unsupported attribute value type");
        return SQL_ERROR;
    }

    SQLRETURN ret = SQLSetConnectAttr_ptr(_dbcHandle->get(), attribute, ptr, length);
    if (!SQL_SUCCEEDED(ret)) {
        LOG("Failed to set attribute");
    }
    else {
        LOG("Set attribute successfully");
    }
    return ret;
}

void Connection::applyAttrsBefore(const py::dict& attrs) {
    for (const auto& item : attrs) {
        int key;
        try {
            key = py::cast<int>(item.first);
        } catch (...) {
            continue;
        }

        if (key == SQL_COPT_SS_ACCESS_TOKEN) {   
            SQLRETURN ret = setAttribute(key, py::reinterpret_borrow<py::object>(item.second));
            if (!SQL_SUCCEEDED(ret)) {
                ThrowStdException("Failed to set access token before connect");
            }
        }
    }
}

bool Connection::isAlive() const {
    if (!_dbcHandle) {
        ThrowStdException("Connection handle not allocated");
    }
    SQLUINTEGER status;
    SQLRETURN ret = SQLGetConnectAttr_ptr(_dbcHandle->get(), SQL_ATTR_CONNECTION_DEAD,
        &status, 0, nullptr);
    return SQL_SUCCEEDED(ret) && status == SQL_CD_FALSE;
}

bool Connection::reset() {
    if (!_dbcHandle) {
        ThrowStdException("Connection handle not allocated");
    }
    LOG("Resetting connection via SQL_ATTR_RESET_CONNECTION");
    SQLRETURN ret = SQLSetConnectAttr_ptr(
        _dbcHandle->get(),
        SQL_ATTR_RESET_CONNECTION,
        (SQLPOINTER)SQL_RESET_CONNECTION_YES,
        SQL_IS_INTEGER);
    if (!SQL_SUCCEEDED(ret)) {
        LOG("Failed to reset connection. Marking as dead.");
        disconnect();
        return false;
    }
    updateLastUsed();
    return true;
}

void Connection::updateLastUsed() {
    _lastUsed = std::chrono::steady_clock::now();
}

std::chrono::steady_clock::time_point Connection::lastUsed() const {
    return _lastUsed;
}

ConnectionHandle::ConnectionHandle(const std::string& connStr, bool usePool, const py::dict& attrsBefore)
    : _usePool(usePool) {
    _connStr = Utf8ToWString(connStr);
    if (_usePool) {
        _conn = ConnectionPoolManager::getInstance().acquireConnection(_connStr, attrsBefore);
    } else {
        _conn = std::make_shared<Connection>(_connStr, false);
        _conn->connect(attrsBefore);
    }
}

ConnectionHandle::~ConnectionHandle() {
    if (_conn) {
        close();
    }
}

void ConnectionHandle::close() {
    if (!_conn) {
        ThrowStdException("Connection object is not initialized");
    }
    if (_usePool) {
        ConnectionPoolManager::getInstance().returnConnection(_connStr, _conn);
    } else {
        _conn->disconnect();
    }
    _conn = nullptr;
}

void ConnectionHandle::commit() {
    if (!_conn) {
        ThrowStdException("Connection object is not initialized");
    }
    _conn->commit();
}

void ConnectionHandle::rollback() {
    if (!_conn) {
        ThrowStdException("Connection object is not initialized");
    }
    _conn->rollback();
}

void ConnectionHandle::setAutocommit(bool enabled) {
    if (!_conn) {
        ThrowStdException("Connection object is not initialized");
    }
    _conn->setAutocommit(enabled);
}

bool ConnectionHandle::getAutocommit() const {
    if (!_conn) {
        ThrowStdException("Connection object is not initialized");
    }
    return _conn->getAutocommit();
}

SqlHandlePtr ConnectionHandle::allocStatementHandle() {
    if (!_conn) {
        ThrowStdException("Connection object is not initialized");
    }
    return _conn->allocStatementHandle();
}

py::object Connection::getInfo(SQLUSMALLINT infoType) const {
    if (!_dbcHandle) {
        ThrowStdException("Connection handle not allocated");
    }
    
    LOG("Getting connection info for type {}", infoType);
    
    // Use a vector for dynamic sizing
    std::vector<char> buffer(1024, 0);
    SQLSMALLINT actualLength = 0;
    SQLRETURN ret;
    
    // First try to get the info - handle SQLSMALLINT size limit
    SQLSMALLINT bufferSize = (buffer.size() <= SQL_MAX_SMALL_INT) 
        ? static_cast<SQLSMALLINT>(buffer.size()) 
        : SQL_MAX_SMALL_INT;
        
    ret = SQLGetInfo_ptr(_dbcHandle->get(), infoType, buffer.data(), bufferSize, &actualLength);
    
    // If truncation occurred (actualLength >= bufferSize means truncation)
    if (SQL_SUCCEEDED(ret) && actualLength >= bufferSize) {
        // Resize buffer to the needed size (add 1 for null terminator)
        buffer.resize(actualLength + 1, 0);
        
        // Call again with the larger buffer - handle SQLSMALLINT size limit again
        bufferSize = (buffer.size() <= SQL_MAX_SMALL_INT) 
            ? static_cast<SQLSMALLINT>(buffer.size()) 
            : SQL_MAX_SMALL_INT;
            
        ret = SQLGetInfo_ptr(_dbcHandle->get(), infoType, buffer.data(), bufferSize, &actualLength);
    }
    
    // Check for errors
    if (!SQL_SUCCEEDED(ret)) {
        checkError(ret);
    }
    
    // Note: This implementation assumes the ODBC driver handles any necessary
    // endianness conversions between the database server and the client.
    
    // Determine return type based on the InfoType
    // String types usually have InfoType > 10000 or are specifically known string values
    if (infoType > 10000 || 
        infoType == SQL_DATA_SOURCE_NAME || 
        infoType == SQL_DBMS_NAME || 
        infoType == SQL_DBMS_VER || 
        infoType == SQL_DRIVER_NAME || 
        infoType == SQL_DRIVER_VER ||
        // Add missing string types
        infoType == SQL_IDENTIFIER_QUOTE_CHAR ||
        infoType == SQL_CATALOG_NAME_SEPARATOR ||
        infoType == SQL_CATALOG_TERM ||
        infoType == SQL_SCHEMA_TERM ||
        infoType == SQL_TABLE_TERM ||
        infoType == SQL_KEYWORDS ||
        infoType == SQL_PROCEDURE_TERM) {
        // Return as string
        return py::str(buffer.data());
    } 
    else if (infoType == SQL_DRIVER_ODBC_VER || 
             infoType == SQL_SERVER_NAME) {
        // Return as string
        return py::str(buffer.data());
    }
    else {
        // For numeric types, use memcpy to safely extract the values
        // This avoids potential alignment issues with direct casting
        
        // Ensure buffer has enough data for the expected type
        switch (infoType) {
            // 16-bit unsigned integers
            case SQL_MAX_CONCURRENT_ACTIVITIES:
            case SQL_MAX_DRIVER_CONNECTIONS:
            case SQL_ODBC_API_CONFORMANCE:
            case SQL_ODBC_SQL_CONFORMANCE:
            case SQL_TXN_CAPABLE:              // Add missing numeric types
            case SQL_MULTIPLE_ACTIVE_TXN:
            case SQL_MAX_COLUMN_NAME_LEN:
            case SQL_MAX_TABLE_NAME_LEN:
            case SQL_PROCEDURES:
            {
                if (actualLength >= sizeof(SQLUSMALLINT)) {
                    SQLUSMALLINT value;
                    std::memcpy(&value, buffer.data(), sizeof(SQLUSMALLINT));
                    return py::int_(value);
                }
                break;
            }
            
            // 32-bit unsigned integers 
            case SQL_ASYNC_MODE:
            case SQL_GETDATA_EXTENSIONS:
            case SQL_MAX_ASYNC_CONCURRENT_STATEMENTS:
            case SQL_MAX_COLUMNS_IN_GROUP_BY:
            case SQL_MAX_COLUMNS_IN_ORDER_BY:
            case SQL_MAX_COLUMNS_IN_SELECT:
            case SQL_MAX_COLUMNS_IN_TABLE:
            case SQL_MAX_ROW_SIZE:
            case SQL_MAX_TABLES_IN_SELECT:
            case SQL_MAX_USER_NAME_LEN:
            case SQL_NUMERIC_FUNCTIONS:
            case SQL_STRING_FUNCTIONS:
            case SQL_SYSTEM_FUNCTIONS:
            case SQL_TIMEDATE_FUNCTIONS:
            case SQL_DEFAULT_TXN_ISOLATION:    // Add missing numeric types
            case SQL_MAX_STATEMENT_LEN:
            {
                if (actualLength >= sizeof(SQLUINTEGER)) {
                    SQLUINTEGER value;
                    std::memcpy(&value, buffer.data(), sizeof(SQLUINTEGER));
                    return py::int_(value);
                }
                break;
            }
            
            // Boolean flags (32-bit mask)
            case SQL_AGGREGATE_FUNCTIONS:
            case SQL_ALTER_TABLE:
            case SQL_CATALOG_USAGE:
            case SQL_DATETIME_LITERALS:
            case SQL_INDEX_KEYWORDS:
            case SQL_INSERT_STATEMENT:
            case SQL_SCHEMA_USAGE:
            case SQL_SQL_CONFORMANCE:
            case SQL_SQL92_DATETIME_FUNCTIONS:
            case SQL_SQL92_NUMERIC_VALUE_FUNCTIONS:
            case SQL_SQL92_PREDICATES:
            case SQL_SQL92_RELATIONAL_JOIN_OPERATORS:
            case SQL_SQL92_STRING_FUNCTIONS:
            case SQL_STATIC_CURSOR_ATTRIBUTES1:
            case SQL_STATIC_CURSOR_ATTRIBUTES2:
            {
                if (actualLength >= sizeof(SQLUINTEGER)) {
                    SQLUINTEGER value;
                    std::memcpy(&value, buffer.data(), sizeof(SQLUINTEGER));
                    return py::int_(value);
                }
                break;
            }
            
            // Handle any other types as integers, if enough data
            default:
                if (actualLength >= sizeof(SQLUINTEGER)) {
                    SQLUINTEGER value;
                    std::memcpy(&value, buffer.data(), sizeof(SQLUINTEGER));
                    return py::int_(value);
                }
                else if (actualLength >= sizeof(SQLUSMALLINT)) {
                    SQLUSMALLINT value;
                    std::memcpy(&value, buffer.data(), sizeof(SQLUSMALLINT));
                    return py::int_(value);
                }
                // For very small integers (like bytes/chars)
                else if (actualLength > 0) {
                    // Try to interpret as a small integer
                    unsigned char value;
                    std::memcpy(&value, buffer.data(), sizeof(unsigned char));
                    return py::int_(value);
                }
                break;
        }
    }
    
    // If we get here and actualLength > 0, try to return as string as a last resort
    if (actualLength > 0) {
        return py::str(buffer.data());
    }
    
    // Default return in case nothing matched or buffer is too small
    LOG("Unable to convert result for info type {}", infoType);
    return py::none();
}

py::object ConnectionHandle::getInfo(SQLUSMALLINT infoType) const {
    if (!_conn) {
        ThrowStdException("Connection object is not initialized");
    }
    return _conn->getInfo(infoType);
}