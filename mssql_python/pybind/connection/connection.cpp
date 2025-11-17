// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#include "connection/connection.h"
#include "connection/connection_pool.h"
#include <algorithm>
#include <memory>
#include <pybind11/pybind11.h>
#include <regex>
#include <string>
#include <utility>
#include <vector>

#define SQL_COPT_SS_ACCESS_TOKEN 1256  // Custom attribute ID for access token
#define SQL_MAX_SMALL_INT 32767        // Maximum value for SQLSMALLINT

// Logging uses LOG() macro for all diagnostic output
#include "logger_bridge.hpp"

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
        ret = SQLSetEnvAttr_ptr(env, SQL_ATTR_ODBC_VERSION,
                                reinterpret_cast<void*>(SQL_OV_ODBC3_80), 0);
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
    disconnect();  // fallback if user forgets to disconnect
}

// Allocates connection handle
void Connection::allocateDbcHandle() {
    auto _envHandle = getEnvHandle();
    SQLHANDLE dbc = nullptr;
    LOG("Allocating SQL Connection Handle");
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
#if defined(__APPLE__) || defined(__linux__)  // macOS/Linux handling
    LOG("Creating connection string buffer for macOS/Linux");
    std::vector<SQLWCHAR> connStrBuffer = WStringToSQLWCHAR(_connStr);
    // Ensure the buffer is null-terminated
    LOG("Connection string buffer size=%zu", connStrBuffer.size());
    connStrPtr = connStrBuffer.data();
    LOG("Connection string buffer created");
#else
    connStrPtr = const_cast<SQLWCHAR*>(_connStr.c_str());
#endif
    SQLRETURN ret = SQLDriverConnect_ptr(_dbcHandle->get(), nullptr, connStrPtr, SQL_NTS, nullptr,
                                         0, nullptr, SQL_DRIVER_NOPROMPT);
    checkError(ret);
    updateLastUsed();
}

void Connection::disconnect() {
    if (_dbcHandle) {
        LOG("Disconnecting from database");
        SQLRETURN ret = SQLDisconnect_ptr(_dbcHandle->get());
        checkError(ret);
        // triggers SQLFreeHandle via destructor, if last owner
        _dbcHandle.reset();
    } else {
        LOG("No connection handle to disconnect");
    }
}

// TODO(microsoft): Add an exception class in C++ for error handling,
// DB spec compliant
void Connection::checkError(SQLRETURN ret) const {
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
    LOG("Setting autocommit=%d", enable);
    SQLRETURN ret =
        SQLSetConnectAttr_ptr(_dbcHandle->get(), SQL_ATTR_AUTOCOMMIT,
                              reinterpret_cast<SQLPOINTER>(static_cast<SQLULEN>(value)), 0);
    checkError(ret);
    if (value == SQL_AUTOCOMMIT_ON) {
        LOG("Autocommit enabled");
    } else {
        LOG("Autocommit disabled");
    }
    _autocommit = enable;
}

bool Connection::getAutocommit() const {
    if (!_dbcHandle) {
        ThrowStdException("Connection handle not allocated");
    }
    LOG("Getting autocommit attribute");
    SQLINTEGER value;
    SQLINTEGER string_length;
    SQLRETURN ret = SQLGetConnectAttr_ptr(_dbcHandle->get(), SQL_ATTR_AUTOCOMMIT, &value,
                                          sizeof(value), &string_length);
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
    LOG("Setting SQL attribute=%d", attribute);
    // SQLPOINTER ptr = nullptr;
    // SQLINTEGER length = 0;

    if (py::isinstance<py::int_>(value)) {
        // Get the integer value
        int64_t longValue = value.cast<int64_t>();

        SQLRETURN ret = SQLSetConnectAttr_ptr(
            _dbcHandle->get(), attribute,
            reinterpret_cast<SQLPOINTER>(static_cast<SQLULEN>(longValue)), SQL_IS_INTEGER);

        if (!SQL_SUCCEEDED(ret)) {
            LOG("Failed to set integer attribute=%d, ret=%d", attribute, ret);
        } else {
            LOG("Set integer attribute=%d successfully", attribute);
        }
        return ret;
    } else if (py::isinstance<py::str>(value)) {
        try {
            std::string utf8_str = value.cast<std::string>();

            // Convert to wide string
            std::wstring wstr = Utf8ToWString(utf8_str);
            if (wstr.empty() && !utf8_str.empty()) {
                LOG("Failed to convert string value to wide string for "
                    "attribute=%d",
                    attribute);
                return SQL_ERROR;
            }
            this->wstrStringBuffer.clear();
            this->wstrStringBuffer = std::move(wstr);

            SQLPOINTER ptr;
            SQLINTEGER length;

#if defined(__APPLE__) || defined(__linux__)
            // For macOS/Linux, convert wstring to SQLWCHAR buffer
            std::vector<SQLWCHAR> sqlwcharBuffer = WStringToSQLWCHAR(this->wstrStringBuffer);
            if (sqlwcharBuffer.empty() && !this->wstrStringBuffer.empty()) {
                LOG("Failed to convert wide string to SQLWCHAR buffer for "
                    "attribute=%d",
                    attribute);
                return SQL_ERROR;
            }

            ptr = sqlwcharBuffer.data();
            length = static_cast<SQLINTEGER>(sqlwcharBuffer.size() * sizeof(SQLWCHAR));
#else
            // On Windows, wchar_t and SQLWCHAR are the same size
            ptr = const_cast<SQLWCHAR*>(this->wstrStringBuffer.c_str());
            length = static_cast<SQLINTEGER>(this->wstrStringBuffer.length() * sizeof(SQLWCHAR));
#endif

            SQLRETURN ret = SQLSetConnectAttr_ptr(_dbcHandle->get(), attribute, ptr, length);
            if (!SQL_SUCCEEDED(ret)) {
                LOG("Failed to set string attribute=%d, ret=%d", attribute, ret);
            } else {
                LOG("Set string attribute=%d successfully", attribute);
            }
            return ret;
        } catch (const std::exception& e) {
            LOG("Exception during string attribute=%d setting: %s", attribute, e.what());
            return SQL_ERROR;
        }
    } else if (py::isinstance<py::bytes>(value) || py::isinstance<py::bytearray>(value)) {
        try {
            std::string binary_data = value.cast<std::string>();
            this->strBytesBuffer.clear();
            this->strBytesBuffer = std::move(binary_data);
            SQLPOINTER ptr = const_cast<char*>(this->strBytesBuffer.c_str());
            SQLINTEGER length = static_cast<SQLINTEGER>(this->strBytesBuffer.size());

            SQLRETURN ret = SQLSetConnectAttr_ptr(_dbcHandle->get(), attribute, ptr, length);
            if (!SQL_SUCCEEDED(ret)) {
                LOG("Failed to set binary attribute=%d, ret=%d", attribute, ret);
            } else {
                LOG("Set binary attribute=%d successfully (length=%d)", attribute, length);
            }
            return ret;
        } catch (const std::exception& e) {
            LOG("Exception during binary attribute=%d setting: %s", attribute, e.what());
            return SQL_ERROR;
        }
    } else {
        LOG("Unsupported attribute value type for attribute=%d", attribute);
        return SQL_ERROR;
    }
}

void Connection::applyAttrsBefore(const py::dict& attrs) {
    for (const auto& item : attrs) {
        int key;
        try {
            key = py::cast<int>(item.first);
        } catch (...) {
            continue;
        }

        // Apply all supported attributes
        SQLRETURN ret = setAttribute(key, py::reinterpret_borrow<py::object>(item.second));
        if (!SQL_SUCCEEDED(ret)) {
            std::string attrName = std::to_string(key);
            std::string errorMsg = "Failed to set attribute " + attrName + " before connect";
            ThrowStdException(errorMsg);
        }
    }
}

bool Connection::isAlive() const {
    if (!_dbcHandle) {
        ThrowStdException("Connection handle not allocated");
    }
    SQLUINTEGER status;
    SQLRETURN ret =
        SQLGetConnectAttr_ptr(_dbcHandle->get(), SQL_ATTR_CONNECTION_DEAD, &status, 0, nullptr);
    return SQL_SUCCEEDED(ret) && status == SQL_CD_FALSE;
}

bool Connection::reset() {
    if (!_dbcHandle) {
        ThrowStdException("Connection handle not allocated");
    }
    LOG("Resetting connection via SQL_ATTR_RESET_CONNECTION");
    SQLRETURN ret = SQLSetConnectAttr_ptr(_dbcHandle->get(), SQL_ATTR_RESET_CONNECTION,
                                          (SQLPOINTER)SQL_RESET_CONNECTION_YES, SQL_IS_INTEGER);
    if (!SQL_SUCCEEDED(ret)) {
        LOG("Failed to reset connection (ret=%d). Marking as dead.", ret);
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

ConnectionHandle::ConnectionHandle(const std::string& connStr, bool usePool,
                                   const py::dict& attrsBefore)
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

    // First call with NULL buffer to get required length
    SQLSMALLINT requiredLen = 0;
    SQLRETURN ret = SQLGetInfo_ptr(_dbcHandle->get(), infoType, NULL, 0, &requiredLen);

    if (!SQL_SUCCEEDED(ret)) {
        checkError(ret);
        return py::none();
    }

    // For zero-length results
    if (requiredLen == 0) {
        py::dict result;
        result["data"] = py::bytes("", 0);
        result["length"] = 0;
        result["info_type"] = infoType;
        return result;
    }

    // Cap buffer allocation to SQL_MAX_SMALL_INT to prevent excessive
    // memory usage
    SQLSMALLINT allocSize = requiredLen + 10;
    if (allocSize > SQL_MAX_SMALL_INT) {
        allocSize = SQL_MAX_SMALL_INT;
    }
    std::vector<char> buffer(allocSize, 0);  // Extra padding for safety

    // Get the actual data - avoid using std::min
    SQLSMALLINT bufferSize = requiredLen + 10;
    if (bufferSize > SQL_MAX_SMALL_INT) {
        bufferSize = SQL_MAX_SMALL_INT;
    }

    SQLSMALLINT returnedLen = 0;
    ret = SQLGetInfo_ptr(_dbcHandle->get(), infoType, buffer.data(), bufferSize, &returnedLen);

    if (!SQL_SUCCEEDED(ret)) {
        checkError(ret);
        return py::none();
    }

    // Create a dictionary with the raw data
    py::dict result;

    // IMPORTANT: Pass exactly what SQLGetInfo returned
    // No null-terminator manipulation, just pass the raw data
    result["data"] = py::bytes(buffer.data(), returnedLen);
    result["length"] = returnedLen;
    result["info_type"] = infoType;

    return result;
}

py::object ConnectionHandle::getInfo(SQLUSMALLINT infoType) const {
    if (!_conn) {
        ThrowStdException("Connection object is not initialized");
    }
    return _conn->getInfo(infoType);
}

void ConnectionHandle::setAttr(int attribute, py::object value) {
    if (!_conn) {
        ThrowStdException("Connection not established");
    }

    // Use existing setAttribute with better error handling
    SQLRETURN ret = _conn->setAttribute(static_cast<SQLINTEGER>(attribute), value);
    if (!SQL_SUCCEEDED(ret)) {
        // Get detailed error information from ODBC
        try {
            ErrorInfo errorInfo = SQLCheckError_Wrap(SQL_HANDLE_DBC, _conn->getDbcHandle(), ret);

            std::string errorMsg =
                "Failed to set connection attribute " + std::to_string(attribute);
            if (!errorInfo.ddbcErrorMsg.empty()) {
                // Convert wstring to string for concatenation
                std::string ddbcErrorStr = WideToUTF8(errorInfo.ddbcErrorMsg);
                errorMsg += ": " + ddbcErrorStr;
            }

            LOG("Connection setAttribute failed: %s", errorMsg.c_str());
            ThrowStdException(errorMsg);
        } catch (...) {
            // Fallback to generic error if detailed error retrieval fails
            std::string errorMsg =
                "Failed to set connection attribute " + std::to_string(attribute);
            LOG("Connection setAttribute failed: %s", errorMsg.c_str());
            ThrowStdException(errorMsg);
        }
    }
}
