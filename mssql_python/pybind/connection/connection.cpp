// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

// INFO|TODO - Note that is file is Windows specific right now. Making it arch agnostic will be
//             taken up in future

#include "connection.h"
// #include <vector>
// #include <pybind11/pybind11.h>
#include "connection_pool.h"
#include <iostream>

// #define SQL_COPT_SS_ACCESS_TOKEN   1256  // Custom attribute ID for access token

//-------------------------------------------------------------------------------------------------
// Implements the Connection class declared in connection.h.
// This class wraps low-level ODBC operations like connect/disconnect,
// transaction control, and autocommit configuration.
//-------------------------------------------------------------------------------------------------
// Connection::Connection(const std::wstring& conn_str, bool autocommit, bool usePool)
//     : _conn_str(conn_str), _is_closed(true), _usePool(usePool), _autocommit(autocommit) {}

// Connection::~Connection() {
//     std::cout << "[Connection::dtor] Destructor called" << std::endl;
//     close();
// }

// SQLRETURN Connection::connect(const py::dict& attrs_before) {
//     std::cout << "[connect] Starting connection. usePool=" << (_usePool ? "true" : "false") << std::endl;
//     if (_usePool) {
//         _conn = ConnectionPoolManager::getInstance().acquireConnection(_conn_str);
//         if (!_conn || !_conn->_dbc_handle) {
//             std::cout << "[connect] Failed to acquire pooled connection." << std::endl;
//             throw std::runtime_error("Failed to acquire pooled connection.");
//         }
//         std::cout << "[connect] Acquired pooled connection." << std::endl;
//         _dbc_handle = _conn->_dbc_handle;
//         _usePool = true;
//         _is_closed = false;
//     } else {
//         std::cout << "[connect] Connecting without pooling..." << std::endl;
//         SQLRETURN ret = directConnect(attrs_before);
//         if (SQL_SUCCEEDED(ret)) {
//             std::cout << "[connect] Direct connection successful." << std::endl;
//             _is_closed = false;
//         }else {
//             std::cout << "[connect] Direct connection failed." << std::endl;
//         }
//         return ret;
//     }
//     return SQL_SUCCESS;
// }

// SQLRETURN Connection::directConnect(const py::dict& attrs_before) {
//     std::cout << "[directConnect] Allocating DBC handle..." << std::endl;
//     allocDbcHandle();
//     // Apply access token before connect
//     if (!attrs_before.is_none() && py::len(attrs_before) > 0) {
//         std::cout << "[directConnect] Applying attributes before connect..." << std::endl;
//         LOG("Apply attributes before connect");
//         applyAttrsBefore(attrs_before);
//         if (_autocommit) {
//             setAutocommit(_autocommit);
//         }
//     }
//     return connectToDb();
// }

// // Allocates DBC handle
// void Connection::allocDbcHandle() {
//     std::cout << "[allocDbcHandle] Allocating SQL handle..." << std::endl;
//     SQLHANDLE dbc = nullptr;
//     LOG("Allocate SQL Connection Handle");
//     SQLRETURN ret = SQLAllocHandle_ptr(SQL_HANDLE_DBC, getSharedEnvHandle()->get(), &dbc);
//     if (!SQL_SUCCEEDED(ret)) {
//         std::cout << "[allocDbcHandle] Failed to allocate DBC handle." << std::endl;
//         throw std::runtime_error("Failed to allocate connection handle");
//     }
//     _dbc_handle = std::make_shared<SqlHandle>(SQL_HANDLE_DBC, dbc);
//     std::cout << "[allocDbcHandle] Handle allocated successfully." << std::endl;
// }

// // Connects to the database
// SQLRETURN Connection::connectToDb() {
//     std::cout << "[connectToDb] Connecting to database..." << std::endl;
//     LOG("Connecting to database");
//     SQLRETURN ret = SQLDriverConnect_ptr(_dbc_handle->get(), nullptr,
//                                          (SQLWCHAR*)_conn_str.c_str(), SQL_NTS,
//                                          nullptr, 0, nullptr, SQL_DRIVER_NOPROMPT);
//     if (!SQL_SUCCEEDED(ret)) {
//         std::cout << "[connectToDb] Connection failed." << std::endl;
//         ThrowStdException("Client unable to establish connection");
//     }
//     std::cout << "[connectToDb] Connected successfully." << std::endl;
//     return ret;
// }

// SQLRETURN Connection::close() {
//     std::cout << "[close] Closing connection. usePool=" << (_usePool ? "true" : "false") << std::endl;
//     if (_is_closed) return SQL_SUCCESS;

//     if (_usePool) {
//         if (_conn) {
//             std::cout << "[close] Returning connection to pool." << std::endl;
//             ConnectionPoolManager::getInstance().returnConnection(_conn_str, _conn);
//         }
//     } else {
//         std::cout << "[close] Disconnecting non-pooled connection." << std::endl;
//         disconnect();
//     }
//     _is_closed = true;
//     return SQL_SUCCESS;
// }

// SQLRETURN Connection::disconnect() {
//     std::cout << "[disconnect] Disconnecting..." << std::endl;
//     if (_dbc_handle) {
//         std::cout << "[disconnect] Disconnecting from database..." << std::endl;
//         SQLDisconnect_ptr(_dbc_handle->get());
//         SQLFreeHandle_ptr(SQL_HANDLE_DBC, _dbc_handle->get());
//         _dbc_handle.reset();
//         std::cout << "[disconnect] Disconnected successfully." << std::endl;
//     }
//     return SQL_SUCCESS;
// }

// SQLRETURN Connection::commit() {
//     if (_usePool) {
//         if (!_conn || !_conn->_dbc_handle) {
//             throw std::runtime_error("Cannot commit: invalid pooled connection.");
//         }
//         LOG("Committing pooled transaction");
//         SQLRETURN ret = SQLEndTran_ptr(SQL_HANDLE_DBC, _conn->_dbc_handle->get(), SQL_COMMIT);
//         if (!SQL_SUCCEEDED(ret)) {
//             throw std::runtime_error("Failed to commit transaction (pooled)");
//         }
//         return ret;
//     } else {
//         if (_is_closed || !_dbc_handle) {
//             throw std::runtime_error("Cannot commit: connection is closed.");
//         }
//         LOG("Committing direct transaction");
//         SQLRETURN ret = SQLEndTran_ptr(SQL_HANDLE_DBC, _dbc_handle->get(), SQL_COMMIT);
//         if (!SQL_SUCCEEDED(ret)) {
//             throw std::runtime_error("Failed to commit transaction");
//         }
//         return ret;
//     }
// }

// SQLRETURN Connection::rollback() {
//     if (_usePool) {
//         if (!_conn || !_conn->_dbc_handle) {
//             throw std::runtime_error("Cannot rollback: invalid pooled connection.");
//         }
//         LOG("Rolling back pooled transaction");
//         SQLRETURN ret = SQLEndTran_ptr(SQL_HANDLE_DBC, _conn->_dbc_handle->get(), SQL_ROLLBACK);
//         if (!SQL_SUCCEEDED(ret)) {
//             throw std::runtime_error("Failed to rollback transaction (pooled)");
//         }
//         return ret;
//     } else {
//         if (_is_closed || !_dbc_handle) {
//             throw std::runtime_error("Cannot rollback: connection is closed.");
//         }
//         LOG("Rolling back direct transaction");
//         SQLRETURN ret = SQLEndTran_ptr(SQL_HANDLE_DBC, _dbc_handle->get(), SQL_ROLLBACK);
//         if (!SQL_SUCCEEDED(ret)) {
//             throw std::runtime_error("Failed to rollback transaction");
//         }
//         return ret;
//     }
// }

// SQLRETURN Connection::setAutocommit(bool enable) {
//     SQLHANDLE handle = _usePool ? (_conn ? _conn->_dbc_handle->get() : nullptr)
//                                 : (_dbc_handle ? _dbc_handle->get() : nullptr);
//     if (!handle) {
//         throw std::runtime_error("Cannot get autocommit: Connection handle is null.");
//     }
//     SQLINTEGER value = enable ? SQL_AUTOCOMMIT_ON : SQL_AUTOCOMMIT_OFF;
//     SQLRETURN ret = SQLSetConnectAttr_ptr(handle, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)value, 0);
//     if (!SQL_SUCCEEDED(ret)) {
//         throw std::runtime_error("Failed to set autocommit mode.");
//     }
//     _autocommit = enable;
//     std::cout << "[setAutocommit] Autocommit set successfully." << std::endl;
//     return ret;
// }

// bool Connection::getAutocommit() const {
//     SQLHANDLE handle = _usePool ? (_conn ? _conn->_dbc_handle->get() : nullptr)
//                                 : (_dbc_handle ? _dbc_handle->get() : nullptr);
//     if (!handle) {
//         throw std::runtime_error("Cannot get autocommit: Connection handle is null.");
//     }
//     SQLINTEGER value;
//     SQLINTEGER string_length;
//     SQLGetConnectAttr_ptr(handle, SQL_ATTR_AUTOCOMMIT, &value, sizeof(value), &string_length);
//     return value == SQL_AUTOCOMMIT_ON;
// }

SqlHandlePtr Connection::allocStatementHandle() {
    if (!_dbcHandle) {
        throw std::runtime_error("Connection handle not allocated");
    }
    // std::cout << "[allocStatementHandle] Allocating statement handle..." << std::endl;
    LOG("Allocating statement handle");
    SQLHANDLE stmt = nullptr;
    SQLRETURN ret = SQLAllocHandle_ptr(SQL_HANDLE_STMT, _dbcHandle->get(), &stmt);
    if (!SQL_SUCCEEDED(ret)) {
        throw std::runtime_error("Failed to allocate statement handle");
    }
    return std::make_shared<SqlHandle>(SQL_HANDLE_STMT, stmt);
}

// SQLRETURN Connection::setAttribute(SQLINTEGER attribute, py::object value) {
//     LOG("Setting SQL attribute");
//     std::cout << "[setAttribute] Setting attribute " << attribute << std::endl;

//     SQLPOINTER ptr = nullptr;
//     SQLINTEGER length = 0;

//     if (py::isinstance<py::int_>(value)) {
//         int intValue = value.cast<int>();
//         ptr = reinterpret_cast<SQLPOINTER>(static_cast<uintptr_t>(intValue));
//         length = SQL_IS_INTEGER;
//     } else if (py::isinstance<py::bytes>(value) || py::isinstance<py::bytearray>(value)) {
//         static std::vector<std::string> buffers;
//         buffers.emplace_back(value.cast<std::string>());
//         ptr = const_cast<char*>(buffers.back().c_str());
//         length = static_cast<SQLINTEGER>(buffers.back().size());
//     } else {
//         LOG("Unsupported attribute value type");
//         return SQL_ERROR;
//     }

//     SQLRETURN ret = SQLSetConnectAttr_ptr(_dbc_handle->get(), attribute, ptr, length);
//     if (!SQL_SUCCEEDED(ret)) {
//         LOG("Failed to set attribute");
//     }
//     else {
//         LOG("Set attribute successfully");
//     }
//     return ret;
// }

// void Connection::applyAttrsBefore(const py::dict& attrs) {
//     std::cout << "[applyAttrsBefore] Applying attributes..." << std::endl;
//     for (const auto& item : attrs) {
//         int key;
//         key = py::cast<int>(item.first);
//         if (key == SQL_COPT_SS_ACCESS_TOKEN) {   
//             SQLRETURN ret = setAttribute(key, py::reinterpret_borrow<py::object>(item.second));
//             if (!SQL_SUCCEEDED(ret)) {
//                 throw std::runtime_error("Failed to set access token before connect");
//             }
//         }
//     }
// }

// SqlHandlePtr Connection::getSharedEnvHandle() {
//     static std::once_flag flag;
    // static SqlHandlePtr env_handle;

    // std::call_once(flag, []() {
    //     std::cout << "[getSharedEnvHandle] Allocating environment handle..." << std::endl;
    //     LOG("Allocating environment handle");
    //     SQLHANDLE env = nullptr;
    //     if (!SQLAllocHandle_ptr) {
    //         LOG("Function pointers not initialized, loading driver");
    //         DriverLoader::getInstance().loadDriver();
    //     }
    //     SQLRETURN ret = SQLAllocHandle_ptr(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    //     if (!SQL_SUCCEEDED(ret)) {
    //         throw std::runtime_error("Failed to allocate environment handle");
    //     }
    //     env_handle = std::make_shared<SqlHandle>(SQL_HANDLE_ENV, env);

    //     LOG("Setting environment attributes");
    //     ret = SQLSetEnvAttr_ptr(env_handle->get(), SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3_80, 0);
    //     if (!SQL_SUCCEEDED(ret)) {
    //         throw std::runtime_error("Failed to set environment attribute");
    //     }
//     });
//     return env_handle;
// }

// bool Connection::isAlive() const {
//     if (!_dbc_handle)
//         return false;
//     SQLINTEGER value;
//     bool alive = SQL_SUCCEEDED(SQLGetConnectAttr_ptr(_dbc_handle->get(), SQL_ATTR_CONNECTION_DEAD, &value, sizeof(value), nullptr))
//                  && value == SQL_CD_FALSE;
//     std::cout << "[isAlive] Connection is " << (alive ? "alive" : "dead") << std::endl;
//     return alive;
// }

// void Connection::reset() {
//     // Reset the connection state
//     if (_dbc_handle) {
//         std::cout << "[reset] Resetting connection..." << std::endl;
//         SQLRETURN ret = SQLSetConnectAttr_ptr(_dbc_handle->get(), SQL_ATTR_CONNECTION_TIMEOUT, (SQLPOINTER)(uintptr_t)1, 0);
//         if (!SQL_SUCCEEDED(ret)) {
//             throw std::runtime_error("Failed to reset connection");
//         }
//         std::cout << "[reset] Reset successful." << std::endl;
//     }
// }

// void Connection::updateLastUsed() {
//     std::cout << "[updateLastUsed] Updating last used time." << std::endl;
//     _last_used = std::chrono::steady_clock::now();
// }

SqlHandlePtr Connection::_envHandle = nullptr;

Connection::Connection(const std::wstring& connStr, bool usePool)
    : _connStr(connStr), _usePool(usePool) {
    if (!_envHandle) {
        // std::cout << "Allocating environment handle..." << std::endl;
        LOG("Allocating environment handle");
        SQLHANDLE env = nullptr;
        if (!SQLAllocHandle_ptr) {
            LOG("Function pointers not initialized, loading driver");
            DriverLoader::getInstance().loadDriver();
        }
        SQLRETURN ret = SQLAllocHandle_ptr(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
        if (!SQL_SUCCEEDED(ret)) {
            throw std::runtime_error("Failed to allocate environment handle");
        }
        _envHandle = std::make_shared<SqlHandle>(SQL_HANDLE_ENV, env);

        // std::cout<<"Setting environment attributes"<<std::endl;
        ret = SQLSetEnvAttr_ptr(_envHandle->get(), SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3_80, 0);
        if (!SQL_SUCCEEDED(ret)) {
            throw std::runtime_error("Failed to set environment attribute");
        }
    }
    allocate();
}

void Connection::allocate() {
    //  std::cout << "[allocDbcHandle] Allocating SQL handle..." << std::endl;
    SQLHANDLE dbc = nullptr;
    LOG("Allocate SQL Connection Handle");
    SQLRETURN ret = SQLAllocHandle_ptr(SQL_HANDLE_DBC, _envHandle->get(), &dbc);
    if (!SQL_SUCCEEDED(ret)) {
        // std::cout << "[allocDbcHandle] Failed to allocate DBC handle." << std::endl;
        throw std::runtime_error("Failed to allocate connection handle");
    }
    _dbcHandle = std::make_shared<SqlHandle>(SQL_HANDLE_DBC, dbc);
}

Connection::~Connection() {
    disconnect();
}

void Connection::connect() {
    // std::wcout << L"[Connection] Connecting with: " << _connStr << "\n";
    SQLRETURN ret = SQLDriverConnect_ptr(
        _dbcHandle->get(), nullptr,
        (SQLWCHAR*)_connStr.c_str(), SQL_NTS,
        nullptr, 0, nullptr, SQL_DRIVER_NOPROMPT);
    checkError(ret, "SQLDriverConnect");
    setAutocommit(_autocommit);
}

void Connection::disconnect() {
    if (_dbcHandle) {
        SQLDisconnect_ptr(_dbcHandle->get());
        // std::cout << "[Connection] Disconnected.\n";
    }
}

bool Connection::reset() {
    // std::cout << "[Connection] Resetting connection.\n";
    // disconnect();
    // connect();
    SQLRETURN ret = SQLSetConnectAttr_ptr(
        _dbcHandle->get(),  // your HDBC handle
        SQL_ATTR_RESET_CONNECTION,
        (SQLPOINTER)SQL_RESET_CONNECTION_YES,
        SQL_IS_INTEGER
    );

    if (!SQL_SUCCEEDED(ret)) {
        LOG("SQL_ATTR_RESET_CONNECTION failed during reset()");
        return false;
    }

    LOG("Connection reset using SQL_ATTR_RESET_CONNECTION");
    return true;
}

void Connection::commit() {
    SQLRETURN ret = SQLEndTran_ptr(SQL_HANDLE_DBC, _dbcHandle->get(), SQL_COMMIT);
    checkError(ret, "Commit failed");
}

void Connection::rollback() {
    SQLRETURN ret = SQLEndTran_ptr(SQL_HANDLE_DBC, _dbcHandle->get(), SQL_ROLLBACK);
    checkError(ret, "Rollback failed");
}

void Connection::setAutocommit(bool enabled) {
    SQLRETURN ret = SQLSetConnectAttr_ptr(
        _dbcHandle->get(), SQL_ATTR_AUTOCOMMIT,
        (SQLPOINTER)(enabled ? SQL_AUTOCOMMIT_ON : SQL_AUTOCOMMIT_OFF), 0);
    checkError(ret, "Setting autocommit failed");
    _autocommit = enabled;
}

bool Connection::getAutocommit() const {
    return _autocommit;
}

bool Connection::isAlive() const {
    return true;  // Placeholder
}

const std::wstring& Connection::connStr() const {
    return _connStr;
}

void Connection::checkError(SQLRETURN ret, const std::string& msg) {
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        throw std::runtime_error("[ODBC Error] " + msg);
    }
}



ConnectionHandle::ConnectionHandle(const std::wstring& connStr, bool usePool)
    : _connStr(connStr), _usePool(usePool) {
        // std::wcout << L"[ConnectionHandle] Creating handle for connection: " << connStr << "\n";
    if (_usePool) {
        // std::wcout << L"[ConnectionHandle] Using connection pool for: " << connStr << "\n";
        _conn = ConnectionPoolManager::getInstance().acquireConnection(connStr);
    } else {
        // std::wcout << L"[ConnectionHandle] Creating direct connection: " << connStr << "\n";
        _conn = std::make_shared<Connection>(connStr, false);
        _conn->connect();
    }
}

void ConnectionHandle::close() {
    if (_closed) return;
    if (_usePool) {
        ConnectionPoolManager::getInstance().returnConnection(_connStr, _conn);
    } else {
        _conn->disconnect();
    }
    _closed = true;
}

void ConnectionHandle::commit() {
    _conn->commit();
}

void ConnectionHandle::rollback() {
    _conn->rollback();
}

void ConnectionHandle::setAutocommit(bool enabled) {
    _conn->setAutocommit(enabled);
}

bool ConnectionHandle::getAutocommit() const {
    return _conn->getAutocommit();
}

SqlHandlePtr ConnectionHandle::allocStatementHandle() {
    return _conn->allocStatementHandle();
}