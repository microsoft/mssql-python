// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

// INFO|TODO - Note that is file is Windows specific right now. Making it arch agnostic will be
//             taken up in future

#include "connection.h"
#include <iostream>

//-------------------------------------------------------------------------------------------------
// Implements the Connection class declared in connection.h.
// This class wraps low-level ODBC operations like connect/disconnect,
// transaction control, and autocommit configuration.
//-------------------------------------------------------------------------------------------------
Connection::Connection(const std::wstring& conn_str, bool autocommit)
    : _conn_str(conn_str) , _autocommit(autocommit) {}

Connection::~Connection() {
    close();    // Ensure the connection is closed when the object is destroyed.
}

SQLRETURN Connection::connect() {
    allocDbcHandle();
    return connectToDb();
}

// Allocates DBC handle
void Connection::allocDbcHandle() {
    SQLHANDLE dbc = nullptr;
    LOG("Allocate SQL Connection Handle");
    SQLRETURN ret = SQLAllocHandle_ptr(SQL_HANDLE_DBC, getSharedEnvHandle()->get(), &dbc);
    if (!SQL_SUCCEEDED(ret)) {
        throw std::runtime_error("Failed to allocate connection handle");
    }
    _dbc_handle = std::make_shared<SqlHandle>(SQL_HANDLE_DBC, dbc);
}

// Connects to the database
SQLRETURN Connection::connectToDb() {
    LOG("Connecting to database");
    SQLRETURN ret = SQLDriverConnect_ptr(_dbc_handle->get(), nullptr,
                                         (SQLWCHAR*)_conn_str.c_str(), SQL_NTS,
                                         nullptr, 0, nullptr, SQL_DRIVER_NOPROMPT);
    if (!SQL_SUCCEEDED(ret)) {
        throw std::runtime_error("Failed to connect to database");
    }
    LOG("Connected to database successfully");
    return ret;
}

SQLRETURN Connection::close() {
    if (!_dbc_handle) {
        LOG("No connection handle to close");
        return SQL_SUCCESS;
    }
    LOG("Disconnect from MSSQL");
    if (!SQLDisconnect_ptr) {
        LOG("Function pointer not initialized. Loading the driver.");
        DriverLoader::getInstance().loadDriver();
    }

    SQLRETURN ret = SQLDisconnect_ptr(_dbc_handle->get());
    _dbc_handle.reset();
    return ret;
}

SQLRETURN Connection::commit() {
    if (!_dbc_handle) {
        throw std::runtime_error("Connection handle not allocated");
    }
    LOG("Committing transaction");
    SQLRETURN ret = SQLEndTran_ptr(SQL_HANDLE_DBC, _dbc_handle->get(), SQL_COMMIT);
    if (!SQL_SUCCEEDED(ret)) {
        throw std::runtime_error("Failed to commit transaction");
    }
    return ret;
}

SQLRETURN Connection::rollback() {
    if (!_dbc_handle) {
        throw std::runtime_error("Connection handle not allocated");
    }
    LOG("Rolling back transaction");
    SQLRETURN ret = SQLEndTran_ptr(SQL_HANDLE_DBC, _dbc_handle->get(), SQL_ROLLBACK);
    if (!SQL_SUCCEEDED(ret)) {
        throw std::runtime_error("Failed to rollback transaction");
    }
    return ret;
}

SQLRETURN Connection::setAutocommit(bool enable) {
    if (!_dbc_handle) {
        throw std::runtime_error("Connection handle not allocated");
    }
    SQLINTEGER value = enable ? SQL_AUTOCOMMIT_ON : SQL_AUTOCOMMIT_OFF;
    LOG("Set SQL Connection Attribute");
    SQLRETURN ret = SQLSetConnectAttr_ptr(_dbc_handle->get(), SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)value, 0);
    if (!SQL_SUCCEEDED(ret)) {
        throw std::runtime_error("Failed to set autocommit mode.");
    }
    _autocommit = enable;
    return ret;
}

bool Connection::getAutocommit() const {
    if (!_dbc_handle) {
        throw std::runtime_error("Connection handle not allocated");
    }
    LOG("Get SQL Connection Attribute");
    SQLINTEGER value;
    SQLINTEGER string_length;
    SQLGetConnectAttr_ptr(_dbc_handle->get(), SQL_ATTR_AUTOCOMMIT, &value, sizeof(value), &string_length);
    
    return value == SQL_AUTOCOMMIT_ON;
}

SqlHandlePtr Connection::allocStatementHandle() {
    if (!_dbc_handle) {
        throw std::runtime_error("Connection handle not allocated");
    }
    LOG("Allocating statement handle");
    SQLHANDLE stmt = nullptr;
    SQLRETURN ret = SQLAllocHandle_ptr(SQL_HANDLE_STMT, _dbc_handle->get(), &stmt);
    if (!SQL_SUCCEEDED(ret)) {
        throw std::runtime_error("Failed to allocate statement handle");
    }
    return std::make_shared<SqlHandle>(SQL_HANDLE_STMT, stmt);
}

SqlHandlePtr Connection::getSharedEnvHandle() {
    static std::once_flag flag;
    static SqlHandlePtr env_handle;

    std::call_once(flag, []() {
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
        env_handle = std::make_shared<SqlHandle>(SQL_HANDLE_ENV, env);

        LOG("Setting environment attributes");
        ret = SQLSetEnvAttr_ptr(env_handle->get(), SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3_80, 0);
        if (!SQL_SUCCEEDED(ret)) {
            throw std::runtime_error("Failed to set environment attribute");
        }
    });
    return env_handle;
}