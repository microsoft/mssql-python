// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

// INFO|TODO - Note that is file is Windows specific right now. Making it arch agnostic will be
//             taken up in future

#include "connection.h"
#include <iostream>

SqlHandlePtr Connection::_envHandle = nullptr;
//-------------------------------------------------------------------------------------------------
// Implements the Connection class declared in connection.h.
// This class wraps low-level ODBC operations like connect/disconnect,
// transaction control, and autocommit configuration.
//-------------------------------------------------------------------------------------------------
Connection::Connection(const std::wstring& conn_str, bool autocommit)
    : _connStr(conn_str) , _autocommit(autocommit) {
    if (!_envHandle) {
        LOG("Allocating environment handle");
        SQLHANDLE env = nullptr;
        if (!SQLAllocHandle_ptr) {
            LOG("Function pointers not initialized, loading driver");
            DriverLoader::getInstance().loadDriver();
        }
        SQLRETURN ret = SQLAllocHandle_ptr(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
        checkError(ret, "Failed to allocate environment handle");
        _envHandle = std::make_shared<SqlHandle>(SQL_HANDLE_ENV, env);

        LOG("Setting environment attributes");
        ret = SQLSetEnvAttr_ptr(_envHandle->get(), SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3_80, 0);
        checkError(ret, "Failed to set environment attribute");
    }
    allocateDbcHandle();
}

Connection::~Connection() {
    disconnect();   // fallback if app forgets to disconnect
}

// Allocates connection handle
void Connection::allocateDbcHandle() {
    SQLHANDLE dbc = nullptr;
    LOG("Allocate SQL Connection Handle");
    SQLRETURN ret = SQLAllocHandle_ptr(SQL_HANDLE_DBC, _envHandle->get(), &dbc);
    checkError(ret, "Failed to allocate connection handle");
    _dbcHandle = std::make_shared<SqlHandle>(SQL_HANDLE_DBC, dbc);
}

void Connection::connect() {
    LOG("Connecting to database");
    SQLRETURN ret = SQLDriverConnect_ptr(
        _dbcHandle->get(), nullptr,
        (SQLWCHAR*)_connStr.c_str(), SQL_NTS,
        nullptr, 0, nullptr, SQL_DRIVER_NOPROMPT);
    checkError(ret, "SQLDriverConnect failed");
    setAutocommit(_autocommit);
}

void Connection::disconnect() {
    if (_dbcHandle) {
        LOG("Disconnecting from database");
        SQLRETURN ret = SQLDisconnect_ptr(_dbcHandle->get());
        checkError(ret, "Failed to disconnect from database");
        _dbcHandle.reset(); // triggers SQLFreeHandle via destructor, if last owner
    }
    else {
        LOG("No connection handle to disconnect");
    }
}

void Connection::checkError(SQLRETURN ret, const std::string& msg) const{
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        throw std::runtime_error("[ODBC Error] " + msg);
    }
}

void Connection::commit() {
    if (!_dbcHandle) {
        throw std::runtime_error("Connection handle not allocated");
    }
    LOG("Committing transaction");
    SQLRETURN ret = SQLEndTran_ptr(SQL_HANDLE_DBC, _dbcHandle->get(), SQL_COMMIT);
    checkError(ret, "Failed to commit transaction");
}

void Connection::rollback() {
    if (!_dbcHandle) {
        throw std::runtime_error("Connection handle not allocated");
    }
    LOG("Rolling back transaction");
    SQLRETURN ret = SQLEndTran_ptr(SQL_HANDLE_DBC, _dbcHandle->get(), SQL_ROLLBACK);
    checkError(ret, "Failed to rollback transaction");
}

void Connection::setAutocommit(bool enable) {
    if (!_dbcHandle) {
        throw std::runtime_error("Connection handle not allocated");
    }
    SQLINTEGER value = enable ? SQL_AUTOCOMMIT_ON : SQL_AUTOCOMMIT_OFF;
    LOG("Set SQL Connection Attribute");
    SQLRETURN ret = SQLSetConnectAttr_ptr(_dbcHandle->get(), SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)value, 0);
    checkError(ret, "Failed to set autocommit attribute");
    _autocommit = enable;
}

bool Connection::getAutocommit() const {
    if (!_dbcHandle) {
        throw std::runtime_error("Connection handle not allocated");
    }
    LOG("Get SQL Connection Attribute");
    SQLINTEGER value;
    SQLINTEGER string_length;
    SQLRETURN ret = SQLGetConnectAttr_ptr(_dbcHandle->get(), SQL_ATTR_AUTOCOMMIT, &value, sizeof(value), &string_length);
    checkError(ret, "Failed to get autocommit attribute");
    return value == SQL_AUTOCOMMIT_ON;
}

SqlHandlePtr Connection::allocStatementHandle() {
    if (!_dbcHandle) {
        throw std::runtime_error("Connection handle not allocated");
    }
    LOG("Allocating statement handle");
    SQLHANDLE stmt = nullptr;
    SQLRETURN ret = SQLAllocHandle_ptr(SQL_HANDLE_STMT, _dbcHandle->get(), &stmt);
    checkError(ret, "Failed to allocate statement handle");
    return std::make_shared<SqlHandle>(SQL_HANDLE_STMT, stmt);
}
