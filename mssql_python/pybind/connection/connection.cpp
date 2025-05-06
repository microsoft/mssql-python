// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#include "connection.h"

Connection::Connection(const std::wstring& conn_str) : _conn_str(conn_str), _is_open(false) {}

Connection::~Connection() {
    close();
}

SQLRETURN Connection::connect() {
    SQLHANDLE env = nullptr;
    SQLHANDLE dbc = nullptr;

    LOG("Allocate SQL Handle");
    if (!SQLAllocHandle_ptr) {
        LOG("Function pointer not initialized. Loading the driver.");
        DriverLoader::getInstance().loadDriver();  // Load the driver
    }
    SQLRETURN ret = SQLAllocHandle_ptr(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    if (!SQL_SUCCEEDED(ret)) {
        LOG("Failed to allocate environment handle");
        throw std::runtime_error("Failed to allocate environment handle");
    }
    _env_handle = std::make_shared<SqlHandle>(SQL_HANDLE_ENV, env);

    ret =  SQLSetEnvAttr_ptr(env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3_80, 0);
    if (!SQL_SUCCEEDED(ret)) {
        LOG("Failed to set environment attribute");
        throw std::runtime_error("Failed to allocate connection handle");
    }

    LOG("Allocate SQL Connection Handle");
    ret = SQLAllocHandle_ptr(SQL_HANDLE_DBC, env, &dbc);
    if (!SQL_SUCCEEDED(ret)) {
        LOG("Failed to allocate connection handle");
        throw std::runtime_error("Failed to allocate connection handle");
    }
    _dbc_handle = std::make_shared<SqlHandle>(SQL_HANDLE_DBC, dbc);

    ret = SQLDriverConnect_ptr(dbc, nullptr,
                                         (SQLWCHAR*)_conn_str.c_str(), SQL_NTS,
                                         nullptr, 0, nullptr, SQL_DRIVER_NOPROMPT);
    if (!SQL_SUCCEEDED(ret)) {
        LOG("Failed to connect to database");
    }
    else {
        LOG("Connected to database successfully");
    }
    _is_open = true;
    return ret;
}

SQLRETURN Connection::commit() {
    if (_is_open) {
        return SQLEndTran_ptr(SQL_HANDLE_DBC, _dbc_handle->get(), SQL_COMMIT);
    }
    return SQL_ERROR;
}

SQLRETURN Connection::rollback() {
    if (_is_open) {
        return SQLEndTran_ptr(SQL_HANDLE_DBC, _dbc_handle->get(), SQL_ROLLBACK);
    }
    return SQL_ERROR;
}

SQLRETURN Connection::set_autocommit(bool enable) {
    SQLUINTEGER value = enable ? SQL_AUTOCOMMIT_ON : SQL_AUTOCOMMIT_OFF;
    SQLRETURN ret = SQLSetConnectAttr_ptr(_dbc_handle->get(), SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)value, 0);
    if (!SQL_SUCCEEDED(ret)) {
        throw std::runtime_error("Failed to set autocommit mode.");
    }
    return ret;
}

bool Connection::get_autocommit() const {
    SQLINTEGER value;
    SQLINTEGER string_length;
    SQLGetConnectAttr_ptr(_dbc_handle->get(), SQL_ATTR_AUTOCOMMIT, &value, sizeof(value), &string_length);
    
    return value == SQL_AUTOCOMMIT_ON;
}

SQLRETURN Connection::close() {
    if (_dbc_handle && _is_open) {
        SQLRETURN ret = SQLDisconnect_ptr(_dbc_handle->get());
        _is_open = false;
        LOG("Disconnected from database");
        return ret;
    }
    return SQL_ERROR;
}

bool Connection::is_open() const {
    return _is_open;
}

SqlHandlePtr Connection::alloc_statement_handle() {
    SQLHANDLE stmt = nullptr;
    SQLRETURN ret = SQLAllocHandle_ptr(SQL_HANDLE_STMT, _dbc_handle->get(), &stmt);
    if (!SQL_SUCCEEDED(ret)) {
        throw std::runtime_error("Failed to allocate statement handle");
    }
    return std::make_shared<SqlHandle>(SQL_HANDLE_STMT, stmt);
}