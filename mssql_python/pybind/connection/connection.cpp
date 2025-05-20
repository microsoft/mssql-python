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
Connection::Connection(const std::wstring& conn_str, bool autocommit) : _conn_str(conn_str) , _autocommit(autocommit) {}

Connection::~Connection() {
    close(); // Ensure the connection is closed when the object is destroyed.
}

SQLRETURN Connection::connect() {
    SQLHANDLE env = nullptr;
    SQLHANDLE dbc = nullptr;

    LOG("Allocate SQL Handle");
    if (!SQLAllocHandle_ptr) {
        LOG("Function pointer not initialized. Loading the driver.");
        DriverLoader::getInstance().loadDriver();
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
        throw std::runtime_error("Failed to set environment attribute");
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
    return ret;
}

SQLRETURN Connection::close() {
    LOG("Disconnect from MSSQL");
    if (!SQLDisconnect_ptr) {
        LOG("Function pointer not initialized. Loading the driver.");
        DriverLoader::getInstance().loadDriver();
    }

    return SQLDisconnect_ptr(_dbc_handle->get());
}

SQLRETURN Connection::end_transaction(SQLSMALLINT completion_type) {
    LOG(completion_type == SQL_COMMIT ? "End SQL Transaction (Commit)" : "End SQL Transaction (Rollback)");
    if (!SQLEndTran_ptr) {
        LOG("Function pointer not initialized. Loading the driver.");
        DriverLoader::getInstance().loadDriver();
    }
    return SQLEndTran_ptr(_dbc_handle->type(), _dbc_handle->get(), completion_type);
}

SQLRETURN Connection::set_autocommit(bool enable) {
    SQLINTEGER value = enable ? SQL_AUTOCOMMIT_ON : SQL_AUTOCOMMIT_OFF;
    LOG("Set SQL Connection Attribute");
    SQLRETURN ret = SQLSetConnectAttr_ptr(_dbc_handle->get(), SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)value, 0);
    if (!SQL_SUCCEEDED(ret)) {
        throw std::runtime_error("Failed to set autocommit mode.");
    }
    return ret;
}

bool Connection::get_autocommit() const {
    LOG("Get SQL Connection Attribute");
    SQLINTEGER value;
    SQLINTEGER string_length;
    SQLGetConnectAttr_ptr(_dbc_handle->get(), SQL_ATTR_AUTOCOMMIT, &value, sizeof(value), &string_length);
    
    return value == SQL_AUTOCOMMIT_ON;
}

SqlHandlePtr Connection::alloc_statement_handle() {
    LOG("Allocating statement handle");
    SQLHANDLE stmt = nullptr;
    SQLRETURN ret = SQLAllocHandle_ptr(SQL_HANDLE_STMT, _dbc_handle->get(), &stmt);
    if (!SQL_SUCCEEDED(ret)) {
        throw std::runtime_error("Failed to allocate statement handle");
    }
    return std::make_shared<SqlHandle>(SQL_HANDLE_STMT, stmt);
}