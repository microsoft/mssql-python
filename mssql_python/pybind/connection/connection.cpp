// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#include "connection.h"
#include <iostream>

Connection::Connection(const std::wstring& conn_str) : _conn_str(conn_str) {}

Connection::~Connection() {
    close();
}

SQLRETURN Connection::connect() {
    LOG("Connecting to MSSQL");
    // to be added 
}

SQLRETURN Connection::close() {
    LOG("Disconnect from MSSQL");
    // to be added
}

SQLRETURN Connection::commit() {
    LOG("Committing transaction");
    // to be added
}

SQLRETURN Connection::rollback() {
    LOG("Rolling back transaction");
    // to be added
}

SQLRETURN Connection::end_transaction(SQLSMALLINT completion_type) {
    // to be added
}

SQLRETURN Connection::set_autocommit(bool enable) {
    LOG("Setting autocommit mode C++");
    // to be added
}

bool Connection::get_autocommit() const {
    LOG("Getting autocommit mode C++");
    // to be added
}
