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
Connection::Connection(const std::wstring& conn_str) : _conn_str(conn_str) {}

Connection::~Connection() {
    LOG("Connection destructor called");
    close();    // Ensure the connection is closed when the object is destroyed.
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

SQLRETURN Connection::set_autocommit(bool enable) {
    LOG("Setting autocommit mode");
    // to be added
}

bool Connection::get_autocommit() const {
    LOG("Getting autocommit mode");
    // to be added
}
