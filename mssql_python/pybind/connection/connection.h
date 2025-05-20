// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

// INFO|TODO - Note that is file is Windows specific right now. Making it arch agnostic will be
//             taken up in future.

#ifndef CONNECTION_H
#define CONNECTION_H

#include "ddbc_bindings.h"

// Represents a single ODBC database connection.
// Manages its own environment and connection handles.
// Note: This class does NOT implement pooling logic directly.

class Connection {
public:
    Connection(const std::wstring& conn_str);
    ~Connection();

    // Establish the connection using the stored connection string.
    SQLRETURN connect();

    // Close the connection and free resources.
    SQLRETURN close();

    // Commit the current transaction.
    SQLRETURN commit();

    // Rollback the current transaction.
    SQLRETURN rollback();

    // End the transaction with the specified completion type.
    SQLRETURN end_transaction(SQLSMALLINT completion_type);

    // Enable or disable autocommit mode.
    SQLRETURN set_autocommit(bool value);

    //  Check whether autocommit is enabled.
    bool get_autocommit() const;

private:

    std::wstring _conn_str;     // Connection string
    SqlHandlePtr _env_handle;   // Environment handle
    SqlHandlePtr _dbc_handle;   // Connection handle

    bool _autocommit = false;
    std::shared_ptr<Connection> _conn; 
};
#endif // CONNECTION_H