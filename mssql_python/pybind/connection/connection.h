// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

// INFO|TODO - Note that is file is Windows specific right now. Making it arch agnostic will be
//             taken up in future.

#pragma once
#include "ddbc_bindings.h"

// Represents a single ODBC database connection.
// Manages connection handles.
// Note: This class does NOT implement pooling logic directly.

class Connection {
public:
    Connection(const std::wstring& conn_str, bool autocommit = false);
    ~Connection();

    // Establish the connection using the stored connection string.
    void connect();

    // Disconnect and free the connection handle.
    void disconnect();

    // Commit the current transaction.
    void commit();

    // Rollback the current transaction.
    void rollback();

    // Enable or disable autocommit mode.
    void setAutocommit(bool value);

    //  Check whether autocommit is enabled.
    bool getAutocommit() const;

    // Allocate a new statement handle on this connection.
    SqlHandlePtr allocStatementHandle();

private:
    void allocateDbcHandle();
    void checkError(SQLRETURN ret, const std::string& msg) const;

    std::wstring _connStr;
    bool _usePool = false;
    bool _autocommit = true;
    SqlHandlePtr _dbcHandle;

    static SqlHandlePtr _envHandle;
};
