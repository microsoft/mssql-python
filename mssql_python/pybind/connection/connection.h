// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

// INFO|TODO - Note that is file is Windows specific right now. Making it arch agnostic will be
//             taken up in future.

#ifndef CONNECTION_H
#define CONNECTION_H

#include "ddbc_bindings.h"

// Represents a single ODBC database connection.
// Manages connection handles.
// Note: This class does NOT implement pooling logic directly.

class Connection {
public:
    Connection(const std::wstring& conn_str, bool autocommit = false);
    ~Connection();

    // Establish the connection using the stored connection string.
    SQLRETURN connect(const py::dict& attrs_before = py::dict());

    // Close the connection and free resources.
    SQLRETURN close();

    // Commit the current transaction.
    SQLRETURN commit();

    // Rollback the current transaction.
    SQLRETURN rollback();

    // Enable or disable autocommit mode.
    SQLRETURN setAutocommit(bool value);

    //  Check whether autocommit is enabled.
    bool getAutocommit() const;

    // Allocate a new statement handle on this connection.
    SqlHandlePtr allocStatementHandle();

private:
    void allocDbcHandle();
    SQLRETURN connectToDb();

    std::wstring _conn_str;
    SqlHandlePtr _dbc_handle;
    bool _autocommit = false;
    std::shared_ptr<Connection> _conn; 

    SQLRETURN set_attribute(SQLINTEGER attribute, pybind11::object value);
    void apply_attrs_before(const pybind11::dict& attrs);
};

#endif // CONNECTION_H