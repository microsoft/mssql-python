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
    // Connection(const std::wstring& conn_str, bool autocommit, bool usePool);
    Connection(const std::wstring& connStr, bool usePool = false);

    ~Connection();

    // SQLRETURN connect(const py::dict& attrs_before = py::dict());

    // SQLRETURN directConnect(const py::dict& attrs_before = py::dict());

    void connect();
    void disconnect();
    bool reset();  // for reuse in pool

    void commit();
    void rollback();
    void setAutocommit(bool enabled);
    bool getAutocommit() const;

    bool isAlive() const;
    const std::wstring& connStr() const;

    // SQLRETURN close();

    // // Close the connection and free resources.
    // SQLRETURN disconnect();

    // // Commit the current transaction.
    // SQLRETURN commit();

    // // Rollback the current transaction.
    // SQLRETURN rollback();

    // // Enable or disable autocommit mode.
    // SQLRETURN setAutocommit(bool value);

    // //  Check whether autocommit is enabled.
    // bool getAutocommit() const;

    // // Allocate a new statement handle on this connection.
    SqlHandlePtr allocStatementHandle();

    // bool isAlive() const;
    // void reset();
    // void updateLastUsed();
    // std::chrono::steady_clock::time_point lastUsed() const { return _last_used; }

private:
    // void allocDbcHandle();
    // SQLRETURN connectToDb();    

    // std::wstring _conn_str;
    // SqlHandlePtr _dbc_handle;
    // bool _autocommit = false;
    
    // static SqlHandlePtr getSharedEnvHandle();
    // SQLRETURN setAttribute(SQLINTEGER attribute, pybind11::object value);
    // void applyAttrsBefore(const pybind11::dict& attrs);

    // bool _usePool;
    // bool _is_closed;
    // std::chrono::steady_clock::time_point _last_used;
    // std::shared_ptr<Connection> _conn;

    void allocate();
    void checkError(SQLRETURN ret, const std::string& msg);

    std::wstring _connStr;
    bool _usePool = false;
    bool _autocommit = true;
    SqlHandlePtr _dbcHandle;

    static SqlHandlePtr _envHandle;
};


class ConnectionHandle {
public:
    ConnectionHandle(const std::wstring& connStr, bool usePool);

    void close();
    void commit();
    void rollback();
    void setAutocommit(bool enabled);
    bool getAutocommit() const;
    SqlHandlePtr allocStatementHandle();


private:
    std::shared_ptr<Connection> _conn;
    bool _usePool;
    std::wstring _connStr;
    bool _closed = false;
};