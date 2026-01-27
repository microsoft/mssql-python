// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma once
#include "../ddbc_bindings.h"
#include <memory>
#include <string>

// Represents a single ODBC database connection.
// Manages connection handles.
// Note: This class does NOT implement pooling logic directly.

class Connection {
  public:
    Connection(const std::wstring& connStr, bool fromPool);

    ~Connection();

    // Establish the connection using the stored connection string.
    void connect(const py::dict& attrs_before = py::dict());

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
    bool isAlive() const;
    bool reset();
    void updateLastUsed();
    std::chrono::steady_clock::time_point lastUsed() const;

    // Allocate a new statement handle on this connection.
    SqlHandlePtr allocStatementHandle();

    // Get information about the driver and data source
    py::object getInfo(SQLUSMALLINT infoType) const;

    SQLRETURN setAttribute(SQLINTEGER attribute, py::object value);

    // Add getter for DBC handle for error reporting
    const SqlHandlePtr& getDbcHandle() const { return _dbcHandle; }

  private:
    void allocateDbcHandle();
    void checkError(SQLRETURN ret) const;
    void applyAttrsBefore(const py::dict& attrs_before);

    std::wstring _connStr;
    bool _fromPool = false;
    bool _autocommit = true;
    SqlHandlePtr _dbcHandle;
    std::chrono::steady_clock::time_point _lastUsed;
    std::wstring wstrStringBuffer;  // wstr buffer for string attribute setting
    std::string strBytesBuffer;     // string buffer for byte attributes setting
    
    // Track child statement handles to mark them as implicitly freed when connection closes
    // Uses weak_ptr to avoid circular references and allow normal cleanup
    std::vector<std::weak_ptr<SqlHandle>> _childStatementHandles;
};

class ConnectionHandle {
  public:
    ConnectionHandle(const std::string& connStr, bool usePool,
                     const py::dict& attrsBefore = py::dict());
    ~ConnectionHandle();

    void close();
    void commit();
    void rollback();
    void setAutocommit(bool enabled);
    bool getAutocommit() const;
    SqlHandlePtr allocStatementHandle();
    void setAttr(int attribute, py::object value);

    // Get information about the driver and data source
    py::object getInfo(SQLUSMALLINT infoType) const;

  private:
    std::shared_ptr<Connection> _conn;
    bool _usePool;
    std::wstring _connStr;
};
