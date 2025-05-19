// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#ifndef CONNECTION_H
#define CONNECTION_H

#include "ddbc_bindings.h"

class Connection {
public:
    Connection(const std::wstring& conn_str);
    ~Connection();

    SQLRETURN connect();
    SQLRETURN close();
    SQLRETURN commit();
    SQLRETURN rollback();
    SQLRETURN end_transaction(SQLSMALLINT completion_type);
    SQLRETURN set_autocommit(bool value);
    bool get_autocommit() const;

private:

    std::wstring _conn_str;
    SqlHandlePtr _env_handle;
    SqlHandlePtr _dbc_handle;

    bool _autocommit = false;
    std::shared_ptr<Connection> _conn; 
};
#endif // CONNECTION_H