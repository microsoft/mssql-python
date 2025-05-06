// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#ifndef CONNECTION_H
#define CONNECTION_H

#include "ddbc_bindings.h"

class Connection{
public:
    Connection(const std::wstring& conn_str);
    ~Connection();

    SQLRETURN connect();
    SQLRETURN close();
    SQLRETURN commit();
    SQLRETURN rollback();
    
    SQLRETURN set_autocommit(bool value);
    bool get_autocommit() const;
    SqlHandlePtr alloc_statement_handle(); // Will later be moved to cursor c++ class
    bool is_open() const;

private:

    std::wstring _conn_str;
    SqlHandlePtr _env_handle;
    SqlHandlePtr _dbc_handle;
    bool _is_open = false;
    bool _autocommit = false;
};
#endif // CONNECTION_H