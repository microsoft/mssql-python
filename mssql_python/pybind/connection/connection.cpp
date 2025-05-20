// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

// INFO|TODO - Note that is file is Windows specific right now. Making it arch agnostic will be
//             taken up in future

#include "connection.h"
#include <iostream>
#include <vector>
#include <pybind11/pybind11.h>

//-------------------------------------------------------------------------------------------------
// Implements the Connection class declared in connection.h.
// This class wraps low-level ODBC operations like connect/disconnect,
// transaction control, and autocommit configuration.
//-------------------------------------------------------------------------------------------------
Connection::Connection(const std::wstring& conn_str, bool autocommit)
    : _conn_str(conn_str) , _autocommit(autocommit) {}

Connection::~Connection() {
    close();    // Ensure the connection is closed when the object is destroyed.
}

SQLRETURN Connection::connect() {
    allocDbcHandle();
    return connectToDb();
}

// Allocates DBC handle
void Connection::allocDbcHandle() {
    SQLHANDLE dbc = nullptr;
    LOG("Allocate SQL Connection Handle");
    SQLRETURN ret = SQLAllocHandle_ptr(SQL_HANDLE_DBC, getSharedEnvHandle()->get(), &dbc);
    if (!SQL_SUCCEEDED(ret)) {
        throw std::runtime_error("Failed to allocate connection handle");
    }
    _dbc_handle = std::make_shared<SqlHandle>(SQL_HANDLE_DBC, dbc);
}

// Connects to the database
SQLRETURN Connection::connectToDb() {
    LOG("Connecting to database");
    SQLRETURN ret = SQLDriverConnect_ptr(_dbc_handle->get(), nullptr,
                                         (SQLWCHAR*)_conn_str.c_str(), SQL_NTS,
                                         nullptr, 0, nullptr, SQL_DRIVER_NOPROMPT);
    if (!SQL_SUCCEEDED(ret)) {
        throw std::runtime_error("Failed to connect to database");
    }
    LOG("Connected to database successfully");
    return ret;
}

SQLRETURN Connection::close() {
    if (!_dbc_handle) {
        LOG("No connection handle to close");
        return SQL_SUCCESS;
    }
    LOG("Disconnect from MSSQL");
    if (!SQLDisconnect_ptr) {
        LOG("Function pointer not initialized. Loading the driver.");
        DriverLoader::getInstance().loadDriver();
    }

    SQLRETURN ret = SQLDisconnect_ptr(_dbc_handle->get());
    _dbc_handle.reset();
    return ret;
}

SQLRETURN Connection::commit() {
    if (!_dbc_handle) {
        throw std::runtime_error("Connection handle not allocated");
    }
    LOG("Committing transaction");
    SQLRETURN ret = SQLEndTran_ptr(SQL_HANDLE_DBC, _dbc_handle->get(), SQL_COMMIT);
    if (!SQL_SUCCEEDED(ret)) {
        throw std::runtime_error("Failed to commit transaction");
    }
    return ret;
}

SQLRETURN Connection::rollback() {
    if (!_dbc_handle) {
        throw std::runtime_error("Connection handle not allocated");
    }
    LOG("Rolling back transaction");
    SQLRETURN ret = SQLEndTran_ptr(SQL_HANDLE_DBC, _dbc_handle->get(), SQL_ROLLBACK);
    if (!SQL_SUCCEEDED(ret)) {
        throw std::runtime_error("Failed to rollback transaction");
    }
    return ret;
}

SQLRETURN Connection::setAutocommit(bool enable) {
    if (!_dbc_handle) {
        throw std::runtime_error("Connection handle not allocated");
    }
    SQLINTEGER value = enable ? SQL_AUTOCOMMIT_ON : SQL_AUTOCOMMIT_OFF;
    LOG("Set SQL Connection Attribute - Autocommit");   
    SQLRETURN ret = SQLSetConnectAttr_ptr(_dbc_handle->get(), SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)value, 0);
    if (!SQL_SUCCEEDED(ret)) {
        throw std::runtime_error("Failed to set autocommit mode.");
    }
    _autocommit = enable;
    return ret;
}

bool Connection::getAutocommit() const {
    if (!_dbc_handle) {
        throw std::runtime_error("Connection handle not allocated");
    }
    LOG("Get SQL Connection Attribute");
    SQLINTEGER value;
    SQLINTEGER string_length;
    SQLGetConnectAttr_ptr(_dbc_handle->get(), SQL_ATTR_AUTOCOMMIT, &value, sizeof(value), &string_length);
    
    return value == SQL_AUTOCOMMIT_ON;
}

SQLRETURN set_attribute(SQLINTEGER Attribute, py::object ValuePtr) {
    LOG("Set SQL Connection Attribute");
    if (!SQLSetConnectAttr_ptr) {
        LOG("Function pointer not initialized. Loading the driver.");
        DriverLoader::getInstance().loadDriver();  // Load the driver
    }

    // Print the type of ValuePtr and attribute value - helpful for debugging
    LOG("Type of ValuePtr: {}, Attribute: {}", py::type::of(ValuePtr).attr("__name__").cast<std::string>(), Attribute);

    SQLPOINTER value = 0;
    SQLINTEGER length = 0;

    if (py::isinstance<py::int_>(ValuePtr)) {
        // Handle integer values
        int intValue = ValuePtr.cast<int>();
        value = reinterpret_cast<SQLPOINTER>(intValue);
        length = SQL_IS_INTEGER;  // Integer values don't require a length
    } else if (py::isinstance<py::bytes>(ValuePtr) || py::isinstance<py::bytearray>(ValuePtr)) {
        // Handle byte or bytearray values (like access tokens)
        // Store in static buffer to ensure memory remains valid during connection
        static std::vector<std::string> bytesBuffers;
        bytesBuffers.push_back(ValuePtr.cast<std::string>());
        value = const_cast<char*>(bytesBuffers.back().c_str());
        length = SQL_IS_POINTER;  // Indicates we're passing a pointer (required for token)
    } else {
        LOG("Unsupported ValuePtr type");
        return SQL_ERROR;
    }

    SQLRETURN ret = SQLSetConnectAttr_ptr(_dbc_handle->get(), Attribute, value, length);
    if (!SQL_SUCCEEDED(ret)) {
        LOG("Failed to set Connection attribute");
    }
    else {
        LOG("Set Connection attribute successfully");
    }
    return ret;
}

SqlHandlePtr Connection::allocStatementHandle() {
    LOG("Allocating statement handle");
    SQLHANDLE stmt = nullptr;
    SQLRETURN ret = SQLAllocHandle_ptr(SQL_HANDLE_STMT, _dbc_handle->get(), &stmt);
    if (!SQL_SUCCEEDED(ret)) {
        throw std::runtime_error("Failed to allocate statement handle");
    }
    return std::make_shared<SqlHandle>(SQL_HANDLE_STMT, stmt);
}

SQLRETURN Connection::set_attribute(SQLINTEGER attribute, py::object value) {
    LOG("Setting SQL attribute");

    SQLPOINTER ptr = nullptr;
    SQLINTEGER length = 0;

    if (py::isinstance<py::int_>(value)) {
        int intValue = value.cast<int>();
        ptr = reinterpret_cast<SQLPOINTER>(static_cast<uintptr_t>(intValue));
        length = SQL_IS_INTEGER;
    } else if (py::isinstance<py::bytes>(value) || py::isinstance<py::bytearray>(value)) {
        static std::vector<std::string> buffers;
        buffers.emplace_back(value.cast<std::string>());
        ptr = const_cast<char*>(buffers.back().c_str());
        length = static_cast<SQLINTEGER>(buffers.back().size());
    } else {
        LOG("Unsupported attribute value type");
        return SQL_ERROR;
    }

    SQLRETURN ret = SQLSetConnectAttr_ptr(_dbc_handle->get(), attribute, ptr, length);
    if (!SQL_SUCCEEDED(ret)) {
        LOG("Failed to set attribute");
    }
    else {
        LOG("Set attribute successfully");
    }
    return ret;
}

void Connection::apply_attrs_before(const py::dict& attrs) {
    for (const auto& item : attrs) {
        int key;
        try {
            key = py::cast<int>(item.first);
        } catch (...) {
            continue;
        }

        //do not hard code the key values
        if (key == 1256) {   
            SQLRETURN ret = set_attribute(key, py::reinterpret_borrow<py::object>(item.second));
            if (!SQL_SUCCEEDED(ret)) {
                throw std::runtime_error("Failed to set access token before connect");
            }
        }
    }
}

SqlHandlePtr Connection::getSharedEnvHandle() {
    static std::once_flag flag;
    static SqlHandlePtr env_handle;

    std::call_once(flag, []() {
        LOG("Allocating environment handle");
        SQLHANDLE env = nullptr;
        if (!SQLAllocHandle_ptr) {
            LOG("Function pointers not initialized, loading driver");
            DriverLoader::getInstance().loadDriver();
        }
        SQLRETURN ret = SQLAllocHandle_ptr(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
        if (!SQL_SUCCEEDED(ret)) {
            throw std::runtime_error("Failed to allocate environment handle");
        }
        env_handle = std::make_shared<SqlHandle>(SQL_HANDLE_ENV, env);

        LOG("Setting environment attributes");
        ret = SQLSetEnvAttr_ptr(env_handle->get(), SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3_80, 0);
        if (!SQL_SUCCEEDED(ret)) {
            throw std::runtime_error("Failed to set environment attribute");
        }
    });
    return env_handle;
}