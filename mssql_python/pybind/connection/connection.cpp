// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

// INFO|TODO - Note that is file is Windows specific right now. Making it arch agnostic will be
//             taken up in future

#include "connection.h"
#include <vector>
#include <pybind11/pybind11.h>

#define SQL_COPT_SS_ACCESS_TOKEN   1256  // Custom attribute ID for access token

SqlHandlePtr Connection::_envHandle = nullptr;
//-------------------------------------------------------------------------------------------------
// Implements the Connection class declared in connection.h.
// This class wraps low-level ODBC operations like connect/disconnect,
// transaction control, and autocommit configuration.
//-------------------------------------------------------------------------------------------------
Connection::Connection(const std::wstring& conn_str, bool autocommit)
    : _connStr(conn_str) , _autocommit(autocommit) {
    if (!_envHandle) {
        LOG("Allocating environment handle");
        SQLHANDLE env = nullptr;
        if (!SQLAllocHandle_ptr) {
            LOG("Function pointers not initialized, loading driver");
            DriverLoader::getInstance().loadDriver();
        }
        SQLRETURN ret = SQLAllocHandle_ptr(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
        checkError(ret);
        _envHandle = std::make_shared<SqlHandle>(SQL_HANDLE_ENV, env);

        LOG("Setting environment attributes");
        ret = SQLSetEnvAttr_ptr(_envHandle->get(), SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3_80, 0);
        checkError(ret);
    }
    allocateDbcHandle();
}

Connection::~Connection() {
    disconnect();   // fallback if user forgets to disconnect
}

// Allocates connection handle
void Connection::allocateDbcHandle() {
    SQLHANDLE dbc = nullptr;
    LOG("Allocate SQL Connection Handle");
    SQLRETURN ret = SQLAllocHandle_ptr(SQL_HANDLE_DBC, _envHandle->get(), &dbc);
    checkError(ret);
    _dbcHandle = std::make_shared<SqlHandle>(SQL_HANDLE_DBC, dbc);
}

SQLRETURN Connection::connect(const py::dict& attrs_before) {
    LOG("Connecting to database");
    // Apply access token before connect
    if (!attrs_before.is_none() && py::len(attrs_before) > 0) {
        LOG("Apply attributes before connect");
        applyAttrsBefore(attrs_before);
        if (_autocommit) {
            setAutocommit(_autocommit);
        }
    }
    SQLRETURN ret = SQLDriverConnect_ptr(
        _dbcHandle->get(), nullptr,
        (SQLWCHAR*)_connStr.c_str(), SQL_NTS,
        nullptr, 0, nullptr, SQL_DRIVER_NOPROMPT);
    checkError(ret);
}

void Connection::disconnect() {
    if (_dbcHandle) {
        LOG("Disconnecting from database");
        SQLRETURN ret = SQLDisconnect_ptr(_dbcHandle->get());
        checkError(ret);
        _dbcHandle.reset(); // triggers SQLFreeHandle via destructor, if last owner
    }
    else {
        LOG("No connection handle to disconnect");
    }
}

// TODO: Add an exception class in C++ for error handling, DB spec compliant
void Connection::checkError(SQLRETURN ret) const{
    if (!SQL_SUCCEEDED(ret)) {
        ErrorInfo err = SQLCheckError_Wrap(SQL_HANDLE_DBC, _dbcHandle, ret);
        std::string errorMsg = std::string(err.ddbcErrorMsg.begin(), err.ddbcErrorMsg.end());
        ThrowStdException(errorMsg);
    }
}

void Connection::commit() {
    if (!_dbcHandle) {
        ThrowStdException("Connection handle not allocated");
    }
    LOG("Committing transaction");
    SQLRETURN ret = SQLEndTran_ptr(SQL_HANDLE_DBC, _dbcHandle->get(), SQL_COMMIT);
    checkError(ret);
}

void Connection::rollback() {
    if (!_dbcHandle) {
        ThrowStdException("Connection handle not allocated");
    }
    LOG("Rolling back transaction");
    SQLRETURN ret = SQLEndTran_ptr(SQL_HANDLE_DBC, _dbcHandle->get(), SQL_ROLLBACK);
    checkError(ret);
}

void Connection::setAutocommit(bool enable) {
    if (!_dbcHandle) {
        ThrowStdException("Connection handle not allocated");
    }
    SQLINTEGER value = enable ? SQL_AUTOCOMMIT_ON : SQL_AUTOCOMMIT_OFF;
    LOG("Set SQL Connection Attribute");
    SQLRETURN ret = SQLSetConnectAttr_ptr(_dbcHandle->get(), SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)value, 0);
    checkError(ret);
    _autocommit = enable;
}

bool Connection::getAutocommit() const {
    if (!_dbcHandle) {
        ThrowStdException("Connection handle not allocated");
    }
    LOG("Get SQL Connection Attribute");
    SQLINTEGER value;
    SQLINTEGER string_length;
    SQLRETURN ret = SQLGetConnectAttr_ptr(_dbcHandle->get(), SQL_ATTR_AUTOCOMMIT, &value, sizeof(value), &string_length);
    checkError(ret);
    return value == SQL_AUTOCOMMIT_ON;
}

SqlHandlePtr Connection::allocStatementHandle() {
    if (!_dbcHandle) {
        ThrowStdException("Connection handle not allocated");
    }
    LOG("Allocating statement handle");
    SQLHANDLE stmt = nullptr;
    SQLRETURN ret = SQLAllocHandle_ptr(SQL_HANDLE_STMT, _dbcHandle->get(), &stmt);
    checkError(ret);
    return std::make_shared<SqlHandle>(SQL_HANDLE_STMT, stmt);
}


SQLRETURN Connection::setAttribute(SQLINTEGER attribute, py::object value) {
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

    SQLRETURN ret = SQLSetConnectAttr_ptr(_dbcHandle->get(), attribute, ptr, length);
    if (!SQL_SUCCEEDED(ret)) {
        LOG("Failed to set attribute");
    }
    else {
        LOG("Set attribute successfully");
    }
    return ret;
}

void Connection::applyAttrsBefore(const py::dict& attrs) {
    for (const auto& item : attrs) {
        int key;
        try {
            key = py::cast<int>(item.first);
        } catch (...) {
            continue;
        }

        if (key == SQL_COPT_SS_ACCESS_TOKEN) {   
            SQLRETURN ret = setAttribute(key, py::reinterpret_borrow<py::object>(item.second));
            if (!SQL_SUCCEEDED(ret)) {
                ThrowStdException("Failed to set access token before connect");
            }
        }
    }
}