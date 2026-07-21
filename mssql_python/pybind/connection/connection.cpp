// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#include "connection/connection.h"
#include "connection/connection_pool.h"
#include "utf_utils.h"
#include <algorithm>
#include <chrono>
#include <memory>
#include <pybind11/pybind11.h>
#include <regex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#define SQL_COPT_SS_ACCESS_TOKEN 1256  // Custom attribute ID for access token
#define SQL_MAX_SMALL_INT 32767        // Maximum value for SQLSMALLINT

// Logging uses LOG() macro for all diagnostic output
#include "logger_bridge.hpp"

static SqlHandlePtr getEnvHandle() {
    static SqlHandlePtr envHandle = []() -> SqlHandlePtr {
        LOG("Allocating ODBC environment handle");
        if (!SQLAllocHandle_ptr) {
            LOG("Function pointers not initialized, loading driver");
            DriverLoader::getInstance().loadDriver();
        }
        SQLHANDLE env = nullptr;
        SQLRETURN ret = SQLAllocHandle_ptr(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
        if (!SQL_SUCCEEDED(ret)) {
            ThrowStdException("Failed to allocate environment handle");
        }
        ret = SQLSetEnvAttr_ptr(env, SQL_ATTR_ODBC_VERSION,
                                reinterpret_cast<void*>(SQL_OV_ODBC3_80), 0);
        if (!SQL_SUCCEEDED(ret)) {
            ThrowStdException("Failed to set environment attributes");
        }
        return std::make_shared<SqlHandle>(static_cast<SQLSMALLINT>(SQL_HANDLE_ENV), env);
    }();

    return envHandle;
}

//-------------------------------------------------------------------------------------------------
// Implements the Connection class declared in connection.h.
// This class wraps low-level ODBC operations like connect/disconnect,
// transaction control, and autocommit configuration.
//-------------------------------------------------------------------------------------------------
Connection::Connection(const std::u16string& conn_str, bool use_pool)
    : _connStr(conn_str), _autocommit(false), _fromPool(use_pool) {
    allocateDbcHandle();
}

Connection::~Connection() {
    disconnect();  // fallback if user forgets to disconnect
}

// Allocates connection handle
void Connection::allocateDbcHandle() {
    auto _envHandle = getEnvHandle();
    SQLHANDLE dbc = nullptr;
    LOG("Allocating SQL Connection Handle");
    SQLRETURN ret = SQLAllocHandle_ptr(SQL_HANDLE_DBC, _envHandle->get(), &dbc);
    checkError(ret);
    _dbcHandle = std::make_shared<SqlHandle>(static_cast<SQLSMALLINT>(SQL_HANDLE_DBC), dbc);
}

void Connection::connect(const py::dict& attrs_before) {
    LOG("Connecting to database");
    // Apply access token before connect
    if (!attrs_before.is_none() && py::len(attrs_before) > 0) {
        LOG("Apply attributes before connect");
        applyAttrsBefore(attrs_before);
        if (_autocommit) {
            setAutocommit(_autocommit);
        }
    }
    SQLWCHAR* connStrPtr = reinterpretU16stringAsSqlWChar(_connStr);
    SQLRETURN ret;
    {
        // Release the GIL during the blocking ODBC connect call.
        // SQLDriverConnect involves DNS resolution, TCP handshake, TLS negotiation,
        // and SQL Server authentication — all pure I/O that doesn't need the GIL.
        // This allows other Python threads to run concurrently.
        py::gil_scoped_release release;
        ret = SQLDriverConnect_ptr(_dbcHandle->get(), nullptr, connStrPtr, SQL_NTS, nullptr,
                                   0, nullptr, SQL_DRIVER_NOPROMPT);
    }
    checkError(ret);
    updateLastUsed();
}

void Connection::disconnect() {
    if (_dbcHandle) {
        LOG("Disconnecting from database");

        // Check if we hold the GIL so we can conditionally release it.
        // The GIL is held when called from pybind11-bound methods but may NOT
        // be held in destructor paths (C++ shared_ptr ref-count drop, shutdown).
        bool hasGil = PyGILState_Check() != 0;

        // CRITICAL FIX: Mark all child statement handles as implicitly freed
        // When we free the DBC handle below, the ODBC driver will automatically free
        // all child STMT handles. We need to tell the SqlHandle objects about this
        // so they don't try to free the handles again during their destruction.
        
        // THREAD-SAFETY: Lock mutex to safely access _childStatementHandles
        // This protects against concurrent allocStatementHandle() calls or GC finalizers
        {
            std::lock_guard<std::mutex> lock(_childHandlesMutex);
            
            // First compact: remove expired weak_ptrs (they're already destroyed)
            size_t originalSize = _childStatementHandles.size();
            _childStatementHandles.erase(
                std::remove_if(_childStatementHandles.begin(), _childStatementHandles.end(),
                               [](const std::weak_ptr<SqlHandle>& wp) { return wp.expired(); }),
                _childStatementHandles.end());
            
            LOG("Compacted child handles: %zu -> %zu (removed %zu expired)",
                originalSize, _childStatementHandles.size(),
                originalSize - _childStatementHandles.size());
            
            LOG("Marking %zu child statement handles as implicitly freed",
                _childStatementHandles.size());
            for (auto& weakHandle : _childStatementHandles) {
                if (auto handle = weakHandle.lock()) {
                    // SAFETY ASSERTION: Only STMT handles should be in this vector
                    // This is guaranteed by allocStatementHandle() which only creates STMT handles
                    // If this assertion fails, it indicates a serious bug in handle tracking
                    if (handle->type() != SQL_HANDLE_STMT) {
                        LOG_ERROR("CRITICAL: Non-STMT handle (type=%d) found in _childStatementHandles. "
                                  "This will cause a handle leak!", handle->type());
                        continue;  // Skip marking to prevent leak
                    }
                    handle->markImplicitlyFreed();
                }
            }
            _childStatementHandles.clear();
            _allocationsSinceCompaction = 0;
        }  // Release lock before potentially slow SQLDisconnect call

        SQLRETURN ret;
        if (hasGil) {
            // Release the GIL during the blocking ODBC disconnect call.
            // This allows other Python threads to run while the network
            // round-trip completes.
            py::gil_scoped_release release;
            ret = SQLDisconnect_ptr(_dbcHandle->get());
        } else {
            // Destructor / shutdown path — GIL is not held, call directly.
            ret = SQLDisconnect_ptr(_dbcHandle->get());
        }
        // In destructor/shutdown paths, suppress errors to avoid
        // std::terminate() if this throws during stack unwinding.
        if (hasGil) {
            checkError(ret);
        } else if (!SQL_SUCCEEDED(ret)) {
            // Intentionally no LOG() here: LOG() acquires the GIL internally
            // via py::gil_scoped_acquire, which is unsafe during interpreter
            // shutdown or stack unwinding (can deadlock or call std::terminate).
        }
        // triggers SQLFreeHandle via destructor, if last owner
        _dbcHandle.reset();
    } else {
        LOG("No connection handle to disconnect");
    }
}

// TODO(microsoft): Add an exception class in C++ for error handling,
// DB spec compliant
void Connection::checkError(SQLRETURN ret) const {
    if (!SQL_SUCCEEDED(ret)) {
        // Format: "SQLSTATE:XXXXX:<odbc_error_message>" — parsed by _raise_connection_error()
        ErrorInfo err = SQLCheckError_Wrap(SQL_HANDLE_DBC, _dbcHandle, ret);
        std::string sqlState = err.sqlState;
        std::string errorMsg = err.ddbcErrorMsg;
        // Only add SQLSTATE prefix if we have a valid 5-character code
        if (sqlState.length() == 5) {
            ThrowStdException("SQLSTATE:" + sqlState + ":" + errorMsg);
        } else {
            // No valid SQLSTATE (e.g., SQL_INVALID_HANDLE) — throw clean error message
            ThrowStdException(errorMsg);
        }
    }
}

void Connection::commit() {
    if (!_dbcHandle) {
        ThrowStdException("Connection handle not allocated");
    }
    updateLastUsed();
    LOG("Committing transaction");
    SQLRETURN ret;
    {
        // Release the GIL during the blocking SQLEndTran network round-trip.
        py::gil_scoped_release release;
        ret = SQLEndTran_ptr(SQL_HANDLE_DBC, _dbcHandle->get(), SQL_COMMIT);
    }
    checkError(ret);
}

void Connection::rollback() {
    if (!_dbcHandle) {
        ThrowStdException("Connection handle not allocated");
    }
    updateLastUsed();
    LOG("Rolling back transaction");
    SQLRETURN ret;
    {
        // Release the GIL during the blocking SQLEndTran network round-trip.
        py::gil_scoped_release release;
        ret = SQLEndTran_ptr(SQL_HANDLE_DBC, _dbcHandle->get(), SQL_ROLLBACK);
    }
    checkError(ret);
}

void Connection::setAutocommit(bool enable) {
    if (!_dbcHandle) {
        ThrowStdException("Connection handle not allocated");
    }
    SQLINTEGER value = enable ? SQL_AUTOCOMMIT_ON : SQL_AUTOCOMMIT_OFF;
    LOG("Setting autocommit=%d", enable);
    SQLRETURN ret;
    {
        // Release the GIL during the blocking ODBC call. Holding the GIL
        // here can deadlock when the network path goes through another
        // Python thread (e.g. an in-process SSH tunnel via paramiko +
        // sshtunnel), since that thread also needs the GIL to run.
        py::gil_scoped_release release;
        ret = SQLSetConnectAttr_ptr(_dbcHandle->get(), SQL_ATTR_AUTOCOMMIT,
                                    reinterpret_cast<SQLPOINTER>(static_cast<SQLULEN>(value)), 0);
    }
    checkError(ret);
    if (value == SQL_AUTOCOMMIT_ON) {
        LOG("Autocommit enabled");
    } else {
        LOG("Autocommit disabled");
    }
    _autocommit = enable;
}

bool Connection::getAutocommit() const {
    if (!_dbcHandle) {
        ThrowStdException("Connection handle not allocated");
    }
    LOG("Getting autocommit attribute");
    SQLINTEGER value;
    SQLINTEGER string_length;
    SQLRETURN ret = SQLGetConnectAttr_ptr(_dbcHandle->get(), SQL_ATTR_AUTOCOMMIT, &value,
                                          sizeof(value), &string_length);
    checkError(ret);
    return value == SQL_AUTOCOMMIT_ON;
}

SqlHandlePtr Connection::allocStatementHandle() {
    if (!_dbcHandle) {
        ThrowStdException("Connection handle not allocated");
    }
    updateLastUsed();
    LOG("Allocating statement handle");
    SQLHANDLE stmt = nullptr;
    SQLRETURN ret = SQLAllocHandle_ptr(SQL_HANDLE_STMT, _dbcHandle->get(), &stmt);
    checkError(ret);
    auto stmtHandle = std::make_shared<SqlHandle>(static_cast<SQLSMALLINT>(SQL_HANDLE_STMT), stmt);

    // THREAD-SAFETY: Lock mutex before modifying _childStatementHandles
    // This protects against concurrent disconnect() or allocStatementHandle() calls,
    // or GC finalizers running from different threads
    {
        std::lock_guard<std::mutex> lock(_childHandlesMutex);
        
        // Track this child handle so we can mark it as implicitly freed when connection closes
        // Use weak_ptr to avoid circular references and allow normal cleanup
        _childStatementHandles.push_back(stmtHandle);
        _allocationsSinceCompaction++;

        // Compact expired weak_ptrs only periodically to avoid O(n²) overhead
        // This keeps allocation fast (O(1) amortized) while preventing unbounded growth
        // disconnect() also compacts, so this is just for long-lived connections with many cursors
        if (_allocationsSinceCompaction >= COMPACTION_INTERVAL) {
            size_t originalSize = _childStatementHandles.size();
            _childStatementHandles.erase(
                std::remove_if(_childStatementHandles.begin(), _childStatementHandles.end(),
                               [](const std::weak_ptr<SqlHandle>& wp) { return wp.expired(); }),
                _childStatementHandles.end());
            _allocationsSinceCompaction = 0;
            LOG("Periodic compaction: %zu -> %zu handles (removed %zu expired)",
                originalSize, _childStatementHandles.size(),
                originalSize - _childStatementHandles.size());
        }
    }  // Release lock

    return stmtHandle;
}

SQLRETURN Connection::setAttribute(SQLINTEGER attribute, py::object value) {
    LOG("Setting SQL attribute=%d", attribute);
    // SQLPOINTER ptr = nullptr;
    // SQLINTEGER length = 0;

    if (py::isinstance<py::int_>(value)) {
        // Get the integer value
        int64_t longValue = value.cast<int64_t>();

        SQLRETURN ret;
        {
            // Release the GIL around the ODBC call for consistency with the
            // other connection-attribute paths; some attributes can block.
            py::gil_scoped_release release;
            ret = SQLSetConnectAttr_ptr(
                _dbcHandle->get(), attribute,
                reinterpret_cast<SQLPOINTER>(static_cast<SQLULEN>(longValue)), SQL_IS_INTEGER);
        }

        if (!SQL_SUCCEEDED(ret)) {
            LOG("Failed to set integer attribute=%d, ret=%d", attribute, ret);
        } else {
            LOG("Set integer attribute=%d successfully", attribute);
        }
        return ret;
    } else if (py::isinstance<py::str>(value)) {
        try {
            // Store the value in a Connection-owned, per-attribute member
            // buffer so the memory remains valid for the lifetime of the
            // Connection object. Some ODBC connect attributes (notably
            // SQL_COPT_SS_ACCESS_TOKEN, 1256) are "deferred": the MS driver
            // stores the caller's pointer at SQLSetConnectAttr time and
            // dereferences it later during SQLDriverConnect to build the
            // FedAuth login packet. A stack-local buffer freed when this
            // function returns would cause a use-after-free during connect
            // (issue #594). Keying by attribute id also prevents a second
            // deferred attribute from invalidating the pointer stored for
            // the first.
            //
            // Lifetime: the buffer MUST outlive every potential dereference
            // of the deferred-attribute pointer by the driver, which
            // includes paths beyond the initial connect (Idle Connection
            // Resiliency re-auth on a dropped socket, transparent pool
            // checkout re-handshake). SQL_ATTR_RESET_CONNECTION (see
            // Connection::reset()) only wipes per-session state and does
            // NOT tear down the driver-side authentication context, so the
            // per-attribute buffers are NOT cleared on reset()/checkin;
            // they are released only when the Connection object itself is
            // destroyed.
            //
            // Note: attrs_before is applied once, sequentially, during
            // connect(); the Connection's attribute setters are not designed
            // for concurrent mutation from multiple threads.
            auto& buf = this->_attrStringBuffers[attribute];
            buf = value.cast<std::u16string>();

            SQLPOINTER ptr = reinterpretU16stringAsSqlWChar(buf);
            SQLINTEGER length =
                static_cast<SQLINTEGER>(buf.length() * sizeof(SQLWCHAR));

            SQLRETURN ret;
            {
                py::gil_scoped_release release;
                ret = SQLSetConnectAttr_ptr(_dbcHandle->get(), attribute, ptr, length);
            }
            if (!SQL_SUCCEEDED(ret)) {
                LOG("Failed to set string attribute=%d, ret=%d", attribute, ret);
            } else {
                LOG("Set string attribute=%d successfully", attribute);
            }
            return ret;
        } catch (const std::exception& e) {
            LOG("Exception during string attribute=%d setting: %s", attribute, e.what());
            return SQL_ERROR;
        }
    } else if (py::isinstance<py::bytes>(value) || py::isinstance<py::bytearray>(value)) {
        try {
            // Store the value in a Connection-owned, per-attribute member
            // buffer so the memory remains valid for the lifetime of the
            // Connection object. SQL_COPT_SS_ACCESS_TOKEN (1256) is a
            // deferred attribute: the driver stores this pointer at
            // SQLSetConnectAttr time and dereferences it later during
            // SQLDriverConnect. A stack-local buffer freed when this
            // function returns would cause a use-after-free during connect
            // (issue #594, symptoms: SIGBUS on macOS, "Authentication
            // token is missing in the federated authentication message"
            // on Windows, TCP reset 0x2746 against Azure SQL). Keying by
            // attribute id also prevents a second deferred attribute from
            // invalidating the pointer stored for the first.
            //
            // Lifetime: the buffer MUST outlive every potential dereference
            // of the deferred-attribute pointer by the driver, which
            // includes paths beyond the initial connect:
            //   * Idle Connection Resiliency (ICR): if the underlying TCP
            //     connection drops while the connection sits idle in the
            //     pool, the driver transparently re-establishes it on the
            //     next use and re-runs the Login7 / FedAuth handshake,
            //     dereferencing the same stashed token pointer.
            //   * SQL_ATTR_RESET_CONNECTION pool checkin (see
            //     Connection::reset()) only wipes per-session state; the
            //     driver-side authentication context and the stashed
            //     deferred-attribute pointer are intentionally retained.
            // For these reasons the per-attribute buffers are NOT cleared
            // on reset()/checkin; they are released only when the
            // Connection object itself is destroyed.
            //
            // Note: attrs_before is applied once, sequentially, during
            // connect(); concurrent setAttribute() on the same Connection
            // from different threads is not a supported pattern.
            auto& buf = this->_attrBytesBuffers[attribute];
            buf = value.cast<std::string>();
            SQLPOINTER ptr = const_cast<char*>(buf.data());
            SQLINTEGER length = static_cast<SQLINTEGER>(buf.size());

            SQLRETURN ret;
            {
                py::gil_scoped_release release;
                ret = SQLSetConnectAttr_ptr(_dbcHandle->get(), attribute, ptr, length);
            }
            if (!SQL_SUCCEEDED(ret)) {
                LOG("Failed to set binary attribute=%d, ret=%d", attribute, ret);
            } else {
                LOG("Set binary attribute=%d successfully (length=%d)", attribute, length);
            }
            return ret;
        } catch (const std::exception& e) {
            LOG("Exception during binary attribute=%d setting: %s", attribute, e.what());
            return SQL_ERROR;
        }
    } else {
        LOG("Unsupported attribute value type for attribute=%d", attribute);
        return SQL_ERROR;
    }
}

void Connection::applyAttrsBefore(const py::dict& attrs) {
    for (const auto& item : attrs) {
        int key;
        try {
            key = py::cast<int>(item.first);
        } catch (...) {
            continue;
        }

        // Apply all supported attributes
        SQLRETURN ret = setAttribute(key, py::reinterpret_borrow<py::object>(item.second));
        if (!SQL_SUCCEEDED(ret)) {
            std::string attrName = std::to_string(key);
            std::string errorMsg = "Failed to set attribute " + attrName + " before connect";
            ThrowStdException(errorMsg);
        }
    }
}

bool Connection::isAlive() const {
    if (!_dbcHandle) {
        ThrowStdException("Connection handle not allocated");
    }
    SQLUINTEGER status;
    SQLRETURN ret =
        SQLGetConnectAttr_ptr(_dbcHandle->get(), SQL_ATTR_CONNECTION_DEAD, &status, 0, nullptr);
    return SQL_SUCCEEDED(ret) && status == SQL_CD_FALSE;
}

bool Connection::reset() {
    if (!_dbcHandle) {
        ThrowStdException("Connection handle not allocated");
    }
    LOG("Resetting connection via SQL_ATTR_RESET_CONNECTION");
    // NOTE: SQL_ATTR_RESET_CONNECTION is a pool-checkin reset: it asks the
    // driver to wipe per-session state (temp tables, open cursors, SET
    // options, etc.) on the next use. It does NOT tear down the underlying
    // TCP/TLS connection nor the driver-side authentication context, and
    // it does NOT discard the deferred connect attributes the driver has
    // stashed (e.g., the SQL_COPT_SS_ACCESS_TOKEN pointer used to build
    // the FedAuth Login7 packet). The driver may still dereference those
    // pointers after this reset on Idle Connection Resiliency re-auth or
    // a transparent reconnect, so the per-attribute buffers owned by this
    // Connection (_attrStringBuffers / _attrBytesBuffers) are intentionally
    // retained here. Clearing them would reintroduce issue #594 in a new
    // form (UAF during silent reconnect).
    SQLRETURN ret;
    {
        // Release the GIL around the ODBC call for consistency with the
        // other connection-attribute paths; some attributes can block.
        py::gil_scoped_release release;
        ret = SQLSetConnectAttr_ptr(_dbcHandle->get(), SQL_ATTR_RESET_CONNECTION,
                                    (SQLPOINTER)SQL_RESET_CONNECTION_YES, SQL_IS_INTEGER);
    }
    if (!SQL_SUCCEEDED(ret)) {
        LOG("Failed to reset connection (ret=%d). Marking as dead.", ret);
        return false;
    }

    // SQL_ATTR_RESET_CONNECTION does NOT reset the transaction isolation level.
    // Explicitly reset it to the default (SQL_TXN_READ_COMMITTED) to prevent
    // isolation level settings from leaking between pooled connection usages.
    LOG("Resetting transaction isolation level to READ COMMITTED");
    {
        py::gil_scoped_release release;
        ret = SQLSetConnectAttr_ptr(_dbcHandle->get(), SQL_ATTR_TXN_ISOLATION,
                                    (SQLPOINTER)SQL_TXN_READ_COMMITTED, SQL_IS_INTEGER);
    }
    if (!SQL_SUCCEEDED(ret)) {
        LOG("Failed to reset transaction isolation level (ret=%d). Marking as dead.", ret);
        return false;
    }

    updateLastUsed();
    return true;
}

void Connection::updateLastUsed() {
    _lastUsed = std::chrono::steady_clock::now();
}

std::chrono::steady_clock::time_point Connection::lastUsed() const {
    return _lastUsed;
}

ConnectionHandle::ConnectionHandle(const std::u16string& connStr, bool usePool,
                                   const py::dict& attrsBefore)
    : _usePool(usePool), _connStr(connStr) {
    if (_usePool) {
        _conn = ConnectionPoolManager::getInstance().acquireConnection(_connStr, attrsBefore);
    } else {
        _conn = std::make_shared<Connection>(_connStr, false);
        _conn->connect(attrsBefore);
    }
}

ConnectionHandle::~ConnectionHandle() {
    if (_conn) {
        close();
    }
}

void ConnectionHandle::close() {
    if (!_conn) {
        ThrowStdException("Connection object is not initialized");
    }
    if (_usePool) {
        ConnectionPoolManager::getInstance().returnConnection(_connStr, _conn);
    } else {
        _conn->disconnect();
    }
    _conn = nullptr;
}

void ConnectionHandle::commit() {
    if (!_conn) {
        ThrowStdException("Connection object is not initialized");
    }
    _conn->commit();
}

void ConnectionHandle::rollback() {
    if (!_conn) {
        ThrowStdException("Connection object is not initialized");
    }
    _conn->rollback();
}

void ConnectionHandle::setAutocommit(bool enabled) {
    if (!_conn) {
        ThrowStdException("Connection object is not initialized");
    }
    _conn->setAutocommit(enabled);
}

bool ConnectionHandle::getAutocommit() const {
    if (!_conn) {
        ThrowStdException("Connection object is not initialized");
    }
    return _conn->getAutocommit();
}

SqlHandlePtr ConnectionHandle::allocStatementHandle() {
    if (!_conn) {
        ThrowStdException("Connection object is not initialized");
    }
    return _conn->allocStatementHandle();
}

py::object Connection::getInfo(SQLUSMALLINT infoType) const {
    if (!_dbcHandle) {
        ThrowStdException("Connection handle not allocated");
    }

    // First call with NULL buffer to get required length
    SQLSMALLINT requiredLen = 0;
    SQLRETURN ret = SQLGetInfo_ptr(_dbcHandle->get(), infoType, NULL, 0, &requiredLen);

    if (!SQL_SUCCEEDED(ret)) {
        checkError(ret);
        return py::none();
    }

    // For zero-length results
    if (requiredLen == 0) {
        py::dict result;
        result["data"] = py::bytes("", 0);
        result["length"] = 0;
        result["info_type"] = infoType;
        return result;
    }

    // Cap buffer allocation to SQL_MAX_SMALL_INT to prevent excessive
    // memory usage
    SQLSMALLINT allocSize = requiredLen + 10;
    if (allocSize > SQL_MAX_SMALL_INT) {
        allocSize = SQL_MAX_SMALL_INT;
    }
    std::vector<char> buffer(allocSize, 0);  // Extra padding for safety

    // Get the actual data - avoid using std::min
    SQLSMALLINT bufferSize = requiredLen + 10;
    if (bufferSize > SQL_MAX_SMALL_INT) {
        bufferSize = SQL_MAX_SMALL_INT;
    }

    SQLSMALLINT returnedLen = 0;
    ret = SQLGetInfo_ptr(_dbcHandle->get(), infoType, buffer.data(), bufferSize, &returnedLen);

    if (!SQL_SUCCEEDED(ret)) {
        checkError(ret);
        return py::none();
    }

    // Create a dictionary with the raw data
    py::dict result;

    // IMPORTANT: Pass exactly what SQLGetInfo returned
    // No null-terminator manipulation, just pass the raw data
    result["data"] = py::bytes(buffer.data(), returnedLen);
    result["length"] = returnedLen;
    result["info_type"] = infoType;

    return result;
}

py::object ConnectionHandle::getInfo(SQLUSMALLINT infoType) const {
    if (!_conn) {
        ThrowStdException("Connection object is not initialized");
    }
    return _conn->getInfo(infoType);
}

// -----------------------------------------------------------------------------
// POC: Async query execution — driver capability probe (Step 1 of the plan
// described in ASYNC_QUERY_POC_ANALYSIS.md).
//
// Issues fixed-size SQLGetInfo calls to expose the driver's async support to
// Python as typed values, then performs a functional check by actually
// enabling SQL_ATTR_ASYNC_ENABLE on a fresh statement handle.
//
// The existing generic Connection::getInfo returns raw bytes; for these
// numeric infotypes we want the value as an int, decoded on the C++ side.
// -----------------------------------------------------------------------------

// SQL_ASYNC_STMT_FUNCTIONS is defined by the Microsoft ODBC headers but is
// missing from some unixODBC releases. The numeric ID is stable across the
// spec (10005), so provide a local fallback.
#ifndef SQL_ASYNC_STMT_FUNCTIONS
#define SQL_ASYNC_STMT_FUNCTIONS 10005
#endif

py::dict Connection::getAsyncCapability() const {
    if (!_dbcHandle) {
        ThrowStdException("Connection handle not allocated");
    }
    if (!SQLGetInfo_ptr || !SQLAllocHandle_ptr || !SQLSetStmtAttr_ptr ||
        !SQLFreeHandle_ptr) {
        LOG("getAsyncCapability: driver function pointers not initialized, loading driver");
        DriverLoader::getInstance().loadDriver();
    }

    SQLHDBC hDbc = _dbcHandle->get();
    py::dict result;

    // --- SQL_ASYNC_MODE (SQLUSMALLINT) -----------------------------------
    {
        SQLUSMALLINT asyncMode = 0;
        SQLSMALLINT outLen = 0;
        SQLRETURN ret =
            SQLGetInfo_ptr(hDbc, SQL_ASYNC_MODE, &asyncMode, sizeof(asyncMode), &outLen);
        if (SQL_SUCCEEDED(ret)) {
            result["async_mode"] = static_cast<unsigned int>(asyncMode);
            const char* name = "SQL_AM_NONE";
            if (asyncMode == SQL_AM_CONNECTION) {
                name = "SQL_AM_CONNECTION";
            } else if (asyncMode == SQL_AM_STATEMENT) {
                name = "SQL_AM_STATEMENT";
            }
            result["async_mode_name"] = std::string(name);
        } else {
            LOG("getAsyncCapability: SQLGetInfo(SQL_ASYNC_MODE) failed - "
                "SQLRETURN=%d",
                ret);
            result["async_mode"] = py::none();
            result["async_mode_name"] = py::none();
        }
    }

    // --- SQL_ASYNC_STMT_FUNCTIONS (SQLUINTEGER bitmask) ------------------
    // We return the raw bitmask value. Interpreting the bitmask across
    // drivers is unreliable, so the functional check below is the real
    // gate. This value is exposed for informational logging only.
    {
        SQLUINTEGER stmtFns = 0;
        SQLSMALLINT outLen = 0;
        SQLRETURN ret = SQLGetInfo_ptr(hDbc, SQL_ASYNC_STMT_FUNCTIONS, &stmtFns,
                                       sizeof(stmtFns), &outLen);
        if (SQL_SUCCEEDED(ret)) {
            result["async_stmt_functions_bitmask"] = static_cast<unsigned int>(stmtFns);
        } else {
            LOG("getAsyncCapability: SQLGetInfo(SQL_ASYNC_STMT_FUNCTIONS) not "
                "reported by driver - SQLRETURN=%d",
                ret);
            result["async_stmt_functions_bitmask"] = py::none();
        }
    }

    // --- SQL_ASYNC_DBC_FUNCTIONS (SQLUINTEGER bitmask, ODBC 3.8+) --------
    {
        SQLUINTEGER dbcFns = 0;
        SQLSMALLINT outLen = 0;
        SQLRETURN ret =
            SQLGetInfo_ptr(hDbc, SQL_ASYNC_DBC_FUNCTIONS, &dbcFns, sizeof(dbcFns), &outLen);
        if (SQL_SUCCEEDED(ret)) {
            result["async_dbc_functions_bitmask"] = static_cast<unsigned int>(dbcFns);
        } else {
            LOG("getAsyncCapability: SQLGetInfo(SQL_ASYNC_DBC_FUNCTIONS) not "
                "supported by driver - SQLRETURN=%d",
                ret);
            result["async_dbc_functions_bitmask"] = py::none();
        }
    }

    // --- SQL_ASYNC_NOTIFICATION (SQLUINTEGER, ODBC 3.8+) -----------------
    // Returns SQL_ASYNC_NOTIFICATION_CAPABLE (1) or SQL_ASYNC_NOTIFICATION_NOT_CAPABLE (0).
    // Tells us whether the driver supports the event-driven / callback
    // completion path (SQL_ATTR_ASYNC_STMT_EVENT / SQL_ATTR_ASYNC_STMT_PCALLBACK)
    // as an alternative to polling.
    {
        SQLUINTEGER notify = 0;
        SQLSMALLINT outLen = 0;
        SQLRETURN ret =
            SQLGetInfo_ptr(hDbc, SQL_ASYNC_NOTIFICATION, &notify, sizeof(notify), &outLen);
        if (SQL_SUCCEEDED(ret)) {
            result["async_notification"] = static_cast<unsigned int>(notify);
            result["async_notification_capable"] = (notify != 0);
        } else {
            LOG("getAsyncCapability: SQLGetInfo(SQL_ASYNC_NOTIFICATION) not "
                "reported by driver - SQLRETURN=%d",
                ret);
            result["async_notification"] = py::none();
            result["async_notification_capable"] = py::none();
        }
    }

    // --- Functional smoke test: run WAITFOR + SELECT under polling --------
    // This is the real gate. We allocate an HSTMT, enable statement-level
    // async, and drive a short server-side delay through the polling loop:
    //   1. SQLExecDirect returns SQL_STILL_EXECUTING while the server sleeps
    //   2. Re-invoke SQLExecDirect until it returns SQL_SUCCESS
    //   3. SQLFetch may also return SQL_STILL_EXECUTING; poll it too
    // A pass here proves polling works end-to-end on this OS + driver.
    // mssql-python loads msodbcsql directly via dlopen / LoadLibraryW (see
    // LoadDriverLibrary in ddbc_bindings.cpp), so this test is independent
    // of any ODBC Driver Manager (unixODBC / iODBC / Windows DM).
    {
        py::dict smoke;
        smoke["ran"] = false;

        if (!SQLExecDirect_ptr || !SQLFetch_ptr || !SQLFreeStmt_ptr) {
            smoke["error"] = std::string(
                "polling smoke test skipped: required driver function pointers not initialized");
            result["polling_smoke_test"] = smoke;
        } else {
            SQLHANDLE hStmt = nullptr;
            SQLRETURN ret = SQLAllocHandle_ptr(SQL_HANDLE_STMT, hDbc, &hStmt);
            if (!SQL_SUCCEEDED(ret) || hStmt == nullptr) {
                smoke["error"] = std::string("SQLAllocHandle(SQL_HANDLE_STMT) failed");
                smoke["sqlreturn"] = static_cast<int>(ret);
                result["polling_smoke_test"] = smoke;
            } else {
                ret = SQLSetStmtAttr_ptr(hStmt, SQL_ATTR_ASYNC_ENABLE,
                                         (SQLPOINTER)(uintptr_t)SQL_ASYNC_ENABLE_ON, 0);
                smoke["set_async_enable_sqlreturn"] = static_cast<int>(ret);
                if (!SQL_SUCCEEDED(ret)) {
                    smoke["error"] =
                        std::string("SQLSetStmtAttr(SQL_ATTR_ASYNC_ENABLE) failed");
                    SQLFreeHandle_ptr(SQL_HANDLE_STMT, hStmt);
                    result["polling_smoke_test"] = smoke;
                } else {
                    // 1-second server-side sleep guarantees the initial call
                    // returns SQL_STILL_EXECUTING at least once.
                    std::u16string queryStr =
                        u"WAITFOR DELAY '00:00:01'; SELECT 1 AS async_probe";
                    SQLWCHAR* queryPtr = reinterpretU16stringAsSqlWChar(queryStr);

                    // --- SQLExecDirect polling loop -----------------------
                    using Clock = std::chrono::steady_clock;
                    auto execStart = Clock::now();
                    unsigned int execPollCount = 0;
                    SQLRETURN execRet;
                    {
                        py::gil_scoped_release release;
                        execRet = SQLExecDirect_ptr(hStmt, queryPtr, SQL_NTS);
                        while (execRet == SQL_STILL_EXECUTING) {
                            ++execPollCount;
                            std::this_thread::sleep_for(std::chrono::milliseconds(10));
                            execRet = SQLExecDirect_ptr(hStmt, queryPtr, SQL_NTS);
                        }
                    }
                    auto execMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                                      Clock::now() - execStart)
                                      .count();
                    smoke["execute_sqlreturn"] = static_cast<int>(execRet);
                    smoke["execute_ok"] = SQL_SUCCEEDED(execRet);
                    smoke["execute_poll_count"] = execPollCount;
                    smoke["execute_elapsed_ms"] = static_cast<long long>(execMs);
                    // observed_still_executing is the critical evidence: if
                    // this is false, either the driver blocked internally or
                    // the query completed too quickly (unlikely with WAITFOR).
                    smoke["execute_observed_still_executing"] = (execPollCount > 0);

                    // --- SQLFetch polling loop ----------------------------
                    if (SQL_SUCCEEDED(execRet)) {
                        auto fetchStart = Clock::now();
                        unsigned int fetchPollCount = 0;
                        SQLRETURN fetchRet;
                        {
                            py::gil_scoped_release release;
                            fetchRet = SQLFetch_ptr(hStmt);
                            while (fetchRet == SQL_STILL_EXECUTING) {
                                ++fetchPollCount;
                                std::this_thread::sleep_for(std::chrono::milliseconds(5));
                                fetchRet = SQLFetch_ptr(hStmt);
                            }
                        }
                        auto fetchMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                                           Clock::now() - fetchStart)
                                           .count();
                        smoke["fetch_sqlreturn"] = static_cast<int>(fetchRet);
                        smoke["fetch_ok"] = SQL_SUCCEEDED(fetchRet);
                        smoke["fetch_poll_count"] = fetchPollCount;
                        smoke["fetch_elapsed_ms"] = static_cast<long long>(fetchMs);
                        smoke["fetch_observed_still_executing"] = (fetchPollCount > 0);
                    } else {
                        smoke["fetch_sqlreturn"] = py::none();
                        smoke["fetch_ok"] = false;
                        smoke["fetch_poll_count"] = py::none();
                        smoke["fetch_elapsed_ms"] = py::none();
                        smoke["fetch_observed_still_executing"] = py::none();
                    }

                    smoke["ran"] = true;

                    // Cleanup — close cursor before freeing to release
                    // any server-side resources cleanly.
                    SQLFreeStmt_ptr(hStmt, SQL_CLOSE);
                    SQLFreeHandle_ptr(SQL_HANDLE_STMT, hStmt);
                    result["polling_smoke_test"] = smoke;
                }
            }
        }
    }

    // --- Fetch-stream smoke test ------------------------------------------
    // The polling smoke test above uses a query that returns a single tiny
    // row — SQLFetch completes instantly from the TCP receive buffer with
    // zero polling, so it does NOT exercise async fetch. This second test
    // deliberately streams a multi-megabyte result set so that SQLFetch has
    // to wait on TDS packet arrivals from the network. That is where
    // SQL_STILL_EXECUTING on SQLFetch actually shows up in practice.
    //
    // Query is deliberately server-heavy: TOP 50000 rows from a cross-join
    // of sys.all_objects, with three moderately wide columns per row. On a
    // stock master DB this yields several MB and spans ~1000 TDS packets.
    {
        py::dict fetchStream;
        fetchStream["ran"] = false;

        if (!SQLExecDirect_ptr || !SQLFetch_ptr || !SQLFreeStmt_ptr) {
            fetchStream["error"] = std::string(
                "fetch-stream test skipped: required driver function pointers "
                "not initialized");
            result["polling_fetch_stream_test"] = fetchStream;
        } else {
            SQLHANDLE hStmt = nullptr;
            SQLRETURN ret = SQLAllocHandle_ptr(SQL_HANDLE_STMT, hDbc, &hStmt);
            if (!SQL_SUCCEEDED(ret) || hStmt == nullptr) {
                fetchStream["error"] =
                    std::string("SQLAllocHandle(SQL_HANDLE_STMT) failed");
                fetchStream["sqlreturn"] = static_cast<int>(ret);
                result["polling_fetch_stream_test"] = fetchStream;
            } else {
                ret = SQLSetStmtAttr_ptr(
                    hStmt, SQL_ATTR_ASYNC_ENABLE,
                    (SQLPOINTER)(uintptr_t)SQL_ASYNC_ENABLE_ON, 0);
                fetchStream["set_async_enable_sqlreturn"] = static_cast<int>(ret);
                if (!SQL_SUCCEEDED(ret)) {
                    fetchStream["error"] =
                        std::string("SQLSetStmtAttr(SQL_ATTR_ASYNC_ENABLE) failed");
                    SQLFreeHandle_ptr(SQL_HANDLE_STMT, hStmt);
                    result["polling_fetch_stream_test"] = fetchStream;
                } else {
                    // ~50000 rows × 3 columns from a cross-join. Uses only
                    // system catalog views so it runs on any SQL Server DB.
                    std::u16string queryStr = u"SELECT TOP 50000 "
                        u"CAST(a.object_id AS BIGINT) AS id, "
                        u"CAST(a.name AS NVARCHAR(128)) AS n, "
                        u"CAST(a.create_date AS DATETIME2) AS cd "
                        u"FROM sys.all_objects a CROSS JOIN sys.all_objects b";
                    SQLWCHAR* queryPtr = reinterpretU16stringAsSqlWChar(queryStr);

                    // Poll the execute first — this may or may not observe
                    // STILL_EXECUTING depending on server plan-cache state.
                    using Clock = std::chrono::steady_clock;
                    auto execStart = Clock::now();
                    unsigned int execPollCount = 0;
                    SQLRETURN execRet;
                    {
                        py::gil_scoped_release release;
                        execRet = SQLExecDirect_ptr(hStmt, queryPtr, SQL_NTS);
                        while (execRet == SQL_STILL_EXECUTING) {
                            ++execPollCount;
                            std::this_thread::sleep_for(std::chrono::milliseconds(10));
                            execRet = SQLExecDirect_ptr(hStmt, queryPtr, SQL_NTS);
                        }
                    }
                    auto execMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                                      Clock::now() - execStart)
                                      .count();
                    fetchStream["execute_sqlreturn"] = static_cast<int>(execRet);
                    fetchStream["execute_ok"] = SQL_SUCCEEDED(execRet);
                    fetchStream["execute_poll_count"] = execPollCount;
                    fetchStream["execute_elapsed_ms"] =
                        static_cast<long long>(execMs);

                    if (SQL_SUCCEEDED(execRet)) {
                        // Stream all rows. For each SQLFetch call, count how
                        // many STILL_EXECUTING returns we saw before it
                        // completed. Rows are not bound/materialized — we're
                        // only measuring whether the fetch phase EVER blocks
                        // on the network and how often.
                        auto fetchStart = Clock::now();
                        unsigned int rowsRead = 0;
                        unsigned int totalPollCount = 0;
                        unsigned int rowsThatPolled = 0;
                        SQLRETURN fetchRet = SQL_SUCCESS;
                        {
                            py::gil_scoped_release release;
                            while (true) {
                                unsigned int rowPolls = 0;
                                fetchRet = SQLFetch_ptr(hStmt);
                                while (fetchRet == SQL_STILL_EXECUTING) {
                                    ++rowPolls;
                                    // 500us — tight enough to catch short
                                    // network waits without spinning.
                                    std::this_thread::sleep_for(
                                        std::chrono::microseconds(500));
                                    fetchRet = SQLFetch_ptr(hStmt);
                                }
                                if (fetchRet == SQL_NO_DATA) {
                                    break;
                                }
                                if (!SQL_SUCCEEDED(fetchRet)) {
                                    break;
                                }
                                ++rowsRead;
                                totalPollCount += rowPolls;
                                if (rowPolls > 0) {
                                    ++rowsThatPolled;
                                }
                            }
                        }
                        auto fetchMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                                           Clock::now() - fetchStart)
                                           .count();
                        fetchStream["fetch_final_sqlreturn"] =
                            static_cast<int>(fetchRet);
                        fetchStream["fetch_ok"] =
                            (fetchRet == SQL_NO_DATA) ||
                            SQL_SUCCEEDED(fetchRet);
                        fetchStream["fetch_rows_read"] = rowsRead;
                        fetchStream["fetch_total_poll_count"] = totalPollCount;
                        fetchStream["fetch_rows_that_polled"] = rowsThatPolled;
                        fetchStream["fetch_elapsed_ms"] =
                            static_cast<long long>(fetchMs);
                        fetchStream["fetch_observed_still_executing"] =
                            (totalPollCount > 0);
                    } else {
                        fetchStream["fetch_final_sqlreturn"] = py::none();
                        fetchStream["fetch_ok"] = false;
                        fetchStream["fetch_rows_read"] = py::none();
                        fetchStream["fetch_total_poll_count"] = py::none();
                        fetchStream["fetch_rows_that_polled"] = py::none();
                        fetchStream["fetch_elapsed_ms"] = py::none();
                        fetchStream["fetch_observed_still_executing"] = py::none();
                    }

                    fetchStream["ran"] = true;

                    SQLFreeStmt_ptr(hStmt, SQL_CLOSE);
                    SQLFreeHandle_ptr(SQL_HANDLE_STMT, hStmt);
                    result["polling_fetch_stream_test"] = fetchStream;
                }
            }
        }
    }

    return result;
}

py::dict ConnectionHandle::getAsyncCapability() const {
    if (!_conn) {
        ThrowStdException("Connection object is not initialized");
    }
    return _conn->getAsyncCapability();
}

void ConnectionHandle::setAttr(int attribute, py::object value) {
    if (!_conn) {
        ThrowStdException("Connection not established");
    }

    // Use existing setAttribute with better error handling
    SQLRETURN ret = _conn->setAttribute(static_cast<SQLINTEGER>(attribute), value);
    if (!SQL_SUCCEEDED(ret)) {
        // Get detailed error information from ODBC
        try {
            ErrorInfo errorInfo = SQLCheckError_Wrap(SQL_HANDLE_DBC, _conn->getDbcHandle(), ret);

            std::string errorMsg =
                "Failed to set connection attribute " + std::to_string(attribute);
            if (!errorInfo.ddbcErrorMsg.empty()) {
                errorMsg += ": " + errorInfo.ddbcErrorMsg;
            }

            LOG("Connection setAttribute failed: %s", errorMsg.c_str());
            ThrowStdException(errorMsg);
        } catch (...) {
            // Fallback to generic error if detailed error retrieval fails
            std::string errorMsg =
                "Failed to set connection attribute " + std::to_string(attribute);
            LOG("Connection setAttribute failed: %s", errorMsg.c_str());
            ThrowStdException(errorMsg);
        }
    }
}
