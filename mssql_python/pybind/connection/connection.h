// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma once
#include "../ddbc_bindings.h"
#include <memory>
#include <string>
#include <mutex>
#include <unordered_map>

// Custom msodbcsql (SQL Server ODBC) connection-attribute id carrying the Entra
// access-token struct. It is consumed once at login. Shared single source of
// truth: connection.cpp applies it, and expiry-aware pooled checkout in
// connection_pool.cpp uses it to compare a freshly minted token against the one
// a pooled connection already holds. constexpr at namespace scope has internal
// linkage, so each translation unit gets its own copy (no ODR issue).
constexpr long SQL_COPT_SS_ACCESS_TOKEN = 1256;

// Represents a single ODBC database connection.
// Manages connection handles.
// Note: This class does NOT implement pooling logic directly.
//
// THREADING MODEL (per DB-API 2.0 threadsafety=1):
// - Connections should NOT be shared between threads in normal usage
// - However, _childStatementHandles is mutex-protected because:
//   1. Python GC/finalizers can run from any thread
//   2. Native code may release GIL during blocking ODBC calls
//   3. Provides safety if user accidentally shares connection
// - All accesses to _childStatementHandles are guarded by _childHandlesMutex

class Connection {
  public:
    Connection(const std::u16string& connStr, bool fromPool);

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

    // Materialize connect-attrs from a Python token-factory callback.
    // The factory may return either a bare attrs dict (legacy) or a
    // ``(attrs, expires_on)`` tuple (expiry-aware pooling),
    // where expires_on is the token's POSIX-epoch expiry or None. Returns the
    // attrs dict; outExpiryEpoch is set to the expiry, or 0 when unknown.
    // GIL must be held by the caller (it invokes Python).
    static py::dict invokeTokenFactory(const py::object& tokenFactory,
                                       long long& outExpiryEpoch);

    // Record / inspect the pooled access token's expiry so a near-expiry
    // connection is refreshed on checkout instead of being handed out with a
    // token about to expire. Epoch seconds; 0 = unknown
    // (non-token auth), for which isTokenNearExpiry() always returns false.
    void setTokenExpiry(long long epochSeconds);
    bool isTokenNearExpiry(int thresholdSecs) const;

    // Returns the raw SQL_COPT_SS_ACCESS_TOKEN bytes this connection last
    // authenticated with, or an empty string if it never used a token. Used by
    // expiry-aware checkout to decide whether a freshly minted token differs
    // from the one the pooled connection already holds: if unchanged, the
    // healthy connection is reused; only a rotated token forces reopen.
    std::string currentAccessToken() const;

    // True if this connection's last access token equals `token`, compared in
    // place against the internal buffer (no copy). Used by the sibling-drain
    // sweep so rotating one token does not copy every pooled connection's token
    // per comparison. Matches currentAccessToken() == token semantics (a
    // connection with no token matches only an empty comparand).
    bool accessTokenEquals(const std::string& token) const;

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

    std::u16string _connStr;
    bool _fromPool = false;
    bool _autocommit = true;
    SqlHandlePtr _dbcHandle;
    std::chrono::steady_clock::time_point _lastUsed;
    // POSIX-epoch expiry (seconds) of the access token this connection last
    // authenticated with. 0 means unknown / non-token auth, in which case the
    // expiry-aware checkout logic never treats the connection as near-expiry.
    long long _tokenExpiryEpoch = 0;
    // Per-attribute owned buffers for connect attributes whose pointer the
    // driver may dereference *after* SQLSetConnectAttr returns (deferred
    // attributes, e.g. SQL_COPT_SS_ACCESS_TOKEN). Keyed by attribute ID so
    // that setting a second deferred attribute does not invalidate the
    // pointer the driver stashed for the first. See issue #594.
    std::unordered_map<SQLINTEGER, std::u16string> _attrStringBuffers;
    std::unordered_map<SQLINTEGER, std::string>    _attrBytesBuffers;

    // Track child statement handles to mark them as implicitly freed when connection closes
    // Uses weak_ptr to avoid circular references and allow normal cleanup
    // THREAD-SAFETY: All accesses must be guarded by _childHandlesMutex
    std::vector<std::weak_ptr<SqlHandle>> _childStatementHandles;
    
    // Counter for periodic compaction of expired weak_ptrs
    // Compact every N allocations to avoid O(n²) overhead in hot path
    // THREAD-SAFETY: Protected by _childHandlesMutex
    size_t _allocationsSinceCompaction = 0;
    static constexpr size_t COMPACTION_INTERVAL = 100;
    
    // Mutex protecting _childStatementHandles and _allocationsSinceCompaction
    // Prevents data races between allocStatementHandle() and disconnect(),
    // or concurrent GC finalizers running from different threads
    mutable std::mutex _childHandlesMutex;
};

class ConnectionHandle {
  public:
    ConnectionHandle(const std::u16string& connStr, bool usePool,
                     const py::dict& attrsBefore = py::dict(),
                     const std::u16string& poolKey = std::u16string(),
                     const py::object& tokenFactory = py::object());
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
    std::u16string _connStr;
    // Key under which this connection's pool is stored. Defaults to _connStr
    // (legacy behavior) but is set to an identity-aware composite key for
    // Entra access-token auth so distinct identities never share a pool.
    // Empty is never stored; the ctor falls back to _connStr.
    std::u16string _poolKey;
};
