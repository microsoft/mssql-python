// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#ifndef MSSQL_PYTHON_CONNECTION_POOL_H_
#define MSSQL_PYTHON_CONNECTION_POOL_H_

#pragma once
#include "connection/connection.h"
#include <chrono>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

// Manages a fixed-size pool of reusable database connections for a
// single connection string
class ConnectionPool {
  public:
    ConnectionPool(size_t max_size, int idle_timeout_secs);

    // Acquires a connection from the pool or creates a new one if under limit.
    // token_factory, when set, is a Python callable returning the connect-attrs
    // (including the access token) to connect with; it is invoked *only* when a
    // new physical connection is opened, so a pool hit skips it entirely.
    // (Internal callback — unrelated to the public token_provider= API.)
    // When token_factory is set, a pooled candidate whose access token is within
    // the expiry threshold is discarded and reopened so a caller never receives
    // a connection with an about-to-expire token.
    std::shared_ptr<Connection> acquire(const std::u16string& connStr,
                                        const py::dict& attrs_before = py::dict(),
                                        const py::object& token_factory = py::object());

    // Returns a connection to the pool for reuse
    void release(std::shared_ptr<Connection> conn);

    // Closes all connections in the pool, releasing resources
    void close();

    // True when the pool holds no live or in-flight connections and can be
    // dropped by the manager to reclaim memory (lazy eviction).
    bool canEvict();

  private:
    size_t _max_size;        // Maximum number of connections allowed
    int _idle_timeout_secs;  // Idle time before connections are stale
    size_t _current_size = 0;
    std::deque<std::shared_ptr<Connection>> _pool;  // Available connections
    std::mutex _mutex;                              // Mutex for thread-safe access
};

// Singleton manager that handles multiple pools keyed by connection string
class ConnectionPoolManager {
  public:
    // Returns the singleton instance of the manager
    static ConnectionPoolManager& getInstance();

    void configure(int max_size, int idle_timeout);

    // Gets a connection from the appropriate pool (creates one if none exists).
    // The pool is keyed by pool_key when supplied, else by conn_str. conn_str
    // is always used to establish new physical connections. Keying separately
    // keeps distinct Entra identities in distinct pools.
    // token_factory is forwarded to ConnectionPool::acquire for lazy token
    // acquisition on a pool miss.
    std::shared_ptr<Connection> acquireConnection(
        const std::u16string& conn_str, const py::dict& attrs_before = py::dict(),
        const std::u16string& pool_key = std::u16string(),
        const py::object& token_factory = py::object());

    // Returns a connection to its original pool, identified by pool_key
    // (the same key passed to acquireConnection).
    void returnConnection(const std::u16string& pool_key, std::shared_ptr<Connection> conn);

    // Closes all pools and their connections
    void closePools();

    // Diagnostic: number of pools currently tracked by the manager. Used by
    // tests to observe lazy eviction of idle identity pools.
    size_t poolCount();

  private:
    ConnectionPoolManager() = default;
    ~ConnectionPoolManager() = default;

    // Map from connection string to connection pool
    std::unordered_map<std::u16string, std::shared_ptr<ConnectionPool>> _pools;

    // Protects access to the _pools map
    std::mutex _manager_mutex;
    size_t _default_max_size = 10;
    int _default_idle_secs = 300;

    // Throttle for the lazy-eviction sweep in acquireConnection(). The sweep
    // iterates every pool (and every idle connection within each) under
    // _manager_mutex, so running it on literally every connect is an O(pools ×
    // conns) contention hotspot for many-identity workloads. A pool cannot
    // become evictable faster than the idle timeout, so sweeping more often
    // than that buys nothing; we run it at most once per idle-timeout window.
    std::chrono::steady_clock::time_point _last_sweep{};

    // Prevent copying
    ConnectionPoolManager(const ConnectionPoolManager&) = delete;
    ConnectionPoolManager& operator=(const ConnectionPoolManager&) = delete;
};

#endif  // MSSQL_PYTHON_CONNECTION_POOL_H_
