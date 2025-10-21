// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

// INFO|TODO - Note that is file is Windows specific right now. Making it
//             arch agnostic will be taken up in future.

#ifndef MSSQL_PYTHON_CONNECTION_POOL_H_
#define MSSQL_PYTHON_CONNECTION_POOL_H_

#pragma once
#include <chrono>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include "connection/connection.h"

// Manages a fixed-size pool of reusable database connections for a
// single connection string
class ConnectionPool {
 public:
    ConnectionPool(size_t max_size, int idle_timeout_secs);

    // Acquires a connection from the pool or creates a new one if under limit
    std::shared_ptr<Connection> acquire(
        const std::wstring& connStr,
        const py::dict& attrs_before = py::dict());

    // Returns a connection to the pool for reuse
    void release(std::shared_ptr<Connection> conn);

    // Closes all connections in the pool, releasing resources
    void close();

 private:
    size_t _max_size;  // Maximum number of connections allowed
    int _idle_timeout_secs;  // Idle time before connections are stale
    size_t _current_size = 0;
    std::deque<std::shared_ptr<Connection>> _pool;  // Available connections
    std::mutex _mutex;  // Mutex for thread-safe access
};

// Singleton manager that handles multiple pools keyed by connection string
class ConnectionPoolManager {
 public:
    // Returns the singleton instance of the manager
    static ConnectionPoolManager& getInstance();

    void configure(int max_size, int idle_timeout);

    // Gets a connection from the appropriate pool (creates one if none exists)
    std::shared_ptr<Connection> acquireConnection(
        const std::wstring& conn_str,
        const py::dict& attrs_before = py::dict());

    // Returns a connection to its original pool
    void returnConnection(const std::wstring& conn_str,
                          std::shared_ptr<Connection> conn);

    // Closes all pools and their connections
    void closePools();

 private:
    ConnectionPoolManager() = default;
    ~ConnectionPoolManager() = default;

    // Map from connection string to connection pool
    std::unordered_map<std::wstring, std::shared_ptr<ConnectionPool>> _pools;

    // Protects access to the _pools map
    std::mutex _manager_mutex;
    size_t _default_max_size = 10;
    int _default_idle_secs = 300;

    // Prevent copying
    ConnectionPoolManager(const ConnectionPoolManager&) = delete;
    ConnectionPoolManager& operator=(const ConnectionPoolManager&) = delete;
};

#endif  // MSSQL_PYTHON_CONNECTION_POOL_H_
