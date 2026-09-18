// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#ifndef MSSQL_PYTHON_CONNECTION_POOL_H_
#define MSSQL_PYTHON_CONNECTION_POOL_H_

#pragma once
#include "connection/connection.h"
#include <atomic>
#include <chrono>
#include <deque>
#include <functional>
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

    // Test accessors for pool generation, checked-out count, and current size
    size_t current_size() const {
        std::lock_guard<std::mutex> lock(const_cast<std::mutex&>(_mutex));
        return _current_size;
    }
    size_t checked_out() const {
        std::lock_guard<std::mutex> lock(const_cast<std::mutex&>(_mutex));
        return _checked_out;
    }
    size_t in_flight() const {
        std::lock_guard<std::mutex> lock(const_cast<std::mutex&>(_mutex));
        return _in_flight;
    }
    uint64_t generation() const {
        std::lock_guard<std::mutex> lock(const_cast<std::mutex&>(_mutex));
        return _generation;
    }
    uint64_t pool_id() const {
        return _pool_id;
    }

    // Test helper to inject a candidate connection for race testing
    void inject_candidate(std::shared_ptr<Connection> conn) {
        std::lock_guard<std::mutex> lock(_mutex);
        _pool.push_back(conn);
        ++_current_size;
    }

    // Test hooks for deterministic race testing (#746)
    void set_mock_mode(bool enable) {
        _mock_mode = enable;
    }
    bool mock_mode() const {
        return _mock_mode;
    }
    void set_on_disconnect_hook(std::function<void()> hook) {
        std::lock_guard<std::mutex> lock(_mutex);
        _on_disconnect_hook = hook;
    }

  private:
    size_t _max_size;        // Maximum number of connections allowed
    int _idle_timeout_secs;  // Idle time before connections are stale
    size_t _current_size = 0;
    size_t _checked_out = 0;   // Live connections currently checked out by callers (#746)
    size_t _in_flight = 0;     // Connects or validations currently in flight (#746)
    uint64_t _generation = 0;  // Pool reset generation for reservation attribution (#746)
    uint64_t _pool_id = 0;     // Monotonic process-wide pool ID to avoid ABA reuse (#746)
    std::atomic<bool> _mock_mode{false};
    std::function<void()> _on_disconnect_hook;
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
    //
    // Returns nullptr when pooling is not currently accepting new pools (i.e. a
    // concurrent disable won the race). The enabled-check and the pool creation
    // share _manager_mutex, so this decision is atomic with respect to
    // closePools(): the caller must fall back to a non-pooled connection.
    std::shared_ptr<Connection> acquireConnection(
        const std::u16string& conn_str, const py::dict& attrs_before = py::dict(),
        const std::u16string& pool_key = std::u16string(),
        const py::object& token_factory = py::object());

    // Arms (true) or disarms (false) new-pool creation. Disarming, done under
    // _manager_mutex, guarantees that any acquireConnection() serialized after
    // it declines to create a pool, closing the disable()-vs-connect() race.
    void setAccepting(bool accepting);

    // Returns a connection to its original pool, identified by pool_key
    // (the same key passed to acquireConnection).
    void returnConnection(const std::u16string& pool_key, std::shared_ptr<Connection> conn);

    // Closes all pools and their connections
    void closePools();

    // Test hooks for mock mode
    void set_mock_mode(bool enable) {
        std::lock_guard<std::mutex> lock(_manager_mutex);
        _mock_mode = enable;
        for (auto& [_, pool] : _pools) {
            if (pool) {
                pool->set_mock_mode(enable);
            }
        }
    }
    bool mock_mode() const {
        std::lock_guard<std::mutex> lock(const_cast<std::mutex&>(_manager_mutex));
        return _mock_mode;
    }

  private:
    ConnectionPoolManager() = default;
    ~ConnectionPoolManager() = default;

    // Map from connection string to connection pool
    std::unordered_map<std::u16string, std::shared_ptr<ConnectionPool>> _pools;

    // Protects access to the _pools map
    std::mutex _manager_mutex;
    size_t _default_max_size = 10;
    int _default_idle_secs = 300;

    // When false, acquireConnection() refuses to create/hand out pools so a
    // connect racing a disable() cannot resurrect a pool after closePools()
    // cleared the map. Read and written only under _manager_mutex. Defaults to
    // true so direct native pool use (and auto-enable) works without an
    // explicit enable_pooling() call; only disable_pooling() disarms it, and
    // enable_pooling() re-arms it.
    bool _accepting = true;
    bool _mock_mode = false;

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
