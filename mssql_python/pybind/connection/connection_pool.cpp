// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#include "connection/connection_pool.h"
#include <exception>
#include <memory>
#include <vector>

// Logging uses LOG() macro for all diagnostic output
#include "logger_bridge.hpp"

ConnectionPool::ConnectionPool(size_t max_size, int idle_timeout_secs)
    : _max_size(max_size), _idle_timeout_secs(idle_timeout_secs), _current_size(0) {}

std::shared_ptr<Connection> ConnectionPool::acquire(const std::wstring& connStr,
                                                    const py::dict& attrs_before) {
    std::vector<std::shared_ptr<Connection>> to_disconnect;
    std::shared_ptr<Connection> valid_conn = nullptr;
    bool needs_connect = false;
    {
        std::lock_guard<std::mutex> lock(_mutex);
        auto now = std::chrono::steady_clock::now();
        size_t before = _pool.size();

        // Phase 1: Remove stale connections, collect for later disconnect
        _pool.erase(std::remove_if(_pool.begin(), _pool.end(),
                                   [&](const std::shared_ptr<Connection>& conn) {
                                       auto idle_time =
                                           std::chrono::duration_cast<std::chrono::seconds>(
                                               now - conn->lastUsed())
                                               .count();
                                       if (idle_time > _idle_timeout_secs) {
                                           to_disconnect.push_back(conn);
                                           return true;
                                       }
                                       return false;
                                   }),
                    _pool.end());

        size_t pruned = before - _pool.size();
        _current_size = (_current_size >= pruned) ? (_current_size - pruned) : 0;

        // Phase 2: Attempt to reuse healthy connections
        while (!_pool.empty()) {
            auto conn = _pool.front();
            _pool.pop_front();
            if (conn->isAlive()) {
                if (!conn->reset()) {
                    to_disconnect.push_back(conn);
                    --_current_size;
                    continue;
                }
                valid_conn = conn;
                break;
            } else {
                to_disconnect.push_back(conn);
                --_current_size;
            }
        }

        // Reserve a slot for a new connection if none reusable.
        // The actual connect() call happens outside the mutex to avoid
        // holding the mutex during the blocking ODBC call (which releases
        // the GIL and could otherwise cause a mutex/GIL deadlock).
        if (!valid_conn && _current_size < _max_size) {
            valid_conn = std::make_shared<Connection>(connStr, true);
            ++_current_size;
            needs_connect = true;
        } else if (!valid_conn) {
            throw std::runtime_error("ConnectionPool::acquire: pool size limit reached");
        }
    }

    // Phase 2.5: Connect the new connection outside the mutex.
    if (needs_connect) {
        try {
            valid_conn->connect(attrs_before);
        } catch (...) {
            // Connect failed — release the reserved slot
            {
                std::lock_guard<std::mutex> lock(_mutex);
                if (_current_size > 0) --_current_size;
            }
            throw;
        }
    }

    // Phase 3: Disconnect expired/bad connections outside lock
    for (auto& conn : to_disconnect) {
        try {
            conn->disconnect();
        } catch (const std::exception& ex) {
            LOG("Disconnect bad/expired connections failed: %s", ex.what());
        }
    }
    return valid_conn;
}

void ConnectionPool::release(std::shared_ptr<Connection> conn) {
    bool should_disconnect = false;
    {
        std::lock_guard<std::mutex> lock(_mutex);
        if (_pool.size() < _max_size) {
            conn->updateLastUsed();
            _pool.push_back(conn);
        } else {
            should_disconnect = true;
        }
    }
    // Disconnect outside the mutex to avoid holding it during the
    // blocking ODBC call (which releases the GIL).
    if (should_disconnect) {
        try {
            conn->disconnect();
        } catch (const std::exception& ex) {
            LOG("ConnectionPool::release: disconnect failed: %s", ex.what());
        }
        std::lock_guard<std::mutex> lock(_mutex);
        if (_current_size > 0)
            --_current_size;
    }
}

void ConnectionPool::close() {
    std::vector<std::shared_ptr<Connection>> to_close;
    {
        std::lock_guard<std::mutex> lock(_mutex);
        while (!_pool.empty()) {
            to_close.push_back(_pool.front());
            _pool.pop_front();
        }
        _current_size = 0;
    }
    for (auto& conn : to_close) {
        try {
            conn->disconnect();
        } catch (const std::exception& ex) {
            LOG("ConnectionPool::close: disconnect failed: %s", ex.what());
        }
    }
}

ConnectionPoolManager& ConnectionPoolManager::getInstance() {
    static ConnectionPoolManager manager;
    return manager;
}

std::shared_ptr<Connection> ConnectionPoolManager::acquireConnection(const std::wstring& connStr,
                                                                     const py::dict& attrs_before) {
    std::shared_ptr<ConnectionPool> pool;
    {
        std::lock_guard<std::mutex> lock(_manager_mutex);
        auto& pool_ref = _pools[connStr];
        if (!pool_ref) {
            LOG("Creating new connection pool");
            pool_ref = std::make_shared<ConnectionPool>(_default_max_size, _default_idle_secs);
        }
        pool = pool_ref;
    }
    // Call acquire() outside _manager_mutex.  acquire() may release the GIL
    // during the ODBC connect call; holding _manager_mutex across that would
    // create a mutex/GIL lock-ordering deadlock.
    return pool->acquire(connStr, attrs_before);
}

void ConnectionPoolManager::returnConnection(const std::wstring& conn_str,
                                             const std::shared_ptr<Connection> conn) {
    std::shared_ptr<ConnectionPool> pool;
    {
        std::lock_guard<std::mutex> lock(_manager_mutex);
        auto it = _pools.find(conn_str);
        if (it != _pools.end()) {
            pool = it->second;
        }
    }
    // Call release() outside _manager_mutex to avoid deadlock.
    if (pool) {
        pool->release(conn);
    }
}

void ConnectionPoolManager::configure(int max_size, int idle_timeout_secs) {
    std::lock_guard<std::mutex> lock(_manager_mutex);
    _default_max_size = max_size;
    _default_idle_secs = idle_timeout_secs;
}

void ConnectionPoolManager::closePools() {
    std::lock_guard<std::mutex> lock(_manager_mutex);
    // Keep _manager_mutex held for the full close operation so that
    // acquireConnection()/returnConnection() cannot create or use pools
    // while closePools() is in progress.
    for (auto& [conn_str, pool] : _pools) {
        if (pool) {
            pool->close();
        }
    }
    _pools.clear();
}
