// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

// INFO|TODO - Note that is file is Windows specific right now. Making it arch
// agnostic will be taken up in future.

#include "connection/connection_pool.h"

#include <exception>
#include <vector>
#include <memory>

ConnectionPool::ConnectionPool(size_t max_size, int idle_timeout_secs)
    : _max_size(max_size),
      _idle_timeout_secs(idle_timeout_secs),
      _current_size(0) {}

std::shared_ptr<Connection> ConnectionPool::acquire(
    const std::wstring& connStr,
    const py::dict& attrs_before) {
    std::vector<std::shared_ptr<Connection>> to_disconnect;
    std::shared_ptr<Connection> valid_conn = nullptr;
    {
        std::lock_guard<std::mutex> lock(_mutex);
        auto now = std::chrono::steady_clock::now();
        size_t before = _pool.size();

        // Phase 1: Remove stale connections, collect for later disconnect
        _pool.erase(
            std::remove_if(
                _pool.begin(), _pool.end(),
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
        _current_size = (_current_size >= pruned)
                    ? (_current_size - pruned)
                    : 0;

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

        // Create new connection if none reusable
        if (!valid_conn && _current_size < _max_size) {
            valid_conn = std::make_shared<Connection>(connStr, true);
            valid_conn->connect(attrs_before);
            ++_current_size;
        } else if (!valid_conn) {
            throw std::runtime_error(
                "ConnectionPool::acquire: pool size limit reached");
        }
    }

    // Phase 3: Disconnect expired/bad connections outside lock
    for (auto& conn : to_disconnect) {
        try {
            conn->disconnect();
        } catch (const std::exception& ex) {
            LOG("Disconnect bad/expired connections failed: {}", ex.what());
        }
    }
    return valid_conn;
}

void ConnectionPool::release(std::shared_ptr<Connection> conn) {
    std::lock_guard<std::mutex> lock(_mutex);
    if (_pool.size() < _max_size) {
        conn->updateLastUsed();
        _pool.push_back(conn);
    } else {
        conn->disconnect();
        if (_current_size > 0) --_current_size;
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
            LOG("ConnectionPool::close: disconnect failed: {}", ex.what());
        }
    }
}

ConnectionPoolManager& ConnectionPoolManager::getInstance() {
    static ConnectionPoolManager manager;
    return manager;
}

std::shared_ptr<Connection> ConnectionPoolManager::acquireConnection(
    const std::wstring& connStr,
    const py::dict& attrs_before) {
    std::lock_guard<std::mutex> lock(_manager_mutex);

    auto& pool = _pools[connStr];
    if (!pool) {
        LOG("Creating new connection pool");
        pool = std::make_shared<ConnectionPool>(
            _default_max_size, _default_idle_secs);
    }
    return pool->acquire(connStr, attrs_before);
}

void ConnectionPoolManager::returnConnection(
    const std::wstring& conn_str,
    const std::shared_ptr<Connection> conn) {
    std::lock_guard<std::mutex> lock(_manager_mutex);
    if (_pools.find(conn_str) != _pools.end()) {
        _pools[conn_str]->release((conn));
    }
}

void ConnectionPoolManager::configure(int max_size, int idle_timeout_secs) {
    std::lock_guard<std::mutex> lock(_manager_mutex);
    _default_max_size = max_size;
    _default_idle_secs = idle_timeout_secs;
}

void ConnectionPoolManager::closePools() {
    std::lock_guard<std::mutex> lock(_manager_mutex);
    for (auto& [conn_str, pool] : _pools) {
        if (pool) {
            pool->close();
        }
    }
    _pools.clear();
}
