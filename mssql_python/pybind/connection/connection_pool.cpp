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

std::shared_ptr<Connection> ConnectionPool::acquire(const std::u16string& connStr,
                                                    const py::dict& attrs_before) {
    std::vector<std::shared_ptr<Connection>> to_disconnect;
    std::shared_ptr<Connection> valid_conn = nullptr;
    bool needs_connect = false;

    // Phase 1: Prune stale connections (under mutex — no ODBC calls).
    {
        std::lock_guard<std::mutex> lock(_mutex);
        auto now = std::chrono::steady_clock::now();
        size_t before = _pool.size();

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
        // Decrement _current_size eagerly so new slots can be reserved while
        // stale connections are being disconnected (Phase 4).  This means
        // _current_size tracks *reserved capacity* (pooled + checked-out +
        // in-flight new), not necessarily live ODBC handles.
        _current_size = (_current_size >= pruned) ? (_current_size - pruned) : 0;
    }

    // Phase 2: Pop one candidate at a time and validate it outside the
    // mutex.  isAlive() and reset() perform ODBC calls that release the
    // GIL; calling them while holding the mutex would create a mutex/GIL
    // lock-ordering deadlock when multiple threads acquire concurrently.
    while (true) {
        std::shared_ptr<Connection> candidate;
        {
            std::lock_guard<std::mutex> lock(_mutex);
            if (_pool.empty()) {
                // No more candidates — try to reserve a slot for a new connection.
                if (_current_size < _max_size) {
                    // Reserve the slot here but construct the Connection outside
                    // _mutex (Phase 3): the Connection constructor allocates ODBC
                    // handles and emits log records that acquire the GIL, and
                    // holding _mutex across a GIL acquisition deadlocks a thread
                    // that holds the GIL and is waiting on _mutex (#671).
                    ++_current_size;
                    needs_connect = true;
                } else {
                    // NOTE: Another thread may be validating a popped candidate
                    // outside the mutex right now.  If that candidate fails, a
                    // slot will open up — but we can't wait for it here without
                    // adding a condition-variable retry loop.  This is an
                    // acceptable trade-off: transient "pool full" errors under
                    // heavy contention are rare and callers can retry.
                    throw std::runtime_error("ConnectionPool::acquire: pool size limit reached");
                }
                break;
            }
            candidate = _pool.front();
            _pool.pop_front();
        }

        // Validate the candidate outside the mutex.
        try {
            if (candidate->isAlive() && candidate->reset()) {
                valid_conn = candidate;
                break;
            }
        } catch (const std::exception& ex) {
            LOG("Candidate connection validation failed: %s", ex.what());
        }

        // Candidate is dead or reset failed — mark for disconnect and
        // decrement the pool size.
        to_disconnect.push_back(candidate);
        {
            std::lock_guard<std::mutex> lock(_mutex);
            if (_current_size > 0) --_current_size;
        }
    }

    // Phase 3: Construct and connect the new connection outside the mutex.
    if (needs_connect) {
        try {
            valid_conn = std::make_shared<Connection>(connStr, true);
            valid_conn->connect(attrs_before);
        } catch (...) {
            // Construct/connect failed — release the reserved slot
            {
                std::lock_guard<std::mutex> lock(_mutex);
                if (_current_size > 0) --_current_size;
            }
            throw;
        }
    }

    // Phase 4: Disconnect expired/bad connections outside lock.
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

std::shared_ptr<Connection> ConnectionPoolManager::acquireConnection(const std::u16string& connStr,
                                                                     const py::dict& attrs_before) {
    std::shared_ptr<ConnectionPool> pool;
    bool created = false;
    {
        std::lock_guard<std::mutex> lock(_manager_mutex);
        auto& pool_ref = _pools[connStr];
        if (!pool_ref) {
            pool_ref = std::make_shared<ConnectionPool>(_default_max_size, _default_idle_secs);
            created = true;
        }
        pool = pool_ref;
    }
    // Log after releasing _manager_mutex (#671): LOG() acquires the GIL, and
    // holding a native mutex across a GIL acquisition deadlocks a thread that
    // holds the GIL and is waiting on the same mutex.
    if (created) {
        LOG("Creating new connection pool");
    }
    // Call acquire() outside _manager_mutex.  acquire() may release the GIL
    // during the ODBC connect call; holding _manager_mutex across that would
    // create a mutex/GIL lock-ordering deadlock.
    return pool->acquire(connStr, attrs_before);
}

void ConnectionPoolManager::returnConnection(const std::u16string& conn_str,
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
    std::vector<std::shared_ptr<ConnectionPool>> pools_to_close;
    {
        std::lock_guard<std::mutex> lock(_manager_mutex);
        // Detach the pools under the lock so acquireConnection()/returnConnection()
        // immediately start fresh and cannot use the pools being closed.
        for (auto& [conn_str, pool] : _pools) {
            if (pool) {
                pools_to_close.push_back(pool);
            }
        }
        _pools.clear();
    }
    // Close outside _manager_mutex (#671): pool->close() disconnects connections,
    // which emit log records (acquiring the GIL) and release/reacquire the GIL
    // around the blocking ODBC disconnect. Holding _manager_mutex across a GIL
    // acquisition deadlocks a thread that holds the GIL and is waiting on
    // _manager_mutex.
    for (auto& pool : pools_to_close) {
        pool->close();
    }
}
