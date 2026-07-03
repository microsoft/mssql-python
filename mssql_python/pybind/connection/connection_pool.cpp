// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#include "connection/connection_pool.h"
#include <exception>
#include <memory>
#include <vector>

// Logging uses LOG() macro for all diagnostic output
#include "logger_bridge.hpp"

// Refresh threshold for expiry-aware checkout: a pooled connection whose access
// token expires within this many seconds is discarded and reopened with a fresh
// token rather than handed out (<=10 min).
static constexpr int TOKEN_EXPIRY_THRESHOLD_SECS = 300;

ConnectionPool::ConnectionPool(size_t max_size, int idle_timeout_secs)
    : _max_size(max_size),
      _idle_timeout_secs(idle_timeout_secs),
      _current_size(0) {}

std::shared_ptr<Connection> ConnectionPool::acquire(const std::u16string& connStr,
                                                    const py::dict& attrs_before,
                                                    const py::object& token_factory) {
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
            std::unique_lock<std::mutex> lock(_mutex);
            if (_pool.empty()) {
                // No more candidates — try to reserve a slot for a new connection.
                if (_current_size < _max_size) {
                    valid_conn = std::make_shared<Connection>(connStr, true);
                    ++_current_size;
                    needs_connect = true;
                    break;
                }
                // Pool is full — throw immediately. Another thread may be
                // validating a popped candidate outside the mutex right now, so
                // a transient "pool full" is an acceptable trade-off that
                // callers can retry.
                throw std::runtime_error(
                    "ConnectionPool::acquire: pool size limit reached");
            }
            candidate = _pool.front();
            _pool.pop_front();
        }

        // Validate the candidate outside the mutex.
        try {
            // Expiry-aware checkout: if this connection was opened
            // with an access token that is at/near expiry and we have a factory
            // to mint a fresh one, do not reuse it — discard and reopen below so
            // the caller never authenticates with an about-to-expire token.
            bool token_expiring =
                token_factory && !token_factory.is_none() &&
                candidate->isTokenNearExpiry(TOKEN_EXPIRY_THRESHOLD_SECS);
            if (!token_expiring && candidate->isAlive() && candidate->reset()) {
                valid_conn = candidate;
                break;
            }
        } catch (const std::exception& ex) {
            LOG("Candidate connection validation failed: %s", ex.what());
        }

        // Candidate is dead, near token expiry, or reset failed — mark for
        // disconnect and decrement the pool size.
        to_disconnect.push_back(candidate);
        {
            std::lock_guard<std::mutex> lock(_mutex);
            if (_current_size > 0) --_current_size;
        }
    }

    // Phase 3: Connect the new connection outside the mutex.
    if (needs_connect) {
        try {
            // Lazy token acquisition: only now, when a physical
            // connection is actually being opened, do we materialize the
            // token. On a pool reuse this whole branch is skipped, so a
            // same-identity hit never acquires a token. The GIL is held here
            // (connect() releases it only around the ODBC call itself), so
            // invoking the Python callback is safe.
            if (token_factory && !token_factory.is_none()) {
                long long expiry = 0;
                py::dict connect_attrs = Connection::invokeTokenFactory(token_factory, expiry);
                valid_conn->connect(connect_attrs);
                // Record the token expiry so a later checkout can refresh this
                // connection before the token lapses.
                valid_conn->setTokenExpiry(expiry);
            } else {
                valid_conn->connect(attrs_before);
            }
        } catch (...) {
            // Connect failed — release the reserved slot
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

bool ConnectionPool::canEvict() {
    std::lock_guard<std::mutex> lock(_mutex);
    // Only evictable when there are no pooled connections AND no checked-out or
    // in-flight ones (reserved capacity is zero). This avoids dropping a pool
    // whose connections are temporarily all in use.
    return _current_size == 0 && _pool.empty();
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
                                                                     const py::dict& attrs_before,
                                                                     const std::u16string& pool_key,
                                                                     const py::object& token_factory) {
    // Key the pool by pool_key when provided (identity-aware),
    // else fall back to the connection string (legacy behavior).
    const std::u16string& key = pool_key.empty() ? connStr : pool_key;
    std::shared_ptr<ConnectionPool> pool;
    {
        std::lock_guard<std::mutex> lock(_manager_mutex);
        // Lazy eviction: drop pools that hold no live or in-flight
        // connections so distinct short-lived identities (e.g. per-request
        // Entra users) do not accumulate empty pools forever. The pool we are
        // about to use is skipped so it is never evicted from under us.
        for (auto it = _pools.begin(); it != _pools.end();) {
            if (it->first != key && it->second && it->second->canEvict()) {
                it = _pools.erase(it);
            } else {
                ++it;
            }
        }
        auto& pool_ref = _pools[key];
        if (!pool_ref) {
            LOG("Creating new connection pool");
            pool_ref = std::make_shared<ConnectionPool>(_default_max_size, _default_idle_secs);
        }
        pool = pool_ref;
    }
    // Call acquire() outside _manager_mutex.  acquire() may release the GIL
    // during the ODBC connect call; holding _manager_mutex across that would
    // create a mutex/GIL lock-ordering deadlock. connStr (not key) is used to
    // establish new physical connections.
    return pool->acquire(connStr, attrs_before, token_factory);
}

void ConnectionPoolManager::returnConnection(const std::u16string& pool_key,
                                             const std::shared_ptr<Connection> conn) {
    std::shared_ptr<ConnectionPool> pool;
    {
        std::lock_guard<std::mutex> lock(_manager_mutex);
        auto it = _pools.find(pool_key);
        if (it != _pools.end()) {
            pool = it->second;
        }
    }
    // Call release() outside _manager_mutex to avoid deadlock.
    if (pool) {
        pool->release(conn);
    } else {
        // No pool is registered under this key (e.g. the pool was lazily
        // evicted while this connection was checked out, or the key changed).
        // Disconnect the orphaned connection instead of leaking it:
        // dropping the shared_ptr alone would keep the ODBC handle
        // around until GC, and returnConnection is the deterministic close
        // path. Done outside _manager_mutex (disconnect releases the GIL).
        if (conn) {
            try {
                conn->disconnect();
            } catch (const std::exception& ex) {
                LOG("ConnectionPoolManager::returnConnection: disconnect of orphaned "
                    "connection failed: %s",
                    ex.what());
            }
        }
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
