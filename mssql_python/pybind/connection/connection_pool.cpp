// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#include "connection/connection_pool.h"
#include <algorithm>
#include <chrono>
#include <exception>
#include <memory>
#include <vector>

// Logging uses LOG() macro for all diagnostic output
#include "logger_bridge.hpp"

// Refresh threshold for expiry-aware checkout: a pooled connection whose access
// token expires within this many seconds is discarded and reopened with a fresh
// token rather than handed out (<=5 min).
static constexpr int TOKEN_EXPIRY_THRESHOLD_SECS = 300;

// True only when *expiryEpoch* is a known POSIX-second expiry that is safely
// beyond now + thresholdSecs. A zero/negative (unknown/missing) expiry is
// treated as NOT safe so the caller fails closed rather than reusing a token it
// cannot prove is still valid. Caller need not hold any lock; reads the wall
// clock only.
static bool tokenExpirySafelyBeyond(long long expiryEpoch, int thresholdSecs) {
    if (expiryEpoch <= 0) {
        return false;
    }
    const long long now = static_cast<long long>(
        std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
    return (now + static_cast<long long>(thresholdSecs)) < expiryEpoch;
}

// Pull the raw access-token bytes out of a connect-attrs dict returned by the
// token factory, or an empty string if the dict carries no token. Caller must
// hold the GIL. Keys in the dict are attribute ids (ints). SQL_COPT_SS_ACCESS_TOKEN
// is defined once in connection.h (shared with connection.cpp).
static std::string extractAccessToken(const py::dict& attrs) {
    for (auto item : attrs) {
        if (py::isinstance<py::int_>(item.first) &&
            item.first.cast<long>() == SQL_COPT_SS_ACCESS_TOKEN) {
            try {
                return item.second.cast<std::string>();
            } catch (const py::cast_error&) {
                return std::string();
            }
        }
    }
    return std::string();
}

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
    //
    // Expiry-aware checkout may capture a freshly minted token here so a
    // rotated-token pool can be reopened without invoking the factory twice.
    py::dict pending_attrs;
    long long pending_expiry = 0;
    bool have_pending_token = false;
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
        bool reuse_candidate = false;
        try {
            if (token_factory && !token_factory.is_none() &&
                candidate->isTokenNearExpiry(TOKEN_EXPIRY_THRESHOLD_SECS)) {
                // Expiry-aware checkout with token compare: the pooled token is
                // at/near expiry, so mint a fresh one and compare. If the
                // provider returns the SAME token (its cache is still valid),
                // the connection is healthy — refresh the recorded expiry and
                // reuse it rather than needlessly churning. Only a DIFFERENT
                // (rotated) token forces discard-and-reopen, and we carry the
                // fresh attrs forward so the reopen below does not invoke the
                // factory a second time.
                long long fresh_expiry = 0;
                py::dict fresh_attrs =
                    Connection::invokeTokenFactory(token_factory, fresh_expiry);
                std::string fresh_token = extractAccessToken(fresh_attrs);
                if (!fresh_token.empty() &&
                    fresh_token == candidate->currentAccessToken() &&
                    tokenExpirySafelyBeyond(fresh_expiry, TOKEN_EXPIRY_THRESHOLD_SECS)) {
                    // Same token AND its refreshed expiry is safely beyond the
                    // threshold: the provider's cache is still valid and the
                    // connection is healthy, so refresh the recorded expiry and
                    // reuse. We deliberately do NOT reuse when the returned
                    // expiry is unknown (<=0) or still inside the threshold —
                    // extending the recorded expiry and handing the connection
                    // back would defeat the very refresh this checkout intended
                    // (the token could expire mid-query). Those cases fall
                    // through to discard-and-reopen below.
                    //
                    // Narrow edge: a MISBEHAVING provider that repeatedly hands
                    // back the same token still inside the threshold makes every
                    // checkout discard + reopen (and get the same near-expiry
                    // token) — pure churn, no benefit. This is acceptable: a
                    // well-behaved azure-identity credential refreshes
                    // proactively (returning a token with a fresh, far-out
                    // expiry) before the threshold, so the safe-reuse path above
                    // is taken in practice. We favor never handing out a token
                    // that may expire mid-query over avoiding the churn.
                    candidate->setTokenExpiry(fresh_expiry);
                    reuse_candidate = candidate->isAlive() && candidate->reset();
                } else {
                    // Token rotated, or the "fresh" token is still at/near
                    // expiry (or has an unknown expiry): discard and reopen with
                    // the fresh attrs. Remember the fresh token to reopen with,
                    // and eagerly drain the sibling idle connections that still
                    // hold the now-stale token. They were all minted from the
                    // same provider before the rotation, so they are equally
                    // stale; discarding them together here avoids rediscovering
                    // each one (and paying another factory compare) on later
                    // checkouts. No ODBC calls under the mutex — the actual
                    // disconnects happen in Phase 4, outside the lock.
                    pending_attrs = fresh_attrs;
                    pending_expiry = fresh_expiry;
                    have_pending_token = true;
                    const std::string stale_token = candidate->currentAccessToken();
                    if (!stale_token.empty()) {
                        std::lock_guard<std::mutex> lock(_mutex);
                        _pool.erase(
                            std::remove_if(
                                _pool.begin(), _pool.end(),
                                [&](const std::shared_ptr<Connection>& sibling) {
                                    if (sibling->currentAccessToken() == stale_token) {
                                        to_disconnect.push_back(sibling);
                                        if (_current_size > 0) --_current_size;
                                        return true;
                                    }
                                    return false;
                                }),
                            _pool.end());
                    }
                }
            } else {
                reuse_candidate = candidate->isAlive() && candidate->reset();
            }
        } catch (const std::exception& ex) {
            LOG("Candidate connection validation failed: %s", ex.what());
        }

        if (reuse_candidate) {
            valid_conn = candidate;
            break;
        }

        // Candidate is dead, reset failed, or its token rotated — mark for
        // disconnect and decrement the pool size.
        to_disconnect.push_back(candidate);
        {
            std::lock_guard<std::mutex> lock(_mutex);
            if (_current_size > 0) --_current_size;
        }

        // If a rotated token was captured, reserve a slot and reopen with it
        // immediately instead of churning through the remaining candidates
        // (which hold the same stale token and would all be discarded anyway).
        if (have_pending_token) {
            std::lock_guard<std::mutex> lock(_mutex);
            if (_current_size < _max_size) {
                valid_conn = std::make_shared<Connection>(connStr, true);
                ++_current_size;
                needs_connect = true;
                break;
            }
            // Pool momentarily full; fall through and retry the loop. On the
            // retry another near-expiry candidate may re-invoke the factory and
            // overwrite pending_attrs/pending_expiry with a newer token. That
            // needs a full pool AND a simultaneous rotation, is rare, and is
            // harmless: we simply reopen with the most recently minted token.
        }
    }

    // Phase 3: Connect the new connection outside the mutex.
    if (needs_connect) {
        try {
            if (have_pending_token) {
                // Reopen with the fresh token captured during expiry-aware
                // checkout (the previous connection's token had rotated).
                valid_conn->connect(pending_attrs);
                valid_conn->setTokenExpiry(pending_expiry);
            } else if (token_factory && !token_factory.is_none()) {
                // Lazy token acquisition: only now, when a physical
                // connection is actually being opened, do we materialize the
                // token. On a pool reuse this whole branch is skipped, so a
                // same-identity hit never acquires a token. The GIL is held here
                // (connect() releases it only around the ODBC call itself), so
                // invoking the Python callback is safe.
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
    // Never evict while any connection is checked out or in-flight. Reserved
    // capacity (_current_size) beyond what is sitting idle in _pool means a
    // caller still holds one, so the pool must stay.
    size_t checked_out = (_current_size > _pool.size()) ? (_current_size - _pool.size()) : 0;
    if (checked_out > 0) {
        return false;
    }
    // Nothing checked out and the pool is empty: safe to drop immediately.
    if (_pool.empty()) {
        return true;
    }
    // Nothing checked out but idle connections remain. Evict the whole pool
    // only once EVERY pooled connection has been idle longer than the idle
    // timeout. This is what reclaims pools for rotating / single-use identities
    // (e.g. per-request Entra users keyed by token hash): such a pool is never
    // acquired again, so its idle connection is never pruned by acquire() and
    // _current_size would otherwise stay > 0 forever. Evaluating the idle
    // timeout here lets the next acquireConnection() on any key sweep it away.
    auto now = std::chrono::steady_clock::now();
    for (const auto& conn : _pool) {
        auto idle_time =
            std::chrono::duration_cast<std::chrono::seconds>(now - conn->lastUsed()).count();
        if (idle_time <= _idle_timeout_secs) {
            return false;
        }
    }
    return true;
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
    std::vector<std::shared_ptr<ConnectionPool>> evicted;
    {
        std::lock_guard<std::mutex> lock(_manager_mutex);
        // Pooling disabled (a concurrent disable_pooling() disarmed us): decline
        // to create or hand out a pool. Because this check and the pool creation
        // below share _manager_mutex with the setAccepting(false) in
        // disable_pooling(), the decision is atomic — a connect either creates
        // its pool before the disable (and closePools() then reaps it) or sees
        // _accepting == false here and never creates one. The caller
        // (ConnectionHandle) falls back to a non-pooled connection.
        if (!_accepting) {
            return nullptr;
        }
        // Lazy eviction: drop pools whose connections are all idle past the
        // idle timeout (and none checked out) so distinct short-lived
        // identities (e.g. per-request Entra users keyed by token hash) do not
        // accumulate pools forever. canEvict() only inspects state (no ODBC
        // calls), so it is safe under _manager_mutex; the actual disconnects
        // happen via close() below, outside the lock. The pool we are about to
        // use is skipped so it is never evicted from under us.
        //
        // The sweep is O(pools × idle-conns) under the global mutex, so it is
        // throttled: a pool can only become evictable after its connections
        // sit idle past the idle timeout, so sweeping more often than that
        // window is pure overhead. Between sweeps we skip straight to the pool
        // lookup, keeping the hot path cheap under a many-identity connect load.
        auto now = std::chrono::steady_clock::now();
        auto sweep_interval = std::chrono::seconds(std::max(1, _default_idle_secs));
        if (now - _last_sweep >= sweep_interval) {
            _last_sweep = now;
            for (auto it = _pools.begin(); it != _pools.end();) {
                // Only evict a pool that no one else is holding: use_count == 1
                // means the map is the sole owner. An in-flight acquirer copies
                // its pool shared_ptr while holding _manager_mutex (same section
                // as this sweep) and keeps that copy across the unlocked
                // acquire(); returnConnection() likewise takes a ref under the
                // mutex before releasing. Either bumps use_count above 1 for the
                // whole window, so this guard prevents evicting — and then
                // closing (disconnecting) — a pool a peer thread has already
                // selected but not yet finished using.
                if (it->first != key && it->second && it->second.use_count() == 1 &&
                    it->second->canEvict()) {
                    evicted.push_back(it->second);
                    it = _pools.erase(it);
                } else {
                    ++it;
                }
            }
        }
        auto& pool_ref = _pools[key];
        if (!pool_ref) {
            LOG("Creating new connection pool");
            pool_ref = std::make_shared<ConnectionPool>(_default_max_size, _default_idle_secs);
        }
        pool = pool_ref;
    }
    // Close evicted pools outside _manager_mutex: close() disconnects ODBC
    // handles (releasing the GIL), which must never run while holding
    // _manager_mutex or we risk a mutex/GIL lock-ordering deadlock.
    for (auto& evicted_pool : evicted) {
        try {
            evicted_pool->close();
        } catch (const std::exception& ex) {
            LOG("ConnectionPoolManager: closing evicted pool failed: %s", ex.what());
        }
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
    // Reset the sweep throttle so the new idle timeout takes effect on the very
    // next acquireConnection() instead of waiting out a stale interval.
    _last_sweep = std::chrono::steady_clock::time_point{};
}

void ConnectionPoolManager::closePools() {
    // Mirror the eviction-sweep pattern: under the mutex, move every pool into
    // a local vector and clear the map, then release the mutex before closing.
    // close() disconnects ODBC handles (releasing the GIL), which must never
    // run while holding _manager_mutex or we risk a mutex/GIL lock-ordering
    // deadlock with a concurrent acquireConnection()/returnConnection().
    std::vector<std::shared_ptr<ConnectionPool>> to_close;
    {
        std::lock_guard<std::mutex> lock(_manager_mutex);
        to_close.reserve(_pools.size());
        for (auto& [conn_str, pool] : _pools) {
            if (pool) {
                to_close.push_back(pool);
            }
        }
        _pools.clear();
        // Nothing left to sweep; reset the throttle so a fresh pool set after
        // this is swept on its next acquireConnection().
        _last_sweep = std::chrono::steady_clock::time_point{};
    }
    // Close each pool outside _manager_mutex.
    for (auto& pool : to_close) {
        try {
            pool->close();
        } catch (const std::exception& ex) {
            LOG("ConnectionPoolManager::closePools: closing pool failed: %s", ex.what());
        }
    }
}

size_t ConnectionPoolManager::poolCount() {
    std::lock_guard<std::mutex> lock(_manager_mutex);
    return _pools.size();
}

void ConnectionPoolManager::setAccepting(bool accepting) {
    std::lock_guard<std::mutex> lock(_manager_mutex);
    _accepting = accepting;
}
