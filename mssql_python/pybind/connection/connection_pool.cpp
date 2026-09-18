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
#include "performance_counter.hpp"

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

// Process-wide monotonic counter for pool IDs to prevent ABA address-reuse (#746)
static std::atomic<uint64_t> s_next_pool_id{1};

ConnectionPool::ConnectionPool(size_t max_size, int idle_timeout_secs)
    : _max_size(max_size),
      _idle_timeout_secs(idle_timeout_secs),
      _current_size(0),
      _checked_out(0),
      _in_flight(0),
      _pool_id(s_next_pool_id.fetch_add(1)) {}

void ConnectionPool::invokeDisconnectHook() {
    std::shared_ptr<std::function<void()>> hook;
    {
        std::lock_guard<std::mutex> lock(_mutex);
        hook = _on_disconnect_hook;
    }
    if (hook) {
        if (*hook) {
            (*hook)();
        }
        py::gil_scoped_acquire gil;
        hook.reset();
    }
}

void ConnectionPool::drainDisconnectList(std::vector<std::shared_ptr<Connection>>& list) {
    for (auto& conn : list) {
        if (!conn) {
            continue;
        }
        invokeDisconnectHook();
        try {
            conn->disconnect();
        } catch (const std::exception& ex) {
            LOG("ConnectionPool::drainDisconnectList: disconnect failed: %s", ex.what());
        }
        {
            std::lock_guard<std::mutex> lock(_mutex);
            if (_in_flight > 0) {
                --_in_flight;
            }
            if (_current_size > 0) {
                --_current_size;
            }
        }
    }
    list.clear();
}

std::shared_ptr<Connection> ConnectionPool::acquire(const std::u16string& connStr,
                                                    const py::dict& attrs_before,
                                                    const py::object& token_factory) {
    PERF_TIMER("ConnectionPool::acquire");
    std::vector<std::shared_ptr<Connection>> to_disconnect;
    struct DisconnectGuard {
        ConnectionPool& pool;
        std::vector<std::shared_ptr<Connection>>& list;
        ~DisconnectGuard() {
            pool.drainDisconnectList(list);
        }
    } guard{*this, to_disconnect};
    std::shared_ptr<Connection> valid_conn = nullptr;

    while (valid_conn == nullptr) {
        bool needs_connect = false;
        py::dict pending_attrs;
        long long pending_expiry = 0;
        bool have_pending_token = false;
        uint64_t reservation_generation = 0;

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
            // Retain capacity for pruned stale connections by accounting for them
            // in _in_flight until Phase 4 disconnects them outside the mutex (#746).
            _in_flight += pruned;
        }

        // Disconnect pruned stale connections outside lock BEFORE attempting
        // candidate validation or slot reservation in Phase 2/3. As each disconnect
        // finishes, drainDisconnectList decrements _in_flight and _current_size.
        drainDisconnectList(to_disconnect);

        // Phase 2: Pop one candidate at a time and validate it outside the
        // mutex.  isAlive() and reset() perform ODBC calls that release the
        // GIL; calling them while holding the mutex would create a mutex/GIL
        // lock-ordering deadlock when multiple threads acquire concurrently.
        //
        // Expiry-aware checkout may capture a freshly minted token here so a
        // rotated-token pool can be reopened without invoking the factory twice.
        while (true) {
            std::shared_ptr<Connection> candidate;
            uint64_t candidate_generation = 0;
            {
                std::unique_lock<std::mutex> lock(_mutex);
                if (_pool.empty()) {
                    // No more candidates — try to reserve a slot for a new connection.
                    if (_current_size < _max_size) {
                        // Reserve the slot here but construct the Connection outside
                        // _mutex (Phase 3): the Connection constructor allocates ODBC
                        // handles and emits log records that acquire the GIL, and
                        // holding _mutex across a GIL acquisition deadlocks a thread
                        // that holds the GIL and is waiting on _mutex (#671).
                        ++_current_size;
                        ++_in_flight;
                        reservation_generation = _generation;
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
                ++_in_flight;
                candidate_generation = _generation;
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
                    if (candidate->isMock()) {
                        candidate->setTokenExpiry(fresh_expiry);
                        reuse_candidate = true;
                    } else {
                        std::string fresh_token = extractAccessToken(fresh_attrs);
                        if (!fresh_token.empty() &&
                            fresh_token == candidate->currentAccessToken() &&
                            tokenExpirySafelyBeyond(fresh_expiry, TOKEN_EXPIRY_THRESHOLD_SECS)) {
                            candidate->setTokenExpiry(fresh_expiry);
                            reuse_candidate = candidate->isAlive() && candidate->reset();
                            if (!reuse_candidate) {
                                pending_attrs = fresh_attrs;
                                pending_expiry = fresh_expiry;
                                have_pending_token = true;
                            }
                        } else {
                            pending_attrs = fresh_attrs;
                            pending_expiry = fresh_expiry;
                            have_pending_token = true;
                            const std::string stale_token = candidate->currentAccessToken();
                            if (!stale_token.empty()) {
                                std::lock_guard<std::mutex> lock(_mutex);
                                if (_generation == candidate_generation) {
                                    _pool.erase(
                                        std::remove_if(
                                            _pool.begin(), _pool.end(),
                                            [&](const std::shared_ptr<Connection>& sibling) {
                                                if (sibling->currentAccessToken() == stale_token) {
                                                    to_disconnect.push_back(sibling);
                                                    ++_in_flight;
                                                    return true;
                                                }
                                                return false;
                                            }),
                                        _pool.end());
                                }
                            }
                        }
                    }
                } else {
                    reuse_candidate = candidate->isAlive() && candidate->reset();
                }
            } catch (const std::exception& ex) {
                LOG("Candidate connection validation failed: %s", ex.what());
            }

            if (reuse_candidate) {
                bool gen_valid = false;
                {
                    std::lock_guard<std::mutex> lock(_mutex);
                    if (_generation == candidate_generation) {
                        candidate->updateLastUsed();
                        candidate->setPoolOrigin(_pool_id, _generation);
                        valid_conn = candidate;
                        ++_checked_out;
                        if (_in_flight > 0) {
                            --_in_flight;
                        }
                        gen_valid = true;
                    }
                }
                if (gen_valid) {
                    break;
                }
                // Pool was closed while validating candidate (#746); discard stale
                // candidate and release in-flight reservation.
                try {
                    candidate->disconnect();
                } catch (const std::exception& ex) {
                    LOG("Disconnect candidate failed: %s", ex.what());
                }
                {
                    std::lock_guard<std::mutex> lock(_mutex);
                    if (_in_flight > 0) {
                        --_in_flight;
                    }
                    if (_current_size > 0) {
                        --_current_size;
                    }
                }
                continue;
            }

            // Candidate is dead, reset failed, or its token rotated — disconnect and
            // release the in-flight reservation (#746).
            try {
                candidate->disconnect();
            } catch (const std::exception& ex) {
                LOG("Disconnect candidate failed: %s", ex.what());
            }
            {
                std::lock_guard<std::mutex> lock(_mutex);
                if (_in_flight > 0) {
                    --_in_flight;
                }
                if (_current_size > 0) {
                    --_current_size;
                }
            }

            // If a rotated token was captured, reserve a slot and reopen with it
            // immediately instead of churning through the remaining candidates
            // (which hold the same stale token and would all be discarded anyway).
            if (have_pending_token) {
                // Drain any siblings placed into to_disconnect before reserving a new slot
                drainDisconnectList(to_disconnect);
                std::lock_guard<std::mutex> lock(_mutex);
                if (_current_size < _max_size) {
                    // Reserve the slot here but construct the Connection outside
                    // _mutex (Phase 3): the constructor emits GIL-acquiring log
                    // records, and holding _mutex across a GIL acquisition
                    // deadlocks a thread that holds the GIL and waits on _mutex (#671).
                    ++_current_size;
                    ++_in_flight;
                    reservation_generation = _generation;
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

        if (valid_conn != nullptr) {
            break;
        }

        // Phase 3: Construct and connect the new connection outside the mutex.
        if (needs_connect) {
            try {
                // Construct the Connection outside _mutex (#671): the constructor
                // allocates ODBC handles and emits log records that acquire the GIL,
                // so it must not run while _mutex is held.
                auto new_conn = std::make_shared<Connection>(connStr, true);
                if (_mock_mode) {
                    new_conn->setMock(true);
                }
                if (have_pending_token) {
                    // Reopen with the fresh token captured during expiry-aware
                    // checkout (the previous connection's token had rotated).
                    new_conn->connect(pending_attrs);
                    new_conn->setTokenExpiry(pending_expiry);
                } else if (token_factory && !token_factory.is_none()) {
                    // Lazy token acquisition: only now, when a physical
                    // connection is actually being opened, do we materialize the
                    // token. On a pool reuse this whole branch is skipped, so a
                    // same-identity hit never acquires a token. The GIL is held here
                    // (connect() releases it only around the ODBC call itself), so
                    // invoking the Python callback is safe.
                    long long expiry = 0;
                    py::dict connect_attrs = Connection::invokeTokenFactory(token_factory, expiry);
                    new_conn->connect(connect_attrs);
                    // Record the token expiry so a later checkout can refresh this
                    // connection before the token lapses.
                    new_conn->setTokenExpiry(expiry);
                } else {
                    new_conn->connect(attrs_before);
                }

                // Verify that pool was not closed while connecting outside the mutex (#746).
                bool gen_valid = false;
                {
                    std::lock_guard<std::mutex> lock(_mutex);
                    if (_generation == reservation_generation) {
                        new_conn->updateLastUsed();
                        new_conn->setPoolOrigin(_pool_id, _generation);
                        valid_conn = new_conn;
                        ++_checked_out;
                        if (_in_flight > 0) {
                            --_in_flight;
                        }
                        gen_valid = true;
                    }
                }
                if (gen_valid) {
                    break;
                }
                // Pool was closed while connecting. Disconnect the stale connection
                // immediately BEFORE relinquishing the in-flight reservation, so that
                // the stale physical connection and any newly reserved connections do
                // not co-exist and exceed max_size (#746).
                invokeDisconnectHook();
                try {
                    new_conn->disconnect();
                } catch (const std::exception& ex) {
                    LOG("Disconnect stale connection failed: %s", ex.what());
                }
                {
                    std::lock_guard<std::mutex> lock(_mutex);
                    if (_in_flight > 0) {
                        --_in_flight;
                    }
                    if (_current_size > 0) {
                        --_current_size;
                    }
                }
                continue;
            } catch (...) {
                // Construct/connect failed — release the reserved slot and in-flight count.
                {
                    std::lock_guard<std::mutex> lock(_mutex);
                    if (_in_flight > 0) {
                        --_in_flight;
                    }
                    if (_current_size > 0) {
                        --_current_size;
                    }
                }
                throw;
            }
        }
    }

    // Phase 4: Disconnect expired/bad connections outside lock and decrement in-flight capacity.
    drainDisconnectList(to_disconnect);
    return valid_conn;
}

void ConnectionPool::release(std::shared_ptr<Connection> conn) {
    PERF_TIMER("ConnectionPool::release");
    if (!conn) {
        return;
    }
    bool should_disconnect = false;
    bool decrement_in_flight = false;
    {
        std::lock_guard<std::mutex> lock(_mutex);
        if (conn->originPoolId() == _pool_id) {
            bool generation_matches = (conn->originGeneration() == _generation);
            if (generation_matches && _pool.size() < _max_size) {
                conn->updateLastUsed();
                _pool.push_back(conn);
                if (_checked_out > 0) {
                    --_checked_out;
                }
                conn->setPoolOrigin(0, 0);
            } else {
                should_disconnect = true;
                if (_checked_out > 0) {
                    --_checked_out;
                    // Keep this connection accounted for as in-flight until disconnect
                    // completes outside the mutex, so a concurrent acquire cannot reserve
                    // and open a new physical handle while this handle is still live (#746).
                    ++_in_flight;
                    decrement_in_flight = true;
                }
                conn->setPoolOrigin(0, 0);
            }
        } else {
            should_disconnect = true;
        }
    }
    // Disconnect outside the mutex to avoid holding it during the
    // blocking ODBC call (which releases the GIL).
    if (should_disconnect) {
        invokeDisconnectHook();
        try {
            conn->disconnect();
        } catch (const std::exception& ex) {
            LOG("ConnectionPool::release: disconnect failed: %s", ex.what());
        }
        if (decrement_in_flight) {
            std::lock_guard<std::mutex> lock(_mutex);
            if (_in_flight > 0) {
                --_in_flight;
            }
            if (_current_size > 0) {
                --_current_size;
            }
        }
    }
}

bool ConnectionPool::canEvict() {
    std::lock_guard<std::mutex> lock(_mutex);
    // Never evict while any connection is checked out or in-flight. Reserved
    // capacity (_current_size) beyond what is sitting idle in _pool means a
    // caller still holds one, so the pool must stay.
    size_t in_flight_or_checked_out =
        (_current_size > _pool.size()) ? (_current_size - _pool.size()) : 0;
    if (in_flight_or_checked_out > 0 || _checked_out > 0 || _in_flight > 0) {
        return false;
    }
    // Nothing checked out and the pool is empty: safe to drop immediately.
    if (_pool.empty()) {
        return true;
    }
    // Empty pools past idle timeout can be evicted. Checking the idle
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
    PERF_TIMER("ConnectionPool::close");
    std::vector<std::shared_ptr<Connection>> to_close;
    {
        std::lock_guard<std::mutex> lock(_mutex);
        while (!_pool.empty()) {
            to_close.push_back(_pool.front());
            _pool.pop_front();
        }
        // Account for closing idle connections in _in_flight so a concurrent
        // acquire cannot reserve and open a new physical handle while these
        // old handles are still connected outside the mutex (#746).
        _in_flight += to_close.size();
        _current_size = _checked_out + _in_flight;
        ++_generation;
    }
    for (auto& conn : to_close) {
        invokeDisconnectHook();
        try {
            conn->disconnect();
        } catch (const std::exception& ex) {
            LOG("ConnectionPool::close: disconnect failed: %s", ex.what());
        }
        {
            std::lock_guard<std::mutex> lock(_mutex);
            if (_in_flight > 0) {
                --_in_flight;
            }
            if (_current_size > 0) {
                --_current_size;
            }
        }
    }
}

ConnectionPoolManager& ConnectionPoolManager::getInstance() {
    static ConnectionPoolManager manager;
    return manager;
}

std::shared_ptr<Connection> ConnectionPoolManager::acquireConnection(
    const std::u16string& connStr,
    const py::dict& attrs_before,
    const std::u16string& pool_key,
    const py::object& token_factory) {
    PERF_TIMER("ConnectionPoolManager::acquireConnection");
    // Key the pool by pool_key when provided (identity-aware),
    // else fall back to the connection string (legacy behavior).
    const std::u16string& key = pool_key.empty() ? connStr : pool_key;
    std::shared_ptr<ConnectionPool> pool;
    std::shared_ptr<ConnectionPool> old_pool_to_close;
    bool created = false;
    std::vector<std::pair<std::u16string, std::shared_ptr<ConnectionPool>>> evicted;

    // RAII guard ensuring any key placed in _closing_keys is removed and
    // _manager_cv notified even if an exception or early return occurs (#746).
    struct ClosingGuard {
        ConnectionPoolManager& mgr;
        std::vector<std::u16string> keys;
        ~ClosingGuard() {
            if (!keys.empty()) {
                std::lock_guard<std::mutex> lock(mgr._manager_mutex);
                for (const auto& k : keys) {
                    mgr._closing_keys.erase(k);
                }
                mgr._manager_cv.notify_all();
            }
        }
        void remove(const std::u16string& k) {
            mgr._closing_keys.erase(k);
            keys.erase(std::remove(keys.begin(), keys.end(), k), keys.end());
            mgr._manager_cv.notify_all();
        }
    } closing_guard{*this};

    {
        py::gil_scoped_release release_gil;
        {
            std::unique_lock<std::mutex> lock(_manager_mutex);
            // Wait if this key is currently undergoing close/replacement by another thread,
            // or until pooling is disabled. Serializes replacement creation with old-pool teardown (#746).
            _manager_cv.wait(lock, [this, &key]() {
                return !_accepting || _closing_keys.find(key) == _closing_keys.end();
            });

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
                        _closing_keys.insert(it->first);
                        closing_guard.keys.push_back(it->first);
                        evicted.push_back({it->first, it->second});
                        it = _pools.erase(it);
                    } else {
                        ++it;
                    }
                }
            }
            // Defer replacement-pool creation if the existing pool still has live
            // work. If the existing pool has finished all live work (canEvict() == true)
            // and is not held by concurrent acquirers (use_count() == 1), evict it
            // and serialize its close BEFORE publishing a new replacement pool (#746).
            auto it = _pools.find(key);
            if (it != _pools.end() && it->second && it->second.use_count() == 1 &&
                it->second->canEvict()) {
                old_pool_to_close = it->second;
                _pools.erase(it);
                _closing_keys.insert(key);
                closing_guard.keys.push_back(key);
            }
            if (!old_pool_to_close) {
                auto& pool_ref = _pools[key];
                if (!pool_ref) {
                    pool_ref = std::make_shared<ConnectionPool>(_default_max_size, _default_idle_secs);
                    if (_mock_mode) {
                        pool_ref->set_mock_mode(true);
                    }
                    created = true;
                }
                pool = pool_ref;
            }
        }
        if (old_pool_to_close) {
            // Close the old pool completely BEFORE creating and publishing the replacement,
            // ensuring its physical handles are disconnected before new ones can be opened (#746).
            try {
                old_pool_to_close->close();
            } catch (const std::exception& ex) {
                LOG("ConnectionPoolManager: closing evicted pool failed: %s", ex.what());
            }
            old_pool_to_close.reset();
            {
                std::lock_guard<std::mutex> lock(_manager_mutex);
                if (_accepting) {
                    auto& pool_ref = _pools[key];
                    if (!pool_ref) {
                        pool_ref = std::make_shared<ConnectionPool>(_default_max_size, _default_idle_secs);
                        if (_mock_mode) {
                            pool_ref->set_mock_mode(true);
                        }
                        created = true;
                    }
                    pool = pool_ref;
                }
                closing_guard.remove(key);
            }
            if (!_accepting) {
                return nullptr;
            }
        }
        // Close evicted pools outside _manager_mutex: close() disconnects ODBC
        // handles (releasing the GIL), which must never run while holding
        // _manager_mutex or we risk a mutex/GIL lock-ordering deadlock.
        for (auto& [evicted_key, evicted_pool] : evicted) {
            try {
                evicted_pool->close();
            } catch (const std::exception& ex) {
                LOG("ConnectionPoolManager: closing evicted pool failed: %s", ex.what());
            }
            evicted_pool.reset();
            {
                std::lock_guard<std::mutex> lock(_manager_mutex);
                closing_guard.remove(evicted_key);
            }
        }
    }
    // Log after releasing _manager_mutex (#671): LOG() acquires the GIL, and
    // holding a native mutex across a GIL acquisition deadlocks a thread that
    // holds the GIL and is waiting on the same mutex.
    if (created) {
        LOG("Creating new connection pool");
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
    py::gil_scoped_release release_gil;
    // Under _manager_mutex, snapshot all pools to close their idle connections.
    // Wait for any in-flight same-key pool replacements to finish closing first (#746).
    std::vector<std::shared_ptr<ConnectionPool>> to_close;
    {
        std::unique_lock<std::mutex> lock(_manager_mutex);
        _manager_cv.wait(lock, [this]() { return _closing_keys.empty(); });
        to_close.reserve(_pools.size());
        for (auto& [conn_str, pool] : _pools) {
            if (pool) {
                to_close.push_back(pool);
            }
        }
    }
    // Close each pool outside _manager_mutex: close() drains idle connections,
    // bumps _generation, and sets _current_size = _checked_out + _in_flight (#746).
    for (auto& pool : to_close) {
        try {
            pool->close();
        } catch (const std::exception& ex) {
            LOG("ConnectionPoolManager::closePools: closing pool failed: %s", ex.what());
        }
    }
    to_close.clear();
    {
        std::lock_guard<std::mutex> lock(_manager_mutex);
        // Only evict pools that have no live work left (canEvict() == true) and
        // are not held by any concurrent thread (use_count() == 1).
        // If an old pool still has checked-out connections or in-flight opens,
        // retain it in _pools so that:
        // 1. Creation of a replacement pool is deferred until the old pool has
        //    no live work, preventing capacity overflow across recreation (#746).
        // 2. Any subsequent acquireConnection() respects live capacity.
        // 3. returnConnection() continues to route to this pool to decrement
        //    _checked_out and _current_size as connections are released.
        for (auto it = _pools.begin(); it != _pools.end();) {
            if (!it->second || (it->second.use_count() == 1 && it->second->canEvict())) {
                it = _pools.erase(it);
            } else {
                ++it;
            }
        }
        // Reset the sweep throttle so a fresh pool set after this is swept
        // on its next acquireConnection().
        _last_sweep = std::chrono::steady_clock::time_point{};
    }
}

void ConnectionPoolManager::setAccepting(bool accepting) {
    std::lock_guard<std::mutex> lock(_manager_mutex);
    _accepting = accepting;
    _manager_cv.notify_all();
}
