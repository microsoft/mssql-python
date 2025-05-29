#include "connection_pool.h"
#include <iostream>
#include <exception>

ConnectionPool::ConnectionPool(size_t max_size, int idle_timeout_secs)
    : _max_size(max_size),  _idle_timeout_secs(idle_timeout_secs), _current_size(0) {}

std::shared_ptr<Connection> ConnectionPool::acquire(const std::wstring& connStr, const py::dict& attrs_before) {
    std::lock_guard<std::mutex> lock(_mutex);
    auto now = std::chrono::steady_clock::now();
    size_t before = _pool.size();
    _pool.erase(std::remove_if(_pool.begin(), _pool.end(), [&](const std::shared_ptr<Connection>& conn) {
        auto idle_time = std::chrono::duration_cast<std::chrono::seconds>(now - conn->lastUsed()).count();
        if (idle_time > _idle_timeout_secs) {
            conn->disconnect();
            return true;
        }
        return false;
    }), _pool.end());
    size_t pruned = before - _pool.size();
    _current_size = (_current_size >= pruned) ? (_current_size - pruned) : 0;

    while (!_pool.empty()) {
        auto conn = _pool.front();
        _pool.pop_front();
        if (conn->isAlive()) {
            if (!conn->reset()) {
                continue;
            }
            return conn;
        } else {
            conn->disconnect();
            --_current_size;
        }
    }
    if (_current_size < _max_size) {
        auto conn = std::make_shared<Connection>(connStr, true);
        conn->connect(attrs_before);
        return conn;
    } else {
        LOG("Cannot acquire connection: pool size limit reached");
        return nullptr;
    }
}

void ConnectionPool::release(std::shared_ptr<Connection> conn) {
    std::lock_guard<std::mutex> lock(_mutex);
    if (_pool.size() < _max_size) {
        conn->updateLastUsed();
        _pool.push_back(conn);
    }
    else {
        conn->disconnect();
        if (_current_size > 0) --_current_size;
    }
}

ConnectionPoolManager& ConnectionPoolManager::getInstance() {
    static ConnectionPoolManager manager;
    return manager;
}

std::shared_ptr<Connection> ConnectionPoolManager::acquireConnection(const std::wstring& connStr, const py::dict& attrs_before) {
    std::lock_guard<std::mutex> lock(_manager_mutex);

    auto& pool = _pools[connStr];
    if (!pool) {
        LOG("Creating new connection pool");
        pool = std::make_shared<ConnectionPool>(_default_max_size, _default_idle_secs);
    }
    return pool->acquire(connStr, attrs_before);
}

void ConnectionPoolManager::returnConnection(const std::wstring& conn_str, const std::shared_ptr<Connection> conn) {
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