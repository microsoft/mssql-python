#include "connection_pool.h"
#include <iostream>
#include <exception>

ConnectionPool::ConnectionPool(const std::wstring& conn_str, size_t max_size, int idle_timeout_secs)
    : _conn_str(conn_str), _max_size(max_size),  _idle_timeout_secs(idle_timeout_secs), _current_size(0) {
        std::wcout << L"[POOL] Created new pool. ConnStr: " << _conn_str 
               << L", Max size: " << _max_size << L", Idle timeout: " << _idle_timeout_secs << L" seconds.\n";
    }

std::shared_ptr<Connection> ConnectionPool::acquire() {
    std::lock_guard<std::mutex> lock(_mutex);
    std::cout << "[POOL] Acquiring connection. Pool size: " << _pool.size() << ", Current size: " << _current_size << "\n";
    
    // Prune idle connections
    size_t pruned_count = 0;
    auto now = std::chrono::steady_clock::now();
    _pool.erase(std::remove_if(_pool.begin(), _pool.end(), [&](const std::shared_ptr<Connection>& conn) {
        auto idle_time = std::chrono::duration_cast<std::chrono::seconds>(now - conn->lastUsed()).count();
        if (idle_time > _idle_timeout_secs) {
            std::cout << "[POOL] Pruning idle connection (idle for " << idle_time << "s).\n";
            conn->disconnect();
            ++pruned_count;
            return true;
        }
        return false;
    }), _pool.end());
    _current_size -= pruned_count;

    while (!_pool.empty()) {
        auto conn = _pool.front(); _pool.pop_front();
        if (conn->isAlive()) {
            std::cout << "[POOL] Reusing alive connection.\n";
            conn->reset();
            conn->updateLastUsed();
            return conn;
        } else {
            std::cout << "[POOL] Discarding dead connection.\n";
            conn->disconnect();
            --_current_size;
        }
    }

    // Create new if under limit
    if (_current_size < _max_size) {
         std::cout << "[POOL] Creating new connection.\n";
        auto conn = std::make_shared<Connection>(_conn_str, true, false); // false → real connection
        SQLRETURN ret = conn->connect();
        if (SQL_SUCCEEDED(ret)) {
            ++_current_size;
            std::cout << "[POOL] Connection successfully created. Current size: " << _current_size << "\n";
            return conn;
        }
         else {
            std::cerr << "[POOL] Connection creation failed.\n";
        }
    } else {
        std::cerr << "[POOL] Pool is at capacity. Cannot create new connection.\n";
    }

    return nullptr; // No available or healthy connections; and creation failed or pool at capacity.
}

void ConnectionPool::release(std::shared_ptr<Connection> conn) {
    std::lock_guard<std::mutex> lock(_mutex);
    std::cout << "[POOL] Releasing connection back to pool. Pool size: " << _pool.size() << "\n";
    conn->updateLastUsed();
    if (_pool.size() < _max_size) {
        std::cout << "[POOL] Connection returned to pool.\n";
        _pool.push_back(conn);
    }
    else {
        std::cout << "[POOL] Pool full. Discarding returned connection.\n";
        conn->disconnect();
        --_current_size;
    }
}

ConnectionPoolManager& ConnectionPoolManager::getInstance() {
    static ConnectionPoolManager manager;
    return manager;
}

void ConnectionPoolManager::configure(int max_size, int idle_timeout) {
    std::lock_guard<std::mutex> lock(_manager_mutex);
    if (max_size > 0) {
        _default_max_size = static_cast<size_t>(max_size);
    }

    if (idle_timeout > 0) {
        _default_idle_secs = idle_timeout;
    }

    // LOG("Configured pooling: max_size = ", _default_max_size,
    //     ", idle_timeout = ", _default_idle_secs);
    std::cout << "[POOL-MGR] Configuration updated. Max size: " << _default_max_size 
              << ", Idle timeout: " << _default_idle_secs << " seconds.\n";
}

std::shared_ptr<Connection> ConnectionPoolManager::acquireConnection(const std::wstring& conn_str) {
    std::lock_guard<std::mutex> lock(_manager_mutex);
    std::wcout << L"[POOL-MGR] Acquiring connection for conn_str: " << conn_str << L"\n";
   
    auto it = _pools.find(conn_str);
    if (it == _pools.end()) {
        std::wcout << L"[POOL-MGR] Creating new connection pool for conn_str: " << conn_str << L"\n";
        auto pool = std::make_shared<ConnectionPool>(conn_str, _default_max_size, _default_idle_secs);
        _pools[conn_str] = pool;
        it = _pools.find(conn_str);
    }
    else {
        std::wcout << L"[POOL-MGR] Found existing pool for conn_str: " << conn_str << L"\n";
    }
    std::cout<<"Returning from acquireConnection" << std::endl;

    return it->second->acquire();
}

void ConnectionPoolManager::returnConnection(const std::wstring& conn_str, std::shared_ptr<Connection> conn) {
    std::lock_guard<std::mutex> lock(_manager_mutex);
    std::wcout << L"[POOL-MGR] Returning connection for conn_str: " << conn_str << L"\n";
    if (_pools.find(conn_str) != _pools.end()) {
        _pools[conn_str]->release((conn));
    }
}

std::shared_ptr<Connection> acquire_pooled(const std::wstring& conn_str) {
    return ConnectionPoolManager::getInstance().acquireConnection(conn_str);
}

void configure_pooling(int max_size, int idle_timeout_secs) {
    ConnectionPoolManager::getInstance().configure(max_size, idle_timeout_secs);
}
