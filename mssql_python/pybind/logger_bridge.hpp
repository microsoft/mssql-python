/**
 * Copyright (c) Microsoft Corporation.
 * Licensed under the MIT license.
 * 
 * Logger Bridge for mssql_python - High-performance logging from C++ to Python
 * 
 * This bridge provides zero-overhead logging when disabled via:
 * - Cached Python logger object (import once)
 * - Atomic log level storage (lock-free reads)
 * - Fast inline level checks
 * - Lazy message formatting
 */

#ifndef MSSQL_PYTHON_LOGGER_BRIDGE_HPP
#define MSSQL_PYTHON_LOGGER_BRIDGE_HPP

#include <pybind11/pybind11.h>
#include <atomic>
#include <mutex>
#include <cstdarg>
#include <string>

namespace py = pybind11;

namespace mssql_python {
namespace logging {

// Log level constants (matching Python levels)
// Note: Avoid using ERROR as it conflicts with Windows.h macro
const int LOG_LEVEL_DEBUG = 10;    // Debug/diagnostic logging
const int LOG_LEVEL_INFO = 20;     // Informational
const int LOG_LEVEL_WARNING = 30;  // Warnings
const int LOG_LEVEL_ERROR = 40;    // Errors
const int LOG_LEVEL_CRITICAL = 50; // Critical errors

/**
 * LoggerBridge - Bridge between C++ and Python logging
 * 
 * Features:
 * - Singleton pattern
 * - Cached Python logger (imported once)
 * - Atomic level check (zero overhead)
 * - Thread-safe
 * - GIL-aware
 */
class LoggerBridge {
public:
    /**
     * Initialize the logger bridge.
     * Must be called once during module initialization.
     * Caches the Python logger object and initial level.
     */
    static void initialize();
    
    /**
     * Update the cached log level.
     * Called from Python when logger.setLevel() is invoked.
     * 
     * @param level New log level
     */
    static void updateLevel(int level);
    
    /**
     * Fast check if a log level is enabled.
     * This is inline and lock-free for zero overhead.
     * 
     * @param level Log level to check
     * @return true if level is enabled, false otherwise
     */
    static inline bool isLoggable(int level) {
        return level >= cached_level_.load(std::memory_order_relaxed);
    }
    
    /**
     * Log a message at the specified level.
     * Only call this if isLoggable() returns true.
     * 
     * @param level Log level
     * @param file Source file name (__FILE__)
     * @param line Line number (__LINE__)
     * @param format Printf-style format string
     * @param ... Variable arguments for format string
     */
    static void log(int level, const char* file, int line, 
                   const char* format, ...);
    
    /**
     * Get the current log level.
     * 
     * @return Current log level
     */
    static int getLevel();
    
    /**
     * Check if the bridge is initialized.
     * 
     * @return true if initialized, false otherwise
     */
    static bool isInitialized();

private:
    // Private constructor (singleton)
    LoggerBridge() = default;
    
    // No copying or moving
    LoggerBridge(const LoggerBridge&) = delete;
    LoggerBridge& operator=(const LoggerBridge&) = delete;
    
    // Cached Python logger object
    static PyObject* cached_logger_;
    
    // Cached log level (atomic for lock-free reads)
    static std::atomic<int> cached_level_;
    
    // Mutex for initialization and Python calls
    static std::mutex mutex_;
    
    // Initialization flag
    static bool initialized_;
    
    /**
     * Helper to format message with va_list.
     * 
     * @param format Printf-style format string
     * @param args Variable arguments
     * @return Formatted string
     */
    static std::string formatMessage(const char* format, va_list args);
    
    /**
     * Helper to extract filename from full path.
     * 
     * @param path Full file path
     * @return Filename only
     */
    static const char* extractFilename(const char* path);
};

} // namespace logging
} // namespace mssql_python

// Convenience macros for logging
// Single LOG() macro for all diagnostic logging (DEBUG level)

#define LOG(fmt, ...) \
    do { \
        if (mssql_python::logging::LoggerBridge::isLoggable(mssql_python::logging::LOG_LEVEL_DEBUG)) { \
            mssql_python::logging::LoggerBridge::log( \
                mssql_python::logging::LOG_LEVEL_DEBUG, __FILE__, __LINE__, fmt, ##__VA_ARGS__); \
        } \
    } while(0)

#define LOG_INFO(fmt, ...) \
    do { \
        if (mssql_python::logging::LoggerBridge::isLoggable(mssql_python::logging::LOG_LEVEL_INFO)) { \
            mssql_python::logging::LoggerBridge::log( \
                mssql_python::logging::LOG_LEVEL_INFO, __FILE__, __LINE__, fmt, ##__VA_ARGS__); \
        } \
    } while(0)

#define LOG_WARNING(fmt, ...) \
    do { \
        if (mssql_python::logging::LoggerBridge::isLoggable(mssql_python::logging::LOG_LEVEL_WARNING)) { \
            mssql_python::logging::LoggerBridge::log( \
                mssql_python::logging::LOG_LEVEL_WARNING, __FILE__, __LINE__, fmt, ##__VA_ARGS__); \
        } \
    } while(0)

#define LOG_ERROR(fmt, ...) \
    do { \
        if (mssql_python::logging::LoggerBridge::isLoggable(mssql_python::logging::LOG_LEVEL_ERROR)) { \
            mssql_python::logging::LoggerBridge::log( \
                mssql_python::logging::LOG_LEVEL_ERROR, __FILE__, __LINE__, fmt, ##__VA_ARGS__); \
        } \
    } while(0)

#endif // MSSQL_PYTHON_LOGGER_BRIDGE_HPP
