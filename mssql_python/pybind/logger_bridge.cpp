/**
 * Copyright (c) Microsoft Corporation.
 * Licensed under the MIT license.
 * 
 * Logger Bridge Implementation
 */

#include "logger_bridge.hpp"
#include <cstring>
#include <cstdio>
#include <algorithm>
#include <vector>
#include <iostream>

namespace mssql_python {
namespace logging {

// Initialize static members
PyObject* LoggerBridge::cached_logger_ = nullptr;
std::atomic<int> LoggerBridge::cached_level_(LOG_LEVEL_CRITICAL);  // Disabled by default
std::mutex LoggerBridge::mutex_;
bool LoggerBridge::initialized_ = false;

void LoggerBridge::initialize() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Skip if already initialized (check inside lock to prevent TOCTOU race)
    if (initialized_) {
        return;
    }
    
    try {
        // Acquire GIL for Python API calls
        py::gil_scoped_acquire gil;
        
        // Import the logging module
        py::module_ logging_module = py::module_::import("mssql_python.logging");
        
        // Get the logger instance
        py::object logger_obj = logging_module.attr("logger");
        
        // Cache the logger object pointer
        // NOTE: We don't increment refcount because pybind11 py::object manages lifetime
        // and the logger is a module-level singleton that persists for program lifetime.
        // Adding Py_INCREF here would cause a memory leak since we never Py_DECREF.
        cached_logger_ = logger_obj.ptr();
        
        // Get initial log level
        py::object level_obj = logger_obj.attr("level");
        int level = level_obj.cast<int>();
        cached_level_.store(level, std::memory_order_relaxed);
        
        initialized_ = true;
        
    } catch (const py::error_already_set& e) {
        // Failed to initialize - log to stderr and continue
        // (logging will be disabled but won't crash)
        std::cerr << "LoggerBridge initialization failed: " << e.what() << std::endl;
        initialized_ = false;
    } catch (const std::exception& e) {
        std::cerr << "LoggerBridge initialization failed: " << e.what() << std::endl;
        initialized_ = false;
    }
}

void LoggerBridge::updateLevel(int level) {
    // Update the cached level atomically
    // This is lock-free and can be called from any thread
    cached_level_.store(level, std::memory_order_relaxed);
}

int LoggerBridge::getLevel() {
    return cached_level_.load(std::memory_order_relaxed);
}

bool LoggerBridge::isInitialized() {
    return initialized_;
}

std::string LoggerBridge::formatMessage(const char* format, va_list args) {
    // Use a stack buffer for most messages (4KB should be enough)
    char buffer[4096];
    
    // Format the message using safe std::vsnprintf (C++11 standard)
    // std::vsnprintf is safe: always null-terminates, never overflows buffer
    // DevSkim warning is false positive - this is the recommended safe alternative
    va_list args_copy;
    va_copy(args_copy, args);
    int result = std::vsnprintf(buffer, sizeof(buffer), format, args_copy);
    va_end(args_copy);
    
    if (result < 0) {
        // Error during formatting
        return "[Formatting error]";
    }
    
    if (result < static_cast<int>(sizeof(buffer))) {
        // Message fit in buffer (vsnprintf guarantees null-termination)
        return std::string(buffer, std::min(static_cast<size_t>(result), sizeof(buffer) - 1));
    }
    
    // Message was truncated - allocate larger buffer
    // (This should be rare for typical log messages)
    std::vector<char> large_buffer(result + 1);
    va_copy(args_copy, args);
    // std::vsnprintf is safe here too - proper bounds checking with buffer size
    std::vsnprintf(large_buffer.data(), large_buffer.size(), format, args_copy);
    va_end(args_copy);
    
    return std::string(large_buffer.data());
}

const char* LoggerBridge::extractFilename(const char* path) {
    // Extract just the filename from full path using safer C++ string search
    if (!path) {
        return "";
    }
    
    // Find last occurrence of Unix path separator
    const char* filename = std::strrchr(path, '/');
    if (filename) {
        return filename + 1;
    }
    
    // Try Windows path separator
    filename = std::strrchr(path, '\\');
    if (filename) {
        return filename + 1;
    }
    
    // No path separator found, return the whole string
    return path;
}

void LoggerBridge::log(int level, const char* file, int line, 
                      const char* format, ...) {
    // Fast level check (should already be done by macro, but double-check)
    if (!isLoggable(level)) {
        return;
    }
    
    // Check if initialized
    if (!initialized_ || !cached_logger_) {
        return;
    }
    
    // Format the message
    va_list args;
    va_start(args, format);
    std::string message = formatMessage(format, args);
    va_end(args);
    
    // Extract filename from path
    const char* filename = extractFilename(file);
    
    // Format the complete log message with [DDBC] prefix for CSV parsing
    // File and line number are handled by the Python formatter (in Location column)
    // std::snprintf is safe: always null-terminates, never overflows buffer
    // DevSkim warning is false positive - this is the recommended safe alternative
    char complete_message[4096];
    int written = std::snprintf(complete_message, sizeof(complete_message), 
                               "[DDBC] %s", message.c_str());
    
    // Ensure null-termination (snprintf guarantees this, but be explicit)
    if (written >= static_cast<int>(sizeof(complete_message))) {
        complete_message[sizeof(complete_message) - 1] = '\0';
    }
    
    // Lock for Python call (minimize critical section)
    std::lock_guard<std::mutex> lock(mutex_);
    
    try {
        // Acquire GIL for Python API call
        py::gil_scoped_acquire gil;
        
        // Get the logger object
        py::handle logger_handle(cached_logger_);
        py::object logger_obj = py::reinterpret_borrow<py::object>(logger_handle);
        
        // Get the underlying Python logger to create LogRecord with correct filename/lineno
        py::object py_logger = logger_obj.attr("_logger");
        
        // Call makeRecord to create a LogRecord with correct attributes
        py::object record = py_logger.attr("makeRecord")(
            py_logger.attr("name"),           // name
            py::int_(level),                  // level
            py::str(filename),                // pathname (just filename)
            py::int_(line),                   // lineno
            py::str(complete_message),        // msg
            py::tuple(),                      // args
            py::none(),                       // exc_info
            py::str(filename),                // func (use filename as func name)
            py::none()                        // extra
        );
        
        // Call handle() to process the record through filters and handlers
        py_logger.attr("handle")(record);
        
    } catch (const py::error_already_set& e) {
        // Python error during logging - ignore to prevent cascading failures
        // (Logging errors should not crash the application)
        (void)e;  // Suppress unused variable warning
    } catch (const std::exception& e) {
        // Other error - ignore
        (void)e;
    }
}

} // namespace logging
} // namespace mssql_python
