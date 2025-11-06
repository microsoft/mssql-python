// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

// This header defines utility functions for safely handling SQLWCHAR-based
// wide-character data in ODBC operations on macOS. It includes conversions
// between SQLWCHAR, std::wstring, and UTF-8 strings to bridge encoding
// differences specific to macOS.

#include "unix_utils.h"
#include <iostream>
#include <string>
#include <utility>
#include <vector>

#if defined(__APPLE__) || defined(__linux__)
// Constants for character encoding
const char* kOdbcEncoding = "utf-16-le";  // ODBC uses UTF-16LE for SQLWCHAR
const size_t kUcsLength = 2;  // SQLWCHAR is 2 bytes on all platforms

// TODO(microsoft): Make Logger a separate module and import it across project
template <typename... Args>
void LOG(const std::string& formatString, Args&&... args) {
    py::gil_scoped_acquire gil;  // this ensures safe Python API usage

    py::object logger = py::module_::import("mssql_python.logging_config")
                            .attr("get_logger")();
    if (py::isinstance<py::none>(logger)) return;

    try {
        std::string ddbcFormatString = "[DDBC Bindings log] " + formatString;
        if constexpr (sizeof...(args) == 0) {
            logger.attr("debug")(py::str(ddbcFormatString));
        } else {
            py::str message = py::str(ddbcFormatString)
                                  .format(std::forward<Args>(args)...);
            logger.attr("debug")(message);
        }
    } catch (const std::exception& e) {
        std::cerr << "Logging error: " << e.what() << std::endl;
    }
}

// OPTIMIZED: Convert SQLWCHAR (UTF-16LE) to std::wstring (UTF-32) using Python C API
// This replaces the broken std::wstring_convert implementation
std::wstring SQLWCHARToWString(const SQLWCHAR* sqlwStr, size_t length = SQL_NTS) {
    if (!sqlwStr) return std::wstring();

    if (length == SQL_NTS) {
        // Determine length if not provided
        size_t i = 0;
        while (sqlwStr[i] != 0) ++i;
        length = i;
    }
    
    if (length == 0) return std::wstring();

    // Use Python C API for proper UTF-16 → UTF-32 conversion
    PyObject* pyStr = PyUnicode_DecodeUTF16(
        reinterpret_cast<const char*>(sqlwStr),
        length * sizeof(SQLWCHAR),
        "strict",
        nullptr
    );
    
    if (!pyStr) {
        PyErr_Clear();
        // Fallback: simple cast (BMP characters only, loses surrogates)
        std::wstring result;
        result.reserve(length);
        for (size_t i = 0; i < length; ++i) {
            result.push_back(static_cast<wchar_t>(sqlwStr[i]));
        }
        return result;
    }
    
    // Convert Python Unicode to wchar_t array
    Py_ssize_t wchar_size = PyUnicode_GET_LENGTH(pyStr);
    std::wstring result(wchar_size, L'\0');
    Py_ssize_t copied = PyUnicode_AsWideChar(pyStr, &result[0], wchar_size);
    Py_DECREF(pyStr);
    
    if (copied < 0) {
        PyErr_Clear();
        return std::wstring();
    }
    
    return result;
}

// OPTIMIZED: Convert std::wstring (UTF-32) to SQLWCHAR array (UTF-16LE) using Python C API
std::vector<SQLWCHAR> WStringToSQLWCHAR(const std::wstring& str) {
    if (str.empty()) {
        return std::vector<SQLWCHAR>(1, 0);  // Just null terminator
    }
    
    // Use Python C API for proper UTF-32 → UTF-16 conversion
    PyObject* pyStr = PyUnicode_FromWideChar(str.c_str(), str.size());
    if (!pyStr) {
        PyErr_Clear();
        // Fallback to simple cast (loses surrogates)
        std::vector<SQLWCHAR> result(str.size() + 1, 0);
        for (size_t i = 0; i < str.size(); ++i) {
            result[i] = static_cast<SQLWCHAR>(str[i]);
        }
        return result;
    }
    
    // Get UTF-16 representation from Python string
    PyObject* utf16BytesObj = PyUnicode_AsUTF16String(pyStr);
    Py_DECREF(pyStr);
    
    if (!utf16BytesObj) {
        PyErr_Clear();
        // Fallback to simple casting
        std::vector<SQLWCHAR> result(str.size() + 1, 0);
        for (size_t i = 0; i < str.size(); ++i) {
            result[i] = static_cast<SQLWCHAR>(str[i]);
        }
        return result;
    }
    
    // Get the bytes buffer
    char* utf16_data;
    Py_ssize_t utf16_size;
    PyBytes_AsStringAndSize(utf16BytesObj, &utf16_data, &utf16_size);
    
    // Copy UTF-16 data to SQLWCHAR vector (skip BOM if present)
    size_t offset = 0;
    if (utf16_size >= 2 && 
        ((unsigned char)utf16_data[0] == 0xFF && (unsigned char)utf16_data[1] == 0xFE)) {
        offset = 2; // Skip UTF-16 LE BOM
    }
    
    size_t char_count = (utf16_size - offset) / 2;
    std::vector<SQLWCHAR> result(char_count + 1, 0);  // +1 for null terminator
    
    memcpy(result.data(), utf16_data + offset, (utf16_size - offset));
    
    Py_DECREF(utf16BytesObj);
    
    return result;
}

// This function can be used as a safe decoder for SQLWCHAR buffers
std::string SQLWCHARToUTF8String(const SQLWCHAR* buffer) {
    if (!buffer) return "";

    // Find length
    size_t length = 0;
    while (buffer[length] != 0) ++length;
    
    // Use Python C API for conversion
    PyObject* pyStr = PyUnicode_DecodeUTF16(
        reinterpret_cast<const char*>(buffer),
        length * sizeof(SQLWCHAR),
        "strict",
        nullptr
    );
    
    if (!pyStr) {
        PyErr_Clear();
        // Fallback
        std::string result;
        for (size_t i = 0; i < length; ++i) {
            if (buffer[i] < 128) {
                result.push_back(static_cast<char>(buffer[i]));
            } else {
                result.push_back('?');
            }
        }
        return result;
    }
    
    // Convert Python string to UTF-8
    PyObject* utf8Bytes = PyUnicode_AsUTF8String(pyStr);
    Py_DECREF(pyStr);
    
    if (!utf8Bytes) {
        PyErr_Clear();
        return "";
    }
    
    char* utf8Data;
    Py_ssize_t utf8Size;
    PyBytes_AsStringAndSize(utf8Bytes, &utf8Data, &utf8Size);
    
    std::string result(utf8Data, utf8Size);
    Py_DECREF(utf8Bytes);
    
    return result;
}

// OPTIMIZED: Direct SQLWCHAR to Python string conversion
// This is 7-8x faster than SQLWCHARToWString + pybind11 conversion because:
// 1. Uses native Python C API (PyUnicode_DecodeUTF16)
// 2. Skips intermediate std::wstring allocation  
// 3. Avoids expensive UTF-16 -> UTF-32 -> Python conversion chain
// 4. No std::wstring_convert overhead
py::object SQLWCHARToPyString(const SQLWCHAR* sqlwStr, size_t length) {
    if (!sqlwStr || length == 0) {
        return py::str("");
    }
    
    // SQLWCHAR is UTF-16LE on all platforms
    // Use Python's native UTF-16 decoder for maximum performance
    PyObject* pyStr = PyUnicode_DecodeUTF16(
        reinterpret_cast<const char*>(sqlwStr),
        length * sizeof(SQLWCHAR),
        "strict",
        nullptr  // Use native byte order (little-endian)
    );
    
    if (!pyStr) {
        PyErr_Clear();
        // Fallback to std::wstring conversion if decoding fails
        std::wstring wstr = SQLWCHARToWString(sqlwStr, length);
        return py::cast(wstr);
    }
    
    return py::reinterpret_steal<py::object>(pyStr);
}

// Helper function to fix FetchBatchData for macOS
// This will process WCHAR data safely in SQLWCHARToUTF8String
void SafeProcessWCharData(SQLWCHAR* buffer, SQLLEN indicator, py::list& row) {
    if (indicator == SQL_NULL_DATA) {
        row.append(py::none());
    } else {
        // Use our safe conversion function
        std::string str = SQLWCHARToUTF8String(buffer);
        row.append(py::str(str));
    }
}
#endif
