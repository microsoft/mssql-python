// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

// This header defines utility functions for safely handling SQLWCHAR-based
// wide-character data in ODBC operations on macOS. It includes conversions
// between SQLWCHAR, std::wstring, and UTF-8 strings to bridge encoding
// differences specific to macOS.

#include "mssql_python/pybind/unix_utils.h"
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

// Function to convert SQLWCHAR strings to std::wstring on macOS
std::wstring SQLWCHARToWString(const SQLWCHAR* sqlwStr,
                               size_t length = SQL_NTS) {
    if (!sqlwStr) return std::wstring();

    if (length == SQL_NTS) {
        // Determine length if not provided
        size_t i = 0;
        while (sqlwStr[i] != 0) ++i;
        length = i;
    }

    // Create a UTF-16LE byte array from the SQLWCHAR array
    std::vector<char> utf16Bytes(length * kUcsLength);
    for (size_t i = 0; i < length; ++i) {
        // Copy each SQLWCHAR (2 bytes) to the byte array
        memcpy(&utf16Bytes[i * kUcsLength], &sqlwStr[i], kUcsLength);
    }

    // Convert UTF-16LE to std::wstring (UTF-32 on macOS)
    try {
        // Use C++11 codecvt to convert between UTF-16LE and wstring
        std::wstring_convert<std::codecvt_utf8_utf16<wchar_t, 0x10ffff,
                                                     std::little_endian>>
            converter;
        return converter.from_bytes(
            reinterpret_cast<const char*>(utf16Bytes.data()),
            reinterpret_cast<const char*>(utf16Bytes.data() +
                                          utf16Bytes.size()));
    } catch (const std::exception& e) {
        // Log a warning about using fallback conversion
        LOG("Warning: Using fallback string conversion on macOS. "
            "Character data might be inexact.");
        // Fallback to character-by-character conversion if codecvt fails
        std::wstring result;
        result.reserve(length);
        for (size_t i = 0; i < length; ++i) {
            result.push_back(static_cast<wchar_t>(sqlwStr[i]));
        }
        return result;
    }
}

// Function to convert std::wstring to SQLWCHAR array on macOS
std::vector<SQLWCHAR> WStringToSQLWCHAR(const std::wstring& str) {
    try {
        // Convert wstring (UTF-32 on macOS) to UTF-16LE bytes
        std::wstring_convert<std::codecvt_utf8_utf16<wchar_t, 0x10ffff,
                                                     std::little_endian>>
            converter;
        std::string utf16Bytes = converter.to_bytes(str);

        // Convert the bytes to SQLWCHAR array
        std::vector<SQLWCHAR> result(utf16Bytes.size() / kUcsLength + 1,
                                     0);  // +1 for null terminator
        for (size_t i = 0; i < utf16Bytes.size() / kUcsLength; ++i) {
            memcpy(&result[i], &utf16Bytes[i * kUcsLength], kUcsLength);
        }
        return result;
    } catch (const std::exception& e) {
        // Log a warning about using fallback conversion
        LOG("Warning: Using fallback conversion for std::wstring to "
            "SQLWCHAR on macOS. Character data might be inexact.");
        // Fallback to simple casting if codecvt fails
        std::vector<SQLWCHAR> result(str.size() + 1,
                                     0);  // +1 for null terminator
        for (size_t i = 0; i < str.size(); ++i) {
            result[i] = static_cast<SQLWCHAR>(str[i]);
        }
        return result;
    }
}

// This function can be used as a safe decoder for SQLWCHAR buffers
// based on your ctypes UCS_dec implementation
std::string SQLWCHARToUTF8String(const SQLWCHAR* buffer) {
    if (!buffer) return "";

    std::vector<char> utf16Bytes;
    size_t i = 0;
    while (buffer[i] != 0) {
        char bytes[kUcsLength];
        memcpy(bytes, &buffer[i], kUcsLength);
        utf16Bytes.push_back(bytes[0]);
        utf16Bytes.push_back(bytes[1]);
        i++;
    }

    try {
        std::wstring_convert<std::codecvt_utf8_utf16<char16_t, 0x10ffff,
                                                     std::little_endian>>
            converter;
        return converter.to_bytes(
            reinterpret_cast<const char16_t*>(utf16Bytes.data()),
            reinterpret_cast<const char16_t*>(utf16Bytes.data() +
                                              utf16Bytes.size()));
    } catch (const std::exception& e) {
        // Log a warning about using fallback conversion
        LOG("Warning: Using fallback conversion for SQLWCHAR to UTF-8 "
            "on macOS. Character data might be inexact.");
        // Simple fallback conversion
        std::string result;
        for (size_t j = 0; j < i; ++j) {
            if (buffer[j] < 128) {
                result.push_back(static_cast<char>(buffer[j]));
            } else {
                result.push_back('?');  // Placeholder for non-ASCII chars
            }
        }
        return result;
    }
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
