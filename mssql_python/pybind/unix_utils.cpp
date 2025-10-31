// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

// This header defines utility functions for safely handling SQLWCHAR-based
// wide-character data in ODBC operations on macOS. It includes conversions
// between SQLWCHAR, std::wstring, and UTF-8 strings to bridge encoding
// differences specific to macOS.

#include "unix_utils.h"
#include "logger_bridge.hpp"
#include <iostream>
#include <string>
#include <utility>
#include <vector>

#if defined(__APPLE__) || defined(__linux__)
// Constants for character encoding
const char* kOdbcEncoding = "utf-16-le";  // ODBC uses UTF-16LE for SQLWCHAR
const size_t kUcsLength = 2;  // SQLWCHAR is 2 bytes on all platforms

// OLD LOG() calls temporarily disabled - migrate to LOG_FINER/LOG_FINE/LOG_FINEST
#define LOG(...) do {} while(0)

// Function to convert SQLWCHAR strings to std::wstring on macOS
std::wstring SQLWCHARToWString(const SQLWCHAR* sqlwStr,
                               size_t length = SQL_NTS) {
    if (!sqlwStr) {
        LOG_FINEST("SQLWCHARToWString: NULL input - returning empty wstring");
        return std::wstring();
    }

    if (length == SQL_NTS) {
        // Determine length if not provided
        size_t i = 0;
        while (sqlwStr[i] != 0) ++i;
        length = i;
        LOG_FINEST("SQLWCHARToWString: Length determined - length=%zu", length);
    } else {
        LOG_FINEST("SQLWCHARToWString: Using provided length=%zu", length);
    }

    // Create a UTF-16LE byte array from the SQLWCHAR array
    std::vector<char> utf16Bytes(length * kUcsLength);
    for (size_t i = 0; i < length; ++i) {
        // Copy each SQLWCHAR (2 bytes) to the byte array
        memcpy(&utf16Bytes[i * kUcsLength], &sqlwStr[i], kUcsLength);
    }
    LOG_FINEST("SQLWCHARToWString: UTF-16LE byte array created - byte_count=%zu", utf16Bytes.size());

    // Convert UTF-16LE to std::wstring (UTF-32 on macOS)
    try {
        // Use C++11 codecvt to convert between UTF-16LE and wstring
        std::wstring_convert<std::codecvt_utf8_utf16<wchar_t, 0x10ffff,
                                                     std::little_endian>>
            converter;
        std::wstring result = converter.from_bytes(
            reinterpret_cast<const char*>(utf16Bytes.data()),
            reinterpret_cast<const char*>(utf16Bytes.data() +
                                          utf16Bytes.size()));
        LOG_FINEST("SQLWCHARToWString: Conversion successful - input_len=%zu, result_len=%zu", 
                  length, result.size());
        return result;
    } catch (const std::exception& e) {
        // Fallback to character-by-character conversion if codecvt fails
        LOG_FINER("SQLWCHARToWString: codecvt failed (%s), using fallback - length=%zu", e.what(), length);
        std::wstring result;
        result.reserve(length);
        for (size_t i = 0; i < length; ++i) {
            result.push_back(static_cast<wchar_t>(sqlwStr[i]));
        }
        LOG_FINEST("SQLWCHARToWString: Fallback conversion complete - result_len=%zu", result.size());
        return result;
    }
}

// Function to convert std::wstring to SQLWCHAR array on macOS
std::vector<SQLWCHAR> WStringToSQLWCHAR(const std::wstring& str) {
    LOG_FINEST("WStringToSQLWCHAR: Starting conversion - input_len=%zu", str.size());
    try {
        // Convert wstring (UTF-32 on macOS) to UTF-16LE bytes
        std::wstring_convert<std::codecvt_utf8_utf16<wchar_t, 0x10ffff,
                                                     std::little_endian>>
            converter;
        std::string utf16Bytes = converter.to_bytes(str);
        LOG_FINEST("WStringToSQLWCHAR: UTF-16LE byte conversion successful - byte_count=%zu", utf16Bytes.size());

        // Convert the bytes to SQLWCHAR array
        std::vector<SQLWCHAR> result(utf16Bytes.size() / kUcsLength + 1,
                                     0);  // +1 for null terminator
        for (size_t i = 0; i < utf16Bytes.size() / kUcsLength; ++i) {
            memcpy(&result[i], &utf16Bytes[i * kUcsLength], kUcsLength);
        }
        LOG_FINEST("WStringToSQLWCHAR: Conversion complete - result_size=%zu (includes null terminator)", result.size());
        return result;
    } catch (const std::exception& e) {
        // Fallback to simple casting if codecvt fails
        LOG_FINER("WStringToSQLWCHAR: codecvt failed (%s), using fallback - input_len=%zu", e.what(), str.size());
        std::vector<SQLWCHAR> result(str.size() + 1,
                                     0);  // +1 for null terminator
        for (size_t i = 0; i < str.size(); ++i) {
            result[i] = static_cast<SQLWCHAR>(str[i]);
        }
        LOG_FINEST("WStringToSQLWCHAR: Fallback conversion complete - result_size=%zu", result.size());
        return result;
    }
}

// This function can be used as a safe decoder for SQLWCHAR buffers
// based on your ctypes UCS_dec implementation
std::string SQLWCHARToUTF8String(const SQLWCHAR* buffer) {
    if (!buffer) {
        LOG_FINEST("SQLWCHARToUTF8String: NULL buffer - returning empty string");
        return "";
    }

    std::vector<char> utf16Bytes;
    size_t i = 0;
    while (buffer[i] != 0) {
        char bytes[kUcsLength];
        memcpy(bytes, &buffer[i], kUcsLength);
        utf16Bytes.push_back(bytes[0]);
        utf16Bytes.push_back(bytes[1]);
        i++;
    }
    LOG_FINEST("SQLWCHARToUTF8String: UTF-16 bytes collected - char_count=%zu, byte_count=%zu", i, utf16Bytes.size());

    try {
        std::wstring_convert<std::codecvt_utf8_utf16<char16_t, 0x10ffff,
                                                     std::little_endian>>
            converter;
        std::string result = converter.to_bytes(
            reinterpret_cast<const char16_t*>(utf16Bytes.data()),
            reinterpret_cast<const char16_t*>(utf16Bytes.data() +
                                              utf16Bytes.size()));
        LOG_FINEST("SQLWCHARToUTF8String: UTF-8 conversion successful - input_chars=%zu, output_bytes=%zu", 
                  i, result.size());
        return result;
    } catch (const std::exception& e) {
        // Simple fallback conversion
        LOG_FINER("SQLWCHARToUTF8String: codecvt failed (%s), using ASCII fallback - char_count=%zu", e.what(), i);
        std::string result;
        size_t non_ascii_count = 0;
        for (size_t j = 0; j < i; ++j) {
            if (buffer[j] < 128) {
                result.push_back(static_cast<char>(buffer[j]));
            } else {
                result.push_back('?');  // Placeholder for non-ASCII chars
                non_ascii_count++;
            }
        }
        LOG_FINER("SQLWCHARToUTF8String: Fallback complete - output_bytes=%zu, non_ascii_replaced=%zu", 
                 result.size(), non_ascii_count);
        return result;
    }
}

// Helper function to fix FetchBatchData for macOS
// This will process WCHAR data safely in SQLWCHARToUTF8String
void SafeProcessWCharData(SQLWCHAR* buffer, SQLLEN indicator, py::list& row) {
    if (indicator == SQL_NULL_DATA) {
        LOG_FINEST("SafeProcessWCharData: NULL data - appending None");
        row.append(py::none());
    } else {
        // Use our safe conversion function
        LOG_FINEST("SafeProcessWCharData: Converting WCHAR data - indicator=%lld", static_cast<long long>(indicator));
        std::string str = SQLWCHARToUTF8String(buffer);
        row.append(py::str(str));
        LOG_FINEST("SafeProcessWCharData: String appended - length=%zu", str.size());
    }
}
#endif
