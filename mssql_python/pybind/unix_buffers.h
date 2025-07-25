/**
 * Copyright (c) Microsoft Corporation.
 * Licensed under the MIT license.
 * 
 * This file provides utilities for handling character encoding and buffer management
 * specifically for macOS ODBC operations. It implements functionality similar to
 * the UCS_dec function in the Python PoC.
 */

#pragma once

#include <string>
#include <vector>
#include <memory>
#include <sql.h>
#include <sqlext.h>

namespace unix_buffers {

// Constants for Unicode character encoding
constexpr const char* ODBC_DECODING = "utf-16-le";
constexpr size_t UCS_LENGTH = 2;

/**
 * SQLWCHARBuffer class manages buffers for SQLWCHAR data,
 * handling memory allocation and conversion to std::wstring.
 */
class SQLWCHARBuffer {
private:
    std::unique_ptr<SQLWCHAR[]> buffer;
    size_t buffer_size;

public:
    /**
     * Constructor allocates a buffer of the specified size
     */
    SQLWCHARBuffer(size_t size) : buffer_size(size) {
        buffer = std::make_unique<SQLWCHAR[]>(size);
        // Initialize to zero
        for (size_t i = 0; i < size; i++) {
            buffer[i] = 0;
        }
    }

    /**
     * Returns the data pointer for use with ODBC functions
     */
    SQLWCHAR* data() {
        return buffer.get();
    }

    /**
     * Returns the size of the buffer
     */
    size_t size() const {
        return buffer_size;
    }

    /**
     * Converts the SQLWCHAR buffer to std::wstring
     * Similar to the UCS_dec function in the Python PoC
     */
    std::wstring toString(SQLSMALLINT length = -1) const {
        std::wstring result;
        
        // If length is provided, use it
        if (length > 0) {
            for (SQLSMALLINT i = 0; i < length; i++) {
                result.push_back(static_cast<wchar_t>(buffer[i]));
            }
            return result;
        }
        
        // Otherwise, read until null terminator
        for (size_t i = 0; i < buffer_size; i++) {
            if (buffer[i] == 0) {
                break;
            }
            result.push_back(static_cast<wchar_t>(buffer[i]));
        }
        
        return result;
    }
};

/**
 * Class to handle diagnostic records collection
 * Similar to the error list handling in the Python PoC _check_ret function
 */
class DiagnosticRecords {
private:
    struct Record {
        std::wstring sqlState;
        std::wstring message;
        SQLINTEGER nativeError;
    };
    
    std::vector<Record> records;

public:
    void addRecord(const std::wstring& sqlState, const std::wstring& message, SQLINTEGER nativeError) {
        records.push_back({sqlState, message, nativeError});
    }
    
    bool empty() const {
        return records.empty();
    }
    
    std::wstring getSQLState() const {
        if (!records.empty()) {
            return records[0].sqlState;
        }
        return L"HY000"; // General error
    }
    
    std::wstring getFirstErrorMessage() const {
        if (!records.empty()) {
            return records[0].message;
        }
        return L"Unknown error";
    }
    
    std::wstring getFullErrorMessage() const {
        if (records.empty()) {
            return L"No error information available";
        }
        
        std::wstring fullMessage = records[0].message;
        
        // Add additional error messages if there are any
        for (size_t i = 1; i < records.size(); i++) {
            fullMessage += L"; [" + records[i].sqlState + L"] " + records[i].message;
        }
        
        return fullMessage;
    }
    
    size_t size() const {
        return records.size();
    }
};

/**
 * Function that decodes a SQLWCHAR buffer into a std::wstring
 * Direct implementation of the UCS_dec logic from the Python PoC
 */
inline std::wstring UCS_dec(const SQLWCHAR* buffer, size_t maxLength = 0) {
    std::wstring result;
    size_t i = 0;
    
    while (true) {
        // Break if we've reached the maximum length
        if (maxLength > 0 && i >= maxLength) {
            break;
        }
        
        // Break if we've reached a null terminator
        if (buffer[i] == 0) {
            break;
        }
        
        result.push_back(static_cast<wchar_t>(buffer[i]));
        i++;
    }
    
    return result;
}

} // namespace unix_buffers
