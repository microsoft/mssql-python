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

// Unicode constants for validation
constexpr uint32_t kUnicodeReplacementChar = 0xFFFD;
constexpr uint32_t kUnicodeMaxCodePoint = 0x10FFFF;

// Constants for character encoding
const char* kOdbcEncoding = "utf-16-le";  // ODBC uses UTF-16LE for SQLWCHAR
const size_t kUcsLength = 2;              // SQLWCHAR is 2 bytes on all platforms

// Function to convert SQLWCHAR strings to std::wstring on macOS
// THREAD-SAFE: Uses thread_local converter to avoid std::wstring_convert race conditions
std::wstring SQLWCHARToWString(const SQLWCHAR* sqlwStr, size_t length = SQL_NTS) {
    if (!sqlwStr) {
        return std::wstring();
    }

    // Lambda to calculate string length using pointer arithmetic
    auto calculateLength = [](const SQLWCHAR* str) -> size_t {
        const SQLWCHAR* p = str;
        while (*p)
            ++p;
        return p - str;
    };

    if (length == SQL_NTS) {
        length = calculateLength(sqlwStr);
    }

    if (length == 0) {
        return std::wstring();
    }

    // Lambda to check if character is in Basic Multilingual Plane
    auto isBMP = [](uint16_t ch) { return ch < 0xD800 || ch > 0xDFFF; };

    // Lambda to decode surrogate pair into code point
    auto decodeSurrogatePair = [](uint16_t high, uint16_t low) -> uint32_t {
        return 0x10000 + (static_cast<uint32_t>(high & 0x3FF) << 10) + (low & 0x3FF);
    };

    // Convert UTF-16 to UTF-32 directly without intermediate buffer
    std::wstring result;
    result.reserve(length);  // Reserve assuming most chars are BMP

    size_t i = 0;
    while (i < length) {
        uint16_t utf16Char = static_cast<uint16_t>(sqlwStr[i]);

        // Fast path: BMP character (most common - ~99% of strings)
        if (isBMP(utf16Char)) {
            result.push_back(static_cast<wchar_t>(utf16Char));
            ++i;
        }
        // Handle surrogate pairs for characters outside BMP
        else if (utf16Char <= 0xDBFF) {  // High surrogate
            if (i + 1 < length) {
                uint16_t lowSurrogate = static_cast<uint16_t>(sqlwStr[i + 1]);
                if (lowSurrogate >= 0xDC00 && lowSurrogate <= 0xDFFF) {
                    uint32_t codePoint = decodeSurrogatePair(utf16Char, lowSurrogate);
                    result.push_back(static_cast<wchar_t>(codePoint));
                    i += 2;
                    continue;
                }
            }
            // Invalid surrogate - replace with Unicode replacement character
            result.push_back(static_cast<wchar_t>(kUnicodeReplacementChar));
            ++i;
        } else {  // Low surrogate without high - invalid, replace with replacement character
            result.push_back(static_cast<wchar_t>(kUnicodeReplacementChar));
            ++i;
        }
    }
    return result;
}

// Function to convert std::wstring to SQLWCHAR array on macOS/Linux
// Converts UTF-32 (wstring on Unix) to UTF-16 (SQLWCHAR)
// Invalid Unicode scalars (surrogates, values > 0x10FFFF) are replaced with U+FFFD
std::vector<SQLWCHAR> WStringToSQLWCHAR(const std::wstring& str) {
    if (str.empty()) {
        return std::vector<SQLWCHAR>(1, 0);  // Just null terminator
    }

    // Lambda to encode code point as surrogate pair and append to result
    auto encodeSurrogatePair = [](std::vector<SQLWCHAR>& vec, uint32_t cp) {
        cp -= 0x10000;
        vec.push_back(static_cast<SQLWCHAR>(0xD800 | ((cp >> 10) & 0x3FF)));
        vec.push_back(static_cast<SQLWCHAR>(0xDC00 | (cp & 0x3FF)));
    };

    // Lambda to check if code point is a valid Unicode scalar value
    auto isValidUnicodeScalar = [](uint32_t cp) -> bool {
        // Exclude surrogate range (0xD800-0xDFFF) and values beyond max Unicode
        return cp <= kUnicodeMaxCodePoint && (cp < 0xD800 || cp > 0xDFFF);
    };

    // Convert wstring (UTF-32) to UTF-16
    std::vector<SQLWCHAR> result;
    result.reserve(str.size() + 1);  // Most chars are BMP, so reserve exact size

    for (wchar_t wc : str) {
        uint32_t codePoint = static_cast<uint32_t>(wc);

        // Validate code point first
        if (!isValidUnicodeScalar(codePoint)) {
            codePoint = kUnicodeReplacementChar;
        }

        // Fast path: BMP character (most common - ~99% of strings)
        // After validation, codePoint cannot be in surrogate range (0xD800-0xDFFF)
        if (codePoint <= 0xFFFF) {
            result.push_back(static_cast<SQLWCHAR>(codePoint));
        }
        // Encode as surrogate pair for characters outside BMP
        else if (codePoint <= kUnicodeMaxCodePoint) {
            encodeSurrogatePair(result, codePoint);
        }
        // Note: Invalid code points (surrogates and > 0x10FFFF) already
        // replaced with replacement character (0xFFFD) at validation above
    }

    result.push_back(0);  // Null terminator
    return result;
}

#endif
