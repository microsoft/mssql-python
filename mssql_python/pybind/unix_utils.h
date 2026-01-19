// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

// This header defines utility functions for safely handling SQLWCHAR-based
// wide-character data in ODBC operations on macOS. It includes conversions
// between SQLWCHAR, std::wstring, and UTF-8 strings to bridge encoding
// differences specific to macOS.

#pragma once

#include <codecvt>
#include <locale>
#include <pybind11/pybind11.h>
#include <sql.h>
#include <sqlext.h>
#include <string>
#include <vector>

namespace py = pybind11;

#if defined(__APPLE__) || defined(__linux__)
// Constants for character encoding
extern const char* kOdbcEncoding;  // ODBC uses UTF-16LE for SQLWCHAR
extern const size_t kUcsLength;    // SQLWCHAR is 2 bytes on all platforms

// Function to convert SQLWCHAR strings to std::wstring on macOS
// Removed default argument to avoid redefinition conflict
std::wstring SQLWCHARToWString(const SQLWCHAR* sqlwStr, size_t length);

// Function to convert std::wstring to SQLWCHAR array on macOS
std::vector<SQLWCHAR> WStringToSQLWCHAR(const std::wstring& str);

#endif
