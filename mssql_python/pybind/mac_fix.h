#pragma once

#include <string>
#include <vector>
#include <locale>
#include <codecvt>
#include <sql.h>
#include <sqlext.h>
#include <pybind11/pybind11.h>

namespace py = pybind11;

#if defined(__APPLE__)
// Constants for character encoding
extern const char* kOdbcEncoding;  // ODBC uses UTF-16LE for SQLWCHAR
extern const size_t kUcsLength;    // SQLWCHAR is 2 bytes on all platforms

// Function to convert SQLWCHAR strings to std::wstring on macOS
// Removed default argument to avoid redefinition conflict
std::wstring SQLWCHARToWString(const SQLWCHAR* sqlwStr, size_t length);

// Function to convert std::wstring to SQLWCHAR array on macOS
std::vector<SQLWCHAR> WStringToSQLWCHAR(const std::wstring& str);

// This function can be used as a safe decoder for SQLWCHAR buffers
std::string SQLWCHARToUTF8String(const SQLWCHAR* buffer);

// Helper function to fix FetchBatchData for macOS
// This will process WCHAR data safely in SQLWCHARToUTF8String
void SafeProcessWCharData(SQLWCHAR* buffer, SQLLEN indicator, py::list& row);
#endif
