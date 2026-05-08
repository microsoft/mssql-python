// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma once

#include <cstddef>
#include <cstring>
#include <simdutf.h>
#include <sql.h>
#include <sqlext.h>
#include <string>

inline std::string utf16LeToUtf8Alloc(const std::u16string& utf16) {
    if (utf16.empty()) {
        return {};
    }

    simdutf::result utf8Length =
        simdutf::utf8_length_from_utf16le_with_replacement(utf16.data(), utf16.size());
    std::string utf8(utf8Length.count, '\0');
    utf8.resize(
        simdutf::convert_utf16le_to_utf8_with_replacement(utf16.data(), utf16.size(), utf8.data()));
    return utf8;
}

inline std::u16string dupeSqlWCharAsUtf16Le(const SQLWCHAR* value, size_t length) {
    std::u16string utf16(length, u'\0');
    static_assert(sizeof(SQLWCHAR) == sizeof(char16_t), "SQLWCHAR must be 16-bit");

    if (length > 0) {
        std::memcpy(utf16.data(), value, length * sizeof(SQLWCHAR));
    }
    return utf16;
}

inline SQLWCHAR* reinterpretU16stringAsSqlWChar(const std::u16string& utf16) {
    static_assert(sizeof(std::u16string::value_type) == sizeof(SQLWCHAR),
        "SQLWCHAR must have the same size as std::u16string::value_type");
    static_assert(alignof(std::u16string::value_type) == alignof(SQLWCHAR),
        "SQLWCHAR must have the same alignment as std::u16string::value_type");
    return const_cast<SQLWCHAR*>(reinterpret_cast<const SQLWCHAR*>(utf16.c_str()));
}
