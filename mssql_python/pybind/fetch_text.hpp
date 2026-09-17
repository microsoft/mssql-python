// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma once

#include <Python.h>

#include "py_ref.hpp"

namespace FetchText {

inline py::object from_utf16_native(const char* data, Py_ssize_t size) {
    // ODBC buffers are native-endian; leading BOM-like code points are payload.
    int byteorder = PY_LITTLE_ENDIAN ? -1 : 1;
    py::object result = steal(PyUnicode_DecodeUTF16(data, size, nullptr, &byteorder));
    if (!result) throw py::error_already_set();
    return result;
}

}  // namespace FetchText
