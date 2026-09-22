// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma once

#include <Python.h>

#include "py_ref.hpp"

namespace FetchText {

// Returns a new reference, or nullptr with the Python error left for the caller.
inline PyObject* decode_utf16_native(const char* data, Py_ssize_t size) {
    // ODBC buffers are native-endian; leading BOM-like code points are payload.
    int byteorder = PY_LITTLE_ENDIAN ? -1 : 1;
    return PyUnicode_DecodeUTF16(data, size, nullptr, &byteorder);
}

inline py::object from_utf16_native(const char* data, Py_ssize_t size) {
    py::object result = steal(decode_utf16_native(data, size));
    if (!result) throw py::error_already_set();
    return result;
}

}  // namespace FetchText
