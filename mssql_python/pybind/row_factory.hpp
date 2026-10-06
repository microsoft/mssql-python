// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma once

#include "py_ref.hpp"

namespace RowFactory {

// Wrap fetched values without converter or UUID processing.
// Accepts Row and its subclasses, bypassing __init__.
inline py::list construct_rows(const py::list& rows_data, const py::object& row_class,
                               const py::object& column_map, const py::object& cursor_obj,
                               const py::object& column_map_lower, const py::object& column_names) {
    if (!PyType_Check(row_class.ptr())) {
        throw py::type_error("row_class must be a type");
    }
    PyTypeObject* row_type = reinterpret_cast<PyTypeObject*>(row_class.ptr());
    const py::object row_base = py::module_::import("mssql_python.row").attr("Row");
    if (!PyType_Check(row_base.ptr()) ||
        !PyType_IsSubtype(row_type, reinterpret_cast<PyTypeObject*>(row_base.ptr()))) {
        throw py::type_error("row_class must be Row or a Row subclass");
    }
    Py_ssize_t n = PyList_GET_SIZE(rows_data.ptr());

    // Keep Python-owned names local to this call and its interpreter.
    py::str attr_values("_values");
    py::str attr_column_map("_column_map");
    py::str attr_cursor("_cursor");
    py::str attr_column_map_lower("_column_map_lower");
    py::str attr_column_names("_column_names");

    py::list result(n);

    for (Py_ssize_t i = 0; i < n; ++i) {
        py::object row = steal(row_type->tp_alloc(row_type, 0));
        if (!row)
            throw py::error_already_set();

        PyObject* row_data = PyList_GET_ITEM(rows_data.ptr(), i);

        if (PyObject_GenericSetAttr(row.ptr(), attr_values.ptr(), row_data) < 0 ||
            PyObject_GenericSetAttr(row.ptr(), attr_column_map.ptr(), column_map.ptr()) < 0 ||
            PyObject_GenericSetAttr(row.ptr(), attr_cursor.ptr(), cursor_obj.ptr()) < 0 ||
            PyObject_GenericSetAttr(row.ptr(), attr_column_map_lower.ptr(),
                                    column_map_lower.ptr()) < 0 ||
            PyObject_GenericSetAttr(row.ptr(), attr_column_names.ptr(), column_names.ptr()) < 0) {
            throw py::error_already_set();
        }

        PyList_SET_ITEM(result.ptr(), i, row.release().ptr());
    }

    return result;
}

}  // namespace RowFactory
