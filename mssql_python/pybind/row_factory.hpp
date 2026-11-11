// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma once

#include "py_ref.hpp"

namespace RowFactory {

// The caller validates the type. Both setters resolve inherited descriptors;
// fetchone selects ordinary assignment dispatch for mutations during construction.
template <setattrofunc SetAttribute = PyObject_GenericSetAttr>
inline py::object construct_row(const py::object& row_class, py::handle values,
                                 py::handle column_map, py::handle cursor,
                                 py::handle column_map_lower, py::handle column_names,
                                 const py::tuple& attribute_names) {
    auto* row_type = reinterpret_cast<PyTypeObject*>(row_class.ptr());
    py::object row = steal(row_type->tp_alloc(row_type, 0));
    if (!row)
        throw py::error_already_set();
    PyObject* fields[] = {values.ptr(), column_map.ptr(), cursor.ptr(),
                          column_map_lower.ptr(), column_names.ptr()};
    for (Py_ssize_t i = 0; i < 5; ++i) {
        if (SetAttribute(row.ptr(), PyTuple_GET_ITEM(attribute_names.ptr(), i),
                         fields[i]) < 0)
            throw py::error_already_set();
    }
    return row;
}

// Wrap fetched values without converter or UUID processing.
// Accepts Row and its subclasses, bypassing __init__.
inline py::list construct_rows(const py::list& rows_data, const py::object& row_class,
                               const py::object& column_map, const py::object& cursor_obj,
                               const py::object& column_map_lower, const py::object& column_names,
                               const py::tuple& attribute_names) {
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

    // Owned by this interpreter's binding, not by a process-static Python cache.
    PyObject* attr_values = PyTuple_GET_ITEM(attribute_names.ptr(), 0);
    PyObject* attr_column_map = PyTuple_GET_ITEM(attribute_names.ptr(), 1);
    PyObject* attr_cursor = PyTuple_GET_ITEM(attribute_names.ptr(), 2);
    PyObject* attr_column_map_lower = PyTuple_GET_ITEM(attribute_names.ptr(), 3);
    PyObject* attr_column_names = PyTuple_GET_ITEM(attribute_names.ptr(), 4);

    py::list result(n);

    for (Py_ssize_t i = 0; i < n; ++i) {
        py::object row = steal(row_type->tp_alloc(row_type, 0));
        if (!row)
            throw py::error_already_set();

        PyObject* row_data = PyList_GET_ITEM(rows_data.ptr(), i);

        if (PyObject_GenericSetAttr(row.ptr(), attr_values, row_data) < 0 ||
            PyObject_GenericSetAttr(row.ptr(), attr_column_map, column_map.ptr()) < 0 ||
            PyObject_GenericSetAttr(row.ptr(), attr_cursor, cursor_obj.ptr()) < 0 ||
            PyObject_GenericSetAttr(row.ptr(), attr_column_map_lower,
                                    column_map_lower.ptr()) < 0 ||
            PyObject_GenericSetAttr(row.ptr(), attr_column_names, column_names.ptr()) < 0) {
            throw py::error_already_set();
        }

        PyList_SET_ITEM(result.ptr(), i, row.release().ptr());
    }

    return result;
}

}  // namespace RowFactory
