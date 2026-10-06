// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma once

#include "py_ref.hpp"

namespace RowFactory {

inline void initialize_row(
    const py::object& row, PyObject* row_data, const py::object& column_map,
    const py::object& cursor_obj, const py::object& column_map_lower,
    const py::object& column_names, const py::handle& attr_values, const py::handle& attr_column_map,
    const py::handle& attr_cursor, const py::handle& attr_column_map_lower,
    const py::handle& attr_column_names,
    int (*set_attr)(PyObject*, PyObject*, PyObject*) = PyObject_GenericSetAttr) {
    if (!row)
        throw py::error_already_set();

    if (set_attr(row.ptr(), attr_values.ptr(), row_data) < 0 ||
        set_attr(row.ptr(), attr_column_map.ptr(), column_map.ptr()) < 0 ||
        set_attr(row.ptr(), attr_cursor.ptr(), cursor_obj.ptr()) < 0 ||
        set_attr(row.ptr(), attr_column_map_lower.ptr(), column_map_lower.ptr()) < 0 ||
        set_attr(row.ptr(), attr_column_names.ptr(), column_names.ptr()) < 0) {
        throw py::error_already_set();
    }
}

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
        initialize_row(row, row_data, column_map, cursor_obj, column_map_lower, column_names,
                       attr_values, attr_column_map, attr_cursor, attr_column_map_lower,
                       attr_column_names);
        PyList_SET_ITEM(result.ptr(), i, row.release().ptr());
    }

    return result;
}

// Passive final eligibility check: do not execute newly installed class descriptors.
inline bool has_default_row_allocation(PyObject* row_class, const py::str& new_name,
                                       const py::str& setattr_name) {
    if (!PyUnicode_CheckExact(new_name.ptr()) || !PyUnicode_CheckExact(setattr_name.ptr())) {
        throw py::type_error("Row allocation guard names must be exact strings");
    }
    if (!row_class || Py_TYPE(row_class) != &PyType_Type) {
        return false;
    }
    auto* row_type = reinterpret_cast<PyTypeObject*>(row_class);
    if (!row_type->tp_bases || !PyTuple_CheckExact(row_type->tp_bases) ||
        PyTuple_GET_SIZE(row_type->tp_bases) != 1 ||
        PyTuple_GET_ITEM(row_type->tp_bases, 0) != reinterpret_cast<PyObject*>(&PyBaseObject_Type) ||
        !row_type->tp_dict) {
        return false;
    }
    PyObject* names[] = {new_name.ptr(), setattr_name.ptr()};
    for (PyObject* name : names) {
        PyObject* member = PyDict_GetItemWithError(row_type->tp_dict, name);
        if (member) {
            return false;
        }
        if (PyErr_Occurred()) {
            throw py::error_already_set();
        }
    }
    return true;
}

// Attribute names are prepared once as binding defaults, not allocated for each row.
inline py::object construct_row(const py::object& values, const py::type& row_class,
                                const py::object& column_map, const py::object& cursor_obj,
                                const py::object& column_map_lower, const py::object& column_names,
                                const py::tuple& attributes) {
    if (attributes.size() != 5) {
        throw py::value_error("Row construction requires five attribute names");
    }
    for (py::handle name : attributes) {
        if (!PyUnicode_Check(name.ptr())) {
            throw py::type_error("Row attribute names must be strings");
        }
    }
    const py::object new_method = row_class.attr("__new__");
    py::object row = steal(PyObject_CallOneArg(new_method.ptr(), row_class.ptr()));
    initialize_row(row, values.ptr(), column_map, cursor_obj, column_map_lower, column_names,
                   py::handle(PyTuple_GET_ITEM(attributes.ptr(), 0)),
                   py::handle(PyTuple_GET_ITEM(attributes.ptr(), 1)),
                   py::handle(PyTuple_GET_ITEM(attributes.ptr(), 2)),
                   py::handle(PyTuple_GET_ITEM(attributes.ptr(), 3)),
                   py::handle(PyTuple_GET_ITEM(attributes.ptr(), 4)), PyObject_SetAttr);
    return row;
}

}  // namespace RowFactory
