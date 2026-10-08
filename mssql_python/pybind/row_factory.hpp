// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma once

#include "py_ref.hpp"

#include <utility>

namespace RowFactory {

// Only the post-fetch cached-map loop: no ODBC access, Row allocation, or UUID work.
inline py::list apply_output_converters(const py::object& values, const py::object& converters) {
    if (!PyList_CheckExact(values.ptr()) || !PyList_CheckExact(converters.ptr())) {
        throw py::type_error("converter values and map must be exact lists");
    }
    py::list result =
        steal<py::list>(PyList_GetSlice(values.ptr(), 0, PyList_GET_SIZE(values.ptr())));
    if (!result)
        throw py::error_already_set();

    // Keep the Python iterators: their retained tuples affect finalizer timing
    // when a callback replaces itself or mutates the source lists.
    py::object pairs = steal(PyObject_CallFunctionObjArgs(reinterpret_cast<PyObject*>(&PyZip_Type),
                                                          values.ptr(), converters.ptr(), nullptr));
    if (!pairs)
        throw py::error_already_set();
    py::object items =
        steal(PyObject_CallOneArg(reinterpret_cast<PyObject*>(&PyEnum_Type), pairs.ptr()));
    if (!items)
        throw py::error_already_set();
    pairs = py::object();

    // Retain the current inputs and last encoded value like the Python locals.
    py::object value, converter, value_bytes;
    for (Py_ssize_t i = 0;; ++i) {
        py::object item = steal(PyIter_Next(items.ptr()));
        if (!item) {
            if (PyErr_Occurred())
                throw py::error_already_set();
            break;
        }
        PyObject* pair = PyTuple_GET_ITEM(item.ptr(), 1);
        py::object next_value = borrow(PyTuple_GET_ITEM(pair, 0));
        py::object next_converter = borrow(PyTuple_GET_ITEM(pair, 1));
        value = std::move(next_value);
        converter = std::move(next_converter);
        item = py::object();
        const int enabled = PyObject_IsTrue(converter.ptr());
        if (enabled < 0)
            throw py::error_already_set();
        if (!enabled || value.is_none())
            continue;

        try {
            const int is_string =
                PyObject_IsInstance(value.ptr(), reinterpret_cast<PyObject*>(&PyUnicode_Type));
            if (is_string < 0)
                throw py::error_already_set();
            PyObject* argument = value.ptr();
            if (is_string) {
                // Match str.encode's codec lookup, including the spelling.
                // Subclasses and __class__ proxies still dispatch encode dynamically.
                py::object encoded =
                    steal(PyUnicode_CheckExact(value.ptr())
                              ? PyUnicode_AsEncodedString(value.ptr(), "utf-16-le", nullptr)
                              : PyObject_CallMethod(value.ptr(), "encode", "s", "utf-16-le"));
                if (!encoded)
                    throw py::error_already_set();
                value_bytes = std::move(encoded);
                argument = value_bytes.ptr();
            }
            py::object converted = steal(PyObject_CallOneArg(converter.ptr(), argument));
            if (!converted)
                throw py::error_already_set();
            // Checked assignment also preserves Python's caught IndexError if a
            // callback grows the input lists beyond the initial result copy.
            if (PyList_SetItem(result.ptr(), i, converted.release().ptr()) < 0)
                throw py::error_already_set();
        } catch (py::error_already_set& error) {
            if (!error.matches(PyExc_Exception))
                throw;
            // Preserve the existing cached path's keep-original-on-Exception contract.
            error.restore();
            PyErr_Clear();
        }
    }
    items = py::object();
    return result;
}

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
