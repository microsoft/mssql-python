// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma once

#include "fetch_text.hpp"
#include "py_ref.hpp"

#include <array>
#include <utility>

namespace OutputConverters {

inline constexpr const char* text_capsule_name = "mssql_python.output_converter.utf16le";

inline bool enabled(py::handle converters, Py_ssize_t column) {
    return converters && PyList_CheckExact(converters.ptr()) &&
           column < PyList_GET_SIZE(converters.ptr()) &&
           PyList_GET_ITEM(converters.ptr(), column) != Py_None;
}

// The capsule owns callback-ready bytes, not a view of an ODBC buffer. Only
// well-formed text takes this path; malformed data retains each fetcher's
// existing decoding/error behavior.
inline py::object text(const char* data, Py_ssize_t size) {
    if (!PY_LITTLE_ENDIAN || size < 0 || size % 2 != 0) return {};
    const auto* bytes = reinterpret_cast<const unsigned char*>(data);
    for (Py_ssize_t i = 0; i < size; i += 2) {
        unsigned int unit = bytes[i] | (bytes[i + 1] << 8);
        if (unit >= 0xD800 && unit <= 0xDBFF) {
            if (i + 3 >= size) return {};
            unsigned int next = bytes[i + 2] | (bytes[i + 3] << 8);
            if (next < 0xDC00 || next > 0xDFFF) return {};
            i += 2;
        } else if (unit >= 0xDC00 && unit <= 0xDFFF) {
            return {};
        }
    }
    py::object raw = steal(PyBytes_FromStringAndSize(data, size));
    if (!raw) throw py::error_already_set();
    py::object result = steal(PyCapsule_New(raw.ptr(), text_capsule_name, [](PyObject* capsule) {
        Py_DECREF(static_cast<PyObject*>(PyCapsule_GetPointer(capsule, text_capsule_name)));
    }));
    if (!result) throw py::error_already_set();
    raw.release();
    return result;
}

inline PyObject* raw_text(py::handle value) {
    return PyCapsule_IsValid(value.ptr(), text_capsule_name)
               ? static_cast<PyObject*>(PyCapsule_GetPointer(value.ptr(), text_capsule_name))
               : nullptr;
}

inline py::object original(py::handle value) {
    PyObject* raw = raw_text(value);
    if (!raw) return borrow(value.ptr());
    return FetchText::from_utf16_native(PyBytes_AS_STRING(raw), PyBytes_GET_SIZE(raw));
}

inline py::list restore(py::list values) {
    for (Py_ssize_t i = 0; i < PyList_GET_SIZE(values.ptr()); ++i) {
        if (raw_text(PyList_GET_ITEM(values.ptr(), i))) {
            py::object value = original(py::handle(PyList_GET_ITEM(values.ptr(), i)));
            if (PyList_SetItem(values.ptr(), i, value.release().ptr()) < 0)
                throw py::error_already_set();
        }
    }
    return values;
}

inline py::list apply(py::list values, py::list converters) {
    py::object result = steal(PyList_GetSlice(values.ptr(), 0, PyList_GET_SIZE(values.ptr())));
    if (!result) throw py::error_already_set();
    Py_ssize_t i = 0;
    for (; i < PyList_GET_SIZE(values.ptr()) && i < PyList_GET_SIZE(converters.ptr()); ++i) {
        py::object value = borrow(PyList_GET_ITEM(values.ptr(), i));
        py::object converter = borrow(PyList_GET_ITEM(converters.ptr(), i));
        int active = PyObject_IsTrue(converter.ptr());
        if (active < 0) throw py::error_already_set();
        bool replaced = false;
        if (active && !value.is_none()) {
            try {
                PyObject* raw = raw_text(value);
                py::object argument;
                if (raw) {
                    argument = borrow(raw);
                } else if (PyUnicode_CheckExact(value.ptr())) {
                    argument = steal(PyUnicode_AsEncodedString(value.ptr(), "utf-16-le", "strict"));
                } else if (PyUnicode_Check(value.ptr())) {
                    argument = steal(PyObject_CallMethod(value.ptr(), "encode", "s", "utf-16-le"));
                } else {
                    argument = value;
                }
                if (!argument) throw py::error_already_set();
                py::object converted = steal(PyObject_CallOneArg(converter.ptr(), argument.ptr()));
                if (!converted) throw py::error_already_set();
                if (PyList_SetItem(result.ptr(), i, converted.release().ptr()) < 0)
                    throw py::error_already_set();
                replaced = true;
            } catch (py::error_already_set& error) {
                // Match Row's existing Exception fallback, but not BaseException.
                if (!error.matches(PyExc_Exception)) throw;
                error.restore();
                PyErr_Clear();
            }
        }
        if (!replaced && i < PyList_GET_SIZE(result.ptr()) &&
            raw_text(PyList_GET_ITEM(result.ptr(), i))) {
            py::object restored = original(py::handle(PyList_GET_ITEM(result.ptr(), i)));
            if (PyList_SetItem(result.ptr(), i, restored.release().ptr()) < 0)
                throw py::error_already_set();
        }
    }
    for (; i < PyList_GET_SIZE(result.ptr()); ++i) {
        if (raw_text(PyList_GET_ITEM(result.ptr(), i))) {
            py::object restored = original(py::handle(PyList_GET_ITEM(result.ptr(), i)));
            if (PyList_SetItem(result.ptr(), i, restored.release().ptr()) < 0)
                throw py::error_already_set();
        }
    }
    return py::reinterpret_borrow<py::list>(result);
}

// Python owns this object's references. Unlike a static type cache, its lifetime
// is bounded by the importing interpreter. Attribute writes still use normal
// descriptor dispatch (including descriptors inherited from a native Row base).
#if defined(__GNUG__) && !defined(_WIN32)
class __attribute__((visibility("hidden"))) RowBuilder {
#else
class RowBuilder {
#endif
    py::object row_class_;
    py::object bindings_;
    py::object apply_;
    std::array<py::str, 5> hook_names_{
        py::str("__init__"), py::str("_apply_output_converters_optimized"),
        py::str("__new__"), py::str("__getattribute__"), py::str("__setattr__")};
    std::array<py::object, 5> hooks_;
    std::array<py::object, 2> code_;
    std::array<py::str, 5> field_names_{
        py::str("_values"), py::str("_column_map"), py::str("_cursor"),
        py::str("_column_map_lower"), py::str("_column_names")};
    py::str apply_name_{"apply_output_converters"};
    py::str row_name_{"Row"};
    unsigned int verified_version_ = 0;

    py::object construct(py::list values, py::object row_class, py::object converters,
                         py::object column_map, py::object cursor, py::object uuid_indices,
                         py::object lower_map, py::object names) {
        // Do not mutate even private-looking lists: a native bridge wrapper may
        // retain them. The original-value snapshot is also needed after callback
        // failures and in-place edits to the converter map.
        if (matches(row_class) && uuid_indices.is_none() &&
            PyList_CheckExact(values.ptr()) && PyList_CheckExact(converters.ptr()) &&
            PyList_GET_SIZE(converters.ptr()) != 0) {
            auto* type = reinterpret_cast<PyTypeObject*>(row_class.ptr());
            // A native base with a custom allocator/new must keep its constructor.
            if (type->tp_new == PyBaseObject_Type.tp_new) {
                py::tuple args(0);
                py::object row = steal(type->tp_new(type, args.ptr(), nullptr));
                if (!row) throw py::error_already_set();
                py::list converted = apply(values, py::reinterpret_borrow<py::list>(converters));
                std::array<PyObject*, 5> fields{
                    converted.ptr(), column_map.ptr(), cursor.ptr(), lower_map.ptr(), names.ptr()};
                for (size_t i = 0; i < fields.size(); ++i) {
                    if (PyObject_SetAttr(row.ptr(), field_names_[i].ptr(), fields[i]) < 0)
                        throw py::error_already_set();
                }
                return row;
            }
        }

        restore(values);
        return row_class(values, column_map, py::arg("cursor") = cursor,
                         py::arg("converter_map") = converters,
                         py::arg("uuid_str_indices") = uuid_indices,
                         py::arg("column_map_lower") = lower_map,
                         py::arg("column_names") = names);
    }

public:
    RowBuilder(py::object row_class, py::object bindings)
        : row_class_(std::move(row_class)), bindings_(std::move(bindings)) {
        if (!PyType_Check(row_class_.ptr()) || !PyModule_Check(bindings_.ptr()))
            throw py::type_error("expected a Row type and bindings module");
        if (row_class_.ptr() != py::module_::import("mssql_python.row").attr("Row").ptr())
            throw py::type_error("expected the canonical Row class");
        for (size_t i = 0; i < hooks_.size(); ++i)
            hooks_[i] = row_class_.attr(hook_names_[i]);
        for (size_t i = 0; i < code_.size(); ++i) {
            if (PyFunction_Check(hooks_[i].ptr()))
                code_[i] = borrow(PyFunction_GetCode(hooks_[i].ptr()));
        }
        apply_ = bindings_.attr(apply_name_);
    }

    bool matches(py::handle row_class) {
        if (row_class.ptr() != row_class_.ptr() ||
            Py_TYPE(bindings_.ptr()) != &PyModule_Type)
            return false;
        PyObject* current_apply = PyDict_GetItemWithError(
            PyModule_GetDict(bindings_.ptr()), apply_name_.ptr());
        if (!current_apply) {
            if (PyErr_Occurred()) throw py::error_already_set();
            return false;
        }
        if (current_apply != apply_.ptr()) return false;
        // Changing function.__code__ does not invalidate the owning type's tag.
        for (size_t i = 0; i < code_.size(); ++i) {
            if (code_[i] && PyFunction_GetCode(hooks_[i].ptr()) != code_[i].ptr())
                return false;
        }

        auto* type = reinterpret_cast<PyTypeObject*>(row_class.ptr());
        // CPython invalidates this tag for mutations to the type or its bases.
        // Zero is not a valid cached tag; recheck the hooks whenever invalidated.
        if (type->tp_version_tag && type->tp_version_tag == verified_version_) return true;
        verified_version_ = 0;
        for (size_t i = 0; i < hooks_.size(); ++i) {
            if (row_class_.attr(hook_names_[i]).ptr() != hooks_[i].ptr()) return false;
        }
        verified_version_ = type->tp_version_tag;
        return true;
    }

    py::object one(py::list values, py::object row_class, py::object converters,
                   py::object column_map, py::object cursor, py::object uuid_indices,
                   py::object lower_map, py::object names) {
        return construct(values, row_class, converters, column_map, cursor, uuid_indices,
                         lower_map, names);
    }

    py::list many(py::list rows_data, py::dict cursor_globals, py::object converters,
                  py::object column_map, py::object cursor, py::object uuid_indices,
                  py::object lower_map) {
        py::list result;
        // Retain ordinary list-iteration behavior if a wrapper/callback holds and
        // edits rows_data. Resolve Row and column names again between callbacks.
        for (Py_ssize_t i = 0; i < PyList_GET_SIZE(rows_data.ptr()); ++i) {
            py::object values = borrow(PyList_GET_ITEM(rows_data.ptr(), i));
            PyObject* selected = PyDict_GetItemWithError(cursor_globals.ptr(), row_name_.ptr());
            if (!selected) {
                if (!PyErr_Occurred()) PyErr_SetString(PyExc_NameError, "name 'Row' is not defined");
                throw py::error_already_set();
            }
            py::object row_class = borrow(selected);
            py::object names = cursor.attr("_cached_result_columns");
            if (!PyList_Check(values.ptr()))
                throw py::type_error("native converter rows must contain lists");
            result.append(construct(py::reinterpret_borrow<py::list>(values), row_class, converters,
                                    column_map, cursor, uuid_indices, lower_map, names));
        }
        return result;
    }
};

}  // namespace OutputConverters
