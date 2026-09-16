// py_ref.hpp — Adopting raw CPython references into pybind11's RAII ownership.
//
// The CPython C API hands back two kinds of PyObject*: NEW references, which the
// caller owns and must release, and BORROWED references, which the caller must
// incref before holding. Getting that wrong is a leak in one direction and a
// use-after-free in the other. These two helpers make the choice explicit at
// every call site and hand ownership to py::object, whose destructor then does
// the releasing.
//
// Named after nanobind's nb::steal / nb::borrow, which have the same signature,
// so a future migration is a namespace change rather than a rewrite.

#pragma once
#include <Python.h>
#include <pybind11/pybind11.h>

namespace py = pybind11;

// Takes ownership of a NEW reference without increfing. Correct for the common
// returns-a-new-reference calls: PyObject_GetAttrString, PyObject_CallMethod,
// PyImport_ImportModule, PyUnicode_FromString.
//
// Applying it to a BORROWED reference (PyList_GetItem, PyTuple_GetItem,
// PyDict_GetItem, PyDict_GetItemString) is a premature decref and a
// use-after-free. Use borrow() for those.
template <typename T = py::object>
inline T steal(PyObject* p) { return py::reinterpret_steal<T>(py::handle(p)); }

// Increfs a BORROWED reference so it can be held safely. Safe on a new reference
// only if the caller still decrefs the original, which it usually should not.
template <typename T = py::object>
inline T borrow(PyObject* p) { return py::reinterpret_borrow<T>(py::handle(p)); }
