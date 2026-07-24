// py_ref.hpp — RAII wrapper for CPython refcount management (Pattern 0).
//
// PyPtr = std::unique_ptr<PyObject, PyDecRefDeleter>
//
// Zero runtime overhead (stateless functor → empty-base optimisation).
// Use adopt() for new references, incref_borrow() for borrowed ones.

#pragma once
#include <Python.h>
#include <memory>

namespace pyref {

struct PyDecRefDeleter {
    void operator()(PyObject* p) const noexcept { Py_XDECREF(p); }
};

using PyPtr = std::unique_ptr<PyObject, PyDecRefDeleter>;

// Wrap a new reference (already +1 from the API that returned it).
inline PyPtr adopt(PyObject* p) noexcept { return PyPtr{p}; }

// Extend a borrowed reference's lifetime past its borrower.
inline PyPtr incref_borrow(PyObject* p) noexcept { Py_XINCREF(p); return PyPtr{p}; }

}  // namespace pyref
