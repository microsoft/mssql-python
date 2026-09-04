// py_type_cache.hpp — One-time cache of Python type objects.
//
// Called on first execute(). Uses raw CPython API (not pybind11) because
// these cached PyObject* are compared via PyObject_IsInstance in the
// hot DetectParamTypes loop — wrapping them in py::object would add
// unnecessary ref-count traffic on every parameter.
//
// All cached pointers are module-lifetime singletons (never DECREFed).

#pragma once
#include <Python.h>
#include <datetime.h>
#include <pybind11/pybind11.h>

#include "py_ref.hpp"  // steal / borrow

namespace py = pybind11;

namespace PyTypeCache {

// Module-lifetime singletons — never DECREFed, alive for the process.
inline PyObject* datetime_class = nullptr;
inline PyObject* date_class = nullptr;
inline PyObject* time_class = nullptr;
inline PyObject* decimal_class = nullptr;
inline PyObject* uuid_class = nullptr;
inline bool cache_initialized = false;

// Import a module and extract an attribute. Returns a new reference.
inline PyObject* import_attr(const char* module_name, const char* attr_name) {
    py::object mod = steal(PyImport_ImportModule(module_name));
    if (!mod) throw py::error_already_set();
    PyObject* attr = PyObject_GetAttrString(mod.ptr(), attr_name);
    if (!attr) throw py::error_already_set();
    return attr;
}

// Return cached type, falling back to a fresh import for callers that run before
// initialize() has. The fallback exists for the legacy execute path, which does its
// type detection in Python and can therefore reach here without the cache being warm;
// it can be dropped once that path is removed.
inline PyObject* get_cached_class(PyObject* cached, const char* module_name, const char* attr_name) {
    if (cache_initialized && cached) return cached;
    py::object mod = steal(PyImport_ImportModule(module_name));
    if (!mod) return nullptr;
    return PyObject_GetAttrString(mod.ptr(), attr_name);
}

// One-time init. Uses local py::objects so exception cleanup is automatic;
// only .release() into globals after ALL acquisitions succeed.
inline void initialize() {
    if (cache_initialized) return;

    PyDateTime_IMPORT;
    if (PyDateTimeAPI == nullptr) throw py::error_already_set();

    py::object dt_mod = steal(PyImport_ImportModule("datetime"));
    if (!dt_mod) throw py::error_already_set();

    py::object dt_cls  = steal(PyObject_GetAttrString(dt_mod.ptr(), "datetime"));
    py::object date_cls = steal(PyObject_GetAttrString(dt_mod.ptr(), "date"));
    py::object time_cls = steal(PyObject_GetAttrString(dt_mod.ptr(), "time"));
    if (!dt_cls || !date_cls || !time_cls) throw py::error_already_set();

    py::object dec_cls  = steal(import_attr("decimal", "Decimal"));
    py::object uuid_cls = steal(import_attr("uuid", "UUID"));

    // Commit to globals — all acquisitions succeeded.
    datetime_class = dt_cls.release().ptr();
    date_class     = date_cls.release().ptr();
    time_class     = time_cls.release().ptr();
    decimal_class  = dec_cls.release().ptr();
    uuid_class     = uuid_cls.release().ptr();
    cache_initialized = true;
}

// Wrap a cached pointer as py::object. A cached class is a module-lifetime
// singleton we do not own, so borrow it; the fallback import path returns a new
// reference, so steal it.
inline py::object wrap_cached_or_imported(PyObject* obj) {
    if (!obj) throw py::error_already_set();
    return cache_initialized ? borrow(obj) : steal(obj);
}

inline PyObject* get_datetime_class() { return get_cached_class(datetime_class, "datetime", "datetime"); }
inline PyObject* get_date_class()     { return get_cached_class(date_class, "datetime", "date"); }
inline PyObject* get_time_class()     { return get_cached_class(time_class, "datetime", "time"); }
inline PyObject* get_decimal_class()  { return get_cached_class(decimal_class, "decimal", "Decimal"); }
inline PyObject* get_uuid_class()     { return get_cached_class(uuid_class, "uuid", "UUID"); }

inline py::object get_datetime_class_obj() { return wrap_cached_or_imported(get_datetime_class()); }
inline py::object get_date_class_obj()     { return wrap_cached_or_imported(get_date_class()); }
inline py::object get_time_class_obj()     { return wrap_cached_or_imported(get_time_class()); }
inline py::object get_decimal_class_obj()  { return wrap_cached_or_imported(get_decimal_class()); }
inline py::object get_uuid_class_obj()     { return wrap_cached_or_imported(get_uuid_class()); }

}  // namespace PyTypeCache
