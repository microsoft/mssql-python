// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma once

#include <Python.h>
#include <datetime.h>

#include "py_ref.hpp"
#include "py_type_cache.hpp"

namespace FetchTemporal {

// datetime.h keeps PyDateTimeAPI per translation unit, so these helpers must too.
static inline void ensure_datetime_api() {
    if (PyDateTimeAPI == nullptr) {
        PyDateTime_IMPORT;
        if (PyDateTimeAPI == nullptr) throw py::error_already_set();
    }
}

static inline py::object date(int year, int month, int day) {
    ensure_datetime_api();
    // Cached substitutes must still receive the original constructor call.
    if (PyTypeCache::get_date_class() !=
        reinterpret_cast<PyObject*>(PyDateTimeAPI->DateType)) {
        return PyTypeCache::get_date_class_obj()(year, month, day);
    }
    py::object result = steal(PyDate_FromDate(year, month, day));
    if (!result) throw py::error_already_set();
    return result;
}

static inline py::object time(int hour, int minute, int second, int microsecond) {
    ensure_datetime_api();
    if (PyTypeCache::get_time_class() !=
        reinterpret_cast<PyObject*>(PyDateTimeAPI->TimeType)) {
        return PyTypeCache::get_time_class_obj()(hour, minute, second, microsecond);
    }
    py::object result = steal(PyTime_FromTime(hour, minute, second, microsecond));
    if (!result) throw py::error_already_set();
    return result;
}

static inline py::object datetime(int year, int month, int day, int hour, int minute, int second,
                                  int microsecond) {
    ensure_datetime_api();
    if (PyTypeCache::get_datetime_class() !=
        reinterpret_cast<PyObject*>(PyDateTimeAPI->DateTimeType)) {
        return PyTypeCache::get_datetime_class_obj()(year, month, day, hour, minute, second,
                                                    microsecond);
    }
    py::object result =
        steal(PyDateTime_FromDateAndTime(year, month, day, hour, minute, second, microsecond));
    if (!result) throw py::error_already_set();
    return result;
}

}  // namespace FetchTemporal
