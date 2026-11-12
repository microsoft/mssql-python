// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma once

#include "py_ref.hpp"
#include <cstdint>
#include <limits>
#include <mutex>
#include <new>
#include <utility>

#if PY_VERSION_HEX >= 0x030C0000 && !defined(Py_GIL_DISABLED)
#define MSSQL_MAPPING_WATCHERS 1
#else
#define MSSQL_MAPPING_WATCHERS 0
#endif

namespace RowMappingCache {

struct Watcher {
    PyInterpreterState* interpreter;
    Watcher* next;
    std::uint64_t epoch;
    int id;
    bool enabled;
};

#if MSSQL_MAPPING_WATCHERS
struct Registry {
    std::mutex mutex;
    Watcher* head = nullptr;
};

inline Registry& registry() {
    // Only native pointers live here; the interpreter capsule owns each entry.
    static Registry value;
    return value;
}

inline int changed(PyDict_WatchEvent event, PyObject*, PyObject*, PyObject*) {
    // Every prepared cache owns its dictionaries, so deallocation cannot stale it.
    if (event == PyDict_EVENT_DEALLOCATED)
        return 0;
    PyInterpreterState* interpreter = PyInterpreterState_Get();
    Registry& entries = registry();
    std::lock_guard<std::mutex> lock(entries.mutex);
    for (Watcher* state = entries.head; state; state = state->next) {
        if (state->interpreter == interpreter) {
            if (state->epoch == std::numeric_limits<std::uint64_t>::max())
                state->enabled = false;
            else
                ++state->epoch;
            break;
        }
    }
    // No allocation, Python execution, dictionary changes or exception changes.
    return 0;
}

inline void destroy_watcher(PyObject* capsule) {
    auto* state = static_cast<Watcher*>(PyCapsule_GetPointer(capsule, "mssql.RowMappingWatcher"));
    if (!state) {
        PyErr_WriteUnraisable(capsule);
        return;
    }
    {
        Registry& entries = registry();
        std::lock_guard<std::mutex> lock(entries.mutex);
        Watcher** link = &entries.head;
        while (*link && *link != state)
            link = &(*link)->next;
        if (*link)
            *link = state->next;
    }
    if (Py_IsInitialized()) {
        PyObject *error_type, *error_value, *error_traceback;
        PyErr_Fetch(&error_type, &error_value, &error_traceback);
        if (PyDict_ClearWatcher(state->id) < 0)
            PyErr_WriteUnraisable(capsule);
        PyErr_Restore(error_type, error_value, error_traceback);
    }
    delete state;
}

inline py::object watcher_owner() {
    PyObject* dictionary = PyInterpreterState_GetDict(PyInterpreterState_Get());
    if (!dictionary) {
        if (!PyErr_Occurred())
            PyErr_SetString(PyExc_RuntimeError, "Cannot initialize RowMapping interpreter state");
        throw py::error_already_set();
    }
    py::str key("_mssql_python_row_mapping_watcher");
    PyObject* existing = PyDict_GetItemWithError(dictionary, key.ptr());
    if (existing) {
        if (existing != Py_None && !PyCapsule_IsValid(existing, "mssql.RowMappingWatcher"))
            throw py::type_error("Invalid RowMapping watcher state");
        return borrow(existing);
    }
    if (PyErr_Occurred())
        throw py::error_already_set();
    auto* state = new (std::nothrow) Watcher{PyInterpreterState_Get(), nullptr, 1, -1, true};
    if (!state)
        throw std::bad_alloc();
    state->id = PyDict_AddWatcher(changed);
    if (state->id < 0) {
        delete state;
        if (!PyErr_ExceptionMatches(PyExc_RuntimeError))
            throw py::error_already_set();
        PyErr_Clear();
        if (PyErr_WarnEx(PyExc_RuntimeWarning,
                        "RowMapping preparation disabled: no CPython dict watcher available", 1) < 0)
            throw py::error_already_set();
        if (PyDict_SetItem(dictionary, key.ptr(), Py_None) < 0)
            throw py::error_already_set();
        return py::none();
    }
    py::object owner = steal(PyCapsule_New(state, "mssql.RowMappingWatcher", destroy_watcher));
    if (!owner) {
        PyDict_ClearWatcher(state->id);
        delete state;
        throw py::error_already_set();
    }
    {
        Registry& entries = registry();
        std::lock_guard<std::mutex> lock(entries.mutex);
        state->next = entries.head;
        entries.head = state;
    }
    if (PyDict_SetItem(dictionary, key.ptr(), owner.ptr()) < 0)
        throw py::error_already_set();
    return owner;
}
#else
inline py::object watcher_owner() { return py::none(); }
#endif

struct Entry {
    PyObject* key;
    PyObject* index;
    Py_ssize_t offset;
    bool representable;
};

struct Schema {
    PyObject_HEAD
    PyObject* columns;
    PyObject* unique;
    PyObject* indices;
    PyObject* watcher_owner;
    Entry* entries;
    Py_ssize_t entry_count;
    std::uint64_t epoch;
};

inline int schema_traverse(Schema* self, visitproc visit, void* arg) {
    Py_VISIT(Py_TYPE(self));
    Py_VISIT(self->columns);
    Py_VISIT(self->unique);
    Py_VISIT(self->indices);
    Py_VISIT(self->watcher_owner);
    for (Py_ssize_t index = 0; index < self->entry_count; ++index) {
        Py_VISIT(self->entries[index].key);
        Py_VISIT(self->entries[index].index);
    }
    return 0;
}

inline int schema_clear(Schema* self) {
    self->epoch = 0;
    Entry* entries = std::exchange(self->entries, nullptr);
    Py_ssize_t count = std::exchange(self->entry_count, 0);
    py::object owner = steal(std::exchange(self->watcher_owner, nullptr));
    py::object columns = steal(std::exchange(self->columns, nullptr));
    py::object unique = steal(std::exchange(self->unique, nullptr));
    py::object indices = steal(std::exchange(self->indices, nullptr));
    for (Py_ssize_t index = 0; index < count; ++index) {
        Py_DECREF(entries[index].key);
        Py_DECREF(entries[index].index);
    }
    PyMem_Free(entries);
    return 0;
}

inline int allocate_entries(Schema* self, Py_ssize_t size) {
    if (size == 0)
        return 0;
    if (size > PY_SSIZE_T_MAX / static_cast<Py_ssize_t>(sizeof(Entry))) {
        PyErr_NoMemory();
        return -1;
    }
    self->entries = static_cast<Entry*>(PyMem_Calloc(static_cast<size_t>(size), sizeof(Entry)));
    if (!self->entries) {
        PyErr_NoMemory();
        return -1;
    }
    return 0;
}

inline int append_entry(Schema* self, PyObject* key, PyObject* index) {
    // Exact ints only: overflow is a fallback marker, not an early lookup error.
    int overflow = 0;
    long long offset = PyLong_AsLongLongAndOverflow(index, &overflow);
    if (offset == -1 && PyErr_Occurred())
        return -1;
    Entry& entry = self->entries[self->entry_count];
    entry.key = Py_NewRef(key);
    entry.index = Py_NewRef(index);
    entry.representable = overflow == 0 &&
        offset >= std::numeric_limits<Py_ssize_t>::min() &&
        offset <= std::numeric_limits<Py_ssize_t>::max();
    if (entry.representable)
        entry.offset = static_cast<Py_ssize_t>(offset);
    ++self->entry_count;
    return 0;
}

inline void schema_dealloc(Schema* self) {
    PyObject_GC_UnTrack(self);
    schema_clear(self);
    PyTypeObject* type = Py_TYPE(self);
    type->tp_free(reinterpret_cast<PyObject*>(self));
    Py_DECREF(type);
}

inline py::object schema_type() {
    static PyType_Slot slots[] = {
        {Py_tp_dealloc, reinterpret_cast<void*>(schema_dealloc)},
        {Py_tp_traverse, reinterpret_cast<void*>(schema_traverse)},
        {Py_tp_clear, reinterpret_cast<void*>(schema_clear)},
        {0, nullptr},
    };
    static PyType_Spec spec = {
        "mssql_python.row._MappingSchema", sizeof(Schema), 0,
        Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC, slots,
    };
    py::object type = steal(PyType_FromSpec(&spec));
    if (!type)
        throw py::error_already_set();
    return type;
}

inline bool builtin_keys(PyObject* dictionary, bool integer_values = false) {
    if (!PyDict_CheckExact(dictionary))
        return false;
    Py_ssize_t position = 0;
    PyObject *key, *value;
    while (PyDict_Next(dictionary, &position, &key, &value)) {
        if (!PyUnicode_CheckExact(key) || (integer_values && !PyLong_CheckExact(value)))
            return false;
    }
    return true;
}

}  // namespace RowMappingCache
