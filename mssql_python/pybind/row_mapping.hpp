// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma once

#include "py_ref.hpp"
#include "row_mapping_cache.hpp"
#include <structmember.h>

namespace RowMappingView {

enum Defaults {
    ROW_TYPE,
    KEY_HOOK,
    NAMES_TYPE,
    ROW_VERSION,
    ITERATOR_TYPE,
    FALLBACK_ITERATOR,
    ATTR_ROW,
    ATTR_KEYS,
    ATTR_NAMES,
    ATTR_UNIQUE,
    ATTR_BUILD_UNIQUE,
    ATTR_COLUMNS,
    ATTR_VALUES,
    ROW_MEMBERS,
    WATCHER_OWNER,
    SCHEMA_TYPE,
    NAMES_VERSION,
    ATTR_PREPARED,
    KEY_CODE,
    GETITEM_HOOK,
    GETITEM_CODE,
    DEFAULTS_SIZE
};

enum RowField { NAMES, COLUMNS, VALUES, ROW_FIELD_COUNT };

struct AccessConfiguration {
    unsigned int row_version;
    unsigned int names_version;
    Py_ssize_t row_offsets[ROW_FIELD_COUNT];
    RowMappingCache::Watcher* watcher;
};

struct Configuration {
    PyObject_HEAD
    PyObject* defaults;
    AccessConfiguration access;
};

struct View {
    PyObject_HEAD
    PyObject* row;
    PyObject* defaults;
    PyObject* configuration;
    unsigned int row_version;
    unsigned int view_version;
    Py_ssize_t row_offsets[ROW_FIELD_COUNT];
    PyObject* prepared_row;
    PyObject* prepared_names;
    PyObject* row_dictionary;
    PyObject* names_dictionary;
    PyObject* prepared_schema;
    PyObject** row_dict_pointer;
    PyObject** names_dict_pointer;
    RowMappingCache::Watcher* watcher;
    unsigned int names_version;
    std::uint64_t prepared_epoch;
    Py_ssize_t next_entry;
    Py_ssize_t preparations;
    Py_ssize_t schema_builds;
    Py_ssize_t fast_lookups;
    Py_ssize_t slow_lookups;
    Py_ssize_t ordered_lookups;
    bool preparing;
};

struct Iterator {
    PyObject_HEAD
    View* view;
    PyObject* iterator;
    bool running;
};

inline PyObject* iterator_next(Iterator* self);

inline int traverse(View* self, visitproc visit, void* arg) {
    Py_VISIT(Py_TYPE(self));
    Py_VISIT(self->row);
    Py_VISIT(self->defaults);
    Py_VISIT(self->configuration);
    Py_VISIT(self->prepared_row);
    Py_VISIT(self->prepared_names);
    Py_VISIT(self->row_dictionary);
    Py_VISIT(self->names_dictionary);
    Py_VISIT(self->prepared_schema);
    return 0;
}

inline void invalidate_prepared(View* self) {
    self->prepared_epoch = 0;
    self->next_entry = 0;
    self->row_dict_pointer = nullptr;
    self->names_dict_pointer = nullptr;
}

inline void clear_prepared(View* self) {
    invalidate_prepared(self);
    // Detach the entire state before any decref can reenter the view.
    py::object row = steal(std::exchange(self->prepared_row, nullptr));
    py::object names = steal(std::exchange(self->prepared_names, nullptr));
    py::object row_dictionary = steal(std::exchange(self->row_dictionary, nullptr));
    py::object names_dictionary = steal(std::exchange(self->names_dictionary, nullptr));
    py::object schema = steal(std::exchange(self->prepared_schema, nullptr));
}

inline int clear(View* self) {
    invalidate_prepared(self);
    self->watcher = nullptr;
    py::object configuration = steal(std::exchange(self->configuration, nullptr));
    py::object defaults = steal(std::exchange(self->defaults, nullptr));
    py::object row = steal(std::exchange(self->row, nullptr));
    clear_prepared(self);
    return 0;
}

inline void dealloc(View* self) {
    PyObject_GC_UnTrack(self);
    clear(self);
    PyTypeObject* type = Py_TYPE(self);
    type->tp_free(reinterpret_cast<PyObject*>(self));
    Py_DECREF(type);
}

inline Py_ssize_t member_offset(PyTypeObject* type, PyObject* descriptor) {
    if (!descriptor || !Py_IS_TYPE(descriptor, &PyMemberDescr_Type) ||
        !PyType_IsSubtype(type, PyDescr_TYPE(descriptor)))
        return -1;
    PyMemberDef* member = reinterpret_cast<PyMemberDescrObject*>(descriptor)->d_member;
    if (member->type != T_OBJECT_EX || member->offset < static_cast<Py_ssize_t>(sizeof(PyObject)) ||
        member->offset > type->tp_basicsize - static_cast<Py_ssize_t>(sizeof(PyObject*)) ||
        member->offset % static_cast<Py_ssize_t>(alignof(PyObject*)) != 0)
        return -1;
    return member->offset;
}

inline bool valid_iterator_type(PyObject* defaults) {
    auto* type = reinterpret_cast<PyTypeObject*>(PyTuple_GET_ITEM(defaults, ITERATOR_TYPE));
    if (type->tp_basicsize != static_cast<Py_ssize_t>(sizeof(Iterator)) ||
        type->tp_iternext != reinterpret_cast<iternextfunc>(iterator_next)) {
        PyErr_SetString(PyExc_TypeError, "Invalid RowMapping iterator type");
        return false;
    }
    return true;
}

inline bool read_configuration(PyObject* defaults, AccessConfiguration& access) {
    if (!PyTuple_CheckExact(defaults) ||
        PyTuple_GET_SIZE(defaults) != DEFAULTS_SIZE ||
        !PyType_Check(PyTuple_GET_ITEM(defaults, ROW_TYPE)) ||
        !PyType_Check(PyTuple_GET_ITEM(defaults, ITERATOR_TYPE))) {
        PyErr_SetString(PyExc_TypeError, "Invalid RowMapping configuration");
        return false;
    }
    if (!valid_iterator_type(defaults))
        return false;
    unsigned long version = PyLong_AsUnsignedLong(PyTuple_GET_ITEM(defaults, ROW_VERSION));
    if (version == static_cast<unsigned long>(-1) && PyErr_Occurred())
        return false;
    unsigned long names_version = PyLong_AsUnsignedLong(
        PyTuple_GET_ITEM(defaults, NAMES_VERSION));
    if (names_version == static_cast<unsigned long>(-1) && PyErr_Occurred())
        return false;
    RowMappingCache::Watcher* watcher = nullptr;
    PyObject* owner = PyTuple_GET_ITEM(defaults, WATCHER_OWNER);
    if (owner != Py_None) {
        watcher = static_cast<RowMappingCache::Watcher*>(
            PyCapsule_GetPointer(owner, "mssql.RowMappingWatcher"));
        if (!watcher)
            return false;
    }
    PyObject* members = PyTuple_GET_ITEM(defaults, ROW_MEMBERS);
    if (!PyTuple_CheckExact(members) || PyTuple_GET_SIZE(members) != ROW_FIELD_COUNT) {
        PyErr_SetString(PyExc_TypeError, "Invalid RowMapping member configuration");
        return false;
    }
    access.row_version = static_cast<unsigned int>(version);
    access.names_version = static_cast<unsigned int>(names_version);
    access.watcher = watcher;
    auto* row_type = reinterpret_cast<PyTypeObject*>(PyTuple_GET_ITEM(defaults, ROW_TYPE));
    for (int field = 0; field < ROW_FIELD_COUNT; ++field)
        access.row_offsets[field] = member_offset(row_type, PyTuple_GET_ITEM(members, field));
    return true;
}

inline int configuration_traverse(Configuration* self, visitproc visit, void* arg) {
    Py_VISIT(Py_TYPE(self));
    Py_VISIT(self->defaults);
    return 0;
}

inline int configuration_clear(Configuration* self) {
    self->access = {};
    Py_CLEAR(self->defaults);
    return 0;
}

inline void configuration_dealloc(Configuration* self) {
    PyObject_GC_UnTrack(self);
    configuration_clear(self);
    PyTypeObject* type = Py_TYPE(self);
    type->tp_free(reinterpret_cast<PyObject*>(self));
    Py_DECREF(type);
}

inline bool is_configuration(PyObject* object) {
    PyTypeObject* type = Py_TYPE(object);
    return type->tp_basicsize == static_cast<Py_ssize_t>(sizeof(Configuration)) &&
           type->tp_dealloc == reinterpret_cast<destructor>(configuration_dealloc) &&
           type->tp_traverse == reinterpret_cast<traverseproc>(configuration_traverse);
}

inline py::object make_configuration(const py::tuple& defaults) {
    static PyType_Slot slots[] = {
        {Py_tp_dealloc, reinterpret_cast<void*>(configuration_dealloc)},
        {Py_tp_traverse, reinterpret_cast<void*>(configuration_traverse)},
        {Py_tp_clear, reinterpret_cast<void*>(configuration_clear)},
        {0, nullptr},
    };
    static PyType_Spec spec = {
        "mssql_python.row._MappingConfiguration", sizeof(Configuration), 0,
        Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC | Py_TPFLAGS_IMMUTABLETYPE |
            Py_TPFLAGS_DISALLOW_INSTANTIATION, slots,
    };
    py::object type = steal(PyType_FromSpec(&spec));
    if (!type)
        throw py::error_already_set();
    auto* native_type = reinterpret_cast<PyTypeObject*>(type.ptr());
    py::object owner = steal(native_type->tp_alloc(native_type, 0));
    if (!owner)
        throw py::error_already_set();
    auto* configuration = reinterpret_cast<Configuration*>(owner.ptr());
    configuration->defaults = Py_NewRef(defaults.ptr());
    if (!read_configuration(defaults.ptr(), configuration->access))
        throw py::error_already_set();
    return owner;
}

inline PyObject* create(PyTypeObject* type, PyObject*, PyObject*) {
    py::object configuration = steal(PyObject_GetAttrString(
        reinterpret_cast<PyObject*>(type), "_row_mapping_defaults"));
    if (!configuration)
        return nullptr;
    py::object defaults;
    AccessConfiguration access{};
    if (is_configuration(configuration.ptr())) {
        auto* native = reinterpret_cast<Configuration*>(configuration.ptr());
        if (!native->defaults) {
            PyErr_SetString(PyExc_TypeError, "Invalid RowMapping configuration");
            return nullptr;
        }
        defaults = borrow(native->defaults);
        access = native->access;
    } else {
        defaults = std::move(configuration);
        if (!read_configuration(defaults.ptr(), access))
            return nullptr;
    }
    // The configuration owns the type, but its live protocol can still change.
    if (!valid_iterator_type(defaults.ptr()))
        return nullptr;
    auto* self = reinterpret_cast<View*>(type->tp_alloc(type, 0));
    if (!self)
        return nullptr;
    self->defaults = defaults.release().ptr();
    self->configuration = configuration.release().ptr();
    self->row_version = access.row_version;
    self->names_version = access.names_version;
    self->watcher = access.watcher;
    self->view_version = 0;
    for (int field = 0; field < ROW_FIELD_COUNT; ++field)
        self->row_offsets[field] = access.row_offsets[field];
    PyObject* row_member = _PyType_Lookup(type, PyTuple_GET_ITEM(self->defaults, ATTR_ROW));
    if (type->tp_getattro == PyObject_GenericGetAttr &&
        member_offset(type, row_member) == static_cast<Py_ssize_t>(offsetof(View, row)))
        self->view_version = type->tp_version_tag;
    return reinterpret_cast<PyObject*>(self);
}

inline bool unchanged_row(View* self, PyObject* row) {
    if (reinterpret_cast<PyObject*>(Py_TYPE(row)) != PyTuple_GET_ITEM(self->defaults, ROW_TYPE) ||
        self->row_version == 0 || Py_TYPE(row)->tp_version_tag != self->row_version)
        return false;
    PyObject* hook = PyTuple_GET_ITEM(self->defaults, KEY_HOOK);
    PyObject* getitem = PyTuple_GET_ITEM(self->defaults, GETITEM_HOOK);
    return (!PyFunction_Check(hook) ||
            PyFunction_GET_CODE(hook) == PyTuple_GET_ITEM(self->defaults, KEY_CODE)) &&
           (!PyFunction_Check(getitem) ||
            PyFunction_GET_CODE(getitem) == PyTuple_GET_ITEM(self->defaults, GETITEM_CODE));
}

inline PyObject* stored_field(View* self, PyObject* row, RowField field) {
    return *reinterpret_cast<PyObject**>(reinterpret_cast<char*>(row) + self->row_offsets[field]);
}

inline py::object observed_row(View* self) {
#ifndef Py_GIL_DISABLED
    if (self->view_version != 0 && Py_TYPE(self)->tp_version_tag == self->view_version && self->row)
        return borrow(self->row);
#endif
    return steal(PyObject_GetAttr(
        reinterpret_cast<PyObject*>(self), PyTuple_GET_ITEM(self->defaults, ATTR_ROW)));
}

inline py::object row_field(View* self, PyObject* row, RowField field, Defaults attribute) {
#ifndef Py_GIL_DISABLED
    if (unchanged_row(self, row) && self->row_offsets[field] >= 0) {
        PyObject* value = stored_field(self, row, field);
        if (value)
            return borrow(value);
    }
#endif
    // Deleted fields still need normal descriptor/__getattr__ behavior.
    return steal(PyObject_GetAttr(row, PyTuple_GET_ITEM(self->defaults, attribute)));
}

inline PyObject* missing_key(PyObject* key) {
    py::object args = steal(PyTuple_Pack(1, key));
    if (args)
        PyErr_SetObject(PyExc_KeyError, args.ptr());
    return nullptr;
}

inline py::object get_names(View* self, PyObject* row, bool& plain_row) {
    PyObject* default_hook = PyTuple_GET_ITEM(self->defaults, KEY_HOOK);
    plain_row = unchanged_row(self, row);
    py::object hook = steal(PyObject_GetAttr(row, PyTuple_GET_ITEM(self->defaults, ATTR_KEYS)));
    if (!hook)
        return {};

    // Resolve the live hook without materializing the Row's instance dictionary.
    // A class edit invalidates the type version; instance overrides are checked here.
    plain_row = plain_row && PyMethod_Check(hook.ptr()) &&
                PyMethod_GET_FUNCTION(hook.ptr()) == default_hook &&
                PyMethod_GET_SELF(hook.ptr()) == row;
    py::object names;
    if (plain_row) {
        names = row_field(self, row, NAMES, ATTR_NAMES);
        if (!names)
            return {};
    }
    if (!plain_row || names.is_none()) {
        names = steal(PyObject_CallNoArgs(hook.ptr()));
    }
    return names;
}

inline py::object get_unique(View* self, PyObject* names) {
    py::object unique = steal(PyObject_GetAttr(names, PyTuple_GET_ITEM(self->defaults, ATTR_UNIQUE)));
    if (unique && unique.is_none())
        unique = steal(PyObject_CallMethodNoArgs(
            names, PyTuple_GET_ITEM(self->defaults, ATTR_BUILD_UNIQUE)));
    return unique;
}

inline int prepare(View* self, PyObject* row, PyObject* names, PyObject* unique) {
#if MSSQL_MAPPING_WATCHERS
    using namespace RowMappingCache;
    if (self->preparing)
        return 0;
    struct Preparing {
        bool& flag;
        explicit Preparing(bool& flag) : flag(flag) { flag = true; }
        ~Preparing() { flag = false; }
    } preparing(self->preparing);
    clear_prepared(self);
    if (!self->watcher || !self->watcher->enabled || !unchanged_row(self, row) ||
        self->view_version == 0 || Py_TYPE(self)->tp_version_tag != self->view_version ||
        self->row != row || self->names_version == 0 ||
        reinterpret_cast<PyObject*>(Py_TYPE(names)) != PyTuple_GET_ITEM(self->defaults, NAMES_TYPE) ||
        Py_TYPE(names)->tp_version_tag != self->names_version)
        return 0;
    if ((!Py_TYPE(row)->tp_dictoffset && !(Py_TYPE(row)->tp_flags & Py_TPFLAGS_MANAGED_DICT)) ||
        (!Py_TYPE(names)->tp_dictoffset && !(Py_TYPE(names)->tp_flags & Py_TPFLAGS_MANAGED_DICT)))
        return 0;
    for (int field = 0; field < ROW_FIELD_COUNT; ++field)
        if (self->row_offsets[field] < 0)
            return 0;
    PyObject* values = stored_field(self, row, VALUES);
    PyObject* columns = stored_field(self, row, COLUMNS);
    if (!values || (!PyList_CheckExact(values) && !PyTuple_CheckExact(values)) ||
        !columns || !PyDict_CheckExact(columns) || !PyDict_CheckExact(unique) ||
        stored_field(self, row, NAMES) != names)
        return 0;
    py::object row_owner = borrow(row);
    py::object names_owner = borrow(names);
    py::object column_owner = borrow(columns);
    // Materialize only the ordinary instance dictionary, never values/getset storage.
    // Its stable pointer also detects wholesale __dict__ replacement without a watcher event.
    py::object row_dictionary = steal(PyObject_GenericGetDict(row, nullptr));
    if (!row_dictionary)
        return -1;
    py::object names_dictionary = steal(PyObject_GenericGetDict(names, nullptr));
    if (!names_dictionary)
        return -1;
    if (!builtin_keys(row_dictionary.ptr()) || !builtin_keys(names_dictionary.ptr()))
        return 0;
    PyObject** row_pointer = _PyObject_GetDictPtr(row);
    PyObject** names_pointer = _PyObject_GetDictPtr(names);
    if (!row_pointer || !names_pointer) {
        if (PyErr_Occurred())
            return -1;
        return 0;
    }
    if (PyDict_Watch(self->watcher->id, columns) < 0 ||
        PyDict_Watch(self->watcher->id, unique) < 0 ||
        PyDict_Watch(self->watcher->id, row_dictionary.ptr()) < 0 ||
        PyDict_Watch(self->watcher->id, names_dictionary.ptr()) < 0)
        return -1;
    if (!builtin_keys(names_dictionary.ptr()))
        return 0;
    PyObject* cached = PyDict_GetItemWithError(
        names_dictionary.ptr(), PyTuple_GET_ITEM(self->defaults, ATTR_PREPARED));
    if (!cached && PyErr_Occurred())
        return -1;
    auto* schema_class = reinterpret_cast<PyTypeObject*>(
        PyTuple_GET_ITEM(self->defaults, SCHEMA_TYPE));
    py::object schema_owner;
    Schema* schema = nullptr;
    if (cached && Py_IS_TYPE(cached, schema_class)) {
        schema_owner = borrow(cached);
        schema = reinterpret_cast<Schema*>(cached);
    } else if (cached && cached != Py_None) {
        return 0;
    }
    if (!schema || schema->epoch != self->watcher->epoch ||
        schema->columns != columns || schema->unique != unique) {
        py::object indices = steal(PyDict_New());
        if (!indices)
            return -1;
        schema_owner = steal(schema_class->tp_alloc(schema_class, 0));
        if (!schema_owner)
            return -1;
        schema = reinterpret_cast<Schema*>(schema_owner.ptr());
        std::uint64_t epoch = self->watcher->epoch;
        if (!builtin_keys(columns, true) || !builtin_keys(unique))
            return 0;
        if (allocate_entries(schema, PyDict_Size(unique)) < 0)
            return -1;
        Py_ssize_t position = 0;
        PyObject *key, *unused;
        while (PyDict_Next(unique, &position, &key, &unused)) {
            PyObject* index = PyDict_GetItemWithError(columns, key);
            if (!index)
                return PyErr_Occurred() ? -1 : 0;
            if (PyDict_SetItem(indices.ptr(), key, index) < 0)
                return -1;
            if (append_entry(schema, key, index) < 0)
                return -1;
        }
        schema->columns = Py_NewRef(columns);
        schema->unique = Py_NewRef(unique);
        schema->indices = indices.release().ptr();
        schema->watcher_owner = Py_NewRef(PyTuple_GET_ITEM(self->defaults, WATCHER_OWNER));
        if (PyDict_Watch(self->watcher->id, schema->indices) < 0)
            return -1;
        if (self->watcher->epoch != epoch)
            return 0;
        if (!builtin_keys(names_dictionary.ptr()))
            return 0;
        // This single watched write advances the epoch. Any additional mutation deopts.
        if (PyDict_SetItem(names_dictionary.ptr(),
                           PyTuple_GET_ITEM(self->defaults, ATTR_PREPARED), schema_owner.ptr()) < 0)
            return -1;
        if (!self->watcher->enabled || self->watcher->epoch != epoch + 1)
            return 0;
        schema->epoch = self->watcher->epoch;
        ++self->schema_builds;
    }
    // Allocations above may run GC/finalizers: seal only after rechecking all dependencies.
    if (!self->watcher->enabled || schema->epoch != self->watcher->epoch ||
        !unchanged_row(self, row) || self->row != row ||
        Py_TYPE(self)->tp_version_tag != self->view_version ||
        Py_TYPE(names)->tp_version_tag != self->names_version ||
        *row_pointer != row_dictionary.ptr() || *names_pointer != names_dictionary.ptr() ||
        stored_field(self, row, NAMES) != names || stored_field(self, row, COLUMNS) != columns)
        return 0;
    if (!builtin_keys(row_dictionary.ptr()) || !builtin_keys(names_dictionary.ptr()))
        return 0;
    if (PyDict_GetItemWithError(row_dictionary.ptr(), PyTuple_GET_ITEM(self->defaults, ATTR_KEYS)))
        return 0;
    if (PyErr_Occurred())
        return -1;
    if (PyDict_GetItemWithError(names_dictionary.ptr(),
                               PyTuple_GET_ITEM(self->defaults, ATTR_UNIQUE)) != unique)
        return PyErr_Occurred() ? -1 : 0;
    self->row_dict_pointer = row_pointer;
    self->names_dict_pointer = names_pointer;
    self->prepared_row = row_owner.release().ptr();
    self->prepared_names = names_owner.release().ptr();
    self->row_dictionary = row_dictionary.release().ptr();
    self->names_dictionary = names_dictionary.release().ptr();
    self->prepared_schema = schema_owner.release().ptr();
    self->next_entry = 0;
    // Publish validity last; no Python code runs during these ownership transfers.
    self->prepared_epoch = schema->epoch;
    ++self->preparations;
#else
    (void)self;
    (void)row;
    (void)names;
    (void)unique;
#endif
    return 0;
}

inline bool prepared_subscript(View* self, PyObject* key, PyObject** result) {
#if MSSQL_MAPPING_WATCHERS
    if (self->prepared_epoch == 0 || !PyUnicode_CheckExact(key))
        return false;
    if (!self->watcher->enabled ||
        self->watcher->epoch != self->prepared_epoch ||
        Py_TYPE(self)->tp_version_tag != self->view_version) {
        invalidate_prepared(self);
        return false;
    }
    PyObject* row = self->prepared_row;
    PyObject* names = self->prepared_names;
    auto* schema = reinterpret_cast<RowMappingCache::Schema*>(self->prepared_schema);
    if (self->row != row || !unchanged_row(self, row) ||
        Py_TYPE(names)->tp_version_tag != self->names_version ||
        *self->row_dict_pointer != self->row_dictionary ||
        *self->names_dict_pointer != self->names_dictionary ||
        stored_field(self, row, NAMES) != names ||
        stored_field(self, row, COLUMNS) != schema->columns) {
        invalidate_prepared(self);
        return false;
    }
    PyObject* values = stored_field(self, row, VALUES);
    if (!values || (!PyList_CheckExact(values) && !PyTuple_CheckExact(values))) {
        invalidate_prepared(self);
        return false;
    }
    ++self->fast_lookups;
    if (self->next_entry < schema->entry_count) {
        const RowMappingCache::Entry& entry = schema->entries[self->next_entry];
        if (key == entry.key && entry.representable) {
            bool list = PyList_CheckExact(values);
            Py_ssize_t size = list ? PyList_GET_SIZE(values) : PyTuple_GET_SIZE(values);
            Py_ssize_t offset = entry.offset;
            if (offset < 0)
                offset += size;
            if (offset >= 0 && offset < size) {
                *result = Py_NewRef(list ? PyList_GET_ITEM(values, offset)
                                        : PyTuple_GET_ITEM(values, offset));
                ++self->next_entry;
                ++self->ordered_lookups;
                return true;
            }
        }
    }
    PyObject* index = PyDict_GetItemWithError(schema->indices, key);
    if (!index) {
        *result = PyErr_Occurred() ? nullptr : missing_key(key);
        return true;
    }
    // Read the current values, not a prepared row snapshot; builtin indexing keeps errors intact.
    Py_INCREF(values);
    Py_INCREF(index);
    *result = PyObject_GetItem(values, index);
    Py_DECREF(index);
    Py_DECREF(values);
    return true;
#else
    (void)self;
    (void)key;
    (void)result;
    return false;
#endif
}

inline PyObject* subscript(View* self, PyObject* key) {
    if (!self->defaults) {
        PyErr_SetString(PyExc_AttributeError, "RowMapping has no row");
        return nullptr;
    }
    PyObject* prepared_result;
    if (prepared_subscript(self, key, &prepared_result))
        return prepared_result;
    ++self->slow_lookups;
    if (!PyUnicode_Check(key)) {
        int is_string = PyObject_IsInstance(key, reinterpret_cast<PyObject*>(&PyUnicode_Type));
        if (is_string < 0)
            return nullptr;
        if (!is_string)
            return missing_key(key);
    }
    py::object row = observed_row(self);
    if (!row)
        return nullptr;
    bool plain_row;
    py::object names = get_names(self, row.ptr(), plain_row);
    if (!names)
        return nullptr;
    PyObject* names_type = PyTuple_GET_ITEM(self->defaults, NAMES_TYPE);
    int contains;
    if (PyUnicode_CheckExact(key) &&
        reinterpret_cast<PyObject*>(Py_TYPE(names.ptr())) == names_type) {
        py::object unique = get_unique(self, names.ptr());
        if (!unique)
            return nullptr;
        contains = PySequence_Contains(unique.ptr() == Py_False ? names.ptr() : unique.ptr(), key);
    } else {
        // str subclasses may be unhashable or customize equality: retain tuple lookup.
        contains = PySequence_Contains(names.ptr(), key);
    }
    if (contains < 0)
        return nullptr;
    if (!contains)
        return missing_key(key);

    // A custom key hook can replace the view's row during the membership check.
    py::object current_row = observed_row(self);
    if (!current_row)
        return nullptr;
    plain_row = plain_row && current_row.ptr() == row.ptr();
    row = std::move(current_row);
    if (plain_row && PyUnicode_CheckExact(key) &&
        Py_TYPE(row.ptr())->tp_version_tag == self->row_version) {
        py::object columns = row_field(self, row.ptr(), COLUMNS, ATTR_COLUMNS);
        if (!columns)
            return nullptr;
        if (PyDict_CheckExact(columns.ptr())) {
            int found = PyDict_Contains(columns.ptr(), key);
            if (found < 0)
                return nullptr;
            columns = py::object();
            if (found) {
                py::object values = row_field(self, row.ptr(), VALUES, ATTR_VALUES);
                if (!values)
                    return nullptr;
                // Equality on a custom dict key can replace the row's column map.
                columns = row_field(self, row.ptr(), COLUMNS, ATTR_COLUMNS);
                if (!columns)
                    return nullptr;
                py::object index = steal(PyObject_GetItem(columns.ptr(), key));
                if (!index)
                    return nullptr;
                return PyObject_GetItem(values.ptr(), index.ptr());
            }

            py::object lower = steal(PyObject_GetAttrString(row.ptr(), "_column_map_lower"));
            if (!lower)
                return nullptr;
            if (!lower.is_none()) {
                lower = steal(PyObject_GetAttrString(row.ptr(), "_column_map_lower"));
                if (!lower)
                    return nullptr;
                py::object get = steal(PyObject_GetAttrString(lower.ptr(), "get"));
                if (!get)
                    return nullptr;
                py::object lowered = steal(PyObject_CallMethod(key, "lower", nullptr));
                if (!lowered)
                    return nullptr;
                py::object index = steal(PyObject_CallOneArg(get.ptr(), lowered.ptr()));
                if (!index)
                    return nullptr;
                if (!index.is_none()) {
                    py::object values = row_field(self, row.ptr(), VALUES, ATTR_VALUES);
                    if (!values)
                        return nullptr;
                    return PyObject_GetItem(values.ptr(), index.ptr());
                }
            }
            py::object message = steal(PyUnicode_FromFormat("Row has no column '%U'", key));
            if (message)
                PyErr_SetObject(PyExc_KeyError, message.ptr());
            return nullptr;
        }
    }
    return PyObject_GetItem(row.ptr(), key);
}

inline int iterator_traverse(Iterator* self, visitproc visit, void* arg) {
    Py_VISIT(Py_TYPE(self));
    Py_VISIT(self->view);
    Py_VISIT(self->iterator);
    return 0;
}

inline int iterator_clear(Iterator* self) {
    Py_CLEAR(self->view);
    Py_CLEAR(self->iterator);
    return 0;
}

inline void iterator_dealloc(Iterator* self) {
    PyObject_GC_UnTrack(self);
    iterator_clear(self);
    PyTypeObject* type = Py_TYPE(self);
    type->tp_free(reinterpret_cast<PyObject*>(self));
    Py_DECREF(type);
}

inline PyObject* iterator_next(Iterator* self) {
    if (!self->view)
        return nullptr;
    if (self->running) {
        PyErr_SetString(PyExc_ValueError, "generator already executing");
        return nullptr;
    }
    struct Running {
        bool& flag;
        explicit Running(bool& flag) : flag(flag) { flag = true; }
        ~Running() { flag = false; }
    } running(self->running);
    if (!self->iterator) {
        py::object owner = borrow(reinterpret_cast<PyObject*>(self->view));
        View* view = self->view;
        py::object row = observed_row(view);
        if (!row) {
            iterator_clear(self);
            return nullptr;
        }
        bool plain_row;
        py::object names = get_names(view, row.ptr(), plain_row);
        if (!names) {
            iterator_clear(self);
            return nullptr;
        }
        py::object unique;
        if (reinterpret_cast<PyObject*>(Py_TYPE(names.ptr())) ==
            PyTuple_GET_ITEM(view->defaults, NAMES_TYPE)) {
            unique = get_unique(view, names.ptr());
            if (!unique) {
                iterator_clear(self);
                return nullptr;
            }
        }
        if (unique && unique.ptr() != Py_False) {
            if (plain_row && prepare(view, row.ptr(), names.ptr(), unique.ptr()) < 0) {
                iterator_clear(self);
                return nullptr;
            }
            self->iterator = PyObject_GetIter(unique.ptr());
        } else {
            self->iterator = PyObject_CallOneArg(
                PyTuple_GET_ITEM(view->defaults, FALLBACK_ITERATOR), names.ptr());
        }
        if (!self->iterator) {
            iterator_clear(self);
            return nullptr;
        }
        if (!PyIter_Check(self->iterator)) {
            PyErr_SetString(PyExc_TypeError, "Mapping key hook did not return an iterator");
            iterator_clear(self);
            return nullptr;
        }
    }
    PyObject* value = PyIter_Next(self->iterator);
    if (!value)
        iterator_clear(self);
    return value;
}

inline PyObject* iterate(View* self) {
    if (!self->defaults) {
        PyErr_SetString(PyExc_AttributeError, "RowMapping has no row");
        return nullptr;
    }
    auto* type = reinterpret_cast<PyTypeObject*>(PyTuple_GET_ITEM(self->defaults, ITERATOR_TYPE));
    auto* iterator = reinterpret_cast<Iterator*>(type->tp_alloc(type, 0));
    if (!iterator)
        return nullptr;
    Py_INCREF(self);
    iterator->view = self;
    return reinterpret_cast<PyObject*>(iterator);
}

inline PyObject* statistics(View* self, void*) {
    return Py_BuildValue("(nnnnn)", self->preparations, self->schema_builds,
                         self->fast_lookups, self->slow_lookups, self->ordered_lookups);
}

inline py::object make_base(const py::object& row_type, const py::object& names_type,
                            const py::object& fallback_iterator) {
    if (!PyType_Check(row_type.ptr()) || !PyType_Check(names_type.ptr()))
        throw py::type_error("Row and column names must be types");

    static PyType_Slot iterator_slots[] = {
        {Py_tp_dealloc, reinterpret_cast<void*>(iterator_dealloc)},
        {Py_tp_traverse, reinterpret_cast<void*>(iterator_traverse)},
        {Py_tp_clear, reinterpret_cast<void*>(iterator_clear)},
        {Py_tp_iter, reinterpret_cast<void*>(PyObject_SelfIter)},
        {Py_tp_iternext, reinterpret_cast<void*>(iterator_next)},
        {0, nullptr},
    };
    static PyType_Spec iterator_spec = {
        "mssql_python.row._RowMappingIterator", sizeof(Iterator), 0,
        Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC, iterator_slots,
    };
    py::object iterator_type = steal(PyType_FromSpec(&iterator_spec));
    if (!iterator_type)
        throw py::error_already_set();
    auto name = [](const char* text) {
        py::object result = steal(PyUnicode_InternFromString(text));
        if (!result)
            throw py::error_already_set();
        return result;
    };
    auto* row_class = reinterpret_cast<PyTypeObject*>(row_type.ptr());
    py::object getattribute = name("__getattribute__");
    py::tuple attributes = py::make_tuple(
        name("_column_names"), name("_column_map"), name("_values"));
    py::tuple row_members(static_cast<Py_ssize_t>(ROW_FIELD_COUNT));
    py::object hook = row_type.attr("_mapping_keys");
    py::object getitem = row_type.attr("__getitem__");
    bool standard_access = _PyType_Lookup(row_class, getattribute.ptr()) ==
                           _PyType_Lookup(&PyBaseObject_Type, getattribute.ptr());
    for (int field = 0; field < ROW_FIELD_COUNT; ++field) {
        PyObject* descriptor = _PyType_Lookup(row_class, PyTuple_GET_ITEM(attributes.ptr(), field));
        row_members[field] = standard_access && member_offset(row_class, descriptor) >= 0
            ? borrow(descriptor) : py::none();
    }
    // MRO-resolved member descriptors support both Python slots and an inherited native base.
    auto version = row_class->tp_version_tag;
    py::object owner = RowMappingCache::watcher_owner();
    py::object schema_class = RowMappingCache::schema_type();
    py::object unique_name = name("_unique");
    py::object prepared_name = name("_prepared");
    auto* names_class = reinterpret_cast<PyTypeObject*>(names_type.ptr());
    bool ordinary_names = names_class->tp_getattro == PyObject_GenericGetAttr &&
                          names_class->tp_setattro == PyObject_GenericSetAttr &&
                          !_PyType_Lookup(names_class, unique_name.ptr()) &&
                          !_PyType_Lookup(names_class, prepared_name.ptr());
    unsigned int names_version = ordinary_names ? names_class->tp_version_tag : 0;
    py::tuple defaults = py::make_tuple(
        row_type, hook, names_type, version, iterator_type, fallback_iterator,
        name("_row"), name("_mapping_keys"), name("_column_names"), name("_unique"),
        name("_unique_keys"), name("_column_map"), name("_values"), row_members,
        owner, schema_class, names_version, prepared_name,
        PyFunction_Check(hook.ptr()) ? borrow(PyFunction_GetCode(hook.ptr())) : py::none(),
        getitem,
        PyFunction_Check(getitem.ptr()) ? borrow(PyFunction_GetCode(getitem.ptr())) : py::none());
    py::object configuration = make_configuration(defaults);

    static PyMemberDef members[] = {
        {"_row", T_OBJECT_EX, offsetof(View, row), 0, "The row observed by this view."},
        {nullptr, 0, 0, 0, nullptr},
    };
    static PyGetSetDef getsets[] = {
        {const_cast<char*>("_prepared_counts"), reinterpret_cast<getter>(statistics), nullptr,
         const_cast<char*>("Preparation, schema-build, prepared, ordinary and ordered-hit counts."),
         nullptr},
        {nullptr, nullptr, nullptr, nullptr, nullptr},
    };
    static PyType_Slot slots[] = {
        {Py_tp_new, reinterpret_cast<void*>(create)},
        {Py_tp_dealloc, reinterpret_cast<void*>(dealloc)},
        {Py_tp_traverse, reinterpret_cast<void*>(traverse)},
        {Py_tp_clear, reinterpret_cast<void*>(clear)},
        {Py_tp_members, members},
        {Py_tp_getset, getsets},
        {Py_mp_subscript, reinterpret_cast<void*>(subscript)},
        {Py_tp_iter, reinterpret_cast<void*>(iterate)},
        {0, nullptr},
    };
    static PyType_Spec spec = {
        "mssql_python.row._RowMappingBase", sizeof(View), 0,
        Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE | Py_TPFLAGS_HAVE_GC, slots,
    };
    py::object type = steal(PyType_FromSpec(&spec));
    if (!type)
        throw py::error_already_set();
    type.attr("_row_mapping_defaults") = configuration;
    type.attr("__slots__") = py::make_tuple("_row");
    type.attr("_prepared_supported") = !owner.is_none();
    return type;
}

}  // namespace RowMappingView
