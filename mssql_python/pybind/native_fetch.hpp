// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma once

#include <atomic>
#include <cstddef>
#include <limits>
#include <new>

#if defined(MSSQL_NATIVE_FETCH_TEST_FAULTS) && !defined(ENABLE_PROFILING)
#error "Native-fetch fault controls require a separate profiling-enabled test build"
#endif

// Included after FetchSingleRow: this prototype deliberately shares value
// decoding, diagnostics and ODBC ownership with the existing native pipeline.
// Keep the full internal ownership chain at pybind11's hidden visibility.
#if defined(__GNUG__) && !defined(_WIN32)
namespace NativeFetch __attribute__((visibility("hidden"))) {
#else
namespace NativeFetch {
#endif

#if PY_VERSION_HEX >= 0x030C0000 && !defined(Py_GIL_DISABLED) && !defined(PYPY_VERSION)

// No Python handles are process-static. Cross-interpreter invalidation is
// conservative; callbacks only change this scalar and never allocate or call Python.
inline std::atomic<uint64_t> mutationEpoch{1};
inline thread_local PyObject* internalDict = nullptr;
inline thread_local PyObject* internalKey = nullptr;
inline thread_local PyObject* internalValue = nullptr;

inline int dict_changed(PyDict_WatchEvent event, PyObject* dict, PyObject* key,
                         PyObject* value) {
    if (event != PyDict_EVENT_MODIFIED || dict != internalDict ||
        (key != internalKey &&
         (!key || !internalKey || !PyUnicode_CheckExact(key) ||
          PyUnicode_Compare(key, internalKey) != 0)) ||
        value != internalValue)
        mutationEpoch.fetch_add(1, std::memory_order_relaxed);
    return 0;
}

inline int type_changed(PyTypeObject*) {
    mutationEpoch.fetch_add(1, std::memory_order_relaxed);
    return 0;
}

inline int function_changed(PyFunction_WatchEvent event, PyFunctionObject*, PyObject*) {
    if (event != PyFunction_EVENT_CREATE && event != PyFunction_EVENT_DESTROY)
        mutationEpoch.fetch_add(1, std::memory_order_relaxed);
    return 0;
}

struct Observer {
    int dict;
    int type;
    int function;
};

enum Input : Py_ssize_t {
    Handle, CursorDict, Connection, ConnectionDict, ColumnMap, LowerMap, Names,
    RowType, Description, Messages, NextIndex, CharEncoding, WideEncoding,
    CharCtype, ArraySize, Factory, ConnectionType, InputCount
};
enum class Mode : size_t { One, Many, Value, Next };

struct Plan {
    py::dict cursorDict;
    py::object inputs = py::none();
    SqlHandlePtr handle;
    std::string charEncoding;
    std::string wideEncoding;
    ResultMetadataCache::Snapshot metadata;
    uint64_t epoch;
    Py_ssize_t nextIndex = 0;
    int charCtype = SQL_C_WCHAR;
    bool manyEligible = false;

    explicit Plan(py::dict dict, uint64_t epoch) : cursorDict(std::move(dict)), epoch(epoch) {}

    py::handle at(Input index) const {
        return PyTuple_GET_ITEM(inputs.ptr(), index);
    }
};

struct ActiveCall;

#ifdef MSSQL_NATIVE_FETCH_TEST_FAULTS
struct TestFaults {
    int armed;
    int hit;
    size_t hits;
    size_t visits[5];
    size_t statesAllocated;
    size_t statesCleared;
    size_t statesDeallocated;
    size_t holdersAllocated;
    size_t holdersDeleted;
    size_t publications;
    size_t positiveClears;
    size_t guardedPositiveClears;
};
#endif

struct Context {
    PyObject_HEAD
    PyObject* cursorType;
    PyObject* functions;
    PyObject* dictDescriptor;
    PyObject* stateType;
    PyObject* stateKey;
    PyObject* runtime;
    ActiveCall* active;
    bool disabled;
#ifdef MSSQL_NATIVE_FETCH_TEST_FAULTS
    TestFaults faults;
#endif
};

struct State {
    PyObject_HEAD
    PyObject* context;
    PyObject* owner;
    PyObject* dict;
    std::shared_ptr<Plan>* plan;
#ifdef MSSQL_NATIVE_FETCH_TEST_FAULTS
    Context* testContext;  // Non-owning: the call, then State.context, keeps it alive.
#endif
};

inline int context_traverse(Context* self, visitproc visit, void* arg) {
    Py_VISIT(Py_TYPE(self));
    Py_VISIT(self->cursorType);
    Py_VISIT(self->functions);
    Py_VISIT(self->dictDescriptor);
    Py_VISIT(self->stateType);
    Py_VISIT(self->stateKey);
    Py_VISIT(self->runtime);
    return 0;
}

inline int context_clear(Context* self) {
    auto* cursorType = self->cursorType;
    auto* functions = self->functions;
    auto* descriptor = self->dictDescriptor;
    auto* stateType = self->stateType;
    auto* stateKey = self->stateKey;
    auto* runtime = self->runtime;
    self->cursorType = self->functions = self->dictDescriptor = nullptr;
    self->stateType = self->stateKey = self->runtime = nullptr;
    Py_XDECREF(runtime);
    Py_XDECREF(functions);
    Py_XDECREF(descriptor);
    Py_XDECREF(cursorType);
    Py_XDECREF(stateKey);
    Py_XDECREF(stateType);
    return 0;
}

inline int state_traverse(State* self, visitproc visit, void* arg) {
    Py_VISIT(Py_TYPE(self));
    Py_VISIT(self->context);
    Py_VISIT(self->owner);
    Py_VISIT(self->dict);
    if (self->plan && *self->plan) {
        Py_VISIT((*self->plan)->cursorDict.ptr());
        Py_VISIT((*self->plan)->inputs.ptr());
    }
    return 0;
}

inline int state_clear(State* self) {
    auto* plan = self->plan;
    auto* context = self->context;
    auto* owner = self->owner;
    auto* dict = self->dict;
#ifdef MSSQL_NATIVE_FETCH_TEST_FAULTS
    if (self->testContext) {
        auto& faults = self->testContext->faults;
        ++faults.statesCleared;
        if (plan)
            ++faults.holdersDeleted;
        if (plan && *plan && !(*plan)->inputs.is_none()) {
            ++faults.positiveClears;
            if (self->testContext->active)
                ++faults.guardedPositiveClears;
        }
        self->testContext = nullptr;
    }
#endif
    self->plan = nullptr;
    self->context = self->owner = self->dict = nullptr;
    // All associations are detached before any last-decref can reenter.
    delete plan;
    Py_XDECREF(dict);
    Py_XDECREF(owner);
    Py_XDECREF(context);
    return 0;
}

template <typename T, int (*Clear)(T*)>
inline void dealloc(T* self) {
    PyObject_GC_UnTrack(self);
    Clear(self);
    auto* type = Py_TYPE(self);
    type->tp_free(reinterpret_cast<PyObject*>(self));
    Py_DECREF(type);
}

#ifdef MSSQL_NATIVE_FETCH_TEST_FAULTS
inline void state_dealloc(State* self) {
    // A prior GC clear already accounted for that ownership release. Direct
    // failure-unwind deallocations reach here with the context still attached.
    if (self->testContext)
        ++self->testContext->faults.statesDeallocated;
    dealloc<State, state_clear>(self);
}
#endif

struct ActiveCall {
    Context* context;
    py::object contextOwner;
    py::object receiver;
    ActiveCall* previous = nullptr;
    ActiveCall* next;

    ActiveCall(Context* context, PyObject* receiver)
        : context(context),
          contextOwner(py::reinterpret_borrow<py::object>(
              reinterpret_cast<PyObject*>(context))),
          receiver(py::reinterpret_borrow<py::object>(receiver)),
          next(context->active) {
        if (next)
            next->previous = this;
        context->active = this;
    }

    ~ActiveCall() {
        // SQL releases the GIL: overlapping calls need not leave in LIFO order.
        if (previous)
            previous->next = next;
        else
            context->active = next;
        if (next)
            next->previous = previous;
    }

    ActiveCall(const ActiveCall&) = delete;
    ActiveCall& operator=(const ActiveCall&) = delete;
};

inline bool active(Context* context, PyObject* receiver) {
    for (auto* call = context->active; call; call = call->next) {
        if (call->receiver.ptr() == receiver)
            return true;
    }
    return false;
}

inline py::dict instance_dict(PyObject* object) {
    return py::reinterpret_steal<py::dict>(PyObject_GenericGetDict(object, nullptr));
}

template <std::size_t N>
inline PyObject* exact_item(PyObject* dict, const char (&name)[N]) {
    constexpr auto length = static_cast<Py_ssize_t>(N - 1);
    PyObject* key;
    PyObject* value;
    Py_ssize_t position = 0;
    while (PyDict_Next(dict, &position, &key, &value)) {
        if (PyUnicode_CheckExact(key) && PyUnicode_GET_LENGTH(key) == length) {
            const int comparison = PyUnicode_CompareWithASCIIString(key, name);
            if (PyErr_Occurred())
                throw py::error_already_set();
            if (comparison == 0)
                return value;
        }
    }
    return nullptr;
}

struct StateEntry {
    PyObject* value = nullptr;
    bool safe;
};

inline StateEntry state_entry(PyObject* dict) {
    StateEntry result{nullptr, PyDict_CheckExact(dict) != 0};
    PyObject* key;
    PyObject* value;
    Py_ssize_t position = 0;
    // No hashing/equality calls, including for dictionary subclasses.
    while (PyDict_Next(dict, &position, &key, &value)) {
        if (!PyUnicode_CheckExact(key)) {
            result.safe = false;
        } else if (PyUnicode_GET_LENGTH(key) ==
                   static_cast<Py_ssize_t>(sizeof("_native_fetch_state") - 1)) {
            const int comparison = PyUnicode_CompareWithASCIIString(key, "_native_fetch_state");
            if (PyErr_Occurred())
                throw py::error_already_set();
            if (comparison == 0)
                result.value = value;
        }
    }
    return result;
}

inline bool owned_by(State* state, Context* context, PyObject* receiver) {
    if (state->context != reinterpret_cast<PyObject*>(context) || !state->owner)
        return false;
#if PY_VERSION_HEX >= 0x030D0000
    PyObject* owner = nullptr;
    const int result = PyWeakref_GetRef(state->owner, &owner);
    if (result < 0)
        throw py::error_already_set();
    auto ownerReference = py::reinterpret_steal<py::object>(owner);
    return owner == receiver;
#else
    // The GIL and the call's strong receiver reference cover this borrowed read.
    PyObject* owner = PyWeakref_GetObject(state->owner);
    if (!owner)
        throw py::error_already_set();
    return owner == receiver;
#endif
}

inline bool associated(State* state, Context* context, PyObject* receiver, PyObject* dict) {
    return state->dict == dict && owned_by(state, context, receiver) &&
           state_entry(dict).value == reinterpret_cast<PyObject*>(state);
}

inline bool storage_admissible(Context* context, PyObject* receiver, PyObject* dict,
                                const StateEntry& entry) {
    if (!entry.safe || reinterpret_cast<PyObject*>(Py_TYPE(receiver)) != context->cursorType ||
        PyType_GetSlot(Py_TYPE(receiver), Py_tp_getattro) !=
            reinterpret_cast<void*>(PyObject_GenericGetAttr) ||
        PyType_GetSlot(Py_TYPE(receiver), Py_tp_setattro) !=
            reinterpret_cast<void*>(PyObject_GenericSetAttr))
        return false;
    auto attributes = steal(PyType_GetDict(Py_TYPE(receiver)));
    if (!attributes)
        throw py::error_already_set();
    if (exact_item(attributes.ptr(), "__dict__") != context->dictDescriptor ||
        exact_item(attributes.ptr(), "_native_fetch_state"))
        return false;
    auto* handle = exact_item(dict, "hstmt");
    auto* index = exact_item(dict, "_next_row_index");
    auto* messages = exact_item(dict, "messages");
    return exact_item(dict, "closed") == Py_False && handle && handle != Py_None &&
           exact_item(dict, "_connection") && index && PyLong_CheckExact(index) &&
           messages && PyList_CheckExact(messages);
}

#ifdef MSSQL_NATIVE_FETCH_TEST_FAULTS
inline bool test_fault(Context* context, int stage) {
    auto& faults = context->faults;
    ++faults.visits[stage - 1];
    if (faults.armed != stage)
        return false;
    faults.armed = 0;
    faults.hit = stage;
    ++faults.hits;
    return true;
}

inline void test_before_publication(Context* context, PyObject* dict) {
    if (!test_fault(context, 5))
        return;
    auto* callback = exact_item(dict, "_native_fetch_test_callback");
    if (!callback || !PyCallable_Check(callback))
        throw py::type_error("Native-fetch test boundary requires a callback");
    auto owner = py::reinterpret_borrow<py::object>(callback);
    auto result = steal(PyObject_CallNoArgs(owner.ptr()));
    if (!result)
        throw py::error_already_set();
}
#endif

inline py::object acquire_state(Context* context, PyObject* receiver, const py::dict& dict,
                                bool& needsRevalidation) {
    needsRevalidation = true;
    auto entry = state_entry(dict.ptr());
    if (entry.value) {
        if (reinterpret_cast<PyObject*>(Py_TYPE(entry.value)) != context->stateType)
            return {};
        auto* state = reinterpret_cast<State*>(entry.value);
        if (owned_by(state, context, receiver)) {
            if (state->dict == dict.ptr()) {
                // No allocation, publication or last-owner release on this return path.
                needsRevalidation = false;
                return py::reinterpret_borrow<py::object>(entry.value);
            }
        } else if (state->context || state->owner || state->dict || state->plan) {
            return {};
        }
    }
    if (!storage_admissible(context, receiver, dict.ptr(), entry))
        return {};

    auto old = py::reinterpret_borrow<py::object>(entry.value);
    PyObject* weakOwner;
#ifdef MSSQL_NATIVE_FETCH_TEST_FAULTS
    if (test_fault(context, 1))
        weakOwner = PyErr_NoMemory();
    else
#endif
        weakOwner = PyWeakref_NewRef(receiver, nullptr);
    auto owner = steal(weakOwner);
    if (!owner)
        throw py::error_already_set();
    PyObject* allocated;
#ifdef MSSQL_NATIVE_FETCH_TEST_FAULTS
    if (test_fault(context, 2))
        allocated = PyErr_NoMemory();
    else
#endif
        allocated = PyType_GenericAlloc(
            reinterpret_cast<PyTypeObject*>(context->stateType), 0);
    auto result = steal(allocated);
    if (!result)
        throw py::error_already_set();
    auto* state = reinterpret_cast<State*>(result.ptr());
#ifdef MSSQL_NATIVE_FETCH_TEST_FAULTS
    state->testContext = context;
    ++context->faults.statesAllocated;
    if (test_fault(context, 3))
        throw std::bad_alloc();
#endif
    state->plan = new std::shared_ptr<Plan>();
#ifdef MSSQL_NATIVE_FETCH_TEST_FAULTS
    ++context->faults.holdersAllocated;
#endif
    state->context = Py_NewRef(reinterpret_cast<PyObject*>(context));
    state->owner = owner.release().ptr();
    state->dict = Py_NewRef(dict.ptr());

#ifdef MSSQL_NATIVE_FETCH_TEST_FAULTS
    test_before_publication(context, dict.ptr());
#endif
    // Allocation can run GC callbacks. Revalidate before a callback-free write,
    // retaining the old value so publication cannot release its last reference.
    auto now = instance_dict(receiver);
    if (!now)
        throw py::error_already_set();
    entry = state_entry(dict.ptr());
    if (!now.is(dict) || entry.value != old.ptr() ||
        !storage_admissible(context, receiver, dict.ptr(), entry))
        return {};
    int published;
#ifdef MSSQL_NATIVE_FETCH_TEST_FAULTS
    if (test_fault(context, 4)) {
        PyErr_NoMemory();
        published = -1;
    } else
#endif
        published = PyDict_SetItem(dict.ptr(), context->stateKey, result.ptr());
    if (published < 0)
        throw py::error_already_set();
#ifdef MSSQL_NATIVE_FETCH_TEST_FAULTS
    ++context->faults.publications;
#endif
    return result;
}

inline py::object fallback(PyObject* self, const py::tuple& rt, Mode mode,
                            PyObject* const* args, Py_ssize_t nargs, PyObject* keywords) {
    PERF_TIMER("NativeCursor::fallback");
    auto functions = py::reinterpret_borrow<py::tuple>(rt[3]);
    if (mode == Mode::One && nargs == 0 && !keywords) {
        PyObject* receiver = self;
        return steal(PyObject_Vectorcall(functions[static_cast<size_t>(mode)].ptr(),
                                         &receiver, 1, keywords));
    }
    // Only fetchmany accepts arguments; vectorcall forwards invalid shapes to
    // the original Python signature rather than silently normalizing them.
    const Py_ssize_t keywordCount = keywords ? PyTuple_GET_SIZE(keywords) : 0;
    std::vector<PyObject*> forwarded;
    forwarded.reserve(static_cast<size_t>(nargs + keywordCount + 1));
    forwarded.push_back(self);
    for (Py_ssize_t i = 0; i < nargs + keywordCount; ++i)
        forwarded.push_back(args[i]);
    return steal(PyObject_Vectorcall(functions[static_cast<size_t>(mode)].ptr(),
                                     forwarded.data(), static_cast<size_t>(nargs + 1), keywords));
}

inline std::shared_ptr<Plan> prepare(PyObject* self, const py::tuple& rt, py::dict dict,
                                    uint64_t epoch) {
    PERF_TIMER("NativeCursor::prepare");
    auto plan = std::make_shared<Plan>(std::move(dict), epoch);
    py::object values = rt[2](py::handle(self), plan->cursorDict);
    if (!values.is_none()) {
        if (!PyTuple_CheckExact(values.ptr()) || PyTuple_GET_SIZE(values.ptr()) != InputCount)
            throw py::type_error("Invalid native fetch plan");
        plan->inputs = std::move(values);
        if (!PyDict_CheckExact(plan->at(CursorDict).ptr()) ||
            !PyDict_CheckExact(plan->at(ConnectionDict).ptr()) ||
            !PyDict_CheckExact(plan->at(ColumnMap).ptr()) ||
            !PyTuple_CheckExact(plan->at(Names).ptr()) ||
            !PyType_Check(plan->at(RowType).ptr()) ||
            !PyList_CheckExact(plan->at(Description).ptr()) ||
            !PyList_CheckExact(plan->at(Messages).ptr()) ||
            !PyType_Check(plan->at(ConnectionType).ptr()) ||
            !PyLong_CheckExact(plan->at(NextIndex).ptr()) ||
            !PyLong_CheckExact(plan->at(ArraySize).ptr()) ||
            !PyCallable_Check(plan->at(Factory).ptr()))
            throw py::type_error("Invalid native fetch plan fields");
        // current() relies on this watch, not on the Python policy registering it.
        auto* observer = static_cast<Observer*>(
            PyCapsule_GetPointer(rt[0].ptr(), "mssql_python.native_fetch.Observer"));
        if (!observer || PyDict_Watch(observer->dict, plan->cursorDict.ptr()) < 0)
            throw py::error_already_set();
        plan->handle = plan->at(Handle).cast<SqlHandlePtr>();
        plan->charEncoding = plan->at(CharEncoding).cast<std::string>();
        plan->wideEncoding = plan->at(WideEncoding).cast<std::string>();
        plan->charCtype = EffectiveCharCtypeForFetch(
            plan->at(CharCtype).cast<int>(), plan->charEncoding);
        plan->nextIndex = plan->at(NextIndex).cast<Py_ssize_t>();
        plan->metadata = plan->handle->resultMetadata.snapshot();
        auto metadata = plan->metadata.metadata;
        plan->manyEligible = metadata && metadata->completeResult && metadata->namesValidated;
        if (plan->manyEligible) {
            for (const auto& column : metadata->columns) {
                if (IsLobOrVariantColumn(column.dataType, column.columnSize)) {
                    plan->manyEligible = false;
                    break;
                }
            }
        }
    }
    return plan;
}

inline bool current(PyObject* self, Context* context, State* state, const Plan& plan,
                     const py::tuple& rt) {
    if (plan.epoch != mutationEpoch.load(std::memory_order_relaxed) ||
        !state->plan || state->plan->get() != &plan ||
        reinterpret_cast<PyObject*>(Py_TYPE(self)) != rt[1].ptr() ||
        reinterpret_cast<PyObject*>(Py_TYPE(plan.at(Connection).ptr())) !=
            plan.at(ConnectionType).ptr())
        return false;
    auto dict = instance_dict(self);
    auto connectionDict = instance_dict(plan.at(Connection).ptr());
    if (!dict || !connectionDict)
        throw py::error_already_set();
    // fetch() checks the key association before use. The native dictionary watch
    // invalidates this epoch on a later state-key change; replacement is checked here.
    return dict.is(plan.cursorDict) && connectionDict.is(plan.at(ConnectionDict)) &&
           state->dict == dict.ptr() && owned_by(state, context, self) &&
           plan.epoch == mutationEpoch.load(std::memory_order_relaxed);
}

inline void set_counter(Plan& plan, py::handle name, py::handle value) {
    struct InternalWrite {
        PyObject* dict = internalDict;
        PyObject* key = internalKey;
        PyObject* value = internalValue;
        ~InternalWrite() {
            internalDict = dict;
            internalKey = key;
            internalValue = value;
        }
    } restore;
    internalDict = plan.cursorDict.ptr();
    internalKey = name.ptr();
    internalValue = value.ptr();
    if (PyDict_SetItem(plan.cursorDict.ptr(), name.ptr(), value.ptr()) < 0)
        throw py::error_already_set();
}

inline py::object finish_python(PyObject* self, Mode mode, const py::list& values,
                                 SQLRETURN ret, bool positionCommitted) {
    PERF_TIMER("NativeCursor::completion_fallback");
    py::object cursor = py::reinterpret_borrow<py::object>(self);
    if (mode == Mode::Many) {
        py::list rows;
        if (SQL_SUCCEEDED(ret))
            rows.append(values);
        if (positionCommitted)
            return cursor.attr("_wrap_fetchmany")(rows);
        return cursor.attr("_complete_fetchmany")(rows, ret);
    }
    py::object row = positionCommitted ? cursor.attr("_wrap_fetchone")(values)
                                      : cursor.attr("_complete_fetchone")(values, ret);
    if (mode == Mode::Value && !row.is_none())
        return row[py::int_(0)];
    if (mode == Mode::Next && row.is_none())
        throw py::stop_iteration();
    return row;
}

inline bool conversion_ineligible(py::handle data) {
    if (!PyDict_CheckExact(data.ptr()))
        return false;
    PyObject* converters = nullptr;
    PyObject* uuidIndices = Py_None;
    PyObject* key;
    PyObject* value;
    Py_ssize_t position = 0;
    // A lookup can invoke equality on a stored key. Inspect entries without
    // hashing, and defer every uncertain key shape to the original policy.
    while (PyDict_Next(data.ptr(), &position, &key, &value)) {
        if (!PyUnicode_CheckExact(key))
            return false;
        const Py_ssize_t length = PyUnicode_GET_LENGTH(key);
        if (length == static_cast<Py_ssize_t>(sizeof("_cached_converter_map") - 1) &&
            PyUnicode_CompareWithASCIIString(key, "_cached_converter_map") == 0)
            converters = value;
        else if (length == static_cast<Py_ssize_t>(sizeof("_uuid_str_indices") - 1) &&
                 PyUnicode_CompareWithASCIIString(key, "_uuid_str_indices") == 0)
            uuidIndices = value;
        if (PyErr_Occurred())
            throw py::error_already_set();
    }
    return !converters || !PyTuple_CheckExact(converters) ||
           PyTuple_GET_SIZE(converters) != 0 || uuidIndices != Py_None;
}

inline bool explicit_nonunit_size(PyObject* const* args, Py_ssize_t nargs,
                                  PyObject* keywords) {
    const Py_ssize_t count = keywords ? PyTuple_GET_SIZE(keywords) : 0;
    if (nargs == 0 && count == 1) {
        PyObject* key = PyTuple_GET_ITEM(keywords, 0);
        if (!PyUnicode_CheckExact(key))
            return false;
        const int comparison = PyUnicode_CompareWithASCIIString(key, "size");
        if (PyErr_Occurred())
            throw py::error_already_set();
        if (comparison != 0)
            return false;
    } else if (nargs != 1 || count != 0) {
        return false;
    }
    if (!PyLong_CheckExact(args[0]))
        return false;
    int overflow = 0;
    const long size = PyLong_AsLongAndOverflow(args[0], &overflow);
    if (PyErr_Occurred())
        throw py::error_already_set();
    return overflow || size != 1;
}

inline bool single_size(const Plan& plan, PyObject* const* args, Py_ssize_t nargs,
                         PyObject* keywords) {
    const Py_ssize_t count = keywords ? PyTuple_GET_SIZE(keywords) : 0;
    if (nargs + count > 1)
        return false;
    if (count) {
        PyObject* key = PyTuple_GET_ITEM(keywords, 0);
        if (!PyUnicode_CheckExact(key))
            return false;
        const int comparison = PyUnicode_CompareWithASCIIString(key, "size");
        if (PyErr_Occurred())
            throw py::error_already_set();
        if (comparison != 0)
            return false;
    }
    PyObject* size = nargs + count ? args[0] : Py_None;
    if (size == Py_None)
        size = plan.at(ArraySize).ptr();
    if (!PyLong_CheckExact(size))
        return false;
    int overflow = 0;
    const long value = PyLong_AsLongAndOverflow(size, &overflow);
    if (PyErr_Occurred())
        throw py::error_already_set();
    return !overflow && value == 1;
}

inline py::object fetch(PyObject* self, Context* context, Mode mode,
                         PyObject* const* args = nullptr,
                         Py_ssize_t nargs = 0, PyObject* keywords = nullptr) {
    if (active(context, self) ||
        reinterpret_cast<PyObject*>(Py_TYPE(self)) != context->cursorType) {
        auto rt = py::reinterpret_borrow<py::tuple>(context->runtime);
        return fallback(self, rt, mode, args, nargs, keywords);
    }
    // Declared first: retain the guard through every local owner/Plan release.
    ActiveCall call(context, self);
    auto rt = py::reinterpret_borrow<py::tuple>(context->runtime);
    auto* object = self;

    if (mode == Mode::Many && explicit_nonunit_size(args, nargs, keywords))
        return fallback(self, rt, mode, args, nargs, keywords);

    py::dict dict = instance_dict(object);
    if (!dict)
        throw py::error_already_set();
    bool needsRevalidation;
    auto stateOwner = acquire_state(context, self, dict, needsRevalidation);
    auto* state = reinterpret_cast<State*>(stateOwner.ptr());
    auto admittedDict = needsRevalidation ? instance_dict(self) : dict;
    if (!admittedDict)
        throw py::error_already_set();
    if (!admittedDict.is(dict) ||
        (needsRevalidation && state && !associated(state, context, self, dict.ptr())))
        return fallback(self, rt, mode, args, nargs, keywords);
    const uint64_t epoch = mutationEpoch.load(std::memory_order_relaxed);
    auto plan = state && state->plan ? *state->plan : nullptr;
    bool stale = !plan || plan->epoch != epoch || !plan->cursorDict.is(dict);
    if (!stale && !plan->inputs.is_none()) {
        stale = !current(self, context, state, *plan, rt);
    }
    if (mode == Mode::One && stale && plan && plan->inputs.is_none() &&
        plan->cursorDict.is(dict) && conversion_ineligible(dict))
        return fallback(self, rt, mode, args, nargs, keywords);
    if (stale) {
        plan = prepare(self, rt, dict, epoch);
        auto preparedDict = instance_dict(self);
        if (!preparedDict)
            throw py::error_already_set();
        if (state && state->plan && preparedDict.is(dict) &&
            associated(state, context, self, dict.ptr())) {
            auto old = std::move(*state->plan);
            *state->plan = plan;
            old.reset();
        }
    }
    if (!state || plan->inputs.is_none() || !current(self, context, state, *plan, rt))
        return fallback(self, rt, mode, args, nargs, keywords);

    if (mode == Mode::Many) {
        if (!single_size(*plan, args, nargs, keywords) || !plan->manyEligible)
            return fallback(self, rt, mode, args, nargs, keywords);
        auto metadata = plan->handle->resultMetadata.snapshot();
        if (metadata.generation != plan->metadata.generation ||
            metadata.metadata != plan->metadata.metadata)
            return fallback(self, rt, mode, args, nargs, keywords);
    }
    if (mode == Mode::Value && PyList_GET_SIZE(plan->at(Description).ptr()) == 0)
        return fallback(self, rt, mode, args, nargs, keywords);

    PERF_TIMER("NativeCursor::fetch");
    py::list values;
    SQLRETURN ret = FetchSingleRow(plan->handle, values, plan->charEncoding,
                                   plan->wideEncoding, plan->charCtype, plan->at(Messages));
    // A codec, finalizer, another thread or reentrant callback can mutate state
    // while SQLFetch releases the GIL. Complete the consumed row, never refetch it.
    if (!current(self, context, state, *plan, rt) ||
        (!SQL_SUCCEEDED(ret) && ret != SQL_NO_DATA))
        return finish_python(self, mode, values, ret, false);
    if (ret == SQL_NO_DATA) {
        if (mode == Mode::Many)
            return py::list();
        if (mode == Mode::Next)
            throw py::stop_iteration();
        return py::none();
    }
    if (plan->nextIndex == std::numeric_limits<Py_ssize_t>::max())
        return finish_python(self, mode, values, ret, false);
    py::int_ next(plan->nextIndex + 1);
    py::int_ rowNumber(plan->nextIndex);
    if (!current(self, context, state, *plan, rt))
        return finish_python(self, mode, values, ret, false);
    auto counterNames = py::reinterpret_borrow<py::tuple>(rt[5]);
    set_counter(*plan, counterNames[0], next);
    set_counter(*plan, counterNames[1], rowNumber);
    set_counter(*plan, counterNames[2], next);
    ++plan->nextIndex;
    if (!current(self, context, state, *plan, rt))
        return finish_python(self, mode, values, ret, true);

    if (mode == Mode::Value) {
        // Decode every column, not just column zero. Eligible plans have no
        // converters, UUID processing or customized Row/fetchone behavior.
        return values[py::int_(0)];
    }
    if (mode == Mode::One) {
        auto names = py::reinterpret_borrow<py::tuple>(rt[4]);
        return RowFactory::construct_row<PyObject_SetAttr>(
            py::reinterpret_borrow<py::object>(plan->at(RowType)), values,
            plan->at(ColumnMap), object, plan->at(LowerMap), plan->at(Names), names);
    }
    if (mode == Mode::Many) {
        auto names = py::reinterpret_borrow<py::tuple>(rt[4]);
        py::object row = RowFactory::construct_row(
            py::reinterpret_borrow<py::object>(plan->at(RowType)), values,
            plan->at(ColumnMap), object, plan->at(LowerMap), plan->at(Names), names);
        py::list rows(1);
        PyList_SET_ITEM(rows.ptr(), 0, row.release().ptr());
        return rows;
    }
    PyObject* rowArgs[] = {values.ptr(), plan->at(ColumnMap).ptr(), object,
                           plan->at(LowerMap).ptr(), plan->at(Names).ptr()};
    // Keep the Python factory's allocation/assignment semantics for iteration.
    return steal(PyObject_Vectorcall(plan->at(Factory).ptr(), rowArgs, 5, nullptr));
}

template <Mode mode>
inline PyObject* method_entry(PyObject* owner, PyObject* const* args, Py_ssize_t count,
                               PyObject* keywords) {
    try {
        auto* context = reinterpret_cast<Context*>(owner);
        if (!context->functions)
            throw std::runtime_error("Native fetch context is not initialized");
        if (context->runtime && count > 0 &&
            PyObject_TypeCheck(args[0], reinterpret_cast<PyTypeObject*>(context->cursorType)) &&
            (mode == Mode::Many ||
             (count == 1 && (!keywords || PyTuple_GET_SIZE(keywords) == 0)))) {
            return fetch(args[0], context, mode, args + 1,
                         count - 1, keywords).release().ptr();
        }
        // Unbound keyword-self calls and duck-typed receivers keep the original
        // Python signature. Ordinary bound calls reach fetch directly.
        auto functions = py::reinterpret_borrow<py::tuple>(context->functions);
        return PyObject_Vectorcall(functions[static_cast<size_t>(mode)].ptr(), args,
                                   static_cast<size_t>(count), keywords);
    } catch (...) {
        py::detail::try_translate_exceptions();
        return nullptr;
    }
}

inline PyObject* internal_new(PyTypeObject*, PyObject*, PyObject*) {
    PyErr_SetString(PyExc_TypeError, "Native fetch ownership objects are created internally");
    return nullptr;
}

inline PyObject* begin_install(PyObject* owner, PyObject* args) {
    try {
        auto* context = reinterpret_cast<Context*>(owner);
        if (context->disabled)
            throw std::runtime_error("Native fetch installation is disabled after incomplete rollback");
        if (context->cursorType || context->active)
            throw std::runtime_error("Native fetch context is already installed");
        PyObject* cursorType;
        PyObject* functions;
        if (!PyArg_ParseTuple(args, "OO", &cursorType, &functions))
            return nullptr;
        if (!PyType_CheckExact(cursorType) || !PyTuple_CheckExact(functions) ||
            PyTuple_GET_SIZE(functions) != 4)
            throw py::type_error("Invalid native fetch installation");
        auto bases = steal(PyObject_GetAttrString(cursorType, "__bases__"));
        if (!bases)
            throw py::error_already_set();
        if (!PyTuple_CheckExact(bases.ptr()) || PyTuple_GET_SIZE(bases.ptr()) != 1 ||
            PyTuple_GET_ITEM(bases.ptr(), 0) != reinterpret_cast<PyObject*>(&PyBaseObject_Type))
            throw py::type_error("Native fetch requires a plain Python cursor");
        for (Py_ssize_t i = 0; i < 4; ++i) {
            if (!PyFunction_Check(PyTuple_GET_ITEM(functions, i)))
                throw py::type_error("Native fetch fallbacks must be Python functions");
        }
        auto attributes = steal(PyType_GetDict(reinterpret_cast<PyTypeObject*>(cursorType)));
        if (!attributes)
            throw py::error_already_set();
        auto* descriptor = exact_item(attributes.ptr(), "__dict__");
        if (!descriptor || Py_TYPE(descriptor) != &PyGetSetDescr_Type)
            throw py::type_error("Native fetch requires an ordinary cursor dictionary");
        context->cursorType = Py_NewRef(cursorType);
        context->functions = Py_NewRef(functions);
        context->dictDescriptor = Py_NewRef(descriptor);
        Py_RETURN_NONE;
    } catch (...) {
        py::detail::try_translate_exceptions();
        return nullptr;
    }
}

inline PyObject* activate_install(PyObject* owner, PyObject* runtime) {
    auto* context = reinterpret_cast<Context*>(owner);
    if (context->disabled || !context->cursorType || context->runtime ||
        !PyTuple_CheckExact(runtime) || PyTuple_GET_SIZE(runtime) != 7 ||
        PyTuple_GET_ITEM(runtime, 1) != context->cursorType ||
        PyTuple_GET_ITEM(runtime, 3) != context->functions ||
        PyTuple_GET_ITEM(runtime, 6) != owner ||
        !PyCallable_Check(PyTuple_GET_ITEM(runtime, 2))) {
        PyErr_SetString(PyExc_TypeError, "Invalid native fetch runtime schema 2");
        return nullptr;
    }
    const char* rowNames[] = {"_values", "_column_map", "_cursor",
                              "_column_map_lower", "_column_names"};
    const char* counterNames[] = {"_next_row_index", "_rownumber", "rowcount"};
    for (Py_ssize_t field = 4; field <= 5; ++field) {
        auto* names = PyTuple_GET_ITEM(runtime, field);
        const Py_ssize_t count = field == 4 ? 5 : 3;
        if (!PyTuple_CheckExact(names) || PyTuple_GET_SIZE(names) != count) {
            PyErr_SetString(PyExc_TypeError, "Invalid native fetch field names");
            return nullptr;
        }
        for (Py_ssize_t i = 0; i < count; ++i) {
            auto* name = PyTuple_GET_ITEM(names, i);
            if (!PyUnicode_CheckExact(name) ||
                PyUnicode_CompareWithASCIIString(
                    name, field == 4 ? rowNames[i] : counterNames[i]) != 0) {
                if (!PyErr_Occurred())
                    PyErr_SetString(PyExc_TypeError, "Invalid native fetch field name");
                return nullptr;
            }
        }
    }
    context->runtime = Py_NewRef(runtime);
    Py_RETURN_NONE;
}

inline PyObject* abort_install(PyObject* owner, PyObject*) {
    auto* context = reinterpret_cast<Context*>(owner);
    if (context->disabled || context->runtime || context->active) {
        PyErr_SetString(PyExc_RuntimeError, "Cannot abort an active native fetch context");
        return nullptr;
    }
    auto* cursorType = context->cursorType;
    auto* functions = context->functions;
    auto* descriptor = context->dictDescriptor;
    context->cursorType = context->functions = context->dictDescriptor = nullptr;
    Py_XDECREF(functions);
    Py_XDECREF(descriptor);
    Py_XDECREF(cursorType);
    Py_RETURN_NONE;
}

inline PyObject* disable_install(PyObject* owner, PyObject*) {
    auto* context = reinterpret_cast<Context*>(owner);
    if (context->runtime || context->active) {
        PyErr_SetString(PyExc_RuntimeError, "Cannot disable an active native fetch context");
        return nullptr;
    }
    // Keep every forwarding owner: even descriptors restored on the class may
    // have been saved elsewhere. Unpublished entries can never create State.
    context->disabled = true;
    Py_RETURN_NONE;
}

#ifdef MSSQL_NATIVE_FETCH_TEST_FAULTS
inline PyObject* test_arm(PyObject* owner, PyObject* stage) {
    auto* context = reinterpret_cast<Context*>(owner);
    if (!PyLong_CheckExact(stage)) {
        PyErr_SetString(PyExc_TypeError, "Expected a native-fetch test stage integer");
        return nullptr;
    }
    const long value = PyLong_AsLong(stage);
    if (value == -1 && PyErr_Occurred())
        return nullptr;
    if (value < 0 || value > 5 || context->active ||
        (value && context->faults.armed)) {
        PyErr_SetString(PyExc_RuntimeError, "Invalid or already active native-fetch test stage");
        return nullptr;
    }
    context->faults.armed = static_cast<int>(value);
    Py_RETURN_NONE;
}

inline PyObject* test_snapshot(PyObject* owner, PyObject* receiver) {
    try {
        auto* context = reinterpret_cast<Context*>(owner);
        size_t activeCount = 0;
        for (auto* call = context->active; call; call = call->next)
            ++activeCount;
        bool positive = false;
        bool attached = false;
        if (reinterpret_cast<PyObject*>(Py_TYPE(receiver)) == context->cursorType) {
            auto dict = instance_dict(receiver);
            if (!dict)
                throw py::error_already_set();
            auto* value = state_entry(dict.ptr()).value;
            if (value && reinterpret_cast<PyObject*>(Py_TYPE(value)) == context->stateType) {
                auto* state = reinterpret_cast<State*>(value);
                attached = associated(state, context, receiver, dict.ptr());
                positive = state->plan && *state->plan && !(*state->plan)->inputs.is_none();
            }
        }
        const auto& f = context->faults;
        py::dict result;
        result["schema"] = 1;
        result["armed"] = f.armed;
        result["hit"] = f.hit;
        result["hits"] = f.hits;
        result["visits"] = py::make_tuple(f.visits[0], f.visits[1], f.visits[2],
                                         f.visits[3], f.visits[4]);
        result["active"] = activeCount;
        result["receiver_active"] = active(context, receiver);
        result["states_allocated"] = f.statesAllocated;
        result["states_cleared"] = f.statesCleared;
        result["states_deallocated"] = f.statesDeallocated;
        result["holders_allocated"] = f.holdersAllocated;
        result["holders_deleted"] = f.holdersDeleted;
        result["publications"] = f.publications;
        result["positive_clears"] = f.positiveClears;
        result["guarded_positive_clears"] = f.guardedPositiveClears;
        result["associated"] = attached;
        result["positive_plan"] = positive;
        return result.release().ptr();
    } catch (...) {
        py::detail::try_translate_exceptions();
        return nullptr;
    }
}
#endif

inline PyObject* invalidate(PyObject* owner, PyObject* receiver) {
    try {
        auto* context = reinterpret_cast<Context*>(owner);
        if (reinterpret_cast<PyObject*>(Py_TYPE(receiver)) == context->cursorType) {
            auto dict = instance_dict(receiver);
            if (!dict)
                throw py::error_already_set();
            auto* value = state_entry(dict.ptr()).value;
            if (value && reinterpret_cast<PyObject*>(Py_TYPE(value)) == context->stateType) {
                auto stateOwner = py::reinterpret_borrow<py::object>(value);
                auto* state = reinterpret_cast<State*>(value);
                if (state->plan && associated(state, context, receiver, dict.ptr())) {
                    auto old = std::move(*state->plan);
                }
            }
        }
        Py_RETURN_NONE;
    } catch (...) {
        py::detail::try_translate_exceptions();
        return nullptr;
    }
}

inline void bind(py::module_& m) {
#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4191)  // PyMethodDef stores FASTCALL functions as PyCFunction.
#endif
    static PyMethodDef methods[] = {
        {"fetchone", reinterpret_cast<PyCFunction>(method_entry<Mode::One>),
         METH_FASTCALL | METH_KEYWORDS,
         "fetchone(self)\n--\n\nFetch the next row without read-ahead."},
        {"fetchmany", reinterpret_cast<PyCFunction>(method_entry<Mode::Many>),
         METH_FASTCALL | METH_KEYWORDS,
         "fetchmany(self, size=None)\n--\n\nFetch the requested number of rows."},
        {"fetchval", reinterpret_cast<PyCFunction>(method_entry<Mode::Value>),
         METH_FASTCALL | METH_KEYWORDS,
         "fetchval(self)\n--\n\nFetch the first column of the next row."},
        {"__next__", reinterpret_cast<PyCFunction>(method_entry<Mode::Next>),
         METH_FASTCALL | METH_KEYWORDS,
         "__next__(self)\n--\n\nFetch the next row or raise StopIteration."},
        {nullptr, nullptr, 0, nullptr}
    };
#ifdef _MSC_VER
#pragma warning(pop)
#endif
    static PyType_Slot stateSlots[] = {
        {Py_tp_new, reinterpret_cast<void*>(internal_new)},
#ifdef MSSQL_NATIVE_FETCH_TEST_FAULTS
        {Py_tp_dealloc, reinterpret_cast<void*>(state_dealloc)},
#else
        {Py_tp_dealloc, reinterpret_cast<void*>(dealloc<State, state_clear>)},
#endif
        {Py_tp_traverse, reinterpret_cast<void*>(state_traverse)},
        {Py_tp_clear, reinterpret_cast<void*>(state_clear)},
        {0, nullptr}
    };
    PyType_Spec stateSpec = {"ddbc_bindings._FetchState", sizeof(State), 0,
                             Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC |
                                 Py_TPFLAGS_IMMUTABLETYPE,
                             stateSlots};
    auto stateType = steal(PyType_FromSpec(&stateSpec));
    if (!stateType)
        throw py::error_already_set();
    static PyType_Slot contextSlots[] = {
        {Py_tp_new, reinterpret_cast<void*>(internal_new)},
        {Py_tp_dealloc, reinterpret_cast<void*>(dealloc<Context, context_clear>)},
        {Py_tp_traverse, reinterpret_cast<void*>(context_traverse)},
        {Py_tp_clear, reinterpret_cast<void*>(context_clear)},
        {0, nullptr}
    };
    PyType_Spec contextSpec = {"ddbc_bindings._FetchContext", sizeof(Context), 0,
                               Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC |
                                   Py_TPFLAGS_IMMUTABLETYPE,
                               contextSlots};
    auto contextType = steal(PyType_FromSpec(&contextSpec));
    if (!contextType)
        throw py::error_already_set();
    auto contextOwner = steal(PyType_GenericAlloc(
        reinterpret_cast<PyTypeObject*>(contextType.ptr()), 0));
    if (!contextOwner)
        throw py::error_already_set();
    auto* context = reinterpret_cast<Context*>(contextOwner.ptr());
    context->stateType = stateType.release().ptr();
    context->stateKey = PyUnicode_InternFromString("_native_fetch_state");
    if (!context->stateKey)
        throw py::error_already_set();
    m.attr("_native_fetch_schema") = 2;
#ifdef MSSQL_NATIVE_FETCH_TEST_FAULTS
    m.attr("_native_fetch_test_fault_schema") = 1;
#endif
    m.attr("_native_fetch_context") = contextOwner;
    py::tuple entries(4);
    for (Py_ssize_t i = 0; i < 4; ++i) {
        py::object function = steal(PyCFunction_NewEx(&methods[i], contextOwner.ptr(), nullptr));
        if (!function)
            throw py::error_already_set();
        py::object descriptor = steal(PyInstanceMethod_New(function.ptr()));
        if (!descriptor)
            throw py::error_already_set();
        PyTuple_SET_ITEM(entries.ptr(), i, descriptor.release().ptr());
    }
    m.attr("_native_fetch_methods") = std::move(entries);
    static PyMethodDef controls[] = {
        {"_begin_native_fetch_install", begin_install, METH_VARARGS, nullptr},
        {"_activate_native_fetch_install", activate_install, METH_O, nullptr},
        {"_abort_native_fetch_install", abort_install, METH_NOARGS, nullptr},
        {"_disable_native_fetch_install", disable_install, METH_NOARGS, nullptr},
        {"_invalidate_native_fetch", invalidate, METH_O, nullptr},
#ifdef MSSQL_NATIVE_FETCH_TEST_FAULTS
        {"_native_fetch_test_arm", test_arm, METH_O, nullptr},
        {"_native_fetch_test_snapshot", test_snapshot, METH_O, nullptr},
#endif
        {nullptr, nullptr, 0, nullptr}
    };
    for (auto* method = controls; method->ml_name; ++method) {
        auto function = steal(PyCFunction_NewEx(method, contextOwner.ptr(), nullptr));
        if (!function)
            throw py::error_already_set();
        m.attr(method->ml_name) = std::move(function);
    }

    auto observer = std::make_unique<Observer>();
    auto unavailable = [&m]() {
        PyErr_Clear();
        if (PyErr_WarnEx(PyExc_RuntimeWarning,
                        "Native fetch prototype disabled: CPython watcher slots unavailable",
                        1) < 0)
            throw py::error_already_set();
        m.attr("_fetch_observer") = py::none();
    };
    observer->dict = PyDict_AddWatcher(dict_changed);
    if (observer->dict < 0) {
        unavailable();
        return;
    }
    observer->type = PyType_AddWatcher(type_changed);
    if (observer->type < 0) {
        PyDict_ClearWatcher(observer->dict);
        unavailable();
        return;
    }
    observer->function = PyFunction_AddWatcher(function_changed);
    if (observer->function < 0) {
        PyType_ClearWatcher(observer->type);
        PyDict_ClearWatcher(observer->dict);
        unavailable();
        return;
    }
    // One registration per native-module lifetime. The callbacks retain no
    // watched objects; CPython removes registrations at interpreter teardown.
    m.attr("_fetch_observer") = py::capsule(
        observer.release(), "mssql_python.native_fetch.Observer", [](void* pointer) {
            delete static_cast<Observer*>(pointer);
        });
    m.def("_fetch_conversion_ineligible", &conversion_ineligible);
    m.def("_watch_fetch_dependencies", [](const py::capsule& capsule,
                                           const py::tuple& dicts, const py::tuple& types) {
        auto* observer = static_cast<Observer*>(
            PyCapsule_GetPointer(capsule.ptr(), "mssql_python.native_fetch.Observer"));
        if (!observer)
            throw py::error_already_set();
        for (auto dict : dicts) {
            if (PyDict_Watch(observer->dict, dict.ptr()) < 0)
                throw py::error_already_set();
        }
        for (auto type : types) {
            if (!PyType_Check(type.ptr()))
                throw py::type_error("Expected a type dependency");
            if (!(PyType_GetFlags(reinterpret_cast<PyTypeObject*>(type.ptr())) &
                  Py_TPFLAGS_IMMUTABLETYPE) &&
                PyType_Watch(observer->type, type.ptr()) < 0)
                throw py::error_already_set();
        }
    });
}

#else

inline void bind(py::module_& m) {
    m.attr("_fetch_observer") = py::none();
    m.def("_invalidate_native_fetch", [](py::handle) {});
}

#endif

}  // namespace NativeFetch
