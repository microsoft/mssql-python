// Raw CPython version of DetectParamTypes hot loop.
// Mirrors the approach used in the current PR:
//   mssql_python/pybind/ddbc_bindings.cpp — DetectParamTypes.
// Uses only CPython macros and functions — no pybind11.
// Exposed as a single Python function `detect(list)` that returns nothing
// (the loop's side effects are the type dispatch itself).

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <datetime.h>
#include <cstdint>

// One-time cache — Decimal and UUID types (Python-level classes).
static PyObject* g_decimal_type = nullptr;
static PyObject* g_uuid_type = nullptr;
static PyObject* g_smallmoney_min = nullptr;
static PyObject* g_smallmoney_max = nullptr;
static PyObject* g_money_min = nullptr;
static PyObject* g_money_max = nullptr;
static bool g_initialized = false;

static int init_cache() {
    if (g_initialized) return 0;
    PyDateTime_IMPORT;
    if (PyDateTimeAPI == nullptr) return -1;

    PyObject* dec_mod = PyImport_ImportModule("decimal");
    if (!dec_mod) return -1;
    g_decimal_type = PyObject_GetAttrString(dec_mod, "Decimal");
    Py_DECREF(dec_mod);
    if (!g_decimal_type) return -1;

    PyObject* uuid_mod = PyImport_ImportModule("uuid");
    if (!uuid_mod) return -1;
    g_uuid_type = PyObject_GetAttrString(uuid_mod, "UUID");
    Py_DECREF(uuid_mod);
    if (!g_uuid_type) return -1;

    g_smallmoney_min = PyObject_CallFunction(g_decimal_type, "s", "-214748.3648");
    g_smallmoney_max = PyObject_CallFunction(g_decimal_type, "s", "214748.3647");
    g_money_min = PyObject_CallFunction(g_decimal_type, "s", "-922337203685477.5808");
    g_money_max = PyObject_CallFunction(g_decimal_type, "s", "922337203685477.5807");
    if (!g_smallmoney_min || !g_smallmoney_max || !g_money_min || !g_money_max)
        return -1;

    g_initialized = true;
    return 0;
}

// The "detected type" — kept identical between all three benchmarks so we
// measure only the dispatch cost, not what we do with the result.
struct DetectedInfo {
    int sql_type;
    int c_type;
    long column_size;
    int decimal_digits;
    bool is_dae;
};

static int detect_one(PyObject* obj, DetectedInfo* out) {
    // None
    if (obj == Py_None) {
        out->sql_type = 0; out->c_type = 99; out->column_size = 1;
        out->decimal_digits = 0; out->is_dae = false;
        return 0;
    }

    // bool BEFORE int (bool is subclass of int)
    if (PyBool_Check(obj)) {
        out->sql_type = -7; out->c_type = -7; out->column_size = 1;
        out->decimal_digits = 0; out->is_dae = false;
        return 0;
    }

    // int
    if (PyLong_Check(obj)) {
        int overflow = 0;
        int64_t val = PyLong_AsLongLongAndOverflow(obj, &overflow);
        if (overflow == 0 && !PyErr_Occurred()) {
            if (val >= 0 && val <= UINT8_MAX) {
                out->sql_type = -6; out->c_type = -6; out->column_size = 3;
            } else if (val >= INT16_MIN && val <= INT16_MAX) {
                out->sql_type = 5;  out->c_type = 5;  out->column_size = 5;
            } else if (val >= INT32_MIN && val <= INT32_MAX) {
                out->sql_type = 4;  out->c_type = 4;  out->column_size = 10;
            } else {
                out->sql_type = -5; out->c_type = -25; out->column_size = 19;
            }
        } else {
            PyErr_Clear();
            out->sql_type = -5; out->c_type = -25; out->column_size = 19;
        }
        out->decimal_digits = 0; out->is_dae = false;
        return 0;
    }

    // float
    if (PyFloat_Check(obj)) {
        out->sql_type = 8; out->c_type = 8; out->column_size = 15;
        out->decimal_digits = 0; out->is_dae = false;
        return 0;
    }

    // str
    if (PyUnicode_Check(obj)) {
        Py_ssize_t length = PyUnicode_GET_LENGTH(obj);
        unsigned int kind = PyUnicode_KIND(obj);
        Py_ssize_t utf16_len;
        if (kind <= PyUnicode_2BYTE_KIND) {
            utf16_len = length;
        } else {
            utf16_len = 0;
            const Py_UCS4* data = PyUnicode_4BYTE_DATA(obj);
            for (Py_ssize_t j = 0; j < length; ++j)
                utf16_len += (data[j] > 0xFFFF) ? 2 : 1;
        }
        bool is_unicode = (kind > PyUnicode_1BYTE_KIND) ||
            (!PyUnicode_IS_COMPACT_ASCII(obj) && kind == PyUnicode_1BYTE_KIND &&
             PyUnicode_MAX_CHAR_VALUE(obj) > 127);
        if (utf16_len > 4000) {
            out->is_dae = true;
            out->column_size = 0;
            out->sql_type = is_unicode ? -9 : 12;
            out->c_type = is_unicode ? -8 : -8;
        } else {
            out->is_dae = false;
            out->column_size = is_unicode ? utf16_len : length;
            out->sql_type = is_unicode ? -9 : 12;
            out->c_type = is_unicode ? -8 : -8;
        }
        out->decimal_digits = 0;
        return 0;
    }

    // bytes / bytearray
    if (PyBytes_Check(obj) || PyByteArray_Check(obj)) {
        Py_ssize_t length = PyBytes_Check(obj) ? PyBytes_GET_SIZE(obj)
                                               : PyByteArray_GET_SIZE(obj);
        out->sql_type = -3; out->c_type = -2; out->decimal_digits = 0;
        if (length > 8000) {
            out->is_dae = true; out->column_size = 0;
        } else {
            out->is_dae = false; out->column_size = length < 1 ? 1 : length;
        }
        return 0;
    }

    // datetime BEFORE date (datetime subclass of date)
    if (PyDateTime_Check(obj)) {
        PyObject* tz = PyObject_GetAttrString(obj, "tzinfo");
        if (!tz) return -1;
        bool has_tz = (tz != Py_None);
        Py_DECREF(tz);
        if (has_tz) {
            out->sql_type = -155; out->c_type = -155;
            out->column_size = 34; out->decimal_digits = 7;
        } else {
            out->sql_type = 93; out->c_type = 93;
            out->column_size = 26; out->decimal_digits = 6;
        }
        out->is_dae = false;
        return 0;
    }

    if (PyDate_Check(obj)) {
        out->sql_type = 91; out->c_type = 91;
        out->column_size = 10; out->decimal_digits = 0;
        out->is_dae = false;
        return 0;
    }

    if (PyTime_Check(obj)) {
        int h = PyDateTime_TIME_GET_HOUR(obj);
        int m = PyDateTime_TIME_GET_MINUTE(obj);
        int s = PyDateTime_TIME_GET_SECOND(obj);
        int us = PyDateTime_TIME_GET_MICROSECOND(obj);
        char buf[32];
        snprintf(buf, sizeof(buf), "%02d:%02d:%02d.%06d", h, m, s, us);
        // Emulate work: create the string (but do NOT mutate the list — we want
        // to time the same work as the other benchmarks and mutation is out of scope).
        PyObject* str = PyUnicode_FromString(buf);
        if (!str) return -1;
        Py_ssize_t time_len = PyUnicode_GET_LENGTH(str);
        Py_DECREF(str);
        // DetectParamTypes seeds columnSize=16 then max()es with formatted length.
        long col = 16;
        if (time_len > col) col = time_len;
        out->column_size = col;
        out->sql_type = 92; out->c_type = -8; out->decimal_digits = 6;
        out->is_dae = false;
        return 0;
    }

    // Decimal
    int is_dec = PyObject_IsInstance(obj, g_decimal_type);
    if (is_dec == -1) return -1;
    if (is_dec == 1) {
        PyObject* as_tuple = PyObject_CallMethod(obj, "as_tuple", NULL);
        if (!as_tuple) return -1;
        PyObject* exp = PyObject_GetAttrString(as_tuple, "exponent");
        if (!exp) { Py_DECREF(as_tuple); return -1; }
        // NaN / Inf: exponent is a string (e.g. 'n', 'N', 'F'). Refuse.
        if (PyUnicode_Check(exp)) {
            Py_DECREF(exp); Py_DECREF(as_tuple);
            PyErr_SetString(PyExc_ValueError, "non-finite Decimal");
            return -1;
        }
        PyObject* digits = PyObject_GetAttrString(as_tuple, "digits");
        if (!digits) { Py_DECREF(exp); Py_DECREF(as_tuple); return -1; }
        Py_ssize_t nd = PyTuple_GET_SIZE(digits);
        int exponent = (int)PyLong_AsLong(exp);
        Py_DECREF(digits); Py_DECREF(exp); Py_DECREF(as_tuple);

        // Two-tier MONEY range check (smallmoney first, then money).
        bool in_money_range = false;
        int cmp_ge = PyObject_RichCompareBool(obj, g_smallmoney_min, Py_GE);
        int cmp_le = PyObject_RichCompareBool(obj, g_smallmoney_max, Py_LE);
        if (cmp_ge == -1 || cmp_le == -1) return -1;
        if (cmp_ge == 1 && cmp_le == 1) {
            in_money_range = true;
        } else {
            cmp_ge = PyObject_RichCompareBool(obj, g_money_min, Py_GE);
            cmp_le = PyObject_RichCompareBool(obj, g_money_max, Py_LE);
            if (cmp_ge == -1 || cmp_le == -1) return -1;
            if (cmp_ge == 1 && cmp_le == 1) in_money_range = true;
        }

        if (in_money_range) {
            PyObject* formatted = PyObject_CallMethod(obj, "__format__", "s", "f");
            if (!formatted) return -1;
            out->sql_type = 12; out->c_type = -8;
            out->column_size = PyUnicode_GET_LENGTH(formatted);
            out->decimal_digits = 0;
            Py_DECREF(formatted);
        } else {
            int precision;
            if (exponent >= 0)          precision = (int)nd + exponent;
            else if ((-exponent) <= nd) precision = (int)nd;
            else                        precision = -exponent;
            out->sql_type = 2; out->c_type = 2;
            out->column_size = precision;
            out->decimal_digits = exponent < 0 ? -exponent : 0;
        }
        out->is_dae = false;
        return 0;
    }

    // UUID
    int is_uuid = PyObject_IsInstance(obj, g_uuid_type);
    if (is_uuid == -1) return -1;
    if (is_uuid == 1) {
        PyObject* b = PyObject_GetAttrString(obj, "bytes_le");
        if (!b) return -1;
        Py_DECREF(b);
        out->sql_type = -11; out->c_type = -11;
        out->column_size = 16; out->decimal_digits = 0;
        out->is_dae = false;
        return 0;
    }

    PyErr_SetString(PyExc_TypeError, "Unsupported parameter type");
    return -1;
}

static PyObject* py_detect(PyObject* /*self*/, PyObject* args) {
    PyObject* params;
    if (!PyArg_ParseTuple(args, "O!", &PyList_Type, &params)) return NULL;
    if (init_cache() != 0) return NULL;

    const Py_ssize_t n = PyList_GET_SIZE(params);
    DetectedInfo tmp;
    long checksum = 0;   // prevent dead-code elimination
    for (Py_ssize_t i = 0; i < n; ++i) {
        PyObject* obj = PyList_GET_ITEM(params, i);
        if (detect_one(obj, &tmp) != 0) return NULL;
        checksum += tmp.sql_type + tmp.c_type + tmp.column_size + tmp.decimal_digits;
    }
    return PyLong_FromLong(checksum);
}

// detect_types(list) -> list[tuple(sql_type, c_type, column_size, decimal_digits, is_dae)]
// Used for cross-variant parity validation (not perf timing).
static PyObject* py_detect_types(PyObject* /*self*/, PyObject* args) {
    PyObject* params;
    if (!PyArg_ParseTuple(args, "O!", &PyList_Type, &params)) return NULL;
    if (init_cache() != 0) return NULL;

    const Py_ssize_t n = PyList_GET_SIZE(params);
    PyObject* result = PyList_New(n);
    if (!result) return NULL;

    DetectedInfo tmp;
    for (Py_ssize_t i = 0; i < n; ++i) {
        PyObject* obj = PyList_GET_ITEM(params, i);
        if (detect_one(obj, &tmp) != 0) {
            Py_DECREF(result);
            return NULL;
        }
        PyObject* t = Py_BuildValue("(iilOi)",
            tmp.sql_type, tmp.c_type, (long)tmp.column_size,
            tmp.is_dae ? Py_True : Py_False, tmp.decimal_digits);
        if (!t) { Py_DECREF(result); return NULL; }
        PyList_SET_ITEM(result, i, t);
    }
    return result;
}

static PyMethodDef Methods[] = {
    {"detect", py_detect, METH_VARARGS, "Detect parameter types (raw CPython)"},
    {"detect_types", py_detect_types, METH_VARARGS,
     "Detect parameter types, returning per-param tuples for parity checks"},
    {NULL, NULL, 0, NULL}
};

static struct PyModuleDef Module = {
    PyModuleDef_HEAD_INIT, "detect_cpython", NULL, -1, Methods,
    NULL, NULL, NULL, NULL
};

PyMODINIT_FUNC PyInit_detect_cpython(void) {
    return PyModule_Create(&Module);
}
