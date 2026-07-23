// nanobind version — uses RAII + type-safe casters, but escapes to CPython
// macros for the ~5 hotspots where they matter (UCS kind, PyDateTime accessors).
// Represents the "idiomatic C++ with selective CPython macros" approach.

#include <nanobind/nanobind.h>
#include <datetime.h>
#include <cstdint>

namespace nb = nanobind;

struct DetectedInfo {
    int sql_type;
    int c_type;
    long column_size;
    int decimal_digits;
    bool is_dae;
};

// Cache — leak-on-purpose singletons (avoids static-destructor issues at
// interpreter shutdown). Same pattern the current PR uses for its raw
// CPython singletons.
struct Cache {
    nb::object* decimal_class = nullptr;
    nb::object* uuid_class = nullptr;
    nb::object* smallmoney_min = nullptr;
    nb::object* smallmoney_max = nullptr;
    nb::object* money_min = nullptr;
    nb::object* money_max = nullptr;
    bool initialized = false;
};
static Cache g_cache;

static void init_cache() {
    if (g_cache.initialized) return;
    PyDateTime_IMPORT;
    auto dec  = nb::module_::import_("decimal");
    auto uuid = nb::module_::import_("uuid");
    g_cache.decimal_class = new nb::object(dec.attr("Decimal"));
    g_cache.uuid_class    = new nb::object(uuid.attr("UUID"));
    g_cache.smallmoney_min = new nb::object((*g_cache.decimal_class)("-214748.3648"));
    g_cache.smallmoney_max = new nb::object((*g_cache.decimal_class)("214748.3647"));
    g_cache.money_min = new nb::object((*g_cache.decimal_class)("-922337203685477.5808"));
    g_cache.money_max = new nb::object((*g_cache.decimal_class)("922337203685477.5807"));
    g_cache.initialized = true;
}

static void detect_one(nb::handle obj, DetectedInfo* out) {
    PyObject* raw = obj.ptr();
    if (obj.is_none()) {
        out->sql_type = 0; out->c_type = 99; out->column_size = 1;
        out->decimal_digits = 0; out->is_dae = false;
        return;
    }
    // nb::isinstance<bool_>(h) inlines to PyBool_Check(h.ptr())
    if (nb::isinstance<nb::bool_>(obj)) {
        out->sql_type = -7; out->c_type = -7; out->column_size = 1;
        out->decimal_digits = 0; out->is_dae = false;
        return;
    }
    if (nb::isinstance<nb::int_>(obj)) {
        int overflow = 0;
        int64_t val = PyLong_AsLongLongAndOverflow(raw, &overflow);
        if (overflow == 0 && !PyErr_Occurred()) {
            if (val >= 0 && val <= UINT8_MAX) {
                out->sql_type = -6; out->c_type = -6; out->column_size = 3;
            } else if (val >= INT16_MIN && val <= INT16_MAX) {
                out->sql_type = 5; out->c_type = 5; out->column_size = 5;
            } else if (val >= INT32_MIN && val <= INT32_MAX) {
                out->sql_type = 4; out->c_type = 4; out->column_size = 10;
            } else {
                out->sql_type = -5; out->c_type = -25; out->column_size = 19;
            }
        } else {
            PyErr_Clear();
            out->sql_type = -5; out->c_type = -25; out->column_size = 19;
        }
        out->decimal_digits = 0; out->is_dae = false;
        return;
    }
    if (nb::isinstance<nb::float_>(obj)) {
        out->sql_type = 8; out->c_type = 8; out->column_size = 15;
        out->decimal_digits = 0; out->is_dae = false;
        return;
    }
    if (nb::isinstance<nb::str>(obj)) {
        // Escape to CPython macros — nanobind (correctly) doesn't wrap these.
        Py_ssize_t length = PyUnicode_GET_LENGTH(raw);
        unsigned int kind = PyUnicode_KIND(raw);
        Py_ssize_t utf16_len;
        if (kind <= PyUnicode_2BYTE_KIND) {
            utf16_len = length;
        } else {
            utf16_len = 0;
            const Py_UCS4* data = PyUnicode_4BYTE_DATA(raw);
            for (Py_ssize_t j = 0; j < length; ++j)
                utf16_len += (data[j] > 0xFFFF) ? 2 : 1;
        }
        bool is_unicode = (kind > PyUnicode_1BYTE_KIND) ||
            (!PyUnicode_IS_COMPACT_ASCII(raw) && kind == PyUnicode_1BYTE_KIND &&
             PyUnicode_MAX_CHAR_VALUE(raw) > 127);
        if (utf16_len > 4000) {
            out->is_dae = true;  out->column_size = 0;
        } else {
            out->is_dae = false; out->column_size = is_unicode ? utf16_len : length;
        }
        out->sql_type = is_unicode ? -9 : 12;
        out->c_type = -8;
        out->decimal_digits = 0;
        return;
    }
    if (nb::isinstance<nb::bytes>(obj) || PyByteArray_Check(raw)) {
        Py_ssize_t length = nb::isinstance<nb::bytes>(obj)
            ? PyBytes_GET_SIZE(raw) : PyByteArray_GET_SIZE(raw);
        out->sql_type = -3; out->c_type = -2; out->decimal_digits = 0;
        if (length > 8000) { out->is_dae = true; out->column_size = 0; }
        else { out->is_dae = false; out->column_size = length < 1 ? 1 : length; }
        return;
    }
    // datetime/date/time via CPython macros — same escape as the raw path.
    if (PyDateTime_Check(raw)) {
        nb::object tz = obj.attr("tzinfo");   // RAII: no manual Py_DECREF
        bool has_tz = !tz.is_none();
        if (has_tz) {
            out->sql_type = -155; out->c_type = -155;
            out->column_size = 34; out->decimal_digits = 7;
        } else {
            out->sql_type = 93; out->c_type = 93;
            out->column_size = 26; out->decimal_digits = 6;
        }
        out->is_dae = false;
        return;
    }
    if (PyDate_Check(raw)) {
        out->sql_type = 91; out->c_type = 91;
        out->column_size = 10; out->decimal_digits = 0;
        out->is_dae = false;
        return;
    }
    if (PyTime_Check(raw)) {
        int h  = PyDateTime_TIME_GET_HOUR(raw);
        int m  = PyDateTime_TIME_GET_MINUTE(raw);
        int s  = PyDateTime_TIME_GET_SECOND(raw);
        int us = PyDateTime_TIME_GET_MICROSECOND(raw);
        char buf[32];
        snprintf(buf, sizeof(buf), "%02d:%02d:%02d.%06d", h, m, s, us);
        nb::str time_str(buf);
        Py_ssize_t time_len = PyUnicode_GET_LENGTH(time_str.ptr());
        // DetectParamTypes seeds columnSize=16 then max()es with formatted length.
        long col = 16;
        if (time_len > col) col = time_len;
        out->column_size = col;
        out->sql_type = 92; out->c_type = -8; out->decimal_digits = 6;
        out->is_dae = false;
        return;
    }
    // Decimal — PyObject_IsInstance is unavoidable (user Python class).
    if (nb::isinstance(obj, *g_cache.decimal_class)) {
        nb::tuple t = nb::cast<nb::tuple>(obj.attr("as_tuple")());
        nb::object exponent = t.attr("exponent");
        // NaN / Inf: exponent is a string.
        if (nb::isinstance<nb::str>(exponent))
            throw nb::value_error("non-finite Decimal");
        nb::tuple digits = nb::cast<nb::tuple>(t.attr("digits"));
        int nd = static_cast<int>(digits.size());
        int exponent_val = nb::cast<int>(exponent);

        // Two-tier MONEY range check.
        bool in_money = false;
        int cmp_ge = PyObject_RichCompareBool(raw, g_cache.smallmoney_min->ptr(), Py_GE);
        int cmp_le = PyObject_RichCompareBool(raw, g_cache.smallmoney_max->ptr(), Py_LE);
        if (cmp_ge == 1 && cmp_le == 1) {
            in_money = true;
        } else {
            cmp_ge = PyObject_RichCompareBool(raw, g_cache.money_min->ptr(), Py_GE);
            cmp_le = PyObject_RichCompareBool(raw, g_cache.money_max->ptr(), Py_LE);
            if (cmp_ge == 1 && cmp_le == 1) in_money = true;
        }

        if (in_money) {
            nb::object formatted = obj.attr("__format__")("f");
            out->sql_type = 12; out->c_type = -8;
            out->column_size = PyUnicode_GET_LENGTH(formatted.ptr());
            out->decimal_digits = 0;
        } else {
            int precision;
            if (exponent_val >= 0)              precision = nd + exponent_val;
            else if ((-exponent_val) <= nd)     precision = nd;
            else                                precision = -exponent_val;
            out->sql_type = 2; out->c_type = 2;
            out->column_size = precision;
            out->decimal_digits = exponent_val < 0 ? -exponent_val : 0;
        }
        out->is_dae = false;
        return;
    }
    if (nb::isinstance(obj, *g_cache.uuid_class)) {
        nb::object b = obj.attr("bytes_le");
        (void)b;
        out->sql_type = -11; out->c_type = -11;
        out->column_size = 16; out->decimal_digits = 0;
        out->is_dae = false;
        return;
    }
    throw nb::type_error("Unsupported parameter type");
}

static long detect(nb::list params) {
    init_cache();
    long checksum = 0;
    DetectedInfo tmp;
    const Py_ssize_t n = params.size();
    for (Py_ssize_t i = 0; i < n; ++i) {
        detect_one(params[i], &tmp);
        checksum += tmp.sql_type + tmp.c_type + tmp.column_size + tmp.decimal_digits;
    }
    return checksum;
}

// detect_types(list) -> list[tuple] for parity validation.
static nb::list detect_types(nb::list params) {
    init_cache();
    nb::list out;
    DetectedInfo tmp;
    const Py_ssize_t n = params.size();
    for (Py_ssize_t i = 0; i < n; ++i) {
        detect_one(params[i], &tmp);
        out.append(nb::make_tuple(
            tmp.sql_type, tmp.c_type, (long)tmp.column_size,
            tmp.is_dae, tmp.decimal_digits));
    }
    return out;
}

NB_MODULE(detect_nanobind, m) {
    m.def("detect", &detect, "Detect parameter types (nanobind)");
    m.def("detect_types", &detect_types,
          "Detect parameter types, returning per-param tuples");
}
