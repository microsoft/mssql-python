// pybind11 baseline — mirrors the ORIGINAL DetectParamTypes before the perf
// commit. Uses pybind11 idioms (.attr(), .cast<>(), py::isinstance<>, etc.).
// This is what the current PR is optimizing away from.

#include <pybind11/pybind11.h>
#include <cstdint>

namespace py = pybind11;

typedef long SQLULEN;

struct DetectedInfo {
    int sql_type;
    int c_type;
    long column_size;
    int decimal_digits;
    bool is_dae;
};

// Cached types — leak-on-purpose (avoid static-destructor shutdown crash).
static py::object* g_datetime_class = nullptr;
static py::object* g_date_class = nullptr;
static py::object* g_time_class = nullptr;
static py::object* g_decimal_class = nullptr;
static py::object* g_uuid_class = nullptr;
static py::object* g_smallmoney_min = nullptr;
static py::object* g_smallmoney_max = nullptr;
static py::object* g_money_min = nullptr;
static py::object* g_money_max = nullptr;
static bool g_initialized = false;

static void init_cache() {
    if (g_initialized) return;
    auto dt = py::module_::import("datetime");
    g_datetime_class = new py::object(dt.attr("datetime"));
    g_date_class     = new py::object(dt.attr("date"));
    g_time_class     = new py::object(dt.attr("time"));
    auto dec = py::module_::import("decimal");
    g_decimal_class = new py::object(dec.attr("Decimal"));
    auto uuid = py::module_::import("uuid");
    g_uuid_class = new py::object(uuid.attr("UUID"));
    g_smallmoney_min = new py::object((*g_decimal_class)(py::str("-214748.3648")));
    g_smallmoney_max = new py::object((*g_decimal_class)(py::str("214748.3647")));
    g_money_min = new py::object((*g_decimal_class)(py::str("-922337203685477.5808")));
    g_money_max = new py::object((*g_decimal_class)(py::str("922337203685477.5807")));
    g_initialized = true;
}

static void detect_one(py::handle obj, DetectedInfo* out) {
    if (obj.is_none()) {
        out->sql_type = 0; out->c_type = 99; out->column_size = 1;
        out->decimal_digits = 0; out->is_dae = false;
        return;
    }
    // Bool before int
    if (py::isinstance<py::bool_>(obj)) {
        out->sql_type = -7; out->c_type = -7; out->column_size = 1;
        out->decimal_digits = 0; out->is_dae = false;
        return;
    }
    if (py::isinstance<py::int_>(obj)) {
        int64_t val = obj.cast<int64_t>();
        if (val >= 0 && val <= UINT8_MAX) {
            out->sql_type = -6; out->c_type = -6; out->column_size = 3;
        } else if (val >= INT16_MIN && val <= INT16_MAX) {
            out->sql_type = 5;  out->c_type = 5;  out->column_size = 5;
        } else if (val >= INT32_MIN && val <= INT32_MAX) {
            out->sql_type = 4;  out->c_type = 4;  out->column_size = 10;
        } else {
            out->sql_type = -5; out->c_type = -25; out->column_size = 19;
        }
        out->decimal_digits = 0; out->is_dae = false;
        return;
    }
    if (py::isinstance<py::float_>(obj)) {
        out->sql_type = 8; out->c_type = 8; out->column_size = 15;
        out->decimal_digits = 0; out->is_dae = false;
        return;
    }
    if (py::isinstance<py::str>(obj)) {
        Py_ssize_t length = py::len(obj);
        PyObject* raw = obj.ptr();
        unsigned int kind = PyUnicode_KIND(raw);
        bool is_unicode = (kind > PyUnicode_1BYTE_KIND) ||
            (!PyUnicode_IS_COMPACT_ASCII(raw) && kind == PyUnicode_1BYTE_KIND &&
             PyUnicode_MAX_CHAR_VALUE(raw) > 127);
        // UTF-16 length: BMP chars = 1 code unit, non-BMP = 2 (surrogate pair).
        Py_ssize_t utf16_len;
        if (kind <= PyUnicode_2BYTE_KIND) {
            utf16_len = length;
        } else {
            utf16_len = 0;
            const Py_UCS4* data = PyUnicode_4BYTE_DATA(raw);
            for (Py_ssize_t j = 0; j < length; ++j)
                utf16_len += (data[j] > 0xFFFF) ? 2 : 1;
        }
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
    if (py::isinstance<py::bytes>(obj) || py::isinstance<py::bytearray>(obj)) {
        Py_ssize_t length = py::len(obj);
        out->sql_type = -3; out->c_type = -2; out->decimal_digits = 0;
        if (length > 8000) { out->is_dae = true; out->column_size = 0; }
        else { out->is_dae = false; out->column_size = length < 1 ? 1 : length; }
        return;
    }

    // datetime / date / time — via cached Python classes + isinstance
    if (py::isinstance(obj, *g_datetime_class)) {
        py::object tz = obj.attr("tzinfo");
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
    if (py::isinstance(obj, *g_date_class)) {
        out->sql_type = 91; out->c_type = 91;
        out->column_size = 10; out->decimal_digits = 0;
        out->is_dae = false;
        return;
    }
    if (py::isinstance(obj, *g_time_class)) {
        // Original approach: .attr("hour"), .attr("minute"), etc.
        int h  = obj.attr("hour").cast<int>();
        int m  = obj.attr("minute").cast<int>();
        int s  = obj.attr("second").cast<int>();
        int us = obj.attr("microsecond").cast<int>();
        char buf[32];
        snprintf(buf, sizeof(buf), "%02d:%02d:%02d.%06d", h, m, s, us);
        py::str time_str(buf);
        Py_ssize_t time_len = py::len(time_str);
        // DetectParamTypes seeds columnSize=16 then max()es with formatted length.
        SQLULEN col = 16;
        if ((SQLULEN)time_len > col) col = time_len;
        out->column_size = col;
        out->sql_type = 92; out->c_type = -8; out->decimal_digits = 6;
        out->is_dae = false;
        return;
    }
    if (py::isinstance(obj, *g_decimal_class)) {
        py::object as_tuple = obj.attr("as_tuple")();
        py::object exponent_obj = as_tuple.attr("exponent");
        // NaN / Inf: exponent is a string.
        if (py::isinstance<py::str>(exponent_obj))
            throw py::value_error("non-finite Decimal");
        py::tuple digits = as_tuple.attr("digits").cast<py::tuple>();
        int nd = static_cast<int>(py::len(digits));
        int exponent = exponent_obj.cast<int>();

        // Two-tier MONEY range check.
        bool in_money = false;
        try {
            if (obj >= *g_smallmoney_min && obj <= *g_smallmoney_max) in_money = true;
            else if (obj >= *g_money_min && obj <= *g_money_max)       in_money = true;
        } catch (...) {}

        if (in_money) {
            py::object formatted = obj.attr("__format__")(py::str("f"));
            out->sql_type = 12; out->c_type = -8;
            out->column_size = py::len(formatted);
            out->decimal_digits = 0;
        } else {
            int precision;
            if (exponent >= 0)          precision = nd + exponent;
            else if ((-exponent) <= nd) precision = nd;
            else                        precision = -exponent;
            out->sql_type = 2; out->c_type = 2;
            out->column_size = precision;
            out->decimal_digits = exponent < 0 ? -exponent : 0;
        }
        out->is_dae = false;
        return;
    }
    if (py::isinstance(obj, *g_uuid_class)) {
        py::object b = obj.attr("bytes_le");
        (void)b;
        out->sql_type = -11; out->c_type = -11;
        out->column_size = 16; out->decimal_digits = 0;
        out->is_dae = false;
        return;
    }
    throw py::type_error("Unsupported parameter type");
}

static long detect(py::list params) {
    init_cache();
    long checksum = 0;
    DetectedInfo tmp;
    for (auto obj : params) {
        detect_one(obj, &tmp);
        checksum += tmp.sql_type + tmp.c_type + tmp.column_size + tmp.decimal_digits;
    }
    return checksum;
}

// detect_types(list) -> list[tuple] for parity validation.
static py::list detect_types(py::list params) {
    init_cache();
    py::list out;
    DetectedInfo tmp;
    for (auto obj : params) {
        detect_one(obj, &tmp);
        out.append(py::make_tuple(
            tmp.sql_type, tmp.c_type, (long)tmp.column_size,
            tmp.is_dae, tmp.decimal_digits));
    }
    return out;
}

PYBIND11_MODULE(detect_pybind11, m) {
    m.def("detect", &detect, "Detect parameter types (pybind11 baseline)");
    m.def("detect_types", &detect_types,
          "Detect parameter types, returning per-param tuples");
}
