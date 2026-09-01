// param_detect.hpp — Python parameter type detection and explicit input-size overrides.
//
// Owns the first stage of the native execute pipeline:
//
//     DetectParamTypes  ->  BindParameters  ->  SQLExecute
//     (this file)           (ddbc_bindings.cpp)
//
// DetectParamTypes inspects each Python parameter, decides the ODBC C type / SQL type
// / column size to bind it as, and returns a ParamInfo per parameter. It also carries
// the ParamInfo / NumericData / Int128_t types those results are expressed in, and
// build_numeric_data, which converts a Python Decimal into the SQL_NUMERIC_STRUCT
// byte layout.
//
// Header-only, and deliberately so. The build compiles with -O3 but without LTO, so a
// .cpp boundary would also be an inlining boundary: the small helpers here are called
// once per parameter per execute, and moving them out of the caller's translation unit
// would turn inlined code into real calls on the hot path. Defining them inline in a
// header keeps them in whichever translation unit uses them. If LTO is enabled later
// this can become a normal .cpp.

#pragma once

#include "ddbc_bindings.h"    // ParamInfo consumers, SQL Server ODBC constants
#include "py_ref.hpp"         // steal / borrow
#include "py_type_cache.hpp"  // PyTypeCache::get_*_class

#include <Python.h>
#include <datetime.h>  // CPython datetime API (PyDateTime_Check, PyDateTime_GET_*, etc.)

#include <algorithm>  // std::min
#include <cstdint>
#include <cstdio>  // snprintf
#include <cstring>  // std::memcpy
#include <string>
#include <vector>

//-------------------------------------------------------------------------------------------------
// Parameter description types
//-------------------------------------------------------------------------------------------------

// This struct is shared between C++ & Python code.
// Suppress -Wattributes warning for ParamInfo struct
// The warning is triggered because pybind11 handles visibility attributes automatically,
// and having additional attributes on the struct can cause conflicts on Linux with GCC
#ifdef __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wattributes"
#endif
struct ParamInfo {
    SQLSMALLINT inputOutputType = SQL_PARAM_INPUT;
    SQLSMALLINT paramCType = SQL_C_DEFAULT;
    SQLSMALLINT paramSQLType = SQL_UNKNOWN_TYPE;
    SQLULEN columnSize = 0;
    SQLSMALLINT decimalDigits = 0;
    SQLLEN strLenOrInd = 0;  // Required for DAE
    bool isDAE = false;      // Indicates if we need to stream
    // Strong reference to the Python object for DAE (data-at-execution) streaming.
    // py::object owns the refcount, so the compiler-generated destructor, copy and
    // move operations are all correct and this struct needs no rule-of-five.
    py::object dataPtr;
    Py_ssize_t utf16Len = 0;  // UTF-16 code unit count for string params
};
#ifdef __GNUC__
#pragma GCC diagnostic pop
#endif

// Mirrors the SQL_NUMERIC_STRUCT. But redefined to replace val char array
// with std::string, because pybind doesn't allow binding char array.
// This struct is shared between C++ & Python code.
struct NumericData {
    SQLCHAR precision;
    SQLSCHAR scale;
    SQLCHAR sign;     // 1=pos, 0=neg
    std::string val;  // 123.45 -> 12345

    NumericData() : precision(0), scale(0), sign(0), val(SQL_MAX_NUMERIC_LEN, '\0') {}

    NumericData(SQLCHAR precision, SQLSCHAR scale, SQLCHAR sign, const std::string& valueBytes)
        : precision(precision), scale(scale), sign(sign), val(SQL_MAX_NUMERIC_LEN, '\0') {
        if (valueBytes.size() > SQL_MAX_NUMERIC_LEN) {
            throw std::runtime_error(
                "NumericData valueBytes size exceeds SQL_MAX_NUMERIC_LEN (16)");
        }
        // Copy binary data to buffer, remaining bytes stay zero-padded
        std::memcpy(&val[0], valueBytes.data(), valueBytes.size());
    }
};

struct Int128_t {
    uint64_t low;
    int64_t high;

    Int128_t() : low(0), high(0) {}
    Int128_t(uint64_t l, int64_t h) : low(l), high(h) {}

    Int128_t multiply_by_10() const {
        // value * 10 = (value * 8) + (value * 2)
        Int128_t shift3 = *this << 3;
        Int128_t shift1 = *this << 1;
        return shift3 + shift1;
    }

    Int128_t operator<<(int shift) const {
        // These would require special cases. We only shift by 1 and 3 for multiply_by_10.
        assert(shift > 0);
        assert(shift < 64);
        uint64_t new_low = low << shift;
        uint64_t new_high = (static_cast<uint64_t>(high) << shift) | (low >> (64 - shift));
        return {new_low, static_cast<int64_t>(new_high)};
    }

    Int128_t operator+(const Int128_t& other) const {
        uint64_t sum_low = low + other.low;
        uint64_t carry = (sum_low < low) ? 1 : 0;
        int64_t sum_high = high + other.high + carry;
        return {sum_low, sum_high};
    }

    Int128_t operator+(uint64_t digit) const {
        uint64_t sum_low = low + digit;
        uint64_t carry = (sum_low < low) ? 1 : 0;
        int64_t sum_high = high + carry;
        return {sum_low, sum_high};
    }

    Int128_t operator-() const {
        uint64_t new_low = ~low + 1;
        uint64_t new_high = ~high + (new_low == 0 ? 1 : 0);
        return {new_low, static_cast<int64_t>(new_high)};
    }
};

// ---------------------------------------------------------------------------
// Constants for DetectParamTypes
// ---------------------------------------------------------------------------

// Strings longer than this use data-at-execution (DAE) streaming
inline constexpr int MAX_INLINE_CHAR = 4000;

// Binary data longer than this uses DAE streaming (SQL Server max for non-MAX types)
inline constexpr int MAX_INLINE_BINARY = 8000;

// SQL Server maximum numeric precision
inline constexpr int MAX_NUMERIC_PRECISION = 38;

// C type used to bind narrow (ASCII) text: SQL_C_WCHAR on every platform.
//
// The legacy Python path binds text with its own SQL_C_CHAR constant, which is
// numerically -8 — that is ODBC's SQL_C_WCHAR, not SQL_C_CHAR (1). So the legacy
// path has always bound text wide, on every platform. unixODBC also requires wide
// chars for text on Linux/macOS, so those platforms agreed already; Windows was
// the only one resolving this to a real SQL_C_CHAR and binding narrow, which made
// it diverge from both the legacy path and the other platforms in C type and in
// the driver-side encoding path it took. Bind wide everywhere so all four
// combinations agree.
inline constexpr SQLSMALLINT PARAM_C_TYPE_TEXT = SQL_C_WCHAR;

// Forward declare NumericData helper used by decimal path
inline NumericData build_numeric_data(PyObject* as_tuple, PyObject* digits, int exponent);

// True if the ready unicode string starts with the given ASCII literal, matching
// str.startswith semantics across every storage kind (UCS1/2/4). PyUnicode_READ
// yields the code point at each index regardless of kind, so a WKT prefix is
// detected even when a later non-ASCII char forces the string into a wider kind.
// The legacy path uses str.startswith, which is kind-independent, so native must
// be too for parity. N is deduced from the literal (includes the trailing NUL),
// so callers never pass a hand-counted length that could drift from the string.
template <Py_ssize_t N>
inline bool StartsWithAscii(unsigned int kind, const void* data, Py_ssize_t length,
                            const char (&prefix)[N]) {
    constexpr Py_ssize_t prefixLen = N - 1;
    if (length < prefixLen) {
        return false;
    }
    for (Py_ssize_t j = 0; j < prefixLen; ++j) {
        if (PyUnicode_READ(kind, data, j) !=
            static_cast<Py_UCS4>(static_cast<unsigned char>(prefix[j]))) {
            return false;
        }
    }
    return true;
}

inline bool PyLongGreaterThan(PyObject* value, long long threshold) {
    int overflow = 0;
    long long result = PyLong_AsLongLongAndOverflow(value, &overflow);
    if (result == -1 && PyErr_Occurred()) {
        throw py::error_already_set();
    }
    return overflow > 0 || (overflow == 0 && result > threshold);
}

inline PyObject* FormatDecimalParam(PyObject* params, Py_ssize_t index, PyObject* value) {
    py::object formatted = steal(PyObject_CallMethod(value, "__format__", "s", "f"));
    if (!formatted) throw py::error_already_set();
    if (PyList_SetItem(params, index, formatted.release().ptr()) != 0) {
        throw py::error_already_set();
    }
    return PyList_GET_ITEM(params, index);
}

inline void NormalizeTimeParam(PyObject* params, Py_ssize_t index, SQLULEN& columnSize) {
    PyObject* value = PyList_GET_ITEM(params, index);
    py::object formatted = steal(PyObject_CallMethod(value, "isoformat", "s", "microseconds"));
    if (!formatted) throw py::error_already_set();
    if (!PyUnicode_Check(formatted.ptr())) {
        throw py::type_error("datetime.time.isoformat() must return a str");
    }
    columnSize = std::max<SQLULEN>(columnSize, PyUnicode_GET_LENGTH(formatted.ptr()));
    if (PyList_SetItem(params, index, formatted.release().ptr()) != 0) {
        throw py::error_already_set();
    }
}

inline void ApplyInputSizeOverride(PyObject* params, PyObject* inputSize, Py_ssize_t index,
                                   ParamInfo& info) {
    py::tuple values = borrow<py::tuple>(inputSize);
    info.paramSQLType = values[0].cast<SQLSMALLINT>();
    info.paramCType = values[1].cast<SQLSMALLINT>();

    PyObject* columnSize = values[2].ptr();
    PyObject* decimalDigits = values[3].ptr();
    const bool isNumeric =
        info.paramSQLType == SQL_DECIMAL || info.paramSQLType == SQL_NUMERIC;

    if (isNumeric) {
        if (PyLongGreaterThan(columnSize, MAX_NUMERIC_PRECISION)) {
            info.columnSize = MAX_NUMERIC_PRECISION;
        } else {
            SQLULEN requestedSize = values[2].cast<SQLULEN>();
            info.columnSize = requestedSize > 0 ? requestedSize : 18;
        }
        info.decimalDigits =
            PyLongGreaterThan(decimalDigits, info.columnSize)
                ? static_cast<SQLSMALLINT>(info.columnSize)
                : values[3].cast<SQLSMALLINT>();
    } else {
        info.columnSize = values[2].cast<SQLULEN>();
        info.decimalDigits = values[3].cast<SQLSMALLINT>();
    }

    PyObject* obj = PyList_GET_ITEM(params, index);
    if (obj == Py_None) {
        info.paramCType = SQL_C_DEFAULT;
        return;
    }

    if (isNumeric) {
        info.paramCType = PARAM_C_TYPE_TEXT;
        int isDecimal = PyObject_IsInstance(obj, PyTypeCache::get_decimal_class());
        if (isDecimal == -1) throw py::error_already_set();
        if (isDecimal == 1) {
            obj = FormatDecimalParam(params, index, obj);
        }
    }

    info.isDAE =
        (PyUnicode_Check(obj) && PyLongGreaterThan(columnSize, MAX_INLINE_CHAR)) ||
        ((PyBytes_Check(obj) || PyByteArray_Check(obj)) &&
         PyLongGreaterThan(columnSize, MAX_INLINE_BINARY));

    if (PyTime_Check(obj) && info.paramCType == PARAM_C_TYPE_TEXT) {
        NormalizeTimeParam(params, index, info.columnSize);
        obj = PyList_GET_ITEM(params, index);
    }

    if (info.isDAE) {
        info.dataPtr = borrow(obj);
    }
}

// ---------------------------------------------------------------------------
// DetectParamTypes: Raw CPython parameter type detection for the primary execute path.
//
// Design decisions:
// 1. Operates on a COPY of the user's param list (cursor.py does list(actual_params)).
//    We mutate it in-place (PyList_SetItem) for types that need pre-processing
//    (time→isoformat string, Decimal→NumericData, UUID→bytes_le).
// 2. Uses CPython macros (PyLong_Check, PyDateTime_Check, etc.) instead of pybind11's
//    py::isinstance<> for ~3x faster type checks (direct struct field test vs virtual call).
// 3. Integer range detection uses <cstdint> constants — these match the SQL Server
//    storage engine's range exactly (TINYINT: 0-255, SMALLINT: -32768..32767, etc.)
// 4. String handling inspects UCS kind directly for O(1) ASCII detection rather than
//    scanning content — critical for bulk insert scenarios with thousands of params.
// 5. MONEY/SMALLMONEY uses exact Decimal comparison (PyObject_RichCompareBool) to avoid
//    double-precision boundary errors (e.g., 214748.3647 would round incorrectly as double).
// ---------------------------------------------------------------------------
//
// ORDERING MATTERS:
//   - bool before int (bool is a subclass of int in Python)
//   - datetime before date (datetime is a subclass of date)
//
// Takes raw PyObject* lists. Caller guarantees params is a fresh copy (cursor.py
// does list(actual_params)), so in-place mutation via PyList_SetItem is safe.
inline std::vector<ParamInfo> DetectParamTypes(PyObject* params, PyObject* inputSizes) {
    PyTypeCache::initialize();

    const Py_ssize_t n = PyList_GET_SIZE(params);
    const Py_ssize_t inputSizeCount = inputSizes == Py_None ? 0 : PyList_GET_SIZE(inputSizes);
    std::vector<ParamInfo> infos(n);

    PyObject* decimal_type = PyTypeCache::get_decimal_class();
    PyObject* uuid_type = PyTypeCache::get_uuid_class();

    for (Py_ssize_t i = 0; i < n; ++i) {
        ParamInfo& info = infos[i];
        info.inputOutputType = SQL_PARAM_INPUT;
        info.isDAE = false;

        if (i < inputSizeCount) {
            ApplyInputSizeOverride(params, PyList_GET_ITEM(inputSizes, i), i, info);
            continue;
        }

        PyObject* obj = PyList_GET_ITEM(params, i);

        // --- None ---
        if (obj == Py_None) {
            info.paramSQLType = SQL_UNKNOWN_TYPE;
            info.paramCType = SQL_C_DEFAULT;
            info.columnSize = 1;
            info.decimalDigits = 0;
            continue;
        }

        // bool must be checked before int: in CPython, PyBool_Type is a subclass of
        // PyLong_Type, so PyLong_Check(True) returns 1. If we hit the int branch first,
        // True→1 instead of BIT.
        if (PyBool_Check(obj)) {
            info.paramSQLType = SQL_BIT;
            info.paramCType = SQL_C_BIT;
            info.columnSize = 1;
            info.decimalDigits = 0;
            continue;
        }

        // --- int (allow subclasses, but bool was already caught above) ---
        if (PyLong_Check(obj)) {
            int overflow = 0;
            int64_t val = PyLong_AsLongLongAndOverflow(obj, &overflow);
            if (overflow == 0 && !PyErr_Occurred()) {
                if (val >= 0 && val <= UINT8_MAX) {
                    info.paramSQLType = SQL_TINYINT;
                    info.paramCType = SQL_C_TINYINT;
                    info.columnSize = 3;
                } else if (val >= INT16_MIN && val <= INT16_MAX) {
                    info.paramSQLType = SQL_SMALLINT;
                    info.paramCType = SQL_C_SHORT;
                    info.columnSize = 5;
                } else if (val >= INT32_MIN && val <= INT32_MAX) {
                    info.paramSQLType = SQL_INTEGER;
                    info.paramCType = SQL_C_LONG;
                    info.columnSize = 10;
                } else {
                    info.paramSQLType = SQL_BIGINT;
                    info.paramCType = SQL_C_SBIGINT;
                    info.columnSize = 19;
                }
            } else if (overflow != 0) {
                // Python int outside [INT64_MIN, INT64_MAX] cannot bind as SQL BIGINT.
                // Reject here with a clear message rather than mislabelling it BIGINT and
                // letting param.cast<int64_t>() fail later with a generic pybind11 error.
                PyErr_Clear();
                py::object as_str = steal(PyObject_Str(obj));
                if (!as_str) {
                    // PyObject_Str can fail (e.g. CPython's int->str digit limit for a
                    // multi-thousand-digit int). Drop that error and fall back to a
                    // placeholder so we still raise our own clear ValueError.
                    PyErr_Clear();
                }
                std::string s = as_str ? as_str.cast<std::string>() : std::string("<int>");
                throw py::value_error("integer " + s +
                                      " is out of range for SQL BIGINT [-2^63, 2^63-1]");
            } else {
                // A real Python error from PyLong_AsLongLongAndOverflow, not overflow.
                throw py::error_already_set();
            }
            info.decimalDigits = 0;
            continue;
        }

        // --- float (allow subclasses) ---
        if (PyFloat_Check(obj)) {
            info.paramSQLType = SQL_DOUBLE;
            info.paramCType = SQL_C_DOUBLE;
            info.columnSize = 15;
            info.decimalDigits = 0;
            continue;
        }

        // --- str (allow subclasses) ---
        if (PyUnicode_Check(obj)) {
            Py_ssize_t length = PyUnicode_GET_LENGTH(obj);
            unsigned int kind = PyUnicode_KIND(obj);
            const void* udata = PyUnicode_DATA(obj);

            Py_ssize_t utf16_len;
            if (kind <= PyUnicode_2BYTE_KIND) {
                utf16_len = length;
            } else {
                utf16_len = 0;
                const Py_UCS4* data = PyUnicode_4BYTE_DATA(obj);
                for (Py_ssize_t j = 0; j < length; ++j) {
                    utf16_len += (data[j] > 0xFFFF) ? 2 : 1;
                }
            }

            // Detect whether the string needs wide-char (NVARCHAR) or narrow (VARCHAR) binding.
            // PyUnicode_IS_COMPACT_ASCII is a struct field check (O(1)), not a content scan.
            // UCS-1 strings with max_char > 127 contain Latin-1 chars → need NVARCHAR.
            bool is_unicode =
                (kind > PyUnicode_1BYTE_KIND) ||
                (PyUnicode_IS_COMPACT_ASCII(obj) == 0 && kind == PyUnicode_1BYTE_KIND &&
                 PyUnicode_MAX_CHAR_VALUE(obj) > 127);

            // Geometry WKT (POINT / LINESTRING / POLYGON) always binds as NVARCHAR, so the
            // SQL type is stable regardless of the ASCII/Latin-1 heuristic above and matches
            // the small-geometry case. The prefix is checked kind-agnostically (matching the
            // legacy str.startswith), and this runs BEFORE the length/DAE decision so a large
            // polygon keeps the SAME wide type while still taking the DAE path below.
            //
            // NB: we deliberately do NOT copy the legacy path's exact tuple here. The legacy
            // _map_sql_type returns NVARCHAR with columnSize == len and DAE=false even for a
            // 7790-char polygon, which is unbindable (SQLBindParameter rejects a non-MAX
            // NVARCHAR precision > 4000 with "Invalid precision value"). Folding geometry into
            // is_unicode instead keeps geometry on NVARCHAR for both size regimes and lets the
            // length gate stream large values via DAE, which actually binds.
            if (StartsWithAscii(kind, udata, length, "POINT") ||
                StartsWithAscii(kind, udata, length, "LINESTRING") ||
                StartsWithAscii(kind, udata, length, "POLYGON")) {
                is_unicode = true;
            }

            if (utf16_len > MAX_INLINE_CHAR) {
                // Strings > 4000 UTF-16 code units exceed SQL Server's inline NVARCHAR(MAX)
                // threshold. Switch to data-at-execution (DAE) streaming: ODBC driver pulls
                // data in chunks via SQLPutData, avoiding a single massive buffer allocation.
                // DAE path: match slow-path types exactly.
                // Non-unicode (ASCII) → SQL_VARCHAR + PARAM_C_TYPE_TEXT, which is
                //   SQL_C_WCHAR and matches the slow path's SQL_C_CHAR (numerically
                //   -8 == SQL_C_WCHAR — a long-standing alias in the Python layer).
                // Unicode → SQL_WVARCHAR + SQL_C_WCHAR (wide-char streaming)
                info.isDAE = true;
                info.columnSize = 0;
                info.utf16Len = utf16_len;
                info.dataPtr = borrow(obj);
                info.paramSQLType = is_unicode ? SQL_WVARCHAR : SQL_VARCHAR;
                info.paramCType = is_unicode ? SQL_C_WCHAR : PARAM_C_TYPE_TEXT;
            } else {
                info.columnSize = is_unicode ? utf16_len : length;
                info.paramSQLType = is_unicode ? SQL_WVARCHAR : SQL_VARCHAR;
                info.paramCType = is_unicode ? SQL_C_WCHAR : PARAM_C_TYPE_TEXT;
            }
            info.decimalDigits = 0;
            continue;
        }

        // --- bytes / bytearray (allow subclasses) ---
        if (PyBytes_Check(obj) || PyByteArray_Check(obj)) {
            Py_ssize_t length = PyBytes_Check(obj) ? PyBytes_Size(obj) : PyByteArray_Size(obj);
            info.paramSQLType = SQL_VARBINARY;
            info.paramCType = SQL_C_BINARY;
            info.decimalDigits = 0;
            if (length > MAX_INLINE_BINARY) {
                info.isDAE = true;
                info.columnSize = 0;
                info.dataPtr = borrow(obj);
            } else {
                info.columnSize = std::max<SQLULEN>(length, 1);
            }
            continue;
        }

        // --- datetime (must check before date, since datetime is subclass of date) ---
        if (PyDateTime_Check(obj)) {
            py::object tzinfo = steal(PyObject_GetAttrString(obj, "tzinfo"));
            if (!tzinfo) throw py::error_already_set();
            bool has_tz = (tzinfo.ptr() != Py_None);
            if (has_tz) {
                info.paramSQLType = SQL_SS_TIMESTAMPOFFSET;
                info.paramCType = SQL_C_SS_TIMESTAMPOFFSET;
                info.columnSize = 34;
                info.decimalDigits = 7;
            } else {
                info.paramSQLType = SQL_TYPE_TIMESTAMP;
                info.paramCType = SQL_C_TYPE_TIMESTAMP;
                info.columnSize = 26;
                info.decimalDigits = 6;
            }
            continue;
        }

        // --- date ---
        if (PyDate_Check(obj)) {
            info.paramSQLType = SQL_TYPE_DATE;
            info.paramCType = SQL_C_TYPE_DATE;
            info.columnSize = 10;
            info.decimalDigits = 0;
            continue;
        }

        // --- time (normalized to string for binding) ---
        if (PyTime_Check(obj)) {
            info.paramSQLType = SQL_TYPE_TIME;
            info.paramCType =
                PARAM_C_TYPE_TEXT;  // matches slow path (its SQL_C_CHAR is -8 = SQL_C_WCHAR)
            info.columnSize = 16;
            info.decimalDigits = 6;
            // Delegate to isoformat rather than formatting the raw H/M/S/us fields by hand.
            // Hand-formatting silently drops tzinfo (an aware time rendered as
            // "01:02:03.000004+05:30" became "01:02:03.000004") and ignores isoformat
            // overrides on time subclasses. The legacy path calls
            // isoformat(timespec="microseconds") via _normalize_time_param in cursor.py,
            // so calling the same method is what keeps the two paths in agreement.
            NormalizeTimeParam(params, i, info.columnSize);
            continue;
        }

        // --- Decimal ---
        int is_decimal = PyObject_IsInstance(obj, decimal_type);
        if (is_decimal == -1) throw py::error_already_set();
        if (is_decimal == 1) {
            py::object as_tuple_ptr = steal(PyObject_CallMethod(obj, "as_tuple", NULL));
            if (!as_tuple_ptr) throw py::error_already_set();

            py::object exponent_obj = steal(PyObject_GetAttrString(as_tuple_ptr.ptr(), "exponent"));
            if (!exponent_obj) throw py::error_already_set();

            // NaN / Infinity / sNaN: refuse rather than silently writing 0.
            if (PyUnicode_Check(exponent_obj.ptr())) {
                throw py::value_error(
                    "Cannot bind non-finite Decimal (NaN/Infinity) as SQL NUMERIC");
            }

            py::object digits_obj = steal(PyObject_GetAttrString(as_tuple_ptr.ptr(), "digits"));
            if (!digits_obj) throw py::error_already_set();

            if (!PyTuple_Check(digits_obj.ptr())) {
                throw py::type_error("Decimal.as_tuple().digits must be a tuple");
            }

            Py_ssize_t num_digits = PyTuple_GET_SIZE(digits_obj.ptr());

            // Read the exponent at full width and range-check it BEFORE narrowing to int.
            // Decimal exponents are arbitrary-precision, so a value like Decimal("1E+4294967297")
            // would otherwise truncate to 1 on LP64, sail past the precision gate below, and
            // silently bind 10. An out-of-range exponent cannot produce a bindable NUMERIC at
            // any precision, so treat overflow as precision overflow rather than propagating
            // OverflowError, matching what the legacy Python path reports.
            long long exponent_ll = PyLong_AsLongLong(exponent_obj.ptr());
            if (exponent_ll == -1 && PyErr_Occurred()) {
                PyErr_Clear();
                throw py::value_error(
                    "Precision of the numeric value is too high. "
                    "The maximum precision supported by SQL Server is " +
                    std::to_string(MAX_NUMERIC_PRECISION) + ".");
            }
            // Bound before any arithmetic or negation. MAX_NUMERIC_PRECISION on both sides is
            // wider than anything bindable, and keeps -exponent well clear of INT_MIN, whose
            // negation would be signed-overflow UB.
            if (exponent_ll > MAX_NUMERIC_PRECISION || exponent_ll < -MAX_NUMERIC_PRECISION) {
                throw py::value_error(
                    "Precision of the numeric value is too high. "
                    "The maximum precision supported by SQL Server is " +
                    std::to_string(MAX_NUMERIC_PRECISION) + ".");
            }
            int exponent = static_cast<int>(exponent_ll);

            // Digit count is likewise capped before it feeds the precision arithmetic.
            if (num_digits > MAX_NUMERIC_PRECISION) {
                throw py::value_error(
                    "Precision of the numeric value is too high. "
                    "The maximum precision supported by SQL Server is " +
                    std::to_string(MAX_NUMERIC_PRECISION) + ", but got " +
                    std::to_string(num_digits) + ".");
            }

            int precision;
            // Precision is total base-10 digits after applying Decimal's exponent: positive exponents
            // add trailing zeros, in-range negative exponents keep the original digit count, and larger
            // negative exponents force leading fractional zeros such as Decimal("0.001") -> precision 3.
            if (exponent >= 0)
                precision = static_cast<int>(num_digits) + exponent;
            else if ((-exponent) <= num_digits)
                precision = static_cast<int>(num_digits);
            else
                precision = -exponent;

            if (precision > MAX_NUMERIC_PRECISION) {
                throw py::value_error(
                    "Precision of the numeric value is too high. "
                    "The maximum precision supported by SQL Server is " +
                    std::to_string(MAX_NUMERIC_PRECISION) + ", but got " +
                    std::to_string(precision) + ".");
            }

            // Check SMALLMONEY first, then widen to MONEY, so common small values keep the narrowest
            // exact range while still accepting larger fixed-point values supported by SQL Server.
            // MONEY/SMALLMONEY: SQL Server stores these as fixed-point integers internally.
            // We bind as formatted VARCHAR (e.g., "214748.3647") because SQL_C_NUMERIC can't
            // represent the exact money range without precision loss on certain ODBC drivers.
            // Use exact Decimal comparison (not double) to avoid boundary misclassification.
            bool in_money_range = false;
            int cmp_ge = PyObject_RichCompareBool(obj, PyTypeCache::smallmoney_min, Py_GE);
            int cmp_le = PyObject_RichCompareBool(obj, PyTypeCache::smallmoney_max, Py_LE);
            if (cmp_ge == -1 || cmp_le == -1) throw py::error_already_set();
            if (cmp_ge == 1 && cmp_le == 1) {
                in_money_range = true;
            } else {
                cmp_ge = PyObject_RichCompareBool(obj, PyTypeCache::money_min, Py_GE);
                cmp_le = PyObject_RichCompareBool(obj, PyTypeCache::money_max, Py_LE);
                if (cmp_ge == -1 || cmp_le == -1) throw py::error_already_set();
                if (cmp_ge == 1 && cmp_le == 1) {
                    in_money_range = true;
                }
            }

            if (in_money_range) {
                PyObject* formatted = FormatDecimalParam(params, i, obj);
                info.paramSQLType = SQL_VARCHAR;
                info.paramCType = PARAM_C_TYPE_TEXT;
                info.columnSize = PyUnicode_GET_LENGTH(formatted);
                info.decimalDigits = 0;
                continue;
            }

            // Build SQL_NUMERIC_STRUCT from the Decimal object. Store as a pybind11-castable
            // object in the param list so BindParameters can extract it as NumericData.
            info.paramSQLType = SQL_NUMERIC;
            info.paramCType = SQL_C_NUMERIC;
            NumericData nd = build_numeric_data(as_tuple_ptr.ptr(), digits_obj.ptr(), exponent);
            info.columnSize = nd.precision;
            info.decimalDigits = nd.scale;
            // Store NumericData as a Python object in the param list for the binder.
            py::object numeric_obj = py::cast(nd);
            PyObject* raw = numeric_obj.release().ptr();
            if (PyList_SetItem(params, i, raw) != 0) {
                // PyList_SetItem steals (decrefs) the item even on failure.
                throw py::error_already_set();
            }
            continue;
        }

        // --- UUID ---
        int is_uuid = PyObject_IsInstance(obj, uuid_type);
        if (is_uuid == -1) throw py::error_already_set();
        if (is_uuid == 1) {
            PyObject* bytes_le = PyObject_GetAttrString(obj, "bytes_le");
            if (!bytes_le) throw py::error_already_set();
            info.paramSQLType = SQL_GUID;
            info.paramCType = SQL_C_GUID;
            info.columnSize = 16;
            info.decimalDigits = 0;
            if (PyList_SetItem(params, i, bytes_le) != 0) {
                // PyList_SetItem steals (decrefs) the item even on failure.
                throw py::error_already_set();
            }
            continue;
        }

        // --- Unknown type: raise TypeError (matches Python _map_sql_type) ---
        throw py::type_error(
            "Unsupported parameter type: The driver cannot safely convert it to a SQL type.");
    }

    return infos;
}

// Helper: build SQL_NUMERIC_STRUCT from an already-unpacked Decimal.as_tuple().
//
// Callers in DetectParamTypes have already called as_tuple() and pulled out the digits
// tuple and exponent, so those are passed in rather than re-entering Python to fetch
// them a second time.
//
// The mantissa is accumulated into a fixed 128-bit value held as four 32-bit limbs
// instead of Python bigint arithmetic. SQL Server caps NUMERIC precision at
// MAX_NUMERIC_PRECISION (38) digits and callers reject anything larger, so the value
// always fits the 16 bytes SQL_NUMERIC_STRUCT provides. Limbs keep this portable
// (MSVC has no __int128) and the result is written out byte-by-byte so host endianness
// does not matter.
inline NumericData build_numeric_data(PyObject* as_tuple, PyObject* digits, int exponent) {
    py::object sign_obj = steal(PyObject_GetAttrString(as_tuple, "sign"));
    if (!sign_obj) throw py::error_already_set();
    int sign_val = static_cast<int>(PyLong_AsLong(sign_obj.ptr()));
    if (sign_val == -1 && PyErr_Occurred()) throw py::error_already_set();

    if (!PyTuple_Check(digits)) {
        throw py::type_error("Decimal.as_tuple().digits must be a tuple");
    }

    // SQL Server precision counts all stored decimal digits, while scale is just the
    // fractional digits. A positive exponent moves trailing zeros into the integer part;
    // a negative exponent means scale = -exponent and precision must still cover leading
    // fractional zeros such as 0.001.
    const Py_ssize_t digit_count = PyTuple_GET_SIZE(digits);
    const int num_digits = static_cast<int>(digit_count);
    int precision, scale;
    if (exponent >= 0) {
        precision = num_digits + exponent;
        scale = 0;
    } else {
        scale = -exponent;
        precision = std::max(num_digits, scale);
    }
    precision = std::max(1, std::min(precision, MAX_NUMERIC_PRECISION));
    scale = std::min(scale, precision);

    // 128-bit magnitude as four little-endian 32-bit limbs. Returns the carry out of the
    // top limb, which is non-zero only if the value overflowed 128 bits.
    uint32_t limb[4] = {0, 0, 0, 0};
    auto mul10_add = [&limb](uint32_t addend) -> uint64_t {
        uint64_t carry = addend;
        for (int k = 0; k < 4; ++k) {
            uint64_t cur = static_cast<uint64_t>(limb[k]) * 10u + carry;
            limb[k] = static_cast<uint32_t>(cur);
            carry = cur >> 32;
        }
        return carry;
    };

    uint64_t overflow = 0;
    for (Py_ssize_t i = 0; i < digit_count; ++i) {
        PyObject* digit_obj = PyTuple_GET_ITEM(digits, i);
        long digit = PyLong_AsLong(digit_obj);
        if (digit == -1 && PyErr_Occurred()) throw py::error_already_set();
        overflow |= mul10_add(static_cast<uint32_t>(digit));
    }
    // A positive exponent means as_tuple() omitted trailing zeros, so Decimal("123e2")
    // must become mantissa 12300 before packing.
    for (int j = 0; j < exponent; ++j) {
        overflow |= mul10_add(0);
    }
    if (overflow != 0) {
        throw py::value_error("Decimal magnitude exceeds the 16-byte SQL NUMERIC capacity");
    }

    NumericData nd;
    nd.precision = static_cast<SQLCHAR>(precision);
    nd.scale = static_cast<SQLSCHAR>(scale);
    // SQL uses sign=1 for positive and sign=0 for negative, the inverse of
    // Decimal.as_tuple().sign.
    nd.sign = (sign_val == 0) ? 1 : 0;
    nd.val.assign(SQL_MAX_NUMERIC_LEN, '\0');
    for (int k = 0; k < 4; ++k) {
        nd.val[k * 4 + 0] = static_cast<char>(limb[k] & 0xFF);
        nd.val[k * 4 + 1] = static_cast<char>((limb[k] >> 8) & 0xFF);
        nd.val[k * 4 + 2] = static_cast<char>((limb[k] >> 16) & 0xFF);
        nd.val[k * 4 + 3] = static_cast<char>((limb[k] >> 24) & 0xFF);
    }
    return nd;
}
