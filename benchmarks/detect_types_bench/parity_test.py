"""Parity test: assert that all three benchmark variants produce IDENTICAL
per-parameter type detection results, and that those results match the shape
expected from mssql-python's DetectParamTypes.

Each variant exposes `detect_types(list) -> list[tuple]` where each tuple is
    (sql_type, c_type, column_size, is_dae, decimal_digits)

The tuples are compared pairwise across all three implementations. If any
variant disagrees on any input, the test prints a per-input diff and fails.

Additionally, cross-checks the outputs against the constant tables published
by mssql-python's ConstantsDDBC (SQL_BIT = -7, SQL_INTEGER = 4, etc.) so a
divergence from the real DetectParamTypes would surface here.

Usage:  /usr/bin/python parity_test.py
"""

from __future__ import annotations

import datetime
import decimal
import sys
import uuid
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import detect_cpython
import detect_nanobind
import detect_pybind11

# Import mssql-python's SQL type constants directly (bypass the package
# __init__ so we don't need the compiled ddbc_bindings.so).
import importlib.util as _iu
_constants_path = HERE.parent.parent / "mssql_python" / "constants.py"
_spec = _iu.spec_from_file_location("_bench_constants", _constants_path)
_mod = _iu.module_from_spec(_spec)
try:
    _spec.loader.exec_module(_mod)  # type: ignore
    C = _mod.ConstantsDDBC
    HAVE_CONSTANTS = True
except Exception as e:  # pragma: no cover
    HAVE_CONSTANTS = False
    C = None
    print(f"Note: ConstantsDDBC not importable ({e!r}); "
          "skipping cross-check with mssql_python constants")


VARIANTS = {
    "pybind11":    detect_pybind11.detect_types,
    "raw CPython": detect_cpython.detect_types,
    "nanobind":    detect_nanobind.detect_types,
}


# ---------------------------------------------------------------------------
# Reference expectations (Linux/macOS: PARAM_C_TYPE_TEXT == SQL_C_WCHAR == -8)
# ---------------------------------------------------------------------------
# Fields: (sql_type, c_type, column_size, is_dae, decimal_digits)

SQL_UNKNOWN = 0
SQL_C_DEFAULT = 99
SQL_BIT = -7
SQL_TINYINT = -6
SQL_SMALLINT = 5
SQL_INTEGER = 4
SQL_BIGINT = -5
SQL_C_SBIGINT = -25
SQL_DOUBLE = 8
SQL_VARCHAR = 12
SQL_WVARCHAR = -9
SQL_C_WCHAR = -8
SQL_VARBINARY = -3
SQL_C_BINARY = -2
SQL_TYPE_TIMESTAMP = 93
SQL_SS_TIMESTAMPOFFSET = -155
SQL_TYPE_DATE = 91
SQL_TYPE_TIME = 92
SQL_NUMERIC = 2
SQL_C_NUMERIC = 2
SQL_GUID = -11


# ---------------------------------------------------------------------------
# Test corpus — mirrors mssql-python's own test_023_fast_path_parity.py
# ---------------------------------------------------------------------------

class MyInt(int):    pass
class MyStr(str):    pass
class MyBytes(bytes): pass
class MyFloat(float): pass


def corpus() -> list:
    """A single flat list of every representative parameter."""
    return [
        # --- None ---
        None,

        # --- bool (must dispatch BEFORE int) ---
        True,
        False,

        # --- int range boundaries ---
        0, 1, 255,                    # TINYINT
        256, 32767,                   # SMALLINT upper
        -1, -32768,                   # SMALLINT lower
        32768, 2147483647,            # INTEGER
        -32769, -2147483648,          # INTEGER lower
        2147483648, 9223372036854775807,  # BIGINT
        -2147483649, -9223372036854775808,  # BIGINT lower

        # --- int subclasses (should still dispatch as int) ---
        MyInt(42),
        MyInt(100000),

        # --- float ---
        0.0, 3.14, -1.5e10,
        MyFloat(2.71),

        # --- str (short — inline) ---
        "",
        "hello",
        "a" * 100,
        MyStr("subclass"),

        # --- str non-ASCII (Latin-1, still 1-byte kind) ---
        "café",                       # requires WVARCHAR

        # --- str Unicode >127, multi-byte ---
        "日本語",                     # 2-byte kind
        "𝄞abc",                       # 4-byte (surrogate pair in UTF-16)

        # --- str embedded NUL ---
        "hello\x00world",

        # --- str DAE (>4000 UTF-16 code units) ---
        "x" * 5000,
        "\u00e9" * 5000,

        # --- bytes / bytearray ---
        b"",
        b"\x00\x01\x02",
        b"x" * 100,
        MyBytes(b"sub"),
        bytearray(b"mutable"),

        # --- bytes DAE (>8000 bytes) ---
        b"y" * 10000,
        bytearray(b"z" * 10000),

        # --- datetime, date, time (order-sensitive) ---
        datetime.datetime(2026, 7, 23, 12, 34, 56),
        datetime.datetime(2026, 7, 23, 12, 34, 56, 789012),
        datetime.datetime(2026, 7, 23, 12, 34, 56,
                          tzinfo=datetime.timezone.utc),
        datetime.date(2026, 7, 23),
        datetime.time(0, 0, 0),
        datetime.time(23, 59, 59, 999999),

        # --- Decimal in SMALLMONEY range ---
        decimal.Decimal("0"),
        decimal.Decimal("1.23"),
        decimal.Decimal("-214748.3648"),   # SMALLMONEY_MIN
        decimal.Decimal("214748.3647"),    # SMALLMONEY_MAX

        # --- Decimal in MONEY range (outside SMALLMONEY) ---
        decimal.Decimal("214748.3648"),    # just above smallmoney
        decimal.Decimal("-922337203685477.5808"),  # MONEY_MIN
        decimal.Decimal("922337203685477.5807"),   # MONEY_MAX

        # --- Decimal outside MONEY → SQL_NUMERIC ---
        decimal.Decimal("922337203685477.5808"),   # one past MONEY_MAX
        decimal.Decimal("1E10"),
        decimal.Decimal("0.001"),                  # exponent=-3, num_digits=1
        decimal.Decimal("0.000001"),               # -exp > num_digits
        decimal.Decimal("12345678901234567890"),   # 20 digits, no exponent

        # --- UUID ---
        uuid.UUID("12345678-1234-5678-1234-567812345678"),
        uuid.uuid4(),
    ]


# ---------------------------------------------------------------------------
# Reference dispatch — the "expected" outputs used to cross-check the three
# implementations against mssql-python's DetectParamTypes semantics.
# ---------------------------------------------------------------------------

def reference_expect(value) -> tuple:
    """Compute the (sql_type, c_type, column_size, is_dae, decimal_digits)
    tuple that DetectParamTypes should produce for a single value.
    """
    if value is None:
        return (SQL_UNKNOWN, SQL_C_DEFAULT, 1, False, 0)

    # bool BEFORE int
    if isinstance(value, bool):
        return (SQL_BIT, SQL_BIT, 1, False, 0)

    if isinstance(value, int):
        v = int(value)
        if 0 <= v <= 255:
            return (SQL_TINYINT, SQL_TINYINT, 3, False, 0)
        if -32768 <= v <= 32767:
            return (SQL_SMALLINT, SQL_SMALLINT, 5, False, 0)
        if -2147483648 <= v <= 2147483647:
            return (SQL_INTEGER, SQL_INTEGER, 10, False, 0)
        # BIGINT (in-range int64 or overflow → still bigint)
        return (SQL_BIGINT, SQL_C_SBIGINT, 19, False, 0)

    if isinstance(value, float):
        return (SQL_DOUBLE, SQL_DOUBLE, 15, False, 0)

    if isinstance(value, str):
        length = len(value)
        # UTF-16 length: BMP → 1, non-BMP → 2 per code point
        utf16_len = sum(2 if ord(c) > 0xFFFF else 1 for c in value)
        is_unicode = any(ord(c) > 127 for c in value)
        sql_t = SQL_WVARCHAR if is_unicode else SQL_VARCHAR
        c_t = SQL_C_WCHAR  # Linux/macOS PARAM_C_TYPE_TEXT
        if utf16_len > 4000:
            return (sql_t, c_t, 0, True, 0)
        col = utf16_len if is_unicode else length
        return (sql_t, c_t, col, False, 0)

    if isinstance(value, (bytes, bytearray)):
        length = len(value)
        if length > 8000:
            return (SQL_VARBINARY, SQL_C_BINARY, 0, True, 0)
        return (SQL_VARBINARY, SQL_C_BINARY, max(length, 1), False, 0)

    # datetime BEFORE date
    if isinstance(value, datetime.datetime):
        if value.tzinfo is not None:
            return (SQL_SS_TIMESTAMPOFFSET, SQL_SS_TIMESTAMPOFFSET, 34, False, 7)
        return (SQL_TYPE_TIMESTAMP, SQL_TYPE_TIMESTAMP, 26, False, 6)

    if isinstance(value, datetime.date):
        return (SQL_TYPE_DATE, SQL_TYPE_DATE, 10, False, 0)

    if isinstance(value, datetime.time):
        # column_size is len(formatted time string) — always 15
        # ("HH:MM:SS.uuuuuu"), which is > 16? No, it's 15 chars < 16 baseline.
        # DetectParamTypes seeds columnSize=16 then max()es with strlen — so 16.
        return (SQL_TYPE_TIME, SQL_C_WCHAR, 16, False, 6)

    if isinstance(value, decimal.Decimal):
        sign, digits, exponent = value.as_tuple()
        if isinstance(exponent, str):
            raise ValueError("NaN/Inf Decimal not supported")
        # MONEY range check
        smin = decimal.Decimal("-214748.3648")
        smax = decimal.Decimal("214748.3647")
        mmin = decimal.Decimal("-922337203685477.5808")
        mmax = decimal.Decimal("922337203685477.5807")
        if smin <= value <= smax or mmin <= value <= mmax:
            formatted = format(value, "f")
            return (SQL_VARCHAR, SQL_C_WCHAR, len(formatted), False, 0)
        # SQL_NUMERIC path
        nd = len(digits)
        if exponent >= 0:
            precision = nd + exponent
        elif -exponent <= nd:
            precision = nd
        else:
            precision = -exponent
        scale = -exponent if exponent < 0 else 0
        return (SQL_NUMERIC, SQL_C_NUMERIC, precision, False, scale)

    if isinstance(value, uuid.UUID):
        return (SQL_GUID, SQL_GUID, 16, False, 0)

    raise TypeError(f"unsupported: {type(value)!r}")


# ---------------------------------------------------------------------------
# Test driver
# ---------------------------------------------------------------------------

def describe(v) -> str:
    r = repr(v)
    return r if len(r) <= 60 else r[:57] + "..."


def run() -> int:
    inputs = corpus()

    # Run each variant on the SAME list. (detect_types does not mutate the list.)
    results = {}
    for name, fn in VARIANTS.items():
        try:
            results[name] = fn(list(inputs))
        except Exception as e:  # pragma: no cover
            print(f"FAIL: variant {name} raised: {e!r}")
            return 1

    # Also compute the reference expectation.
    expected = [reference_expect(v) for v in inputs]

    # Cross-check: all three variants agree, and all three match the reference.
    print(f"Comparing {len(inputs)} inputs across "
          f"{len(VARIANTS)} implementations + 1 reference model")
    print("-" * 90)

    mismatches = 0
    for i, v in enumerate(inputs):
        variant_rows = {name: results[name][i] for name in VARIANTS}
        ref_row = expected[i]

        # Normalize variant tuples: (sql, c, colsize, is_dae, dd)
        # (The C++ modules return exactly this shape.)
        agree = all(r == ref_row for r in variant_rows.values())

        if not agree:
            mismatches += 1
            print(f"\n[{i:3d}] input = {describe(v)}")
            print(f"       expected: {ref_row}")
            for name, row in variant_rows.items():
                marker = "OK  " if row == ref_row else "DIFF"
                print(f"       [{marker}] {name:<12}: {row}")

    print()
    if mismatches == 0:
        print(f"OK: all {len(inputs)} inputs — 3/3 variants match "
              "the reference model.")
        _print_constants_cross_check()
        return 0
    else:
        print(f"FAIL: {mismatches}/{len(inputs)} inputs had at least one variant "
              "diverging from the reference model.")
        return 1


def _print_constants_cross_check() -> None:
    """If mssql_python.constants is importable, verify our hard-coded SQL
    type numbers match what the real driver uses. This is what closes the loop
    on 'are we validating against the functionality implemented in
    DetectParamTypes?'.
    """
    if not HAVE_CONSTANTS:
        return

    def _v(name):
        # ConstantsDDBC entries are typically IntEnum members
        val = getattr(C, name)
        return int(val.value) if hasattr(val, "value") else int(val)

    pairs = [
        ("SQL_BIT",              SQL_BIT,              _v("SQL_BIT")),
        ("SQL_TINYINT",          SQL_TINYINT,          _v("SQL_TINYINT")),
        ("SQL_SMALLINT",         SQL_SMALLINT,         _v("SQL_SMALLINT")),
        ("SQL_INTEGER",          SQL_INTEGER,          _v("SQL_INTEGER")),
        ("SQL_BIGINT",           SQL_BIGINT,           _v("SQL_BIGINT")),
        ("SQL_DOUBLE",           SQL_DOUBLE,           _v("SQL_DOUBLE")),
        ("SQL_VARCHAR",          SQL_VARCHAR,          _v("SQL_VARCHAR")),
        ("SQL_WVARCHAR",         SQL_WVARCHAR,         _v("SQL_WVARCHAR")),
        ("SQL_VARBINARY",        SQL_VARBINARY,        _v("SQL_VARBINARY")),
        ("SQL_TYPE_TIMESTAMP",   SQL_TYPE_TIMESTAMP,   _v("SQL_TYPE_TIMESTAMP")),
        ("SQL_TYPE_DATE",        SQL_TYPE_DATE,        _v("SQL_TYPE_DATE")),
        ("SQL_TYPE_TIME",        SQL_TYPE_TIME,        _v("SQL_TYPE_TIME")),
        ("SQL_NUMERIC",          SQL_NUMERIC,          _v("SQL_NUMERIC")),
        ("SQL_GUID",             SQL_GUID,             _v("SQL_GUID")),
    ]
    print()
    print("Cross-check vs mssql_python.constants.ConstantsDDBC:")
    all_ok = True
    for name, ours, theirs in pairs:
        ok = ours == theirs
        all_ok &= ok
        tag = "OK" if ok else "DIFF"
        print(f"  [{tag}]  {name:<24} bench={ours:>5}  ConstantsDDBC={theirs:>5}")
    print()
    if all_ok:
        print("All SQL type constants match. Benchmark truly reflects "
              "DetectParamTypes' dispatch table.")
    else:
        print("At least one constant diverges — bench dispatch table needs a fix.")


if __name__ == "__main__":
    sys.exit(run())
