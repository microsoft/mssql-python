"""
Coverage for the two parameter paths, each tested on its own terms — no path is
forced through a door real callers do not use.

1. Native path (C++ DetectParamTypes + DDBCSQLExecute), with and without
   ``setinputsizes`` overrides. Exercised end to end through ``cursor.execute(...)``.
2. Python type detection (``_map_sql_type`` / ``_get_numeric_data``) — the reference
   the native path was ported from. Asserted directly as a pure function: value in,
   (SQL type, C type, column size, decimal digits, DAE) out. No DB round-trip, so
   the assertion cannot be masked by SQL Server coercing a wrong-but-convertible
   type back to the right value.
Uses the project's `cursor` fixture from conftest.py so the tests work in any
environment that runs the rest of the suite.
"""

import datetime
import decimal
import gc
import uuid
import weakref

import pytest

from mssql_python.constants import ConstantsDDBC as ddbc_sql_const

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _standard_roundtrip(cursor, value):
    """Native path: no setinputsizes, so C++ detects the types."""
    cursor.execute("SELECT ?", [value])
    return cursor.fetchone()[0]


def _override_roundtrip(cursor, value, sql_type, column_size):
    """Native execute with a caller-declared SQL type."""
    cursor.setinputsizes([(sql_type, column_size, 0)])
    try:
        cursor.execute("SELECT ?", [value])
        return cursor.fetchone()[0]
    finally:
        cursor.setinputsizes(None)


# ---------------------------------------------------------------------------
# Standard-path coverage: representative type matrix
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        # int range detection (TINYINT / SMALLINT / INTEGER / BIGINT)
        0,
        1,
        255,
        256,
        32767,
        32768,
        2147483647,
        2147483648,
        -1,
        -32768,
        -2147483648,
        # bool
        True,
        False,
        # float
        0.0,
        3.14,
        -1.5e10,
        # str (ASCII inline)
        "",
        "hello",
        "a" * 100,
        # bytes
        b"",
        b"\x00\x01\x02",
        b"x" * 100,
    ],
)
def test_standard_path_basic_types(cursor, value):
    """Standard path round-trips representative scalar types correctly."""
    result = _standard_roundtrip(cursor, value)
    assert result == value, (
        f"Standard-path roundtrip mismatch for {type(value).__name__} {value!r}: " f"got {result!r}"
    )


# ---------------------------------------------------------------------------
# Subclass support — regression for the *_CheckExact bug from PR review
# ---------------------------------------------------------------------------


def test_int_subclass(cursor):
    class MyInt(int):
        pass

    assert _standard_roundtrip(cursor, MyInt(42)) == 42


def test_str_subclass(cursor):
    class MyStr(str):
        pass

    assert _standard_roundtrip(cursor, MyStr("hello")) == "hello"


def test_bytes_subclass(cursor):
    class MyBytes(bytes):
        pass

    assert _standard_roundtrip(cursor, MyBytes(b"hello")) == b"hello"


def test_float_subclass(cursor):
    class MyFloat(float):
        pass

    assert _standard_roundtrip(cursor, MyFloat(3.14)) == 3.14


# ---------------------------------------------------------------------------
# Caller-list isolation and refcount safety
# ---------------------------------------------------------------------------


def test_caller_param_list_not_mutated(cursor):
    """DetectParamTypes must not mutate the caller's parameter list."""
    params = ["hello", 42, 3.14, datetime.date(2024, 1, 1), uuid.uuid4()]
    snapshot = list(params)
    cursor.execute("SELECT ?, ?, ?, ?, ?", params)
    cursor.fetchone()
    assert params == snapshot, f"Caller list was mutated: {params} != {snapshot}"


def test_no_refcount_leak_on_in_place_replacement(cursor):
    """Decimal/UUID/time params get replaced in-place inside DetectParamTypes
    via PyList_SetItem. The replaced object must have its reference dropped —
    a regression caught in PR review where PyList_SET_ITEM (uppercase, no
    decref) leaked one reference per replaced item per execute."""

    class TrackedDec(decimal.Decimal):
        pass

    td = TrackedDec("123.45")
    ref = weakref.ref(td)
    params = [td]
    del td  # drop our local strong reference

    cursor.execute("SELECT ?", params)
    cursor.fetchone()
    del params  # drop the list's strong reference
    gc.collect()

    assert ref() is None, (
        "Decimal parameter was leaked: PyList_SetItem must decref the old "
        "slot before stealing the new reference."
    )


# ---------------------------------------------------------------------------
# Error semantics
# ---------------------------------------------------------------------------


def test_unsupported_type_raises_typeerror(cursor):
    """Standard path must raise TypeError for unknown parameter types — matching
    the legacy path's `_map_sql_type` final branch."""
    with pytest.raises(TypeError):
        cursor.execute("SELECT ?", [{1, 2, 3}])  # set is not bindable


def test_decimal_nan_rejected(cursor):
    """Non-finite Decimals must raise rather than silently bind as 0."""
    with pytest.raises(ValueError):
        cursor.execute("SELECT ?", [decimal.Decimal("NaN")])


@pytest.mark.parametrize(
    "exp",
    [
        2**32 + 1,  # truncated to 1 by a 32-bit narrowing cast
        2**31,  # truncates to exactly INT_MIN; negating that is signed-overflow UB
        2**31 - 1,  # INT_MAX
        -(2**32 + 1),
        -(2**31),
        39,  # first out-of-range exponent that needs no truncation to be invalid
        -39,
    ],
)
def test_decimal_out_of_range_exponent_rejected(cursor, exp):
    """Exponents beyond SQL Server's 38-digit precision must raise, including ones
    that only look valid after a 32-bit narrowing cast.

    Regression guard: the exponent used to be cast to int before being range
    checked, so Decimal("1E+4294967297") truncated to 1, passed the precision
    gate, and silently bound 10 while the legacy path raised.
    """
    with pytest.raises(Exception) as excinfo:
        cursor.execute("SELECT ?", [decimal.Decimal(f"1E{exp:+d}")])
        cursor.fetchone()
    # the failure must be about precision, not an OverflowError leaking from the cast
    assert not isinstance(excinfo.value, OverflowError)


@pytest.mark.parametrize("exp", [37, -38, 0])
def test_decimal_in_range_exponent_still_binds(cursor, exp):
    """The range check must not reject exponents SQL Server can represent."""
    value = decimal.Decimal(f"1E{exp:+d}")
    cursor.execute("SELECT ?", [value])
    assert cursor.fetchone()[0] is not None


def test_aware_time_matches_legacy(cursor):
    """A tz-aware datetime.time must behave the same on both paths.

    SQL Server's TIME has no UTC offset, so isoformat's "+05:30" cannot bind and
    both paths reject it. The standard path used to hand-format the raw H/M/S/us
    fields, which silently dropped the offset and bound a different time than the
    caller passed while the legacy path raised.
    """
    aware = datetime.time(
        1, 2, 3, 4, tzinfo=datetime.timezone(datetime.timedelta(hours=5, minutes=30))
    )
    with pytest.raises(Exception):
        cursor.execute("SELECT ?", [aware])
        cursor.fetchone()


def test_naive_time_roundtrips(cursor):
    """Naive times are unaffected by the aware-time handling above."""
    naive = datetime.time(1, 2, 3, 4)
    assert _standard_roundtrip(cursor, naive) == naive


# ---------------------------------------------------------------------------
# Native execute path: user-supplied type overrides via setinputsizes
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value, sql_type, column_size",
    [
        ("hello", ddbc_sql_const.SQL_VARCHAR.value, 5),
        (42, ddbc_sql_const.SQL_INTEGER.value, 0),
        (3.14, ddbc_sql_const.SQL_DOUBLE.value, 0),
        (b"data", ddbc_sql_const.SQL_VARBINARY.value, 4),
    ],
)
def test_setinputsizes_override_roundtrips(cursor, value, sql_type, column_size):
    """A user-declared type round-trips through native execute."""
    assert _override_roundtrip(cursor, value, sql_type, column_size) == value


@pytest.mark.parametrize(
    "size",
    [
        ddbc_sql_const.SQL_INTEGER.value,
        (ddbc_sql_const.SQL_INTEGER.value,),
        (ddbc_sql_const.SQL_INTEGER.value, 0),
    ],
)
def test_setinputsizes_short_forms_use_native_execute(cursor, size):
    """Every documented shorthand is normalized for the native path."""
    cursor.setinputsizes([size])
    cursor.execute("SELECT ?", [42])
    assert cursor.fetchone()[0] == 42


def test_setinputsizes_shorter_than_params_detects_the_rest(cursor):
    """setinputsizes with fewer entries than parameters: covered indices use the
    override, uncovered ones fall through to DetectParamTypes.

    (A None entry cannot be used here — setinputsizes validates and rejects None.)
    """
    cursor.setinputsizes([(ddbc_sql_const.SQL_VARCHAR.value, 5, 0)])
    try:
        with pytest.warns(Warning):  # count mismatch is warned, then execution proceeds
            cursor.execute("SELECT ?, ?", ["hello", 42])
        row = cursor.fetchone()
        assert row[0] == "hello"
        assert row[1] == 42
    finally:
        cursor.setinputsizes(None)


def test_setinputsizes_text_binding_normalizes_time(cursor):
    """Text overrides preserve the DB-API time normalization contract."""
    value = datetime.time(1, 2, 3, 4)
    assert (
        _override_roundtrip(cursor, value, ddbc_sql_const.SQL_VARCHAR.value, 32)
        == "01:02:03.000004"
    )


@pytest.mark.parametrize("sql_type", [None, ddbc_sql_const.SQL_VARCHAR.value])
def test_time_isoformat_must_return_string(cursor, sql_type):
    """Native time normalization rejects a broken subclass contract on either path."""

    class BadTime(datetime.time):
        def isoformat(self, *args, **kwargs):
            return 42

    if sql_type is not None:
        cursor.setinputsizes([(sql_type, 32, 0)])
    try:
        with pytest.raises(TypeError, match=r"isoformat\(\) must return a str"):
            cursor.execute("SELECT ?", [BadTime(1, 2, 3)])
    finally:
        cursor.setinputsizes(None)


def test_setinputsizes_binary_dae(cursor):
    """Declared binary sizes over 8000 stream through the native DAE path."""
    value = b"\xab" * 10000
    cursor.setinputsizes([(ddbc_sql_const.SQL_LONGVARBINARY.value, len(value), 0)])
    try:
        cursor.execute("SELECT DATALENGTH(CAST(? AS VARBINARY(MAX)))", [value])
        assert cursor.fetchone()[0] == len(value)
    finally:
        cursor.setinputsizes(None)


def test_setinputsizes_numeric_precision_and_scale_are_clamped(cursor):
    """Oversized numeric metadata is clamped before narrowing to ODBC types."""
    value = decimal.Decimal("0.1")
    cursor.setinputsizes([(ddbc_sql_const.SQL_DECIMAL.value, 10**100, 10**100)])
    try:
        cursor.execute("SELECT ?", [value])
        assert cursor.fetchone()[0] == value
    finally:
        cursor.setinputsizes(None)


# ---------------------------------------------------------------------------
# Edge case tests (issues caught in rubber-duck review)
# ---------------------------------------------------------------------------


def test_large_bytearray_dae(cursor):
    """Large bytearray (>8000 bytes) must stream via DAE without crashing.
    This catches the pybind11 bytes-cast-from-bytearray bug."""
    large_ba = bytearray(b"\xab" * 10000)
    cursor.execute("SELECT DATALENGTH(CAST(? AS VARBINARY(MAX)))", [large_ba])
    result = cursor.fetchone()[0]
    assert result == 10000


def test_large_bytes_dae(cursor):
    """Large bytes (>8000 bytes) must stream via DAE correctly."""
    large_b = b"\xcd" * 10000
    cursor.execute("SELECT DATALENGTH(CAST(? AS VARBINARY(MAX)))", [large_b])
    result = cursor.fetchone()[0]
    assert result == 10000


def test_large_string_dae(cursor):
    """Large string (>4000 chars) must stream via DAE correctly."""
    large_str = "x" * 5000
    cursor.execute("SELECT LEN(?)", [large_str])
    result = cursor.fetchone()[0]
    assert result == 5000


def test_large_unicode_string_dae(cursor):
    """Large unicode string (>4000 UTF-16 code units) streams via DAE."""
    large_str = "\u00e9" * 5000  # é = 1 UTF-16 code unit each
    cursor.execute("SELECT LEN(?)", [large_str])
    result = cursor.fetchone()[0]
    assert result == 5000


@pytest.mark.parametrize(
    "value",
    [
        decimal.Decimal("-922337203685477.5808"),  # MONEY_MIN boundary
        decimal.Decimal("922337203685477.5807"),  # MONEY_MAX boundary
        decimal.Decimal("-214748.3648"),  # SMALLMONEY_MIN
        decimal.Decimal("214748.3647"),  # SMALLMONEY_MAX
        decimal.Decimal("0.01"),  # typical money value
    ],
)
def test_decimal_money_boundary(cursor, value):
    """Decimal values at MONEY/SMALLMONEY boundaries must round-trip correctly."""
    cursor.execute("SELECT CAST(? AS DECIMAL(38,4))", [value])
    result = cursor.fetchone()[0]
    assert result == value, f"MONEY boundary mismatch: sent {value}, got {result}"


def test_decimal_outside_money_uses_numeric(cursor):
    """Decimal outside MONEY range must use SQL_NUMERIC binding."""
    # One unit above MONEY_MAX
    value = decimal.Decimal("922337203685477.5808")
    cursor.execute("SELECT CAST(? AS DECIMAL(38,4))", [value])
    result = cursor.fetchone()[0]
    assert result == value


def test_decimal_infinity_rejected(cursor):
    """Decimal('Infinity') must raise, not silently bind as 0."""
    with pytest.raises(ValueError):
        cursor.execute("SELECT ?", [decimal.Decimal("Infinity")])


def test_binary_with_embedded_nulls(cursor):
    """Binary data with embedded null bytes must not be truncated."""
    data = b"\x00\x01\x00\x02\x00"
    cursor.execute("SELECT DATALENGTH(CAST(? AS VARBINARY(MAX)))", [data])
    result = cursor.fetchone()[0]
    assert result == 5


def test_string_with_embedded_nulls(cursor):
    """String with embedded NUL chars must not be truncated."""
    value = "hello\x00world"
    cursor.execute("SELECT LEN(?)", [value])
    result = cursor.fetchone()[0]
    assert result == 11


def test_integer_overflow_detected(cursor):
    """Integers beyond int64 range are rejected at detect time on both paths with an
    identical clear message, not a generic cast failure deep in binding. SQL Server has
    no integer type wider than BIGINT (signed 64-bit)."""
    for value in (2**63, -(2**63) - 1, 2**64):
        expected = f"integer {value} is out of range for SQL BIGINT [-2^63, 2^63-1]"
        # Native path (C++ DetectParamTypes) via execute().
        with pytest.raises(ValueError) as native_exc:
            cursor.execute("SELECT ?", [value])
        # Legacy path (Python _map_sql_type) directly.
        with pytest.raises(ValueError) as legacy_exc:
            cursor._map_sql_type(value, [value], 0)
        # Parity contract: byte-identical message on both paths.
        assert str(native_exc.value) == expected
        assert str(legacy_exc.value) == expected


def test_integer_bigint_boundary_still_binds(cursor):
    """The int64 boundaries themselves are valid BIGINT and must still round-trip."""
    for value in (2**63 - 1, -(2**63)):
        cursor.execute("SELECT ?", [value])
        assert cursor.fetchone()[0] == value


@pytest.mark.parametrize("value", [decimal.Decimal("NaN"), decimal.Decimal("sNaN")])
def test_decimal_nan_variants_rejected(cursor, value):
    """Decimal NaN variants must raise, not silently bind as 0."""
    with pytest.raises(ValueError):
        cursor.execute("SELECT ?", [value])


NON_FINITE_DECIMALS = [
    decimal.Decimal("NaN"),
    decimal.Decimal("-NaN"),
    decimal.Decimal("sNaN"),
    decimal.Decimal("Infinity"),
    decimal.Decimal("-Infinity"),
]


@pytest.mark.parametrize("value", NON_FINITE_DECIMALS, ids=str)
def test_non_finite_decimal_exception_type_parity(cursor, value):
    """Both paths must reject non-finite Decimals with the *same* exception type.

    Before this was made explicit, the two paths diverged on type: the native
    detector raised ValueError, while the legacy path raised decimal.InvalidOperation
    for NaN (from the MONEY range comparison) and TypeError for Infinity (from
    comparing a str exponent against an int inside `_get_numeric_data`). Callers
    writing `except ValueError` therefore saw different behaviour depending on
    whether setinputsizes happened to be set.
    """
    # Native path — C++ DetectParamTypes, reached through execute().
    with pytest.raises(ValueError) as native_exc:
        cursor.execute("SELECT ?", [value])

    # Legacy path — Python type detection. Called directly because setinputsizes,
    # the only way to reach the legacy branch from execute(), bypasses _map_sql_type.
    params = [value]
    with pytest.raises(ValueError) as legacy_exc:
        cursor._map_sql_type(value, params, 0)

    assert "non-finite" in str(native_exc.value).lower()
    assert "non-finite" in str(legacy_exc.value).lower()


@pytest.mark.parametrize("value", NON_FINITE_DECIMALS, ids=str)
def test_get_numeric_data_rejects_non_finite(cursor, value):
    """`_get_numeric_data` is also reachable from executemany's typing pass, so it
    needs the same explicit rejection rather than falling through to precision=38
    and packing a silent zero."""
    with pytest.raises(ValueError, match="non-finite"):
        cursor._get_numeric_data(value)


def test_decimal_precision_overflow_rejected(cursor):
    """Decimals beyond SQL Server's max precision must raise."""
    with pytest.raises(Exception):
        cursor.execute("SELECT ?", [decimal.Decimal("123456789012345678901234567890123456789")])


# ---------------------------------------------------------------------------
# Text parameter C-type parity (all platforms bind wide)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        "",
        "plain ascii",
        "a" * 4000,  # inline boundary
        "a" * 4001,  # DAE boundary
        "café",  # non-ASCII, forces the unicode branch
        "naïve café ☕",
        "日本語テキスト",
        "mixed ascii and 日本語",
        "with 'quote' and \"double\"",
    ],
    ids=repr,
)
def test_text_params_bind_wide_on_every_platform(cursor, value):
    """Text parameters must survive a round-trip identically on every platform.

    The native detector used to resolve its text C type to a real `SQL_C_CHAR (1)`
    on Windows while using `SQL_C_WCHAR` on Linux/macOS. The legacy path binds with
    the Python layer's `SQL_C_CHAR` constant, which is numerically -8 — that is
    ODBC's `SQL_C_WCHAR` — so the legacy path has always bound wide everywhere.
    Windows was therefore the only platform where the two paths disagreed on C type
    and on the driver-side encoding path. This test pins the round-trip behaviour so
    a reintroduced narrow binding shows up as a Windows-only failure.
    """
    cursor.execute("SELECT ?", [value])
    assert cursor.fetchone()[0] == value


def test_ascii_text_roundtrip_into_nvarchar_column(cursor):
    """ASCII strings take the `SQL_VARCHAR` + wide-C-type combination, which is the
    exact pairing that differed on Windows. Round-trip through a real NVARCHAR column
    so the driver's conversion is exercised, not just SELECT ? echo."""
    cursor.execute("SELECT CAST(? AS NVARCHAR(100))", ["ascii only"])
    assert cursor.fetchone()[0] == "ascii only"

    cursor.execute("SELECT CAST(? AS NVARCHAR(100))", ["café ☕"])
    assert cursor.fetchone()[0] == "café ☕"


def test_time_param_binds_wide(cursor):
    """`datetime.time` is normalized to a string and bound with the same text C type,
    so it shares the Windows narrow/wide divergence."""
    value = datetime.time(1, 2, 3, 4)
    cursor.execute("SELECT CAST(? AS TIME(6))", [value])
    assert cursor.fetchone()[0] == value


def test_money_range_decimal_binds_wide(cursor):
    """Decimals inside the MONEY range are formatted to text and bound with the text
    C type, the third consumer of the platform-dependent constant."""
    value = decimal.Decimal("214748.3647")
    cursor.execute("SELECT CAST(? AS MONEY)", [value])
    assert cursor.fetchone()[0] == value


# ---------------------------------------------------------------------------
# Python type detection, asserted directly as a pure function.
#
# _map_sql_type(value, [value], 0) returns the 5-tuple
#   (SQL type, C type, column size, decimal digits, DAE)
# with no DB round-trip, so a wrong type cannot be hidden by SQL Server coercing
# the value back. A fresh single-element list is passed per call because the
# function mutates its slot in place for numeric / uuid / money / time.
# ---------------------------------------------------------------------------

_c = ddbc_sql_const


def _detect(cursor, value):
    return cursor._map_sql_type(value, [value], 0)


def _param_basetype(cursor, value):
    """The SQL Server base type a parameter arrives as, observed from outside.

    CAST(? AS sql_variant) preserves the parameter's declared SQL type, so this
    returns 'varchar' vs 'nvarchar' for the native path without needing a test-only
    detector. Limited to values that fit in sql_variant's 8000-byte cap, so it works
    for the small/inline cases only."""
    cursor.execute("SELECT SQL_VARIANT_PROPERTY(CAST(? AS sql_variant), 'BaseType')", [value])
    return cursor.fetchone()[0]


# Deterministic cases: the full 5-tuple is fixed by the type alone.
DETECTION_CASES = [
    # None and bool
    (None, _c.SQL_UNKNOWN_TYPE, _c.SQL_C_DEFAULT, 1, 0, False),
    (True, _c.SQL_BIT, _c.SQL_C_BIT, 1, 0, False),
    (False, _c.SQL_BIT, _c.SQL_C_BIT, 1, 0, False),
    # int width detection
    (0, _c.SQL_TINYINT, _c.SQL_C_TINYINT, 3, 0, False),
    (255, _c.SQL_TINYINT, _c.SQL_C_TINYINT, 3, 0, False),
    (256, _c.SQL_SMALLINT, _c.SQL_C_SHORT, 5, 0, False),
    (-1, _c.SQL_SMALLINT, _c.SQL_C_SHORT, 5, 0, False),
    (32767, _c.SQL_SMALLINT, _c.SQL_C_SHORT, 5, 0, False),
    (32768, _c.SQL_INTEGER, _c.SQL_C_LONG, 10, 0, False),
    (-32769, _c.SQL_INTEGER, _c.SQL_C_LONG, 10, 0, False),
    (2147483647, _c.SQL_INTEGER, _c.SQL_C_LONG, 10, 0, False),
    (2147483648, _c.SQL_BIGINT, _c.SQL_C_SBIGINT, 19, 0, False),
    (-2147483649, _c.SQL_BIGINT, _c.SQL_C_SBIGINT, 19, 0, False),
    # float
    (3.14, _c.SQL_DOUBLE, _c.SQL_C_DOUBLE, 15, 0, False),
    # small binary
    (b"", _c.SQL_VARBINARY, _c.SQL_C_BINARY, 1, 0, False),
    (b"abc", _c.SQL_VARBINARY, _c.SQL_C_BINARY, 3, 0, False),
    # date / datetime / time
    (datetime.date(2024, 1, 1), _c.SQL_DATE, _c.SQL_C_TYPE_DATE, 10, 0, False),
    (
        datetime.datetime(2024, 1, 1, 2, 3, 4),
        _c.SQL_TIMESTAMP,
        _c.SQL_C_TYPE_TIMESTAMP,
        26,
        6,
        False,
    ),
    (datetime.time(1, 2, 3), _c.SQL_TYPE_TIME, _c.SQL_C_CHAR, 16, 6, False),
]


@pytest.mark.parametrize(
    "value, sql_type, c_type, column_size, decimal_digits, is_dae",
    DETECTION_CASES,
    ids=[repr(row[0]) for row in DETECTION_CASES],
)
def test_map_sql_type_detection(
    cursor, value, sql_type, c_type, column_size, decimal_digits, is_dae
):
    assert _detect(cursor, value) == (
        sql_type.value,
        c_type.value,
        column_size,
        decimal_digits,
        is_dae,
    )


def test_map_sql_type_uuid(cursor):
    """UUID → SQL_GUID, and the slot is replaced with its little-endian bytes."""
    u = uuid.uuid4()
    params = [u]
    assert cursor._map_sql_type(u, params, 0) == (
        _c.SQL_GUID.value,
        _c.SQL_C_GUID.value,
        16,
        0,
        False,
    )
    assert params[0] == u.bytes_le


@pytest.mark.parametrize(
    "value, sql_type, c_type, column_size, is_dae",
    [
        ("", _c.SQL_VARCHAR, _c.SQL_C_CHAR, 0, False),
        ("hello", _c.SQL_VARCHAR, _c.SQL_C_CHAR, 5, False),
        ("a" * 4000, _c.SQL_VARCHAR, _c.SQL_C_CHAR, 4000, False),  # inline boundary
        ("a" * 4001, _c.SQL_VARCHAR, _c.SQL_C_CHAR, 0, True),  # ASCII DAE
        ("café", _c.SQL_WVARCHAR, _c.SQL_C_WCHAR, 4, False),  # unicode inline
        ("é" * 4001, _c.SQL_WVARCHAR, _c.SQL_C_WCHAR, 0, True),  # unicode DAE
    ],
    ids=["empty", "ascii", "ascii-4000", "ascii-4001-dae", "unicode", "unicode-dae"],
)
def test_map_sql_type_strings(cursor, value, sql_type, c_type, column_size, is_dae):
    assert _detect(cursor, value) == (sql_type.value, c_type.value, column_size, 0, is_dae)


@pytest.mark.parametrize(
    "prefix", ["POINT(1 2)", "LINESTRING(0 0, 1 1)", "POLYGON((0 0,1 0,1 1,0 0))"]
)
def test_map_sql_type_geometry_wkt(cursor, prefix):
    """Legacy detection: geometry WKT is SQL_WVARCHAR regardless of the unicode heuristic."""
    assert _detect(cursor, prefix) == (
        _c.SQL_WVARCHAR.value,
        _c.SQL_C_WCHAR.value,
        len(prefix),
        0,
        False,
    )


@pytest.mark.parametrize(
    "prefix", ["POINT(1 2)", "LINESTRING(0 0, 1 1)", "POLYGON((0 0,1 0,1 1,0 0))"]
)
def test_native_small_geometry_binds_nvarchar(cursor, prefix):
    """Native path: small geometry WKT arrives as nvarchar, while a plain ASCII string
    arrives as varchar. This is the observable proxy that native geometry detection
    fires and picks the wide type, matching the legacy tuple above."""
    assert _param_basetype(cursor, prefix) == "nvarchar"
    assert _param_basetype(cursor, "hello") == "varchar"


def test_native_unicode_kind_geometry_still_detected(cursor):
    """Native path: a WKT string carrying a non-ASCII char is stored by CPython in a
    wider (UCS-2/4) kind. Geometry detection must still fire — the old code gated the
    prefix check on kind == 1BYTE and would have missed this, binding varchar."""
    tagged = "POLYGON((0 0,1 1,0 0)) café"
    assert _param_basetype(cursor, tagged) == "nvarchar"


def test_native_large_geometry_binds_and_roundtrips(cursor):
    """Native path: a >4000-char geometry WKT binds and round-trips.

    Regression guard for the geometry fix. Geometry now folds into the wide-type
    decision but keeps the length-based DAE gate, so a large polygon streams as
    NVARCHAR(MAX) via DAE. The earlier code that forced non-DAE columnSize == len
    for geometry produced an unbindable NVARCHAR precision > 4000 ("Invalid precision
    value"); see test_legacy_map_sql_type_large_geometry_is_unbindable for the shape
    this deliberately avoids.
    """
    ring = ",".join(f"{n} {n}" for n in range(900)) + ",900 0,0 0"
    wkt = f"POLYGON((0 0,{ring}))"
    assert len(wkt) > 4000
    cursor.execute("SELECT ?", [wkt])
    assert cursor.fetchone()[0] == wkt
    # And SQL Server accepts it as real geometry, confirming the WKT arrived intact.
    cursor.execute("SELECT geometry::STGeomFromText(?, 0).STAsText()", [wkt])
    assert cursor.fetchone()[0]


def test_legacy_map_sql_type_large_geometry_is_unbindable(cursor):
    """Pins a known legacy defect so it stays visible: for a >4000-char polygon the
    Python _map_sql_type returns SQL_WVARCHAR with columnSize == len and DAE=False.
    That precision exceeds SQL Server's non-MAX NVARCHAR limit (4000) and is rejected
    at bind time with "Invalid precision value". The native path deliberately does not
    reproduce this shape — it streams via DAE instead. If legacy is ever fixed to gate
    geometry on length, update this test.
    """
    wkt = "POLYGON((" + ",".join(f"{n} {n}" for n in range(1000)) + "))"
    assert len(wkt) > 4000
    sql_type, c_type, column_size, decimal_digits, is_dae = _detect(cursor, wkt)
    assert sql_type == _c.SQL_WVARCHAR.value
    assert column_size == len(wkt) > 4000
    assert is_dae is False


def test_map_sql_type_large_binary_uses_dae(cursor):
    assert _detect(cursor, b"x" * 8001) == (
        _c.SQL_VARBINARY.value,
        _c.SQL_C_BINARY.value,
        0,
        0,
        True,
    )


def test_map_sql_type_aware_datetime(cursor):
    aware = datetime.datetime(
        2024, 1, 1, 2, 3, 4, tzinfo=datetime.timezone(datetime.timedelta(hours=5, minutes=30))
    )
    assert _detect(cursor, aware) == (
        _c.SQL_DATETIMEOFFSET.value,
        _c.SQL_C_SS_TIMESTAMPOFFSET.value,
        34,
        7,
        False,
    )


@pytest.mark.parametrize(
    "value",
    [decimal.Decimal("100.50"), decimal.Decimal("214748.3647"), decimal.Decimal("214748.3648")],
    ids=["smallmoney", "smallmoney-max", "money"],
)
def test_map_sql_type_money_range_binds_as_text(cursor, value):
    """MONEY / SMALLMONEY range Decimals are formatted to text and the slot is
    replaced with that formatted string."""
    params = [value]
    sql_type, c_type, column_size, decimal_digits, is_dae = cursor._map_sql_type(value, params, 0)
    assert (sql_type, c_type, decimal_digits, is_dae) == (
        _c.SQL_VARCHAR.value,
        _c.SQL_C_CHAR.value,
        0,
        False,
    )
    assert params[0] == format(value, "f")
    assert column_size == len(params[0])


def test_map_sql_type_numeric_out_of_money_range(cursor):
    """A Decimal beyond the MONEY range falls to the generic NUMERIC binding and the
    slot is replaced with a NumericData struct."""
    value = decimal.Decimal("1E20")  # 1e20 > MONEY_MAX (~9.2e14)
    params = [value]
    sql_type, c_type, column_size, decimal_digits, is_dae = cursor._map_sql_type(value, params, 0)
    assert (sql_type, c_type, is_dae) == (
        _c.SQL_NUMERIC.value,
        _c.SQL_C_NUMERIC.value,
        False,
    )
    assert column_size == params[0].precision
    assert decimal_digits == params[0].scale


def test_map_sql_type_unsupported_raises_typeerror(cursor):
    with pytest.raises(TypeError):
        cursor._map_sql_type({1, 2, 3}, [{1, 2, 3}], 0)


# ---------------------------------------------------------------------------
# _get_numeric_data: precision/scale arithmetic and digit packing
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value, precision, scale",
    [
        (decimal.Decimal("0"), 1, 0),
        (decimal.Decimal("314E2"), 5, 0),  # positive exponent
        (decimal.Decimal("3.140"), 4, 3),  # -exp <= num_digits
        (decimal.Decimal("0.03140"), 5, 5),  # -exp > num_digits (leading-zero pad)
    ],
    ids=["zero", "pos-exp", "frac", "leading-zeros"],
)
def test_get_numeric_data_precision_scale(cursor, value, precision, scale):
    nd = cursor._get_numeric_data(value)
    assert nd.precision == precision
    assert nd.scale == scale


def test_get_numeric_data_precision_overflow(cursor):
    with pytest.raises(ValueError, match="too high"):
        cursor._get_numeric_data(decimal.Decimal("1" * 39))
