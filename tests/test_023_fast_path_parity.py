"""
Parity tests: assert that the primary path (C++ DetectParamTypes + DDBCSQLExecute)
and legacy path (Python _map_sql_type + DDBCSQLExecuteLegacy) produce identical query
results for representative parameter types.

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


def _fast_path_roundtrip(cursor, value):
    """Default fast path: no setinputsizes."""
    cursor.execute("SELECT ?", [value])
    return cursor.fetchone()[0]


def _slow_path_roundtrip(cursor, value, sql_type, column_size):
    """Force the slow path by setting an explicit inputsizes entry. The fast
    path is gated on `not (self._inputsizes and any(s is not None ...))`, so a
    non-None tuple here flips us to the legacy Python type-detection path."""
    cursor.setinputsizes([(sql_type, column_size, 0)])
    try:
        cursor.execute("SELECT ?", [value])
        return cursor.fetchone()[0]
    finally:
        cursor.setinputsizes(None)


# ---------------------------------------------------------------------------
# Fast-path coverage: representative type matrix
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
def test_fast_path_basic_types(cursor, value):
    """Fast path round-trips representative scalar types correctly."""
    result = _fast_path_roundtrip(cursor, value)
    assert result == value, (
        f"Fast-path roundtrip mismatch for {type(value).__name__} {value!r}: " f"got {result!r}"
    )


# ---------------------------------------------------------------------------
# Subclass support — regression for the *_CheckExact bug from PR review
# ---------------------------------------------------------------------------


def test_int_subclass(cursor):
    class MyInt(int):
        pass

    assert _fast_path_roundtrip(cursor, MyInt(42)) == 42


def test_str_subclass(cursor):
    class MyStr(str):
        pass

    assert _fast_path_roundtrip(cursor, MyStr("hello")) == "hello"


def test_bytes_subclass(cursor):
    class MyBytes(bytes):
        pass

    assert _fast_path_roundtrip(cursor, MyBytes(b"hello")) == b"hello"


def test_float_subclass(cursor):
    class MyFloat(float):
        pass

    assert _fast_path_roundtrip(cursor, MyFloat(3.14)) == 3.14


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
    """Fast path must raise TypeError for unknown parameter types — matching
    the slow path's `_map_sql_type` final branch."""
    with pytest.raises(TypeError):
        cursor.execute("SELECT ?", [{1, 2, 3}])  # set is not bindable


def test_decimal_nan_rejected(cursor):
    """Non-finite Decimals must raise rather than silently bind as 0."""
    with pytest.raises(Exception):  # ValueError or DataError, not silent zero
        cursor.execute("SELECT ?", [decimal.Decimal("NaN")])


# ---------------------------------------------------------------------------
# Fast-vs-slow parity for representative types
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
def test_fast_slow_path_parity(cursor, value, sql_type, column_size):
    """Same input through both paths produces the same output."""
    fast = _fast_path_roundtrip(cursor, value)
    slow = _slow_path_roundtrip(cursor, value, sql_type=sql_type, column_size=column_size)
    assert fast == slow, f"Fast/slow path divergence for {value!r}: fast={fast!r} slow={slow!r}"


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
    with pytest.raises(Exception):
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
    """Integers beyond int64 range trigger overflow detection in C++ and bind as BIGINT.
    SQL Server cannot store values outside [-2^63, 2^63-1] in BIGINT, so these raise."""
    with pytest.raises(Exception):
        cursor.execute("SELECT ?", [2**63])
    with pytest.raises(Exception):
        cursor.execute("SELECT ?", [-(2**63) - 1])


@pytest.mark.parametrize("value", [decimal.Decimal("NaN"), decimal.Decimal("sNaN")])
def test_decimal_nan_variants_rejected(cursor, value):
    """Decimal NaN variants must raise, not silently bind as 0."""
    with pytest.raises(Exception):
        cursor.execute("SELECT ?", [value])


def test_decimal_precision_overflow_rejected(cursor):
    """Decimals beyond SQL Server's max precision must raise."""
    with pytest.raises(Exception):
        cursor.execute("SELECT ?", [decimal.Decimal("123456789012345678901234567890123456789")])
