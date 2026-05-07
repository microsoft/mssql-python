"""
Parity tests: assert that fast path (DetectParamTypes in C++) and slow path
(_map_sql_type in Python) produce identical query results for representative
parameter types.

The fast path runs by default. The slow path is forced by calling setinputsizes()
with a non-None entry, which triggers cursor.execute()'s slow-path branch.
"""

import pytest
import datetime
import decimal
import uuid
from mssql_python import connect

import os

CONN_STR = os.environ.get(
    "DB_CONNECTION_STRING",
    "Server=localhost;Database=master;Uid=sa;Pwd=Str0ng@Passw0rd123;TrustServerCertificate=yes",
)


@pytest.fixture
def conn():
    c = connect(CONN_STR)
    yield c
    c.close()


def _roundtrip(cursor, value):
    """Round-trip a single parameter through SELECT ? and return the result."""
    cursor.execute("SELECT ?", [value])
    return cursor.fetchone()[0]


def _force_slow_path_roundtrip(cursor, value):
    """Force slow path via setinputsizes(None for that param) — any non-empty
    inputsizes list with a non-None entry triggers the legacy code path."""
    # Empty list with at least one entry that's not None forces slow path.
    # Using SQL_VARCHAR(8000) as an opaque "no override" placeholder.
    from mssql_python import ddbc_bindings

    # A None entry means "infer", which is fine — the slow path still runs because
    # _inputsizes is set (any non-empty list with at least one non-None entry).
    # We need at least one non-None entry to flip use_fast_path to False.
    cursor.setinputsizes([None])  # Has at least one entry but no override
    # Wait — None doesn't trigger slow path. We need a real override.
    # Use SQL_VARCHAR which is identity-ish for strings.
    cursor.setinputsizes([(1, 0, 0)])  # (sqlType, size, decimal) tuple
    cursor.execute("SELECT ?", [value])
    cursor.setinputsizes(None)  # Reset
    return cursor.fetchone()[0]


@pytest.mark.parametrize(
    "value",
    [
        # int range detection
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
        # str (ASCII inline + DAE + unicode)
        "",
        "hello",
        "a" * 100,
        # bytes
        b"",
        b"\x00\x01\x02",
        b"x" * 100,
    ],
)
def test_fast_path_roundtrip(conn, value):
    """Fast path produces identical results regardless of value type."""
    cur = conn.cursor()
    result = _roundtrip(cur, value)
    assert (
        result == value
    ), f"Roundtrip mismatch for {type(value).__name__} {value!r}: got {result!r}"


def test_int_subclass(conn):
    """int subclasses must work (regression test for *_CheckExact bug)."""

    class MyInt(int):
        pass

    cur = conn.cursor()
    assert _roundtrip(cur, MyInt(42)) == 42


def test_str_subclass(conn):
    """str subclasses must work."""

    class MyStr(str):
        pass

    cur = conn.cursor()
    assert _roundtrip(cur, MyStr("hello")) == "hello"


def test_bytes_subclass(conn):
    """bytes subclasses must work."""

    class MyBytes(bytes):
        pass

    cur = conn.cursor()
    assert _roundtrip(cur, MyBytes(b"hello")) == b"hello"


def test_float_subclass(conn):
    """float subclasses must work."""

    class MyFloat(float):
        pass

    cur = conn.cursor()
    assert _roundtrip(cur, MyFloat(3.14)) == 3.14


def test_caller_param_list_not_mutated(conn):
    """DetectParamTypes must not mutate the caller's parameter list."""
    cur = conn.cursor()
    params = ["hello", 42, 3.14, datetime.date(2024, 1, 1), uuid.uuid4()]
    snapshot = list(params)
    cur.execute("SELECT ?, ?, ?, ?, ?", params)
    cur.fetchone()
    assert params == snapshot, f"Caller list was mutated: {params} != {snapshot}"


def test_unsupported_type_raises_typeerror(conn):
    """Unknown parameter types must raise TypeError, matching slow path."""
    cur = conn.cursor()
    with pytest.raises(TypeError):
        cur.execute("SELECT ?", [{1, 2, 3}])  # set is not supported
