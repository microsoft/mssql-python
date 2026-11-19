"""Vector unit contracts and live SQL Server end-to-end API coverage.

Live tests require DB_CONNECTION_STRING. Enable the v2 matrix with
MSSQL_TEST_VECTOR_FLOAT16=1 on an ODBC 18.7.1.1+ / FLOAT16-capable server.
Live tests require the Microsoft ODBC provider and a server advertising the
vector type. No live operation is mocked; connection/backend errors fail.
"""

import json
import math
import os
import struct
import uuid
from collections import deque
from itertools import repeat
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock

import pytest

import mssql_python
from mssql_python import InputOutputParameter, OutputParameter, Vector
from mssql_python import ddbc_bindings
from mssql_python.constants import ConstantsDDBC
from mssql_python.cursor import Cursor


@pytest.mark.parametrize("dtype,limit", [("float32", 1998), ("float16", 3996)])
def test_vector_dimensions_and_subtype(dtype, limit):
    value = Vector(range(limit), base_type=dtype)
    assert value.dimensions == limit
    assert value.base_type == dtype
    assert value[0] == 0.0
    assert Vector(value, base_type=dtype) == value
    with pytest.raises(ValueError, match="dimensions"):
        Vector(range(limit + 1), base_type=dtype)


@pytest.mark.parametrize("values", [[], [math.nan], [math.inf], [-math.inf], [1e40]])
def test_invalid_vector_values(values):
    with pytest.raises(ValueError):
        Vector(values)


@pytest.mark.parametrize("values", ["123", b"123", [None], [True], ["1"]])
def test_non_numeric_vector_values(values):
    with pytest.raises(TypeError):
        Vector(values)


def test_vector_is_immutable_and_preserves_input():
    source = [1, 2, 3]
    value = Vector(source, base_type="float16")
    source[0] = 9
    assert value.values == (1.0, 2.0, 3.0)
    with pytest.raises(AttributeError):
        value.base_type = "float32"
    with pytest.raises(ValueError, match="base_type"):
        Vector([1], base_type="float64")


def test_vector_procedure_parameters():
    output = OutputParameter(mssql_python.SQL_SS_VECTOR, 3, 1)
    assert (output.sql_type, output.size, output.scale) == (mssql_python.SQL_SS_VECTOR, 3, 1)
    assert output.base_type == "float16"
    assert output.dimensions == 3
    assert output.value is None
    assert OutputParameter(mssql_python.SQL_SS_VECTOR, 3).base_type == "float32"
    value = Vector([1, 2, 3], base_type="float16")
    inout = InputOutputParameter(value)
    assert inout.value == value
    assert (inout.sql_type, inout.size, inout.scale) == (mssql_python.SQL_SS_VECTOR, 3, 1)
    with pytest.raises(TypeError):
        InputOutputParameter([1, 2])


@pytest.mark.parametrize("sql_type", [mssql_python.SQL_INTEGER, mssql_python.SQL_WVARCHAR])
def test_output_parameter_other_sql_types_not_supported_yet(sql_type):
    with pytest.raises(mssql_python.NotSupportedError, match="SQL_SS_VECTOR"):
        OutputParameter(sql_type, 10, 0)


@pytest.mark.parametrize("scale", [2, -1, True])
def test_output_parameter_invalid_vector_subtype(scale):
    with pytest.raises(ValueError, match="subtype"):
        OutputParameter(mssql_python.SQL_SS_VECTOR, 3, scale)


@pytest.mark.parametrize("base_type,limit", [("float32", 1998), ("float16", 3996)])
@pytest.mark.parametrize("container", [list, tuple])
def test_vector_float_fast_path_matches_checked_loop(monkeypatch, base_type, limit, container):
    import mssql_python.vector as vector_module

    values = [(-1) ** i * (i * 0.1 + 1e-40) for i in range(limit)]
    values[1] = 65504.0 if base_type == "float16" else 3.4028234663852886e38
    expected = tuple(struct.unpack("=f", struct.pack("=f", value))[0] for value in values)
    # The per-element packer is only used by the checked loop.
    monkeypatch.setattr(vector_module, "_FLOAT32", None)
    vector = Vector(container(values), base_type=base_type)
    assert vector.values == expected
    assert vector.base_type == base_type


@pytest.mark.parametrize("base_type", ["float32", "float16"])
@pytest.mark.parametrize(
    "invalid,error,message",
    [
        (math.nan, ValueError, "index 1.*finite float32"),
        (math.inf, ValueError, "index 1.*finite float32"),
        (-3.5e38, ValueError, "index 1.*finite float32"),
        (True, TypeError, "index 1"),
        (None, TypeError, "index 1"),
    ],
)
def test_vector_all_float_errors_match_checked_loop(base_type, invalid, error, message):
    with pytest.raises(error, match=message):
        Vector([0.0, invalid, 1.0], base_type=base_type)


@pytest.mark.parametrize("values", [[0.0, 65505.0], (0.0, -70000.0)])
def test_vector_all_float_half_range_errors_match_checked_loop(values):
    with pytest.raises(ValueError, match="index 1.*float16"):
        Vector(values, base_type="float16")


@pytest.mark.parametrize("values", [[], [0.5] * 1999])
def test_vector_all_float_shape_errors_match_checked_loop(values):
    with pytest.raises(ValueError, match="dimensions"):
        Vector(values)


def test_trusted_vector_construction_skips_public_validation(monkeypatch):
    def reject(*args, **kwargs):
        raise AssertionError("trusted construction must not call Vector.__init__")

    monkeypatch.setattr(Vector, "__init__", reject)
    vector = Vector._from_trusted((1.25, -2.5), "float16")
    assert vector.values == (1.25, -2.5)
    assert vector.base_type == "float16"
    assert vector.dimensions == 2
    with pytest.raises(AttributeError):
        vector.values = ()


@pytest.mark.parametrize("mode", ["off", "v1", "v2"])
def test_vector_connection_keyword(mode):
    from mssql_python.connection_string_parser import _ConnectionStringParser

    parser = _ConnectionStringParser(validate_keywords=True)
    params = parser._parse(f"Server=localhost;VectorTypeSupport={mode.upper()}")
    assert params["vectortypesupport"] == mode.upper()
    assert parser._normalize_params(params)["VectorTypeSupport"] == mode


@pytest.mark.parametrize("dtype,dimensions,subtype", [("float32", 1998, 0), ("float16", 3996, 1)])
def test_native_vector_detection(dtype, dimensions, subtype):
    value = Vector(repeat(1.25, dimensions), dtype)
    info = ddbc_bindings._detect_param_types([value])[0]
    assert info.paramSQLType == mssql_python.SQL_SS_VECTOR
    assert info.paramCType == ConstantsDDBC.SQL_C_SS_VECTOR.value
    assert info.columnSize == ddbc_bindings._vector_buffer_size(dimensions)
    assert info.decimalDigits == subtype
    assert not info.isDAE
    if dtype == "float16":
        assert info.columnSize > 8000


@pytest.mark.parametrize("value", [None, Vector([1, 2, 3], "float16"), [1, 2, 3]])
def test_native_vector_input_sizes(value):
    hints = [(mssql_python.SQL_SS_VECTOR, ConstantsDDBC.SQL_C_SS_VECTOR.value, 3, 1)]
    info = ddbc_bindings._detect_param_types([value], hints)[0]
    assert info.columnSize == ddbc_bindings._vector_buffer_size(3)
    assert info.decimalDigits == 1
    assert info.paramCType == ConstantsDDBC.SQL_C_SS_VECTOR.value
    assert not info.isDAE


@pytest.mark.parametrize(
    "value,size,subtype,error",
    [
        (Vector([1, 2]), 3, 0, ValueError),
        (Vector([1, 2], "float16"), 2, 0, ValueError),
        (None, 3997, 1, ValueError),
        (None, 0, 0, ValueError),
        (None, 2, 2, ValueError),
        ((1, 2), 2, 0, TypeError),
        ([1, 2], 3, 0, ValueError),
        ([1e40, 2], 2, 0, ValueError),
        ([70000, 2], 2, 1, ValueError),
        ([[1], 2], 2, 0, TypeError),
        ("x" * 8001, 2, 0, TypeError),
    ],
)
def test_native_vector_invalid_input_sizes(value, size, subtype, error):
    hints = [(mssql_python.SQL_SS_VECTOR, ConstantsDDBC.SQL_C_SS_VECTOR.value, size, subtype)]
    with pytest.raises(error):
        ddbc_bindings._detect_param_types([value], hints)


def test_vector_large_integer_and_unbounded_iterable():
    with pytest.raises(ValueError, match="finite float32"):
        Vector([10**1000])
    with pytest.raises(ValueError, match="dimensions"):
        Vector(repeat(1))


def test_no_python_half_precision_conversion():
    number = 1.0001
    float32 = struct.unpack("=f", struct.pack("=f", number))[0]
    float16 = struct.unpack("=e", struct.pack("=e", number))[0]
    assert Vector([number], "float16")[0] == float32
    assert float32 != float16


@pytest.mark.parametrize("base_type", ["float32", "float16"])
def test_vector_normalizes_to_float32_and_supports_list(base_type):
    source = [0.1, -0.1, -0.0, 1e-50, 1e-40]
    expected = [struct.unpack("=f", struct.pack("=f", value))[0] for value in source]
    value = Vector(iter(source), base_type=base_type)
    assert list(value) == expected
    assert value.values == tuple(expected)
    assert math.copysign(1, value[2]) == -1
    assert value[3] == 0
    assert value[4] != 0
    assert value[:2] == tuple(expected[:2])
    assert hash(value) == hash(Vector(expected, base_type=base_type))
    copy = list(value)
    copy[0] = 9
    assert value[0] == expected[0]
    assert not hasattr(value, "dtype")


@pytest.mark.parametrize(
    "base_type,limit",
    [("float32", 3.4028234663852886e38), ("float16", 65504.0)],
)
def test_vector_accepts_finite_range_boundaries(base_type, limit):
    assert list(Vector([limit, -limit], base_type=base_type)) == [limit, -limit]
    for outside in (math.nextafter(limit, math.inf), math.nextafter(-limit, -math.inf)):
        with pytest.raises(ValueError, match=f"index 1.*{base_type}"):
            Vector([0, outside], base_type=base_type)


@pytest.mark.parametrize("base_type", ["float32", "float16"])
@pytest.mark.parametrize("invalid", [math.nan, math.inf, -math.inf, 3.5e38, -3.5e38, 10**1000])
def test_vector_overflow_is_rejected_at_construction(base_type, invalid):
    with pytest.raises(ValueError, match="index 1.*finite float32"):
        Vector([0, invalid], base_type=base_type)


@pytest.mark.parametrize("invalid", [65505, -65505, 70000, -70000])
def test_vector_half_range_is_checked_without_encoding(invalid):
    with pytest.raises(ValueError, match="index 1.*float16"):
        Vector([0, invalid], base_type="float16")


@pytest.mark.parametrize("base_type,subtype", [("float32", 0), ("float16", 1)])
def test_native_hinted_list_preserves_input(base_type, subtype):
    source = [0.1, 2, 3]
    parameters = [source]
    hints = [(mssql_python.SQL_SS_VECTOR, ConstantsDDBC.SQL_C_SS_VECTOR.value, 3, subtype)]
    info = ddbc_bindings._detect_param_types(parameters, hints)[0]
    assert parameters[0] is source
    assert source == [0.1, 2, 3]
    assert info.columnSize == ddbc_bindings._vector_buffer_size(3)
    assert info.decimalDigits == subtype


def test_unhinted_list_is_not_implicitly_a_vector():
    with pytest.raises(TypeError, match="Unsupported parameter type"):
        ddbc_bindings._detect_param_types([[1, 2, 3]])


@pytest.mark.parametrize("rows", [[], [[1], [2]]])
def test_native_fetch_policy_excludes_buffered_procedures(rows):
    from mssql_python._native_fetch import _Policy

    policy = object.__new__(_Policy)
    assert policy(Cursor.__new__(Cursor), {"_procedure_rows": deque(rows)}) is None


@pytest.mark.parametrize("dimensions", [True, 1.5, "3"])
def test_output_parameter_invalid_shape_type(dimensions):
    with pytest.raises(TypeError, match="integer"):
        OutputParameter(mssql_python.SQL_SS_VECTOR, dimensions, 0)


@pytest.mark.parametrize("mode", ["", "v3", "true"])
def test_invalid_vector_negotiation(mode):
    from mssql_python.connection_string_parser import _ConnectionStringParser
    from mssql_python.exceptions import ConnectionStringParseError

    parser = _ConnectionStringParser(validate_keywords=True)
    with pytest.raises(ConnectionStringParseError, match="(?i)vectortypesupport"):
        parser._normalize_params(parser._parse(f"Server=localhost;VectorTypeSupport={mode}"))


def test_vector_negotiation_is_opt_in():
    from mssql_python.connection_string_parser import _ConnectionStringParser

    parser = _ConnectionStringParser(validate_keywords=True)
    assert "VectorTypeSupport" not in parser._normalize_params(parser._parse("Server=localhost"))
    assert parser._normalize_params({"vectortypesupport": "V1"})["VectorTypeSupport"] == "v1"


@pytest.fixture
def unit_cursor(monkeypatch):
    monkeypatch.setattr(Cursor, "_initialize_cursor", lambda self: None)
    connection = SimpleNamespace(
        _converters_generation=0,
        _decoding_generation=0,
        _output_converters={},
        _native_uuid=None,
        getdecoding=lambda sql_type: {"encoding": "utf-16le", "ctype": -8},
        getencoding=lambda: {"encoding": "utf-16le", "ctype": -8},
    )
    connection.get_output_converter = connection._output_converters.get
    cursor = Cursor(connection)
    yield cursor
    cursor.close()


def vector_metadata(dtype="float32"):
    return [
        {
            "ColumnName": "embedding",
            "DataType": mssql_python.SQL_SS_VECTOR,
            "ColumnSize": ddbc_bindings._vector_buffer_size(3),
            "DecimalDigits": int(dtype == "float16"),
            "Nullable": 1,
        }
    ]


def test_callproc_buffered_results_and_cursor_reuse(unit_cursor, monkeypatch):
    vectors = [Vector([i, 2, 3]) for i in range(4)]
    output = OutputParameter(mssql_python.SQL_SS_VECTOR, 3, 0)

    def callproc(handle, sql, params, settings, messages, *decoding):
        assert sql == "{CALL [dbo].[a]]b](?,?)}"
        assert params == [vectors[0], output]
        assert decoding == ("utf-16le", "utf-16le", -8)
        output.value = vectors[0]
        return [vectors[0], vectors[0]], [
            (vector_metadata(), [[value] for value in vectors], -1),
            ([], [], 4),
            (vector_metadata(), [[None]], -1),
        ]

    monkeypatch.setattr(ddbc_bindings, "DDBCCallProc", callproc)
    assert unit_cursor.callproc("dbo.a]b", [vectors[0], output]) == [vectors[0], vectors[0]]
    assert output.value == vectors[0]
    assert unit_cursor.description[0][1] is Vector
    assert unit_cursor.fetchone().embedding == vectors[0]
    assert [row[0] for row in unit_cursor.fetchmany(2)] == vectors[1:3]
    assert [row[0] for row in unit_cursor.fetchall()] == vectors[3:]
    assert unit_cursor.fetchone() is None
    assert unit_cursor.rowcount == 4
    assert unit_cursor.rownumber == 3
    assert unit_cursor.nextset()
    assert unit_cursor.rowcount == 4
    with pytest.raises(mssql_python.ProgrammingError):
        unit_cursor.fetchone()
    assert unit_cursor.nextset()
    assert unit_cursor.fetchone()[0] is None
    assert not unit_cursor.nextset()
    assert unit_cursor.description is None
    unit_cursor._soft_reset_cursor()
    assert unit_cursor._procedure_rows is None
    assert unit_cursor._procedure_results is None


def test_callproc_refreshes_decoding_after_connection_change(unit_cursor, monkeypatch):
    seen = []

    def callproc(handle, sql, params, settings, messages, *decoding):
        seen.append(decoding)
        return [], []

    monkeypatch.setattr(ddbc_bindings, "DDBCCallProc", callproc)
    unit_cursor.callproc("p")
    connection = unit_cursor._connection
    connection.getdecoding = lambda sql_type: (
        {"encoding": "latin-1", "ctype": mssql_python.SQL_CHAR}
        if sql_type == mssql_python.SQL_CHAR
        else {"encoding": "utf-16le", "ctype": -8}
    )
    connection._decoding_generation += 1
    unit_cursor.callproc("p")
    assert seen == [("utf-16le", "utf-16le", -8), ("latin-1", "utf-16le", mssql_python.SQL_CHAR)]


@pytest.mark.parametrize("name", ["", "a..b", "a.b.c.d", "a\0b"])
def test_callproc_invalid_name_without_database(unit_cursor, name):
    with pytest.raises(ValueError):
        unit_cursor.callproc(name)


@pytest.mark.parametrize(
    "name,expected",
    [
        ("dbo.vecproc", "[dbo].[vecproc]"),
        ("[dbo].[vecproc]", "[dbo].[vecproc]"),
        ("[dbo].[vec.proc]", "[dbo].[vec.proc]"),
        ("db.[dbo].vecproc", "[db].[dbo].[vecproc]"),
        ("[a]]b].c", "[a]]b].[c]"),
        ("dbo.a]b", "[dbo].[a]]b]"),
        ("[#temp.proc]", "[#temp.proc]"),
    ],
)
def test_callproc_accepts_bracketed_names(unit_cursor, monkeypatch, name, expected):
    calls = []
    monkeypatch.setattr(
        ddbc_bindings, "DDBCCallProc", lambda handle, sql, *args: calls.append(sql) or ([], [])
    )
    unit_cursor.callproc(name)
    assert calls == ["{CALL " + expected + "()}"]


@pytest.mark.parametrize(
    "name,message",
    [
        ("[dbo", "unterminated"),
        ("[dbo]x.p", "after a closing"),
        ("[].p", "procedure"),
        ("[a].[b].[c].[d]", "procedure"),
        ("[dbo].", "procedure"),
    ],
)
def test_callproc_rejects_malformed_bracketed_names(unit_cursor, monkeypatch, name, message):
    native = Mock()
    monkeypatch.setattr(ddbc_bindings, "DDBCCallProc", native)
    with pytest.raises(ValueError, match=message):
        unit_cursor.callproc(name)
    native.assert_not_called()


def test_executemany_non_vector_columns_skip_vector_instance_checks(unit_cursor, monkeypatch):
    from abc import ABCMeta

    import mssql_python.cursor as cursor_module

    checks = []

    class CountingMeta(ABCMeta):
        def __instancecheck__(cls, instance):
            checks.append(instance)
            return super().__instancecheck__(instance)

    class Probe(metaclass=CountingMeta):
        pass

    monkeypatch.setattr(cursor_module, "Vector", Probe)
    unit_cursor._compute_column_type([None, 1, 2, 3])
    unit_cursor._compute_column_type(["a", None, "b"])
    assert checks == []


def test_buffered_procedure_rejects_arrow_and_releases_rows(unit_cursor, monkeypatch):
    monkeypatch.setattr(
        ddbc_bindings,
        "DDBCCallProc",
        lambda *args: ([], [(vector_metadata(), [[Vector([1, 2, 3])]], -1)]),
    )
    unit_cursor.callproc("dbo.v")
    with pytest.raises(mssql_python.NotSupportedError, match="row fetch"):
        unit_cursor.arrow_batch()
    unit_cursor.close()
    assert unit_cursor._procedure_rows is None
    assert unit_cursor._procedure_results is None


def test_callproc_propagates_backend_errors(unit_cursor, monkeypatch):
    execute = Mock(side_effect=mssql_python.ProgrammingError("procedure failed", "42000"))
    monkeypatch.setattr(ddbc_bindings, "DDBCCallProc", execute)
    with pytest.raises(mssql_python.ProgrammingError, match="procedure failed"):
        unit_cursor.callproc("dbo.failure", [Vector([1, 2, 3])])
    assert unit_cursor._procedure_rows is None


def test_failing_callproc_invalidates_prepared_query(unit_cursor, monkeypatch):
    unit_cursor.hstmt = Mock()
    monkeypatch.setattr(ddbc_bindings, "DDBCSQLResetStmt", lambda *args: 0)
    monkeypatch.setattr(ddbc_bindings, "DDBCSQLRowCount", lambda *args: -1)
    monkeypatch.setattr(ddbc_bindings, "DDBCSQLDescribeCol", lambda *args: 0)

    def execute(handle, sql, params, hints, prepared, use_prepare, settings):
        prepared[0] = True
        return 0

    execute = Mock(side_effect=execute)
    monkeypatch.setattr(ddbc_bindings, "DDBCSQLExecute", execute)
    query = "SELECT CAST(? AS INT)"
    unit_cursor.execute(query, 1)
    assert unit_cursor.last_executed_stmt == query
    assert unit_cursor.is_stmt_prepared == [True]
    monkeypatch.setattr(
        ddbc_bindings,
        "DDBCCallProc",
        Mock(side_effect=mssql_python.ProgrammingError("procedure failed", "42000")),
    )
    with pytest.raises(mssql_python.ProgrammingError, match="procedure failed"):
        unit_cursor.callproc("dbo.failure", [Vector([1, 2, 3])])
    unit_cursor.execute(query, 2)
    assert execute.call_count == 2
    assert all(call.args[5] is True for call in execute.call_args_list)


def test_buffered_fetchall_finalizes_rowcount_for_empty_results(unit_cursor, monkeypatch):
    value = Vector([1, 2, 3])
    monkeypatch.setattr(
        ddbc_bindings,
        "DDBCCallProc",
        lambda *args: (
            [],
            [
                (vector_metadata(), [], -1),
                (vector_metadata(), [[value], [value]], -1),
                (vector_metadata(), [], -1),
            ],
        ),
    )
    unit_cursor.callproc("dbo.v")
    assert unit_cursor.fetchall() == []
    assert unit_cursor.rowcount == 0
    assert unit_cursor.nextset()
    assert [row[0] for row in unit_cursor.fetchall()] == [value, value]
    assert unit_cursor.rowcount == 2
    assert unit_cursor.nextset()
    assert unit_cursor.fetchall() == []
    assert unit_cursor.rowcount == 0


def test_procedure_result_converters_are_refreshed(unit_cursor, monkeypatch):
    value = Vector([1, 2, 3])
    unit_cursor.connection._output_converters[mssql_python.SQL_SS_VECTOR] = lambda v: v.dimensions
    monkeypatch.setattr(
        ddbc_bindings,
        "DDBCCallProc",
        lambda *args: ([], [(vector_metadata(), [[value]], -1)] * 2),
    )
    unit_cursor.callproc("dbo.v")
    assert unit_cursor.fetchone()[0] == 3
    assert unit_cursor.nextset()
    assert unit_cursor.fetchone()[0] == 3


@pytest.mark.parametrize("explicit", [False, True])
@pytest.mark.parametrize("dtype", ["float32", "float16"])
def test_executemany_vector_type_mapping(unit_cursor, monkeypatch, explicit, dtype):
    execute = Mock(return_value=0)
    monkeypatch.setattr(ddbc_bindings, "SQLExecuteMany", execute)
    monkeypatch.setattr(ddbc_bindings, "DDBCSQLRowCount", lambda handle: 3)
    monkeypatch.setattr(ddbc_bindings, "DDBCSQLDescribeCol", lambda *args: 0)
    subtype = int(dtype == "float16")
    if explicit:
        unit_cursor.setinputsizes([(mssql_python.SQL_SS_VECTOR, 3, subtype)])
    value = Vector([1, 2, 3], dtype)
    unit_cursor.executemany("INSERT INTO v VALUES (?)", [(None,), (value,), (value,)])
    arguments = execute.call_args.args
    assert arguments[2] == [[None, value, value]]
    info = arguments[3][0]
    assert info.paramSQLType == mssql_python.SQL_SS_VECTOR
    assert info.paramCType == ConstantsDDBC.SQL_C_SS_VECTOR.value
    assert info.decimalDigits == subtype
    assert info.columnSize == ddbc_bindings._vector_buffer_size(3)
    assert not info.isDAE


@pytest.mark.parametrize("base_type,subtype", [("float32", 0), ("float16", 1)])
@pytest.mark.parametrize("pyformat", [False, True])
def test_executemany_hinted_lists_are_normalized(
    unit_cursor, monkeypatch, base_type, subtype, pyformat
):
    execute = Mock(return_value=0)
    monkeypatch.setattr(ddbc_bindings, "SQLExecuteMany", execute)
    monkeypatch.setattr(ddbc_bindings, "DDBCSQLRowCount", lambda handle: 3)
    monkeypatch.setattr(ddbc_bindings, "DDBCSQLDescribeCol", lambda *args: 0)
    source = [0.1, 2, 3]
    expected = Vector(source, base_type=base_type)
    rows = [(None,), (source,), (expected,)]
    sql = "INSERT INTO v VALUES (?)"
    if pyformat:
        rows = [{"v": row[0]} for row in rows]
        sql = "INSERT INTO v VALUES (%(v)s)"
    unit_cursor.setinputsizes([(mssql_python.SQL_SS_VECTOR, 3, subtype)])
    unit_cursor.executemany(sql, rows)
    source[0] = 9
    assert execute.call_args.args[2] == [[None, expected, expected]]
    assert execute.call_args.args[3][0].decimalDigits == subtype
    assert unit_cursor._inputsizes is None


@pytest.mark.parametrize(
    "invalid,error",
    [
        ([1, 2], ValueError),
        ([1e40, 2, 3], ValueError),
        ([70000, 2, 3], ValueError),
        ([None, 2, 3], TypeError),
        ("123", TypeError),
        (Vector([1, 2, 3]), ValueError),
    ],
)
def test_executemany_hinted_lists_validate_before_execution(
    unit_cursor, monkeypatch, invalid, error
):
    execute = Mock()
    monkeypatch.setattr(ddbc_bindings, "SQLExecuteMany", execute)
    unit_cursor.setinputsizes([(mssql_python.SQL_SS_VECTOR, 3, 1)])
    with pytest.raises(error):
        unit_cursor.executemany("INSERT INTO v VALUES (?)", [([1, 2, 3],), (invalid,)])
    execute.assert_not_called()


@pytest.mark.parametrize(
    "values,error",
    [
        ([Vector([1]), Vector([1, 2])], ValueError),
        ([Vector([1]), Vector([1], "float16")], ValueError),
        ([Vector([1]), 1], TypeError),
    ],
)
def test_executemany_rejects_mixed_vector_columns(unit_cursor, values, error):
    with pytest.raises(error):
        unit_cursor.executemany("INSERT INTO v VALUES (?)", [(value,) for value in values])


def test_bulkcopy_does_not_invent_pycore_serialization():
    rows = [(1, "x"), (2, None)]
    assert list(Cursor._validate_bulkcopy_vectors(rows)) == rows
    with pytest.raises(mssql_python.NotSupportedError, match="pycore"):
        list(Cursor._validate_bulkcopy_vectors([(1, Vector([1, 2, 3]))]))


@pytest.mark.parametrize("leading_row", [False, True])
@pytest.mark.parametrize("streaming", [False, True])
@pytest.mark.parametrize("wrap_error", [False, True])
def test_bulkcopy_preserves_vector_rejection(
    unit_cursor, monkeypatch, leading_row, streaming, wrap_error
):
    def consume(table_name, rows, **kwargs):
        try:
            list(rows)
        except mssql_python.NotSupportedError:
            if wrap_error:
                raise RuntimeError("Failed to read bulk row") from None
        return {"rows_copied": 0}

    core_cursor = Mock()
    core_cursor.bulkcopy.side_effect = consume
    core_connection = Mock()
    core_connection.cursor.return_value = core_cursor
    core = SimpleNamespace(PyCoreConnection=Mock(return_value=core_connection))
    monkeypatch.setattr(Cursor, "_bulkcopy_core_and_validate", staticmethod(lambda *args: core))
    monkeypatch.setattr(Cursor, "_build_pycore_context", lambda self: {})
    rows = ([(1,)] if leading_row else []) + [(Vector([1, 2, 3]),)]
    with pytest.raises(mssql_python.NotSupportedError, match="pycore"):
        unit_cursor.bulkcopy("dbo.vectors", iter(rows) if streaming else rows)
    core_cursor.close.assert_called_once()
    core_connection.close.assert_called_once()


def _require_live_vector_server(conn_str):
    if not conn_str:
        pytest.skip("DB_CONNECTION_STRING is not configured")
    if mssql_python.get_native_provider_info()["id"] != "msodbcsql18":
        pytest.skip("Vector integration currently qualifies the Microsoft ODBC provider only")
    connection = mssql_python.connect(conn_str)
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) FROM sys.types WHERE name=N'vector' AND is_user_defined=0"
            )
            if not cursor.fetchval():
                pytest.skip("Server does not advertise the native vector type")
    finally:
        connection.close()


@pytest.mark.parametrize("supported", [False, True])
def test_vector_server_gate_uses_type_metadata_and_closes_connection(monkeypatch, supported):
    connection = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchval.return_value = int(supported)
    monkeypatch.setattr(mssql_python, "connect", Mock(return_value=connection))
    monkeypatch.setattr(mssql_python, "get_native_provider_info", lambda: {"id": "msodbcsql18"})
    if supported:
        _require_live_vector_server("Server=localhost")
    else:
        with pytest.raises(pytest.skip.Exception, match="native vector type"):
            _require_live_vector_server("Server=localhost")
    cursor.execute.assert_called_once_with(
        "SELECT COUNT(*) FROM sys.types WHERE name=N'vector' AND is_user_defined=0"
    )
    connection.close.assert_called_once()


def test_vector_server_gate_does_not_hide_connection_failures(monkeypatch):
    monkeypatch.setattr(mssql_python, "get_native_provider_info", lambda: {"id": "msodbcsql18"})
    monkeypatch.setattr(
        mssql_python, "connect", Mock(side_effect=mssql_python.OperationalError("failed", "08001"))
    )
    with pytest.raises(mssql_python.OperationalError, match="failed"):
        _require_live_vector_server("Server=localhost")


def test_vector_server_gate_does_not_connect_unqualified_provider(monkeypatch):
    connect = Mock()
    monkeypatch.setattr(mssql_python, "connect", connect)
    monkeypatch.setattr(mssql_python, "get_native_provider_info", lambda: {"id": "mssql-odbc"})
    with pytest.raises(pytest.skip.Exception, match="provider"):
        _require_live_vector_server("Server=localhost")
    connect.assert_not_called()


@pytest.fixture(scope="session")
def vector_server(conn_str):
    _require_live_vector_server(conn_str)


@pytest.fixture(
    params=[
        pytest.param(("v1", "float32"), id="v1-float32"),
        pytest.param(("v2", "float32"), id="v2-float32"),
        pytest.param(("v2", "float16"), id="v2-float16"),
    ]
)
def live_vector_connection(request, conn_str, vector_server):
    mode, dtype = request.param
    if mode == "v2" and os.getenv("MSSQL_TEST_VECTOR_FLOAT16") != "1":
        pytest.skip("Set MSSQL_TEST_VECTOR_FLOAT16=1 with ODBC 18.7.1.1+ and server support")
    # Keyword overrides avoid duplicate keys or a pre-existing mode in conn_str.
    connection = mssql_python.connect(conn_str, VectorTypeSupport=mode)
    try:
        yield connection, dtype
    finally:
        connection.close()


def _expected_stored_vector(values, base_type):
    normalized = [struct.unpack("=f", struct.pack("=f", value))[0] for value in values]
    if base_type == "float16":
        normalized = [struct.unpack("=e", struct.pack("=e", value))[0] for value in normalized]
    return Vector(normalized, base_type=base_type)


@pytest.mark.parametrize("style", ["tuple", "list", "pyformat"])
def test_live_vector_hinted_lists_and_hint_lifetime(live_vector_connection, style):
    connection, base_type = live_vector_connection
    source = [0.1, 1.0001, -0.0]
    expected = _expected_stored_vector(source, base_type)
    with connection.cursor() as cursor:
        cursor.setinputsizes([(mssql_python.SQL_SS_VECTOR, 3, int(base_type == "float16"))])
        if style == "pyformat":
            cursor.execute("SELECT %(v)s AS embedding", {"v": source})
        else:
            cursor.execute("SELECT ? AS embedding", (source,) if style == "tuple" else [source])
        assert cursor._inputsizes is None
        assert source == [0.1, 1.0001, -0.0]
        source[0] = 9
        retained = cursor.fetchval()
        assert retained == expected
        assert retained.base_type == base_type
        assert cursor.fetchval() is None
        with pytest.raises(TypeError, match="Unsupported parameter type"):
            cursor.execute("SELECT ? AS embedding", (source,))
        cursor.execute("SELECT 1")
        assert cursor.fetchval() == 1
        assert retained == expected


@pytest.mark.parametrize("pyformat", [False, True])
@pytest.mark.parametrize("streamed", [False, True])
def test_live_vector_hinted_list_batches(live_vector_connection, pyformat, streamed):
    connection, base_type = live_vector_connection
    source = [0.1, 1.0001, -2.5]
    expected = _expected_stored_vector(source, base_type)
    wrapped = Vector(source, base_type=base_type)
    text = "x" * (5001 if streamed else 5)
    rows = [(0, None, text), (1, source, text), (2, wrapped, text)]
    sql = "INSERT INTO #hinted_vectors VALUES (?, ?, ?)"
    if pyformat:
        rows = [dict(id=row[0], v=row[1], label=row[2]) for row in rows]
        sql = "INSERT INTO #hinted_vectors VALUES (%(id)s, %(v)s, %(label)s)"
    with connection.cursor() as cursor:
        cursor.execute(
            f"CREATE TABLE #hinted_vectors (id INT, embedding VECTOR(3, {base_type}), label NVARCHAR(MAX))"
        )
        cursor.setinputsizes(
            [
                (mssql_python.SQL_INTEGER, 10, 0),
                (mssql_python.SQL_SS_VECTOR, 3, int(base_type == "float16")),
                (
                    mssql_python.SQL_WLONGVARCHAR if streamed else mssql_python.SQL_WVARCHAR,
                    len(text),
                    0,
                ),
            ]
        )
        cursor.executemany(sql, rows)
        assert source == [0.1, 1.0001, -2.5]
        assert cursor._inputsizes is None
        cursor.execute("SELECT embedding, label FROM #hinted_vectors ORDER BY id")
        assert [tuple(row) for row in cursor.fetchall()] == [
            (None, text),
            (expected, text),
            (expected, text),
        ]


@pytest.mark.parametrize("method", ["execute", "executemany"])
@pytest.mark.parametrize("invalid", [[1, 2], [1e40, 2, 3], [None, 2, 3], [[1], 2, 3]])
def test_live_vector_hinted_list_validation_and_recovery(live_vector_connection, method, invalid):
    connection, base_type = live_vector_connection
    with connection.cursor() as cursor:
        cursor.execute(f"CREATE TABLE #hint_errors (embedding VECTOR(3, {base_type}))")
        connection.commit()
        cursor.setinputsizes([(mssql_python.SQL_SS_VECTOR, 3, int(base_type == "float16"))])
        with pytest.raises((TypeError, ValueError)):
            if method == "execute":
                cursor.execute("INSERT INTO #hint_errors VALUES (?)", (invalid,))
            else:
                cursor.executemany(
                    "INSERT INTO #hint_errors VALUES (?)", [([1, 2, 3],), (invalid,)]
                )
        cursor.setinputsizes([])
        cursor.execute("SELECT COUNT(*) FROM #hint_errors")
        assert cursor.fetchval() == 0
        cursor.setinputsizes([(mssql_python.SQL_SS_VECTOR, 3, int(base_type == "float16"))])
        cursor.execute("INSERT INTO #hint_errors VALUES (?)", ([1, 2, 3],))
        cursor.execute("SELECT embedding FROM #hint_errors")
        assert cursor.fetchval() == Vector([1, 2, 3], base_type=base_type)


@pytest.mark.parametrize("method", ["execute", "executemany"])
def test_live_vector_numeric_boundaries(live_vector_connection, method):
    connection, base_type = live_vector_connection
    limit = 65504.0 if base_type == "float16" else 3.4028234663852886e38
    source = [limit, -limit, 0.1]
    expected = _expected_stored_vector(source, base_type)
    with connection.cursor() as cursor:
        cursor.execute(f"CREATE TABLE #range_vectors (embedding VECTOR(3, {base_type}))")
        connection.commit()
        cursor.setinputsizes([(mssql_python.SQL_SS_VECTOR, 3, int(base_type == "float16"))])
        if method == "execute":
            cursor.execute("INSERT INTO #range_vectors VALUES (?)", (source,))
        else:
            cursor.executemany("INSERT INTO #range_vectors VALUES (?)", [(source,)])
        cursor.execute("SELECT embedding FROM #range_vectors")
        assert cursor.fetchval() == expected
        for invalid in (3.5e38, -3.5e38, math.inf, math.nan):
            with pytest.raises(ValueError, match="finite float32"):
                Vector([invalid], base_type=base_type)
        if base_type == "float16":
            with pytest.raises(ValueError, match="float16"):
                Vector([70000], base_type=base_type)
        cursor.execute("SELECT COUNT(*) FROM #range_vectors")
        assert cursor.fetchval() == 1


def test_live_vector_tiny_values(live_vector_connection):
    connection, base_type = live_vector_connection
    source = [1e-50, 1e-40, -0.0]
    with connection.cursor() as cursor:
        cursor.execute("SELECT ?", Vector(source, base_type=base_type))
        assert cursor.fetchval() == _expected_stored_vector(source, base_type)


def test_live_vector_rebinding_prepared_statement(live_vector_connection):
    connection, base_type = live_vector_connection
    with connection.cursor() as cursor:
        for dimensions in (3, 4, 1, 3):
            value = Vector(range(dimensions), base_type=base_type)
            cursor.execute("SELECT ? AS embedding", value)
            assert cursor.fetchval() == value
            cursor.setinputsizes(
                [(mssql_python.SQL_SS_VECTOR, dimensions, int(base_type == "float16"))]
            )
            cursor.execute("SELECT ? AS embedding", None)
            assert cursor.fetchval() is None


@pytest.mark.parametrize("method", ["fetchone", "fetchmany", "fetchval", "iteration"])
def test_live_vector_buffered_procedure_fetch_reuse(live_vector_connection, method):
    connection, base_type = live_vector_connection
    value = Vector([1, 2, 3], base_type=base_type)
    with connection.cursor() as cursor:
        cursor.execute("SELECT n FROM (VALUES (1), (2), (3)) AS t(n) ORDER BY n")
        assert cursor.fetchone()[0] == 1
        assert cursor.fetchone()[0] == 2
        cursor.execute(
            f"CREATE PROCEDURE #buffered_vector @v VECTOR(3, {base_type}) AS "
            "BEGIN SET NOCOUNT ON; "
            "SELECT @v AS embedding FROM (VALUES (1), (2), (3), (4)) AS t(n) ORDER BY n; END"
        )
        assert cursor.callproc("#buffered_vector", [value]) == [value]
        if method == "fetchone":
            actual = [cursor.fetchone()[0] for _ in range(4)]
            assert cursor.fetchone() is None
        elif method == "fetchmany":
            actual = [row[0] for _ in range(4) for row in cursor.fetchmany(1)]
            assert cursor.fetchmany(1) == []
        elif method == "fetchval":
            actual = [cursor.fetchval() for _ in range(4)]
            assert cursor.fetchval() is None
        else:
            actual = [row[0] for row in cursor]
        assert actual == [value] * 4
        cursor.execute("SELECT 42")
        assert cursor.fetchval() == 42


def test_live_vector_scalar_array_and_nulls(live_vector_connection):
    connection, dtype = live_vector_connection
    value = Vector([1.25, -2.5, 0], dtype)
    with connection.cursor() as cursor:
        cursor.execute(f"CREATE TABLE #vectors (id INT, embedding VECTOR(3, {dtype}) NULL)")
        cursor.execute("INSERT INTO #vectors VALUES (?, ?)", 1, value)
        cursor.executemany("INSERT INTO #vectors VALUES (?, ?)", [(2, None), (3, value)])
        cursor.setinputsizes([(mssql_python.SQL_SS_VECTOR, 3, int(dtype == "float16"))])
        cursor.execute("INSERT INTO #vectors VALUES (4, ?)", None)
        cursor.execute("SELECT embedding FROM #vectors ORDER BY id")
        assert cursor.description[0][1] is Vector
        assert cursor.fetchone()[0] == value
        assert cursor.fetchmany(1)[0][0] is None
        assert [row[0] for row in cursor.fetchall()] == [value, None]
        assert cursor.fetchone() is None
        cursor.execute("SELECT embedding FROM #vectors WHERE id=1")
        assert cursor.fetchall()[0][0] == value


@pytest.mark.parametrize("with_results", [False, True], ids=["outputs-only", "multiple-results"])
def test_live_vector_procedure_outputs(live_vector_connection, with_results):
    connection, dtype = live_vector_connection
    value = Vector([1.25, -2.5, 0], dtype)
    selects = "SELECT @i AS embedding; SELECT @o AS embedding;" if with_results else ""
    with connection.cursor() as cursor:
        cursor.execute(
            f"CREATE PROCEDURE #vector_echo @i VECTOR(3, {dtype}), "
            f"@o VECTOR(3, {dtype}) OUTPUT, @io VECTOR(3, {dtype}) OUTPUT AS "
            f"BEGIN SET NOCOUNT ON; SET @o=@i; SET @io=@i; {selects} END"
        )
        output = OutputParameter(mssql_python.SQL_SS_VECTOR, 3, int(dtype == "float16"))
        inout = InputOutputParameter(Vector([0, 0, 0], dtype))
        assert cursor.callproc("#vector_echo", [value, output, inout]) == [value] * 3
        assert output.value == inout.value == value
        if with_results:
            assert cursor.fetchone()[0] == value
            assert cursor.nextset()
            assert cursor.fetchall()[0][0] == value
        else:
            assert cursor.description is None
        assert not cursor.nextset()
        assert cursor.callproc("#vector_echo", [None, output, inout]) == [None] * 3
        assert output.value is None and inout.value is None
        if not with_results:
            assert cursor.description is None
        if with_results:
            assert cursor.fetchone()[0] is None
            assert cursor.nextset()
            assert cursor.fetchone()[0] is None
        # Reusing the wrapper after a NULL result binds a typed NULL input.
        cursor.execute(
            f"CREATE PROCEDURE #vector_inout_null @io VECTOR(3, {dtype}) OUTPUT AS "
            "BEGIN SET NOCOUNT ON; "
            "SELECT CAST(CASE WHEN @io IS NULL THEN 1 ELSE 0 END AS BIT) AS was_null; "
            f"SET @io = CAST('[4, 5, 6]' AS VECTOR(3, {dtype})); END"
        )
        expected = Vector([4, 5, 6], dtype)
        assert cursor.callproc("#vector_inout_null", [inout]) == [expected]
        assert inout.value == expected
        assert cursor.fetchone()[0] is True
        cursor.execute("SELECT 1", reset_cursor=False)
        assert cursor.fetchone()[0] == 1


@pytest.mark.parametrize("failure", ["missing-procedure", "procedure-error"])
def test_live_failing_callproc_reprepares_previous_query(live_vector_connection, failure):
    connection, base_type = live_vector_connection
    value = Vector([1, 2, 3], base_type=base_type)
    with connection.cursor() as cursor:
        if failure == "procedure-error":
            cursor.execute(
                f"CREATE PROCEDURE #failing_vector @v VECTOR(3, {base_type}) AS "
                "BEGIN SET NOCOUNT ON; THROW 50000, 'expected procedure failure', 1; END"
            )
        query = f"SELECT CAST(? AS VECTOR(3, {base_type})) AS embedding"
        cursor.execute(query, value)
        assert cursor.fetchval() == value
        assert cursor.is_stmt_prepared == [True]
        with pytest.raises(mssql_python.DatabaseError):
            cursor.callproc("#failing_vector", [value])
        assert cursor.last_executed_stmt == ""
        assert cursor.is_stmt_prepared == [False]
        replacement = Vector([4, 5, 6], base_type=base_type)
        cursor.execute(query, replacement)
        assert cursor.fetchval() == replacement


@pytest.mark.parametrize(
    "invalid,error",
    [(1, TypeError), (True, TypeError), ("abc", TypeError), ([1], TypeError)],
)
def test_live_vector_rejects_mutated_inout(live_vector_connection, invalid, error):
    connection, base_type = live_vector_connection
    parameter = InputOutputParameter(Vector([1, 2, 3], base_type=base_type))
    parameter.value = invalid
    with connection.cursor() as cursor:
        with pytest.raises(error, match="INPUT_OUTPUT.*Vector"):
            cursor.callproc("#invalid_inout", [parameter])
        cursor.execute("SELECT 1")
        assert cursor.fetchval() == 1


@pytest.mark.parametrize("method", ["execute", "executemany"])
def test_live_vector_maximum_dimensions_and_rounding(live_vector_connection, method):
    connection, dtype = live_vector_connection
    dimensions = 3996 if dtype == "float16" else 1998
    value = Vector(repeat(0.1, dimensions), dtype)
    format_code = "<e" if dtype == "float16" else "<f"
    expected = struct.unpack(format_code, struct.pack(format_code, 0.1))[0]
    with connection.cursor() as cursor:
        if method == "execute":
            cursor.execute("SELECT ?", value)
        else:
            cursor.execute(
                f"CREATE TABLE #max_vectors (id INT, embedding VECTOR({dimensions}, {dtype}))"
            )
            cursor.executemany(
                "INSERT INTO #max_vectors VALUES (?, ?)", [(1, value), (2, None), (3, value)]
            )
            cursor.execute("SELECT embedding FROM #max_vectors ORDER BY id")
        actual = cursor.fetchone()[0]
        assert actual.base_type == dtype
        assert actual.dimensions == dimensions
        assert actual.values == (expected,) * dimensions
        if method == "executemany":
            assert [row[0] for row in cursor.fetchall()] == [None, actual]


def test_live_buffered_procedure_empty_results_rowcount(live_vector_connection):
    connection, dtype = live_vector_connection
    value = Vector([1, 2, 3], dtype)
    with connection.cursor() as cursor:
        cursor.execute(
            f"CREATE PROCEDURE #empty_vector_results @v VECTOR(3, {dtype}) AS "
            "BEGIN SET NOCOUNT ON; SELECT @v AS embedding WHERE 1 = 0; "
            "SELECT @v AS embedding; SELECT @v AS embedding WHERE 1 = 0; END"
        )
        assert cursor.callproc("#empty_vector_results", [value]) == [value]
        assert cursor.fetchall() == []
        assert cursor.rowcount == 0
        assert cursor.nextset()
        assert [row[0] for row in cursor.fetchall()] == [value]
        assert cursor.rowcount == 1
        assert cursor.nextset()
        assert cursor.fetchall() == []
        assert cursor.rowcount == 0
        assert not cursor.nextset()


@pytest.mark.parametrize("name", ["[#quoted.vector]", "[#quoted]]vector]"])
def test_live_callproc_bracketed_names(live_vector_connection, name):
    connection, dtype = live_vector_connection
    value = Vector([1, 2, 3], dtype)
    with connection.cursor() as cursor:
        cursor.execute(
            f"CREATE PROCEDURE {name} @v VECTOR(3, {dtype}) AS "
            "BEGIN SET NOCOUNT ON; SELECT @v AS embedding; END"
        )
        assert cursor.callproc(name, [value]) == [value]
        assert cursor.fetchone()[0] == value


def test_live_callproc_uses_decoding_changed_after_cursor_creation(live_vector_connection):
    connection, _ = live_vector_connection
    with connection.cursor() as cursor:
        cursor.execute(
            "CREATE PROCEDURE #varchar_probe AS BEGIN SET NOCOUNT ON; "
            "SELECT CAST(0x80 AS VARCHAR(1)) COLLATE SQL_Latin1_General_CP1_CI_AS AS value; END"
        )
        connection.setdecoding(
            mssql_python.SQL_CHAR, encoding="latin-1", ctype=mssql_python.SQL_CHAR
        )
        assert cursor.callproc("#varchar_probe") == []
        assert cursor.fetchone()[0] == "\x80"


def test_live_fetch_and_outputs_use_trusted_vector_construction(
    live_vector_connection, monkeypatch
):
    connection, dtype = live_vector_connection
    value = Vector([1.25, -2.5, 0], dtype)
    output = OutputParameter(mssql_python.SQL_SS_VECTOR, 3, int(dtype == "float16"))
    with connection.cursor() as cursor:
        cursor.execute(
            f"CREATE PROCEDURE #trusted_vector @i VECTOR(3, {dtype}), "
            f"@o VECTOR(3, {dtype}) OUTPUT AS BEGIN SET NOCOUNT ON; SET @o=@i; END"
        )
        cursor.execute(f"CREATE TABLE #trusted (id INT, embedding VECTOR(3, {dtype}))")
        cursor.executemany(
            "INSERT INTO #trusted VALUES (?, ?)", [(i, value) for i in range(50)] + [(50, None)]
        )
        # Values produced by the driver must not pass through the public constructor.
        public_init = Vector.__init__
        calls = []

        def counting_init(self, *args, **kwargs):
            calls.append(args)
            public_init(self, *args, **kwargs)

        monkeypatch.setattr(Vector, "__init__", counting_init)
        cursor.execute("SELECT embedding FROM #trusted ORDER BY id")
        rows = [row[0] for row in cursor.fetchall()]
        assert rows == [value] * 50 + [None]
        assert all(row.base_type == dtype for row in rows[:-1])
        assert calls == []
        monkeypatch.setattr(Vector, "__init__", public_init)
        cursor.callproc("#trusted_vector", [value, output])
        assert output.value == value


@pytest.mark.parametrize("method", ["fetchone", "fetchmany", "fetchall", "iteration"])
@pytest.mark.parametrize("with_lob", [False, True], ids=["vector", "vector-with-lob"])
def test_live_vector_fetch_apis(live_vector_connection, method, with_lob):
    connection, dtype = live_vector_connection
    value = Vector([1.25, -2.5, 0], dtype)
    with connection.cursor() as cursor:
        cursor.execute(f"CREATE TABLE #fetch_vectors (id INT, embedding VECTOR(3, {dtype}))")
        cursor.executemany(
            "INSERT INTO #fetch_vectors VALUES (?, ?)", [(1, value), (2, None), (3, value)]
        )
        lob = ", REPLICATE(CAST(N'x' AS NVARCHAR(MAX)), 5001) AS payload" if with_lob else ""
        query = f"SELECT embedding, id{lob} FROM #fetch_vectors ORDER BY id"
        assert cursor.execute(query) is cursor
        assert cursor.description[0][1] is Vector
        assert cursor.description[0][3] == ddbc_bindings._vector_buffer_size(3)
        assert cursor.description[0][5] == int(dtype == "float16")
        assert cursor.description[0][6]
        if method == "fetchone":
            rows = []
            while (row := cursor.fetchone()) is not None:
                rows.append(row)
        elif method == "fetchmany":
            cursor.arraysize = 2
            rows = cursor.fetchmany() + cursor.fetchmany(2)
            assert cursor.fetchmany() == []
        elif method == "fetchall":
            rows = cursor.fetchall()
            assert cursor.fetchall() == []
        else:
            rows = list(cursor)
            assert list(cursor) == []
        assert [row.embedding for row in rows] == [value, None, value]
        assert [row.id for row in rows] == [1, 2, 3]
        assert [row._mapping["embedding"] for row in rows] == [value, None, value]
        assert cursor.rowcount == 3
        assert cursor.rownumber == 2
        if with_lob:
            assert all(row.payload == "x" * 5001 for row in rows)
        cursor.execute(f"SELECT embedding FROM #fetch_vectors WHERE id=0")
        assert cursor.fetchone() is None
        assert cursor.fetchmany(2) == cursor.fetchall() == []
        assert cursor.rowcount == 0


@pytest.mark.parametrize("style", ["varargs", "tuple", "list", "pyformat"])
def test_live_vector_execute_parameter_styles(live_vector_connection, style):
    connection, dtype = live_vector_connection
    value = Vector([1.25, -2.5, 0], dtype)
    with connection.cursor() as cursor:
        sql = "SELECT ? AS id, ? AS embedding"
        if style == "varargs":
            cursor.execute(sql, 1, value)
        elif style == "tuple":
            cursor.execute(sql, (1, value))
        elif style == "list":
            cursor.execute(sql, [1, value])
        else:
            cursor.execute(
                "SELECT %(id)s AS id, %(embedding)s AS embedding",
                {"id": 1, "embedding": value},
            )
        row = cursor.fetchone()
        assert row.id == 1
        assert row.embedding == value


@pytest.mark.parametrize("explicit", [False, True], ids=["described-null", "typed-null"])
@pytest.mark.parametrize("all_null", [False, True], ids=["mixed", "all-null"])
def test_live_vector_executemany_nulls(live_vector_connection, explicit, all_null):
    connection, dtype = live_vector_connection
    value = Vector([1.25, -2.5, 0], dtype)
    values = [None, None, None] if all_null else [None, value, None]
    with connection.cursor() as cursor:
        cursor.execute(f"CREATE TABLE #batch_vectors (embedding VECTOR(3, {dtype}) NULL, id INT)")
        if explicit:
            cursor.setinputsizes(
                [
                    (mssql_python.SQL_SS_VECTOR, 3, int(dtype == "float16")),
                    (mssql_python.SQL_INTEGER, 10, 0),
                ]
            )
        cursor.executemany(
            "INSERT INTO #batch_vectors VALUES (?, ?)",
            [(item, index) for index, item in enumerate(values)],
        )
        cursor.execute("SELECT embedding FROM #batch_vectors ORDER BY id")
        assert [row[0] for row in cursor.fetchall()] == values
        cursor.executemany(
            "INSERT INTO #batch_vectors VALUES (%(embedding)s, %(id)s)",
            [{"embedding": value, "id": 4}, {"embedding": None, "id": 5}],
        )
        cursor.execute("SELECT embedding FROM #batch_vectors WHERE id>=4 ORDER BY id")
        assert [row[0] for row in cursor.fetchall()] == [value, None]


def test_live_vector_executemany_with_streamed_text(live_vector_connection):
    connection, dtype = live_vector_connection
    first = Vector([1, 2, 3], dtype)
    second = Vector([4, 5, 6], dtype)
    text = "streamed " * 1000
    with connection.cursor() as cursor:
        cursor.execute(
            f"CREATE TABLE #stream_vectors (id INT, embedding VECTOR(3, {dtype}), label NVARCHAR(MAX))"
        )
        cursor.executemany(
            "INSERT INTO #stream_vectors VALUES (?, ?, ?)",
            [(1, first, text), (2, second, text + "second"), (3, None, text)],
        )
        cursor.execute("SELECT embedding, label FROM #stream_vectors ORDER BY id")
        assert [tuple(row) for row in cursor.fetchall()] == [
            (first, text),
            (second, text + "second"),
            (None, text),
        ]


def test_live_vector_commit_rollback_and_multiple_cursors(live_vector_connection):
    connection, dtype = live_vector_connection
    value = Vector([1, 2, 3], dtype)
    with connection.cursor() as writer, connection.cursor() as reader:
        writer.execute(f"CREATE TABLE #tx_vectors (embedding VECTOR(3, {dtype}))")
        connection.commit()
        writer.execute("INSERT INTO #tx_vectors VALUES (?)", value)
        connection.commit()
        writer.execute("INSERT INTO #tx_vectors VALUES (?)", Vector([4, 5, 6], dtype))
        connection.rollback()
        reader.execute("SELECT embedding FROM #tx_vectors")
        assert [row[0] for row in reader.fetchall()] == [value]
        writer.close()
        reader.execute("SELECT embedding FROM #tx_vectors")
        assert reader.fetchone()[0] == value


def test_live_vector_input_hints_reject_mismatch_and_cursor_recovers(live_vector_connection):
    connection, dtype = live_vector_connection
    wrong_dtype = "float32" if dtype == "float16" else "float16"
    with connection.cursor() as cursor:
        for value in (Vector([1, 2], dtype), Vector([1, 2, 3], wrong_dtype)):
            cursor.setinputsizes([(mssql_python.SQL_SS_VECTOR, 3, int(dtype == "float16"))])
            with pytest.raises(ValueError, match="dimensions|subtype"):
                cursor.execute("SELECT ?", value)
            cursor.execute("SELECT 1")
            assert cursor.fetchone()[0] == 1
        cursor.execute(f"CREATE TABLE #mismatch_vectors (embedding VECTOR(3, {dtype}))")
        connection.commit()
        with pytest.raises(mssql_python.Error):
            cursor.execute("INSERT INTO #mismatch_vectors VALUES (?)", Vector([1, 2], dtype))
        cursor.execute("SELECT COUNT(*) FROM #mismatch_vectors")
        assert cursor.fetchone()[0] == 0


def test_live_vector_output_converters(live_vector_connection):
    connection, dtype = live_vector_connection
    value = Vector([1, 2, 3], dtype)
    with connection.cursor() as cursor:
        for key in (mssql_python.SQL_SS_VECTOR, Vector):
            connection.add_output_converter(key, lambda item: tuple(item))
            try:
                cursor.execute("SELECT ? AS embedding", value)
                assert cursor.fetchone()[0] == value.values
                cursor.execute(
                    f"CREATE OR ALTER PROCEDURE #converted_vector @v VECTOR(3, {dtype}) AS "
                    "BEGIN SET NOCOUNT ON; SELECT @v AS embedding; SELECT @v AS embedding; END"
                )
                assert cursor.callproc("#converted_vector", [value]) == [value]
                assert cursor.fetchone()[0] == value.values
                assert cursor.nextset()
                assert cursor.fetchone()[0] == value.values
            finally:
                connection.remove_output_converter(key)
        cursor.execute("SELECT ?", value)
        assert cursor.fetchone()[0] == value


@pytest.mark.parametrize("method", ["arrow_batch", "arrow", "arrow_reader"])
def test_live_vector_arrow_boundary(live_vector_connection, method):
    pytest.importorskip("pyarrow")
    connection, dtype = live_vector_connection
    value = Vector([1, 2, 3], dtype)
    with connection.cursor() as cursor:
        cursor.execute("SELECT ? AS embedding", value)
        with pytest.raises(mssql_python.NotSupportedError, match="row fetch"):
            getattr(cursor, method)()
        assert cursor.fetchone()[0] == value


def test_live_vector_bulkcopy_boundary(live_vector_connection):
    connection, dtype = live_vector_connection
    table_name = f"dbo.pytest_vector_bulk_{uuid.uuid4().hex}"
    connection.autocommit = True
    with connection.cursor() as cursor:
        cursor.execute(f"CREATE TABLE {table_name} (embedding VECTOR(3, {dtype}))")
        try:
            with pytest.raises(mssql_python.NotSupportedError, match="pycore"):
                cursor.bulkcopy(table_name, [(Vector([1, 2, 3], dtype),)])
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            assert cursor.fetchone()[0] == 0
        finally:
            cursor.execute(f"DROP TABLE {table_name}")


@pytest.mark.parametrize(
    "mode,dtype",
    [
        pytest.param("off", "float32", id="off-rejects-float32"),
        pytest.param("off", "float16", id="off-rejects-float16"),
        pytest.param("v1", "float16", id="v1-rejects-float16"),
    ],
)
def test_live_vector_negotiation_rejects_unsupported_inputs(conn_str, vector_server, mode, dtype):
    if not conn_str:
        pytest.skip("DB_CONNECTION_STRING is not configured")
    if dtype == "float16" and os.getenv("MSSQL_TEST_VECTOR_FLOAT16") != "1":
        pytest.skip("Set MSSQL_TEST_VECTOR_FLOAT16=1 with a FLOAT16-capable server")
    connection = mssql_python.connect(conn_str, VectorTypeSupport=mode)
    try:
        with connection.cursor() as cursor:
            with pytest.raises(mssql_python.Error):
                cursor.execute("SELECT ?", Vector([1, 2, 3], dtype))
            cursor.execute("SELECT 1")
            assert cursor.fetchone()[0] == 1
    finally:
        connection.close()


@pytest.mark.parametrize("dtype", ["float32", "float16"])
def test_live_vector_off_returns_json(conn_str, vector_server, dtype):
    if not conn_str:
        pytest.skip("DB_CONNECTION_STRING is not configured")
    if dtype == "float16" and os.getenv("MSSQL_TEST_VECTOR_FLOAT16") != "1":
        pytest.skip("Set MSSQL_TEST_VECTOR_FLOAT16=1 with a FLOAT16-capable server")
    connection = mssql_python.connect(conn_str, VectorTypeSupport="off")
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT CAST('[1.25,-2.5,0]' AS VECTOR(3, {dtype})) AS embedding")
            assert cursor.description[0][1] is str
            value = cursor.fetchone().embedding
            assert isinstance(value, str)
            assert json.loads(value) == [1.25, -2.5, 0]
            cursor.execute(f"SELECT CAST(NULL AS VECTOR(3, {dtype}))")
            assert cursor.fetchone()[0] is None
    finally:
        connection.close()


def test_live_vector_server_converts_between_base_types(conn_str, vector_server):
    if not conn_str:
        pytest.skip("DB_CONNECTION_STRING is not configured")
    if os.getenv("MSSQL_TEST_VECTOR_FLOAT16") != "1":
        pytest.skip("Set MSSQL_TEST_VECTOR_FLOAT16=1 with a FLOAT16-capable server")
    connection = mssql_python.connect(conn_str, VectorTypeSupport="v2", autocommit=True)
    try:
        with connection.cursor() as cursor:
            cursor.execute("CREATE TABLE #cross (f16 VECTOR(3, float16) NULL, f32 VECTOR(3) NULL)")
            # The driver sends each Vector with its own base type; SQL Server converts.
            cursor.execute("INSERT INTO #cross (f16) VALUES (?)", Vector([0.1, 65519.0, 1e-8]))
            cursor.execute(
                "INSERT INTO #cross (f32) VALUES (?)", Vector([0.1, 2, 3], base_type="float16")
            )
            cursor.execute("SELECT f16 FROM #cross WHERE f16 IS NOT NULL")
            stored = cursor.fetchval()
            assert stored.base_type == "float16"
            assert stored == _expected_stored_vector([0.1, 65504.0, 0.0], "float16")
            cursor.execute("SELECT f32 FROM #cross WHERE f32 IS NOT NULL")
            widened = cursor.fetchval()
            assert widened.base_type == "float32"
            assert widened.values == _expected_stored_vector([0.1, 2, 3], "float16").values
            with pytest.raises(mssql_python.Error, match="out-of-range"):
                cursor.execute("INSERT INTO #cross (f16) VALUES (?)", Vector([70000.0, 0, 0]))
            cursor.execute("SELECT COUNT(*) FROM #cross")
            assert cursor.fetchval() == 2
    finally:
        connection.close()
