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
from mssql_python import InputOutputParameter, OutputParameter, TableValuedParameter, Vector
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
    output = OutputParameter(3, base_type="float16")
    assert output.base_type == "float16"
    assert output.dimensions == 3
    assert output.value is None
    value = Vector([1, 2, 3], base_type="float16")
    inout = InputOutputParameter(value)
    assert inout.value == value
    with pytest.raises(TypeError):
        InputOutputParameter([1, 2])


def test_tvp_schema_and_rows():
    columns = [(mssql_python.SQL_INTEGER, 0, 0), (mssql_python.SQL_SS_VECTOR, 3, 1)]
    value = Vector([1, 2, 3], base_type="float16")
    tvp = TableValuedParameter("dbo.Embeddings", [(1, value), (2, None)], columns)
    assert tvp.rows == ((1, value), (2, None))
    assert TableValuedParameter("dbo.Embeddings", [], columns).rows == ()
    with pytest.raises(ValueError, match="columns"):
        TableValuedParameter("dbo.Embeddings", [(1,)], columns)
    with pytest.raises(ValueError, match="dimensions|subtype"):
        TableValuedParameter("dbo.Embeddings", [(1, Vector([1, 2]))], columns)


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


def test_tvp_vector_lists_use_explicit_schema_and_snapshot():
    source = [0.1, 2, 3]
    columns = [(mssql_python.SQL_SS_VECTOR, 3, 1)]
    parameter = TableValuedParameter("dbo.v", [(source,), (None,)], columns)
    expected = Vector(source, base_type="float16")
    source[0] = 9
    assert parameter.rows == ((expected,), (None,))
    for invalid, error in (([1, 2], ValueError), ([1e40, 2, 3], ValueError), ("123", TypeError)):
        with pytest.raises(error):
            TableValuedParameter("dbo.v", [(invalid,)], columns)


@pytest.mark.parametrize("dimensions", [True, 1.5, "3"])
def test_output_parameter_invalid_shape_type(dimensions):
    with pytest.raises(TypeError, match="integer"):
        OutputParameter(dimensions)


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


@pytest.mark.parametrize("name", ["", "dbo..v", "dbo.v;drop", "a.b.c", "dbo.\0v"])
def test_invalid_tvp_name(name):
    with pytest.raises(ValueError):
        TableValuedParameter(name, [], [(mssql_python.SQL_SS_VECTOR, 3, 0)])


def test_tvp_immutable_snapshot_and_native_detection():
    rows = [[Vector([1, 2, 3])]]
    columns = [[mssql_python.SQL_SS_VECTOR, 3, 0]]
    parameter = TableValuedParameter("dbo.v", rows, columns)
    rows.clear()
    columns.clear()
    assert len(parameter.rows) == 1
    with pytest.raises(AttributeError):
        parameter.rows = ()
    info = ddbc_bindings._detect_param_types([parameter])[0]
    assert info.paramSQLType == -153
    assert not info.isDAE


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
    output = OutputParameter(3)

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


@pytest.mark.parametrize("name", ["", "a..b", "a.b.c.d", "a\0b"])
def test_callproc_invalid_name_without_database(unit_cursor, name):
    with pytest.raises(ValueError):
        unit_cursor.callproc(name)


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
    core = SimpleNamespace()
    assert list(Cursor._adapt_bulkcopy_vectors(rows, core)) == rows
    with pytest.raises(mssql_python.NotSupportedError, match="pycore"):
        list(Cursor._adapt_bulkcopy_vectors([(1, Vector([1, 2, 3]))], core))


@pytest.mark.parametrize("base_type", ["float32", "float16"])
def test_bulkcopy_adapts_only_vectors_and_preserves_base_type(base_type):
    value = Vector([0.1, 2, 3], base_type)
    converted = object()
    factory = Mock(return_value=converted)
    core = SimpleNamespace(BulkCopyVector=factory)
    ordinary = (1, [1, 2, 3], None)
    rows = list(Cursor._adapt_bulkcopy_vectors(iter([ordinary, (value, None)]), core))
    assert rows[0] is ordinary
    assert rows[1] == (converted, None)
    factory.assert_called_once_with(value.values, base_type)


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


@pytest.fixture
def vector_bulk_core():
    core = pytest.importorskip("mssql_py_core")
    if not hasattr(core, "BulkCopyVector"):
        pytest.skip(
            "Vector bulk-copy integration requires the companion vector-capable pycore build"
        )
    return core


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
        output = OutputParameter(3, dtype)
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
        cursor.execute("SELECT 1", reset_cursor=False)
        assert cursor.fetchone()[0] == 1


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


@pytest.mark.parametrize("method", ["execute", "callproc"])
def test_live_vector_tvp(live_vector_connection, method):
    connection, dtype = live_vector_connection
    suffix = uuid.uuid4().hex
    type_name = f"dbo.pytest_vector_type_{suffix}"
    proc_name = f"dbo.pytest_vector_proc_{suffix}"
    # Named types cannot be temporary; use unique names and scoped cleanup.
    connection.autocommit = True
    value = Vector([1.25, -2.5, 0], dtype)
    with connection.cursor() as cursor:
        cursor.execute(f"CREATE TYPE {type_name} AS TABLE (id INT, embedding VECTOR(3, {dtype}))")
        try:
            cursor.execute(
                f"CREATE PROCEDURE {proc_name} @rows {type_name} READONLY AS "
                "BEGIN SET NOCOUNT ON; SELECT id, embedding FROM @rows ORDER BY id; END"
            )
            try:
                columns = [
                    (mssql_python.SQL_INTEGER, 10, 0),
                    (mssql_python.SQL_SS_VECTOR, 3, int(dtype == "float16")),
                ]
                for rows in (
                    [(1, value), (2, None), (3, value)],
                    [(1, [1.25, -2.5, 0])],
                    [(1, None), (2, None)],
                    [],
                ):
                    parameter = TableValuedParameter(type_name, rows, columns)
                    if method == "callproc":
                        assert cursor.callproc(proc_name, [parameter]) == [parameter]
                    else:
                        cursor.execute(f"EXEC {proc_name} ?", parameter)
                    assert cursor.description[1][1] is Vector
                    assert [tuple(row) for row in cursor.fetchall()] == list(parameter.rows)
            finally:
                cursor.execute(f"DROP PROCEDURE {proc_name}")
        finally:
            cursor.execute(f"DROP TYPE {type_name}")


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
def test_live_vector_arrow_roundtrip(live_vector_connection, method):
    pa = pytest.importorskip("pyarrow")
    connection, dtype = live_vector_connection
    value = Vector([1, 2, 3], dtype)
    with connection.cursor() as cursor:
        cursor.execute("SELECT ? AS embedding", value)
        result = getattr(cursor, method)(batch_size=2)
        if method == "arrow_reader":
            with result:
                result = result.read_all()
        assert result.schema.field("embedding").type == pa.list_(
            pa.field("item", pa.float32(), nullable=False), 3
        )
        assert result.schema.field("embedding").metadata == {
            b"mssql.vector.base_type": dtype.encode()
        }
        assert result.column(0).to_pylist() == [list(value)]


@pytest.mark.parametrize("source_kind", ["tuples", "generator", "rows", "lists"])
def test_live_vector_bulkcopy_roundtrip(live_vector_connection, vector_bulk_core, source_kind):
    connection, dtype = live_vector_connection
    table_name = f"dbo.pytest_vector_bulk_{uuid.uuid4().hex}"
    connection.autocommit = True
    with connection.cursor() as cursor:
        cursor.execute(f"CREATE TABLE {table_name} (id INT, embedding VECTOR(3, {dtype}))")
        try:
            value = Vector([0.1, -0.0, 1.0001], dtype)
            data = [(1, None), (2, value), (3, value)]
            if source_kind == "generator":
                data = (row for row in data)
            elif source_kind == "lists":
                data = [(row_id, list(v) if v is not None else None) for row_id, v in data]
            elif source_kind == "rows":
                cursor.execute(
                    "SELECT 1 AS id, CAST(NULL AS VECTOR(3, " + dtype + ")) AS embedding "
                    "UNION ALL SELECT 2, ? UNION ALL SELECT 3, ?",
                    value,
                    value,
                )
                data = cursor.fetchall()
            result = cursor.bulkcopy(
                table_name,
                data,
                batch_size=2,
                column_mappings=[(0, "id"), (1, "embedding")],
            )
            assert result["rows_copied"] == 3
            cursor.execute(f"SELECT id, embedding FROM {table_name} ORDER BY id")
            rows = cursor.fetchall()
            expected = _expected_stored_vector(value, dtype)
            assert [tuple(row) for row in rows] == [(1, None), (2, expected), (3, expected)]
        finally:
            cursor.execute(f"DROP TABLE {table_name}")


@pytest.mark.parametrize("batch_size", [0, 1, 7, 64])
def test_live_vector_arrow_nulls_slices_and_ownership(live_vector_connection, batch_size):
    import gc

    pa = pytest.importorskip("pyarrow")
    connection, dtype = live_vector_connection
    value = Vector([0.1, 2.5, -0.0], dtype)
    expected = list(_expected_stored_vector(value, dtype))
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT TOP (71) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS id, "
            "CASE WHEN ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) % 2 = 0 "
            f"THEN ? ELSE CAST(NULL AS VECTOR(3, {dtype})) END AS v, "
            "REPLICATE(CAST(N'x' AS NVARCHAR(MAX)), 9000) AS text_value "
            "FROM sys.all_objects a",
            value,
        )
        schema_only = cursor.arrow_batch(0)
        assert schema_only.num_rows == 0
        assert schema_only.schema.field("v").type == pa.list_(
            pa.field("item", pa.float32(), nullable=False), 3
        )
        table = cursor.arrow(batch_size or 7)
        assert table.num_rows == 71
        assert all(v is None or v == expected for v in table.column("v").to_pylist())
        assert table.column("v").null_count > 0
        assert table.column("text_value").to_pylist() == ["x" * 9000] * 71
        sliced = table.slice(1, 5)
        retained = sliced.column("v").chunk(0)
        retained_children = retained.values
        expected_children = retained_children.to_pylist()
        cursor.execute("SELECT 1")
    del table, sliced
    gc.collect()
    assert all(v is None or v == expected for v in retained.to_pylist())
    del retained
    gc.collect()
    assert retained_children.to_pylist() == expected_children


@pytest.mark.parametrize(
    "source_kind", ["table", "batch", "reader", "batches", "c_stream", "c_array"]
)
def test_live_vector_bulkcopy_arrow_roundtrip(
    live_vector_connection, vector_bulk_core, source_kind
):
    pa = pytest.importorskip("pyarrow")
    connection, dtype = live_vector_connection
    connection.autocommit = True
    table_name = f"dbo.pytest_vector_arrow_{uuid.uuid4().hex}"
    value = [0.1, -0.0, 1.0001]
    with connection.cursor() as cursor:
        cursor.execute(f"CREATE TABLE {table_name} (id INT, embedding VECTOR(3, {dtype}))")
        try:
            field = pa.field(
                "v",
                pa.list_(pa.float32(), 3),
                metadata={b"mssql.vector.base_type": dtype.encode()},
            )
            batch = pa.record_batch(
                [
                    pa.array([99, 1, 2, 3], type=pa.int32()),
                    pa.array([value, None, value, value], type=field.type),
                ],
                schema=pa.schema([pa.field("i", pa.int32()), field]),
            ).slice(1)
            if source_kind == "table":
                source = pa.Table.from_batches([batch.slice(0, 1), batch.slice(1)])
            elif source_kind == "reader":
                source = pa.RecordBatchReader.from_batches(batch.schema, [batch])
            elif source_kind == "batches":
                source = iter([batch])
            elif source_kind == "c_stream":

                class Stream:
                    def __arrow_c_stream__(self, requested_schema=None):
                        return pa.Table.from_batches([batch]).__arrow_c_stream__(requested_schema)

                source = Stream()
            elif source_kind == "c_array":

                class Array:
                    def __arrow_c_array__(self, requested_schema=None):
                        return batch.__arrow_c_array__(requested_schema)

                source = Array()
            else:
                source = batch
            result = cursor.bulkcopy_arrow(
                table_name,
                source,
                batch_size=2,
                column_mappings=[(1, "embedding"), (0, "id")],
            )
            assert result["rows_copied"] == 3
            cursor.execute(f"SELECT id, embedding FROM {table_name} ORDER BY id")
            expected = _expected_stored_vector(value, dtype)
            assert [tuple(row) for row in cursor.fetchall()] == [
                (1, None),
                (2, expected),
                (3, expected),
            ]
            copy_name = table_name + "_copy"
            cursor.execute(f"CREATE TABLE {copy_name} (id INT, embedding VECTOR(3, {dtype}))")
            cursor.execute(f"SELECT id, embedding FROM {table_name} ORDER BY id")
            with cursor.arrow_reader(batch_size=2) as reader:
                copied = cursor.bulkcopy_arrow(
                    copy_name, reader, column_mappings=["id", "embedding"]
                )
            assert copied["rows_copied"] == 3
        finally:
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}_copy")
            cursor.execute(f"DROP TABLE {table_name}")


@pytest.mark.parametrize(
    "method,failure",
    [
        ("bulkcopy", "dimensions"),
        ("bulkcopy", "base_type"),
        ("bulkcopy_arrow", "dimensions"),
        ("bulkcopy_arrow", "base_type"),
        ("bulkcopy_arrow", "nonfinite"),
        ("bulkcopy_arrow", "null_element"),
        ("bulkcopy_arrow", "range"),
    ],
)
def test_live_vector_bulkcopy_validation(live_vector_connection, vector_bulk_core, method, failure):
    pa = pytest.importorskip("pyarrow")
    connection, dtype = live_vector_connection
    connection.autocommit = True
    table_name = f"dbo.pytest_vector_invalid_{uuid.uuid4().hex}"
    with connection.cursor() as cursor:
        cursor.execute(f"CREATE TABLE {table_name} (embedding VECTOR(3, {dtype}))")
        try:
            if method == "bulkcopy":
                value = Vector(
                    [1, 2] if failure == "dimensions" else [1, 2, 3],
                    (
                        ("float16" if dtype == "float32" else "float32")
                        if failure == "base_type"
                        else dtype
                    ),
                )
                source = [(value,)]
            else:
                values = {
                    "dimensions": [1, 2],
                    "base_type": [1, 2, 3],
                    "nonfinite": [math.inf, 2, 3],
                    "null_element": [None, 2, 3],
                    "range": [65505, 2, 3],
                }[failure]
                arrow_type = pa.list_(pa.float32(), len(values))
                base_type = (
                    ("float16" if dtype == "float32" else "float32")
                    if failure == "base_type"
                    else dtype
                )
                field = pa.field(
                    "embedding",
                    arrow_type,
                    metadata={b"mssql.vector.base_type": base_type.encode()},
                )
                source = pa.record_batch(
                    [pa.array([values], type=arrow_type)], schema=pa.schema([field])
                )
            expected_rows = 0
            if failure == "range" and dtype == "float32":
                assert getattr(cursor, method)(table_name, source)["rows_copied"] == 1
                expected_rows = 1
            else:
                with pytest.raises(
                    ValueError, match="(?i)vector|dimension|base_type|range|finite|NULL"
                ):
                    getattr(cursor, method)(table_name, source, use_internal_transaction=True)
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            assert cursor.fetchval() == expected_rows
            cursor.execute("SELECT 1")
            assert cursor.fetchval() == 1
        finally:
            cursor.execute(f"DROP TABLE {table_name}")


@pytest.mark.parametrize("method", ["bulkcopy", "bulkcopy_arrow"])
def test_live_vector_bulkcopy_maximum_dimensions(live_vector_connection, vector_bulk_core, method):
    pa = pytest.importorskip("pyarrow")
    connection, dtype = live_vector_connection
    connection.autocommit = True
    dimensions = 3996 if dtype == "float16" else 1998
    table_name = f"dbo.pytest_vector_max_bulk_{uuid.uuid4().hex}"
    value = Vector([0.125] * dimensions, dtype)
    with connection.cursor() as cursor:
        cursor.execute(f"CREATE TABLE {table_name} (embedding VECTOR({dimensions}, {dtype}))")
        try:
            if method == "bulkcopy":
                source = [(None,), (value,)]
            else:
                source = pa.table(
                    {
                        "embedding": pa.array(
                            [None, list(value)], type=pa.list_(pa.float32(), dimensions)
                        )
                    }
                )
            assert getattr(cursor, method)(table_name, source)["rows_copied"] == 2
            cursor.execute(f"SELECT embedding FROM {table_name} WHERE embedding IS NOT NULL")
            table = cursor.arrow()
            assert table.schema.field("embedding").type == pa.list_(
                pa.field("item", pa.float32(), nullable=False), dimensions
            )
            assert table.column(0).to_pylist() == [list(value)]
        finally:
            cursor.execute(f"DROP TABLE {table_name}")


@pytest.mark.parametrize("method", ["bulkcopy", "bulkcopy_arrow"])
@pytest.mark.parametrize("empty", [False, True])
def test_live_vector_bulkcopy_empty_and_all_null(
    live_vector_connection, vector_bulk_core, method, empty
):
    pa = pytest.importorskip("pyarrow")
    connection, dtype = live_vector_connection
    connection.autocommit = True
    table_name = f"dbo.pytest_vector_null_bulk_{uuid.uuid4().hex}"
    count = 0 if empty else 2
    with connection.cursor() as cursor:
        cursor.execute(f"CREATE TABLE {table_name} (embedding VECTOR(3, {dtype}))")
        try:
            if method == "bulkcopy":
                source = [(None,)] * count
            else:
                # Hidden child values under NULL parents must not be validated as data.
                values = pa.array([math.nan] * (3 * count), type=pa.float32())
                vectors = pa.FixedSizeListArray.from_arrays(
                    values, 3, mask=pa.array([True] * count, type=pa.bool_())
                )
                source = pa.table({"embedding": vectors})
            assert getattr(cursor, method)(table_name, source)["rows_copied"] == count
            cursor.execute(f"SELECT embedding FROM {table_name}")
            table = cursor.arrow()
            assert table.num_rows == count
            assert table.column(0).to_pylist() == [None] * count
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
