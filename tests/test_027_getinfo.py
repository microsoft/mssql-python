"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.
Regression coverage for SQLGetInfo IDs and ODBC return types (GH-769).
"""

import ast
from decimal import Decimal
from enum import Enum
import logging
from pathlib import Path
import pickle
import struct
import sys
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import mssql_python
from mssql_python import constants
from mssql_python.connection import Connection, _GETINFO_RETURN_TYPES
from mssql_python.constants import ConstantsDDBC, GetInfoConstants as G
from mssql_python.exceptions import DatabaseError, InterfaceError

# Independent reference from ODBC 3.x sql.h/sqlext.h (Windows SDK 10.0.26100.0
# and unixODBC 2.3.x), and the SQLGetInfo return-type descriptions:
# https://learn.microsoft.com/sql/odbc/reference/syntax/sqlgetinfo-function
# Each entry is (information-type ID, numeric byte width or 0 for text).
# Keep literal IDs here: deriving expectations from G would mask transcription
# mistakes. __members__ must be used so that erroneous Enum aliases are visible.
ODBC_INFO = {
    "SQL_DRIVER_NAME": (6, 0),
    "SQL_DRIVER_VER": (7, 0),
    "SQL_DRIVER_ODBC_VER": (77, 0),
    "SQL_DRIVER_HLIB": (76, struct.calcsize("P")),
    "SQL_DRIVER_HENV": (4, struct.calcsize("P")),
    "SQL_DRIVER_HDBC": (3, struct.calcsize("P")),
    "SQL_DATA_SOURCE_NAME": (2, 0),
    "SQL_DATABASE_NAME": (16, 0),
    "SQL_SERVER_NAME": (13, 0),
    "SQL_USER_NAME": (47, 0),
    "SQL_SQL_CONFORMANCE": (118, 4),
    "SQL_KEYWORDS": (89, 0),
    "SQL_IDENTIFIER_CASE": (28, 2),
    "SQL_IDENTIFIER_QUOTE_CHAR": (29, 0),
    "SQL_SPECIAL_CHARACTERS": (94, 0),
    "SQL_SUBQUERIES": (95, 4),
    "SQL_EXPRESSIONS_IN_ORDERBY": (27, 0),
    "SQL_CORRELATION_NAME": (74, 2),
    "SQL_SEARCH_PATTERN_ESCAPE": (14, 0),
    "SQL_CATALOG_TERM": (42, 0),
    "SQL_CATALOG_NAME_SEPARATOR": (41, 0),
    "SQL_SCHEMA_TERM": (39, 0),
    "SQL_TABLE_TERM": (45, 0),
    "SQL_PROCEDURES": (21, 0),
    "SQL_ACCESSIBLE_TABLES": (19, 0),
    "SQL_ACCESSIBLE_PROCEDURES": (20, 0),
    "SQL_CATALOG_NAME": (10003, 0),
    "SQL_CATALOG_USAGE": (92, 4),
    "SQL_SCHEMA_USAGE": (91, 4),
    "SQL_COLUMN_ALIAS": (87, 0),
    "SQL_DESCRIBE_PARAMETER": (10002, 0),
    "SQL_TXN_CAPABLE": (46, 2),
    "SQL_TXN_ISOLATION_OPTION": (72, 4),
    "SQL_DEFAULT_TXN_ISOLATION": (26, 4),
    "SQL_MULTIPLE_ACTIVE_TXN": (37, 0),
    "SQL_NUMERIC_FUNCTIONS": (49, 4),
    "SQL_STRING_FUNCTIONS": (50, 4),
    "SQL_TIMEDATE_FUNCTIONS": (52, 4),
    "SQL_DATETIME_FUNCTIONS": (52, 4),  # Deliberate Python compatibility spelling
    "SQL_SYSTEM_FUNCTIONS": (51, 4),
    "SQL_CONVERT_FUNCTIONS": (48, 4),
    "SQL_LIKE_ESCAPE_CLAUSE": (113, 0),
    "SQL_MAX_COLUMN_NAME_LEN": (30, 2),
    "SQL_MAX_TABLE_NAME_LEN": (35, 2),
    "SQL_MAX_SCHEMA_NAME_LEN": (32, 2),
    "SQL_MAX_CATALOG_NAME_LEN": (34, 2),
    "SQL_MAX_IDENTIFIER_LEN": (10005, 2),
    "SQL_MAX_STATEMENT_LEN": (105, 4),
    "SQL_MAX_CHAR_LITERAL_LEN": (108, 4),
    "SQL_MAX_BINARY_LITERAL_LEN": (112, 4),
    "SQL_MAX_COLUMNS_IN_TABLE": (101, 2),
    "SQL_MAX_COLUMNS_IN_SELECT": (100, 2),
    "SQL_MAX_COLUMNS_IN_GROUP_BY": (97, 2),
    "SQL_MAX_COLUMNS_IN_ORDER_BY": (99, 2),
    "SQL_MAX_COLUMNS_IN_INDEX": (98, 2),
    "SQL_MAX_TABLES_IN_SELECT": (106, 2),
    "SQL_MAX_CONCURRENT_ACTIVITIES": (1, 2),
    "SQL_MAX_DRIVER_CONNECTIONS": (0, 2),
    "SQL_MAX_ROW_SIZE": (104, 4),
    "SQL_MAX_USER_NAME_LEN": (107, 2),
    "SQL_ACTIVE_CONNECTIONS": (0, 2),
    "SQL_ACTIVE_STATEMENTS": (1, 2),
    "SQL_DATA_SOURCE_READ_ONLY": (25, 0),
    "SQL_NEED_LONG_DATA_LEN": (111, 0),
    "SQL_GETDATA_EXTENSIONS": (81, 4),
    "SQL_CURSOR_COMMIT_BEHAVIOR": (23, 2),
    "SQL_CURSOR_ROLLBACK_BEHAVIOR": (24, 2),
    "SQL_CURSOR_SENSITIVITY": (10001, 4),
    "SQL_BOOKMARK_PERSISTENCE": (82, 4),
    "SQL_DYNAMIC_CURSOR_ATTRIBUTES1": (144, 4),
    "SQL_DYNAMIC_CURSOR_ATTRIBUTES2": (145, 4),
    "SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1": (146, 4),
    "SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2": (147, 4),
    "SQL_STATIC_CURSOR_ATTRIBUTES1": (167, 4),
    "SQL_STATIC_CURSOR_ATTRIBUTES2": (168, 4),
    "SQL_KEYSET_CURSOR_ATTRIBUTES1": (150, 4),
    "SQL_KEYSET_CURSOR_ATTRIBUTES2": (151, 4),
    "SQL_SCROLL_OPTIONS": (44, 4),
    "SQL_SCROLL_CONCURRENCY": (43, 4),
    "SQL_FETCH_DIRECTION": (8, 4),
    "SQL_STATIC_SENSITIVITY": (83, 4),
    "SQL_BATCH_SUPPORT": (121, 4),
    "SQL_BATCH_ROW_COUNT": (120, 4),
    "SQL_PARAM_ARRAY_ROW_COUNTS": (153, 4),
    "SQL_PARAM_ARRAY_SELECTS": (154, 4),
    "SQL_PROCEDURE_TERM": (40, 0),
    "SQL_POSITIONED_STATEMENTS": (80, 4),
    "SQL_GROUP_BY": (88, 2),
    "SQL_OJ_CAPABILITIES": (115, 4),
    "SQL_ORDER_BY_COLUMNS_IN_SELECT": (90, 0),
    "SQL_OUTER_JOINS": (38, 0),
    "SQL_QUOTED_IDENTIFIER_CASE": (93, 2),
    "SQL_CONCAT_NULL_BEHAVIOR": (22, 2),
    "SQL_NULL_COLLATION": (85, 2),
    "SQL_ALTER_TABLE": (86, 4),
    "SQL_UNION": (96, 4),
    "SQL_DDL_INDEX": (170, 4),
    "SQL_MULT_RESULT_SETS": (36, 0),
    "SQL_OWNER_USAGE": (91, 4),
    "SQL_QUALIFIER_USAGE": (92, 4),
    "SQL_TIMEDATE_ADD_INTERVALS": (109, 4),
    "SQL_TIMEDATE_DIFF_INTERVALS": (110, 4),
}

LEGACY_GETINFO_CONSTANTS = {
    "SQL_TXN_ISOLATION_LEVEL": 108,
    "SQL_CONCURRENCY": 7,
    "SQL_ROWSET_SIZE": 9,
    "SQL_ROW_NUMBER": 14,
    "SQL_IC_UPPER": 1,
    "SQL_IC_LOWER": 2,
    "SQL_IC_SENSITIVE": 3,
    "SQL_IC_MIXED": 4,
    "SQL_SQL92_ENTRY_SQL": 127,
    "SQL_SQL92_INTERMEDIATE_SQL": 128,
    "SQL_SQL92_FULL_SQL": 129,
}
CONFORMANCE_VALUES = {
    "SQL_SC_SQL92_ENTRY": 1,
    "SQL_SC_FIPS127_2_TRANSITIONAL": 2,
    "SQL_SC_SQL92_INTERMEDIATE": 4,
    "SQL_SC_SQL92_FULL": 8,
}
NON_INFO_CONSTANTS = LEGACY_GETINFO_CONSTANTS | CONFORMANCE_VALUES

UNLISTED_ODBC_INFO = {
    "SQL_DBMS_NAME": (17, 0),
    "SQL_DBMS_VER": (18, 0),
    "SQL_XOPEN_CLI_YEAR": (10000, 0),
    "SQL_ASYNC_MODE": (10021, 4),
    "SQL_CREATE_ASSERTION": (127, 4),
}
ALL_ODBC_INFO = ODBC_INFO | UNLISTED_ODBC_INFO
NUMERIC_INFO = {name: spec for name, spec in ALL_ODBC_INFO.items() if spec[1]}
STRING_INFO = {name: spec for name, spec in ALL_ODBC_INFO.items() if not spec[1]}
DRIVER_MANAGER_INFO = {"SQL_DRIVER_HDBC", "SQL_DRIVER_HENV", "SQL_DRIVER_HLIB"}


@pytest.fixture
def mock_connection():
    # Avoid constructing a native connection for the decoder and export tests.
    return SimpleNamespace(_closed=False, _conn=Mock())


def test_getinfo_reference_covers_every_member_and_alias():
    assert set(G.__members__) == ODBC_INFO.keys() | LEGACY_GETINFO_CONSTANTS.keys()
    assert constants.get_info_constants() == (
        {name: spec[0] for name, spec in ODBC_INFO.items()} | LEGACY_GETINFO_CONSTANTS
    )
    assert set(_GETINFO_RETURN_TYPES) == {spec[0] for spec in ALL_ODBC_INFO.values()}
    assert {name: member.name for name, member in G.__members__.items() if name != member.name} == {
        "SQL_TIMEDATE_FUNCTIONS": "SQL_DATETIME_FUNCTIONS",
        "SQL_ACTIVE_CONNECTIONS": "SQL_MAX_DRIVER_CONNECTIONS",
        "SQL_ACTIVE_STATEMENTS": "SQL_MAX_CONCURRENT_ACTIVITIES",
        "SQL_OWNER_USAGE": "SQL_SCHEMA_USAGE",
        "SQL_QUALIFIER_USAGE": "SQL_CATALOG_USAGE",
        "SQL_TXN_ISOLATION_LEVEL": "SQL_MAX_CHAR_LITERAL_LEN",
        "SQL_CONCURRENCY": "SQL_DRIVER_VER",
        "SQL_ROW_NUMBER": "SQL_SEARCH_PATTERN_ESCAPE",
        "SQL_IC_UPPER": "SQL_MAX_CONCURRENT_ACTIVITIES",
        "SQL_IC_LOWER": "SQL_DATA_SOURCE_NAME",
        "SQL_IC_SENSITIVE": "SQL_DRIVER_HDBC",
        "SQL_IC_MIXED": "SQL_DRIVER_HENV",
    }


@pytest.mark.parametrize("name", ALL_ODBC_INFO)
def test_getinfo_ids_and_return_types_match_odbc(name):
    info_id, size = ALL_ODBC_INFO[name]
    if name in ODBC_INFO:
        assert G.__members__[name].value == info_id
    return_type = _GETINFO_RETURN_TYPES[info_id]
    if size:
        assert isinstance(return_type, struct.Struct)
        assert return_type.size == size
        assert return_type.format == (
            "P" if name in DRIVER_MANAGER_INFO else {2: "=H", 4: "=I"}[size]
        )
    else:
        assert return_type is str


def test_getinfo_type_registry_is_immutable():
    with pytest.raises(TypeError):
        _GETINFO_RETURN_TYPES[118] = str


def test_getinfo_public_exports_and_stubs():
    expected = {name: spec[0] for name, spec in ODBC_INFO.items()} | NON_INFO_CONSTANTS
    stub = Path(mssql_python.__file__).with_name("mssql_python.pyi")
    declarations = {
        node.target.id: node.annotation.id
        for node in ast.parse(stub.read_text(encoding="utf-8")).body
        if isinstance(node, ast.AnnAssign)
        and isinstance(node.target, ast.Name)
        and isinstance(node.annotation, ast.Name)
    }
    for name, value in expected.items():
        assert getattr(constants, name) == value
        assert getattr(mssql_python, name) == value
        assert constants.__all__.count(name) == 1
        assert mssql_python.__all__.count(name) == 1
    changed_info_names = {
        "SQL_DRIVER_HDBC",
        "SQL_DRIVER_HENV",
        "SQL_CATALOG_NAME",
        "SQL_DESCRIBE_PARAMETER",
        "SQL_DATETIME_FUNCTIONS",
        "SQL_TIMEDATE_FUNCTIONS",
        "SQL_SYSTEM_FUNCTIONS",
        "SQL_KEYSET_CURSOR_ATTRIBUTES1",
        "SQL_KEYSET_CURSOR_ATTRIBUTES2",
        "SQL_STATIC_CURSOR_ATTRIBUTES1",
        "SQL_STATIC_CURSOR_ATTRIBUTES2",
        "SQL_OJ_CAPABILITIES",
    }
    for name in changed_info_names | NON_INFO_CONSTANTS.keys():
        assert declarations[name] == "int"


@pytest.mark.parametrize("name,value", CONFORMANCE_VALUES.items())
def test_conformance_values_are_not_advertised_as_information_types(name, value):
    assert name not in G.__members__
    assert name not in constants.get_info_constants()
    assert ConstantsDDBC.__members__[name].value == value


@pytest.mark.parametrize("name,value", LEGACY_GETINFO_CONSTANTS.items())
def test_getinfo_legacy_attributes_and_imports_preserve_original_values(name, value):
    assert getattr(G, name).value == value
    assert G[name].value == value
    assert constants.get_info_constants()[name] == value
    assert getattr(ConstantsDDBC, name).value == value
    assert getattr(constants, name) == value
    assert getattr(mssql_python, name) == value


def test_getinfo_datetime_alias_preserves_existing_canonical_name():
    assert G.SQL_DATETIME_FUNCTIONS.name == "SQL_DATETIME_FUNCTIONS"
    assert G.SQL_TIMEDATE_FUNCTIONS is G.SQL_DATETIME_FUNCTIONS
    assert G.SQL_DATETIME_FUNCTIONS.value == 52


def test_getinfo_legacy_name_does_not_change_colliding_information_type(mock_connection):
    mock_connection._conn.get_info.return_value = {"data": b"\\\x00", "length": 2}
    assert Connection.getinfo(mock_connection, G.SQL_ROW_NUMBER.value) == "\\"
    assert Connection.getinfo(mock_connection, G.SQL_SEARCH_PATTERN_ESCAPE.value) == "\\"
    assert mock_connection._conn.get_info.call_count == 2
    mock_connection._conn.get_info.assert_called_with(14)


@pytest.mark.parametrize("name", NUMERIC_INFO)
@pytest.mark.parametrize("case", ["zero", "one", "ascii", "high_bit", "maximum"])
def test_getinfo_unsigned_numeric_values_and_forwarded_ids(mock_connection, name, case):
    info_id, size = NUMERIC_INFO[name]
    value = {
        "zero": 0,
        "one": 1,
        "ascii": 65,
        "high_bit": 1 << (size * 8 - 1),
        "maximum": (1 << (size * 8)) - 1,
    }[case]
    mock_connection._conn.get_info.return_value = {
        "data": value.to_bytes(size, sys.byteorder) + b"ignored padding",
        "length": size,
    }
    request = G.__members__[name].value if name in ODBC_INFO else info_id
    result = Connection.getinfo(mock_connection, request)
    assert type(result) is int
    assert result == value
    mock_connection._conn.get_info.assert_called_once_with(info_id)


@pytest.mark.parametrize("name", STRING_INFO)
@pytest.mark.parametrize("value", ["", "Y", "N", "F", "catalog_\u03a9_\U0001f600"])
def test_getinfo_character_values_are_preserved(mock_connection, name, value):
    info_id, _ = STRING_INFO[name]
    data = value.encode("utf-16-le")
    mock_connection._conn.get_info.return_value = {
        "data": data + "\0ignored padding".encode("utf-16-le"),
        "length": len(data),
    }
    request = G.__members__[name].value if name in ODBC_INFO else info_id
    result = Connection.getinfo(mock_connection, request)
    assert type(result) is str
    assert result == value
    mock_connection._conn.get_info.assert_called_once_with(info_id)


@pytest.mark.parametrize(
    "info_id,data,length",
    [
        (118, b"", 0),
        (118, b"\x01", 2),
        (118, b"\x01\x00\x00", 4),
        (118, b"\x01\x00\x00", 3),
        (118, b"\x01\x00\x00\x00\x00", 5),
        (118, b"\x01\x00\x00\x00", -1),
        (118, b"\x01\x00\x00\x00", 4.0),
        (118, b"\x01\x00\x00\x00", "4"),
        (118, b"\x01\x00\x00\x00", None),
        (118, b"\x01\x00\x00\x00", True),
    ],
)
def test_getinfo_rejects_malformed_numeric_data(mock_connection, info_id, data, length):
    mock_connection._conn.get_info.return_value = {"data": data, "length": length}
    with pytest.raises(DatabaseError, match="Invalid numeric result length"):
        Connection.getinfo(mock_connection, info_id)


@pytest.mark.parametrize("info_id", [9, 75, 58, 65, 127, 128, 129, 148, 149])
def test_getinfo_old_colliding_ids_are_still_forwarded(mock_connection, info_id):
    mock_connection._conn.get_info.return_value = 1
    result = Connection.getinfo(mock_connection, info_id)
    assert type(result) is int
    assert result == 1
    mock_connection._conn.get_info.assert_called_once_with(info_id)


@pytest.mark.parametrize(
    "info_id,error",
    [
        (118, RuntimeError("SQLSTATE:HY096:Invalid information type")),
        (65536, TypeError("Information type out of range")),
        (65536, OverflowError("Information type out of range")),
    ],
)
def test_getinfo_unsupported_native_requests_keep_returning_none(mock_connection, info_id, error):
    mock_connection._conn.get_info.side_effect = error
    assert Connection.getinfo(mock_connection, info_id) is None
    mock_connection._conn.get_info.assert_called_once_with(info_id)


@pytest.mark.parametrize("value", ["invalid", None, G.SQL_SQL_CONFORMANCE, 1.5])
def test_getinfo_non_integer_input_is_rejected(mock_connection, value):
    with pytest.raises(ValueError, match="info_type must be an integer"):
        Connection.getinfo(mock_connection, value)
    mock_connection._conn.get_info.assert_not_called()


def test_getinfo_closed_and_negative_requests(mock_connection):
    assert Connection.getinfo(mock_connection, -1) is None
    mock_connection._closed = True
    with pytest.raises(InterfaceError):
        Connection.getinfo(mock_connection, 118)
    mock_connection._conn.get_info.assert_not_called()


@pytest.mark.parametrize("info_type", [-1, -65536])
def test_getinfo_negative_id_logs_without_formatting_failure(
    mock_connection, monkeypatch, capsys, info_type
):
    log_sink = Mock()
    monkeypatch.setattr("mssql_python.connection.logger._logger", log_sink)
    monkeypatch.setattr("mssql_python.connection.logger._cached_level", logging.DEBUG)

    assert Connection.getinfo(mock_connection, info_type) is None
    mock_connection._conn.get_info.assert_not_called()
    log_sink.log.assert_called_once_with(
        logging.DEBUG,
        f"[Python] Invalid info_type: {info_type}. Must be non-negative.",
        stacklevel=3,
    )
    assert capsys.readouterr().err == ""


@pytest.mark.parametrize("result", [None, 1, "Y", True])
def test_getinfo_already_decoded_native_results(mock_connection, result):
    mock_connection._conn.get_info.return_value = result
    assert Connection.getinfo(mock_connection, 118) is result


def test_getinfo_unknown_driver_specific_type_keeps_legacy_handling(mock_connection):
    mock_connection._conn.get_info.return_value = {"data": b"vendor", "length": 6}
    assert Connection.getinfo(mock_connection, 999) == "vendor"


@pytest.mark.parametrize("value", ["Example Driver", "\u00e9", "", "\u03a9_\U0001f600"])
def test_getinfo_unlisted_high_ids_keep_unicode_decoding(mock_connection, value):
    data = value.encode("utf-16-le")
    mock_connection._conn.get_info.return_value = {"data": data, "length": len(data)}
    result = Connection.getinfo(mock_connection, 65000)
    assert type(result) is str
    assert result == value
    mock_connection._conn.get_info.assert_called_once_with(65000)


@pytest.mark.parametrize(
    "name",
    [
        (
            pytest.param(
                name,
                marks=pytest.mark.skip(
                    reason=f"{name} requires a Driver Manager; native providers are loaded directly"
                ),
            )
            if name in DRIVER_MANAGER_INFO
            else name
        )
        for name in ALL_ODBC_INFO
    ],
)
def test_getinfo_matches_native_odbc_payload(db_connection, name):
    info_id, size = ALL_ODBC_INFO[name]
    raw = db_connection._conn.get_info(info_id)
    assert isinstance(raw, dict)
    assert raw["info_type"] == info_id
    data = raw["data"][: raw["length"]]
    if size:
        assert raw["length"] == size
        expected = int.from_bytes(data, sys.byteorder, signed=False)
        expected_type = int
    else:
        expected = data.decode("utf-16-le").rstrip("\0")
        expected_type = str
    request = G.__members__[name].value if name in ODBC_INFO else info_id
    result = db_connection.getinfo(request)
    assert type(result) is expected_type
    assert result == expected


def test_getinfo_distinguishes_swapped_ids_even_when_driver_values_match(mock_connection):
    # SQL Server commonly answers "Y" to both, hiding the swapped constants in
    # output-only integration tests.
    payloads = {10002: "N", 10003: "Y"}
    mock_connection._conn.get_info.side_effect = lambda info_id: {
        "data": payloads[info_id].encode("utf-16-le"),
        "length": 2,
    }
    assert Connection.getinfo(mock_connection, G.SQL_CATALOG_NAME.value) == "Y"
    assert Connection.getinfo(mock_connection, G.SQL_DESCRIBE_PARAMETER.value) == "N"


@pytest.mark.parametrize(
    "name,length",
    [
        (name, length)
        for name, (_, size) in NUMERIC_INFO.items()
        for length in (1, 2, 4, 8)
        if length != size
    ],
)
def test_getinfo_rejects_type_specific_wrong_widths(mock_connection, name, length):
    info_id, _ = NUMERIC_INFO[name]
    mock_connection._conn.get_info.return_value = {"data": b"\xff" * length, "length": length}
    with pytest.raises(DatabaseError, match="Invalid numeric result length"):
        Connection.getinfo(mock_connection, info_id)
    mock_connection._conn.get_info.assert_called_once_with(info_id)


@pytest.mark.parametrize(
    "data",
    [1, -1, True, False, 1.9, float("inf"), Decimal("1.9"), "\u00b2", "", "1.9", "text", None],
)
def test_getinfo_nonbyte_numeric_values_are_not_lossily_coerced(mock_connection, data):
    mock_connection._conn.get_info.return_value = {"data": data, "length": 4}
    assert Connection.getinfo(mock_connection, 118) is data


@pytest.mark.parametrize(
    "data,expected", [("0", 0), ("000123", 123), ("123", 123), ("\u0661\u0662", 12)]
)
def test_getinfo_decimal_strings_keep_integer_compatibility(mock_connection, data, expected):
    mock_connection._conn.get_info.return_value = {"data": data, "length": len(data)}
    result = Connection.getinfo(mock_connection, 118)
    assert type(result) is int
    assert result == expected


def test_getinfo_oversized_decimal_string_keeps_compatibility(mock_connection):
    limit = getattr(sys, "get_int_max_str_digits", lambda: 0)()
    if not limit:
        pytest.skip("Interpreter integer-string conversion limit is disabled or unavailable")
    data = "1" * (limit + 1)
    mock_connection._conn.get_info.return_value = {"data": data, "length": len(data)}
    assert Connection.getinfo(mock_connection, 118) is data


@pytest.mark.parametrize("sqlstate", ["HY096", "HYC00", "08S01", "08003", "HYT00", "HYT01"])
def test_getinfo_native_failures_keep_logged_none_contract(mock_connection, monkeypatch, sqlstate):
    warning = Mock()
    monkeypatch.setattr("mssql_python.connection.logger.warning", warning)
    mock_connection._conn.get_info.side_effect = RuntimeError(f"SQLSTATE:{sqlstate}:Native failure")
    assert Connection.getinfo(mock_connection, 118) is None
    mock_connection._conn.get_info.assert_called_once_with(118)
    warning.assert_called_once()
    assert sqlstate in warning.call_args.args[0]


@pytest.mark.parametrize("info_id", [6, 999])
@pytest.mark.parametrize("data", ["metadata", 1, 1.9, True, None, {"value": 1}])
def test_getinfo_nonbyte_text_and_unknown_values_are_unchanged(mock_connection, info_id, data):
    mock_connection._conn.get_info.return_value = {"data": data, "length": 4}
    assert Connection.getinfo(mock_connection, info_id) is data


@pytest.mark.parametrize("data,expected", [(b"\xff\xff", -1), (b"\xff" * 9, b"\xff" * 9)])
def test_getinfo_unknown_binary_fallback_is_preserved(mock_connection, data, expected):
    mock_connection._conn.get_info.return_value = {"data": data, "length": len(data)}
    result = Connection.getinfo(mock_connection, 999)
    assert type(result) is type(expected)
    assert result == expected


@pytest.mark.parametrize("result", [1.9, b"raw", [], {}, {"length": 4}])
def test_getinfo_unrecognized_native_result_is_unchanged(mock_connection, result):
    mock_connection._conn.get_info.return_value = result
    assert Connection.getinfo(mock_connection, 118) is result


def test_getinfo_missing_native_length_is_not_silently_defaulted(mock_connection):
    mock_connection._conn.get_info.return_value = {"data": b"\x01\x00\x00\x00"}
    with pytest.raises(KeyError, match="length"):
        Connection.getinfo(mock_connection, 118)


def test_getinfo_does_not_cache_metadata_across_connections(mock_connection):
    other = SimpleNamespace(_closed=False, _conn=Mock())
    mock_connection._conn.get_info.return_value = {
        "data": (1).to_bytes(4, sys.byteorder),
        "length": 4,
    }
    other._conn.get_info.return_value = {"data": (2).to_bytes(4, sys.byteorder), "length": 4}
    assert Connection.getinfo(mock_connection, 118) == 1
    assert Connection.getinfo(other, 118) == 2
    mock_connection._conn.get_info.return_value = {
        "data": (4).to_bytes(4, sys.byteorder),
        "length": 4,
    }
    assert Connection.getinfo(mock_connection, 118) == 4
    assert mock_connection._conn.get_info.call_count == 2
    other._conn.get_info.assert_called_once_with(118)


@pytest.mark.parametrize("name", list(ODBC_INFO) + list(LEGACY_GETINFO_CONSTANTS))
def test_getinfo_current_enum_pickle_round_trip(name):
    member = G.__members__[name]
    assert pickle.loads(pickle.dumps(member)) is member


@pytest.mark.parametrize("name,value", LEGACY_GETINFO_CONSTANTS.items())
def test_getinfo_legacy_attribute_pickles_preserve_values(monkeypatch, name, value):
    legacy = Enum("GetInfoConstants", {name: value}, module=constants.__name__)
    with monkeypatch.context() as patch:
        patch.setattr(constants, "GetInfoConstants", legacy)
        serialized = pickle.dumps(legacy[name])

    restored = pickle.loads(serialized)
    assert restored is getattr(G, name)
    assert restored.value == value


def test_getinfo_legacy_pickle_values_cannot_identify_the_original_name(monkeypatch):
    legacy = Enum(
        "GetInfoConstants", {"SQL_STATIC_CURSOR_ATTRIBUTES1": 150}, module=constants.__name__
    )
    with monkeypatch.context() as patch:
        patch.setattr(constants, "GetInfoConstants", legacy)
        serialized = pickle.dumps(legacy.SQL_STATIC_CURSOR_ATTRIBUTES1)

    # Old enum pickles store 150, not the name; after correction 150 means keyset.
    assert pickle.loads(serialized) is G.SQL_KEYSET_CURSOR_ATTRIBUTES1
    assert G["SQL_STATIC_CURSOR_ATTRIBUTES1"].value == 167
