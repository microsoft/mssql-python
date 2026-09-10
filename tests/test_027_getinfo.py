"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.
Regression coverage for SQLGetInfo IDs and ODBC return types (GH-769).
"""

import ast
from pathlib import Path
import struct
import sys
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import mssql_python
from mssql_python import constants
from mssql_python.connection import Connection, _GETINFO_NUMERIC_TYPES, _GETINFO_STRING_TYPES
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

NON_INFO_CONSTANTS = {
    "SQL_TXN_ISOLATION_LEVEL": 108,
    "SQL_CONCURRENCY": 7,
    "SQL_ROWSET_SIZE": 9,
    "SQL_ROW_NUMBER": 14,
    "SQL_IC_UPPER": 1,
    "SQL_IC_LOWER": 2,
    "SQL_IC_SENSITIVE": 3,
    "SQL_IC_MIXED": 4,
    "SQL_SQL92_ENTRY_SQL": 1,
    "SQL_SQL92_INTERMEDIATE_SQL": 4,
    "SQL_SQL92_FULL_SQL": 8,
    "SQL_SC_SQL92_ENTRY": 1,
    "SQL_SC_FIPS127_2_TRANSITIONAL": 2,
    "SQL_SC_SQL92_INTERMEDIATE": 4,
    "SQL_SC_SQL92_FULL": 8,
}

NUMERIC_INFO = {name: spec for name, spec in ODBC_INFO.items() if spec[1]}
STRING_INFO = {name: spec for name, spec in ODBC_INFO.items() if not spec[1]}
DRIVER_MANAGER_INFO = {"SQL_DRIVER_HDBC", "SQL_DRIVER_HENV", "SQL_DRIVER_HLIB"}


@pytest.fixture
def mock_connection():
    # Avoid constructing a native connection for the decoder and export tests.
    return SimpleNamespace(_closed=False, _conn=Mock())


def test_getinfo_reference_covers_every_member_and_alias():
    assert set(G.__members__) == set(ODBC_INFO)
    assert constants.get_info_constants() == {name: spec[0] for name, spec in ODBC_INFO.items()}
    assert {name: member.name for name, member in G.__members__.items() if name != member.name} == {
        "SQL_DATETIME_FUNCTIONS": "SQL_TIMEDATE_FUNCTIONS",
        "SQL_ACTIVE_CONNECTIONS": "SQL_MAX_DRIVER_CONNECTIONS",
        "SQL_ACTIVE_STATEMENTS": "SQL_MAX_CONCURRENT_ACTIVITIES",
        "SQL_OWNER_USAGE": "SQL_SCHEMA_USAGE",
        "SQL_QUALIFIER_USAGE": "SQL_CATALOG_USAGE",
    }


@pytest.mark.parametrize("name", ODBC_INFO)
def test_getinfo_ids_and_return_types_match_odbc(name):
    info_id, size = ODBC_INFO[name]
    assert G.__members__[name].value == info_id
    if size:
        assert info_id in _GETINFO_NUMERIC_TYPES
        assert info_id not in _GETINFO_STRING_TYPES
    else:
        assert info_id in _GETINFO_STRING_TYPES
        assert info_id not in _GETINFO_NUMERIC_TYPES


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
        assert name in constants.__all__
        assert name in mssql_python.__all__
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


@pytest.mark.parametrize("name,value", NON_INFO_CONSTANTS.items())
def test_non_info_constants_are_not_advertised_as_information_types(name, value):
    assert name not in G.__members__
    assert name not in constants.get_info_constants()
    assert ConstantsDDBC.__members__[name].value == value


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
    result = Connection.getinfo(mock_connection, G.__members__[name].value)
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
    result = Connection.getinfo(mock_connection, G.__members__[name].value)
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


@pytest.mark.parametrize("name", [name for name in ODBC_INFO if name not in DRIVER_MANAGER_INFO])
def test_getinfo_matches_native_odbc_payload(db_connection, name):
    info_id, size = ODBC_INFO[name]
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
    result = db_connection.getinfo(G.__members__[name].value)
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
