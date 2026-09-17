# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Bounded text payload fidelity, including row-wise routing beside a MAX column.

The MAX value here is only a routing control. Actual MAX text BOM/NUL fidelity
belongs to the separate LOB decoder and is not covered by this regression.
"""

import pytest

from mssql_python import SQL_CHAR, SQL_WCHAR


@pytest.fixture(scope="module")
def utf8_collation(db_connection):
    with db_connection.cursor() as cursor:
        cursor.execute(
            "SELECT name FROM sys.fn_helpcollations() "
            "WHERE name = 'Latin1_General_100_BIN2_UTF8'"
        )
        row = cursor.fetchone()
    if row is None:
        pytest.skip("VARCHAR BOM payloads require SQL Server UTF-8 collation support")
    return row[0]


def _fetch_rows(cursor, method):
    if method == "fetchall":
        rows = cursor.fetchall()
    elif method == "fetchmany":
        rows = []
        while batch := cursor.fetchmany(2):
            rows.extend(batch)
    else:
        rows = []
        while (row := cursor.fetchone()) is not None:
            rows.append(row)
    assert cursor.fetchone() is None
    return [tuple(row) for row in rows]


def _assert_payloads(connection, expressions, expected, method, forced_max, storage_encoding):
    values = ", ".join(f"({index}, {expression})" for index, expression in enumerate(expressions))
    relation = f"FROM (VALUES {values}) AS p(row_id, payload)"
    with connection.cursor() as cursor:
        # Fetch SQL evidence separately so text conversion cannot mask stored data.
        cursor.execute(
            "SELECT row_id, UNICODE(payload), DATALENGTH(payload), "
            f"CAST(payload AS varbinary(128)) {relation} ORDER BY row_id"
        )
        evidence = [tuple(row) for row in cursor.fetchall()]
        wanted_evidence = []
        for index, payload in enumerate(expected):
            raw = None if payload is None else payload.encode(storage_encoding)
            wanted_evidence.append(
                (
                    index,
                    ord(payload[0]) if payload else None,
                    None if raw is None else len(raw),
                    raw,
                )
            )
        assert evidence == wanted_evidence

        extra = ", CAST(N'route' AS nvarchar(max)) AS force_max" if forced_max else ""
        cursor.execute(
            f"SELECT r.repeat_id, p.row_id, p.payload {extra} {relation} "
            "CROSS JOIN (VALUES (1), (2)) AS r(repeat_id) ORDER BY r.repeat_id, p.row_id"
        )
        wanted = [
            (repeat_id, index, payload) + (("route",) if forced_max else ())
            for repeat_id in (1, 2)
            for index, payload in enumerate(expected)
        ]
        actual = _fetch_rows(cursor, method)
        assert actual == wanted
        assert all(row[2] is None or type(row[2]) is str for row in actual)


@pytest.mark.parametrize("prefix", [0xFEFF, 0xFFFE], ids=["feff", "fffe"])
@pytest.mark.parametrize("method", ["fetchone", "fetchmany", "fetchall"])
@pytest.mark.parametrize("forced_max", [False, True], ids=["bounded", "beside-max"])
def test_bounded_nvarchar_bom_payload(db_connection, prefix, method, forced_max):
    _assert_payloads(
        db_connection,
        [f"CAST(NCHAR({prefix}) + N'BOM' AS nvarchar(64))"],
        [chr(prefix) + "BOM"],
        method,
        forced_max,
        "utf-16le",
    )


@pytest.mark.parametrize("prefix", [0xFEFF, 0xFFFE], ids=["feff", "fffe"])
@pytest.mark.parametrize("method", ["fetchone", "fetchmany", "fetchall"])
@pytest.mark.parametrize("forced_max", [False, True], ids=["bounded", "beside-max"])
def test_bounded_varchar_bom_payload(db_connection, utf8_collation, prefix, method, forced_max):
    assert db_connection.getdecoding(SQL_CHAR)["ctype"] == SQL_WCHAR
    _assert_payloads(
        db_connection,
        [f"CAST((NCHAR({prefix}) + N'BOM') COLLATE {utf8_collation} AS varchar(64))"],
        [chr(prefix) + "BOM"],
        method,
        forced_max,
        "utf-8",
    )


@pytest.mark.parametrize("method", ["fetchone", "fetchmany", "fetchall"])
@pytest.mark.parametrize("forced_max", [False, True], ids=["bounded", "beside-max"])
def test_bounded_nvarchar_unicode_and_lengths(db_connection, method, forced_max):
    expected = [
        None,
        "",
        "plain ASCII",
        "caf\u00e9 \u4e2d\u6587",
        "A\U0001f642Z",
        "A\0B",
        "A\0",
        "\0",
        "A\ufeff\ufffeZ",
        "x" * 62 + "\U0001f642",
    ]
    expressions = [
        (
            "CAST(NULL AS nvarchar(64))"
            if value is None
            else f"CAST(0x{value.encode('utf-16le').hex()} AS nvarchar(64))"
        )
        for value in expected
    ]
    _assert_payloads(db_connection, expressions, expected, method, forced_max, "utf-16le")


@pytest.mark.parametrize(
    "encoding, ctype", [("utf-16le", SQL_WCHAR), ("latin-1", SQL_CHAR)], ids=["wide", "narrow"]
)
@pytest.mark.parametrize("method", ["fetchone", "fetchmany", "fetchall"])
@pytest.mark.parametrize("forced_max", [False, True], ids=["bounded", "beside-max"])
def test_bounded_varchar_decoding_controls(db_connection, encoding, ctype, method, forced_max):
    expected = [None, "", "ASCII", "caf\u00e9", "A\0B", "A\0", "\0", "x" * 64]
    expressions = [
        (
            "CAST(NULL AS varchar(64))"
            if value is None
            else (
                f"CAST(CAST(0x{value.encode('utf-16le').hex()} AS nvarchar(64)) "
                "COLLATE Latin1_General_100_BIN2 AS varchar(64))"
            )
        )
        for value in expected
    ]
    original = db_connection.getdecoding(SQL_CHAR)
    try:
        db_connection.setdecoding(SQL_CHAR, encoding=encoding, ctype=ctype)
        _assert_payloads(db_connection, expressions, expected, method, forced_max, "latin-1")
    finally:
        db_connection.setdecoding(SQL_CHAR, encoding=original["encoding"], ctype=original["ctype"])


@pytest.mark.parametrize("raw", ["00D8", "00DC"], ids=["unpaired-high", "unpaired-low"])
@pytest.mark.parametrize(
    "method, forced_max",
    [("fetchone", False), ("fetchone", True), ("fetchmany", True), ("fetchall", True)],
    ids=["bounded-fetchone", "beside-max-fetchone", "beside-max-fetchmany", "beside-max-fetchall"],
)
def test_bounded_nvarchar_strict_decode_error(db_connection, raw, method, forced_max):
    extra = ", CAST(N'route' AS nvarchar(max)) AS force_max" if forced_max else ""
    with db_connection.cursor() as cursor:
        cursor.execute(f"SELECT CAST(0x{raw} AS nvarchar(64)) {extra}")
        with pytest.raises(UnicodeDecodeError):
            _fetch_rows(cursor, method)
        cursor.execute("SELECT CAST(N'recovered' AS nvarchar(64))")
        assert cursor.fetchone()[0] == "recovered"
