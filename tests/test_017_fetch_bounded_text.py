# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Bounded text payload fidelity, including row-wise routing beside a MAX column.

The MAX value here is only a routing control. Actual MAX text BOM/NUL fidelity
belongs to the separate LOB decoder and is not covered by this regression.
"""

import sys

import pytest

from mssql_python import SQL_CHAR, SQL_WCHAR


@pytest.mark.parametrize("dense_first", [False, True])
@pytest.mark.parametrize("batch_size", [None, 7, 9, 65], ids=["all", "many7", "many9", "many65"])
def test_reusable_rowsets_preserve_density_transitions(db_connection, dense_first, batch_size):
    full = "x" * 3998 + "\U0001f642"
    expression = (
        f"CAST(CASE WHEN {'n < 16' if dense_first else 'n >= 16'} THEN ? "
        "WHEN n % 4 = 0 THEN NULL WHEN n % 4 = 1 THEN N'' "
        "WHEN n % 4 = 2 THEN N'A' + NCHAR(0) + N'B' "
        "ELSE CAST(0x3DD842DE AS nvarchar(2)) END AS nvarchar(4000))"
    )
    sql = (
        "WITH d(n) AS (SELECT n FROM (VALUES (0),(1),(2),(3),(4),"
        "(5),(6),(7),(8),(9)) AS v(n)), "
        "numbers AS (SELECT a.n + 10*b.n + 100*c.n AS n "
        "FROM d a CROSS JOIN d b CROSS JOIN d c) "
        f"SELECT n, {', '.join([expression] * 8)} FROM numbers WHERE n < 137 ORDER BY n"
    )
    with db_connection.cursor() as cursor:
        cursor.execute(sql, (full,) * 8)
        if batch_size is None:
            rows = cursor.fetchall()
        else:
            rows = []
            while batch := cursor.fetchmany(batch_size):
                assert len(batch) <= batch_size
                rows.extend(batch)
        expected = []
        for n in range(137):
            dense = n < 16 if dense_first else n >= 16
            value = full if dense else (None, "", "A\0B", "\U0001f642")[n % 4]
            expected.append((n,) + (value,) * 8)
        assert [tuple(row) for row in rows] == expected
        assert cursor.fetchmany(9) == []
        assert cursor.fetchone() is None


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


def test_reusable_rowset_mixed_native_types_and_nulls(db_connection):
    from datetime import date, datetime, time
    from decimal import Decimal
    from uuid import UUID

    columns = [
        ("N'wide'", "nvarchar(4000)", "wide"),
        ("1", "int", 1),
        ("2", "smallint", 2),
        ("3", "bigint", 3),
        ("4", "tinyint", 4),
        ("1", "bit", True),
        ("1.5", "real", 1.5),
        ("2.5", "float", 2.5),
        ("12.50", "decimal(10,2)", Decimal("12.50")),
        ("0x0001", "varbinary(2)", b"\0\x01"),
        ("'2026-01-02'", "date", date(2026, 1, 2)),
        ("'03:04:05.123456'", "time(6)", time(3, 4, 5, 123456)),
        (
            "'2026-01-02T03:04:05.123456'",
            "datetime2(6)",
            datetime(2026, 1, 2, 3, 4, 5, 123456),
        ),
        (
            "'2026-01-02T03:04:05.123456+05:30'",
            "datetimeoffset(6)",
            datetime.fromisoformat("2026-01-02T03:04:05.123456+05:30"),
        ),
        (
            "'00112233-4455-6677-8899-aabbccddeeff'",
            "uniqueidentifier",
            UUID("00112233-4455-6677-8899-aabbccddeeff"),
        ),
    ]
    expressions = [
        f"CAST(CASE WHEN n = 0 THEN {literal} END AS {sqltype})" for literal, sqltype, _ in columns
    ]
    with db_connection.cursor() as cursor:
        cursor.execute(f"SELECT {', '.join(expressions)} FROM (VALUES (0),(1)) AS v(n) ORDER BY n")
        assert [tuple(row) for row in cursor.fetchall()] == [
            tuple(value for _, _, value in columns),
            (None,) * len(columns),
        ]


def _fetch_rows(cursor, method):
    if method == "fetchall":
        rows = cursor.fetchall()
    elif method.startswith("fetchmany"):
        rows = []
        while True:
            batch = (
                cursor.fetchmany()
                if method == "fetchmany_default"
                else cursor.fetchmany(1 if method == "fetchmany_one" else 2)
            )
            if not batch:
                break
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
    [
        ("fetchone", False),
        ("fetchone", True),
        ("fetchmany_one", True),
        ("fetchmany_default", True),
        ("fetchmany", True),
        ("fetchall", True),
    ],
    ids=[
        "bounded-fetchone",
        "beside-max-fetchone",
        "beside-max-explicit",
        "beside-max-default",
        "beside-max-fetchmany",
        "beside-max-fetchall",
    ],
)
def test_bounded_nvarchar_strict_decode_error(db_connection, raw, method, forced_max):
    extra = ", CAST(N'route' AS nvarchar(max)) AS force_max" if forced_max else ""
    with db_connection.cursor() as cursor:
        cursor.execute(
            f"SELECT CASE WHEN n = 1 THEN CAST(0x{raw} AS nvarchar(64)) ELSE N'after' END {extra} "
            "FROM (VALUES (1), (2)) AS v(n) ORDER BY n"
        )
        position = (cursor.rowcount, cursor.rownumber, cursor._next_row_index)
        with pytest.raises(UnicodeDecodeError):
            _fetch_rows(cursor, method)
        assert (cursor.rowcount, cursor.rownumber, cursor._next_row_index) == position
        assert cursor.fetchone()[0] == "after"
        assert (cursor.rowcount, cursor.rownumber, cursor._next_row_index) == (1, 0, 1)
        assert cursor.fetchone() is None
        cursor.execute("SELECT CAST(N'recovered' AS nvarchar(64))")
        assert cursor.fetchone()[0] == "recovered"


@pytest.mark.parametrize("raw", ["00D8", "00DC"], ids=["unpaired-high", "unpaired-low"])
@pytest.mark.parametrize(
    "method, warmed",
    [
        ("fetchmany_one", False),
        ("fetchmany_one", True),
        ("fetchmany_default", False),
        ("fetchmany_default", True),
        ("fetchmany", False),
        ("fetchall", False),
    ],
    ids=["explicit-cold", "explicit-warm", "default-cold", "default-warm", "batch", "all"],
)
@pytest.mark.parametrize("width", [64, 4000])
def test_bounded_nvarchar_batch_malformed_fallback(db_connection, raw, method, warmed, width):
    """Preserve the existing platform-specific batch behavior for unpaired surrogates."""
    expression = f"CAST(0x{raw} AS nvarchar({width}))"
    raw_bytes = bytes.fromhex(raw)
    with db_connection.cursor() as cursor:
        cursor.execute(f"SELECT DATALENGTH({expression}), CAST({expression} AS varbinary(64))")
        assert tuple(cursor.fetchone()) == (len(raw_bytes), raw_bytes)
        cursor.execute(
            f"SELECT n, CASE WHEN n IN (1, 3) THEN {expression} ELSE N'valid' END AS payload "
            "FROM (VALUES (-1), (0), (1), (2), (3), (4)) AS v(n) "
            f"WHERE n >= {-1 if warmed else 1} ORDER BY n"
        )
        expected = (
            raw_bytes.decode("utf-16le", errors="surrogatepass") if sys.platform == "win32" else ""
        )
        assert cursor.arraysize == 1
        if warmed:
            for n in (-1, 0):
                batch = cursor.fetchmany(1) if method == "fetchmany_one" else cursor.fetchmany()
                assert [tuple(row) for row in batch] == [(n, "valid")]
        offset = 2 if warmed else 0
        rows = []
        while True:
            if method == "fetchmany_one":
                batch = cursor.fetchmany(1)
            elif method == "fetchmany_default":
                batch = cursor.fetchmany()
            elif method == "fetchmany":
                batch = cursor.fetchmany(2)
            else:
                batch = cursor.fetchall()
            assert type(batch) is list
            rows.extend(tuple(row) for row in batch)
            assert cursor.rowcount == offset + len(rows)
            assert cursor.rownumber == offset + len(rows) - 1
            assert cursor._next_row_index == offset + len(rows)
            if not batch:
                break
        assert rows == [(1, expected), (2, "valid"), (3, expected), (4, "valid")]
        assert cursor.fetchone() is None
        cursor.execute("SELECT CAST(N'recovered' AS nvarchar(64))")
        assert cursor.fetchone()[0] == "recovered"


@pytest.mark.parametrize("method", ["fetchall", "fetchmany", "interleave"])
@pytest.mark.parametrize("converted", [False, True])
def test_wide_bounded_text_actual_lengths(db_connection, method, converted):
    payloads = [None, "", "\ufeffA\0B\ufffe", "A\U0001f642Z", "x" * 3998 + "\U0001f642"]
    cases = " ".join(
        f"WHEN {i} THEN "
        + (
            "NULL"
            if value is None
            else f"CAST(0x{value.encode('utf-16le').hex()} AS nvarchar(4000))"
        )
        for i, value in enumerate(payloads)
    )
    observed = []
    if converted:
        db_connection.add_output_converter(
            str, lambda raw: observed.append(raw) or raw.decode("utf-16le") + "!"
        )
        db_connection.add_output_converter(int, lambda value: value + 10000)
    try:
        with db_connection.cursor() as cursor:
            cursor.execute(
                "WITH d(n) AS (SELECT n FROM (VALUES (0),(1),(2),(3),(4),"
                "(5),(6),(7),(8),(9)) AS v(n)) "
                f"SELECT n, CAST(CASE n % 5 {cases} END AS nvarchar(4000)), "
                "CAST(NCHAR(233) COLLATE Latin1_General_100_BIN2 AS varchar(8000)), "
                "CAST(0x0001FF AS varbinary(8)), CAST(n AS decimal(10,2)) "
                "FROM (SELECT a.n+10*b.n+100*c.n AS n FROM d a CROSS JOIN d b CROSS JOIN d c) v "
                "WHERE n < 207 ORDER BY n"
            )
            actual = []
            if method == "interleave":
                actual.append(cursor.fetchone())
            if method == "fetchall":
                actual.extend(cursor.fetchall())
            else:
                while batch := cursor.fetchmany(7):
                    actual.extend(batch)
            assert [(row[0], row[1], row[2], row[3], int(row[4])) for row in actual] == [
                (
                    n + (10000 if converted else 0),
                    (
                        None
                        if payloads[n % 5] is None
                        else payloads[n % 5] + ("!" if converted else "")
                    ),
                    "\u00e9" + ("!" if converted else ""),
                    b"\0\x01\xff",
                    n,
                )
                for n in range(207)
            ]
            assert cursor.fetchone() is None
            assert cursor.fetchall() == []
            if converted:
                assert observed == [
                    value.encode("utf-16le")
                    for n in range(207)
                    for value in (payloads[n % 5], "\u00e9")
                    if value is not None
                ]
    finally:
        if converted:
            db_connection.remove_output_converter(str)
            db_connection.remove_output_converter(int)


@pytest.mark.parametrize("method", ["fetchall", "fetchmany"])
@pytest.mark.parametrize("sqltype", ["nvarchar(4000)", "varchar(8000)"])
@pytest.mark.parametrize("offset", [253, 254, 255, 256, 511])
def test_wide_text_continuation_boundaries(db_connection, utf8_collation, method, sqltype, offset):
    payload = "a" * offset + "\U0001f642" + "\0" + "\u00e9\u20ac" + "z" * 550
    expected = ["before", payload, "after"]
    values = ", ".join(
        f"({i}, CAST(CAST(0x{value.encode('utf-16le').hex()} AS nvarchar(4000)) "
        f"COLLATE {utf8_collation} AS {sqltype}))"
        for i, value in enumerate(expected)
    )
    with db_connection.cursor() as cursor:
        cursor.execute(f"SELECT n, payload FROM (VALUES {values}) v(n, payload) ORDER BY n")
        rows = cursor.fetchall() if method == "fetchall" else cursor.fetchmany(3)
        assert [tuple(row) for row in rows] == list(enumerate(expected))
        assert cursor.messages == []
        assert cursor.fetchone() is None
