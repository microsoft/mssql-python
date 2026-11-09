"""
Re-executing a prepared statement keeps its parameter bindings when nothing changed.

Drivers keep their prepared handle (sp_execute) only while the parameter definitions in
the IPD are unchanged; SQL_RESET_PARAMS makes them re-prepare (sp_prepexec) on every
execute. The native execute path therefore rebinds over the previous execute's records
when every parameter definition matches and resets them otherwise. These tests cover the
cases where a stale binding could leak into the next execution: changed types and sizes,
NULLs resolved through SQLDescribeParam, changed SQL, parameter-free executes,
executemany, data-at-execution values, and failures part-way through binding. Uses the
project's fixtures from conftest.py.
"""

import datetime
import decimal
import threading
import time
import uuid
import xml.etree.ElementTree as ET

import pytest

from conftest import is_azure_sql_connection
from mssql_python import connect


def _roundtrip(cursor, sql, params):
    cursor.execute(sql, params)
    return cursor.fetchone()[0]


def test_reexecute_same_sql_with_new_values(cursor):
    sql = "SELECT CAST(? AS INT) + CAST(? AS INT)"
    for value in range(20):
        assert _roundtrip(cursor, sql, (value, 1)) == value + 1


# Each value changes at least one parameter definition (type, size, precision or
# data-at-execution) relative to its neighbours on the same statement.
CHANGING_VALUES = [
    1,
    300,
    70_000,
    5_000_000_000,
    None,
    "a",
    "abcdef",
    "x" * 5000,
    "y",
    None,
    1.5,
    decimal.Decimal("1.5"),
    decimal.Decimal("12345.6789"),
    b"\x00\x01",
    b"z" * 9000,
    bytes(3),
    True,
    datetime.date(2024, 2, 29),
    datetime.datetime(2024, 2, 29, 23, 59, 58, 123456),
    None,
    7,
]


def test_reexecute_same_sql_with_changing_types(cursor):
    for _ in range(2):
        for value in CHANGING_VALUES:
            assert _roundtrip(cursor, "SELECT ?", (value,)) == value


def test_reexecute_with_none_into_typed_columns(cursor, db_connection):
    # A permanent table: SQLDescribeParam cannot type parameters against #temp tables.
    table = f"dbo.pytest_param_rebind_nulls_{uuid.uuid4().hex}"
    cursor.execute(f"CREATE TABLE {table} (id INT, b VARBINARY(20), d DECIMAL(10, 2))")
    db_connection.commit()
    try:
        rows = [
            (1, None, None),
            (2, b"xy", decimal.Decimal("1.25")),
            (3, None, decimal.Decimal("2.50")),
            (4, None, None),
            (5, b"z", None),
            (6, b"z", None),
        ]
        for row in rows:
            cursor.execute(f"INSERT INTO {table} (id, b, d) VALUES (?, ?, ?)", row)
            assert cursor.rowcount == 1
        cursor.execute(f"SELECT id, b, d FROM {table} ORDER BY id")
        assert [tuple(r) for r in cursor.fetchall()] == rows
    finally:
        cursor.execute(f"DROP TABLE IF EXISTS {table}")
        db_connection.commit()


def test_changing_sql_and_parameter_count(cursor):
    for value in range(3):
        assert _roundtrip(cursor, "SELECT ? + ? + ?", (value, 1, 2)) == value + 3
        assert _roundtrip(cursor, "SELECT ?", (value,)) == value
        assert _roundtrip(cursor, "SELECT ? * 2", ("4",)) == 8


def test_parameter_free_execute_between_parameterized(cursor):
    sql = "SELECT CAST(? AS INT)"
    assert _roundtrip(cursor, sql, (1,)) == 1
    cursor.execute("SELECT 42")
    assert cursor.fetchone()[0] == 42
    assert _roundtrip(cursor, sql, (2,)) == 2
    assert _roundtrip(cursor, sql, (3,)) == 3


def test_executemany_interleaved_with_execute(cursor):
    cursor.execute("CREATE TABLE #rebind_many (id INT, label NVARCHAR(50))")
    try:
        insert = "INSERT INTO #rebind_many (id, label) VALUES (?, ?)"
        cursor.execute(insert, (1, "one"))
        cursor.executemany(insert, [(2, "two"), (3, None), (4, "four")])
        cursor.execute(insert, (5, "five"))
        cursor.execute(insert, (6, None))
        cursor.executemany(insert, [(7, "seven")])
        cursor.execute(insert, (8, "eight"))
        cursor.execute("SELECT id, label FROM #rebind_many ORDER BY id")
        assert [tuple(r) for r in cursor.fetchall()] == [
            (1, "one"),
            (2, "two"),
            (3, None),
            (4, "four"),
            (5, "five"),
            (6, None),
            (7, "seven"),
            (8, "eight"),
        ]
    finally:
        cursor.execute("DROP TABLE #rebind_many")


@pytest.mark.parametrize(
    "sql, large, small",
    [
        ("SELECT LEN(?)", "x" * 6000, "small"),
        ("SELECT LEN(?)", "ü" * 5000, "ü"),
        ("SELECT DATALENGTH(?)", b"\x01" * 10000, b"\x02\x03"),
    ],
    ids=["varchar", "nvarchar", "varbinary"],
)
def test_data_at_execution_values_alternate_with_inline(cursor, sql, large, small):
    for value in (large, large, small, small, large, small, large):
        assert _roundtrip(cursor, sql, (value,)) == len(value)


def test_reexecute_after_failure_part_way_through_binding(cursor):
    sql = "SELECT CAST(? AS INT), CAST(? AS DATE)"
    cursor.execute(sql, (1, datetime.date(2024, 1, 1)))
    assert tuple(cursor.fetchone()) == (1, datetime.date(2024, 1, 1))
    # The first parameter is bound before the second is rejected.
    with pytest.raises(Exception, match="Date out of range"):
        cursor.execute(sql, (2, datetime.date(1500, 1, 1)))
    cursor.execute(sql, (3, datetime.date(2024, 1, 3)))
    assert tuple(cursor.fetchone()) == (3, datetime.date(2024, 1, 3))
    cursor.execute(sql, (4, datetime.date(2024, 1, 4)))
    assert tuple(cursor.fetchone()) == (4, datetime.date(2024, 1, 4))


def test_reexecute_after_server_error(cursor):
    sql = "SELECT 10 / CAST(? AS INT)"
    assert _roundtrip(cursor, sql, (2,)) == 5
    with pytest.raises(Exception):
        _roundtrip(cursor, sql, (0,))
    assert _roundtrip(cursor, sql, (5,)) == 2
    assert _roundtrip(cursor, sql, (10,)) == 1


def test_bytearray_mutated_between_executes(cursor):
    value = bytearray(b"abc")
    assert _roundtrip(cursor, "SELECT ?", (value,)) == b"abc"
    value[0] = ord("z")
    assert _roundtrip(cursor, "SELECT ?", (value,)) == b"zbc"


def test_reexecution_on_separate_cursors_in_threads(conn_str):
    errors = []

    def worker(seed):
        try:
            with connect(conn_str) as conn:
                cur = conn.cursor()
                sql = "SELECT CAST(? AS INT) + CAST(? AS INT), ?"
                for i in range(100):
                    label = None if i % 7 == 0 else f"t{seed}-{i}"
                    cur.execute(sql, (i, seed, label))
                    assert tuple(cur.fetchone()) == (i + seed, label)
                cur.close()
        except Exception as exc:  # pylint: disable=broad-exception-caught
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(seed,)) for seed in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors


def _rpc_names(admin_cursor, conn_str, run, min_events):
    """Run ``run(cursor)`` on a fresh session and return the RPCs it sent, in order."""
    with connect(conn_str, autocommit=True) as conn:
        cur = conn.cursor()
        cur.execute("SELECT @@SPID")
        spid = cur.fetchone()[0]
        session = f"mssql_python_rebind_{uuid.uuid4().hex}"
        admin_cursor.execute(
            f"CREATE EVENT SESSION [{session}] ON SERVER ADD EVENT sqlserver.rpc_completed("
            f"WHERE sqlserver.session_id = {int(spid)}) ADD TARGET package0.ring_buffer "
            "WITH (MAX_DISPATCH_LATENCY = 1 SECONDS)"
        )
        try:
            admin_cursor.execute(f"ALTER EVENT SESSION [{session}] ON SERVER STATE = START")
            run(cur)
            target_xml, previous = None, -1
            for _ in range(30):
                time.sleep(0.5)
                admin_cursor.execute(
                    "SELECT CAST(t.target_data AS NVARCHAR(MAX)) FROM sys.dm_xe_sessions s "
                    "JOIN sys.dm_xe_session_targets t ON s.address = t.event_session_address "
                    "WHERE s.name = ?",
                    (session,),
                )
                target_xml = admin_cursor.fetchone()[0]
                count = int(ET.fromstring(target_xml).get("eventCount", "0"))
                if count >= min_events and count == previous:
                    break
                previous = count
        finally:
            admin_cursor.execute(f"DROP EVENT SESSION [{session}] ON SERVER")
        cur.close()
    return [
        event.find("data[@name='object_name']/value").text
        for event in ET.fromstring(target_xml).iter("event")
    ]


def _prepare_or_execute(rpcs):
    """Reduce RPCs to one entry per cursor.execute: 'prepare' or 'execute'.

    msodbcsql prepares a statement with data-at-execution parameters through
    sp_prepare followed by sp_execute; mssql-odbc uses sp_prepexec. Unprepare and
    NULL-type discovery RPCs are not executions.
    """
    ops = []
    rpcs = [r for r in rpcs if r not in ("sp_unprepare", "sp_describe_undeclared_parameters")]
    i = 0
    while i < len(rpcs):
        if rpcs[i] == "sp_prepare" and i + 1 < len(rpcs) and rpcs[i + 1] == "sp_execute":
            ops.append("prepare")
            i += 2
            continue
        ops.append({"sp_prepexec": "prepare", "sp_execute": "execute"}.get(rpcs[i], rpcs[i]))
        i += 1
    return ops


def test_repeat_execute_reuses_prepared_statement(conn_str):
    """Same SQL and parameter definitions: the driver executes the prepared handle."""
    if is_azure_sql_connection(conn_str):
        pytest.skip("Server-scoped Extended Events are not available on Azure SQL Database")
    # CREATE EVENT SESSION cannot run inside a user transaction.
    admin_conn = connect(conn_str, autocommit=True)
    admin = admin_conn.cursor()
    admin.execute("SELECT HAS_PERMS_BY_NAME(NULL, NULL, 'ALTER ANY EVENT SESSION')")
    if admin.fetchone()[0] != 1:
        admin_conn.close()
        pytest.skip("Requires ALTER ANY EVENT SESSION")

    sql = "SELECT CAST(? AS INT) + CAST(? AS INT)"
    lob_sql = "SELECT LEN(CAST(? AS NVARCHAR(MAX)))"

    def run(cur):
        for value in (1, 2, 3):
            assert _roundtrip(cur, sql, (value, 10)) == value + 10
        # 1000 needs SMALLINT instead of TINYINT: a changed definition re-prepares.
        assert _roundtrip(cur, sql, (1000, 10)) == 1010
        assert _roundtrip(cur, sql, (1001, 10)) == 1011
        # A NULL is typed through SQLDescribeParam on unbound records, then reused.
        assert _roundtrip(cur, sql, (None, 10)) is None
        assert _roundtrip(cur, sql, (None, 11)) is None
        # Data-at-execution values with an unchanged definition.
        assert _roundtrip(cur, lob_sql, ("x" * 5000,)) == 5000
        assert _roundtrip(cur, lob_sql, ("y" * 6000,)) == 6000

    try:
        ops = _prepare_or_execute(_rpc_names(admin, conn_str, run, min_events=9))
    finally:
        admin_conn.close()
    assert ops == [
        "prepare",
        "execute",
        "execute",
        "prepare",
        "execute",
        "prepare",
        "execute",
        "prepare",
        "execute",
    ]
