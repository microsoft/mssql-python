"""
Tests for Arrow (pyarrow) integration with the Cursor class.

These tests require pyarrow to be installed; they are skipped otherwise.
"""

import pytest
import decimal
import io
import os
from pathlib import Path
import subprocess
import sys
import textwrap
from datetime import datetime, date, time, timezone

import mssql_python

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    pa = None
    pq = None

# Skip the entire module if pyarrow is not available
pytestmark = pytest.mark.skipif(pa is None, reason="pyarrow is not installed")


_ARROW_BUDGET_SCRIPT = """
import gc
import os
import sys
from datetime import date
from decimal import Decimal
from uuid import UUID
import mssql_python
import pyarrow.compute as pc

def wide_query(sql_type, columns, count=8192, identifiers=False, extra=""):
    payload = ", ".join(
        f"CAST(REPLICATE(N'A', 4000) AS {sql_type}) AS c{i}" for i in range(columns)
    )
    return f'''
        WITH digits(d) AS (
            SELECT d FROM (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) v(d)
        ), numbers AS (
            SELECT TOP ({count})
                ROW_NUMBER() OVER (ORDER BY a.d,b.d,c.d,d.d) AS id
            FROM digits a CROSS JOIN digits b CROSS JOIN digits c CROSS JOIN digits d
        )
        SELECT {"id," if identifiers else ""} {payload} {extra}
        FROM numbers ORDER BY id
    '''

def check_payload(value, columns, offset=0):
    for column in range(offset, offset + columns):
        assert pc.all(pc.equal(value.column(column), "A" * 4000)).as_py()

"""


def _run_arrow_budget_script(conn_str, script, *args):
    env = os.environ.copy()
    env["DB_CONNECTION_STRING"] = str(conn_str)
    result = subprocess.run(
        [sys.executable, "-c", _ARROW_BUDGET_SCRIPT + textwrap.dedent(script), *args],
        cwd=Path(__file__).resolve().parents[1],
        env=env,
        capture_output=True,
        text=True,
        timeout=180,
    )
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize("sql_type", ["nvarchar(4000)", "nvarchar(max)"])
@pytest.mark.parametrize("method", ["arrow", "arrow_batch"])
@pytest.mark.parametrize("columns", [7, 9])
def test_arrow_native_budget_lossless_batches(conn_str, sql_type, method, columns):
    """Fitting results stay whole; oversized batches return every row without loss."""
    _run_arrow_budget_script(
        conn_str,
        """
        sql_type, method, columns = sys.argv[1], sys.argv[2], int(sys.argv[3])
        with mssql_python.connect(os.environ["DB_CONNECTION_STRING"]) as conn:
            with conn.cursor() as cur:
                cur.execute(wide_query(sql_type, columns))
                if method == "arrow":
                    value = cur.arrow()
                    assert value.num_rows == 8192
                    check_payload(value, columns)
                    if columns == 7:
                        assert value.nbytes == 229841920
                    else:
                        assert value.nbytes > 256 * 1024 * 1024
                        assert len(value.column(0).chunks) > 1
                    del value
                else:
                    total = 0
                    total_bytes = 0
                    first = True
                    while True:
                        value = cur.arrow_batch(8192)
                        count = value.num_rows
                        if first:
                            assert count == 8192 if columns == 7 else 0 < count < 8192
                            first = False
                        if not count:
                            break
                        check_payload(value, columns)
                        total += count
                        total_bytes += value.nbytes
                        assert value.nbytes <= 256 * 1024 * 1024
                        assert cur.rownumber == total - 1
                        del value
                    assert total == 8192
                    if columns == 7:
                        assert total_bytes == 229841920
                    else:
                        assert total_bytes > 256 * 1024 * 1024
                assert cur.fetchone() is None
        """,
        sql_type,
        method,
        str(columns),
    )


@pytest.mark.parametrize("sql_type", ["nvarchar(4000)", "nvarchar(max)"])
@pytest.mark.parametrize("remaining", ["mixed", "fetchall", "arrow_reader"])
def test_arrow_native_budget_pending_mixed_fetches(conn_str, sql_type, remaining):
    """A quota boundary preserves the overflow row and the rest of its native rowset."""
    _run_arrow_budget_script(
        conn_str,
        """
        sql_type = sys.argv[1]
        remaining = sys.argv[2]
        row_count = 4096
        extra = '''
            , CAST('123.45' AS DECIMAL(10,2)) amount
            , CAST('2025-01-02' AS DATE) day
            , CAST('58185E0D-3A91-44D8-BC46-7107217E0A6D' AS UNIQUEIDENTIFIER) guid
            , CAST(0x010203 AS VARBINARY(3)) binary_value
            , CAST(NULL AS INT) null_value
            , CAST(1 AS BIT) flag
        '''
        def check_row(row, expected):
            assert row[0] == expected
            assert all(value == "A" * 4000 for value in row[1:10])
            assert row[10] == Decimal("123.45")
            assert row[11] == date(2025, 1, 2)
            assert row[12] == UUID("58185E0D-3A91-44D8-BC46-7107217E0A6D")
            assert row[13] == b"\\x01\\x02\\x03"
            assert row[14] is None
            assert row[15] is True

        with mssql_python.connect(os.environ["DB_CONNECTION_STRING"]) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    wide_query(sql_type, 9, count=row_count, identifiers=True, extra=extra)
                )
                first = cur.arrow_batch(8192)
                n = first.num_rows
                assert 0 < n < row_count
                assert first.column(0).to_pylist() == list(range(1, n + 1))
                del first
                assert cur.arrow_batch(0).num_rows == 0
                from mssql_python import ddbc_bindings
                from mssql_python.constants import ConstantsDDBC
                # Updating the timeout invalidates metadata, not pending result rows.
                assert ddbc_bindings.DDBCSQLSetStmtAttr(
                    cur.hstmt, ConstantsDDBC.SQL_ATTR_QUERY_TIMEOUT.value, 0
                ) in (0, 1)
                if remaining == "mixed":
                    check_row(cur.fetchone(), n + 1)
                    n += 1
                    rows = cur.fetchmany(5)
                    assert len(rows) == 5
                    for i, row in enumerate(rows, n + 1):
                        check_row(row, i)
                    n += 5
                    small = cur.arrow_batch(1)
                    assert small.column(0).to_pylist() == [n + 1]
                    n += 1
                    del small, rows
                    rows = cur.fetchmany(100)
                    assert 0 < len(rows) <= 100
                    for i, row in enumerate(rows, n + 1):
                        check_row(row, i)
                    n += len(rows)
                    del rows
                    rest = cur.arrow()
                    assert rest.column(0).to_pylist() == list(range(n + 1, row_count + 1))
                    del rest
                elif remaining == "fetchall":
                    rows = cur.fetchall()
                    assert len(rows) == row_count - n
                    for i, row in enumerate(rows, n + 1):
                        check_row(row, i)
                    del rows
                else:
                    with cur.arrow_reader() as reader:
                        for batch in reader:
                            count = batch.num_rows
                            assert batch.column(0).to_pylist() == list(
                                range(n + 1, n + count + 1)
                            )
                            n += count
                            del batch
                    assert n == row_count
                if remaining != "arrow_reader":
                    assert cur.rownumber == row_count - 1
                    assert cur.fetchone() is None
            gc.collect()
        """,
        sql_type,
        remaining,
    )


@pytest.mark.parametrize("sql_type", ["nvarchar(4000)", "nvarchar(max)"])
def test_arrow_native_budget_pending_scroll_skip(conn_str, sql_type):
    """Forward skips consume logical pending rows before advancing the driver."""
    _run_arrow_budget_script(
        conn_str,
        """
        sql_type = sys.argv[1]
        with mssql_python.connect(os.environ["DB_CONNECTION_STRING"]) as conn:
            with conn.cursor() as cur:
                cur.execute(wide_query(sql_type, 9, identifiers=True))
                first = cur.arrow_batch(8192)
                position = first.num_rows
                assert 0 < position < 8192
                del first
                cur.scroll(0)
                cur.skip(0)
                assert cur.rownumber == position - 1
                for value, mode in ((-1, "relative"), (0, "absolute"), (1, "absolute")):
                    try:
                        cur.scroll(value, mode)
                    except mssql_python.NotSupportedError:
                        pass
                    else:
                        raise AssertionError("Unsupported Python scroll was accepted")
                assert cur.rownumber == position - 1
                cur.scroll(1)
                position += 1
                assert cur.fetchone()[0] == position + 1
                position += 1
                cur.skip(3)
                position += 3
                batch = cur.arrow_batch(1)
                assert batch.column(0).to_pylist() == [position + 1]
                position += 1
                del batch
                # Cross the retained 64-row bound rowset (or the one-row LOB state).
                offset = 67 if sql_type == "nvarchar(4000)" else 3
                cur.scroll(offset)
                position += offset
                rows = cur.fetchmany(2)
                assert [row[0] for row in rows] == [position + 1, position + 2]
                position += 2
                del rows
                assert cur.rownumber == position - 1
                cur.skip(8192 - position)
                assert cur.rownumber == 8191
                assert cur.fetchone() is None
                try:
                    cur.scroll(1)
                except IndexError:
                    pass
                else:
                    raise AssertionError("Skipping beyond EOF did not raise")
                assert cur.fetchone() is None
                assert cur.execute("SELECT 42").fetchone()[0] == 42
        """,
        sql_type,
    )


@pytest.mark.parametrize("sql_type", ["nvarchar(4000)", "nvarchar(max)"])
def test_arrow_native_budget_pending_raw_fetches(conn_str, sql_type):
    """Raw Fetch/FetchScroll select rows; GetData reads the selected cached row."""
    _run_arrow_budget_script(
        conn_str,
        """
        from mssql_python import ddbc_bindings as ddbc
        from mssql_python.constants import ConstantsDDBC as C
        sql_type = sys.argv[1]
        row_count = 4096
        unsupported = (
            (C.SQL_FETCH_RELATIVE.value, 0), (C.SQL_FETCH_RELATIVE.value, 1),
            (C.SQL_FETCH_RELATIVE.value, 67), (C.SQL_FETCH_RELATIVE.value, -1),
            (C.SQL_FETCH_ABSOLUTE.value, 0), (C.SQL_FETCH_ABSOLUTE.value, 1),
            (C.SQL_FETCH_ABSOLUTE.value, -1), (C.SQL_FETCH_FIRST.value, 0),
            (C.SQL_FETCH_LAST.value, 0), (C.SQL_FETCH_PRIOR.value, 0),
        )
        def get_data(cur, count):
            row = []
            assert ddbc.DDBCSQLGetData(
                cur.hstmt, count, row, "utf-16le", "utf-16le", C.SQL_WCHAR.value
            ) in (0, 1)
            return row
        with mssql_python.connect(os.environ["DB_CONNECTION_STRING"]) as conn:
            with conn.cursor() as cur:
                cur.execute(wide_query(sql_type, 9, count=row_count, identifiers=True))
                first = cur.arrow_batch(8192)
                position = first.num_rows
                assert 0 < position < row_count
                del first
                try:
                    get_data(cur, 1)
                except RuntimeError as exc:
                    assert "requires a successful DDBCSQLFetch" in str(exc)
                else:
                    raise AssertionError("GetData consumed an unselected pending row")
                for orientation, offset in unsupported:
                    try:
                        ddbc.DDBCSQLFetchScroll(cur.hstmt, orientation, offset, [])
                    except RuntimeError as exc:
                        assert "Only SQL_FETCH_NEXT" in str(exc)
                    else:
                        raise AssertionError("Unsupported scroll advanced pending data")
                assert ddbc.DDBCSQLFetch(cur.hstmt) in (0, 1)
                position += 1
                cur.scroll(0)
                cur.skip(0)
                assert get_data(cur, 0) == []
                assert get_data(cur, 1) == [position]
                for orientation, offset in unsupported:
                    try:
                        ddbc.DDBCSQLFetchScroll(cur.hstmt, orientation, offset, [])
                    except RuntimeError as exc:
                        assert "Only SQL_FETCH_NEXT" in str(exc)
                    else:
                        raise AssertionError("Unsupported scroll discarded the selected row")
                assert get_data(cur, 1) == [position]
                assert cur.arrow_batch(0).num_rows == 0
                assert ddbc.DDBCSQLSetStmtAttr(
                    cur.hstmt, C.SQL_ATTR_QUERY_TIMEOUT.value, 0
                ) in (0, 1)
                assert get_data(cur, 3) == [position, "A" * 4000, "A" * 4000]
                try:
                    get_data(cur, 11)
                except RuntimeError as exc:
                    assert "Column count exceeds" in str(exc)
                else:
                    raise AssertionError("Invalid prefix count was accepted")
                row = get_data(cur, 10)
                assert row[0] == position
                assert row[1:] == ["A" * 4000] * 9
                del row
                row = []
                assert ddbc.DDBCSQLFetchScroll(
                    cur.hstmt, C.SQL_FETCH_NEXT.value, 0, row
                ) in (0, 1)
                position += 1
                assert row[0] == position
                assert row[1:] == ["A" * 4000] * 9
                del row
                assert cur.fetchone()[0] == position + 1
                position += 1
                assert ddbc.DDBCSQLFetch(cur.hstmt) in (0, 1)
                position += 1
                assert get_data(cur, 1) == [position]
                rows = cur.fetchmany(2)
                assert [row[0] for row in rows] == [position + 1, position + 2]
                position += 2
                del rows
                # A raw fetch without GetData still consumes exactly one logical row.
                assert ddbc.DDBCSQLFetch(cur.hstmt) in (0, 1)
                position += 1
                batch = cur.arrow_batch(1)
                assert batch.column(0).to_pylist() == [position + 1]
                position += 1
                del batch
                row = []
                assert ddbc.DDBCSQLFetchScroll(
                    cur.hstmt, C.SQL_FETCH_NEXT.value, 0, row
                ) in (0, 1)
                position += 1
                assert row[0] == position
                assert row[1:] == ["A" * 4000] * 9
                del row
                # NEXT ignores its offset, and a nonempty output suppresses materialization.
                for offset in (0, 1, 67, -1, 123):
                    row = ["unchanged"]
                    assert ddbc.DDBCSQLFetchScroll(
                        cur.hstmt, C.SQL_FETCH_NEXT.value, offset, row
                    ) in (0, 1)
                    position += 1
                    assert row == ["unchanged"]
                    assert get_data(cur, 1) == [position]
                while True:
                    if position % 2 == 0:
                        ret = ddbc.DDBCSQLFetchScroll(
                            cur.hstmt, C.SQL_FETCH_NEXT.value, 0, ["no materialization"]
                        )
                    else:
                        ret = ddbc.DDBCSQLFetch(cur.hstmt)
                    if ret == C.SQL_NO_DATA.value:
                        break
                    assert ret in (0, 1)
                    position += 1
                    assert get_data(cur, 1) == [position]
                assert position == row_count
                row = []
                assert ddbc.DDBCSQLFetchScroll(
                    cur.hstmt, C.SQL_FETCH_NEXT.value, 0, row
                ) == C.SQL_NO_DATA.value
                assert row == []
                assert cur.fetchone() is None
        """,
        sql_type,
    )


@pytest.mark.parametrize("sql_type", ["nvarchar(4000)", "nvarchar(max)"])
def test_arrow_native_budget_pending_fetchall_after_raw_fetch(conn_str, sql_type):
    """Fetchall resumes after a raw fetch selects a pending row."""
    _run_arrow_budget_script(
        conn_str,
        """
        from mssql_python import ddbc_bindings as ddbc
        from mssql_python.constants import ConstantsDDBC as C
        sql_type = sys.argv[1]
        row_count = 4096
        with mssql_python.connect(os.environ["DB_CONNECTION_STRING"]) as conn:
            with conn.cursor() as cur:
                cur.execute(wide_query(sql_type, 9, count=row_count, identifiers=True))
                first = cur.arrow_batch(8192)
                position = first.num_rows
                assert 0 < position < row_count
                del first
                assert ddbc.DDBCSQLFetch(cur.hstmt) in (0, 1)
                position += 1
                row = []
                assert ddbc.DDBCSQLGetData(
                    cur.hstmt, 1, row, "utf-16le", "utf-16le", C.SQL_WCHAR.value
                ) in (0, 1)
                assert row == [position]
                rows = cur.fetchall()
                assert [row[0] for row in rows] == list(range(position + 1, row_count + 1))
                assert all(all(value == "A" * 4000 for value in row[1:]) for row in rows)
                del rows
                assert cur.fetchone() is None
        """,
        sql_type,
    )


def test_arrow_native_budget_native_scroll_orientations(conn_str):
    """Driver-rejected forward-only orientations raise and leave the result untouched."""
    _run_arrow_budget_script(
        conn_str,
        """
        from mssql_python import ddbc_bindings as ddbc
        from mssql_python.constants import ConstantsDDBC as C
        unsupported = (
            (C.SQL_FETCH_RELATIVE.value, 0), (C.SQL_FETCH_RELATIVE.value, 1),
            (C.SQL_FETCH_RELATIVE.value, 67), (C.SQL_FETCH_RELATIVE.value, -1),
            (C.SQL_FETCH_ABSOLUTE.value, 0), (C.SQL_FETCH_ABSOLUTE.value, 1),
            (C.SQL_FETCH_ABSOLUTE.value, -1), (C.SQL_FETCH_FIRST.value, 0),
            (C.SQL_FETCH_LAST.value, 0), (C.SQL_FETCH_PRIOR.value, 0),
        )
        with mssql_python.connect(os.environ["DB_CONNECTION_STRING"]) as conn:
            for orientation, offset in unsupported:
                with conn.cursor() as cur:
                    cur.execute("SELECT id FROM (VALUES (1),(2),(3)) v(id) ORDER BY id")
                    row = []
                    try:
                        ddbc.DDBCSQLFetchScroll(cur.hstmt, orientation, offset, row)
                    except mssql_python.Error:
                        pass
                    else:
                        raise AssertionError("Unsupported native scroll did not raise")
                    assert row == []
                    assert cur.fetchone()[0] == 1
            for offset in (0, 1, 67, -1):
                with conn.cursor() as cur:
                    cur.execute("SELECT id FROM (VALUES (1),(2),(3)) v(id) ORDER BY id")
                    row = []
                    assert ddbc.DDBCSQLFetchScroll(
                        cur.hstmt, C.SQL_FETCH_NEXT.value, offset, row
                    ) in (0, 1)
                    assert row == [1]
                    assert cur.fetchone()[0] == 2
        """,
    )


@pytest.mark.parametrize("recovery", ["arrow_batch", "fetchone", "raw_fetch"])
def test_arrow_native_budget_resumes_partial_lob(conn_str, recovery):
    """Resume SQLGetData when source growth, rather than Arrow growth, hits the budget."""
    _run_arrow_budget_script(
        conn_str,
        """
        recovery = sys.argv[1]
        query = wide_query("nvarchar(max)", 7, count=6002, identifiers=True)
        query = query.replace(
            "CAST(REPLICATE(N'A', 4000) AS nvarchar(max)) AS c0",
            "CASE WHEN id = 6001 THEN REPLICATE(CAST(N'A' AS nvarchar(max)), "
            "40 * 1024 * 1024) ELSE CAST(REPLICATE(N'A', 4000) AS nvarchar(max)) END AS c0"
        )
        with mssql_python.connect(os.environ["DB_CONNECTION_STRING"]) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                first = cur.arrow_batch(8192)
                assert first.num_rows == 6000
                del first
                if recovery != "arrow_batch":
                    if recovery == "raw_fetch":
                        from mssql_python import ddbc_bindings as ddbc
                        from mssql_python.constants import ConstantsDDBC as C
                        assert ddbc.DDBCSQLFetch(cur.hstmt) in (0, 1)
                        prefix = []
                        assert ddbc.DDBCSQLGetData(
                            cur.hstmt, 1, prefix, "utf-16le", "utf-16le", C.SQL_WCHAR.value
                        ) in (0, 1)
                        assert prefix == [6001]
                        row = []
                        assert ddbc.DDBCSQLGetData(
                            cur.hstmt, 8, row, "utf-16le", "utf-16le", C.SQL_WCHAR.value
                        ) in (0, 1)
                    else:
                        row = cur.fetchone()
                    assert row[0] == 6001
                    assert len(row[1]) == 40 * 1024 * 1024
                    assert row[1].count("A") == len(row[1])
                    assert all(value == "A" * 4000 for value in row[2:])
                    del row
                    assert cur.fetchone()[0] == 6002
                else:
                    batch = cur.arrow_batch(8192)
                    assert batch.column(0).to_pylist() == [6001, 6002]
                    value = batch.column(1)[0].as_py()
                    assert len(value) == 40 * 1024 * 1024
                    assert value.count("A") == len(value)
                    del value, batch
                assert cur.fetchone() is None
        """,
        recovery,
    )


def test_arrow_native_budget_single_row_recovery(conn_str):
    """A row that cannot coexist with its Arrow output stays available to row-wise fetches."""
    _run_arrow_budget_script(
        conn_str,
        """
        with mssql_python.connect(os.environ["DB_CONNECTION_STRING"]) as conn:
            with conn.cursor() as cur:
                cur.execute('''
                    SELECT CASE WHEN id = 1
                        THEN REPLICATE(CAST(NCHAR(20013) AS NVARCHAR(MAX)), 56 * 1024 * 1024)
                        ELSE N'end' END AS value
                    FROM (VALUES (1),(2)) v(id) ORDER BY id
                ''')
                initial_position = cur.rownumber
                for _ in range(2):
                    try:
                        cur.arrow_batch(8192)
                    except RuntimeError as exc:
                        assert "row remains pending" in str(exc)
                    else:
                        raise AssertionError("Oversized Arrow row did not raise")
                assert cur.rownumber == initial_position
                assert cur.arrow_batch(0).num_rows == 0
                row = cur.fetchone()
                assert len(row[0]) == 56 * 1024 * 1024
                assert row[0].count(chr(20013)) == len(row[0])
                del row
                assert cur.fetchmany(2)[0][0] == "end"
                assert cur.fetchone() is None
                cur.execute('''
                    SELECT CASE WHEN id = 1
                        THEN REPLICATE(CAST(N'Z' AS NVARCHAR(MAX)), 140 * 1024 * 1024)
                        ELSE N'end' END AS value
                    FROM (VALUES (1),(2)) v(id) ORDER BY id
                ''')
                for method in ("arrow_batch", "fetchone", "arrow_batch"):
                    try:
                        getattr(cur, method)()
                    except RuntimeError as exc:
                        assert "256 MiB" in str(exc)
                    else:
                        raise AssertionError("A row exceeding the source budget was dropped")
                cur.skip(1)
                assert cur.fetchone()[0] == "end"
                assert cur.fetchone() is None
                assert cur.execute("SELECT 42").fetchone()[0] == 42
        """,
    )


@pytest.mark.parametrize("sql_type", ["nvarchar(4000)", "nvarchar(max)"])
@pytest.mark.parametrize(
    "operation", ["execute", "nextset", "close", "reader_close", "connection_close"]
)
def test_arrow_native_budget_pending_reset_cleanup(conn_str, sql_type, operation):
    _run_arrow_budget_script(
        conn_str,
        """
        sql_type = sys.argv[1]
        operation = sys.argv[2]
        row_count = 4096
        with mssql_python.connect(os.environ["DB_CONNECTION_STRING"]) as conn:
            with conn.cursor() as cur:
                cur.execute(wide_query(sql_type, 9, count=row_count) + "; SELECT 42 AS answer")
                first = cur.arrow_batch(8192)
                assert 0 < first.num_rows < row_count
                del first
                from mssql_python import ddbc_bindings
                assert ddbc_bindings.DDBCSQLFetch(cur.hstmt) in (0, 1)
                if operation == "nextset":
                    assert cur.nextset()
                    assert cur.fetchone()[0] == 42
                    assert cur.fetchone() is None
                elif operation == "execute":
                    assert cur.execute("SELECT 42").fetchone()[0] == 42
                elif operation == "reader_close":
                    reader = cur.arrow_reader()
                    reader.close()
                    assert cur.execute("SELECT 42").fetchone()[0] == 42
                elif operation == "close":
                    cur.close()
                    with conn.cursor() as fresh:
                        assert fresh.execute("SELECT 42").fetchone()[0] == 42
                else:
                    conn.close()
        gc.collect()
        """,
        sql_type,
        operation,
    )


def get_arrow_test_data(include_lobs: bool, batch_length: int):
    arrow_test_data = [
        (pa.uint8(), "tinyint", [1, 2, None, 4, 5, 0, 2**8 - 1]),
        (pa.int16(), "smallint", [1, 2, None, 4, 5, -(2**15), 2**15 - 1]),
        (pa.int32(), "int", [1, 2, None, 4, 5, 0, -(2**31), 2**31 - 1]),
        (pa.int64(), "bigint", [1, 2, None, 4, 5, 0, -(2**63), 2**63 - 1]),
        (pa.float64(), "float", [1.0, 2.5, None, 4.25, 5.125]),
        (pa.float32(), "real", [1.0, 2.5, None, 4.25, 5.125]),
        (
            pa.decimal128(precision=10, scale=2),
            "decimal(10, 2)",
            [
                decimal.Decimal("1.23"),
                None,
                decimal.Decimal("0.25"),
                decimal.Decimal("-99999999.99"),
                decimal.Decimal("99999999.99"),
            ],
        ),
        (
            pa.decimal128(precision=38, scale=10),
            "decimal(38, 10)",
            [
                decimal.Decimal("1.1234567890"),
                None,
                decimal.Decimal("0"),
                decimal.Decimal("1.0000000001"),
                decimal.Decimal("-9999999999999999999999999999.9999999999"),
                decimal.Decimal("9999999999999999999999999999.9999999999"),
            ],
        ),
        (
            pa.decimal128(precision=38, scale=0),
            "decimal(38, 0)",
            [
                decimal.Decimal(str(2**63)),
                decimal.Decimal(str(-(2**63))),
                decimal.Decimal(str(2**64)),
                decimal.Decimal(str(-(2**64))),
                decimal.Decimal(str(2**64 - 1)),
                decimal.Decimal(str(-(2**64 - 1))),
                decimal.Decimal(str(2**64 + 1)),
                decimal.Decimal(str(-(2**64 + 1))),
                decimal.Decimal(str(2**96)),
                decimal.Decimal(str(-(2**96))),
            ],
        ),
        (pa.bool_(), "bit", [True, None, False]),
        (pa.large_binary(), "binary(9)", [b"asdfghjkl", None, b"lkjhgfdsa"]),
        (pa.large_string(), "varchar(100)", ["asdfghjkl", None, "lkjhgfdsa"]),
        (pa.large_string(), "nvarchar(100)", ["asdfghjkl", None, "lkjhgfdsa"]),
        (pa.large_string(), "uniqueidentifier", ["58185E0D-3A91-44D8-BC46-7107217E0A6D", None]),
        (
            pa.date32(),
            "date",
            [
                date(1, 1, 1),
                None,
                date(2345, 12, 31),
                date(9999, 12, 31),
                date(1970, 1, 1),
                date(1969, 12, 31),
                date(2000, 2, 29),
                date(2001, 2, 28),
            ],
        ),
        (
            pa.time64("ns"),
            "time(0)",
            [time(12, 0, 5, 0), None, time(23, 59, 59, 0), time(0, 0, 0, 0)],
        ),
        (
            pa.time64("ns"),
            "time(7)",
            [time(12, 0, 5, 0), None, time(23, 59, 59, 0), time(0, 0, 0, 0)],
        ),
        (
            pa.time64("ns"),
            "time(7)",
            [time(12, 0, 5, 123456), None, time(23, 59, 59, 123456), time(0, 0, 0, 0)],
        ),
        (
            pa.timestamp("us"),
            "datetime2(0)",
            [datetime(2025, 1, 1, 12, 0, 5, 0), None, datetime(2345, 12, 31, 23, 59, 59, 0)],
        ),
        (
            pa.timestamp("us"),
            "datetime2(3)",
            [datetime(2025, 1, 1, 12, 0, 5, 123_000), None, datetime(2345, 12, 31, 23, 59, 59, 0)],
        ),
        (
            pa.timestamp("us"),
            "datetime2(6)",
            [datetime(2025, 1, 1, 12, 0, 5, 123_456), None, datetime(2345, 12, 31, 23, 59, 59, 0)],
        ),
        (
            pa.timestamp("us"),
            "datetime2(7)",
            [datetime(2025, 1, 1, 12, 0, 5, 123_456), None, datetime(2145, 12, 31, 23, 59, 59, 0)],
        ),
        (
            pa.timestamp("us"),
            "datetime2(2)",
            [datetime(2025, 1, 1, 12, 0, 5, 0), None, datetime(2145, 12, 31, 23, 59, 59, 0)],
        ),
    ]

    if include_lobs:
        arrow_test_data += [
            (pa.large_string(), "nvarchar(max)", ["hey", None, "ho"]),
            (pa.large_string(), "varchar(max)", ["hey", None, "ho"]),
            (pa.large_binary(), "varbinary(max)", [b"hey", None, b"ho"]),
        ]

    for ix in range(len(arrow_test_data)):
        while True:
            T, sql_type, vals = arrow_test_data[ix]
            if len(vals) >= batch_length:
                arrow_test_data[ix] = (T, sql_type, vals[:batch_length])
                break
            arrow_test_data[ix] = (T, sql_type, vals + vals)

    return arrow_test_data


def _test_arrow_test_data(cursor: mssql_python.Cursor, arrow_test_data, fetch_length=500):
    cols = []
    for i_col, (pa_type, sql_type, values) in enumerate(arrow_test_data):
        rows = []
        for value in values:
            if type(value) is bool:
                value = int(value)
            if type(value) is bytes:
                value = value.decode()
            if value is None:
                value = "null"
            else:
                value = f"'{value}'"
            rows.append(f"col_{i_col} = cast({value} as {sql_type})")
        cols.append(rows)

    selects = []
    for row in zip(*cols):
        selects.append(f"select {', '.join(col for col in row)}")
    full_query = "\nunion all\n".join(selects)
    ret = cursor.execute(full_query).arrow_batch(fetch_length)
    for i_col, col in enumerate(ret):
        expected_data = arrow_test_data[i_col][2][:fetch_length]
        for i_row, (v_expected, v_actual) in enumerate(
            zip(expected_data, col.to_pylist(), strict=True)
        ):
            assert (
                v_expected == v_actual
            ), f"Mismatch in column {i_col}, row {i_row}: expected {v_expected}, got {v_actual}"
        # check that null counts match
        expected_null_count = sum(1 for v in expected_data if v is None)
        actual_null_count = col.null_count
        assert expected_null_count == actual_null_count, (expected_null_count, actual_null_count)
    for i_col, (pa_type, sql_type, values) in enumerate(arrow_test_data):
        field = ret.schema.field(i_col)
        assert (
            field.name == f"col_{i_col}"
        ), f"Column {i_col} name mismatch: expected col_{i_col}, got {field.name}"
        assert field.type.equals(
            pa_type
        ), f"Column {i_col} type mismatch: expected {pa_type}, got {field.type}"

    # Validate that Parquet serialization/deserialization does not detect any issues
    tbl = pa.Table.from_batches([ret])
    buffer = io.BytesIO()
    pq.write_table(tbl, buffer)
    buffer.seek(0)
    read_table = pq.read_table(buffer)
    assert read_table.equals(tbl)


def test_arrow_lob_wide(cursor: mssql_python.Cursor):
    "Take the SQLGetData branch for a wide table."
    arrow_test_data = get_arrow_test_data(include_lobs=True, batch_length=123)
    _test_arrow_test_data(cursor, arrow_test_data)


def test_arrow_nolob_wide(cursor: mssql_python.Cursor):
    "Test the SQLBindData branch for a wide table."
    arrow_test_data = get_arrow_test_data(include_lobs=False, batch_length=123)
    _test_arrow_test_data(cursor, arrow_test_data)


def test_arrow_single_column(cursor: mssql_python.Cursor):
    "Test each datatype as a single column fetch."
    arrow_test_data = get_arrow_test_data(include_lobs=True, batch_length=123)
    for col_data in arrow_test_data:
        _test_arrow_test_data(cursor, [col_data])


def test_arrow_empty_fetch(cursor: mssql_python.Cursor):
    "Test each datatype as a single column fetch of length 0."
    arrow_test_data = get_arrow_test_data(include_lobs=True, batch_length=123)
    for col_data in arrow_test_data:
        _test_arrow_test_data(cursor, [col_data], fetch_length=0)


def test_arrow_wide_schema_only_and_single_row_fit_native_buffer_budget(
    cursor: mssql_python.Cursor,
):
    columns = ", ".join(f"CAST(N'x' AS NVARCHAR(4000)) AS col_{index}" for index in range(600))
    cursor.execute(f"SELECT {columns}")

    schema_batch = cursor.arrow_batch(0)
    assert schema_batch.num_rows == 0
    assert schema_batch.num_columns == 600

    data_batch = cursor.arrow_batch(1)
    assert data_batch.num_rows == 1
    assert data_batch.num_columns == 600


def test_arrow_table_batchsize_negative(cursor: mssql_python.Cursor):
    cursor.execute("select 1 a")
    with pytest.raises(ValueError, match="batch_size"):
        cursor.arrow(batch_size=-42)
    assert cursor.fetchone()[0] == 1


def test_arrow_empty_result_set(cursor: mssql_python.Cursor):
    "Test fetching from an empty result set."
    cursor.execute("select 1 where 1 = 0")
    batch = cursor.arrow_batch(10)
    assert batch.num_rows == 0
    assert batch.num_columns == 1
    cursor.execute("select cast(N'' as nvarchar(max)) where 1 = 0")
    batch = cursor.arrow_batch(10)
    assert batch.num_rows == 0
    assert batch.num_columns == 1
    cursor.execute("select 1, cast(N'' as nvarchar(max)) where 1 = 0")
    batch = cursor.arrow_batch(10)
    assert batch.num_rows == 0
    assert batch.num_columns == 2


def test_arrow_no_result_set(cursor: mssql_python.Cursor):
    "Test fetching when there is no result set."
    cursor.execute("declare @a int")
    with pytest.raises(Exception, match=".*No active result set.*"):
        cursor.arrow_batch(10)


def test_arrow_datetimeoffset(cursor: mssql_python.Cursor):
    "Datetimeoffset converts correctly to utc"
    for force_sqlgetdata in (False, True):
        str_val = "cast('asdf' as nvarchar(max))" if force_sqlgetdata else "'asdf'"
        cursor.execute(
            "declare @dt datetimeoffset(0) = '2345-02-03 12:34:56 +00:00';\n"
            f"select {str_val}, @dt, @dt at time zone 'Pacific Standard Time';\n"
        )
        batch = cursor.arrow_batch(10)
        assert batch.num_rows == 1
        assert batch.num_columns == 3
        for col in batch.columns[1:]:
            assert pa.types.is_timestamp(col.type)
            assert col.type.tz == "+00:00", col.type.tz
            assert col.to_pylist() == [
                datetime(2345, 2, 3, 12, 34, 56, tzinfo=timezone.utc),
            ]


def test_arrow_schema_nullable(cursor: mssql_python.Cursor):
    "Test that the schema is nullable."
    cursor.execute("select 1 a, null b")
    batch = cursor.arrow_batch(10)
    assert batch.num_rows == 1
    assert batch.num_columns == 2
    assert not batch.schema.field(0).nullable
    assert batch.schema.field(1).nullable
    assert batch.schema.field(0).name == "a"
    assert batch.schema.field(1).name == "b"


def test_arrow_table(cursor: mssql_python.Cursor):
    tbl = cursor.execute("select top 11 1 a from sys.objects").arrow(batch_size=5)
    assert type(tbl) is pa.Table
    assert tbl.num_rows == 11
    assert tbl.num_columns == 1
    assert [len(b) for b in tbl.to_batches()] == [5, 5, 1]


def test_arrow_reader(cursor: mssql_python.Cursor):
    reader = cursor.execute("select top 11 1 a from sys.objects").arrow_reader(batch_size=4)
    # arrow_reader returns a RecordBatchReader-compatible wrapper (not the
    # raw pyarrow.RecordBatchReader) so that .close() can actually stop
    # fetching and release the server-side cursor.  Verify duck-typed
    # compatibility instead of exact identity.
    assert hasattr(reader, "schema")
    assert hasattr(reader, "read_next_batch")
    assert hasattr(reader, "close")
    batches = list(reader)
    assert [len(b) for b in batches] == [4, 4, 3]
    assert sum(len(b) for b in batches) == 11


def test_arrow_reader_read_all_returns_table(cursor: mssql_python.Cursor):
    """Regression: ``reader.read_all()`` is the idiomatic pyarrow way to
    drain a reader into a Table and must work through the wrapper.  Was
    silently missing when the class hand-enumerated the pyarrow surface;
    covered here to lock the ``__getattr__`` delegation contract in place.
    """
    reader = cursor.execute("select top 11 1 a from sys.objects").arrow_reader(batch_size=4)
    tbl = reader.read_all()
    assert type(tbl) is pa.Table
    assert tbl.num_rows == 11
    assert tbl.num_columns == 1
    # After a successful drain the reader is empty; close() must still be
    # safe (idempotency + generator finally).
    reader.close()
    assert reader.closed is True
    # Parent cursor stays usable.
    cursor.execute("select 42")
    assert cursor.fetchone()[0] == 42


def test_arrow_reader_read_pandas_returns_dataframe(cursor: mssql_python.Cursor):
    """Regression: ``reader.read_pandas()`` must delegate to the inner
    reader and return a pandas ``DataFrame``.  Skipped if pandas isn't
    importable in the test env.
    """
    pd = pytest.importorskip("pandas")
    reader = cursor.execute("select top 7 1 a from sys.objects").arrow_reader(batch_size=3)
    df = reader.read_pandas()
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 7
    assert list(df.columns) == ["a"]
    reader.close()


def test_arrow_reader_cast_delegates(cursor: mssql_python.Cursor):
    """Regression: ``reader.cast(target_schema)`` must delegate to the
    inner reader.  ``cast`` returns a new pyarrow reader that projects the
    stream to a different schema — we're not exercising the projection
    logic itself, only that the delegation path exists and returns
    something reader-shaped.
    """
    reader = cursor.execute("select top 5 1 a from sys.objects").arrow_reader(batch_size=2)
    # Same schema — the cast should succeed (identity projection).
    casted = reader.cast(reader.schema)
    assert casted is not None
    # ``cast`` returns a real pyarrow RecordBatchReader (not another wrapper),
    # which is fine — the consumer just wants a reader-like object.
    assert hasattr(casted, "read_next_batch")
    reader.close()


def test_arrow_reader_delegates_unknown_pyarrow_method(cursor: mssql_python.Cursor):
    """Guard against future regression: any public method the inner
    pyarrow reader exposes must be reachable through the wrapper without
    the class having to enumerate it explicitly.  Probes ``schema`` (a
    property that used to be hand-written) and ``read_next_batch`` (a
    method that used to be hand-written) via ``__getattr__``.
    """
    reader = cursor.execute("select top 3 1 a from sys.objects").arrow_reader(batch_size=2)
    # schema now goes through __getattr__ delegation.
    schema = reader.schema
    assert schema.field(0).name == "a"
    # read_next_batch also goes through __getattr__ (except when reached
    # via the iteration protocol, which uses __next__).
    batch = reader.read_next_batch()
    assert batch.num_rows > 0
    reader.close()


def test_arrow_reader_delegated_method_raises_after_close(cursor: mssql_python.Cursor):
    """Post-close access via ``__getattr__`` must raise ``ArrowInvalid``
    to match the explicit ``__next__`` / ``__arrow_c_stream__`` semantics —
    a delegated call must not silently succeed on a reader the user has
    already closed.
    """
    reader = cursor.execute("select top 5 1 a from sys.objects").arrow_reader(batch_size=2)
    reader.close()
    with pytest.raises(pa.ArrowInvalid):
        reader.read_all()
    with pytest.raises(pa.ArrowInvalid):
        _ = reader.schema


def test_arrow_reader_pycapsule_protocol(cursor: mssql_python.Cursor):
    """The wrapper implements the Arrow PyCapsule Protocol via
    ``__arrow_c_stream__``, so Arrow-aware consumers can accept it
    without an ``isinstance(x, pa.RecordBatchReader)`` check.

    Regression guard for the alternative to subclassing
    ``pyarrow.RecordBatchReader`` (which is a Cython extension type and
    can't carry our extra state).
    """
    if not hasattr(pa.RecordBatchReader, "from_stream"):
        pytest.skip("pyarrow>=14 required for RecordBatchReader.from_stream")

    reader = cursor.execute("select top 11 1 a from sys.objects").arrow_reader(batch_size=4)
    assert hasattr(reader, "__arrow_c_stream__")
    assert callable(reader.__arrow_c_stream__)

    # Consume the wrapper via the PyCapsule Protocol.  ``from_stream`` calls
    # ``__arrow_c_stream__`` internally, which transfers the C stream out of
    # the inner reader — after this the wrapper is effectively drained but
    # still safely closeable.
    native = pa.RecordBatchReader.from_stream(reader)
    assert isinstance(native, pa.RecordBatchReader)
    total_rows = sum(b.num_rows for b in native)
    assert total_rows == 11

    # Wrapper close() must still be safe after the stream has been
    # transferred out (idempotent + generator finally handles teardown).
    reader.close()
    assert reader.closed is True

    # Parent cursor remains usable.
    cursor.execute("select 42")
    assert cursor.fetchone()[0] == 42


def test_arrow_reader_pycapsule_protocol_raises_after_close(cursor: mssql_python.Cursor):
    """``__arrow_c_stream__`` must refuse export after ``close()`` — matches
    the ``read_next_batch()`` / ``schema`` post-close semantics."""
    reader = cursor.execute("select top 5 1 a from sys.objects").arrow_reader(batch_size=2)
    reader.close()
    with pytest.raises(pa.ArrowInvalid):
        reader.__arrow_c_stream__()


def test_arrow_reader_close_semantics(cursor: mssql_python.Cursor):
    """``reader.close()`` must stop fetching, mark the reader closed, leave
    the parent Cursor usable, be idempotent, and work as a context manager."""
    reader = cursor.execute("select top 1000 1 a from sys.objects o1, sys.objects o2").arrow_reader(
        batch_size=10
    )

    # Drain one batch then close mid-iteration.
    first = reader.read_next_batch()
    assert first.num_rows > 0
    assert reader.closed is False

    reader.close()
    assert reader.closed is True

    # Further reads raise (pyarrow.ArrowInvalid expected).
    with pytest.raises(pa.ArrowInvalid):
        reader.read_next_batch()
    with pytest.raises(pa.ArrowInvalid):
        next(iter(reader))

    # close() is idempotent.
    reader.close()
    reader.close()

    # Parent cursor must still be usable after the reader was closed.
    cursor.execute("select 42")
    row = cursor.fetchone()
    assert row[0] == 42


def test_arrow_reader_context_manager(cursor: mssql_python.Cursor):
    """Using the reader as a context manager closes it on exit."""
    with cursor.execute("select top 5 1 a from sys.objects").arrow_reader(batch_size=2) as reader:
        assert reader.closed is False
        _ = reader.read_next_batch()
    assert reader.closed is True
    # Cursor remains usable.
    cursor.execute("select 7")
    assert cursor.fetchone()[0] == 7


def test_arrow_reader_gc_cleanup(cursor: mssql_python.Cursor):
    """Dropping the reader without calling close() must still release the
    server-side cursor — the try/finally in the batch generator runs on GC."""
    import gc

    reader = cursor.execute("select top 100 1 a from sys.objects").arrow_reader(batch_size=10)
    _ = reader.read_next_batch()  # partial consume

    # Drop the only strong reference and force collection. The generator's
    # finally block must run, releasing the cursor so the next execute()
    # succeeds without ProgrammingError("connection busy") etc.
    del reader
    gc.collect()

    cursor.execute("select 5")
    assert cursor.fetchone()[0] == 5


@pytest.mark.stress  # large cross-join + 50ms timing race — flaky under CI CPU contention
def test_arrow_reader_cancel_from_other_thread(cursor: mssql_python.Cursor):
    """close() called from a separate thread must unblock an in-flight fetch
    via SQLCancel and leave the parent Cursor reusable."""
    import threading
    import time

    # Big enough cross-join that streaming will not finish in <100ms.
    reader = cursor.execute(
        "select top 1000000 1 a from sys.objects o1, sys.objects o2, sys.objects o3"
    ).arrow_reader(batch_size=64)

    closer_done = threading.Event()
    closer_exc = []

    def closer():
        try:
            time.sleep(0.05)  # let the consumer get into a fetch
            reader.close()
        except Exception as e:  # pragma: no cover - reported to main thread
            closer_exc.append(e)
        finally:
            closer_done.set()

    t = threading.Thread(target=closer, daemon=True)
    t.start()

    # Iterate; the cancel from the other thread must terminate the loop
    # (either by exhausting cleanly or by raising) within a couple seconds.
    rows = 0
    try:
        for batch in reader:
            rows += batch.num_rows
            if rows > 2_000_000:  # safety net — should never reach this
                pytest.fail("reader was not cancelled by the other thread")
    except pa.ArrowInvalid:
        pass  # acceptable: reader was closed mid-iteration

    closer_done.wait(timeout=5)
    t.join(timeout=5)
    # Fail loudly if the closer thread did not actually finish — otherwise a
    # deadlock in close() would silently masquerade as a downstream failure
    # (or, worse, hang the interpreter while the daemon thread holds the
    # HSTMT).
    assert (
        closer_done.is_set()
    ), "closer thread did not signal completion within 5s — close() may be deadlocked"
    assert (
        not t.is_alive()
    ), "closer thread is still alive after join(timeout=5) — close() may be deadlocked"
    assert not closer_exc, f"closer thread raised: {closer_exc[0]!r}"
    assert reader.closed is True

    # Parent cursor must still work after the cross-thread cancel.
    cursor.execute("select 99")
    assert cursor.fetchone()[0] == 99


def test_arrow_reader_diagnostics_drained_on_close(cursor: mssql_python.Cursor):
    """After close(), any diagnostic messages produced server-side end up on
    cursor.messages (not silently dropped)."""
    # Drive a result-producing query, partially read, then close.
    reader = cursor.execute("select top 50 1 a from sys.objects").arrow_reader(batch_size=5)
    _ = reader.read_next_batch()
    # messages is a list of (sqlstate, text) tuples; should at least exist
    # and not raise when the close path tries to extend it.
    assert isinstance(cursor.messages, list)
    reader.close()
    assert isinstance(cursor.messages, list)


def test_arrow_reader_drains_diagnostics_when_close_cursor_succeeds(
    cursor: mssql_python.Cursor, monkeypatch
):
    """SQLFreeStmt(SQL_CLOSE) can return SQL_SUCCESS_WITH_INFO — a success
    code that still pushes warning records onto the HSTMT diag stack.  The
    cleanup path must drain diagnostics *unconditionally* after the close
    attempt, not only when _close_cursor() raises, otherwise those warnings
    would be silently dropped."""
    from mssql_python import cursor as cursor_mod

    reader = cursor.execute("select top 10 1 a from sys.objects").arrow_reader(batch_size=5)
    _ = reader.read_next_batch()

    # Snapshot any pre-close diagnostics already on the cursor so we can
    # detect *new* records pushed by our monkeypatched drain calls.
    pre_existing = list(cursor.messages)

    real_drain = cursor_mod.ddbc_bindings.DDBCSQLGetAllDiagRecords
    call_count = {"n": 0}

    def fake_drain(hstmt):
        call_count["n"] += 1
        records = list(real_drain(hstmt))
        # Inject one synthetic record per call so we can prove both the
        # pre-close drain AND the post-close (success-path) drain ran.
        records.append(("01000", f"synthetic warning #{call_count['n']}"))
        return records

    monkeypatch.setattr(cursor_mod.ddbc_bindings, "DDBCSQLGetAllDiagRecords", fake_drain)

    # _close_cursor() should succeed (no exception); the bug would skip the
    # post-close drain entirely on that success path.
    reader.close()

    # Strip the snapshot to look only at messages added by the cleanup path.
    added = cursor.messages[len(pre_existing) :]
    synthetic_texts = [m[1] for m in added if isinstance(m, tuple) and len(m) >= 2]

    assert (
        "synthetic warning #1" in synthetic_texts
    ), "pre-close drain did not push diagnostics onto cursor.messages"
    assert "synthetic warning #2" in synthetic_texts, (
        "post-close drain was skipped on the SQL_CLOSE success path "
        "(SQL_SUCCESS_WITH_INFO warnings would be lost)"
    )
    assert call_count["n"] == 2

    reader = cursor.execute("SELECT 1 AS a UNION ALL SELECT 2").arrow_reader(batch_size=1)
    call_count["n"] = 0
    assert sum(batch.num_rows for batch in reader) == 2
    assert call_count["n"] == 1, "Natural exhaustion must only drain SQL_CLOSE diagnostics"
    assert cursor.messages == [("01000", "synthetic warning #1")]


def test_arrow_reader_close_retries_after_failed_attempt(cursor: mssql_python.Cursor):
    """If a first close() raises before the generator is released (e.g. another
    thread held it and gen.close() raised), a subsequent close() must retry
    the cleanup rather than silently no-op'ing — otherwise the server-side
    cursor would leak."""
    reader = cursor.execute("select top 10 1 a from sys.objects").arrow_reader(batch_size=2)

    real_gen = reader._generator
    assert real_gen is not None

    class FlakyGen:
        """Generator wrapper: first close() raises and reports gi_frame as
        still-set (simulating 'generator currently executing on another
        thread'); second close() delegates to the real generator."""

        def __init__(self, inner):
            self._inner = inner
            self._closed_calls = 0
            self.gi_frame = object()  # truthy => 'still alive'

        def close(self):
            self._closed_calls += 1
            if self._closed_calls == 1:
                raise ValueError("generator already executing")
            # Second call: pretend the other thread released it, delegate.
            self.gi_frame = None
            self._inner.close()

    flaky = FlakyGen(real_gen)
    reader._generator = flaky

    # First close: should mark reader closed (racing reads must raise) but
    # leave _generator intact so a retry is possible.
    reader.close()
    assert reader.closed is True
    assert reader._generator is flaky, "failed close() must not drop the generator ref"
    assert reader._cursor is not None, "failed close() must not drop the cursor ref"
    assert flaky._closed_calls == 1

    # Second close: must retry and complete cleanup this time.
    reader.close()
    assert flaky._closed_calls == 2
    assert reader._generator is None
    assert reader._cursor is None

    # Third close: now a true no-op (fully cleaned up).
    reader.close()
    assert flaky._closed_calls == 2  # not invoked again

    # Parent cursor still usable after the recovered close.
    cursor.execute("select 7")
    assert cursor.fetchone()[0] == 7


# ── _ArrowReader cancel / cleanup edge cases (coverage-focused) ─────────────
#
# The tests below target the branches of _ArrowReader / arrow_reader() that
# the existing suite doesn't reach — mostly defensive paths on the close /
# cancel side.  They keep the wrapper's public contract locked in so future
# refactors can't silently drop these guarantees:
#
#   * private-attribute lookup on the wrapper raises AttributeError (does not
#     recursively delegate through __getattr__ into self._inner)
#   * re-entering a closed reader as a context manager raises ArrowInvalid
#   * __arrow_c_stream__ fails loudly when the wrapped pyarrow reader lacks
#     the PyCapsule protocol (pyarrow < 14)
#   * __del__ is a no-op during interpreter finalization (module globals may
#     be gone) and never propagates an exception
#   * the batch-generator finally block is symmetric across every teardown
#     path — parent-cursor-already-closed, diag-drain failure, SQL_CLOSE
#     failure, bookkeeping-reset failure — so a single ODBC glitch cannot
#     leak the server-side cursor or crash close()


def test_arrow_reader_propagates_fetch_error_when_parent_cursor_is_closed(conn_str):
    """Closing the parent cursor must not turn the next fetch error into end-of-stream."""
    conn = mssql_python.connect(conn_str)
    try:
        tmp = conn.cursor()
        reader = tmp.execute("select top 5 1 a from sys.objects").arrow_reader(batch_size=2)
        _ = reader.read_next_batch()
        tmp.close()

        with pytest.raises(mssql_python.ProgrammingError, match="cursor is closed"):
            reader.read_next_batch()
    finally:
        conn.close()


def test_arrow_reader_getattr_refuses_private_names(cursor: mssql_python.Cursor):
    """__getattr__ refuses leading-underscore names so a partially-constructed
    instance during __del__ cannot recurse forever trying to resolve its own
    slot names via self._inner."""
    reader = cursor.execute("select 1 a").arrow_reader(batch_size=10)
    try:
        with pytest.raises(AttributeError):
            _ = reader._does_not_exist_anywhere
    finally:
        reader.close()


def test_arrow_reader_enter_after_close_raises(cursor: mssql_python.Cursor):
    """Using a closed reader as a context manager must raise ArrowInvalid;
    __enter__ refuses to hand out a reader that's already been torn down."""
    reader = cursor.execute("select 1 a").arrow_reader(batch_size=10)
    reader.close()
    with pytest.raises(pa.ArrowInvalid):
        with reader:
            pass


def test_arrow_reader_pycapsule_missing_on_inner_raises(cursor: mssql_python.Cursor):
    """If the wrapped pyarrow reader lacks ``__arrow_c_stream__`` (pyarrow < 14),
    the wrapper must raise ``ArrowInvalid`` rather than silently returning
    something invalid.  Simulated here by swapping ``_inner`` for an object
    that does not implement the protocol."""
    reader = cursor.execute("select 1 a").arrow_reader(batch_size=10)
    try:

        class WithoutProtocol:
            pass

        reader._inner = WithoutProtocol()
        with pytest.raises(pa.ArrowInvalid, match=r"pyarrow>=14"):
            reader.__arrow_c_stream__()
    finally:
        # close() only touches _generator / _cursor; the swapped _inner is
        # safe.  It is nulled out at the end of close() anyway.
        reader.close()


def test_arrow_reader_del_skips_during_interpreter_finalization(
    cursor: mssql_python.Cursor, monkeypatch
):
    """``__del__`` must return early when ``sys.is_finalizing()`` is True —
    module globals (pyarrow, ddbc_bindings) may already be torn down and
    touching native code at that point is unsafe.  With the guard active,
    close() is *not* invoked, so ``_generator`` stays set."""
    import sys as _sys

    reader = cursor.execute("select 1 a").arrow_reader(batch_size=10)
    try:
        monkeypatch.setattr(_sys, "is_finalizing", lambda: True)
        reader.__del__()
        assert (
            reader._generator is not None
        ), "__del__ ran close() during simulated interpreter finalization"
    finally:
        monkeypatch.undo()
        reader.close()


def test_arrow_reader_del_swallows_exceptions(cursor: mssql_python.Cursor, monkeypatch):
    """``__del__`` is best-effort — any exception raised inside it (e.g.
    because module globals were already torn down) must be swallowed rather
    than propagating out of a finalizer, where it would only be printed as
    an unraisable warning at best and abort the interpreter at worst."""
    import sys as _sys

    reader = cursor.execute("select 1 a").arrow_reader(batch_size=10)
    try:

        def boom():
            raise RuntimeError("simulated shutdown noise")

        monkeypatch.setattr(_sys, "is_finalizing", boom)
        # Must not propagate.
        reader.__del__()
    finally:
        monkeypatch.undo()
        reader.close()


def test_arrow_reader_cleanup_no_op_when_parent_cursor_already_closed(conn_str):
    """If the parent ``Cursor`` is closed before the reader's cleanup
    generator runs, the ``finally`` block must safely return without
    touching the freed HSTMT — covers the
    ``(cur.closed or cur.hstmt is None)`` early-return.

    Uses a dedicated connection so the shared module-scoped ``cursor``
    fixture is not affected and cannot race an 'active result set' error."""
    conn = mssql_python.connect(conn_str)
    try:
        tmp = conn.cursor()
        reader = tmp.execute("select top 5 1 a from sys.objects").arrow_reader(batch_size=2)
        _ = reader.read_next_batch()
        tmp.close()  # frees hstmt before the reader's finally block runs
        reader.close()  # must not raise
        assert reader.closed is True
    finally:
        conn.close()


def test_arrow_reader_cleanup_survives_diag_drain_failure(conn_str, monkeypatch):
    """If ``DDBCSQLGetAllDiagRecords`` raises (either the pre-close drain or
    the post-close SQL_SUCCESS_WITH_INFO drain), the cleanup path swallows
    the exception and continues rather than propagating and skipping the
    remaining teardown steps."""
    from mssql_python import cursor as cursor_mod

    conn = mssql_python.connect(conn_str)
    try:
        tmp = conn.cursor()
        reader = tmp.execute("select top 5 1 a from sys.objects").arrow_reader(batch_size=2)
        _ = reader.read_next_batch()

        def raise_drain(_hstmt):
            raise RuntimeError("simulated ODBC diag failure")

        monkeypatch.setattr(cursor_mod.ddbc_bindings, "DDBCSQLGetAllDiagRecords", raise_drain)
        reader.close()  # must not propagate
        assert reader.closed is True
        monkeypatch.undo()
        tmp.close()
    finally:
        conn.close()


def test_arrow_reader_cleanup_survives_close_cursor_failure(conn_str, monkeypatch):
    """If ``hstmt._close_cursor()`` raises inside the cleanup generator, the
    handler swallows and continues to the bookkeeping-reset step.  A
    ``SQLFreeStmt(SQL_CLOSE)`` failure must be a WARNING, not a crash."""
    from mssql_python import cursor as cursor_mod

    conn = mssql_python.connect(conn_str)
    try:
        tmp = conn.cursor()
        reader = tmp.execute("select top 5 1 a from sys.objects").arrow_reader(batch_size=2)
        _ = reader.read_next_batch()

        real_hstmt = tmp.hstmt

        class FakeHstmt:
            # reader.close() invokes SQLCancel via hstmt._cancel() before it
            # closes the generator; provide a no-op so we exercise the SQL_CLOSE
            # path specifically.
            def _cancel(self_inner):
                pass

            def _close_cursor(self_inner):
                raise RuntimeError("simulated SQLFreeStmt failure")

        tmp.hstmt = FakeHstmt()
        # Neutralize the diag drain — it would crash on our fake hstmt.
        monkeypatch.setattr(cursor_mod.ddbc_bindings, "DDBCSQLGetAllDiagRecords", lambda _h: [])
        try:
            reader.close()
            assert reader.closed is True
        finally:
            # Restore the real hstmt before closing tmp so ODBC teardown is clean.
            tmp.hstmt = real_hstmt
            monkeypatch.undo()
            tmp.close()
    finally:
        conn.close()


def test_arrow_reader_cleanup_survives_bookkeeping_reset_failure(conn_str, monkeypatch):
    """If ``Cursor._clear_rownumber()`` raises, cleanup swallows and completes.
    A bookkeeping glitch must not leave the reader in an inconsistent
    'closed but generator alive' state."""
    conn = mssql_python.connect(conn_str)
    try:
        tmp = conn.cursor()
        reader = tmp.execute("select top 5 1 a from sys.objects").arrow_reader(batch_size=2)
        _ = reader.read_next_batch()

        def raise_clear():
            raise RuntimeError("simulated bookkeeping failure")

        monkeypatch.setattr(tmp, "_clear_rownumber", raise_clear)
        reader.close()
        assert reader.closed is True
        monkeypatch.undo()
        tmp.close()
    finally:
        conn.close()


def test_arrow_long_string(cursor: mssql_python.Cursor):
    "Make sure resizing the data buffer works"
    long_string = "A" * 100000  # 100k characters
    cursor.execute("select cast(? as nvarchar(max))", (long_string,))
    batch = cursor.arrow_batch(10)
    assert batch.num_rows == 1
    assert batch.num_columns == 1
    assert batch.column(0).to_pylist() == [long_string]


@pytest.mark.parametrize("sql_type", ["char(32)", "varchar(32)"])
@pytest.mark.parametrize("narrow", [True, False])
def test_arrow_char_utf8_collation_unicode(
    cursor: mssql_python.Cursor, sql_type: str, narrow: bool
):
    table = "#t_arrow_char_decode"
    collation = "Latin1_General_100_CI_AS_SC_UTF8"
    expected = [
        "Grüße",
        "你好😀",
        "こんにちは",
        "Привет",
        "Hello 世界",
        "😀😃😄😁",
        "",
        None,
    ]
    if narrow:
        cursor.connection.setdecoding(mssql_python.SQL_CHAR, ctype=mssql_python.SQL_CHAR)

    try:
        cursor.execute(
            f"create table {table} (id int primary key, v {sql_type} collate {collation})"
        )
    except Exception as exc:
        pytest.skip(f"UTF-8 collation '{collation}' not supported: {exc}")

    try:
        for index, value in enumerate(expected, start=1):
            cursor.execute(f"insert into {table} (id, v) values (?, ?)", index, value)
        tbl = cursor.execute(f"select v from {table} order by id").arrow()
        assert tbl.column(0).type.equals(pa.large_string())
        for expected_val, actual_val in zip(expected, tbl.column(0).to_pylist(), strict=True):
            if actual_val is not None:
                actual_val = actual_val.strip()
            assert expected_val == actual_val
    finally:
        cursor.connection.setdecoding(mssql_python.SQL_CHAR)
        cursor.execute(f"drop table if exists {table}")


@pytest.mark.parametrize("sql_type", ["char(32)", "varchar(32)", "text"])
@pytest.mark.parametrize("narrow", [True, False])
def test_arrow_char_cp1252_collation_unicode(
    cursor: mssql_python.Cursor, sql_type: str, narrow: bool
):
    table = "#t_arrow_char_decode"
    collation = "SQL_Latin1_General_CP1_CI_AS"
    expected = [
        "Grüße",
        "café René!",
        "naïve café",
        "Español",
        "Müller-Öztürk",
        "Françoise",
        "",
        None,
    ]
    if narrow:
        cursor.connection.setdecoding(mssql_python.SQL_CHAR, ctype=mssql_python.SQL_CHAR)

    cursor.execute(f"create table {table} (id int primary key, v {sql_type} collate {collation})")

    try:
        for index, value in enumerate(expected, start=1):
            cursor.execute(f"insert into {table} (id, v) values (?, ?)", index, value)
        tbl = cursor.execute(f"select v from {table} order by id").arrow()
        assert tbl.column(0).type.equals(pa.large_string())
        for expected_val, actual_val in zip(expected, tbl.column(0).to_pylist(), strict=True):
            if actual_val is not None:
                actual_val = actual_val.strip()
            assert expected_val == actual_val
    finally:
        cursor.connection.setdecoding(mssql_python.SQL_CHAR)
        cursor.execute(f"drop table if exists {table}")


def test_rownumber_arrow_batch_interleaved_fetchmany(cursor: mssql_python.Cursor):
    """Verify that arrow_batch and fetchmany can be interleaved
    on the same result set with correct rownumber tracking and values."""
    N = 20
    unions = " union all ".join(f"select {i} as val" for i in range(1, N + 1))
    cursor.execute(f"select val from ({unions}) t order by val")

    batch = cursor.arrow_batch(3)
    assert batch.num_rows == 3
    vals = batch.column(0).to_pylist()
    assert vals == [1, 2, 3]
    assert cursor.rownumber == 2  # 0-based: last row index = 2

    rows = cursor.fetchmany(5)
    assert len(rows) == 5
    vals = [r[0] for r in rows]
    assert vals == [4, 5, 6, 7, 8]
    assert cursor.rownumber == 7

    batch = cursor.arrow_batch(4)
    assert batch.num_rows == 4
    vals = batch.column(0).to_pylist()
    assert vals == [9, 10, 11, 12]
    assert cursor.rownumber == 11

    batch = cursor.arrow_batch(100)
    assert batch.num_rows == 8
    vals = batch.column(0).to_pylist()
    assert vals == [13, 14, 15, 16, 17, 18, 19, 20]
    assert cursor.rownumber == 19


def test_rownumber_interleaved_fetchmany_arrow_fetchone_arrow_fetchall(
    cursor: mssql_python.Cursor,
):
    """Verify mixed fetch APIs can be interleaved on the same result set
    with correct rownumber tracking and values."""
    N = 20
    unions = " union all ".join(f"select {i} as val" for i in range(1, N + 1))
    cursor.execute(f"select val from ({unions}) t order by val")

    rows = cursor.fetchmany(4)
    assert len(rows) == 4
    vals = [r[0] for r in rows]
    assert vals == [1, 2, 3, 4]
    assert cursor.rownumber == 3

    batch = cursor.arrow_batch(3)
    assert batch.num_rows == 3
    vals = batch.column(0).to_pylist()
    assert vals == [5, 6, 7]
    assert cursor.rownumber == 6

    row = cursor.fetchone()
    assert row[0] == 8
    assert cursor.rownumber == 7

    batch = cursor.arrow_batch(5)
    assert batch.num_rows == 5
    vals = batch.column(0).to_pylist()
    assert vals == [9, 10, 11, 12, 13]
    assert cursor.rownumber == 12

    rows = cursor.fetchall()
    vals = [r[0] for r in rows]
    assert vals == [14, 15, 16, 17, 18, 19, 20]
    assert cursor.rownumber == 19


def test_rownumber_interleaved_fetchone_arrow_batch(cursor: mssql_python.Cursor):
    """Verify fetchone followed by arrow_batch returns the remaining rows
    with correct rownumber tracking and values."""
    N = 20
    unions = " union all ".join(f"select {i} as val" for i in range(1, N + 1))
    cursor.execute(f"select val from ({unions}) t order by val")

    row = cursor.fetchone()
    assert row[0] == 1
    assert cursor.rownumber == 0

    batch = cursor.arrow_batch(100)
    assert batch.num_rows == 19
    vals = batch.column(0).to_pylist()
    assert vals == list(range(2, N + 1))
    assert cursor.rownumber == 19


def test_arrow_sql_ss_udt_hierarchyid_fetch_bindcol(cursor: mssql_python.Cursor):
    select_sql = """
        SELECT node
        FROM (
            VALUES
                (1, hierarchyid::Parse('/1/')),
                (2, CAST(NULL AS HIERARCHYID)),
                (3, hierarchyid::Parse('/1/2/3/'))
        ) AS v(id, node)
        ORDER BY id
    """

    expected_rows = cursor.execute(select_sql).fetchall()
    batch = cursor.execute(select_sql).arrow_batch(10)
    assert batch.num_rows == 3
    assert batch.num_columns == 1
    udt_col_index = 0
    udt_column = batch.column(udt_col_index)
    assert udt_column.type.equals(pa.large_binary())
    assert udt_column.to_pylist() == [row[udt_col_index] for row in expected_rows]
    assert udt_column.null_count == 1


def test_arrow_sql_ss_udt_hierarchyid_fetch_getdata(cursor: mssql_python.Cursor):
    select_sql = """
        SELECT filler, node
        FROM (
            VALUES
                (1, CONVERT(VARBINARY(MAX), REPLICATE(CAST('A' AS VARCHAR(MAX)), 9001)), hierarchyid::Parse('/1/')),
                (2, CONVERT(VARBINARY(MAX), REPLICATE(CAST('B' AS VARCHAR(MAX)), 9002)), CAST(NULL AS HIERARCHYID)),
                (3, CONVERT(VARBINARY(MAX), REPLICATE(CAST('C' AS VARCHAR(MAX)), 9003)), hierarchyid::Parse('/1/2/3/'))
        ) AS v(id, filler, node)
        ORDER BY id
    """

    expected_rows = cursor.execute(select_sql).fetchall()
    batch = cursor.execute(select_sql).arrow_batch(10)

    assert batch.num_rows == 3
    assert batch.num_columns == 2
    assert batch.column(0).type.equals(pa.large_binary())
    assert batch.column(0).to_pylist() == [row[0] for row in expected_rows]
    udt_col_index = 1
    udt_column = batch.column(udt_col_index)
    assert udt_column.type.equals(pa.large_binary())
    assert udt_column.to_pylist() == [row[udt_col_index] for row in expected_rows]
    assert udt_column.null_count == 1
