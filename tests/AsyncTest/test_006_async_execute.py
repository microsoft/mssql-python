import asyncio
from datetime import date, datetime, time
from decimal import Decimal
import pytest
from uuid import UUID, uuid4

from mssql_python.constants import ConstantsDDBC

mssql_py_core = pytest.importorskip("mssql_py_core", exc_type=ImportError)

import mssql_python
import mssql_python.async_query as async_query
from mssql_python.async_query import _AsyncConnection  # pyright: ignore[reportPrivateUsage]
from mssql_python.async_query import _AsyncCursor  # pyright: ignore[reportPrivateUsage]
from mssql_python.async_query import async_execute
from mssql_python import DataError, DatabaseError, OperationalError, ProgrammingError
from mssql_python.row import Row


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method", "error", "public_error"),
    (
        (
            "execute",
            TypeError("The SQL contains 2 parameter markers, but 1 parameters were supplied"),
            ProgrammingError,
        ),
        (
            "executemany",
            TypeError("Parameter style mismatch: expected positional parameters"),
            ProgrammingError,
        ),
        (
            "execute",
            RuntimeError("Connection is busy with another cursor operation"),
            OperationalError,
        ),
    ),
)
async def test_rejected_execution_preserves_pending_result_state(method, error, public_error):
    class RejectingNativeCursor:
        description = [("value", int, None, None, None, None, True)]
        rowcount = 1
        arraysize = 1

        async def execute(self, *_args, **_kwargs):
            raise error

        async def executemany(self, *_args, **_kwargs):
            raise error

        async def fetchone(self):
            self.rowcount = 2
            return (2,)

    class StatefulAsyncCursor(_AsyncCursor):
        def seed_result_state(self):
            self._description = [("value", int, None, None, None, None, True)]
            self._column_map = {"value": 0}
            self._fetched_row_count = 1
            self._fetch_rowcount = 1

    cursor = StatefulAsyncCursor(RejectingNativeCursor())
    cursor.seed_result_state()

    with pytest.raises(public_error):
        if method == "execute":
            await cursor.execute("SELECT ?, ?", 1)
        else:
            await cursor.executemany("SELECT ?", [(1,)])

    row = await cursor.fetchone()
    assert row is not None
    assert row == [2]
    assert row.value == 2
    assert cursor.rowcount == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ("execute", "executemany"))
async def test_missing_named_parameter_preserves_result_snapshot(async_cursor, monkeypatch, method):
    monkeypatch.setattr(mssql_python, "lowercase", False)
    monkeypatch.setattr(mssql_python, "native_uuid", False)
    await async_cursor.execute(
        "SELECT CAST('6F9619FF-8B86-D011-B42D-00C04FC964FF' AS UNIQUEIDENTIFIER) "
        "AS MixedGuid FROM (VALUES (1), (2)) AS numbered(ordinal) ORDER BY ordinal"
    )
    first = await async_cursor.fetchone()
    assert first is not None
    assert isinstance(first.MixedGuid, str)
    assert async_cursor.rowcount == 1
    previous_description = async_cursor.description
    previous_generation = getattr(async_cursor, "_result_generation")

    monkeypatch.setattr(mssql_python, "lowercase", True)
    monkeypatch.setattr(mssql_python, "native_uuid", True)
    with pytest.raises(KeyError, match="missing"):
        if method == "execute":
            await async_cursor.execute("SELECT %(missing)s", {"other": 1})
        else:
            await async_cursor.executemany("SELECT %(missing)s", [{"other": 1}])

    assert async_cursor.description is previous_description
    assert getattr(async_cursor, "_result_generation") == previous_generation
    assert async_cursor.rowcount == 1
    second = await async_cursor.fetchone()
    assert second is not None
    assert second.MixedGuid == first.MixedGuid
    assert isinstance(second.MixedGuid, str)
    assert list(second._mapping) == ["MixedGuid"]
    assert async_cursor.rowcount == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ("execute", "executemany"))
async def test_failed_execution_clears_wrapper_state_when_native_discards_result(method):
    class FailingNativeCursor:
        description = [("value", int, None, None, None, None, True)]
        rowcount = 1

        async def execute(self, *_args, **_kwargs):
            self.description = None
            self.rowcount = -1
            raise RuntimeError("execution failed")

        async def executemany(self, *_args, **_kwargs):
            self.description = None
            self.rowcount = -1
            raise RuntimeError("execution failed")

    class StatefulAsyncCursor(_AsyncCursor):
        def seed_result_state(self):
            self._description = [("value", int, None, None, None, None, True)]
            self._column_map = {"value": 0}
            self._fetched_row_count = 1
            self._fetch_rowcount = 1

    cursor = StatefulAsyncCursor(FailingNativeCursor())
    cursor.seed_result_state()

    with pytest.raises(RuntimeError, match="execution failed"):
        if method == "execute":
            await cursor.execute("SELECT 1")
        else:
            await cursor.executemany("SELECT ?", [(1,)])

    assert cursor.description is None
    assert cursor.rowcount == -1


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ("execute", "executemany"))
async def test_cancelled_execution_with_same_state_starts_new_result_generation(method):
    execution_started = asyncio.Event()

    class CancelledNativeCursor:
        description = [("id", int, None, None, None, None, True)]
        rowcount = -1
        value = 1

        async def _execute(self):
            self.description = [("id", int, None, None, None, None, True)]
            self.rowcount = -1
            self.value = 2
            execution_started.set()
            await asyncio.Event().wait()

        async def execute(self, *_args, **_kwargs):
            return await self._execute()

        async def executemany(self, *_args, **_kwargs):
            return await self._execute()

        async def fetchone(self):
            return (self.value,)

    class StatefulAsyncCursor(_AsyncCursor):
        def seed_result_state(self):
            self._description = [("id", int, None, None, None, None, True)]
            self._column_map = {"id": 0}
            self._column_names = ("id",)
            self._fetched_row_count = 1
            self._fetch_rowcount = 1

    cursor = StatefulAsyncCursor(CancelledNativeCursor())
    cursor.seed_result_state()

    execution = (
        cursor.execute("SELECT id")
        if method == "execute"
        else cursor.executemany("SELECT id", [()])
    )
    execution_task = asyncio.create_task(execution)
    await execution_started.wait()

    execution_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await execution_task

    row = await cursor.fetchone()
    assert row is not None
    assert row.id == 2
    assert cursor.rowcount == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ("execute", "executemany"))
async def test_failed_execution_with_same_state_starts_new_result_generation(method):
    class FailingNativeCursor:
        description = [("id", int, None, None, None, None, True)]
        rowcount = -1
        value = 1

        async def _execute(self):
            self.description = [("id", int, None, None, None, None, True)]
            self.rowcount = -1
            self.value = 2
            raise RuntimeError("execution failed")

        async def execute(self, *_args, **_kwargs):
            return await self._execute()

        async def executemany(self, *_args, **_kwargs):
            return await self._execute()

        async def fetchone(self):
            return (self.value,)

    class StatefulAsyncCursor(_AsyncCursor):
        def seed_result_state(self):
            self._description = [("id", int, None, None, None, None, True)]
            self._column_map = {"id": 0}
            self._column_names = ("id",)
            self._fetched_row_count = 1
            self._fetch_rowcount = 1

    cursor = StatefulAsyncCursor(FailingNativeCursor())
    cursor.seed_result_state()

    with pytest.raises(RuntimeError, match="execution failed"):
        if method == "execute":
            await cursor.execute("SELECT id")
        else:
            await cursor.executemany("SELECT id", [()])

    row = await cursor.fetchone()
    assert row is not None
    assert row.id == 2
    assert cursor.rowcount == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ("execute", "executemany"))
async def test_reconciliation_failure_preserves_original_execution_error(method):
    native_error = mssql_py_core.DatabaseError("query failed")
    native_error.sql_errors = [{"number": 50001}]

    class BrokenNativeCursor:
        broken = False

        @property
        def description(self):
            if self.broken:
                raise RuntimeError("Connection is broken")
            return [("value", int, None, None, None, None, True)]

        @property
        def rowcount(self):
            return -1 if self.broken else 1

        async def execute(self, *_args, **_kwargs):
            self.broken = True
            raise native_error

        async def executemany(self, *_args, **_kwargs):
            self.broken = True
            raise native_error

    cursor = _AsyncCursor(BrokenNativeCursor())

    with pytest.raises(DatabaseError) as caught:
        if method == "execute":
            await cursor.execute("SELECT 1")
        else:
            await cursor.executemany("SELECT ?", [(1,)])

    assert caught.value.__cause__ is native_error
    assert getattr(caught.value, "sql_errors") == native_error.sql_errors
    assert cursor.description is None


@pytest.mark.asyncio
@pytest.mark.parametrize("use_prepare", (True, False))
async def test_execute_returns_public_cursor_and_binds_parameters(
    async_connection,
    use_prepare,
):
    assert isinstance(async_connection, _AsyncConnection)

    cursor = async_connection.cursor()
    try:
        assert isinstance(cursor, _AsyncCursor)

        result = await cursor.execute(
            "IF CAST(? AS INT) <> 7 THROW 50000, 'Unexpected parameter value', 1",
            7,
            use_prepare=use_prepare,
            reset_cursor=False,
        )

        assert result is cursor
    finally:
        await cursor.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("parameters", ((1, 2), [1, 2]))
@pytest.mark.parametrize("use_prepare", (True, False))
async def test_execute_accepts_single_parameter_sequence(
    async_cursor,
    parameters,
    use_prepare,
):
    await async_cursor.execute(
        "IF CAST(? AS INT) <> 1 OR CAST(? AS INT) <> 2 "
        "THROW 50000, 'Unexpected parameter values', 1",
        parameters,
        use_prepare=use_prepare,
    )


@pytest.mark.asyncio
async def test_execute_accepts_named_parameters(async_cursor):
    result = await async_cursor.execute(
        "IF CAST(%(first)s AS INT) <> 1 OR CAST(%(second)s AS INT) <> 2 "
        "THROW 50000, 'Unexpected parameter values', 1",
        {"first": 1, "second": 2},
    )

    assert result is async_cursor


@pytest.mark.asyncio
async def test_execute_accepts_dbapi_row(async_cursor, monkeypatch):
    row = Row([1, 2], {"first": 0, "second": 1})
    log_calls = []
    monkeypatch.setattr(
        async_execute.logger,
        "debug",
        lambda message, *args: log_calls.append((message, args)),
    )

    result = await async_cursor.execute(
        "IF CAST(? AS INT) <> 1 OR CAST(? AS INT) <> 2 "
        "THROW 50000, 'Unexpected parameter values', 1",
        row,
    )

    assert result is async_cursor
    assert log_calls[0][1][0] == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("use_prepare", (True, False))
@pytest.mark.parametrize(
    "operation, rows",
    (
        ("INSERT INTO {table} (id, value) VALUES (?, ?)", [(1, "one"), (2, "two")]),
        (
            "INSERT INTO {table} (id, value) VALUES (%(id)s, %(value)s)",
            [{"id": 1, "value": "one"}, {"id": 2, "value": "two"}],
        ),
    ),
)
async def test_executemany_matches_sync_contract(
    async_connection,
    operation,
    rows,
    use_prepare,
):
    cursor = async_connection.cursor()
    table_name = f"async_execute_test_{uuid4().hex}"
    try:
        await cursor.execute(
            f"CREATE TABLE {table_name} (id INT NOT NULL, value NVARCHAR(20) NOT NULL)"
        )
        result = await cursor.executemany(
            operation.format(table=table_name),
            rows,
            use_prepare=use_prepare,
        )
        assert result is None
        assert cursor.rowcount == 2
        await cursor.execute(f"SELECT id, value FROM {table_name} ORDER BY id")
        assert [tuple(row) for row in await cursor.fetchall()] == [(1, "one"), (2, "two")]
    finally:
        await cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        await cursor.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("parameters", (None, 42))
async def test_executemany_rejects_non_iterable(async_cursor, parameters):
    with pytest.raises(TypeError):
        await async_cursor.executemany("SELECT CAST(? AS INT)", parameters)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "value, sql_type",
    (
        (True, "BIT"),
        (-(2**63), "BIGINT"),
        (2**63 - 1, "BIGINT"),
        (3.25, "FLOAT"),
        ("hello\x00world", "NVARCHAR(20)"),
        (b"\x00\x01\xff", "VARBINARY(20)"),
        (Decimal("123.45"), "DECIMAL(10, 2)"),
        (date(2024, 1, 2), "DATE"),
        (time(12, 34, 56, 123456), "TIME(6)"),
        (datetime(2024, 1, 2, 12, 34, 56, 123456), "DATETIME2(6)"),
        (UUID("6f9619ff-8b86-d011-b42d-00c04fc964ff"), "UNIQUEIDENTIFIER"),
    ),
)
async def test_execute_binds_representative_sync_parameter_types(async_cursor, value, sql_type):
    result = await async_cursor.execute(
        f"SELECT CAST(? AS {sql_type}) AS value",
        value,
    )
    row = await async_cursor.fetchone()

    assert result is async_cursor
    assert row is not None
    assert tuple(row) == (value,)


@pytest.mark.asyncio
async def test_execute_does_not_mutate_caller_parameter_list(async_cursor):
    parameters = ["hello", 42, Decimal("3.14"), date(2024, 1, 1)]
    snapshot = list(parameters)

    await async_cursor.execute(
        "IF CAST(? AS NVARCHAR(10)) <> 'hello' OR CAST(? AS INT) <> 42 "
        "OR CAST(? AS DECIMAL(4, 2)) <> 3.14 OR CAST(? AS DATE) <> '2024-01-01' "
        "THROW 50000, 'Unexpected parameter values', 1",
        parameters,
    )

    assert parameters == snapshot


@pytest.mark.asyncio
@pytest.mark.parametrize("value", ({1, 2, 3}, Decimal("NaN"), Decimal("Infinity")))
async def test_execute_rejects_unsupported_or_non_finite_parameters(async_cursor, value):
    with pytest.raises((TypeError, ValueError)):
        await async_cursor.execute("SELECT ?", value)


@pytest.mark.asyncio
async def test_execute_reset_cursor_false_supports_repeated_execution(async_cursor):
    for value in range(5):
        result = await async_cursor.execute(
            "IF CAST(? AS INT) < 0 THROW 50000, 'Unexpected parameter value', 1",
            value,
            reset_cursor=value == 0,
        )

        assert result is async_cursor


@pytest.mark.asyncio
@pytest.mark.parametrize("use_prepare", (False, True))
@pytest.mark.parametrize("reset_cursor", (False, True))
async def test_string_execution_reuse_preserves_values_and_lengths(
    async_cursor, use_prepare, reset_cursor
):
    values = ["", None, "a\x00b", "caf\u00e9", "\u4e2d\u6587", "e\u0301"]
    values.extend("x" * size for size in (3999, 4000, 4001, 7999, 8000, 8001))
    values.extend(
        (
            "\U0001f600" * 1999 + "x",
            "\U0001f600" * 2000,
            "\U0001f600" * 2000 + "x",
            "short again  ",
            None,
            "",
        )
    )
    for value in values:
        await async_cursor.execute(
            "SELECT CAST(? AS NVARCHAR(MAX)) AS value, "
            "DATALENGTH(CAST(? AS NVARCHAR(MAX))) AS byte_length",
            value,
            value,
            use_prepare=use_prepare,
            reset_cursor=reset_cursor,
        )
        row = await async_cursor.fetchone()
        assert row is not None
        assert row[0] == value
        assert row[1] == (None if value is None else len(value.encode("utf-16-le")))
        assert async_cursor.description is not None
        assert async_cursor.description[0][1] is str


@pytest.mark.asyncio
@pytest.mark.parametrize("use_prepare", (False, True))
@pytest.mark.parametrize("mapped", (False, True))
async def test_string_batches_match_individual_execution(async_cursor, use_prepare, mapped):
    values = [
        None,
        "",
        " ",
        "\t\n",
        "a\x00b",
        "caf\u00e9",
        "\u4e2d\u6587",
        "\U0001f600",
        "\u03a9" * 4001,
        "x" * 8001,
        "end  ",
    ]
    operation = (
        "SELECT CAST(%(value)s AS NVARCHAR(MAX)) AS value"
        if mapped
        else "SELECT CAST(? AS NVARCHAR(MAX)) AS value"
    )
    individual = []
    for value in values:
        parameters = {"value": value} if mapped else (value,)
        await async_cursor.execute(operation, parameters, use_prepare=use_prepare)
        individual.append(tuple(await async_cursor.fetchone()))

    rows = ({"value": value} if mapped else (value,) for value in values)
    assert await async_cursor.executemany(operation, rows, use_prepare=use_prepare) is None
    for index, value in enumerate(values):
        assert async_cursor.rowcount == -1
        assert [tuple(row) for row in await async_cursor.fetchall()] == [(value,)]
        assert individual[index] == (value,)
        assert async_cursor.rowcount == 1
        assert await async_cursor.nextset() is (index + 1 < len(values))


@pytest.mark.asyncio
@pytest.mark.parametrize("use_prepare", (False, True))
@pytest.mark.parametrize(
    ("hint", "expected_type", "values"),
    (
        ((-9, 100, 0), "nvarchar", (None, "", "\u4e2d\u6587\U0001f600  ")),
        ((12, 100, 0), "varchar", (None, "", "caf\u00e9  ")),
        ((-8, 100, 0), "nchar", (None, "", "\u4e2d\u6587\U0001f600  ")),
        ((1, 100, 0), "char", (None, "", "caf\u00e9  ")),
    ),
)
async def test_string_setinputsizes_type_null_and_consumption(
    async_cursor, use_prepare, hint, expected_type, values
):
    for value in values:
        async_cursor.setinputsizes([hint, hint])
        await async_cursor.execute(
            "SELECT ?, CONVERT(VARCHAR(30), SQL_VARIANT_PROPERTY(?, 'BaseType'))",
            value,
            value,
            use_prepare=use_prepare,
        )
        row = await async_cursor.fetchone()
        expected_value = value
        if value is not None and expected_type in ("char", "nchar"):
            units = len(value.encode("utf-16-le")) // 2 if expected_type == "nchar" else len(value)
            expected_value = value + " " * (100 - units)
        assert tuple(row) == (expected_value, None if value is None else expected_type)
        assert async_cursor.description[0][1] is str
    await async_cursor.execute(
        "SELECT CONVERT(VARCHAR(30), SQL_VARIANT_PROPERTY(?, 'BaseType'))",
        42,
        use_prepare=use_prepare,
    )
    assert (await async_cursor.fetchone())[0] == "tinyint"


@pytest.mark.asyncio
@pytest.mark.parametrize("use_prepare", (False, True))
async def test_string_setinputsizes_preserved_after_local_rejection(async_cursor, use_prepare):
    async_cursor.setinputsizes([(12, 100, 0)])
    with pytest.raises(TypeError, match="Failed to convert parameter to string"):
        await async_cursor.execute("SELECT ?", object(), use_prepare=use_prepare)
    await async_cursor.execute(
        "SELECT CONVERT(VARCHAR(30), SQL_VARIANT_PROPERTY(?, 'BaseType'))",
        "retained",
        use_prepare=use_prepare,
    )
    assert (await async_cursor.fetchone())[0] == "varchar"


@pytest.mark.asyncio
@pytest.mark.parametrize("use_prepare", (False, True))
@pytest.mark.parametrize("batch", (False, True))
@pytest.mark.parametrize("value_kind", ("null", "empty", "large"))
@pytest.mark.parametrize(
    ("hint", "sql_type", "encoding"),
    (
        (-1, "VARCHAR(MAX)", "ascii"),
        (-10, "NVARCHAR(MAX)", "utf-16-le"),
    ),
)
async def test_string_long_hints_preserve_null_and_payload(
    async_cursor, use_prepare, batch, value_kind, hint, sql_type, encoding
):
    value = None if value_kind == "null" else ""
    if value_kind == "large":
        value = "x" * 8001 + "  " if encoding == "ascii" else "\U0001f600" * 2001 + "  "
    await async_cursor.execute(
        f"CREATE TABLE #async_long_hint (id INT, value {sql_type})", use_prepare=False
    )
    try:
        async_cursor.setinputsizes([4, hint])
        try:
            if batch:
                await async_cursor.executemany(
                    "INSERT INTO #async_long_hint VALUES (?, ?)",
                    [(1, value)],
                    use_prepare=use_prepare,
                )
            else:
                await async_cursor.execute(
                    "INSERT INTO #async_long_hint VALUES (?, ?)",
                    1,
                    value,
                    use_prepare=use_prepare,
                )
        except DatabaseError as error:
            if not (
                hint == -10
                and isinstance(error.__cause__, mssql_py_core.DatabaseError)
                and [item["number"] for item in getattr(error, "sql_errors", [])] == [4002]
            ):
                raise
            async_cursor.setinputsizes(None)
            await async_cursor.execute("SELECT 1")
            assert tuple(await async_cursor.fetchone()) == (1,)
            pytest.xfail("py-core SQL_WLONGVARCHAR emits invalid TDS (SQL 4002)")
        assert async_cursor.rowcount == 1
        await async_cursor.execute(
            "SELECT value, DATALENGTH(value) FROM #async_long_hint ORDER BY id"
        )
        assert [tuple(row) for row in await async_cursor.fetchall()] == [
            (value, None if value is None else len(value.encode(encoding))),
        ]
    finally:
        async_cursor.setinputsizes(None)
        await async_cursor.execute("DROP TABLE IF EXISTS #async_long_hint", use_prepare=False)


@pytest.mark.asyncio
@pytest.mark.parametrize("use_prepare", (False, True))
@pytest.mark.parametrize(
    ("sql_type", "capacity", "encoding"),
    (("VARCHAR(8000)", 8000, "ascii"), ("NVARCHAR(4000)", 4000, "utf-16-le")),
)
async def test_string_column_capacity_and_overflow(
    async_cursor, use_prepare, sql_type, capacity, encoding
):
    values = ["x" * (capacity - 1), "x" * capacity]
    if encoding == "utf-16-le":
        values.append("\U0001f600" * (capacity // 2))
    await async_cursor.execute(
        f"CREATE TABLE #async_capacity (id INT, value {sql_type})", use_prepare=False
    )
    try:
        await async_cursor.executemany(
            "INSERT INTO #async_capacity VALUES (?, ?)", enumerate(values), use_prepare=use_prepare
        )
        with pytest.raises(DataError):
            await async_cursor.execute(
                "INSERT INTO #async_capacity VALUES (?, ?)",
                len(values),
                "x" * (capacity + 1),
                use_prepare=use_prepare,
            )
        await async_cursor.execute(
            "SELECT value, DATALENGTH(value) FROM #async_capacity ORDER BY id"
        )
        assert [tuple(row) for row in await async_cursor.fetchall()] == [
            (value, len(value.encode(encoding))) for value in values
        ]
    finally:
        await async_cursor.execute("DROP TABLE IF EXISTS #async_capacity", use_prepare=False)


@pytest.mark.asyncio
async def test_execute_updates_rowcount_and_description(async_cursor):
    table_name = f"async_execute_state_{uuid4().hex}"
    try:
        await async_cursor.execute(f"CREATE TABLE {table_name} (value INT)")
        await async_cursor.execute(f"INSERT INTO {table_name} VALUES (1), (2), (3)")
        assert async_cursor.rowcount == 3
        assert async_cursor.description is None

        await async_cursor.execute(f"SELECT value AS named_value FROM {table_name}")
        assert async_cursor.rowcount == -1
        description = async_cursor.description
        assert description is not None
        assert description[0][0] == "named_value"
    finally:
        await async_cursor.execute(f"DROP TABLE IF EXISTS {table_name}")


@pytest.mark.asyncio
async def test_execute_repeated_null_parameters(async_cursor):
    table_name = f"async_execute_nulls_{uuid4().hex}"
    try:
        await async_cursor.execute(f"CREATE TABLE {table_name} (id INT, value VARCHAR(20))")
        for identifier in range(1, 4):
            await async_cursor.execute(
                f"INSERT INTO {table_name} VALUES (?, ?)",
                identifier,
                None,
                reset_cursor=identifier == 1,
            )

        assert async_cursor.rowcount == 1
        await async_cursor.execute(
            f"IF (SELECT COUNT(*) FROM {table_name} WHERE value IS NULL) <> 3 "
            "THROW 50000, 'Unexpected NULL count', 1"
        )
    finally:
        await async_cursor.execute(f"DROP TABLE IF EXISTS {table_name}")


@pytest.mark.asyncio
async def test_executemany_empty_sequence_sets_rowcount_zero(async_cursor):
    result = await async_cursor.executemany("SELECT CAST(? AS INT)", [])

    assert result is None
    assert async_cursor.rowcount == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("mapped", (False, True))
@pytest.mark.parametrize("use_prepare", (False, True))
async def test_executemany_consumes_iterable_once(async_cursor, mapped, use_prepare):
    consumed = []

    class ParameterRows:
        def __iter__(self):
            assert not consumed
            for value in (1, 2):
                consumed.append(value)
                yield {"value": value} if mapped else (value,)

        def __len__(self):
            raise AssertionError("Parameter iterable must not require a length")

    operation = "SELECT CAST(%(value)s AS INT)" if mapped else "SELECT CAST(? AS INT)"
    assert (
        await async_cursor.executemany(operation, ParameterRows(), use_prepare=use_prepare) is None
    )
    assert consumed == [1, 2]
    assert await async_cursor.fetchall() == [[1]]
    assert await async_cursor.nextset() is True
    assert await async_cursor.fetchall() == [[2]]
    assert await async_cursor.nextset() is False


@pytest.mark.asyncio
async def test_executemany_empty_generator(async_cursor):
    assert await async_cursor.executemany("SELECT ?", (row for row in ())) is None
    assert async_cursor.rowcount == 0
    assert async_cursor.description is None


@pytest.mark.asyncio
@pytest.mark.parametrize("use_prepare", (False, True))
@pytest.mark.parametrize("yield_before_failure", (False, True))
@pytest.mark.parametrize(
    "failure", (ValueError("iteration failed"), RuntimeError("iteration failed"))
)
async def test_executemany_iterator_failure_preserves_result(
    async_cursor, failure, yield_before_failure, use_prepare
):
    def rows():
        if yield_before_failure:
            yield (3,)
        raise failure

    await async_cursor.execute(
        "CREATE TABLE #async_iterator_failure (value INT)", use_prepare=False
    )
    try:
        await async_cursor.execute("SELECT 1 AS value UNION ALL SELECT 2 ORDER BY value")
        assert await async_cursor.fetchone() == [1]
        description = async_cursor.description

        with pytest.raises(type(failure)) as caught:
            await async_cursor.executemany(
                "INSERT INTO #async_iterator_failure VALUES (?)",
                rows(),
                use_prepare=use_prepare,
            )
        assert caught.value is failure
        assert async_cursor.description is description
        assert async_cursor.rowcount == 1
        assert await async_cursor.fetchone() == [2]
        assert async_cursor.rowcount == 2
        await async_cursor.execute("SELECT COUNT(*) FROM #async_iterator_failure")
        assert await async_cursor.fetchone() == [0]
    finally:
        await async_cursor.execute(
            "DROP TABLE IF EXISTS #async_iterator_failure", use_prepare=False
        )


@pytest.mark.asyncio
async def test_executemany_handles_sync_edge_value_batches(async_cursor):
    table_name = f"async_many_values_{uuid4().hex}"
    try:
        await async_cursor.execute(
            f"CREATE TABLE {table_name} ("
            "id INT, text_value NVARCHAR(50), binary_value VARBINARY(20), "
            "integer_value BIGINT, decimal_value DECIMAL(18, 10), date_value DATE)"
        )
        rows = [
            (1, "", b"", -(2**63), Decimal("-1.25"), date(2024, 1, 1)),
            (2, None, None, 0, None, None),
            (
                3,
                "unicode-\u03bb",
                b"\x00\xff",
                2**63 - 1,
                Decimal("999.99"),
                date(2024, 1, 3),
            ),
        ]
        async_cursor.setinputsizes(
            [
                ConstantsDDBC.SQL_INTEGER.value,
                (ConstantsDDBC.SQL_WVARCHAR.value, 50, 0),
                (ConstantsDDBC.SQL_VARBINARY.value, 20, 0),
                ConstantsDDBC.SQL_BIGINT.value,
                (ConstantsDDBC.SQL_DECIMAL.value, 18, 10),
                ConstantsDDBC.SQL_TYPE_DATE.value,
            ]
        )

        result = await async_cursor.executemany(
            f"INSERT INTO {table_name} VALUES (?, ?, ?, ?, ?, ?)",
            rows,
        )

        assert result is None
        assert async_cursor.rowcount == len(rows)
        await async_cursor.execute(
            f"SELECT id, text_value, binary_value, integer_value, decimal_value, date_value "
            f"FROM {table_name} ORDER BY id"
        )
        assert [tuple(row) for row in await async_cursor.fetchall()] == rows
    finally:
        async_cursor.setinputsizes(None)
        await async_cursor.execute(f"DROP TABLE IF EXISTS {table_name}")


@pytest.mark.asyncio
@pytest.mark.parametrize("use_prepare", (False, True))
@pytest.mark.parametrize("batch", (False, True))
@pytest.mark.parametrize(
    "rows",
    (
        [(1, "first"), (2, None)],
        [],
        None,
        [(1, ""), (2, "\u4e2d\u6587  "), (3, "\U0001f600" * 25), (4, "a\x00b")],
    ),
)
async def test_async_tvp_execute_and_executemany(
    async_cursor, async_connection, use_prepare, batch, rows
):
    type_name = f"dbo.AsyncTvp_{uuid4().hex}"
    await async_cursor.execute(f"CREATE TYPE {type_name} AS TABLE (id INT, value NVARCHAR(50))")
    try:
        await async_connection.commit()
        value = async_query._TableValuedParameter(
            type_name,
            [(4, 0, 0), (-9, 50, 0)] if rows is not None else None,
            rows,
        )
        operation = "SELECT id, value FROM ? ORDER BY id"
        if batch:
            assert (
                await async_cursor.executemany(
                    operation, ((value,) for _ in range(2)), use_prepare=use_prepare
                )
                is None
            )
        else:
            assert (
                await async_cursor.execute(operation, value, use_prepare=use_prepare)
                is async_cursor
            )
        assert [tuple(row) for row in await async_cursor.fetchall()] == (rows or [])
        if batch:
            assert await async_cursor.nextset() is True
            assert [tuple(row) for row in await async_cursor.fetchall()] == (rows or [])
        assert await async_cursor.nextset() is False
    finally:
        await async_cursor.execute(f"DROP TYPE IF EXISTS {type_name}")
        await async_connection.commit()


@pytest.mark.asyncio
@pytest.mark.parametrize("use_prepare", (False, True))
@pytest.mark.parametrize(
    ("token", "sql_type", "value", "operation", "expected"),
    (
        (
            "SQL_MONEY",
            "money",
            Decimal("123.45"),
            "SELECT SQL_VARIANT_PROPERTY(CAST(? AS sql_variant), 'BaseType')",
            "money",
        ),
        (
            "SQL_SMALLMONEY",
            "smallmoney",
            Decimal("123.45"),
            "SELECT SQL_VARIANT_PROPERTY(CAST(? AS sql_variant), 'BaseType')",
            "smallmoney",
        ),
        (
            "SQL_XML",
            "xml",
            "<root />",
            "DECLARE @value xml = ?; SELECT @value.exist('/root')",
            1,
        ),
        (
            "SQL_JSON",
            "json",
            {"answer": 42},
            "SELECT JSON_VALUE(%(payload)s, '$.answer')",
            "42",
        ),
        (
            "SQL_VECTOR",
            "vector",
            [1.0, 2.0, 3.0],
            "SELECT VECTOR_DISTANCE('euclidean', ?, CAST('[1,2,3]' AS VECTOR(3)))",
            0.0,
        ),
    ),
)
async def test_async_exported_type_hints(
    async_cursor, use_prepare, token, sql_type, value, operation, expected
):
    if sql_type in ("json", "vector"):
        await async_cursor.execute(f"SELECT TYPE_ID('{sql_type}')")
        row = await async_cursor.fetchone()
        assert row is not None
        if row[0] is None:
            pytest.skip(f"SQL Server does not expose the {sql_type} type")
    hint = getattr(async_query, token)
    async_cursor.setinputsizes([(hint, 3, 0)] if sql_type == "vector" else [hint])
    try:
        parameters = {"payload": value} if sql_type == "json" else (value,)
        await async_cursor.execute(operation, parameters, use_prepare=use_prepare)
        row = await async_cursor.fetchone()
        assert row is not None
        assert row[0] == expected
    finally:
        async_cursor.setinputsizes(None)


@pytest.mark.asyncio
async def test_executemany_handles_multiple_all_null_columns(async_cursor):
    table_name = f"async_many_nulls_{uuid4().hex}"
    try:
        await async_cursor.execute(
            f"CREATE TABLE {table_name} (id INT, text_value VARCHAR(20), number_value INT)"
        )
        rows = [(1, None, None), (2, None, None), (3, None, None)]

        await async_cursor.executemany(f"INSERT INTO {table_name} VALUES (?, ?, ?)", rows)

        assert async_cursor.rowcount == len(rows)
        await async_cursor.execute(
            f"IF (SELECT COUNT(*) FROM {table_name} "
            "WHERE text_value IS NULL AND number_value IS NULL) <> 3 "
            "THROW 50000, 'Unexpected NULL values', 1"
        )
    finally:
        await async_cursor.execute(f"DROP TABLE IF EXISTS {table_name}")


@pytest.mark.asyncio
@pytest.mark.parametrize("use_prepare", (False, True))
@pytest.mark.parametrize("batch", (False, True))
@pytest.mark.parametrize("named", (False, True))
@pytest.mark.parametrize(
    ("sql_type", "unicode_values"),
    (
        ("VARCHAR(128)", False),
        ("NVARCHAR(128)", True),
        ("VARCHAR(MAX)", False),
        ("NVARCHAR(MAX)", True),
        ("TEXT", False),
        ("NTEXT", True),
    ),
)
async def test_string_roundtrip_matrix(
    async_cursor, use_prepare, batch, named, sql_type, unicode_values
):
    class StringValue(str):
        pass

    values = [None, "", " ", "\t\r\n", "a\x00b", "trailing  ", StringValue("subclass")]
    if unicode_values:
        values.extend(["caf\u00e9", "\u4e2d\u6587", "\U0001f600\U0001f680", "e\u0301"])
    rows = list(enumerate(values))
    parameters = (
        [{"id": identifier, "value": value} for identifier, value in rows] if named else rows
    )
    markers = "%(id)s, %(value)s" if named else "?, ?"
    await async_cursor.execute(
        f"CREATE TABLE #async_strings (id INT, value {sql_type})", use_prepare=False
    )
    try:
        operation = f"INSERT INTO #async_strings VALUES ({markers})"
        if batch:
            assert (
                await async_cursor.executemany(operation, iter(parameters), use_prepare=use_prepare)
                is None
            )
            assert async_cursor.rowcount == len(rows)
        else:
            for parameter in parameters:
                await async_cursor.execute(operation, parameter, use_prepare=use_prepare)
                assert async_cursor.rowcount == 1
        await async_cursor.execute(
            "SELECT id, value, DATALENGTH(value) FROM #async_strings ORDER BY id"
        )
        fetched = await async_cursor.fetchall()
        assert [tuple(row)[:2] for row in fetched] == rows
        encoding = "utf-16-le" if unicode_values else "ascii"
        assert [row[2] for row in fetched] == [
            None if value is None else len(value.encode(encoding)) for value in values
        ]
        assert all(row[1] is None or isinstance(row[1], str) for row in fetched)
    finally:
        await async_cursor.execute("DROP TABLE IF EXISTS #async_strings", use_prepare=False)
