import asyncio
import pytest
from datetime import date, datetime, time
from decimal import Decimal
from uuid import UUID

pytest.importorskip("mssql_py_core", exc_type=ImportError)

import mssql_python
from mssql_python import DataError, OperationalError, ProgrammingError, Row
from mssql_python.async_query import AsyncCursor


@pytest.mark.asyncio
async def test_fetched_row_matches_dbapi_row_equality_semantics():
    class NativeCursor:
        async def fetchone(self):
            return (7,)

    cursor = AsyncCursor(NativeCursor())

    row = await cursor.fetchone()

    assert row == [7]
    assert row == Row([7], {})


@pytest.mark.asyncio
async def test_fetched_row_mapping_preserves_duplicate_column_order(async_cursor):
    await async_cursor.execute("SELECT 1 AS b, 2 AS a, 3 AS b")

    row = await async_cursor.fetchone()

    assert row is not None
    assert list(row._mapping) == ["b", "a"]
    assert dict(row._mapping) == {"b": 3, "a": 2}


@pytest.mark.asyncio
async def test_fetch_and_result_navigation_preserve_native_values(async_connection):
    cursor = async_connection.cursor()
    try:
        await cursor.execute(
            "SELECT CAST(1 AS INT) AS value UNION ALL SELECT 2 ORDER BY value; "
            "SELECT CAST(3 AS INT) AS value"
        )

        first = await cursor.fetchone()
        assert isinstance(first, Row)
        assert tuple(first) == (1,)
        assert first.value == 1
        assert cursor.rowcount == 1

        many = await cursor.fetchmany(1)
        assert all(isinstance(row, Row) for row in many)
        assert [tuple(row) for row in many] == [(2,)]
        assert cursor.rowcount == 2
        assert await cursor.fetchall() == []
        assert await cursor.fetchone() is None
        assert cursor.rowcount == 2
        assert await cursor.nextset() is True
        next_result = await cursor.fetchone()
        assert isinstance(next_result, Row)
        assert tuple(next_result) == (3,)
        assert await cursor.nextset() is False
        assert cursor.description is None
    finally:
        await cursor.close()


@pytest.mark.asyncio
async def test_nextset_failure_after_native_state_change_clears_previous_result_state():
    class FailingNativeCursor:
        description = [("value", int, None, None, None, None, True)]
        rowcount = -1

        async def nextset(self):
            self.description = None
            raise RuntimeError("nextset failed")

    class StatefulAsyncCursor(AsyncCursor):
        def seed_result_state(self):
            self._description = [("value", int, None, None, None, None, True)]
            self._column_map = {"value": 0}
            self._column_map_lower = {"value": 0}
            self._column_names = ("value",)
            self._uuid_str_indices = (0,)
            self._fetched_row_count = 2
            self._fetch_rowcount = 2

        def result_maps(self):
            return (
                self._column_map,
                self._column_map_lower,
                self._column_names,
                self._uuid_str_indices,
            )

    cursor = StatefulAsyncCursor(FailingNativeCursor())
    cursor.seed_result_state()

    with pytest.raises(RuntimeError, match="nextset failed"):
        await cursor.nextset()

    assert cursor.description is None
    assert cursor.rowcount == -1
    assert cursor.result_maps() == ({}, None, None, None)


@pytest.mark.asyncio
async def test_busy_nextset_preserves_pending_fetch_state():
    fetch_started = asyncio.Event()
    release_fetch = asyncio.Event()
    guid_values = (
        UUID("6f9619ff-8b86-d011-b42d-00c04fc964ff"),
        UUID("6f9619ff-8b86-d011-b42d-00c04fc964fe"),
    )

    class BusyNativeCursor:
        description = [("MixedGuid", UUID, None, None, None, None, True)]
        rowcount = -1

        async def execute(self, *_args, **_kwargs):
            return self

        async def fetchone(self):
            return (guid_values[0],)

        async def fetchall(self):
            fetch_started.set()
            await release_fetch.wait()
            return [(guid_values[1],)]

        async def nextset(self):
            raise RuntimeError("Connection is busy with another cursor operation")

    cursor = AsyncCursor(BusyNativeCursor())
    previous_native_uuid = mssql_python.native_uuid
    fetch_task = None
    try:
        mssql_python.native_uuid = False
        await cursor.execute("SELECT MixedGuid")
        first = await cursor.fetchone()
        assert first is not None
        assert first.MixedGuid == str(guid_values[0]).upper()
        assert cursor.rowcount == 1

        fetch_task = asyncio.create_task(cursor.fetchall())
        await fetch_started.wait()

        with pytest.raises(OperationalError, match="Connection is busy"):
            await cursor.nextset()

        release_fetch.set()
        remaining = await fetch_task

        assert remaining[0].MixedGuid == str(guid_values[1]).upper()
        assert cursor.rowcount == 2
    finally:
        release_fetch.set()
        if fetch_task is not None and not fetch_task.done():
            await fetch_task
        mssql_python.native_uuid = previous_native_uuid


@pytest.mark.asyncio
async def test_pending_fetch_cannot_update_rowcount_after_successful_close():
    fetch_started = asyncio.Event()
    release_fetch = asyncio.Event()

    class ClosingNativeCursor:
        description = [("value", int, None, None, None, None, True)]
        rowcount = -1

        async def execute(self, *_args, **_kwargs):
            return self

        async def fetchall(self):
            fetch_started.set()
            await release_fetch.wait()
            return [(value,) for value in range(50_000)]

        async def close(self):
            self.description = None

    cursor = AsyncCursor(ClosingNativeCursor())
    fetch_task = None
    try:
        await cursor.execute("SELECT value")
        fetch_task = asyncio.create_task(cursor.fetchall())
        await fetch_started.wait()

        await cursor.close()
        release_fetch.set()
        rows = await fetch_task

        assert len(rows) == 50_000
        assert cursor.description is None
        assert cursor.rowcount == -1
    finally:
        release_fetch.set()
        if fetch_task is not None and not fetch_task.done():
            await fetch_task


@pytest.mark.asyncio
async def test_rejected_close_preserves_result_state():
    class RejectingNativeCursor:
        description = [("value", int, None, None, None, None, True)]
        rowcount = -1

        async def execute(self, *_args, **_kwargs):
            return self

        async def fetchone(self):
            return (1,)

        async def close(self):
            raise RuntimeError("Connection is busy with another cursor operation")

    cursor = AsyncCursor(RejectingNativeCursor())
    await cursor.execute("SELECT value")
    assert await cursor.fetchone() == [1]
    assert cursor.rowcount == 1

    with pytest.raises(OperationalError, match="Connection is busy"):
        await cursor.close()

    assert cursor.description is not None
    assert cursor.rowcount == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("fetch_method", ("fetchone", "fetchmany", "fetchall"))
async def test_pending_fetch_uses_originating_metadata_after_successful_nextset(fetch_method):
    fetch_started = asyncio.Event()
    release_fetch = asyncio.Event()
    old_guid = UUID("6f9619ff-8b86-d011-b42d-00c04fc964ff")

    class NavigatingNativeCursor:
        description = [("OldGuid", UUID, None, None, None, None, True)]
        rowcount = -1

        async def execute(self, *_args, **_kwargs):
            return self

        async def _fetch(self):
            fetch_started.set()
            await release_fetch.wait()
            return (old_guid,)

        async def fetchone(self):
            return await self._fetch()

        async def fetchmany(self, _size):
            return [await self._fetch()]

        async def fetchall(self):
            return [await self._fetch()]

        async def nextset(self):
            self.description = [
                ("identifier", int, None, None, None, None, True),
                ("NewGuid", UUID, None, None, None, None, True),
            ]
            return True

    cursor = AsyncCursor(NavigatingNativeCursor())
    previous_native_uuid = mssql_python.native_uuid
    fetch_task = None
    try:
        mssql_python.native_uuid = False
        await cursor.execute("SELECT OldGuid")
        fetch_call = (
            cursor.fetchmany(1) if fetch_method == "fetchmany" else getattr(cursor, fetch_method)()
        )
        fetch_task = asyncio.create_task(fetch_call)
        await fetch_started.wait()

        assert await cursor.nextset() is True
        release_fetch.set()
        result = await fetch_task
        assert result is not None
        row = result if fetch_method == "fetchone" else result[0]
        assert isinstance(row, Row)

        assert row.OldGuid == str(old_guid).upper()
        assert row["OldGuid"] == str(old_guid).upper()
        assert cursor.description is not None
        assert [column[0] for column in cursor.description] == ["identifier", "NewGuid"]
        assert cursor.rowcount == -1
    finally:
        release_fetch.set()
        if fetch_task is not None and not fetch_task.done():
            await fetch_task
        mssql_python.native_uuid = previous_native_uuid


@pytest.mark.asyncio
@pytest.mark.parametrize("fetch_method", ("fetchone", "fetchmany", "fetchall"))
async def test_fetch_waits_for_successful_nextset_metadata_publication(fetch_method):
    native_advanced = asyncio.Event()
    release_nextset = asyncio.Event()
    fetch_entered = asyncio.Event()

    class NavigatingNativeCursor:
        description = [
            ("OldGuid1", UUID, None, None, None, None, True),
            ("OldGuid2", UUID, None, None, None, None, True),
        ]
        rowcount = -1

        async def execute(self, *_args, **_kwargs):
            return self

        async def nextset(self):
            self.description = [("new_value", int, None, None, None, None, True)]
            native_advanced.set()
            await release_nextset.wait()
            return True

        async def _fetch(self):
            fetch_entered.set()
            return (7,)

        async def fetchone(self):
            return await self._fetch()

        async def fetchmany(self, _size):
            return [await self._fetch()]

        async def fetchall(self):
            return [await self._fetch()]

    cursor = AsyncCursor(NavigatingNativeCursor())
    previous_native_uuid = mssql_python.native_uuid
    nextset_task = None
    fetch_task = None
    try:
        mssql_python.native_uuid = False
        await cursor.execute("SELECT OldGuid1, OldGuid2")
        nextset_task = asyncio.create_task(cursor.nextset())
        await native_advanced.wait()

        fetch_call = (
            cursor.fetchmany(1) if fetch_method == "fetchmany" else getattr(cursor, fetch_method)()
        )
        fetch_task = asyncio.create_task(fetch_call)
        await asyncio.sleep(0)
        assert fetch_entered.is_set() is False

        release_nextset.set()
        assert await nextset_task is True
        result = await fetch_task
        assert result is not None
        row = result if fetch_method == "fetchone" else result[0]
        assert isinstance(row, Row)
        assert row.new_value == 7
        assert cursor.rowcount == 1
    finally:
        release_nextset.set()
        if nextset_task is not None and not nextset_task.done():
            await nextset_task
        if fetch_task is not None and not fetch_task.done():
            await fetch_task
        mssql_python.native_uuid = previous_native_uuid


@pytest.mark.asyncio
async def test_cancelled_nextset_reconciles_before_releasing_fetch():
    native_advanced = asyncio.Event()
    hold_nextset = asyncio.Event()

    class CancelledNativeCursor:
        description = [("old_value", int, None, None, None, None, True)]
        rowcount = -1

        async def execute(self, *_args, **_kwargs):
            return self

        async def nextset(self):
            self.description = [("new_value", int, None, None, None, None, True)]
            native_advanced.set()
            await hold_nextset.wait()

        async def fetchone(self):
            return (7,)

    cursor = AsyncCursor(CancelledNativeCursor())
    await cursor.execute("SELECT old_value")
    nextset_task = asyncio.create_task(cursor.nextset())
    await native_advanced.wait()
    fetch_task = asyncio.create_task(cursor.fetchone())

    nextset_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await nextset_task

    row = await fetch_task
    assert row is not None
    assert row.new_value == 7


@pytest.mark.asyncio
@pytest.mark.parametrize("fetch_method", ("fetchone", "fetchmany", "fetchall"))
async def test_stale_fetch_failure_does_not_clear_new_result(fetch_method):
    fetch_started = asyncio.Event()
    release_fetch = asyncio.Event()

    class NavigatingNativeCursor:
        description = [("old_value", int, None, None, None, None, True)]
        rowcount = -1

        async def execute(self, *_args, **_kwargs):
            return self

        async def _fetch(self):
            fetch_started.set()
            await release_fetch.wait()
            raise RuntimeError("old result fetch failed")

        async def fetchone(self):
            return await self._fetch()

        async def fetchmany(self, _size):
            return await self._fetch()

        async def fetchall(self):
            return await self._fetch()

        async def nextset(self):
            self.description = [("new_value", int, None, None, None, None, True)]
            return True

    cursor = AsyncCursor(NavigatingNativeCursor())
    fetch_task = None
    await cursor.execute("SELECT old_value")
    try:
        fetch_call = (
            cursor.fetchmany(1) if fetch_method == "fetchmany" else getattr(cursor, fetch_method)()
        )
        fetch_task = asyncio.create_task(fetch_call)
        await fetch_started.wait()

        assert await cursor.nextset() is True
        release_fetch.set()
        with pytest.raises(RuntimeError, match="old result fetch failed"):
            await fetch_task

        assert cursor.description is not None
        assert cursor.description[0][0] == "new_value"
        assert cursor.rowcount == -1
        assert await cursor.fetchmany(0) == []
    finally:
        release_fetch.set()
        if fetch_task is not None and not fetch_task.done():
            with pytest.raises(RuntimeError, match="old result fetch failed"):
                await fetch_task


@pytest.mark.asyncio
@pytest.mark.parametrize("fetch_method", ("fetchone", "fetchmany", "fetchall"))
async def test_fetch_failure_reconciles_discarded_native_result(fetch_method):
    class FailingNativeCursor:
        description = [("value", int, None, None, None, None, True)]
        rowcount = 1

        async def _fail(self):
            self.description = None
            self.rowcount = -1
            raise RuntimeError("fetch failed")

        async def fetchone(self):
            return await self._fail()

        async def fetchmany(self, _size):
            return await self._fail()

        async def fetchall(self):
            return await self._fail()

    class StatefulAsyncCursor(AsyncCursor):
        def seed_result_state(self):
            self._description = [("value", int, None, None, None, None, True)]
            self._column_map = {"value": 0}
            self._column_names = ("value",)
            self._fetched_row_count = 1
            self._fetch_rowcount = 1

    cursor = StatefulAsyncCursor(FailingNativeCursor())
    cursor.seed_result_state()

    with pytest.raises(RuntimeError, match="fetch failed"):
        if fetch_method == "fetchmany":
            await cursor.fetchmany(1)
        else:
            await getattr(cursor, fetch_method)()

    assert cursor.description is None
    assert cursor.rowcount == -1
    with pytest.raises(ProgrammingError, match="No active result set"):
        await cursor.fetchmany(0)


@pytest.mark.asyncio
@pytest.mark.parametrize("fetch_method", ("fetchone", "fetchmany", "fetchall"))
async def test_busy_fetch_rejection_preserves_result_state(fetch_method):
    class BusyNativeCursor:
        description = [("value", int, None, None, None, None, True)]
        rowcount = 1

        async def _reject(self):
            raise RuntimeError("Connection is busy with another cursor operation")

        async def fetchone(self):
            return await self._reject()

        async def fetchmany(self, _size):
            return await self._reject()

        async def fetchall(self):
            return await self._reject()

    class StatefulAsyncCursor(AsyncCursor):
        def seed_result_state(self):
            self._description = [("value", int, None, None, None, None, True)]
            self._column_map = {"value": 0}
            self._column_names = ("value",)
            self._fetched_row_count = 1
            self._fetch_rowcount = 1

    cursor = StatefulAsyncCursor(BusyNativeCursor())
    cursor.seed_result_state()

    with pytest.raises(OperationalError, match="Connection is busy"):
        if fetch_method == "fetchmany":
            await cursor.fetchmany(1)
        else:
            await getattr(cursor, fetch_method)()

    assert cursor.description is not None
    assert cursor.description[0][0] == "value"
    assert cursor.rowcount == 1


@pytest.mark.asyncio
async def test_partial_fetch_error_invalidates_result_state(async_cursor):
    await async_cursor.execute(
        "SELECT 10 / n AS value FROM (VALUES (1), (2), (0)) AS v(n)",
        use_prepare=False,
    )
    assert await async_cursor.fetchone() == [10]
    assert async_cursor.rowcount == 1

    with pytest.raises(DataError) as caught:
        await async_cursor.fetchall()

    assert getattr(caught.value, "sql_errors")[0]["number"] == 8134
    assert async_cursor.description is None
    assert async_cursor.rowcount == -1
    with pytest.raises(ProgrammingError, match="No active result set"):
        await async_cursor.fetchmany(0)


@pytest.mark.asyncio
async def test_fetchmany_uses_arraysize(async_connection):
    cursor = async_connection.cursor()
    try:
        cursor.arraysize = 2
        await cursor.execute(
            "SELECT CAST(1 AS INT) AS value UNION ALL SELECT 2 UNION ALL SELECT 3 ORDER BY value"
        )

        assert await cursor.fetchmany(0) == []
        assert await cursor.fetchmany(-1) == []
        assert [tuple(row) for row in await cursor.fetchmany()] == [(1,), (2,)]
        assert [tuple(row) for row in await cursor.fetchall()] == [(3,)]
    finally:
        await cursor.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("size", "error_type"),
    ((1.5, TypeError), (2**100, OverflowError)),
)
async def test_fetchmany_size_conversion_error_preserves_result_state(
    async_cursor, size, error_type
):
    previous_native_uuid = mssql_python.native_uuid
    try:
        mssql_python.native_uuid = False
        await async_cursor.execute(
            "SELECT CAST('6F9619FF-8B86-D011-B42D-00C04FC964FF' "
            "AS UNIQUEIDENTIFIER) AS MixedGuid UNION ALL "
            "SELECT CAST('6F9619FF-8B86-D011-B42D-00C04FC964FE' AS UNIQUEIDENTIFIER)"
        )
        first = await async_cursor.fetchone()
        assert first is not None
        assert first.MixedGuid == "6F9619FF-8B86-D011-B42D-00C04FC964FF"
        assert async_cursor.rowcount == 1

        with pytest.raises(error_type):
            await async_cursor.fetchmany(size)

        second = await async_cursor.fetchone()
        assert second is not None
        assert second.MixedGuid == "6F9619FF-8B86-D011-B42D-00C04FC964FE"
        assert async_cursor.rowcount == 2
    finally:
        mssql_python.native_uuid = previous_native_uuid


@pytest.mark.asyncio
@pytest.mark.parametrize("size", (0, -1))
async def test_fetchmany_non_positive_size_skips_native_fetch(async_connection, monkeypatch, size):
    cursor = async_connection.cursor()
    try:
        await cursor.execute("SELECT 1 AS value")

        class UnexpectedNativeFetch:
            async def fetchmany(self, *_args):
                pytest.fail("fetchmany must not call py-core for a non-positive size")

        monkeypatch.setattr(
            "mssql_python.async_query.async_fetch._get_py_core_async_cursor",
            lambda _cursor: UnexpectedNativeFetch(),
        )

        assert await cursor.fetchmany(size) == []
        assert cursor.rowcount == -1
    finally:
        await cursor.close()


@pytest.mark.asyncio
async def test_fetch_respects_row_settings(async_connection):
    cursor = async_connection.cursor()
    previous_lowercase = mssql_python.lowercase
    previous_native_uuid = mssql_python.native_uuid
    try:
        mssql_python.lowercase = True
        mssql_python.native_uuid = False
        await cursor.execute(
            "SELECT CAST('6F9619FF-8B86-D011-B42D-00C04FC964FF' "
            "AS UNIQUEIDENTIFIER) AS MixedGuid"
        )

        row = await cursor.fetchone()

        assert isinstance(row, Row)
        assert cursor.description[0][0] == "mixedguid"
        assert row.MixedGuid == "6F9619FF-8B86-D011-B42D-00C04FC964FF"
        assert row.mixedguid == row.MixedGuid
        assert row.MIXEDGUID == row.MixedGuid
    finally:
        mssql_python.lowercase = previous_lowercase
        mssql_python.native_uuid = previous_native_uuid
        await cursor.close()


@pytest.mark.asyncio
async def test_row_settings_are_snapshotted_at_execute(async_connection):
    cursor = async_connection.cursor()
    previous_lowercase = mssql_python.lowercase
    previous_native_uuid = mssql_python.native_uuid
    try:
        mssql_python.lowercase = True
        mssql_python.native_uuid = False
        await cursor.execute(
            "SELECT CAST('6F9619FF-8B86-D011-B42D-00C04FC964FF' "
            "AS UNIQUEIDENTIFIER) AS MixedGuid UNION ALL "
            "SELECT CAST('6F9619FF-8B86-D011-B42D-00C04FC964FE' AS UNIQUEIDENTIFIER)"
        )

        mssql_python.lowercase = False
        mssql_python.native_uuid = True
        first = await cursor.fetchone()
        mssql_python.lowercase = True
        mssql_python.native_uuid = False
        second = await cursor.fetchone()

        assert cursor.description[0][0] == "mixedguid"
        assert isinstance(first[0], str)
        assert isinstance(second[0], str)
        assert first.MixedGuid == first.mixedguid
        assert second.MixedGuid == second.mixedguid
    finally:
        mssql_python.lowercase = previous_lowercase
        mssql_python.native_uuid = previous_native_uuid
        await cursor.close()


@pytest.mark.asyncio
async def test_row_settings_are_resnapshotted_at_nextset(async_connection):
    cursor = async_connection.cursor()
    previous_lowercase = mssql_python.lowercase
    previous_native_uuid = mssql_python.native_uuid
    try:
        mssql_python.lowercase = False
        mssql_python.native_uuid = True
        await cursor.execute(
            "SELECT CAST('6F9619FF-8B86-D011-B42D-00C04FC964FF' "
            "AS UNIQUEIDENTIFIER) AS FirstGuid; "
            "SELECT CAST('6F9619FF-8B86-D011-B42D-00C04FC964FE' "
            "AS UNIQUEIDENTIFIER) AS SecondGuid"
        )

        mssql_python.lowercase = True
        mssql_python.native_uuid = False
        first = await cursor.fetchone()
        assert cursor.description[0][0] == "FirstGuid"
        assert isinstance(first[0], UUID)

        assert await cursor.nextset() is True
        mssql_python.lowercase = False
        mssql_python.native_uuid = True
        second = await cursor.fetchone()

        assert cursor.description[0][0] == "secondguid"
        assert isinstance(second[0], str)
        assert second.SecondGuid == second.secondguid
    finally:
        mssql_python.lowercase = previous_lowercase
        mssql_python.native_uuid = previous_native_uuid
        await cursor.close()


@pytest.mark.asyncio
async def test_fetch_translates_py_core_exception(async_connection):
    cursor = async_connection.cursor()
    try:
        await cursor.execute("SELECT 1 / 0")

        with pytest.raises(DataError) as caught:
            await cursor.fetchone()

        assert type(caught.value.__cause__).__module__ == "mssql_py_core"
        assert type(caught.value.__cause__).__name__ == "DatabaseError"
        sql_errors = getattr(caught.value, "sql_errors")
        assert sql_errors
        assert sql_errors[0]["number"] == 8134
    finally:
        await cursor.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fetch_plan",
    (
        ("many-one",),
        ("one-many",),
        ("many-one-many-one",),
    ),
)
async def test_fetchone_fetchmany_interleaving(async_cursor, fetch_plan):
    await async_cursor.execute(
        "SELECT value FROM (VALUES (1), (2), (3), (4)) AS values_table(value) ORDER BY value"
    )

    if fetch_plan == ("many-one",):
        assert [tuple(row) for row in await async_cursor.fetchmany(1)] == [(1,)]
        assert tuple(await async_cursor.fetchone()) == (2,)
    elif fetch_plan == ("one-many",):
        assert tuple(await async_cursor.fetchone()) == (1,)
        assert [tuple(row) for row in await async_cursor.fetchmany(2)] == [(2,), (3,)]
    else:
        assert [tuple(row) for row in await async_cursor.fetchmany(1)] == [(1,)]
        assert tuple(await async_cursor.fetchone()) == (2,)
        assert [tuple(row) for row in await async_cursor.fetchmany(1)] == [(3,)]
        assert tuple(await async_cursor.fetchone()) == (4,)


@pytest.mark.asyncio
async def test_fetchmany_more_than_available_and_repeated_exhaustion(async_cursor):
    await async_cursor.execute(
        "SELECT value FROM (VALUES (1), (2), (3)) AS rows(value) ORDER BY value"
    )

    rows = await async_cursor.fetchmany(10)

    assert [row[0] for row in rows] == [1, 2, 3]
    assert await async_cursor.fetchmany(10) == []
    assert await async_cursor.fetchmany(10) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("fetch_method", ("fetchone", "fetchmany", "fetchall"))
async def test_fetch_empty_result_set(fetch_method, async_cursor):
    await async_cursor.execute("SELECT CAST(1 AS INT) AS value WHERE 1 = 0")

    result = await getattr(async_cursor, fetch_method)()

    assert result is None if fetch_method == "fetchone" else result == []
    assert async_cursor.rowcount == 0


@pytest.mark.asyncio
async def test_fetch_preserves_empty_string_binary_and_null(async_cursor):
    await async_cursor.execute(
        "SELECT * FROM (VALUES "
        "(1, CAST('' AS NVARCHAR(10)), CAST(0x AS VARBINARY(10))), "
        "(2, CAST(NULL AS NVARCHAR(10)), CAST(NULL AS VARBINARY(10))), "
        "(3, CAST('text' AS NVARCHAR(10)), CAST(0x1234 AS VARBINARY(10)))) "
        "AS values_table(id, text_value, binary_value) ORDER BY id"
    )

    first = await async_cursor.fetchone()
    remaining = await async_cursor.fetchall()

    assert tuple(first) == (1, "", b"")
    assert [tuple(row) for row in remaining] == [
        (2, None, None),
        (3, "text", b"\x12\x34"),
    ]


@pytest.mark.asyncio
async def test_fetchmany_handles_mixed_large_lob_sizes(async_cursor):
    medium = "x" * 1_000
    large = "y" * 10_000
    await async_cursor.execute(
        "SELECT * FROM (VALUES "
        "(1, CAST('' AS NVARCHAR(MAX))), "
        "(2, CAST(NULL AS NVARCHAR(MAX))), "
        "(3, CAST(? AS NVARCHAR(MAX))), "
        "(4, CAST(? AS NVARCHAR(MAX)))) AS values_table(id, value) ORDER BY id",
        medium,
        large,
    )

    first_batch = await async_cursor.fetchmany(3)
    second_batch = await async_cursor.fetchmany(3)

    assert [row[1] for row in first_batch] == ["", None, medium]
    assert [row[1] for row in second_batch] == [large]
    assert await async_cursor.fetchmany(3) == []


@pytest.mark.asyncio
async def test_fetch_roundtrips_representative_sync_result_types(async_cursor):
    expected = (
        True,
        -(2**63),
        3.25,
        Decimal("123.45"),
        date(2024, 1, 2),
        time(12, 34, 56, 123456),
        datetime(2024, 1, 2, 12, 34, 56, 123456),
        UUID("6f9619ff-8b86-d011-b42d-00c04fc964ff"),
    )
    await async_cursor.execute(
        "SELECT CAST(1 AS BIT), CAST(-9223372036854775808 AS BIGINT), CAST(3.25 AS FLOAT), "
        "CAST(123.45 AS DECIMAL(10, 2)), CAST('2024-01-02' AS DATE), "
        "CAST('12:34:56.123456' AS TIME(6)), "
        "CAST('2024-01-02T12:34:56.123456' AS DATETIME2(6)), "
        "CAST('6F9619FF-8B86-D011-B42D-00C04FC964FF' AS UNIQUEIDENTIFIER)"
    )

    row = await async_cursor.fetchone()

    assert tuple(row) == expected
