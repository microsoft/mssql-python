# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Comprehensive tests for the Arrow bulk-copy feature (Cursor.bulkcopy_arrow).

Layout:
  * Unit tests (no live DB) exercise every branch of the new Python code:
      - Cursor._looks_like_arrow_source  (source detection)
      - Cursor.bulkcopy                  (Arrow-steering guard)
      - Cursor.bulkcopy_arrow            (argument validation + dispatch + cleanup)
      - Cursor._build_pycore_context     (conn-string parse + AAD/ServicePrincipal auth)
  * Live-DB integration tests (gated on DB_CONNECTION_STRING) round-trip real
    Arrow data through TDS across the supported type matrix, the accepted source
    shapes, column mappings, and the locked type-conversion decisions
    (int narrowing, uint64->BIGINT, tz handling, __arrow_c_array__).
"""

import os
import secrets
from unittest.mock import MagicMock, patch

import pytest

# Skip the whole module if the native core or pyarrow can't load (build
# containers without the extension, or pyarrow absent). exc_type=ImportError
# so a module that imports but fails its native extension is skipped, not
# collected as an error (pytest >= 9.1 default only skips ModuleNotFoundError).
mssql_py_core = pytest.importorskip("mssql_py_core", exc_type=ImportError)
pa = pytest.importorskip("pyarrow")

LIVE_DB = pytest.mark.skipif(
    not os.getenv("DB_CONNECTION_STRING"),
    reason="DB_CONNECTION_STRING not set",
)

SAMPLE_TOKEN = secrets.token_hex(44)


# ── helpers ──────────────────────────────────────────────────────────────────


def _bare_cursor():
    """A Cursor instance with no wiring — enough for pure validation/detection."""
    from mssql_python.cursor import Cursor

    cursor = Cursor.__new__(Cursor)
    cursor.closed = True  # keep __del__ a no-op during GC teardown
    return cursor


def _cursor_with_conn(connection_str, auth_type=None, credential_kwargs=None):
    """A Cursor whose .connection is a mock carrying just the auth wiring that
    _build_pycore_context reads."""
    from mssql_python.cursor import Cursor

    mock_conn = MagicMock()
    mock_conn.connection_str = connection_str
    mock_conn._auth_type = auth_type
    mock_conn._credential_kwargs = credential_kwargs
    mock_conn._is_connected = True

    cursor = Cursor.__new__(Cursor)
    cursor._connection = mock_conn
    cursor.closed = True  # keep __del__ a no-op during GC teardown
    cursor.hstmt = None
    cursor._timeout = 0  # timeout snapshot _build_pycore_context reads (0 = no override)
    return cursor


class _CStreamOnly:
    """Object exposing only the Arrow C-stream PyCapsule protocol."""

    def __init__(self, table):
        self._t = table

    def __arrow_c_stream__(self, requested_schema=None):
        return self._t.__arrow_c_stream__(requested_schema)


class _CArrayOnly:
    """Object exposing only the Arrow C-array PyCapsule protocol (single batch)."""

    def __init__(self, batch):
        self._b = batch

    def __arrow_c_array__(self, requested_schema=None):
        return self._b.__arrow_c_array__(requested_schema)


# ── unit: _looks_like_arrow_source ───────────────────────────────────────────


class TestArrowDetection:
    def test_table_is_arrow(self):
        assert _bare_cursor()._looks_like_arrow_source(pa.table({"a": [1, 2, 3]})) is True

    def test_record_batch_is_arrow(self):
        batch = pa.record_batch([pa.array([1, 2])], names=["a"])
        assert _bare_cursor()._looks_like_arrow_source(batch) is True

    def test_record_batch_reader_is_arrow(self):
        reader = pa.RecordBatchReader.from_batches(
            pa.schema([("a", pa.int32())]),
            [pa.record_batch([pa.array([1], type=pa.int32())], names=["a"])],
        )
        assert _bare_cursor()._looks_like_arrow_source(reader) is True

    def test_c_stream_producer_is_arrow(self):
        obj = _CStreamOnly(pa.table({"a": [1, 2]}))
        assert _bare_cursor()._looks_like_arrow_source(obj) is True

    def test_c_array_producer_is_arrow(self):
        obj = _CArrayOnly(pa.record_batch([pa.array([1, 2])], names=["a"]))
        assert _bare_cursor()._looks_like_arrow_source(obj) is True

    def test_tuples_are_not_arrow(self):
        assert _bare_cursor()._looks_like_arrow_source([(1,), (2,)]) is False

    def test_none_is_not_arrow(self):
        assert _bare_cursor()._looks_like_arrow_source(None) is False

    def test_string_is_not_arrow(self):
        assert _bare_cursor()._looks_like_arrow_source("abc") is False

    def test_bytes_are_not_arrow(self):
        assert _bare_cursor()._looks_like_arrow_source(b"abc") is False

    def test_plain_object_is_not_arrow(self):
        assert _bare_cursor()._looks_like_arrow_source(object()) is False

    def test_detection_survives_missing_pyarrow(self):
        """A non-capsule object with pyarrow unimportable must return False, not raise."""
        cur = _bare_cursor()
        with patch.dict("sys.modules", {"pyarrow": None}):
            assert cur._looks_like_arrow_source(object()) is False

    def test_capsule_detection_without_pyarrow(self):
        """C-stream producers are detected even when pyarrow itself is absent."""
        cur = _bare_cursor()
        obj = _CStreamOnly(pa.table({"a": [1]}))
        with patch.dict("sys.modules", {"pyarrow": None}):
            assert cur._looks_like_arrow_source(obj) is True


# ── unit: bulkcopy() Arrow-steering guard ────────────────────────────────────


class TestBulkcopyGuard:
    def test_bulkcopy_rejects_table(self):
        from mssql_python.cursor import Cursor

        with pytest.raises(TypeError, match="bulkcopy_arrow"):
            Cursor.bulkcopy(_bare_cursor(), "t", pa.table({"a": [1, 2]}))

    def test_bulkcopy_rejects_record_batch(self):
        from mssql_python.cursor import Cursor

        batch = pa.record_batch([pa.array([1, 2])], names=["a"])
        with pytest.raises(TypeError, match="bulkcopy_arrow"):
            Cursor.bulkcopy(_bare_cursor(), "t", batch)

    def test_bulkcopy_rejects_c_stream_producer(self):
        from mssql_python.cursor import Cursor

        obj = _CStreamOnly(pa.table({"a": [1, 2]}))
        with pytest.raises(TypeError, match="bulkcopy_arrow"):
            Cursor.bulkcopy(_bare_cursor(), "t", obj)


# ── unit: bulkcopy_arrow() argument validation ───────────────────────────────


class TestBulkcopyArrowValidation:
    def test_empty_table_name(self):
        with pytest.raises(ValueError, match="table_name"):
            _bare_cursor().bulkcopy_arrow("", pa.table({"a": [1]}))

    def test_non_string_table_name(self):
        with pytest.raises(ValueError, match="table_name"):
            _bare_cursor().bulkcopy_arrow(123, pa.table({"a": [1]}))

    def test_none_source(self):
        with pytest.raises(TypeError, match="source"):
            _bare_cursor().bulkcopy_arrow("t", None)

    def test_batch_size_wrong_type(self):
        with pytest.raises(TypeError, match="batch_size"):
            _bare_cursor().bulkcopy_arrow("t", pa.table({"a": [1]}), batch_size="5")

    def test_batch_size_negative(self):
        with pytest.raises(ValueError, match="batch_size"):
            _bare_cursor().bulkcopy_arrow("t", pa.table({"a": [1]}), batch_size=-1)

    def test_timeout_wrong_type(self):
        with pytest.raises(TypeError, match="timeout"):
            _bare_cursor().bulkcopy_arrow("t", pa.table({"a": [1]}), timeout="30")

    def test_timeout_non_positive(self):
        with pytest.raises(ValueError, match="timeout"):
            _bare_cursor().bulkcopy_arrow("t", pa.table({"a": [1]}), timeout=0)

    def test_missing_pycore_raises_importerror(self):
        cur = _bare_cursor()
        with patch.dict("sys.modules", {"mssql_py_core": None}):
            with pytest.raises(ImportError, match="mssql_py_core"):
                cur.bulkcopy_arrow("t", pa.table({"a": [1]}))


# ── unit: _build_pycore_context() ────────────────────────────────────────────


class TestBuildPycoreContext:
    def test_missing_connection_str_raises(self):
        from mssql_python.cursor import Cursor

        cur = Cursor.__new__(Cursor)
        cur._connection = object()  # no connection_str attribute
        cur.closed = True
        with pytest.raises(RuntimeError, match="Connection string not available"):
            cur._build_pycore_context()

    def test_missing_server_raises(self):
        cur = _cursor_with_conn("Database=testdb;UID=sa;PWD=pwd", auth_type=None)
        with pytest.raises(ValueError, match="SERVER"):
            cur._build_pycore_context()

    def test_sql_auth_keeps_credentials(self):
        cur = _cursor_with_conn(
            "Server=localhost;Database=testdb;UID=sa;PWD=mypwd", auth_type=None
        )
        ctx = cur._build_pycore_context()
        assert ctx.get("user_name") == "sa"
        assert ctx.get("password") == "mypwd"
        assert "access_token" not in ctx
        assert "entra_id_token_factory" not in ctx

    @patch("mssql_python.cursor.logger")
    def test_token_path_replaces_credentials(self, mock_logger):
        mock_logger.is_debug_enabled = False
        cur = _cursor_with_conn(
            "Server=tcp:x.database.windows.net;Database=d;"
            "Authentication=ActiveDirectoryDefault;UID=u@x.com;PWD=s",
            auth_type="activedirectorydefault",
        )
        with patch("mssql_python.auth.AADAuth.get_raw_token", return_value=SAMPLE_TOKEN):
            ctx = cur._build_pycore_context()
        assert ctx.get("access_token") == SAMPLE_TOKEN
        assert "authentication" not in ctx
        assert "user_name" not in ctx
        assert "password" not in ctx

    @patch("mssql_python.cursor.logger")
    def test_token_acquisition_failure_raises(self, mock_logger):
        mock_logger.is_debug_enabled = False
        cur = _cursor_with_conn(
            "Server=tcp:x.database.windows.net;Database=d;"
            "Authentication=ActiveDirectoryDefault;UID=u@x.com;PWD=s",
            auth_type="activedirectorydefault",
        )
        with patch(
            "mssql_python.auth.AADAuth.get_raw_token",
            side_effect=RuntimeError("no token"),
        ):
            with pytest.raises(RuntimeError, match="unable to acquire Azure AD token"):
                cur._build_pycore_context()

    @patch("mssql_python.cursor.logger")
    def test_service_principal_registers_factory(self, mock_logger):
        mock_logger.is_debug_enabled = False
        from mssql_python.constants import _AuthInternal

        cur = _cursor_with_conn(
            "Server=tcp:x.database.windows.net;Database=d;"
            "Authentication=ActiveDirectoryServicePrincipal;UID=client-id;PWD=client-secret",
            auth_type=_AuthInternal.SERVICE_PRINCIPAL,
        )
        sentinel = object()
        with patch(
            "mssql_python.auth.ServicePrincipalAuth.make_token_factory",
            return_value=sentinel,
        ) as mk:
            ctx = cur._build_pycore_context()
        mk.assert_called_once_with("client-id", "client-secret")
        assert ctx.get("entra_id_token_factory") is sentinel
        # ServicePrincipal keeps auth/user_name/password so py-core can resolve
        # the method before dispatching the factory at handshake time.
        assert "access_token" not in ctx

    @patch("mssql_python.cursor.logger")
    def test_service_principal_missing_secret_raises(self, mock_logger):
        mock_logger.is_debug_enabled = False
        from mssql_python.constants import _AuthInternal

        cur = _cursor_with_conn(
            "Server=tcp:x.database.windows.net;Database=d;"
            "Authentication=ActiveDirectoryServicePrincipal",
            auth_type=_AuthInternal.SERVICE_PRINCIPAL,
        )
        with pytest.raises(RuntimeError, match="client-secret only"):
            cur._build_pycore_context()

    @patch("mssql_python.cursor.logger")
    def test_service_principal_factory_build_failure_raises(self, mock_logger):
        mock_logger.is_debug_enabled = False
        from mssql_python.constants import _AuthInternal

        cur = _cursor_with_conn(
            "Server=tcp:x.database.windows.net;Database=d;"
            "Authentication=ActiveDirectoryServicePrincipal;UID=c;PWD=s",
            auth_type=_AuthInternal.SERVICE_PRINCIPAL,
        )
        with patch(
            "mssql_python.auth.ServicePrincipalAuth.make_token_factory",
            side_effect=ValueError("bad cert"),
        ):
            with pytest.raises(RuntimeError, match="ServicePrincipal token factory"):
                cur._build_pycore_context()


# ── unit: bulkcopy_arrow() dispatch + cleanup ────────────────────────────────


def _mock_pycore(result=None, raise_exc=None):
    """Build a mock mssql_py_core module and return (module, pycore_cursor,
    pycore_conn, captured) where captured['ctx'] holds the exact context dict
    handed to PyCoreConnection."""
    pycore_cursor = MagicMock()
    if raise_exc is not None:
        pycore_cursor.bulkcopy_arrow.side_effect = raise_exc
    else:
        pycore_cursor.bulkcopy_arrow.return_value = result or {
            "rows_copied": 2,
            "batch_count": 1,
            "elapsed_time": 0.01,
            "rows_per_second": 200.0,
        }
    pycore_conn = MagicMock()
    pycore_conn.cursor.return_value = pycore_cursor

    captured = {}

    def make_conn(ctx, **kwargs):
        captured["ctx"] = ctx  # keep the reference so we can inspect post-cleanup
        return pycore_conn

    module = MagicMock()
    module.PyCoreConnection = make_conn
    return module, pycore_cursor, pycore_conn, captured


class TestBulkcopyArrowDispatch:
    @patch("mssql_python.cursor.logger")
    def test_success_returns_result_and_forwards_args(self, mock_logger):
        mock_logger.is_debug_enabled = False
        cur = _cursor_with_conn("Server=localhost;Database=d;UID=sa;PWD=p")
        module, pyc_cursor, _, _ = _mock_pycore()
        src = pa.table({"a": [1, 2]})

        with patch.dict("sys.modules", {"mssql_py_core": module}):
            result = cur.bulkcopy_arrow(
                "dbo.t", src, batch_size=7, timeout=15, column_mappings=["a"]
            )

        assert result["rows_copied"] == 2
        pyc_cursor.bulkcopy_arrow.assert_called_once()
        args, kwargs = pyc_cursor.bulkcopy_arrow.call_args
        assert args[0] == "dbo.t"
        assert args[1] is src
        assert kwargs["batch_size"] == 7
        assert kwargs["timeout"] == 15
        assert kwargs["column_mappings"] == ["a"]

    @patch("mssql_python.cursor.logger")
    def test_batch_size_timeout_accept_positional(self, mock_logger):
        """D13: batch_size/timeout are positional-or-keyword (parity with bulkcopy)."""
        mock_logger.is_debug_enabled = False
        cur = _cursor_with_conn("Server=localhost;Database=d;UID=sa;PWD=p")
        module, pyc_cursor, _, _ = _mock_pycore()
        src = pa.table({"a": [1, 2]})

        with patch.dict("sys.modules", {"mssql_py_core": module}):
            cur.bulkcopy_arrow("dbo.t", src, 7, 15)

        _, kwargs = pyc_cursor.bulkcopy_arrow.call_args
        assert kwargs["batch_size"] == 7
        assert kwargs["timeout"] == 15


    @patch("mssql_python.cursor.logger")
    def test_sensitive_fields_cleared_after_success(self, mock_logger):
        mock_logger.is_debug_enabled = False
        cur = _cursor_with_conn("Server=localhost;Database=d;UID=sa;PWD=secret")
        module, _, _, captured = _mock_pycore()

        with patch.dict("sys.modules", {"mssql_py_core": module}):
            cur.bulkcopy_arrow("t", pa.table({"a": [1]}))

        ctx = captured["ctx"]
        for key in ("password", "user_name", "access_token", "entra_id_token_factory"):
            assert key not in ctx

    @patch("mssql_python.cursor.logger")
    def test_resources_closed_on_success(self, mock_logger):
        mock_logger.is_debug_enabled = False
        cur = _cursor_with_conn("Server=localhost;Database=d;UID=sa;PWD=p")
        module, pyc_cursor, pyc_conn, _ = _mock_pycore()

        with patch.dict("sys.modules", {"mssql_py_core": module}):
            cur.bulkcopy_arrow("t", pa.table({"a": [1]}))

        pyc_cursor.close.assert_called_once()
        pyc_conn.close.assert_called_once()

    @patch("mssql_python.cursor.logger")
    def test_core_exception_is_reraised_and_cleaned_up(self, mock_logger):
        mock_logger.is_debug_enabled = False
        cur = _cursor_with_conn("Server=localhost;Database=d;UID=sa;PWD=p")
        module, pyc_cursor, pyc_conn, captured = _mock_pycore(
            raise_exc=ValueError("boom")
        )

        with patch.dict("sys.modules", {"mssql_py_core": module}):
            with pytest.raises(ValueError, match="boom"):
                cur.bulkcopy_arrow("t", pa.table({"a": [1]}))

        # cleanup still ran despite the failure
        assert "password" not in captured["ctx"]
        pyc_cursor.close.assert_called_once()
        pyc_conn.close.assert_called_once()

    @patch("mssql_python.cursor.logger")
    def test_cleanup_swallows_close_errors(self, mock_logger):
        """A failing resource.close() during teardown must not mask the result."""
        mock_logger.is_debug_enabled = False
        cur = _cursor_with_conn("Server=localhost;Database=d;UID=sa;PWD=p")
        module, pyc_cursor, pyc_conn, _ = _mock_pycore()
        pyc_cursor.close.side_effect = RuntimeError("close failed")

        with patch.dict("sys.modules", {"mssql_py_core": module}):
            result = cur.bulkcopy_arrow("t", pa.table({"a": [1]}))

        # result is still returned; the close error was swallowed and logged
        assert result["rows_copied"] == 2
        pyc_cursor.close.assert_called_once()
        pyc_conn.close.assert_called_once()


# ── live DB integration ──────────────────────────────────────────────────────


def _make_table(cursor, name, cols):
    cursor.execute(f"IF OBJECT_ID('{name}','U') IS NOT NULL DROP TABLE {name}")
    cursor.execute(f"CREATE TABLE {name} ({cols})")
    cursor.connection.commit()


@LIVE_DB
class TestBulkcopyArrowLive:
    def test_table_round_trip_with_nulls(self, cursor):
        t = "mssql_python_arrow_tbl"
        _make_table(cursor, t, "id INT NOT NULL, name NVARCHAR(50) NULL, score FLOAT NULL")
        tbl = pa.table(
            {
                "id": pa.array([1, 2, 3], type=pa.int32()),
                "name": pa.array(["Alice", None, "Charlie"]),
                "score": pa.array([1.5, 2.5, None], type=pa.float64()),
            }
        )
        result = cursor.bulkcopy_arrow(t, tbl)
        assert result["rows_copied"] == 3
        # returned dict shape
        for key in ("rows_copied", "batch_count", "elapsed_time", "rows_per_second"):
            assert key in result
        cursor.execute(f"SELECT id, name, score FROM {t} ORDER BY id")
        rows = cursor.fetchall()
        assert [r[0] for r in rows] == [1, 2, 3]
        assert rows[1][1] is None
        assert rows[2][2] is None
        cursor.execute(f"DROP TABLE {t}")

    def test_record_batch_source(self, cursor):
        t = "mssql_python_arrow_batch"
        _make_table(cursor, t, "id INT NOT NULL")
        batch = pa.record_batch([pa.array([10, 20], type=pa.int32())], names=["id"])
        result = cursor.bulkcopy_arrow(t, batch)
        assert result["rows_copied"] == 2
        cursor.execute(f"SELECT id FROM {t} ORDER BY id")
        assert [r[0] for r in cursor.fetchall()] == [10, 20]
        cursor.execute(f"DROP TABLE {t}")

    def test_record_batch_reader_multibatch(self, cursor):
        t = "mssql_python_arrow_reader"
        _make_table(cursor, t, "id INT NOT NULL, txt NVARCHAR(20) NULL")
        schema = pa.schema([("id", pa.int32()), ("txt", pa.string())])
        batches = [
            pa.record_batch(
                [pa.array([1, 2], type=pa.int32()), pa.array(["a", "b"])], schema=schema
            ),
            pa.record_batch(
                [pa.array([3], type=pa.int32()), pa.array([None])], schema=schema
            ),
        ]
        reader = pa.RecordBatchReader.from_batches(schema, batches)
        result = cursor.bulkcopy_arrow(t, reader)
        assert result["rows_copied"] == 3
        cursor.execute(f"SELECT id FROM {t} ORDER BY id")
        assert [r[0] for r in cursor.fetchall()] == [1, 2, 3]
        cursor.execute(f"DROP TABLE {t}")

    def test_iterable_of_record_batches(self, cursor):
        t = "mssql_python_arrow_iter"
        _make_table(cursor, t, "id INT NOT NULL")
        schema = pa.schema([("id", pa.int32())])
        batches = [
            pa.record_batch([pa.array([1, 2], type=pa.int32())], schema=schema),
            pa.record_batch([pa.array([3, 4], type=pa.int32())], schema=schema),
        ]
        result = cursor.bulkcopy_arrow(t, batches)
        assert result["rows_copied"] == 4
        cursor.execute(f"DROP TABLE {t}")

    def test_c_stream_producer(self, cursor):
        t = "mssql_python_arrow_cstream"
        _make_table(cursor, t, "id INT NOT NULL")
        obj = _CStreamOnly(pa.table({"id": pa.array([5, 6], type=pa.int32())}))
        result = cursor.bulkcopy_arrow(t, obj)
        assert result["rows_copied"] == 2
        cursor.execute(f"DROP TABLE {t}")

    def test_c_array_producer_single_batch(self, cursor):
        t = "mssql_python_arrow_carray"
        _make_table(cursor, t, "id INT NOT NULL, name NVARCHAR(20) NULL")
        batch = pa.record_batch(
            [pa.array([7, 8], type=pa.int32()), pa.array(["x", "y"])], names=["id", "name"]
        )
        result = cursor.bulkcopy_arrow(t, _CArrayOnly(batch))
        assert result["rows_copied"] == 2
        cursor.execute(f"SELECT id FROM {t} ORDER BY id")
        assert [r[0] for r in cursor.fetchall()] == [7, 8]
        cursor.execute(f"DROP TABLE {t}")

    def test_empty_source_copies_zero_rows(self, cursor):
        t = "mssql_python_arrow_empty"
        _make_table(cursor, t, "id INT NOT NULL")
        tbl = pa.table({"id": pa.array([], type=pa.int32())})
        result = cursor.bulkcopy_arrow(t, tbl)
        assert result["rows_copied"] == 0
        cursor.execute(f"DROP TABLE {t}")

    # ── locked type-conversion decisions ────────────────────────────────────

    def test_int64_narrows_to_int(self, cursor):
        """A1: Arrow int64 loads into a SQL INT column when values fit."""
        t = "mssql_python_arrow_a1"
        _make_table(cursor, t, "n INT NOT NULL")
        tbl = pa.table({"n": pa.array([1, 2, 2_000_000_000], type=pa.int64())})
        result = cursor.bulkcopy_arrow(t, tbl)
        assert result["rows_copied"] == 3
        cursor.execute(f"SELECT n FROM {t} ORDER BY n")
        assert [r[0] for r in cursor.fetchall()] == [1, 2, 2_000_000_000]
        cursor.execute(f"DROP TABLE {t}")

    def test_int64_overflow_to_int_raises(self, cursor):
        """A1: out-of-range int64 -> INT must raise, not silently truncate."""
        t = "mssql_python_arrow_a1_of"
        _make_table(cursor, t, "n INT NOT NULL")
        tbl = pa.table({"n": pa.array([5_000_000_000], type=pa.int64())})
        with pytest.raises(Exception):
            cursor.bulkcopy_arrow(t, tbl)
        cursor.execute(f"DROP TABLE {t}")

    def test_uint64_to_bigint(self, cursor):
        """A2: Arrow uint64 loads into BIGINT (values within i64 range)."""
        t = "mssql_python_arrow_a2"
        _make_table(cursor, t, "n BIGINT NOT NULL")
        tbl = pa.table({"n": pa.array([1, 42, 9_000_000_000], type=pa.uint64())})
        result = cursor.bulkcopy_arrow(t, tbl)
        assert result["rows_copied"] == 3
        cursor.execute(f"DROP TABLE {t}")

    def test_tz_aware_timestamp_to_datetime2_raises(self, cursor):
        """C1: tz-aware timestamp -> DATETIME2 must raise and point at datetimeoffset."""
        import datetime as dt

        t = "mssql_python_arrow_c1"
        _make_table(cursor, t, "ts DATETIME2 NULL")
        ts = pa.array([dt.datetime(2020, 1, 1, 12)], type=pa.timestamp("us", tz="UTC"))
        with pytest.raises(Exception, match="(?i)datetimeoffset"):
            cursor.bulkcopy_arrow(t, pa.table({"ts": ts}))
        cursor.execute(f"DROP TABLE {t}")

    # ── column mappings ─────────────────────────────────────────────────────

    def test_column_mappings_by_name(self, cursor):
        t = "mssql_python_arrow_map_name"
        _make_table(cursor, t, "a INT NOT NULL, b NVARCHAR(20) NULL")
        src = pa.table({"x": pa.array([1, 2], type=pa.int32()), "y": pa.array(["p", "q"])})
        result = cursor.bulkcopy_arrow(t, src, column_mappings=["a", "b"])
        assert result["rows_copied"] == 2
        cursor.execute(f"SELECT a, b FROM {t} ORDER BY a")
        rows = cursor.fetchall()
        assert rows[0][1] == "p" and rows[1][1] == "q"
        cursor.execute(f"DROP TABLE {t}")

    def test_column_mappings_by_ordinal(self, cursor):
        t = "mssql_python_arrow_map_ord"
        _make_table(cursor, t, "a INT NOT NULL, b NVARCHAR(20) NULL")
        src = pa.table({"x": pa.array([1, 2], type=pa.int32()), "y": pa.array(["p", "q"])})
        result = cursor.bulkcopy_arrow(t, src, column_mappings=[(0, "a"), (1, "b")])
        assert result["rows_copied"] == 2
        cursor.execute(f"DROP TABLE {t}")

    # ── representative type matrix ──────────────────────────────────────────

    def test_type_matrix_numeric_and_string(self, cursor):
        t = "mssql_python_arrow_types1"
        _make_table(
            cursor,
            t,
            "b BIT, i8 TINYINT, i16 SMALLINT, i32 INT, i64 BIGINT, "
            "f32 REAL, f64 FLOAT, s NVARCHAR(50), v VARCHAR(50)",
        )
        tbl = pa.table(
            {
                "b": pa.array([True, False], type=pa.bool_()),
                "i8": pa.array([1, 255], type=pa.uint8()),
                "i16": pa.array([1, 30000], type=pa.int16()),
                "i32": pa.array([1, 2_000_000_000], type=pa.int32()),
                "i64": pa.array([1, 9_000_000_000], type=pa.int64()),
                "f32": pa.array([1.5, 2.5], type=pa.float32()),
                "f64": pa.array([1.25, 2.75], type=pa.float64()),
                "s": pa.array(["hello", "wörld"]),
                "v": pa.array(["ascii", "text"]),
            }
        )
        result = cursor.bulkcopy_arrow(t, tbl)
        assert result["rows_copied"] == 2
        cursor.execute(f"SELECT i32 FROM {t} ORDER BY i32")
        assert [r[0] for r in cursor.fetchall()] == [1, 2_000_000_000]
        cursor.execute(f"DROP TABLE {t}")

    def test_type_matrix_temporal_and_decimal(self, cursor):
        import datetime as dt
        from decimal import Decimal

        t = "mssql_python_arrow_types2"
        _make_table(
            cursor,
            t,
            "d DATE, ts DATETIME2, tm TIME, amt DECIMAL(10,2), tzo DATETIMEOFFSET",
        )
        tbl = pa.table(
            {
                "d": pa.array([dt.date(2020, 1, 2)], type=pa.date32()),
                "ts": pa.array([dt.datetime(2020, 1, 2, 3, 4, 5)], type=pa.timestamp("us")),
                "tm": pa.array([dt.time(3, 4, 5)], type=pa.time64("us")),
                "amt": pa.array([Decimal("123.45")], type=pa.decimal128(10, 2)),
                "tzo": pa.array(
                    [dt.datetime(2020, 1, 2, 3, 4, 5)], type=pa.timestamp("us", tz="UTC")
                ),
            }
        )
        result = cursor.bulkcopy_arrow(t, tbl)
        assert result["rows_copied"] == 1
        cursor.execute(f"SELECT amt FROM {t}")
        assert cursor.fetchone()[0] == Decimal("123.45")
        cursor.execute(f"DROP TABLE {t}")

    def test_type_matrix_binary(self, cursor):
        t = "mssql_python_arrow_types3"
        _make_table(cursor, t, "vb VARBINARY(16)")
        tbl = pa.table({"vb": pa.array([b"\x01\x02\x03", b"\xff\xee"], type=pa.binary())})
        result = cursor.bulkcopy_arrow(t, tbl)
        assert result["rows_copied"] == 2
        cursor.execute(f"DROP TABLE {t}")
