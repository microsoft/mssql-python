"""Temporal fetch parity, including cached constructors in fresh interpreters."""

import datetime
from decimal import Decimal
import gc
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid

import pytest

MODES = ("default", "custom", "date", "time", "datetime", "uuid")
APIS = ("fetchone", "fetchmany", "fetchall", "iteration")


def _assert_native_origin(package_file, native_file, root, *, require_source=False):
    package_path = Path(package_file).resolve()
    native_path = Path(native_file).resolve()
    assert package_path.is_file(), f"Imported package does not exist: {package_path}"
    assert native_path.is_file(), f"Imported native module does not exist: {native_path}"
    assert (
        native_path.parent == package_path.parent
    ), f"Native module {native_path} is outside imported package {package_path.parent}"
    assert native_path.name.startswith("ddbc_bindings.") and native_path.suffix in (
        ".so",
        ".pyd",
    ), f"Expected the compiled native module, not its Python loader: {native_path}"
    source_package = (root / "mssql_python" / "__init__.py").resolve()
    # Explicit snapshots and source checkouts must not silently import an installed wheel.
    if require_source or source_package.is_file():
        assert (
            package_path == source_package
        ), f"Expected source package {source_package}, imported {package_path}"


@pytest.mark.parametrize("layout", ("source", "explicit-source", "installed-wheel"))
@pytest.mark.parametrize("suffix", (".so", ".pyd"))
def test_native_origin_accepts_package_layout(tmp_path, layout, suffix):
    root = tmp_path / "tests-root"
    location = root if layout != "installed-wheel" else tmp_path / "site-packages"
    package = location / "mssql_python" / "__init__.py"
    package.parent.mkdir(parents=True)
    package.touch()
    native = package.with_name(f"ddbc_bindings.cp313-test{suffix}")
    native.touch()
    _assert_native_origin(package, native, root, require_source=layout == "explicit-source")


@pytest.mark.parametrize(
    "problem, message",
    (
        ("missing-native", "native module does not exist"),
        ("foreign-native", "outside imported package"),
        ("python-loader", "Expected the compiled native module"),
        ("shadowed-source", "Expected source package"),
        ("missing-explicit-source", "Expected source package"),
    ),
)
def test_native_origin_rejects_mismatched_package(tmp_path, problem, message):
    root = tmp_path / "tests-root"
    package = tmp_path / "site-packages" / "mssql_python" / "__init__.py"
    package.parent.mkdir(parents=True)
    package.touch()
    native = package.with_name("ddbc_bindings.cp313-test.so")
    if problem == "foreign-native":
        native = tmp_path / native.name
    elif problem == "python-loader":
        native = package.with_name("ddbc_bindings.py")
    if problem != "missing-native":
        native.touch()
    if problem == "shadowed-source":
        source_package = root / "mssql_python" / "__init__.py"
        source_package.parent.mkdir(parents=True)
        source_package.touch()
    with pytest.raises(AssertionError, match=message):
        _assert_native_origin(
            package, native, root, require_source=problem == "missing-explicit-source"
        )


@pytest.mark.parametrize("mode", MODES)
def test_fetch_temporal_constructors(mode):
    if not os.environ.get("DB_CONNECTION_STRING"):
        pytest.skip("DB_CONNECTION_STRING is required")
    result = subprocess.run(
        [sys.executable, str(Path(__file__).resolve()), mode],
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=120,
        env={**os.environ, "PYTHONIOENCODING": "utf-8"},
    )
    assert result.returncode == 0, result.stdout + result.stderr
    report = json.loads(result.stdout)
    assert report["mode"] == mode
    assert report["checks"] == 24


def _drain(cursor, method):
    if method == "iteration":
        return [tuple(row) for row in cursor]
    result = []
    while True:
        if method == "fetchone":
            row = cursor.fetchone()
            batch = [] if row is None else [row]
        elif method == "fetchmany":
            batch = cursor.fetchmany(2)
        else:
            batch = cursor.fetchall()
        if not batch:
            return result
        result.extend(tuple(row) for row in batch)


def _probe(mode, root, *, require_source=False):
    originals = {
        "date": datetime.date,
        "time": datetime.time,
        "datetime": datetime.datetime,
        "uuid": uuid.UUID,
    }
    replacements = dict(originals)
    calls = []
    state = {"record": False, "raise": None}

    class ConstructorFailure(Exception):
        pass

    failure = ConstructorFailure("cached constructor failure")

    def record(name, positional, keywords):
        if state["record"]:
            calls.append((name, positional, keywords))
            if state["raise"] == name:
                raise failure

    if mode != "default":

        def substitute(name, original):
            class RecordingTemporal(original):
                def __new__(cls, *positional, **keywords):
                    record(name, positional, keywords)
                    return original.__new__(cls, *positional, **keywords)

            return RecordingTemporal

        for name in ("date", "time", "datetime"):
            replacements[name] = substitute(name, originals[name])
            setattr(datetime, name, replacements[name])

        class RecordingUUID(originals["uuid"]):
            def __init__(self, *, bytes):
                record("uuid", (), {"bytes": bytes})
                super().__init__(bytes=bytes)

        replacements["uuid"] = RecordingUUID
        uuid.UUID = RecordingUUID

    # Substitutions must precede native module initialization, not just connection creation.
    sys.path.insert(0, str(root))
    import mssql_python

    _assert_native_origin(
        mssql_python.__file__,
        mssql_python.ddbc_bindings.module.__file__,
        root,
        require_source=require_source,
    )
    mssql_python.native_uuid = True
    query = """
        SELECT CAST(v.d AS date) AS d, CAST(v.t AS time(7)) AS t,
               CAST(v.ts AS datetime2(7)) AS ts, CAST(v.u AS uniqueidentifier) AS u,
               CAST(v.n AS nvarchar(64)) AS n, CAST(v.v AS varchar(64)) AS v,
               CAST(v.b AS varbinary(64)) AS b,
               CASE WHEN v.id = 4 THEN NULL ELSE CAST(123.4567 AS decimal(12,4)) END AS dec,
               CASE WHEN v.id = 4 THEN NULL ELSE
                 CAST('2024-02-29T12:34:56.1234567+05:30' AS datetimeoffset(7)) END AS dto
               {extra}
        FROM (VALUES
          (1, '0001-01-01', '00:00:00.0000000', '0001-01-01T00:00:00.0000000',
           '00112233-4455-6677-8899-aabbccddeeff', N'A' + NCHAR(0) + N'\U0001f642', 'abc', 0x00FF),
          (2, '2000-02-29', '12:34:56.1234567', '2000-02-29T12:34:56.1234567',
           'ffffffff-ffff-ffff-ffff-ffffffffffff', N'', '', 0x),
          (3, '9999-12-31', '23:59:59.9999999', '9999-12-31T23:59:59.9999999',
           '00000000-0000-0000-0000-000000000000', N'caf\u00e9', 'xyz', 0x000100),
          (4, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
        ) AS v(id, d, t, ts, u, n, v, b)
        ORDER BY v.id
    """
    date, time, timestamp, guid = (originals[k] for k in ("date", "time", "datetime", "uuid"))
    dto = timestamp(
        2024, 2, 29, 12, 34, 56, 123456, datetime.timezone(datetime.timedelta(minutes=330))
    )
    expected = [
        (
            date(1, 1, 1),
            time(),
            timestamp(1, 1, 1),
            guid("00112233-4455-6677-8899-aabbccddeeff"),
            "A\0\U0001f642",
            "abc",
            b"\0\xff",
        ),
        (
            date(2000, 2, 29),
            time(12, 34, 56, 123456),
            timestamp(2000, 2, 29, 12, 34, 56, 123456),
            guid("ffffffff-ffff-ffff-ffff-ffffffffffff"),
            "",
            "",
            b"",
        ),
        (
            date(9999, 12, 31),
            time(23, 59, 59, 999999),
            timestamp(9999, 12, 31, 23, 59, 59, 999999),
            guid("00000000-0000-0000-0000-000000000000"),
            "caf\u00e9",
            "xyz",
            b"\0\1\0",
        ),
    ]
    expected = [row + (Decimal("123.4567"), dto) for row in expected] + [(None,) * 9]
    checks = 0
    recovery_checks = 0
    with mssql_python.connect(os.environ["DB_CONNECTION_STRING"]) as connection:
        # The ASCII VARCHAR control also exercises SQL_CHAR without the Windows UTF-8 upgrade.
        for encoding, ctype in (
            ("utf-16le", mssql_python.SQL_WCHAR),
            ("latin-1", mssql_python.SQL_CHAR),
        ):
            connection.setdecoding(mssql_python.SQL_CHAR, encoding=encoding, ctype=ctype)
            for forced_lob in (False, True):
                extra = ", CAST(N'lob' AS nvarchar(max)) AS force_lob" if forced_lob else ""
                wanted = [row + (("lob",) if forced_lob else ()) for row in expected]
                for method in APIS:
                    with connection.cursor() as cursor:
                        cursor.execute(query.format(extra=extra))
                        if mode not in ("default", "custom"):
                            state.update(record=True, **{"raise": mode})
                            try:
                                _drain(cursor, method)
                            except ConstructorFailure as error:
                                assert error is failure
                            else:
                                raise AssertionError("Constructor failure was swallowed")
                            finally:
                                state.update(record=False, **{"raise": None})
                            recovery_checks += 1
                            cursor.execute(query.format(extra=extra))
                        calls.clear()
                        state["record"] = True
                        try:
                            actual = _drain(cursor, method)
                        finally:
                            state["record"] = False
                        assert actual == wanted, (method, encoding, forced_lob, actual)
                        for row in actual[:3]:
                            for index, name in enumerate(("date", "time", "datetime", "uuid")):
                                assert type(row[index]) is replacements[name], (method, name)
                            for index in (1, 2):
                                assert row[index].tzinfo is None and row[index].fold == 0
                            assert type(row[7]) is Decimal
                            assert type(row[8]) is replacements["datetime"]
                            assert row[8].utcoffset() == datetime.timedelta(minutes=330)
                            assert row[8].fold == 0
                            assert all(type(row[i]) is str for i in (4, 5))
                            assert type(row[6]) is bytes
                        if mode != "default":
                            for name, count in (("date", 3), ("time", 4), ("datetime", 7)):
                                recorded = [
                                    call for call in calls if call[0] == name and len(call[1]) != 8
                                ]
                                assert len(recorded) == 3, (method, name, recorded)
                                assert all(len(p) == count and not k for _, p, k in recorded)
                            dto_calls = [call for call in calls if len(call[1]) == 8]
                            assert len(dto_calls) == 3
                            assert all(n == "datetime" and not k for n, _, k in dto_calls)
                            uuid_calls = [call for call in calls if call[0] == "uuid"]
                            assert len(uuid_calls) == 3
                            assert [k["bytes"] for _, _, k in uuid_calls] == [
                                row[3].bytes for row in wanted[:3]
                            ]
                            assert all(not p and set(k) == {"bytes"} for _, p, k in uuid_calls)
                        checks += 1
                    gc.collect()

        legacy_query = """
            SELECT CAST(v.dt AS datetime), CAST(v.small AS smalldatetime) {extra}
            FROM (VALUES
              (1, '1753-01-01T00:00:00.000', '1900-01-01T00:00:00'),
              (2, '9999-12-31T23:59:59.997', '2079-06-06T23:59:00'),
              (3, NULL, NULL)
            ) AS v(id, dt, small) ORDER BY v.id
        """
        legacy = [
            (timestamp(1753, 1, 1), timestamp(1900, 1, 1)),
            (timestamp(9999, 12, 31, 23, 59, 59, 997000), timestamp(2079, 6, 6, 23, 59)),
            (None, None),
        ]
        for forced_lob in (False, True):
            extra = ", CAST(N'lob' AS nvarchar(max))" if forced_lob else ""
            wanted = [row + (("lob",) if forced_lob else ()) for row in legacy]
            for method in APIS:
                with connection.cursor() as cursor:
                    cursor.execute(legacy_query.format(extra=extra))
                    calls.clear()
                    state["record"] = True
                    try:
                        actual = _drain(cursor, method)
                    finally:
                        state["record"] = False
                    assert actual == wanted
                    for row in actual[:2]:
                        for value in row[:2]:
                            assert type(value) is replacements["datetime"]
                            assert value.tzinfo is None and value.fold == 0
                    if mode != "default":
                        assert len(calls) == 4
                        assert all(n == "datetime" and len(p) == 7 and not k for n, p, k in calls)
                    checks += 1
    assert recovery_checks == (16 if mode not in ("default", "custom") else 0)
    print(json.dumps({"mode": mode, "checks": checks, "recovery_checks": recovery_checks}))


if __name__ == "__main__":
    root = Path(sys.argv[2]).resolve() if len(sys.argv) > 2 else Path(__file__).resolve().parents[1]
    _probe(sys.argv[1], root, require_source=len(sys.argv) > 2)
