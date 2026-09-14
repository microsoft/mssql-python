"""Port of reviewed controls: real project paths, explicit hash-verified c8e baseline."""

import concurrent.futures
import json
from pathlib import Path
import sys
import threading
import traceback
import types
import unittest.mock as mock

from contracts import (
    AUTH,
    NAME,
    SCOPE,
    source_pair,
    save,
    configure_source_imports,
    import_source_package,
)

ROOT = configure_source_imports(__file__)
mssql_python = import_source_package(ROOT, "mssql_python")
from mssql_python.connection import Connection


def trial(source, variant, locus):
    module = types.ModuleType("qualification_auth")
    exec(compile(source, str(Path.cwd() / AUTH), "exec"), module.__dict__)
    test = getattr(module.TestCustomTokenProviderConnect(), NAME)
    published = threading.Event()
    release = threading.Event()
    initialized = threading.Event()
    barrier = threading.Barrier(8)
    local = threading.local()
    lock = threading.Lock()
    records, failures, events = [], [], []
    target = constructor = provider = None
    real_connect = mssql_python.connect
    real_close = Connection.close
    real_set = mock._set_return_value

    class Executor(concurrent.futures.ThreadPoolExecutor):
        def map(self, function, iterable):
            # Diagnostic only: drain all futures so the expected failure cannot cancel work.
            futures = [self.submit(function, item) for item in iterable]
            _, pending = concurrent.futures.wait(futures, timeout=30)
            assert not pending, "Control operations exceeded deadline"
            return (future.result(timeout=10) for future in futures)

        def __enter__(self):
            nonlocal target, constructor
            assert self._max_workers == 8
            constructor = mssql_python.connection.ddbc_bindings.Connection
            if variant == "old":
                shared = constructor.return_value
                if locus == "connection":
                    target = shared
                else:
                    shared.__bool__.return_value = True
                    target = shared.get_autocommit.return_value
                assert isinstance(type(target).__dict__["__bool__"], mock.MagicProxy)
            return super().__enter__()

    def connect(conn_str, **kwargs):
        nonlocal provider
        index = int(conn_str.split(";")[0].removeprefix("Server=test"))
        local.index = index
        cred = kwargs["token_provider"]
        with lock:
            if provider is None:
                provider = cred
            assert provider is cred
        conn = real_connect(conn_str, **kwargs)
        native = conn._conn
        with lock:
            records.append((index, conn, native))
        if variant == "fixed":
            assert native.__bool__.return_value is True
            assert native.get_autocommit.return_value is False
        if index < 8:
            barrier.wait(10)
        return conn

    def close(conn):
        index = local.index
        if variant == "old":
            if index == 1:
                assert published.wait(10), "No publication"
            elif index >= 2:
                assert initialized.wait(10), "No completed initialization"
        try:
            return real_close(conn)
        except TypeError as exc:
            frames = traceback.extract_tb(exc.__traceback__)
            with lock:
                failures.append(
                    {
                        "index": index,
                        "type": type(exc).__name__,
                        "message": str(exc),
                        "project_lines": [
                            f.line for f in frames if Path(f.filename).name == "connection.py"
                        ],
                    }
                )
            if index == 1:
                release.set()
            raise

    def set_return(parent, method, name):
        if parent is target and name == "__bool__":
            assert type(parent).__dict__["__bool__"] is method
            assert method.__dict__["_mock_return_value"] is mock.DEFAULT
            events.append("published_before_default")
            published.set()
            assert release.wait(10), "Observer did not release initializer"
            real_set(parent, method, name)
            assert method.__dict__["_mock_return_value"] is True
            events.append("initialized")
            initialized.set()
        else:
            real_set(parent, method, name)

    error = None
    with (
        mock.patch.object(module, "ThreadPoolExecutor", Executor),
        mock.patch.object(mssql_python, "connect", connect),
        mock.patch.object(Connection, "close", close),
        mock.patch.object(mock, "_set_return_value", set_return),
    ):
        try:
            test()
        except TypeError as exc:
            error = str(exc)

    natives = {id(n): n for _, _, n in records}
    rollback = sum(n.rollback.call_count for n in natives.values())
    closes = sum(n.close.call_count for n in natives.values())
    row = {
        "variant": variant,
        "locus": locus,
        "operations": len(records),
        "constructor_calls": constructor.call_count,
        "constructor_args": len(constructor.call_args_list),
        "provider_calls": provider.get_token.call_count,
        "provider_args": len(provider.get_token.call_args_list),
        "scopes_valid": all(
            c.args == (SCOPE,) and c.kwargs == {} for c in provider.get_token.call_args_list
        ),
        "distinct_doubles": len(natives),
        "rollback_calls": rollback,
        "close_calls": closes,
        "failures": failures,
        "events": events,
        "error": error,
    }
    # Persist even the trial that fails counter validation.
    with Path(sys.argv[1]).open("a", encoding="utf-8") as output:
        output.write(json.dumps(row) + "\n")
    assert len(records) == 20, row
    assert constructor.call_count == len(constructor.call_args_list) == 20, row
    assert provider.get_token.call_count == len(provider.get_token.call_args_list) == 20, row
    assert row["scopes_valid"], row
    if variant == "old":
        assert error == "__bool__ should return bool, returned MagicMock", row
        assert len(failures) == 1 and failures[0]["index"] == 1, row
        expected = "if self._conn:" if locus == "connection" else "if not self.autocommit:"
        assert expected in failures[0]["project_lines"], row
        assert len(natives) == 1 and closes == 19 and rollback == 0, row
        assert events == ["published_before_default", "initialized"], row
    else:
        assert error is None and not failures, row
        assert len(natives) == 20 and rollback == closes == 20, row
        for native in natives.values():
            native.set_autocommit.assert_called_once_with(False)
            native.rollback.assert_called_once_with()
            native.close.assert_called_once_with()
    return row


def main():
    old, fixed = source_pair(Path.cwd())
    output = Path(sys.argv[1])
    assert not output.exists(), "Do not append to prior trials"
    results = [
        trial(source, variant, locus)
        for variant, source in (("old", old), ("fixed", fixed))
        for locus in ("connection", "autocommit")
        for _ in range(25)
    ]
    assert len(results) == 100
    save(
        output.with_suffix(".json"),
        {
            "trials": results,
            "schedule": "first-eight barrier; publication events; drain all futures",
            "fixtures": "direct decorated methods; no pytest fixtures in controls",
            "manufactured_bad_returns": False,
            "shipping_executor_map_unchanged": True,
        },
    )


if __name__ == "__main__":
    if sys.argv[1:] == ["--verify-source-imports"]:
        print(json.dumps({"root": str(ROOT), "package_origin": mssql_python.__file__}))
    else:
        main()
