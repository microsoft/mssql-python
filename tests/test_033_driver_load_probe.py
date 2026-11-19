"""Fail-closed classification tests for ``eng.conda_tools.driver_load_probe``.

The conda test-before-publish gate runs the probe's source file directly to prove
the repackaged native ODBC driver actually loads (not just the tiny
``mssql_python_odbc`` shim). The probe MUST fail closed: a broken / missing /
mis-architecture driver -- whose failure surfaces as the C++
``LoadDriverOrThrowException`` family ("Failed to load the driver...", "Failed
to load library: <path>", "Failed to load required function pointers...") -- has
to make the probe exit non-zero, while a genuine connection-stage failure
(driver loaded, TCP/TLS/auth attempted) has to pass.

These are pure, no-DB unit tests: the probe's native ``import mssql_python`` is
deferred into ``main()``, so the classifier can be loaded and exercised with a
stubbed connector without the compiled extension or a live SQL Server.
"""

import builtins
import os
import subprocess
import sys
import types
from pathlib import Path

import pytest

_TOOLS_DIR = Path(__file__).resolve().parent.parent / "eng" / "conda_tools"
_PROBE_PATH = _TOOLS_DIR / "driver_load_probe.py"

# The engineering sources are not shipped inside the built wheel, so the installed-wheel
# test leg copies only tests/ into an isolated dir. Skip the whole module (rather than
# erroring at collection/run) only when the source tree is absent, not when it is broken.
if not _TOOLS_DIR.is_dir():
    pytest.skip(
        f"Conda tooling source not present ({_TOOLS_DIR}); skipping driver-load probe tests",
        allow_module_level=True,
    )

from eng.conda_tools import driver_load_probe as probe

# Messages the loaded msodbcsql driver emits once it has reached the network /
# TLS / auth stage. Every one of these MUST classify as "driver loaded" (PASS).
_LOADED_MESSAGES = [
    "Driver Error: Connection operation failed; DDBC Error: [Microsoft][ODBC Driver 18 for "
    "SQL Server]TCP Provider: No connection could be made because the target machine actively "
    "refused it.",
    "[Microsoft][ODBC Driver 18 for SQL Server]Login timeout expired",
    "[Microsoft][ODBC Driver 18 for SQL Server]TCP Provider: Error code 0x2726",
    "[Microsoft][ODBC Driver 18 for SQL Server]A network-related or instance-specific error "
    "has occurred",
    "[Microsoft][ODBC Driver 18 for SQL Server]SSL Provider: certificate verify failed",
    "[Microsoft][ODBC Driver 18 for SQL Server]Login failed for user 'x'.",
    "connection refused",
]

# Messages that mean the native driver did NOT load / link / resolve. Every one
# of these MUST classify as "not loaded" (FAIL / non-zero exit).
_LOAD_FAILURE_MESSAGES = [
    "Failed to load the driver. Please read the documentation "
    "(https://github.com/microsoft/mssql-python#installation) to install the required "
    "dependencies.",
    "Failed to load library: C:\\x\\msodbcsql18.dll",
    "Failed to load required function pointers from driver.",
    "ODBC driver not found at: /x/libmsodbcsql-18.5.so.2.1",
    "Failed to load mssql-auth.dll. Please ensure it is present in the expected directory.",
    "mssql-auth.dll not found. If you are using Entra ID, please ensure it is present.",
    "The mssql-python-odbc package (which ships the ODBC driver binaries) is not installed.",
    "dlopen(...): image not found",
    "libcrypto.so.3: cannot open shared object file: No such file or directory",
    "Unsupported architecture",
    "Failed to load certificate helper library",
    # Fail-closed default: an unexpected / unrelated error is NOT proof of load.
    "some totally unexpected internal error",
]


@pytest.mark.parametrize("msg", _LOADED_MESSAGES)
def test_driver_loaded_true_for_connection_stage_errors(msg):
    assert probe.driver_loaded(RuntimeError(msg)) is True


@pytest.mark.parametrize("msg", _LOAD_FAILURE_MESSAGES)
def test_driver_loaded_false_for_load_failures(msg):
    assert probe.driver_loaded(RuntimeError(msg)) is False


def test_driver_loaded_true_for_clean_connect():
    assert probe.driver_loaded(None) is True


def _stub_connector(monkeypatch, connect):
    """Replace only the connector and restore the probe's environment after each test."""
    stub = types.ModuleType("mssql_python")
    stub.connect = connect
    monkeypatch.setitem(sys.modules, "mssql_python", stub)
    monkeypatch.setenv(
        probe._NATIVE_PROVIDER_ENV_VAR,
        os.environ.get(probe._NATIVE_PROVIDER_ENV_VAR, probe._REQUIRED_NATIVE_PROVIDER),
    )


def test_connection_outcome_returns_error_without_reporting(monkeypatch, capsys):
    error = RuntimeError("connection refused")

    def connect(**_kwargs):
        raise error

    _stub_connector(monkeypatch, connect)
    assert probe._connection_outcome() is error
    captured = capsys.readouterr()
    assert (captured.out, captured.err) == ("", "")


@pytest.mark.parametrize(
    ("message", "returncode", "marker"),
    [
        ("ODBC Driver 18 for SQL Server: connection refused", 0, "DRIVER_LOADED"),
        ("Failed to load required function pointers from driver.", 1, "DRIVER DID NOT LOAD"),
    ],
)
def test_probe_file_runs_in_target_interpreter_without_eng(tmp_path, message, returncode, marker):
    script = """
import builtins, os, runpy, sys, types
original_import = builtins.__import__
def guarded_import(name, *args, **kwargs):
    assert name != "eng" and not name.startswith("eng."), "target imported engineering tooling"
    if name == "mssql_python":
        assert os.environ["MSSQL_PYTHON_NATIVE_PROVIDER"] == "msodbcsql18"
    return original_import(name, *args, **kwargs)
builtins.__import__ = guarded_import
driver = types.ModuleType("mssql_python")
def connect(**kwargs):
    assert kwargs["Server"] == "127.0.0.1,1"
    raise RuntimeError(sys.argv[2])
driver.connect = connect
sys.modules["mssql_python"] = driver
os.environ["MSSQL_PYTHON_NATIVE_PROVIDER"] = "mssql-odbc"
runpy.run_path(sys.argv[1], run_name="__main__")
"""
    result = subprocess.run(
        [sys.executable, "-I", "-c", script, str(_PROBE_PATH), message],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == returncode, result.stdout + result.stderr
    assert marker in result.stdout + result.stderr


def test_main_exits_nonzero_on_simulated_load_failure(monkeypatch):
    def connect(**_kwargs):
        raise RuntimeError(
            "Failed to load the driver. Please read the documentation to install the "
            "required dependencies."
        )

    _stub_connector(monkeypatch, connect)
    with pytest.raises(SystemExit) as excinfo:
        probe.main()
    # sys.exit(<str>) -> non-zero (truthy) exit code carrying the reason.
    assert excinfo.value.code
    assert "DRIVER DID NOT LOAD" in str(excinfo.value.code)


def test_main_passes_on_simulated_network_failure(monkeypatch):
    def connect(**_kwargs):
        raise RuntimeError(
            "[Microsoft][ODBC Driver 18 for SQL Server]TCP Provider: No connection could be "
            "made because the target machine actively refused it."
        )

    _stub_connector(monkeypatch, connect)
    # A genuine connection-stage failure must NOT raise SystemExit (exit 0).
    probe.main()


def test_main_overrides_inherited_alternative_provider(monkeypatch):
    monkeypatch.setenv("MSSQL_PYTHON_NATIVE_PROVIDER", "mssql-odbc")
    real_import = builtins.__import__
    imported = {"mssql_python": False}

    def checked_import(name, *args, **kwargs):
        if name == "mssql_python":
            imported["mssql_python"] = True
            assert os.environ["MSSQL_PYTHON_NATIVE_PROVIDER"] == "msodbcsql18"
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", checked_import)

    def connect(**_kwargs):
        assert os.environ["MSSQL_PYTHON_NATIVE_PROVIDER"] == "msodbcsql18"
        raise RuntimeError(
            "[Microsoft][ODBC Driver 18 for SQL Server]TCP Provider: connection refused"
        )

    _stub_connector(monkeypatch, connect)
    probe.main()
    assert imported["mssql_python"] is True


def test_main_passes_on_clean_connect(monkeypatch):
    closed = {"value": False}

    class _Conn:
        def close(self):
            closed["value"] = True

    def connect(**_kwargs):
        return _Conn()

    _stub_connector(monkeypatch, connect)
    probe.main()
    assert closed["value"] is True


def test_cleanup_failure_preserves_successful_driver_load(monkeypatch, capsys):
    class Connection:
        def close(self):
            raise RuntimeError("cleanup failed after a successful connection")

    _stub_connector(monkeypatch, lambda **_kwargs: Connection())
    probe.main()
    captured = capsys.readouterr()
    assert (captured.out, captured.err) == ("DRIVER_LOADED (clean connect)\n", "")


def test_native_import_failure_is_not_hidden(monkeypatch):
    _stub_connector(monkeypatch, lambda **_kwargs: None)
    original_import = builtins.__import__

    def fail_native_import(name, *args, **kwargs):
        if name == "mssql_python":
            raise ImportError("native extension unavailable")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fail_native_import)
    with pytest.raises(ImportError, match="native extension unavailable"):
        probe.main()


def test_main_passes_complete_structured_connection_parameters(monkeypatch):
    captured = {}

    def connect(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        raise RuntimeError(
            "[Microsoft][ODBC Driver 18 for SQL Server]TCP Provider: connection refused"
        )

    _stub_connector(monkeypatch, connect)
    probe.main()

    assert captured == {
        "args": (),
        "kwargs": {
            "Server": "127.0.0.1,1",  # DevSkim: ignore DS162092 - asserted loopback probe
            "Database": "x",
            "Trusted_Connection": "yes",
            "Encrypt": "no",
            "TrustServerCertificate": "yes",
        },
    }
