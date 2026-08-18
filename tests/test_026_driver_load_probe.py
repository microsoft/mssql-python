"""Fail-closed classification tests for ``conda/driver_load_probe.py``.

The conda test-before-publish gate runs ``conda/driver_load_probe.py`` to prove
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

import importlib.util
import sys
import types
from pathlib import Path

import pytest

_PROBE_PATH = Path(__file__).resolve().parent.parent / "conda" / "driver_load_probe.py"


def _load_probe():
    """Import ``conda/driver_load_probe.py`` as a standalone module."""
    spec = importlib.util.spec_from_file_location("driver_load_probe_under_test", _PROBE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


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
    # Fail-closed default: an unexpected / unrelated error is NOT proof of load.
    "some totally unexpected internal error",
]


@pytest.mark.parametrize("msg", _LOADED_MESSAGES)
def test_driver_loaded_true_for_connection_stage_errors(msg):
    probe = _load_probe()
    assert probe.driver_loaded(RuntimeError(msg)) is True


@pytest.mark.parametrize("msg", _LOAD_FAILURE_MESSAGES)
def test_driver_loaded_false_for_load_failures(msg):
    probe = _load_probe()
    assert probe.driver_loaded(RuntimeError(msg)) is False


def test_driver_loaded_true_for_clean_connect():
    probe = _load_probe()
    assert probe.driver_loaded(None) is True


def _run_main_with_stub(monkeypatch, connect):
    """Run ``probe.main()`` with a stubbed ``mssql_python`` module."""
    probe = _load_probe()
    stub = types.ModuleType("mssql_python")
    stub.connect = connect
    monkeypatch.setitem(sys.modules, "mssql_python", stub)
    return probe


def test_main_exits_nonzero_on_simulated_load_failure(monkeypatch):
    def connect(_conn_str):
        raise RuntimeError(
            "Failed to load the driver. Please read the documentation to install the "
            "required dependencies."
        )

    probe = _run_main_with_stub(monkeypatch, connect)
    with pytest.raises(SystemExit) as excinfo:
        probe.main()
    # sys.exit(<str>) -> non-zero (truthy) exit code carrying the reason.
    assert excinfo.value.code
    assert "DRIVER DID NOT LOAD" in str(excinfo.value.code)


def test_main_passes_on_simulated_network_failure(monkeypatch):
    def connect(_conn_str):
        raise RuntimeError(
            "[Microsoft][ODBC Driver 18 for SQL Server]TCP Provider: No connection could be "
            "made because the target machine actively refused it."
        )

    probe = _run_main_with_stub(monkeypatch, connect)
    # A genuine connection-stage failure must NOT raise SystemExit (exit 0).
    probe.main()


def test_main_passes_on_clean_connect(monkeypatch):
    closed = {"value": False}

    class _Conn:
        def close(self):
            closed["value"] = True

    def connect(_conn_str):
        return _Conn()

    probe = _run_main_with_stub(monkeypatch, connect)
    probe.main()
    assert closed["value"] is True
