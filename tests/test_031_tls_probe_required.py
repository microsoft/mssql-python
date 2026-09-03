"""Unit tests for the Encrypt=yes TLS gate's REQUIRED mode (conda/tls_connect_probe.py).

The gate normally skips loudly (exit 0) when ``CONDA_TLS_PROBE_CONN`` is unset or is a
non-connection-string (e.g. a bare ``yes``). On the mandatory minimal-base leg
(``CONDA_TLS_PROBE_REQUIRED=1``) a missing OR malformed value must instead FAIL closed,
so a typo can never silently no-op the one gate that actually exercises OpenSSL.

These tests drive only the pre-import guards (they never reach ``import mssql_python``),
so they run without the compiled driver.
"""

import importlib.util
from pathlib import Path

import pytest

_MODULE_PATH = Path(__file__).resolve().parent.parent / "conda" / "tls_connect_probe.py"

if not _MODULE_PATH.is_file():
    pytest.skip(
        f"tls_connect_probe.py not present ({_MODULE_PATH}); skipping TLS gate tests",
        allow_module_level=True,
    )


def _load_module():
    spec = importlib.util.spec_from_file_location("tls_connect_probe_under_test", _MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


tcp = _load_module()


def test_required_flag_parsing(monkeypatch):
    for truthy in ("1", "true", "yes", "YES", "True"):
        monkeypatch.setenv("CONDA_TLS_PROBE_REQUIRED", truthy)
        assert tcp._required() is True
    for falsy in ("", "0", "no", "off"):
        monkeypatch.setenv("CONDA_TLS_PROBE_REQUIRED", falsy)
        assert tcp._required() is False


def test_required_but_unset_fails(monkeypatch):
    monkeypatch.setenv("CONDA_TLS_PROBE_REQUIRED", "1")
    monkeypatch.delenv("CONDA_TLS_PROBE_CONN", raising=False)
    with pytest.raises(SystemExit):
        tcp.main()


def test_required_but_invalid_fails(monkeypatch):
    # A bare 'yes' (the "I thought it was a yes/no toggle" typo) must FAIL when required.
    monkeypatch.setenv("CONDA_TLS_PROBE_REQUIRED", "1")
    monkeypatch.setenv("CONDA_TLS_PROBE_CONN", "yes")
    with pytest.raises(SystemExit):
        tcp.main()


def test_not_required_unset_skips(monkeypatch, capsys):
    monkeypatch.delenv("CONDA_TLS_PROBE_REQUIRED", raising=False)
    monkeypatch.delenv("CONDA_TLS_PROBE_CONN", raising=False)
    tcp.main()  # returns cleanly (no SystemExit)
    assert "TLS_PROBE_SKIPPED" in capsys.readouterr().out


def test_not_required_invalid_skips(monkeypatch, capsys):
    monkeypatch.delenv("CONDA_TLS_PROBE_REQUIRED", raising=False)
    monkeypatch.setenv("CONDA_TLS_PROBE_CONN", "yes")  # not a connection string
    tcp.main()  # returns cleanly (skip loudly), never reaches the driver import
    assert "TLS_PROBE_SKIPPED" in capsys.readouterr().out


def test_required_ambiguous_value_fails_loud(monkeypatch):
    # A typo like 'tru' must NOT silently disable the mandatory gate -- it fails loud.
    monkeypatch.setenv("CONDA_TLS_PROBE_REQUIRED", "tru")
    with pytest.raises(SystemExit):
        tcp._required()


def test_required_accepts_on_rejects_off(monkeypatch):
    monkeypatch.setenv("CONDA_TLS_PROBE_REQUIRED", "on")
    assert tcp._required() is True
    monkeypatch.setenv("CONDA_TLS_PROBE_REQUIRED", "off")
    assert tcp._required() is False


def test_tls_completed_clean_and_login_phrases():
    assert tcp.tls_completed(None) is True
    assert tcp.tls_completed(Exception("Login failed for user 'sa'.")) is True
    assert tcp.tls_completed(Exception("Cannot open database 'x' requested by the login.")) is True


def test_tls_completed_bare_18456_is_not_a_pass():
    # A pre-TLS network error whose text merely contains 18456 (a port/IP/id) must NOT
    # classify as a completed handshake -- the exact false-positive this guards.
    assert tcp.tls_completed(Exception("TCP timeout connecting to 10.18.45.6:18456")) is False


def test_tls_completed_18456_with_login_context_passes():
    assert tcp.tls_completed(Exception("Login failed (error 18456, SQLSTATE 28000)")) is True
    assert tcp.tls_completed(Exception("SQLSTATE 28000: 18456")) is True


def test_tls_completed_openssl_error_fails_closed():
    assert (
        tcp.tls_completed(Exception("SSL Provider: cannot open shared object libssl.so.3")) is False
    )
