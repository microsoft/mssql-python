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


# NOTE: the tls_completed() classifier (clean/login/28000/openssl outcomes) is exercised in
# test_028_tls_connect_probe.py; this module stays focused on the REQUIRED-mode gating so the
# two files don't duplicate that coverage.
