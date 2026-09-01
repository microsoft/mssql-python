"""
Tests for ODBC provider selection (opt-in/opt-out).

Covers the ``ProviderManager`` engine (precedence, normalization, fail-closed
validation, resolve-once freezing, post-freeze warning) and the public surface
(``mssql_python.odbc_provider`` property and ``get_odbc_provider_info()``).
"""

import importlib
import sys

import pytest

import mssql_python
from mssql_python.odbc_provider import (
    ODBC_PROVIDER_ENV_VAR,
    PROVIDER_MSODBCSQL18,
    PROVIDER_MSSQL_ODBC,
    ProviderManager,
)


@pytest.fixture(autouse=True)
def _reset_provider(monkeypatch):
    """Clear provider state and the env var before and after each test."""
    monkeypatch.delenv(ODBC_PROVIDER_ENV_VAR, raising=False)
    ProviderManager._reset_for_testing()
    yield
    ProviderManager._reset_for_testing()


def test_default_is_msodbcsql18():
    assert ProviderManager.effective() == PROVIDER_MSODBCSQL18
    assert ProviderManager.resolve() == PROVIDER_MSODBCSQL18


def test_env_var_selects_provider(monkeypatch):
    monkeypatch.setenv(ODBC_PROVIDER_ENV_VAR, PROVIDER_MSSQL_ODBC)
    assert ProviderManager.resolve() == PROVIDER_MSSQL_ODBC


def test_env_var_is_normalized(monkeypatch):
    monkeypatch.setenv(ODBC_PROVIDER_ENV_VAR, "  MsSql-Odbc  ")
    assert ProviderManager.resolve() == PROVIDER_MSSQL_ODBC


def test_property_used_when_env_unset():
    ProviderManager.set_property(PROVIDER_MSSQL_ODBC)
    assert ProviderManager.resolve() == PROVIDER_MSSQL_ODBC


def test_env_var_takes_precedence_over_property(monkeypatch):
    monkeypatch.setenv(ODBC_PROVIDER_ENV_VAR, PROVIDER_MSODBCSQL18)
    ProviderManager.set_property(PROVIDER_MSSQL_ODBC)
    assert ProviderManager.resolve() == PROVIDER_MSODBCSQL18


def test_empty_env_var_falls_through_to_property(monkeypatch):
    monkeypatch.setenv(ODBC_PROVIDER_ENV_VAR, "   ")
    ProviderManager.set_property(PROVIDER_MSSQL_ODBC)
    assert ProviderManager.resolve() == PROVIDER_MSSQL_ODBC


def test_invalid_property_fails_closed():
    with pytest.raises(ValueError):
        ProviderManager.set_property("classic")


def test_invalid_env_var_fails_closed(monkeypatch):
    monkeypatch.setenv(ODBC_PROVIDER_ENV_VAR, "classic")
    with pytest.raises(ValueError):
        ProviderManager.resolve()


def test_resolve_freezes_selection():
    assert not ProviderManager.is_frozen()
    ProviderManager.resolve()
    assert ProviderManager.is_frozen()
    # A second resolve is stable and does not re-read state.
    assert ProviderManager.resolve() == PROVIDER_MSODBCSQL18


def test_change_after_freeze_is_ignored_with_warning():
    ProviderManager.resolve()  # freezes as default msodbcsql18
    with pytest.warns(RuntimeWarning):
        ProviderManager.set_property(PROVIDER_MSSQL_ODBC)
    assert ProviderManager.effective() == PROVIDER_MSODBCSQL18


def test_same_value_after_freeze_does_not_warn(recwarn):
    ProviderManager.resolve()
    ProviderManager.set_property(PROVIDER_MSODBCSQL18)
    assert len(recwarn) == 0


def test_package_name_mapping():
    assert ProviderManager.package_name(PROVIDER_MSODBCSQL18) == "mssql_python_odbc"
    assert ProviderManager.package_name(PROVIDER_MSSQL_ODBC) == "mssql_python_rust_odbc"


def test_get_info_before_and_after_resolve(monkeypatch):
    info = ProviderManager.get_info()
    assert info["id"] == PROVIDER_MSODBCSQL18
    assert info["package"] == "mssql_python_odbc"
    assert info["frozen"] is False

    monkeypatch.setenv(ODBC_PROVIDER_ENV_VAR, PROVIDER_MSSQL_ODBC)
    ProviderManager.resolve()
    info = ProviderManager.get_info()
    assert info["id"] == PROVIDER_MSSQL_ODBC
    assert info["package"] == "mssql_python_rust_odbc"
    assert info["source"] == "environment"
    assert info["frozen"] is True


def test_public_module_property_get_set():
    assert mssql_python.odbc_provider == PROVIDER_MSODBCSQL18
    mssql_python.odbc_provider = PROVIDER_MSSQL_ODBC
    assert mssql_python.odbc_provider == PROVIDER_MSSQL_ODBC


def test_public_get_odbc_provider_info():
    info = mssql_python.get_odbc_provider_info()
    assert info["id"] == PROVIDER_MSODBCSQL18
    assert info["frozen"] is False


def test_ensure_available_default_ok():
    # The default provider's package (mssql_python_odbc) ships with the driver.
    assert ProviderManager.ensure_available() == PROVIDER_MSODBCSQL18


def test_ensure_available_fails_closed_for_missing_provider(monkeypatch):
    monkeypatch.setenv(ODBC_PROVIDER_ENV_VAR, PROVIDER_MSSQL_ODBC)

    # Force the provider package to appear absent so the test is deterministic
    # regardless of what is installed in the environment.
    real_import = importlib.import_module

    def fake_import(name, *args, **kwargs):
        if name == "mssql_python_rust_odbc":
            raise ModuleNotFoundError(f"No module named '{name}'")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(
        sys.modules[ProviderManager.__module__].importlib, "import_module", fake_import
    )
    with pytest.raises(ImportError) as excinfo:
        ProviderManager.ensure_available()
    message = str(excinfo.value)
    assert PROVIDER_MSSQL_ODBC in message
    assert "mssql-python-rust-odbc" in message
