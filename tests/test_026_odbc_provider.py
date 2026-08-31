"""
Tests for ODBC provider selection (opt-in/opt-out).

Covers the ``ProviderManager`` engine (precedence, normalization, fail-closed
validation, resolve-once freezing, post-freeze warning) and the public surface
(``mssql_python.native_provider`` property and ``get_native_provider_info()``).
"""

import importlib
import os
import subprocess
import sys
import textwrap

import pytest

import mssql_python
from mssql_python.native_provider import (
    NATIVE_PROVIDER_ENV_VAR,
    PROVIDER_MSODBCSQL18,
    PROVIDER_MSSQL_ODBC,
    ProviderManager,
)


@pytest.fixture(autouse=True)
def _reset_provider(monkeypatch):
    """Clear provider state and the env var before and after each test."""
    monkeypatch.delenv(NATIVE_PROVIDER_ENV_VAR, raising=False)
    ProviderManager._reset_for_testing()
    yield
    ProviderManager._reset_for_testing()


def test_default_is_msodbcsql18():
    assert ProviderManager.effective() == PROVIDER_MSODBCSQL18
    assert ProviderManager.resolve() == PROVIDER_MSODBCSQL18


def test_env_var_selects_provider(monkeypatch):
    monkeypatch.setenv(NATIVE_PROVIDER_ENV_VAR, PROVIDER_MSSQL_ODBC)
    assert ProviderManager.resolve() == PROVIDER_MSSQL_ODBC


def test_env_var_is_normalized(monkeypatch):
    monkeypatch.setenv(NATIVE_PROVIDER_ENV_VAR, "  MsSql-Odbc  ")
    assert ProviderManager.resolve() == PROVIDER_MSSQL_ODBC


def test_property_used_when_env_unset():
    ProviderManager.set_property(PROVIDER_MSSQL_ODBC)
    assert ProviderManager.resolve() == PROVIDER_MSSQL_ODBC


def test_env_var_takes_precedence_over_property(monkeypatch):
    monkeypatch.setenv(NATIVE_PROVIDER_ENV_VAR, PROVIDER_MSODBCSQL18)
    ProviderManager.set_property(PROVIDER_MSSQL_ODBC)
    assert ProviderManager.resolve() == PROVIDER_MSODBCSQL18


def test_empty_env_var_falls_through_to_property(monkeypatch):
    monkeypatch.setenv(NATIVE_PROVIDER_ENV_VAR, "   ")
    ProviderManager.set_property(PROVIDER_MSSQL_ODBC)
    assert ProviderManager.resolve() == PROVIDER_MSSQL_ODBC


def test_invalid_property_fails_closed():
    with pytest.raises(ValueError):
        ProviderManager.set_property("classic")


def test_invalid_env_var_fails_closed(monkeypatch):
    monkeypatch.setenv(NATIVE_PROVIDER_ENV_VAR, "classic")
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
    assert not [w for w in recwarn if issubclass(w.category, RuntimeWarning)]


def test_package_name_mapping():
    assert ProviderManager.package_name(PROVIDER_MSODBCSQL18) == "mssql_python_odbc"
    assert ProviderManager.package_name(PROVIDER_MSSQL_ODBC) == "mssql_python_rust_odbc"


def test_get_info_before_and_after_resolve(monkeypatch):
    info = ProviderManager.get_info()
    assert info["id"] == PROVIDER_MSODBCSQL18
    assert info["package"] == "mssql_python_odbc"
    assert info["frozen"] is False

    monkeypatch.setenv(NATIVE_PROVIDER_ENV_VAR, PROVIDER_MSSQL_ODBC)
    ProviderManager.resolve()
    info = ProviderManager.get_info()
    assert info["id"] == PROVIDER_MSSQL_ODBC
    assert info["package"] == "mssql_python_rust_odbc"
    assert info["source"] == "environment"
    assert info["frozen"] is True


def test_public_module_property_get_set():
    assert mssql_python.native_provider == PROVIDER_MSODBCSQL18
    mssql_python.native_provider = PROVIDER_MSSQL_ODBC
    assert mssql_python.native_provider == PROVIDER_MSSQL_ODBC


def test_public_get_native_provider_info():
    info = mssql_python.get_native_provider_info()
    assert info["id"] == PROVIDER_MSODBCSQL18
    assert info["frozen"] is False


def test_ensure_available_default_ok():
    # The default provider's package (mssql_python_odbc) ships with the driver.
    assert ProviderManager.ensure_available() == PROVIDER_MSODBCSQL18


def test_ensure_available_fails_closed_for_missing_provider(monkeypatch):
    monkeypatch.setenv(NATIVE_PROVIDER_ENV_VAR, PROVIDER_MSSQL_ODBC)

    # Force the provider package to appear absent so the test is deterministic
    # regardless of what is installed in the environment.
    real_import = importlib.import_module

    def fake_import(name, *args, **kwargs):
        if name == "mssql_python_rust_odbc":
            raise ModuleNotFoundError(f"No module named '{name}'", name=name)
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(
        sys.modules[ProviderManager.__module__].importlib, "import_module", fake_import
    )
    with pytest.raises(ImportError) as excinfo:
        ProviderManager.ensure_available()
    message = str(excinfo.value)
    assert PROVIDER_MSSQL_ODBC in message
    assert "mssql-python-rust-odbc" in message


def test_ensure_available_reraises_unrelated_import_error(monkeypatch):
    # A ModuleNotFoundError from a transitive import (name != provider package)
    # must surface as-is, not be rewritten into a misleading "not installed" hint.
    def fake_import(name, *args, **kwargs):
        raise ModuleNotFoundError(
            "No module named 'some_transitive_dep'", name="some_transitive_dep"
        )

    monkeypatch.setattr(
        sys.modules[ProviderManager.__module__].importlib, "import_module", fake_import
    )
    with pytest.raises(ModuleNotFoundError) as excinfo:
        ProviderManager.ensure_available()
    assert excinfo.value.name == "some_transitive_dep"


def test_effective_does_not_raise_on_bad_env(monkeypatch):
    # Read-only path: a bad env var reports the default rather than raising, so
    # plain attribute access / `import *` never fail at import.
    monkeypatch.setenv(NATIVE_PROVIDER_ENV_VAR, "classic")
    assert ProviderManager.effective() == PROVIDER_MSODBCSQL18
    assert mssql_python.native_provider == PROVIDER_MSODBCSQL18


def test_get_info_reports_source_before_resolve(monkeypatch):
    # Source is populated pre-freeze, not only after resolve().
    monkeypatch.setenv(NATIVE_PROVIDER_ENV_VAR, PROVIDER_MSSQL_ODBC)
    info = ProviderManager.get_info()
    assert info["id"] == PROVIDER_MSSQL_ODBC
    assert info["source"] == "environment"
    assert info["frozen"] is False


def test_get_info_reports_error_on_bad_env(monkeypatch):
    # A bad env var is surfaced via an `error` key, not an exception.
    monkeypatch.setenv(NATIVE_PROVIDER_ENV_VAR, "classic")
    info = ProviderManager.get_info()
    assert "error" in info
    assert "classic" in info["error"]
    assert info["frozen"] is False


def test_pooling_enable_does_not_freeze_provider(monkeypatch):
    # enable_pooling() only configures pool state; it must not resolve or freeze
    # the provider (that is the first Connection's job). Guards the removed hook.
    import mssql_python.pooling  # noqa: F401  (ensure the submodule is imported)

    # `mssql_python.pooling` the attribute is the public pooling() function, so
    # reach the module object via sys.modules.
    pooling_mod = sys.modules["mssql_python.pooling"]
    PoolingManager = pooling_mod.PoolingManager

    monkeypatch.setattr(pooling_mod.ddbc_bindings, "enable_pooling", lambda *a, **k: None)
    PoolingManager._enabled = False
    PoolingManager._pools_closed = False
    try:
        PoolingManager.enable(max_size=5, idle_timeout=10)
        assert not ProviderManager.is_frozen()
    finally:
        PoolingManager._enabled = False
        PoolingManager._pools_closed = False


def test_rust_provider_load_error_names_rust_distribution(tmp_path):
    # Regression for the import-time load-order bug: selecting mssql-odbc must
    # surface the rust distribution in the native load error, proving the Python
    # push reaches the native loader before the driver loads (rather than the
    # classic default being frozen at import time).
    pkg_dir = tmp_path / "mssql_python_rust_odbc"
    pkg_dir.mkdir()
    (pkg_dir / "__init__.py").write_text("")  # importable, but ships no driver binaries

    script = textwrap.dedent("""
        import mssql_python
        try:
            # Server value is never contacted; the driver load fails first on the
            # incomplete stand-in package.
            mssql_python.connect("Server=test;Database=testdb;Trusted_Connection=yes")
        except Exception as exc:  # noqa: BLE001
            print(type(exc).__name__ + ": " + str(exc))
        else:
            print("NO_ERROR")
        """)

    env = dict(os.environ)
    env["MSSQL_PYTHON_NATIVE_PROVIDER"] = "mssql-odbc"
    env["PYTHONPATH"] = str(tmp_path) + os.pathsep + env.get("PYTHONPATH", "")

    result = subprocess.run(
        [sys.executable, "-c", script],
        env=env,
        capture_output=True,
        text=True,
    )
    output = result.stdout + result.stderr
    assert "mssql-python-rust-odbc" in output, output
