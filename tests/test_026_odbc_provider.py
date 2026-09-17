"""
Tests for ODBC provider selection (opt-in/opt-out).

Covers the ``ProviderManager`` engine (precedence, normalization, fail-closed
validation, resolve-once freezing, post-freeze warning) and the public surface
(``mssql_python.native_provider`` property and ``get_native_provider_info()``).
"""

import importlib
import os
import platform
import subprocess
import sys
from types import SimpleNamespace
from pathlib import Path

import pytest

import mssql_python
from mssql_python.odbc_provider import (
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
    with pytest.warns(RuntimeWarning, match="takes precedence"):
        ProviderManager.set_property(PROVIDER_MSSQL_ODBC)
    assert ProviderManager.resolve() == PROVIDER_MSODBCSQL18


def test_matching_env_var_and_property_do_not_warn(monkeypatch, recwarn):
    monkeypatch.setenv(NATIVE_PROVIDER_ENV_VAR, PROVIDER_MSSQL_ODBC)
    ProviderManager.set_property(PROVIDER_MSSQL_ODBC)
    assert not [w for w in recwarn if issubclass(w.category, RuntimeWarning)]


def test_overridden_property_becomes_effective_if_env_is_removed(monkeypatch):
    monkeypatch.setenv(NATIVE_PROVIDER_ENV_VAR, PROVIDER_MSODBCSQL18)
    with pytest.warns(RuntimeWarning, match="takes precedence"):
        ProviderManager.set_property(PROVIDER_MSSQL_ODBC)
    monkeypatch.delenv(NATIVE_PROVIDER_ENV_VAR)
    assert ProviderManager.resolve() == PROVIDER_MSSQL_ODBC


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
    assert ProviderManager.package_name(PROVIDER_MSSQL_ODBC) == "mssql_py_core"


def _assert_classic_driver_filename(path):
    filename = Path(path).name
    if sys.platform == "linux":
        assert filename.startswith("libmsodbcsql-")
    elif sys.platform == "darwin":
        assert filename.startswith("libmsodbcsql.")
    elif sys.platform == "win32":
        assert filename.startswith("msodbcsql")
    else:
        pytest.skip(f"unsupported test platform: {sys.platform}")


def test_rust_provider_driver_path_matches_packaging_layout():
    # Run in a child process because the native provider selection is
    # intentionally process-wide and cannot be reset after it is pushed.
    script = """
from mssql_python import ddbc_bindings

ddbc_bindings._set_odbc_provider("mssql-odbc")
print(ddbc_bindings.GetDriverPathCpp("provider-root"))
"""
    proc = subprocess.run(
        [sys.executable, "-c", script],
        check=True,
        capture_output=True,
        text=True,
    )

    machine = platform.machine().lower()
    arch = "arm64" if machine in {"aarch64", "arm64"} else "x86_64"
    if sys.platform == "linux":
        libc = "glibc" if platform.libc_ver()[0] == "glibc" else "musl"
        expected = Path("provider-root", "libs", "linux", libc, arch, "lib", "mssqlodbc.so")
    elif sys.platform == "darwin":
        expected = Path("provider-root", "libs", "macos", arch, "lib", "mssqlodbc.dylib")
    elif sys.platform == "win32":
        win_arch = "x64" if arch == "x86_64" else arch
        expected = Path("provider-root", "libs", "windows", win_arch, "mssqlodbc.dll")
    else:
        pytest.skip(f"unsupported test platform: {sys.platform}")
        return

    assert Path(proc.stdout.strip()) == expected


def test_direct_native_default_load_accepts_later_explicit_default():
    # A direct native connection bypasses ProviderManager and therefore loads
    # the classic default without first pushing it into native selection state.
    # The later explicit push made by the public connect() path must recognize
    # that the already-loaded driver is the same provider. Use a child process
    # because native driver state cannot be reset after loading.
    script = """
from mssql_python import ddbc_bindings

try:
    ddbc_bindings.Connection(
        "",
        False,
        {},
        "",
        None,
    )
except RuntimeError:
    pass

ddbc_bindings._set_odbc_provider("msodbcsql18")
print("provider accepted")
"""
    proc = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
    )

    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.strip() == "provider accepted"


def test_direct_native_load_attempt_rejects_later_provider_change():
    # Loading success and failure are both cached process-wide, so selection
    # must become immutable as soon as loading starts rather than only after it
    # succeeds. Otherwise a concurrent/late setter can make native selection
    # report a provider different from the one whose load result was cached.
    script = """
from mssql_python import ddbc_bindings

try:
    ddbc_bindings.Connection("", False, {}, "", None)
except RuntimeError:
    pass

try:
    ddbc_bindings._set_odbc_provider("mssql-odbc")
except RuntimeError as exc:
    print(exc)
"""
    proc = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
    )

    assert proc.returncode == 0, proc.stderr
    assert "Cannot change the ODBC provider" in proc.stdout


def test_get_info_before_and_after_resolve(monkeypatch):
    info = ProviderManager.get_info()
    assert info["id"] == PROVIDER_MSODBCSQL18
    assert info["package"] == "mssql_python_odbc"
    assert info["version"] == "18.6.2.1"
    _assert_classic_driver_filename(info["driver_path"])
    assert info["source"] == "default"
    assert info["frozen"] is False

    monkeypatch.setenv(NATIVE_PROVIDER_ENV_VAR, PROVIDER_MSSQL_ODBC)
    ProviderManager.resolve()
    real_import = importlib.import_module

    def fake_import(name, *args, **kwargs):
        if name == "mssql_py_core":
            return SimpleNamespace(__version__="test-version", __file__=__file__)
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(
        sys.modules[ProviderManager.__module__].importlib, "import_module", fake_import
    )
    info = ProviderManager.get_info()
    assert info["id"] == PROVIDER_MSSQL_ODBC
    assert info["package"] == "mssql_py_core"
    assert info["version"] == "test-version"
    assert Path(info["driver_path"]).name.startswith("mssqlodbc")
    assert info["source"] == "environment"
    assert info["frozen"] is True


def test_get_info_rust_path_does_not_select_or_freeze_native_provider(monkeypatch):
    from mssql_python import ddbc_bindings

    monkeypatch.setenv(NATIVE_PROVIDER_ENV_VAR, PROVIDER_MSSQL_ODBC)
    real_import = importlib.import_module

    def fake_import(name, *args, **kwargs):
        if name == "mssql_py_core":
            return SimpleNamespace(__version__="test-version", __file__=__file__)
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(
        sys.modules[ProviderManager.__module__].importlib, "import_module", fake_import
    )
    info = ProviderManager.get_info()
    assert info["id"] == PROVIDER_MSSQL_ODBC
    assert Path(info["driver_path"]).name.startswith("mssqlodbc")
    assert not ProviderManager.is_frozen()
    _assert_classic_driver_filename(ddbc_bindings.GetDriverPathCpp("provider-root"))


def test_effective_and_get_info_do_not_raise_for_invalid_env_var(monkeypatch):
    # A bad selection must not break read-only diagnostics (or the public
    # getter, which shares effective()) - only resolve()/ensure_available()
    # fail closed, at connection time, where the error is actionable.
    monkeypatch.setenv(NATIVE_PROVIDER_ENV_VAR, "bogus-value")
    assert ProviderManager.effective() == PROVIDER_MSODBCSQL18
    info = ProviderManager.get_info()
    assert info["id"] == PROVIDER_MSODBCSQL18
    assert info["frozen"] is False
    assert "bogus-value" in info["error"]
    with pytest.raises(ValueError):
        ProviderManager.resolve()


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
        if name == "mssql_py_core":
            raise ModuleNotFoundError(f"No module named '{name}'", name=name)
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(
        sys.modules[ProviderManager.__module__].importlib, "import_module", fake_import
    )
    with pytest.raises(ImportError) as excinfo:
        ProviderManager.ensure_available()
    message = str(excinfo.value)
    assert PROVIDER_MSSQL_ODBC in message
    assert "mssql-python-rs" in message
    # The failed check must not freeze the selection - a later, installed
    # provider can still be chosen instead of requiring a process restart.
    assert not ProviderManager.is_frozen()


def test_ensure_available_reraises_nested_import_error(monkeypatch):
    # A transitive dependency missing (or any other broken-package import
    # error) inside an *installed* provider package must not be masked as
    # "package is not installed" - it should propagate as-is.
    monkeypatch.setenv(NATIVE_PROVIDER_ENV_VAR, PROVIDER_MSSQL_ODBC)
    real_import = importlib.import_module

    def fake_import(name, *args, **kwargs):
        if name == "mssql_py_core":
            raise ModuleNotFoundError(
                "No module named 'some_transitive_dependency'", name="some_transitive_dependency"
            )
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(
        sys.modules[ProviderManager.__module__].importlib, "import_module", fake_import
    )
    with pytest.raises(ModuleNotFoundError) as excinfo:
        ProviderManager.ensure_available()
    assert excinfo.value.name == "some_transitive_dependency"


def test_pooling_enable_does_not_freeze_provider():
    """Regression: enabling pooling before selecting a provider must not lock
    in the default - ``enable_pooling()`` configures the pool manager only and
    never loads the native driver, so it has no reason to resolve the provider.
    """
    from mssql_python.pooling import PoolingManager

    try:
        PoolingManager.enable()
        assert not ProviderManager.is_frozen()
        ProviderManager.set_property(PROVIDER_MSSQL_ODBC)
        assert ProviderManager.resolve() == PROVIDER_MSSQL_ODBC
    finally:
        PoolingManager.disable()
        PoolingManager._reset_for_testing()


def test_rejected_connection_does_not_freeze_provider():
    """Regression: a connect() that fails Python-side validation (before the
    native driver loads) must not freeze provider selection, so a later,
    corrected connection can still pick a different provider without restarting
    the process. The provider is resolved only immediately before the native
    Connection is constructed, after all argument/connection-string validation.
    """
    from mssql_python.connection import Connection
    from mssql_python.exceptions import InterfaceError

    assert not ProviderManager.is_frozen()
    # An embedded NUL is rejected in _construct_connection_string, before the
    # native driver ever loads.
    with pytest.raises(InterfaceError):
        Connection("Server=test\x00;Database=db")
    assert not ProviderManager.is_frozen()

    # The selection is still changeable after the rejected attempt.
    ProviderManager.set_property(PROVIDER_MSSQL_ODBC)
    assert ProviderManager.effective() == PROVIDER_MSSQL_ODBC


# ---------------------------------------------------------------------------
# Integration smoke tests: opt into the Rust mssql-odbc provider and connect.
#
# Provider selection is process-wide and frozen once, so each selection method
# runs in its own subprocess. They require a live SQL Server
# (DB_CONNECTION_STRING) and the packaged mssql-odbc driver, and skip otherwise.
# Success is asserted on the SMOKE_OK stdout marker, not the exit code: the Rust
# driver is still maturing and can panic during process teardown after the query
# has already succeeded.
# ---------------------------------------------------------------------------

_DB_CONNECTION_STRING = os.getenv("DB_CONNECTION_STRING")


def _mssql_odbc_driver_packaged():
    """True when the mssql-odbc (Rust) driver ships in the importable core."""
    try:
        import mssql_py_core
    except ImportError:
        return False
    libs = Path(mssql_py_core.__file__).resolve().parent / "libs"
    return libs.is_dir() and any(libs.rglob("mssqlodbc.*"))


requires_rust_provider_and_db = pytest.mark.skipif(
    not (_DB_CONNECTION_STRING and _mssql_odbc_driver_packaged()),
    reason="needs DB_CONNECTION_STRING and the packaged mssql-odbc driver",
)


_SMOKE_SCRIPT = """
import os
import mssql_python

{select}

info = mssql_python.get_native_provider_info()
assert info["id"] == "mssql-odbc", info
assert info["package"] == "mssql_py_core", info
assert "mssqlodbc" in os.path.basename(info["driver_path"]).lower(), info

conn = mssql_python.connect(os.environ["DB_CONNECTION_STRING"])
cursor = conn.cursor()
cursor.execute("SELECT 1")
assert cursor.fetchone()[0] == 1
cursor.close()
conn.close()
print("SMOKE_OK")
"""


def _run_provider_smoke(select_stmt, extra_env):
    env = dict(os.environ)
    env["DB_CONNECTION_STRING"] = _DB_CONNECTION_STRING
    env.pop(NATIVE_PROVIDER_ENV_VAR, None)  # start from a clean selection
    env.update(extra_env)
    proc = subprocess.run(
        [sys.executable, "-c", _SMOKE_SCRIPT.format(select=select_stmt)],
        env=env,
        capture_output=True,
        text=True,
    )
    # Assert on the marker, not the exit code: the WIP Rust driver may panic at
    # teardown after the query succeeds, which can dirty the process exit code.
    assert (
        "SMOKE_OK" in proc.stdout
    ), f"provider smoke failed\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"


@requires_rust_provider_and_db
def test_smoke_mssql_odbc_via_env_var():
    """MSSQL_PYTHON_NATIVE_PROVIDER selects the Rust driver and connects."""
    _run_provider_smoke("pass", {NATIVE_PROVIDER_ENV_VAR: PROVIDER_MSSQL_ODBC})


@requires_rust_provider_and_db
def test_smoke_mssql_odbc_via_module_property():
    """mssql_python.native_provider selects the Rust driver and connects."""
    _run_provider_smoke(f"mssql_python.native_provider = {PROVIDER_MSSQL_ODBC!r}", {})
