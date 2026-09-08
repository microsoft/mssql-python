"""Negative-path tests for the standalone ``mssql-python-odbc`` requirement.

After the Phase 2 package split the native driver resolver
(``GetOdbcLibsBaseDir`` in ``mssql_python/pybind/ddbc_bindings.cpp``) no longer
falls back to bundled binaries -- it *raises* when the standalone
``mssql-python-odbc`` package is missing or incomplete. The happy path is
already covered end to end (the build/pr-validation stages install the odbc
wheel and connect), but nothing exercised these two error branches. These tests
close that gap, mirroring the shape of
``test_008_auth.py::test_import_error_raises_runtime_error`` (which asserts the
actionable message when azure-identity is absent).

The resolver is only reachable through driver loading, which happens once per
process via ``std::call_once`` (so it cannot be re-triggered after a driver has
already loaded). Each case therefore runs in a fresh subprocess that shadows the
``mssql_python_odbc`` import *before* the first connection attempt, then asserts
the actionable guidance surfaced by ``connect()``.
"""

import glob
import os
import subprocess
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_MSSQL_DIR = _REPO_ROOT / "mssql_python"

# These tests drive the *native* resolver, so they require the compiled
# extension. Skip cleanly in a source-only checkout where it was never built.
_EXT_BUILT = bool(
    glob.glob(str(_MSSQL_DIR / "ddbc_bindings.*.pyd"))
    or glob.glob(str(_MSSQL_DIR / "ddbc_bindings.*.so"))
)
pytestmark = pytest.mark.skipif(not _EXT_BUILT, reason="native ddbc_bindings extension not built")

# Child driver: shadow ``mssql_python_odbc`` according to ``mode`` and then let
# ``connect()`` trigger the native driver load. A localhost/no-credential
# connection string keeps this local -- the resolver raises during driver load,
# before any network I/O, so no live SQL Server is required.
_CHILD = r"""
import os
import sys
import tempfile

mode = sys.argv[1]

if mode == "missing":
    # Make the real package un-importable so the resolver hits the
    # ModuleNotFoundError branch (package-not-installed path).
    class _BlockOdbc:
        def find_spec(self, name, path=None, target=None):
            if name == "mssql_python_odbc" or name.startswith("mssql_python_odbc."):
                raise ModuleNotFoundError("No module named 'mssql_python_odbc'", name=name)
            return None

    sys.modules.pop("mssql_python_odbc", None)
    sys.meta_path.insert(0, _BlockOdbc())
elif mode == "incomplete":
    # Shadow the real package with a binary-less stand-in so the resolver's
    # completeness check (the platform driver file does not exist) fails.
    tmp = tempfile.mkdtemp()
    pkg = os.path.join(tmp, "mssql_python_odbc")
    os.makedirs(pkg)
    with open(os.path.join(pkg, "__init__.py"), "w", encoding="utf-8") as fh:
        fh.write("__version__ = '0.0.0-fake'\n")
    sys.path.insert(0, tmp)
    sys.modules.pop("mssql_python_odbc", None)
else:
    sys.stderr.write("bad mode: " + mode + "\n")
    sys.exit(3)

import mssql_python

try:
    mssql_python.connect("Server=localhost;Database=master;Encrypt=no;")
except Exception as exc:
    sys.stdout.write("DRIVER_ERROR>>>" + str(exc))
    sys.exit(0)

sys.stdout.write("NO_ERROR")
sys.exit(4)
"""


def _run_child(mode: str) -> subprocess.CompletedProcess:
    """Run the child driver in a fresh interpreter and return the result."""
    env = dict(os.environ)
    # Propagate the parent's import paths so the child resolves the same
    # ``mssql_python`` package (and its compiled extension) that we are testing.
    env["PYTHONPATH"] = os.pathsep.join(p for p in sys.path if p)
    return subprocess.run(
        [sys.executable, "-c", _CHILD, mode],
        cwd=str(_REPO_ROOT),
        env=env,
        capture_output=True,
        text=True,
        timeout=180,
    )


def test_missing_package_error_points_to_pip_install():
    """A missing mssql-python-odbc surfaces an actionable 'pip install' hint."""
    proc = _run_child("missing")
    assert (
        "DRIVER_ERROR>>>" in proc.stdout
    ), f"expected a driver error; stdout={proc.stdout!r} stderr={proc.stderr!r}"
    message = proc.stdout.split("DRIVER_ERROR>>>", 1)[1]
    assert "mssql-python-odbc" in message
    assert "pip install mssql-python-odbc" in message


def test_incomplete_package_error_points_to_force_reinstall():
    """An incomplete mssql-python-odbc surfaces an actionable '--force-reinstall' hint."""
    proc = _run_child("incomplete")
    assert (
        "DRIVER_ERROR>>>" in proc.stdout
    ), f"expected a driver error; stdout={proc.stdout!r} stderr={proc.stderr!r}"
    message = proc.stdout.split("DRIVER_ERROR>>>", 1)[1]
    assert "mssql-python-odbc" in message
    assert "--force-reinstall" in message
