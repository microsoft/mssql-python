"""
Regression guard for the Windows package-local DLL load path.

The vendored ODBC driver (``msodbcsql18.dll``) and Entra auth DLL
(``mssql-auth.dll``) must be loaded with a constrained search path --
``LoadLibraryExW`` with ``LOAD_LIBRARY_SEARCH_APPLICATION_DIR |
LOAD_LIBRARY_SEARCH_SYSTEM32 | LOAD_LIBRARY_SEARCH_DLL_LOAD_DIR`` -- rather than
the legacy ``LoadLibraryW`` search order, which also consults the current
working directory and ``%PATH``. The broader ``LOAD_LIBRARY_SEARCH_DEFAULT_DIRS``
is intentionally avoided because it also searches user DLL directories
(registered process-wide via ``AddDllDirectory`` / ``SetDllDirectory``).

This is a source-contract test on purpose. The restriction only manifests at
DLL-resolution time on Windows, which cannot be observed without dropping a
file on disk; a success-path "does it still load" check passes on the
unhardened code too (any host with ``msvcp140.dll`` in System32), so it guards
nothing. Asserting the loader keeps using the constrained API is the
deterministic, platform-independent way to fail if the hardening is reverted.
"""

import re
from pathlib import Path

import pytest

_PYBIND_DIR = Path(__file__).resolve().parents[1] / "mssql_python" / "pybind"
_LOADER_SRC = _PYBIND_DIR / "ddbc_bindings.cpp"
pytestmark = pytest.mark.skipif(
    not _PYBIND_DIR.is_dir(),
    reason="requires a source checkout; isolated wheel tests omit the source tree",
)


def _code_without_comments(text):
    # Drop // line comments so prose that mentions LoadLibraryW is not matched.
    return "\n".join(re.sub(r"//.*", "", line) for line in text.splitlines())


def test_loader_source_present():
    assert _LOADER_SRC.is_file(), f"loader source not found at {_LOADER_SRC}"


def test_no_unhardened_loadlibrary_call():
    code = _code_without_comments(_LOADER_SRC.read_text(encoding="utf-8"))
    # A bare LoadLibraryW(...) call resolves dependencies via the legacy search
    # order, which includes the current directory and %PATH%.
    assert re.search(r"\bLoadLibraryW\s*\(", code) is None, (
        "ddbc_bindings.cpp contains a bare LoadLibraryW call; the vendored "
        "driver and auth DLLs must be loaded with LoadLibraryExW and the "
        "constrained search flags instead."
    )


def test_driver_and_auth_loads_use_constrained_search():
    code = _code_without_comments(_LOADER_SRC.read_text(encoding="utf-8"))
    # Match each LoadLibraryExW( ... ); call and require the constrained flag
    # set inside that call's own argument list -- not merely somewhere in the
    # file (the flag names also appear in the #define block, so a file-wide
    # substring check would still pass if a call's flags were replaced).
    calls = re.findall(r"LoadLibraryExW\s*\(.*?\)\s*;", code, re.DOTALL)
    assert len(calls) >= 2, "expected LoadLibraryExW for both the driver and the auth DLL loads"
    required = (
        "LOAD_LIBRARY_SEARCH_APPLICATION_DIR",
        "LOAD_LIBRARY_SEARCH_SYSTEM32",
        "LOAD_LIBRARY_SEARCH_DLL_LOAD_DIR",
    )
    for call in calls:
        for flag in required:
            assert flag in call, f"a LoadLibraryExW call is missing {flag}: {call}"
        # DEFAULT_DIRS re-introduces LOAD_LIBRARY_SEARCH_USER_DIRS (process-wide
        # AddDllDirectory/SetDllDirectory dirs), which is outside the trusted
        # set; the loads must name the individual directories instead.
        assert "LOAD_LIBRARY_SEARCH_DEFAULT_DIRS" not in call, (
            "a LoadLibraryExW call uses LOAD_LIBRARY_SEARCH_DEFAULT_DIRS, which "
            "includes user DLL directories: " + call
        )
