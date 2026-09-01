"""
Windows-only regression coverage for package-local DLL loading.

The vendored ODBC driver and Entra auth DLLs are loaded with a constrained,
package-local search path so their dependencies (notably the bundled VC++
runtime under ``vcredist``) resolve from trusted directories rather than the
current working directory or ``%PATH%``. This test plants a bogus
``msvcp140.dll`` in both a ``%PATH%`` entry and the process working directory
and confirms the driver still loads from the package -- i.e. the bundled
runtime resolution did not regress once those legacy directories are no longer
consulted.
"""

import os
import subprocess
import sys
import textwrap

import pytest

pytestmark = pytest.mark.skipif(
    sys.platform != "win32",
    reason="Package-local DLL search path applies to Windows only.",
)

# A minimal, invalid PE image. A loader that resolved msvcp140.dll from CWD or
# %PATH% would try to load this and fail; the package-local search path must
# ignore it and use the bundled runtime instead.
_JUNK_DLL = b"MZ" + b"\x00" * 256


def _plant_junk(directory):
    with open(os.path.join(directory, "msvcp140.dll"), "wb") as handle:
        handle.write(_JUNK_DLL)


def test_driver_loads_despite_planted_dll_on_cwd_and_path(tmp_path):
    cwd_dir = tmp_path / "cwd"
    path_dir = tmp_path / "onpath"
    cwd_dir.mkdir()
    path_dir.mkdir()
    _plant_junk(str(cwd_dir))
    _plant_junk(str(path_dir))

    env = dict(os.environ)
    env["PATH"] = str(path_dir) + os.pathsep + env.get("PATH", "")

    # Force the ODBC driver to load in a fresh interpreter by attempting a
    # connection to an unreachable endpoint. Handle allocation (which loads the
    # driver and its dependencies) happens before the network handshake, so a
    # short login timeout is enough. We assert only that the failure is NOT a
    # driver/runtime load failure -- the connection itself is expected to fail.
    child = textwrap.dedent("""
        import mssql_python
        _LOAD_FAILURES = (
            "Failed to load the driver",
            "Failed to load mssql-auth.dll",
            "Failed to load library",
        )
        try:
            mssql_python.connect(
                "Server=127.0.0.1,1;Database=x;Uid=x;Pwd=x;"
                "Encrypt=no;TrustServerCertificate=yes",
                timeout=1,
            )
        except Exception as exc:  # noqa: BLE001 - any non-load error is acceptable
            message = str(exc)
            for needle in _LOAD_FAILURES:
                assert needle not in message, message
        print("DRIVER_LOADED_OK")
        """)

    result = subprocess.run(
        [sys.executable, "-c", child],
        cwd=str(cwd_dir),
        env=env,
        capture_output=True,
        text=True,
    )

    assert "DRIVER_LOADED_OK" in result.stdout, (result.stdout, result.stderr)
