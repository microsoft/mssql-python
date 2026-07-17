"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.

mssql_python_odbc — Microsoft ODBC Driver 18 for SQL Server binaries.

Internal implementation package for ``mssql-python``. It ships the
platform-specific ODBC driver binaries (``msodbcsql18``) and their supporting
libraries so that ``mssql-python`` does not have to bundle them in its own
wheel. It is not meant for direct consumption — install ``mssql-python``
instead, which depends on this package.

Driver-path resolution lives entirely in the native
``mssql_python.ddbc_bindings`` extension (``GetOdbcLibsBaseDir`` /
``GetDriverPathCpp``): it imports this package purely for its ``__file__`` and
appends ``libs/<platform>/<arch>/...`` itself. Keeping a single (C++) resolver
avoids a second copy of the arch/distro/filename logic that could silently
drift out of sync — see the drift guard in ``tests/test_000_dependencies.py``.
"""

import os

__all__ = ["get_libs_dir", "__version__"]

# Version tracks the bundled Microsoft ODBC Driver 18 for SQL Server release and
# is the single source of truth for the driver version. ``setup_odbc.py`` reads
# it for the wheel version, and the native build (``mssql_python/pybind/
# CMakeLists.txt``) derives the driver filename version from it and injects it
# into ``GetDriverPathCpp`` -- so the C++ resolver and this package can never
# drift. Bump this one value to move to a new driver release.
__version__ = "18.6.2"


def get_libs_dir() -> str:
    """Return the absolute path to this package's ``libs/`` directory.

    This is the root under which the platform-specific ODBC binaries live
    (``libs/<platform>/<arch>/...``). The parent of this path (the package
    directory) is the base the native loader appends ``libs`` to when resolving
    the driver.
    """
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "libs")
