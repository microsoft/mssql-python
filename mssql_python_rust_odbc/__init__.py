"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.

mssql_python_rust_odbc — mssql-odbc (Rust) native driver binaries.

Internal implementation package for ``mssql-python``. It ships the
platform-specific ``mssql-odbc`` driver binaries built by ``mssql-rs`` so that
``mssql-python`` does not have to bundle them in its own wheel. It is not
meant for direct consumption — select the ``mssql-odbc`` provider via
``mssql_python.native_provider`` or the ``MSSQL_PYTHON_NATIVE_PROVIDER``
environment variable instead, which pulls this package in automatically.

Driver-path resolution lives entirely in the native
``mssql_python.ddbc_bindings`` extension (``GetOdbcLibsBaseDir`` /
``GetDriverPathCpp``): it imports this package purely for its ``__file__`` and
appends ``libs/<platform>/<libc>/<arch>/...`` itself. Keeping a single (C++)
resolver avoids a second copy of the platform/arch/filename logic that could
silently drift out of sync.
"""

import os

__all__ = ["get_libs_dir", "__version__"]

# Version tracks the published mssql-odbc-native NuGet package (built from
# mssql-rs's mssql-odbc/Cargo.toml) and is the single source of truth for the
# driver version. ``setup_rust_odbc.py`` reads it for the wheel version. Bump
# this value when syncing a newer mssql-odbc-native build (see
# eng/scripts/sync-mssql-odbc-native-libs.py and eng/versions/mssql-odbc-native.version).
__version__ = "0.1.0"


def get_libs_dir() -> str:
    """Return the absolute path to this package's ``libs/`` directory.

    This is the root under which the platform-specific mssql-odbc binaries
    live (``libs/<platform>/<libc-or-arch>/...``). The parent of this path
    (the package directory) is the base the native loader appends ``libs`` to
    when resolving the driver.
    """
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "libs")
