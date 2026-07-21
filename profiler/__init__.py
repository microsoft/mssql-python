# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.
"""
mssql-python profiler — unified Python + C++ performance instrumentation.

Usage:
    python -m profiler                          # run all scenarios
    python -m profiler --scenarios fetch insert  # run specific scenarios
    python -m profiler --conn-str "Server=..."   # pass connection string

Programmatic:
    from profiler import Profiler

    p = Profiler(conn_str)
    p.run("fetchall", "insertmanyvalues")
    p.report()
"""

from profiler.core import Profiler

__all__ = ["Profiler"]
