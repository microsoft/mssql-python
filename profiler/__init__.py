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
    results = p.run("fetchall", "insertmanyvalues")  # prints tables, returns results
    p.close()
"""

from profiler.core import Profiler

__all__ = ["Profiler"]
