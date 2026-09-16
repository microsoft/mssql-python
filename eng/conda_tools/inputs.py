"""Maintained source versions shared by public audits and recorded release provenance."""

from __future__ import annotations

import re
from typing import Callable


def read_release_versions(read_source: Callable[[str], str]) -> dict[str, str]:
    """Read the literals maintained by the existing wheel release process, without imports."""
    versions = {}
    for path, field in (
        ("setup.py", "version"),
        ("mssql_python/__init__.py", "__version__"),
        ("mssql_python_odbc/__init__.py", "__version__"),
    ):
        matches = re.findall(
            rf"""(?m)^\s*{field}\s*=\s*['"]([A-Za-z0-9][A-Za-z0-9._-]*)['"]\s*,?\s*(?:#.*)?$""",
            read_source(path),
        )
        if len(matches) != 1:
            raise ValueError(f"{path} must contain exactly one literal {field} release version.")
        versions[path] = matches[0]
    if versions["setup.py"] != versions["mssql_python/__init__.py"]:
        raise ValueError(
            "Binding release versions in setup.py and mssql_python/__init__.py differ."
        )
    return {
        "mssql-python": versions["setup.py"],
        "mssql-python-odbc": versions["mssql_python_odbc/__init__.py"],
    }
