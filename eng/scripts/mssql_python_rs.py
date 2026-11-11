"""Resolve the native runtime once for installation and Python package metadata."""

from __future__ import annotations

import argparse
import importlib
import importlib.metadata
import json
import os
import re
from dataclasses import asdict, dataclass
from pathlib import Path

DEFAULT_INDEX = "https://pypi.org/simple/"
DEV_INDEX = (
    "https://pkgs.dev.azure.com/sqlclientdrivers/public/" "_packaging/mssql-rs_Public/pypi/simple/"
)
VERSION_ENV = "MSSQL_PYTHON_RS_VERSION"
VERSION_FILE = Path(__file__).resolve().parents[2] / "eng/versions/mssql-python-rs.version"
_RELEASE = re.compile(r"[0-9]+\.[0-9]+\.[0-9]+")
_VERSION = re.compile(r"[0-9]+\.[0-9]+\.[0-9]+(?:\.dev[0-9]+)?")


@dataclass(frozen=True)
class Runtime:
    version: str
    index_url: str
    version_override: bool


def resolve_runtime(version_file: Path | None = None, *, version: str | None = None) -> Runtime:
    selected = version if version is not None else os.environ.get(VERSION_ENV, "")
    overridden = bool(selected.strip())
    if overridden:
        selected = selected.strip()
        if not _VERSION.fullmatch(selected):
            raise ValueError(
                f"Invalid {VERSION_ENV}: {selected!r}; use an exact X.Y.Z or X.Y.Z.devN version"
            )
    else:
        source = version_file if version_file is not None else VERSION_FILE
        selected = source.read_text(encoding="ascii").strip()
        if not _RELEASE.fullmatch(selected):
            raise ValueError(f"{source} must contain an exact released X.Y.Z version")
    return Runtime(
        selected,
        DEV_INDEX if ".dev" in selected else DEFAULT_INDEX,
        overridden,
    )


def verify_installed(runtime: Runtime) -> None:
    installed = importlib.metadata.version("mssql-python-rs")
    if installed != runtime.version:
        raise ValueError(
            f"Installed mssql-python-rs=={installed}, expected {runtime.version}; "
            "a later dependency installation may have replaced the selected runtime"
        )
    core = importlib.import_module("mssql_py_core")
    print(f"Verified mssql-python-rs=={installed} loaded from {core.__file__}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version-file", type=Path)
    parser.add_argument("--version")
    parser.add_argument("--write-version-file", type=Path)
    parser.add_argument("--verify", action="store_true")
    args = parser.parse_args()
    runtime = resolve_runtime(args.version_file, version=args.version)
    if args.write_version_file:
        args.write_version_file.write_text(runtime.version + "\n", encoding="ascii")
    if args.verify:
        verify_installed(runtime)
    else:
        print(json.dumps(asdict(runtime)))


if __name__ == "__main__":
    main()
