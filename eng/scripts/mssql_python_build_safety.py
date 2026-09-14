#!/usr/bin/env python3
"""Validate a caller-controlled directory before destructive cleanup."""

from __future__ import annotations

import os
import stat
import sys
from pathlib import Path

_TRUSTED_DARWIN_ALIASES = {
    Path("/tmp"): Path("/private/tmp"),
    Path("/var"): Path("/private/var"),
}


def _is_link_or_junction(path: Path) -> bool:
    if path.is_symlink():
        return True
    try:
        attributes = path.lstat().st_file_attributes
    except (AttributeError, FileNotFoundError, OSError):
        return False
    return bool(attributes & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0))


def _is_trusted_system_alias(path: Path) -> bool:
    if sys.platform != "darwin" or path not in _TRUSTED_DARWIN_ALIASES:
        return False
    try:
        return path.resolve(strict=True) == _TRUSTED_DARWIN_ALIASES[path]
    except OSError:
        return False


def resolve_safe_output_directory(value: str | Path, cwd: Path | None = None) -> Path:
    candidate = Path(os.path.abspath(os.path.expanduser(os.fspath(value))))
    for component in (candidate, *candidate.parents):
        if _is_link_or_junction(component) and (
            component == candidate or not _is_trusted_system_alias(component)
        ):
            raise ValueError(
                f"Refusing output directory through a symbolic link or junction: {component}"
            )

    resolved = candidate.resolve(strict=False)
    if resolved == Path(resolved.anchor):
        raise ValueError(f"Refusing to use a filesystem root as output directory: {resolved}")
    if resolved.exists() and not resolved.is_dir():
        raise ValueError(f"Output directory is a file: {resolved}")

    working_directory = (cwd or Path.cwd()).resolve()
    try:
        working_directory.relative_to(resolved)
    except ValueError:
        pass
    else:
        raise ValueError(
            "Refusing to use the current working directory or one of its ancestors "
            f"as output directory: {resolved}"
        )
    return resolved


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit(f"Usage: {Path(sys.argv[0]).name} OUTPUT_DIR")
    try:
        print(resolve_safe_output_directory(sys.argv[1]))
    except ValueError as exc:
        raise SystemExit(f"ERROR: {exc}") from exc


if __name__ == "__main__":
    main()
