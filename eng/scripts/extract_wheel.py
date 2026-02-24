#!/usr/bin/env python
"""Extract mssql_py_core package files from a wheel into a target directory.

Wheels are ZIP files. This script extracts only mssql_py_core/ entries,
skipping .dist-info metadata and vendored .libs directories.

Usage:
    python extract_wheel.py <wheel_path> <target_dir>
"""
import os
import sys
import zipfile


def extract(wheel_path: str, target_dir: str) -> int:
    """Extract mssql_py_core/ entries from a wheel, return count of files extracted."""
    extracted = 0
    with zipfile.ZipFile(wheel_path, "r") as zf:
        for entry in zf.namelist():
            if ".dist-info/" in entry:
                continue
            if entry.startswith("mssql_py_core.libs/"):
                continue
            if not entry.startswith("mssql_py_core/"):
                continue

            out_path = os.path.join(target_dir, entry)
            real_out = os.path.realpath(out_path)
            if not real_out.startswith(os.path.realpath(target_dir) + os.sep):
                raise ValueError(f"Path traversal blocked: {entry}")
            if entry.endswith("/"):
                os.makedirs(real_out, exist_ok=True)
                continue

            os.makedirs(os.path.dirname(real_out), exist_ok=True)
            with open(real_out, "wb") as f:
                f.write(zf.read(entry))
            extracted += 1
            print(f"  Extracted: {entry}")

    return extracted


def main() -> None:
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <wheel_path> <target_dir>", file=sys.stderr)
        sys.exit(2)

    wheel_path, target_dir = sys.argv[1], sys.argv[2]
    count = extract(wheel_path, target_dir)

    if count == 0:
        print("ERROR: No mssql_py_core files found in wheel", file=sys.stderr)
        sys.exit(1)

    print(f"Extracted {count} file(s) into {target_dir}")


if __name__ == "__main__":
    main()
