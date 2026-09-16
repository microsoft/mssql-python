"""Copy one expected coverage report from a ZIP without extracting archive paths."""

import argparse
from pathlib import Path, PurePosixPath
import stat
import zipfile

MAX_ARCHIVE_FILES = 10_000
MAX_ARCHIVE_BYTES = 256 * 1024 * 1024
MAX_REPORT_BYTES = 64 * 1024 * 1024


def select(archive, kind):
    members = archive.infolist()
    if (
        len(members) > MAX_ARCHIVE_FILES
        or sum(member.file_size for member in members) > MAX_ARCHIVE_BYTES
    ):
        raise ValueError("Coverage artifact exceeds size limits")

    candidates = []
    for member in members:
        path = PurePosixPath(member.filename)
        if (
            path.is_absolute()
            or ".." in path.parts
            or "\\" in member.filename
            or member.flag_bits & 1
            or stat.S_ISLNK(member.external_attr >> 16)
            or member.file_size > MAX_REPORT_BYTES
        ):
            continue
        if kind == "html" and path.name == "index.html" and "Code Coverage Report" in str(path):
            candidates.append((0, member))
        elif kind == "xml" and path.suffix.lower() == ".xml":
            name = path.name.lower()
            if str(path).endswith("unified-coverage/coverage.xml"):
                priority = 0
            elif name == "coverage.xml":
                priority = 1
            elif "coverage" in name:
                priority = 2
            else:
                continue
            candidates.append((priority, member))

    if not candidates:
        raise ValueError(f"No coverage {kind} report found")
    priority = min(item[0] for item in candidates)
    selected = [member for rank, member in candidates if rank == priority]
    return selected


def copy_report(archive_path, output, kind):
    if Path(archive_path).stat().st_size > MAX_ARCHIVE_BYTES:
        raise ValueError("Coverage archive exceeds size limit")
    with zipfile.ZipFile(archive_path) as archive:
        selected = select(archive, kind)
        data = archive.read(selected[0])
        if any(archive.read(member) != data for member in selected[1:]):
            raise ValueError(f"Conflicting coverage {kind} reports")
    Path(output).write_bytes(data)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("kind", choices=("html", "xml"))
    parser.add_argument("archive", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    copy_report(args.archive, args.output, args.kind)


if __name__ == "__main__":
    main()
