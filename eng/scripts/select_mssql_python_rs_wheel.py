#!/usr/bin/env python3
"""Select the compatible mssql-python-rs wheel from an extracted transport."""

from __future__ import annotations

import argparse
import re
from pathlib import Path


def select_wheel(
    wheel_dir: Path, distribution_version: str, python_tag: str, platform_tag: str
) -> Path:
    target = re.fullmatch(r"cp3(\d+)", python_tag)
    if not target:
        raise ValueError(f"unsupported Python tag: {python_tag}")

    candidates: list[tuple[int, int, Path]] = []
    prefix = f"mssql_python_rs-{distribution_version}-"
    for wheel in wheel_dir.glob(f"{prefix}*.whl"):
        parts = wheel.name.removesuffix(".whl").rsplit("-", 3)
        if len(parts) != 4 or parts[3] != platform_tag:
            continue
        wheel_python, wheel_abi = parts[1:3]
        if wheel_python == python_tag and wheel_abi == python_tag:
            candidates.append((2, int(target[1]), wheel))
            continue
        abi3_floor = re.fullmatch(r"cp3(\d+)", wheel_python)
        if wheel_abi == "abi3" and abi3_floor and int(abi3_floor[1]) <= int(target[1]):
            candidates.append((1, int(abi3_floor[1]), wheel))

    if not candidates:
        raise ValueError(
            f"no compatible mssql-python-rs {distribution_version} wheel "
            f"for {python_tag} {platform_tag}"
        )
    best_rank = max((priority, floor) for priority, floor, _ in candidates)
    matches = [wheel for priority, floor, wheel in candidates if (priority, floor) == best_rank]
    if len(matches) != 1:
        raise ValueError(f"multiple equally compatible wheels found: {matches}")
    return matches[0]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("wheel_dir", type=Path)
    parser.add_argument("distribution_version")
    parser.add_argument("python_tag")
    parser.add_argument("platform_tag")
    args = parser.parse_args()
    print(
        select_wheel(
            args.wheel_dir,
            args.distribution_version,
            args.python_tag,
            args.platform_tag,
        )
    )


if __name__ == "__main__":
    main()
