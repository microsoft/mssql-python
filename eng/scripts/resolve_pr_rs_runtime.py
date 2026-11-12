#!/usr/bin/env python3
"""Select an exact override or the newest feed-validated nightly runtime for PR tests."""

from __future__ import annotations

import argparse
import json
import os
import re
import urllib.request
from dataclasses import asdict, dataclass
from datetime import datetime
from itertools import chain
from pathlib import Path
from urllib.parse import parse_qs, unquote, urldefrag, urljoin, urlsplit

if __package__:
    from .download_mssql_python_rs_wheels import PACKAGE_ID, _WheelLinks
    from .mssql_python_build_safety import resolve_safe_output_directory
    from .mssql_python_rs import DEV_INDEX, VERSION_ENV, resolve_runtime
else:
    from download_mssql_python_rs_wheels import PACKAGE_ID, _WheelLinks
    from mssql_python_build_safety import resolve_safe_output_directory
    from mssql_python_rs import DEV_INDEX, VERSION_ENV, resolve_runtime

EXPECTED_PLATFORMS = frozenset(
    {
        "win_amd64",
        "win_arm64",
        "manylinux_2_28_x86_64",
        "manylinux_2_28_aarch64",
        "manylinux_2_34_x86_64",
        "manylinux_2_34_aarch64",
        "musllinux_1_2_x86_64",
        "musllinux_1_2_aarch64",
        "macosx_15_0_universal2",
    }
)
_NIGHTLY = re.compile(r"([0-9]+)\.([0-9]+)\.([0-9]+)\.dev([0-9]{8})")
_WHEEL = re.compile(r"mssql_python_rs-([^-]+?)(?:-(.*))?\.whl")
_NO_NIGHTLY = (
    "No scheduled nightly (X.Y.Z.devYYYYMMDD) wheels found; "
    f"set {VERSION_ENV} to an exact published version to override selection"
)


@dataclass(frozen=True)
class RuntimeSelection:
    """Overrides are unprobed; nightly evidence consists only of public feed links."""

    version: str
    index_url: str
    channel: str
    version_override: bool
    wheel_sha256: dict[str, str]
    wheel_urls: dict[str, str]


class _NightlyLinks(_WheelLinks):
    def __init__(self) -> None:
        super().__init__()
        self.yanked: list[str] = []
        self.anchor_open = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "a":
            attributes = dict(attrs)
            if self.anchor_open or len(attributes) != len(attrs) or not attributes.get("href"):
                raise ValueError("Malformed wheel index anchor")
            self.anchor_open = True
            if "data-yanked" in attributes:
                self.yanked.append(attributes["href"])
        super().handle_starttag(tag, attrs)

    def handle_endtag(self, tag: str) -> None:
        if tag == "a":
            if not self.anchor_open:
                raise ValueError("Malformed wheel index anchor")
            self.anchor_open = False

    def handle_data(self, data: str) -> None:
        if data.lstrip().startswith("<"):
            raise ValueError("Malformed wheel index HTML")

    def close(self) -> None:
        if self.anchor_open or self.rawdata.strip():
            raise ValueError("Malformed wheel index HTML")
        super().close()


def select_nightly(page: str, index_url: str = DEV_INDEX) -> RuntimeSelection:
    """Validate link evidence, not publisher build status or commit provenance."""
    package_url = index_url.rstrip("/") + f"/{PACKAGE_ID}/"
    if urlsplit(package_url).scheme != "https" or not urlsplit(package_url).hostname:
        raise ValueError("The runtime wheel index must use HTTPS")
    parser = _NightlyLinks()
    parser.feed(page)
    parser.close()
    candidates = []
    for link, yanked in chain(
        ((link, False) for link in parser.links),
        ((link, True) for link in parser.yanked),
    ):
        url, fragment = urldefrag(urljoin(package_url, link))
        parts = urlsplit(url)
        if not parts.path or parts.path == "/":
            raise ValueError("Invalid wheel URL; HTTPS wheel file required")
        filename = unquote(parts.path.rsplit("/", 1)[-1])
        wheel = _WHEEL.fullmatch(filename)
        if not wheel:
            continue
        version, tags = wheel.groups()
        nightly = _NIGHTLY.fullmatch(version)
        if not nightly:
            continue
        major, minor, patch, stamp = nightly.groups()
        try:
            date = datetime.strptime(stamp, "%Y%m%d").date()
        except ValueError as exc:
            raise ValueError(f"Invalid nightly calendar date in {filename}") from exc
        if date.strftime("%Y%m%d") != stamp:
            raise ValueError(f"Invalid nightly calendar date in {filename}")
        key = (date, int(major), int(minor), int(patch))
        candidates.append((key, version, tags or "", filename, url, fragment, yanked))
    if not candidates:
        raise ValueError(_NO_NIGHTLY)
    newest = max(candidate[0] for candidate in candidates)
    selected = [candidate for candidate in candidates if candidate[0] == newest]
    versions = {candidate[1] for candidate in selected}
    if len(versions) != 1:
        raise ValueError(f"Ambiguous nightly version spellings: {sorted(versions)}")
    version = versions.pop()
    hashes = {}
    urls = {}
    platforms = set()
    for _, _, tags, filename, url, fragment, yanked in selected:
        if filename in urls:
            raise ValueError(f"Duplicate wheel filename in index: {filename}")
        if yanked:
            raise ValueError(f"Newest nightly {version} has a yanked wheel: {filename}")
        parts = urlsplit(url)
        if parts.scheme != "https" or not parts.hostname or parts.username or parts.password:
            raise ValueError(f"Wheel download must use HTTPS without credentials: {filename}")
        platform = tags.removeprefix("cp310-abi3-")
        if not tags.startswith("cp310-abi3-") or platform not in EXPECTED_PLATFORMS:
            raise ValueError(f"Unexpected newest nightly wheel tags: {filename}")
        digest = parse_qs(fragment, keep_blank_values=True).get("sha256", [])
        if len(digest) != 1 or not re.fullmatch(r"[a-fA-F0-9]{64}", digest[0]):
            raise ValueError(f"Missing or invalid SHA-256 for {filename}")
        hashes[filename] = digest[0].lower()
        urls[filename] = url
        platforms.add(platform)
    missing = EXPECTED_PLATFORMS - platforms
    if missing:
        raise ValueError(
            f"Newest nightly {version} is incomplete; missing platforms: {', '.join(sorted(missing))}"
        )
    return RuntimeSelection(version, index_url, "nightly", False, hashes, urls)


def resolve_pr_runtime(*, version: str | None = None) -> RuntimeSelection:
    selected = version if version is not None else os.environ.get(VERSION_ENV, "")
    if selected.strip():
        runtime = resolve_runtime(version=selected)
        return RuntimeSelection(runtime.version, runtime.index_url, "override", True, {}, {})
    package_url = DEV_INDEX.rstrip("/") + f"/{PACKAGE_ID}/"
    with urllib.request.urlopen(package_url, timeout=120) as response:
        page = response.read().decode("utf-8")
    return select_nightly(page)


def write_selection(selection: RuntimeSelection, output_dir: Path) -> Path:
    output = resolve_safe_output_directory(output_dir)
    # Validate again before publishing a version into an Azure logging command.
    runtime = resolve_runtime(version=selection.version)
    if runtime.version != selection.version or not selection.version:
        raise ValueError("Selection version must be an exact validated version")
    output.mkdir(parents=True, exist_ok=True)
    for filename in ("runtime-selection.json", "mssql-python-rs.version"):
        destination = output / filename
        if destination.is_symlink() or destination.exists() and not destination.is_file():
            raise ValueError(f"Unsafe selection artifact destination: {destination}")
    (output / "runtime-selection.json").write_text(
        json.dumps(asdict(selection), indent=2) + "\n", encoding="utf-8", newline="\n"
    )
    (output / "mssql-python-rs.version").write_text(
        selection.version + "\n", encoding="ascii", newline="\n"
    )
    return output


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version")
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    selection = resolve_pr_runtime(version=args.version)
    write_selection(selection, args.output_dir)
    label = "feed-validated nightly" if selection.channel == "nightly" else "exact override"
    print(f"Selected {label}: {selection.version} from {selection.index_url}")
    print(f"##vso[task.setvariable variable=version;isOutput=true]{selection.version}")


if __name__ == "__main__":
    main()
