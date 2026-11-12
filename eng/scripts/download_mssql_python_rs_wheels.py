#!/usr/bin/env python3
"""Stage exact-version runtime wheels from PyPI or the development wheel index."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import urllib.request
import zipfile
from email.parser import BytesParser
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import parse_qs, unquote, urldefrag, urljoin, urlsplit

if __package__:
    from . import mssql_python_build_safety
    from .mssql_python_rs import DEFAULT_INDEX, DEV_INDEX, VERSION_FILE, resolve_runtime
else:
    import mssql_python_build_safety
    from mssql_python_rs import DEFAULT_INDEX, DEV_INDEX, VERSION_FILE, resolve_runtime

PACKAGE_ID = "mssql-python-rs"


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def validate_wheel(path: Path, version: str) -> None:
    if not path.name.startswith(f"mssql_python_rs-{version}-") or path.suffix != ".whl":
        raise ValueError(f"Unexpected wheel name for {PACKAGE_ID} {version}: {path.name}")
    with zipfile.ZipFile(path) as wheel:
        metadata = [name for name in wheel.namelist() if name.endswith(".dist-info/METADATA")]
        if len(metadata) != 1:
            raise ValueError(f"{path.name} must contain exactly one wheel METADATA")
        identity = BytesParser().parsebytes(wheel.read(metadata[0]))
    name = re.sub(r"[-_.]+", "-", identity.get("Name", "")).lower()
    if name != PACKAGE_ID or identity.get("Version") != version:
        raise ValueError(f"{path.name} metadata does not match {PACKAGE_ID}=={version}")


class _WheelLinks(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "a":
            attributes = dict(attrs)
            href = attributes.get("href")
            if href and "data-yanked" not in attributes:
                self.links.append(href)


def download_wheels(
    index_url: str | None,
    distribution_version_file: Path,
    output_dir: Path,
    *,
    version: str | None = None,
) -> list[Path]:
    runtime = resolve_runtime(distribution_version_file, version=version)
    index_url = index_url or runtime.index_url
    package_url = index_url.rstrip("/") + f"/{PACKAGE_ID}/"
    if urlsplit(package_url).scheme != "https":
        raise ValueError("The runtime wheel index must use HTTPS")
    output_dir = mssql_python_build_safety.resolve_safe_output_directory(output_dir)
    with urllib.request.urlopen(package_url, timeout=120) as response:
        page = response.read().decode("utf-8")
    parser = _WheelLinks()
    parser.feed(page)
    candidates: dict[str, tuple[str, str]] = {}
    pattern = re.compile(
        rf"mssql_python_rs-{re.escape(runtime.version)}-cp310-abi3-[A-Za-z0-9_]+\.whl"
    )
    for link in parser.links:
        url, fragment = urldefrag(urljoin(package_url, link))
        filename = unquote(urlsplit(url).path.rsplit("/", 1)[-1])
        if not pattern.fullmatch(filename):
            continue
        if urlsplit(url).scheme != "https":
            raise ValueError(f"Wheel download must use HTTPS: {filename}")
        if filename in candidates:
            raise ValueError(f"Duplicate wheel filename in index: {filename}")
        hashes = parse_qs(fragment).get("sha256", [])
        if len(hashes) != 1 or not re.fullmatch(r"[a-fA-F0-9]{64}", hashes[0]):
            raise ValueError(f"Missing or invalid SHA-256 for {filename}")
        candidates[filename] = (url, hashes[0].lower())
    if not candidates:
        raise ValueError(f"No {PACKAGE_ID}=={runtime.version} wheels found in {package_url}")
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True)
    staged = []
    for filename, (url, expected_hash) in sorted(candidates.items()):
        destination = output_dir / filename
        with urllib.request.urlopen(url, timeout=120) as response:
            with destination.open("wb") as target:
                shutil.copyfileobj(response, target)
        if file_sha256(destination) != expected_hash:
            raise ValueError(f"SHA-256 mismatch for {filename}")
        validate_wheel(destination, runtime.version)
        staged.append(destination)
    receipt = {
        "distribution_version": runtime.version,
        "index_url": index_url,
        "version_override": runtime.version_override,
        "wheel_sha256": {path.name: file_sha256(path) for path in staged},
    }
    (output_dir / "transport.json").write_text(
        json.dumps(receipt, indent=2) + "\n", encoding="utf-8", newline="\n"
    )
    return staged


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--index-url")
    parser.add_argument("--version")
    parser.add_argument("--distribution-version-file", type=Path, default=VERSION_FILE)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    wheels = download_wheels(
        args.index_url,
        args.distribution_version_file,
        args.output_dir,
        version=args.version,
    )
    for wheel in wheels:
        print(wheel)


if __name__ == "__main__":
    main()
