#!/usr/bin/env python3
"""Stage exact-version runtime wheels from PyPI or the development wheel index."""

from __future__ import annotations

import argparse
import hashlib
import http.client
import json
import re
import shutil
import socket
import tempfile
import time
import urllib.error
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
DOWNLOAD_ATTEMPTS = 3


def _transient(error: Exception) -> bool:
    if isinstance(error, urllib.error.HTTPError):
        return error.code in {408, 429, 500, 502, 503, 504}
    if isinstance(error, urllib.error.URLError):
        error = error.reason
    return isinstance(
        error,
        (TimeoutError, socket.gaierror, ConnectionError, http.client.IncompleteRead),
    )


def _fetch(url: str, consume):
    """Retry only transient transport failures; never retry invalid wheel contents."""
    for attempt in range(DOWNLOAD_ATTEMPTS):
        try:
            with urllib.request.urlopen(url, timeout=120) as response:
                return consume(response)
        except (
            urllib.error.URLError,
            TimeoutError,
            ConnectionError,
            http.client.IncompleteRead,
        ) as error:
            if not _transient(error) or attempt + 1 == DOWNLOAD_ATTEMPTS:
                raise
            delay = 2 ** (attempt + 1)
            print(f"Transient runtime download failure; retry {attempt + 2}/3 in {delay}s")
            time.sleep(delay)


def verify_staged_wheels(
    output_dir: Path, distribution_version_file: Path, *, version: str | None = None
) -> list[Path]:
    """Recheck early-stage transport without a network fallback or version re-resolution."""
    runtime = resolve_runtime(distribution_version_file, version=version)
    receipt = json.loads((output_dir / "transport.json").read_text(encoding="utf-8"))
    if (
        receipt["distribution_version"] != runtime.version
        or receipt["version_override"] != runtime.version_override
        or receipt["index_url"] != runtime.index_url
    ):
        raise ValueError("Staged runtime selection does not match the effective runtime")
    hashes = receipt["wheel_sha256"]
    actual = {path.name for path in output_dir.glob("*.whl")}
    if not hashes or set(hashes) != actual:
        raise ValueError("Staged runtime wheel set does not match receipt")
    wheels = []
    for name, digest in sorted(hashes.items()):
        if Path(name).name != name or "/" in name or "\\" in name:
            raise ValueError("Unsafe staged runtime wheel name")
        path = output_dir / name
        if path.is_symlink() or file_sha256(path) != digest:
            raise ValueError(f"SHA-256 mismatch for {name}")
        validate_wheel(path, runtime.version)
        wheels.append(path)
    return wheels


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
    page = _fetch(package_url, lambda response: response.read().decode("utf-8"))
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

        def stage(response):
            # Each attempt starts with a new file, never appending a partial response.
            with tempfile.NamedTemporaryFile(dir=output_dir, suffix=".part", delete=False) as target:
                temporary = Path(target.name)
                try:
                    shutil.copyfileobj(response, target)
                    target.close()
                    if file_sha256(temporary) != expected_hash:
                        raise ValueError(f"SHA-256 mismatch for {filename}")
                    # Preserve the expected filename for the wheel identity check.
                    temporary.replace(destination)
                    try:
                        validate_wheel(destination, runtime.version)
                    except (ValueError, zipfile.BadZipFile):
                        destination.unlink(missing_ok=True)
                        raise
                finally:
                    target.close()
                    temporary.unlink(missing_ok=True)

        _fetch(url, stage)
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
    parser.add_argument("--verify-only", action="store_true")
    args = parser.parse_args()
    if args.verify_only:
        wheels = verify_staged_wheels(
            args.output_dir, args.distribution_version_file, version=args.version
        )
    else:
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
