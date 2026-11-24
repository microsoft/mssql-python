"""Stage official, hash-pinned ODBC 18.7 inputs for a test-only wheel build."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tarfile
import time
import urllib.request


VERSION = "18.7.1.1"
DOWNLOAD = "https://download.microsoft.com/download/"
PACKAGES = (
    (
        "windows/x64",
        "https://go.microsoft.com/fwlink/?linkid=2378279",
        "21ef69e4b942f18aced55fa7c3a8d7263004e113a96690bac7509645e1b1a310",
        "msi",
    ),
    (
        "windows/arm64",
        "https://go.microsoft.com/fwlink/?linkid=2378647",
        "fc61b5a9c52b4bb62e0c735f275e7579ede0a62e0e4b60811ef4a63ea55ac52b",
        "msi",
    ),
    (
        "macos/x86_64",
        DOWNLOAD + "28977c7a-6a67-4e76-aaf6-7d8516ef91d8/msodbcsql18-18.7.1.1-amd64.tar.gz",
        "4cc7d917aebc81c17be7e7c09c3c3cab63646970903a42555a8115878078d7e0",
        "tar.gz",
    ),
    (
        "macos/arm64",
        DOWNLOAD + "28977c7a-6a67-4e76-aaf6-7d8516ef91d8/msodbcsql18-18.7.1.1-arm64.tar.gz",
        "2baaf65fa40ab6221f62ce759fb53b7ce00c2786beffdc9348c18a463e7ddf08",
        "tar.gz",
    ),
    (
        "linux/debian_ubuntu/x86_64",
        "https://packages.microsoft.com/ubuntu/24.04/prod/pool/main/m/msodbcsql18/"
        "msodbcsql18_18.7.1.1-1_amd64.deb",
        "f506cb3de8b34cd94e9014d247b39e9dfd7effad79edad3ab67096dc3c1b6b0e",
        "deb",
    ),
    (
        "linux/debian_ubuntu/arm64",
        "https://packages.microsoft.com/ubuntu/24.04/prod/pool/main/m/msodbcsql18/"
        "msodbcsql18_18.7.1.1-1_arm64.deb",
        "6a4cafb5efe6b899aeb674de56fdceee2380930f1a46c3693ba70f41291c81eb",
        "deb",
    ),
    (
        "linux/rhel/x86_64",
        "https://packages.microsoft.com/rhel/9/prod/Packages/m/"
        "msodbcsql18-18.7.1.1-1.x86_64.rpm",
        "2f7674c5e7d9df6a83b17d6a13326524ce02999ac37ac6739bfa2616aa088777",
        "rpm",
    ),
    (
        "linux/rhel/arm64",
        "https://packages.microsoft.com/rhel/9/prod/Packages/m/"
        "msodbcsql18-18.7.1.1-1.aarch64.rpm",
        "3bd510518be3eaa87e6b21fa3d74e6c469ac4b1e71b0e254acff47fc46dd89b1",
        "rpm",
    ),
    (
        "linux/suse/x86_64",
        "https://packages.microsoft.com/sles/15/prod/Packages/m/"
        "msodbcsql18-18.7.1.1-1.x86_64.rpm",
        "5d91151af4ee5cd9ed0cc850d4500b7d9e0538c7e22f5a6b23e4fcaeec889af4",
        "rpm",
    ),
    (
        "linux/alpine/x86_64",
        DOWNLOAD + "ade174b7-8cea-4543-91a6-c33ae320c2f0/msodbcsql18_18.7.1.1-1_amd64.apk",
        "9ff10ec65dcbbe9ad57ed7fca648d8e7d09d4a25fc3c1095046f36a29f86b84a",
        "apk",
    ),
    (
        "linux/alpine/arm64",
        DOWNLOAD + "ade174b7-8cea-4543-91a6-c33ae320c2f0/msodbcsql18_18.7.1.1-1_arm64.apk",
        "d42f31124b2eb88633ed1ef5159add84541149d2ecebacdfa6e638719e26a48c",
        "apk",
    ),
)


def copy_file(source, destination):
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, destination)


def audit_license(path):
    audit_license_bytes(path.read_bytes(), str(path))


def audit_license_bytes(data, label):
    encoding = "utf-16" if data.startswith((b"\xff\xfe", b"\xfe\xff")) else "utf-8-sig"
    try:
        text = data.decode(encoding)
    except UnicodeDecodeError:
        if encoding != "utf-8-sig":
            raise
        text = data.decode("cp1252")
    if (
        "\ufffd" in text
        or any(ord(char) < 32 and char not in "\r\n\t" for char in text)
        or "MICROSOFT" not in text.upper()
        or "LICENSE" not in text.upper()
    ):
        raise RuntimeError(f"Missing or corrupted Microsoft license terms: {label}")
    if text.lstrip().startswith("ChangeLog"):
        raise RuntimeError(f"Release notes were substituted for license terms: {label}")



def extract_regular_tar(archive_path, destination):
    with tarfile.open(archive_path, "r:*", ignore_zeros=True) as archive:
        for member in archive:
            if not member.isfile():
                continue
            target = (destination / member.name).resolve()
            if not target.is_relative_to(destination.resolve()):
                raise RuntimeError(f"Unsafe archive member: {member.name}")
            target.parent.mkdir(parents=True, exist_ok=True)
            with archive.extractfile(member) as source, target.open("wb") as out:
                shutil.copyfileobj(source, out)


def unpack_deb_payload(archive, destination):
    data = archive.read_bytes()
    if not data.startswith(b"!<arch>\n"):
        raise RuntimeError(f"Invalid Debian archive: {archive}")
    offset = 8
    while offset + 60 <= len(data):
        header = data[offset : offset + 60]
        size = int(header[48:58])
        start = offset + 60
        if size < 0 or start + size > len(data) or header[58:60] != b"`\n":
            raise RuntimeError(f"Invalid Debian member header: {archive}")
        name = header[:16].decode("ascii").strip().rstrip("/")
        if name == "data.tar.xz":
            payload = destination / name
            payload.write_bytes(data[start : start + size])
            extract_regular_tar(payload, destination)
            return
        offset = start + size + size % 2
    raise RuntimeError(f"Debian archive has no data.tar.xz: {archive}")


def register_runtime():
    if os.environ.get("MSSQL_PYTHON_QUALIFY_ODBC_187") == "1":
        subprocess.run([sys.executable, "setup_odbc.py", "egg_info"], check=True)


def stage(destination, cache, version_file=None):
    receipt = []
    for target, url, expected, kind in PACKAGES:
        name = target.replace("/", "-")
        archive = cache / f"{name}.{kind}"
        extracted = cache / name
        print(f"Preparing {target}: {url}", flush=True)
        if not archive.exists():
            started = time.monotonic()
            with urllib.request.urlopen(url, timeout=20) as response, archive.open("wb") as out:
                while True:
                    if time.monotonic() - started > 120:
                        raise TimeoutError(f"Download exceeded 120 seconds: {url}")
                    chunk = response.read(1024 * 1024)
                    if not chunk:
                        break
                    out.write(chunk)
        actual = hashlib.sha256(archive.read_bytes()).hexdigest()
        if actual != expected:
            raise RuntimeError(f"SHA256 mismatch for {url}: {actual}, expected {expected}")
        print(f"SHA256 verified; extracting {target}", flush=True)
        extracted.mkdir(parents=True, exist_ok=True)
        if kind == "msi":
            log = cache / f"{name}-msi.log"
            try:
                subprocess.run(
                    [
                        "msiexec.exe",
                        "/a",
                        str(archive.resolve()),
                        "/qn",
                        f"TARGETDIR={extracted.resolve()}",
                        "/L*v",
                        str(log.resolve()),
                    ],
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=180,
                    check=True,
                )
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
                print(f"MSI extraction failed for {target}; installer log: {log}", flush=True)
                if log.exists():
                    data = log.read_bytes()
                    encoding = "utf-16" if data.startswith(b"\xff\xfe") else "utf-8"
                    print("\n".join(data.decode(encoding, errors="replace").splitlines()[-40:]), flush=True)
                raise
            payload = extracted / "Windows" / "System32"
            files = [
                (payload / filename, filename)
                for filename in ("msodbcsql18.dll", "msodbcdiag18.dll", "mssql-auth.dll")
            ]
            files.append((payload / "1033" / "msodbcsqlr18.rll", "1033/msodbcsqlr18.rll"))
            for runtime in (extracted / "System64").glob("*.dll"):
                files.append((runtime, "vcredist/" + runtime.name))
            license_path = (
                extracted
                / "Program Files"
                / "Microsoft SQL Server"
                / "Client SDK"
                / "ODBC"
                / "180"
                / "License Terms"
                / "License_msodbcsql_ENU.txt"
            )
        else:
            if kind == "deb":
                unpack_deb_payload(archive, extracted)
            elif kind == "rpm":
                for relative in (
                    "opt/microsoft/msodbcsql18/lib64/libmsodbcsql-18.7.so.1.1",
                    "opt/microsoft/msodbcsql18/share/resources/en_US/msodbcsqlr18.rll",
                    "usr/share/doc/msodbcsql18/LICENSE.txt",
                ):
                    output = extracted / relative
                    output.parent.mkdir(parents=True, exist_ok=True)
                    with output.open("wb") as out:
                        subprocess.run(
                            ["tar", "-xOf", str(archive), "./" + relative],
                            stdout=out,
                            stdin=subprocess.DEVNULL,
                            timeout=60,
                            check=True,
                        )
            else:
                extract_regular_tar(archive, extracted)
            if target.startswith("macos"):
                payload = extracted / f"msodbcsql-{VERSION}"
                files = [
                    (payload / "lib/libmsodbcsql.18.dylib", "lib/libmsodbcsql.18.dylib"),
                    (
                        payload / "share/msodbcsql18/resources/en_US/msodbcsqlr18.rll",
                        "share/msodbcsql18/resources/en_US/msodbcsqlr18.rll",
                    ),
                    (
                        payload / "share/doc/msodbcsql18/RELEASE_NOTES",
                        "share/doc/msodbcsql18/RELEASE_NOTES",
                    ),
                ]
                license_path = payload / "share/doc/msodbcsql18/LICENSE.txt"
            else:
                payload = extracted / "opt/microsoft/msodbcsql18"
                files = [
                    (payload / "lib64/libmsodbcsql-18.7.so.1.1", "lib/libmsodbcsql-18.7.so.1.1"),
                    (
                        payload / "share/resources/en_US/msodbcsqlr18.rll",
                        "share/resources/en_US/msodbcsqlr18.rll",
                    ),
                ]
                license_path = extracted / "usr/share/doc/msodbcsql18/LICENSE.txt"
        target_dir = destination / target
        for source, relative in files:
            copy_file(source, target_dir / relative)
        copy_file(license_path, target_dir / "MICROSOFT_ODBC_DRIVER_FOR_SQL_SERVER_LICENSE.txt")
        audit_license(target_dir / "MICROSOFT_ODBC_DRIVER_FOR_SQL_SERVER_LICENSE.txt")
        if target.startswith("macos"):
            copy_file(license_path, target_dir / "share/doc/msodbcsql18/LICENSE.txt")
        elif target.startswith("linux"):
            for old in (target_dir / "lib").glob("libmsodbcsql-18.6.so.*"):
                old.unlink()
        driver = target_dir / files[0][1]
        manifest = {
            "version": VERSION,
            "target": target,
            "source_url": url,
            "source_sha256": actual,
            "source_commit": os.environ.get("BUILD_SOURCEVERSION"),
            "driver": {
                "path": files[0][1],
                "sha256": hashlib.sha256(driver.read_bytes()).hexdigest(),
            },
            "payload_sha256": {
                relative: hashlib.sha256((target_dir / relative).read_bytes()).hexdigest()
                for relative in [
                    *(relative for _, relative in files),
                    "MICROSOFT_ODBC_DRIVER_FOR_SQL_SERVER_LICENSE.txt",
                ]
            },
        }
        (target_dir / "odbc-187-driver.json").write_text(
            json.dumps(manifest, indent=2), encoding="utf-8"
        )
        receipt.append({"target": target, "url": url, "sha256": actual, "version": VERSION})
        print(f"Staged verified {VERSION}: {target}", flush=True)
    (cache / "odbc-187-inputs.json").write_text(json.dumps(receipt, indent=2), encoding="utf-8")
    if version_file is not None:
        text, replacements = re.subn(
            r"^__version__\s*=\s*['\"][^'\"]+['\"]",
            f'__version__ = "{VERSION}"',
            version_file.read_text(encoding="utf-8"),
            count=1,
            flags=re.MULTILINE,
        )
        if replacements != 1:
            raise RuntimeError(f"Could not update candidate package version: {version_file}")
        version_file.write_text(text, encoding="utf-8")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--destination", type=Path)
    parser.add_argument("--cache", type=Path)
    parser.add_argument("--version-file", type=Path)
    parser.add_argument("--register-runtime", action="store_true")
    args = parser.parse_args()
    if args.register_runtime:
        register_runtime()
    else:
        if args.destination is None or args.cache is None:
            parser.error("--destination and --cache are required for staging")
        args.cache.mkdir(parents=True, exist_ok=True)
        stage(args.destination, args.cache, args.version_file)
