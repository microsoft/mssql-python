"""Verify the loaded ODBC candidate, then run every test file and marker."""

import argparse
import ctypes
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import time

import mssql_python
import mssql_python_odbc

from eng.scripts.stage_odbc_187 import PACKAGES, VERSION


def loaded_driver_path():
    if sys.platform == "win32":
        from ctypes import wintypes

        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel.GetModuleHandleW.argtypes = [wintypes.LPCWSTR]
        kernel.GetModuleHandleW.restype = wintypes.HMODULE
        kernel.GetModuleFileNameW.argtypes = [
            wintypes.HMODULE,
            wintypes.LPWSTR,
            wintypes.DWORD,
        ]
        kernel.GetModuleFileNameW.restype = wintypes.DWORD
        handle = kernel.GetModuleHandleW("msodbcsql18.dll")
        if not handle:
            raise ctypes.WinError(ctypes.get_last_error())
        name = ctypes.create_unicode_buffer(32768)
        length = kernel.GetModuleFileNameW(handle, name, len(name))
        if not length:
            raise ctypes.WinError(ctypes.get_last_error())
        if length == len(name):
            raise RuntimeError("Loaded Windows driver path was truncated")
        return Path(name.value).resolve()
    if sys.platform == "darwin":
        dyld = ctypes.CDLL(None)
        dyld._dyld_image_count.argtypes = []
        dyld._dyld_image_count.restype = ctypes.c_uint32
        dyld._dyld_get_image_name.argtypes = [ctypes.c_uint32]
        dyld._dyld_get_image_name.restype = ctypes.c_char_p
        names = [
            os.fsdecode(dyld._dyld_get_image_name(index))
            for index in range(dyld._dyld_image_count())
        ]
    elif sys.platform.startswith("linux"):
        names = [
            fields[5].replace("\\040", " ")
            for line in Path("/proc/self/maps").read_text().splitlines()
            if len(fields := line.split(maxsplit=5)) == 6
        ]
    else:
        raise RuntimeError(f"Unsupported qualification platform: {sys.platform}")
    paths = {Path(name).resolve() for name in names if Path(name).name.startswith("libmsodbcsql")}
    if len(paths) != 1:
        raise RuntimeError(f"Expected exactly one mapped Microsoft ODBC driver, found {paths}")
    return paths.pop()


def verify_candidate(output):
    distribution_version = importlib.metadata.version("mssql-python-odbc")
    if distribution_version != VERSION or mssql_python_odbc.__version__ != VERSION:
        raise RuntimeError(f"Expected mssql-python-odbc {VERSION}, found {distribution_version}")
    info = mssql_python.get_native_provider_info()
    if info["id"] != "msodbcsql18":
        raise RuntimeError(f"Wrong native provider: {info}")
    driver_path = Path(info["driver_path"]).resolve()
    libs = Path(mssql_python_odbc.get_libs_dir()).resolve()
    if not driver_path.is_relative_to(libs):
        raise RuntimeError(f"Driver loaded outside the candidate package: {driver_path}")
    manifests = []
    for path in libs.rglob("odbc-187-driver.json"):
        manifest = json.loads(path.read_text(encoding="utf-8"))
        if (path.parent / manifest["driver"]["path"]).resolve() == driver_path:
            manifests.append((manifest, path.parent.resolve()))
    if len(manifests) != 1:
        raise RuntimeError(f"Expected one staged driver receipt for {driver_path}")
    manifest, receipt_dir = manifests[0]
    sources = {target: (url, digest) for target, url, digest, _ in PACKAGES}
    if manifest["version"] != VERSION or sources.get(manifest["target"]) != (
        manifest["source_url"],
        manifest["source_sha256"],
    ):
        raise RuntimeError("Driver receipt does not match the official 18.7.1.1 inputs")
    if manifest.get("source_commit") and manifest["source_commit"] != os.environ.get(
        "BUILD_SOURCEVERSION"
    ):
        raise RuntimeError("Driver staging and runtime source revisions do not match")
    driver = manifest["driver"]
    actual = hashlib.sha256(driver_path.read_bytes()).hexdigest()
    if actual != driver.get("prepared_sha256", driver["sha256"]):
        raise RuntimeError(f"Loaded driver does not match its staged binary: {driver_path}")
    for relative, expected in manifest.get("payload_sha256", {}).items():
        path = (receipt_dir / relative).resolve()
        if not path.is_relative_to(receipt_dir.resolve()):
            raise RuntimeError(f"Unsafe driver payload path: {relative}")
        if path != driver_path and hashlib.sha256(path.read_bytes()).hexdigest() != expected:
            raise RuntimeError(f"Driver resource or license does not match its official input: {path}")
    with mssql_python.connect(os.environ["DB_CONNECTION_STRING"]) as connection:
        version = connection.getinfo(mssql_python.SQL_DRIVER_VER)
        name = connection.getinfo(mssql_python.SQL_DRIVER_NAME)
        parts = [int(part) for part in version.split(".")]
        if parts not in ([18, 7, 1], [18, 7, 1, 1]):
            raise RuntimeError(f"Expected ODBC 18.7.1.1, loaded {version} ({name})")
        mapped_path = loaded_driver_path()
        if mapped_path != driver_path:
            raise RuntimeError(f"OS loaded {mapped_path}, not the staged driver {driver_path}")
        expected_sql_major = os.environ.get("MSSQL_PYTHON_EXPECT_SQL_MAJOR")
        with connection.cursor() as cursor:
            cursor.execute("SELECT CAST(SERVERPROPERTY('ProductMajorVersion') AS int)")
            server_major = cursor.fetchone()[0]
        if expected_sql_major and server_major != int(expected_sql_major):
            raise RuntimeError(f"Expected SQL Server major {expected_sql_major}, found {server_major}")
    evidence = {
        "distribution_version": distribution_version,
        "driver": name,
        "driver_version": version,
        "driver_path": str(driver_path),
        "mapped_driver_path": str(mapped_path),
        "driver_sha256": actual,
        "official_input": manifest,
        "provider": info,
        "python": sys.version,
        "os": platform.platform(),
        "architecture": platform.machine(),
        "sql_server_major": server_major,
        "source_commit": os.environ.get("BUILD_SOURCEVERSION"),
    }
    output.write_text(json.dumps(evidence, indent=2), encoding="utf-8")
    print(json.dumps(evidence, indent=2), flush=True)


def pytest_configure(config):
    config.option.markexpr = ""


def pytest_sessionstart(session):
    """Check the actual driver in each pytest process, before executing tests."""
    output = os.environ.get("MSSQL_PYTHON_ODBC_187_EVIDENCE")
    if not output:
        raise RuntimeError("Missing per-process ODBC qualification evidence path")
    verify_candidate(Path(output))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tests", type=Path, required=True)
    parser.add_argument("--results", type=Path, required=True)
    parser.add_argument("--coverage", action="store_true")
    args = parser.parse_args()
    args.results.mkdir(parents=True, exist_ok=True)
    verify_candidate(args.results / "driver.json")
    failures = []
    attempts = []
    files = sorted(args.tests.glob("test_*.py"))
    if not files:
        raise RuntimeError(f"No test files found in {args.tests}")
    for test_file in files:
        started = time.monotonic()
        coverage_args = (
            [
                "--cov=mssql_python",
                "--cov-append",
                f"--cov-report=xml:{(args.results / 'coverage.xml').resolve()}",
            ]
            if args.coverage
            else []
        )
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "pytest",
                "-p",
                "eng.scripts.run_odbc_187_tests",
                str(test_file),
                "-v",
                "-m",
                "",
                "--junitxml",
                str(args.results / f"{test_file.stem}.xml"),
                *coverage_args,
            ],
            check=False,
            env={
                **os.environ,
                "MSSQL_PYTHON_NATIVE_PROVIDER": "msodbcsql18",
                "MSSQL_PYTHON_ODBC_187_EVIDENCE": str(
                    (args.results / f"{test_file.stem}.driver.json").resolve()
                ),
            },
        )
        attempts.append(
            {
                "file": test_file.name,
                "exit_code": result.returncode,
                "elapsed_seconds": time.monotonic() - started,
            }
        )
        if result.returncode != 0:
            failures.append({"file": test_file.name, "exit_code": result.returncode})
    (args.results / "summary.json").write_text(
        json.dumps(
            {"files_attempted": len(files), "attempts": attempts, "failures": failures}, indent=2
        ),
        encoding="utf-8",
    )
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
