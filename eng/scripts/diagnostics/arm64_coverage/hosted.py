"""Hosted-only ARM64 diagnostic admission, execution, redaction, and owned cleanup."""

import argparse
import base64
import hashlib
import html
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
import time
import urllib.parse
import xml.etree.ElementTree as ET

LABEL = "com.microsoft.mssql-python.ci-owner"
RELATIVE = "eng/scripts/diagnostics/arm64_coverage"
TESTS = [
    "tests/test_013_SqlHandle_free_shutdown.py",
    "tests/test_024_context_manager_transaction.py",
]


def admission():
    if (
        os.environ.get("AGENT_ISSELFHOSTED") != "0"
        or os.environ.get("AGENT_OS") != "Linux"
        or os.environ.get("TF_BUILD") != "True"
    ):
        raise RuntimeError("This diagnostic requires a Microsoft-hosted Linux Azure Pipelines job")


def execute(argv, *, env=None, timeout=120):
    return subprocess.run(argv, env=env, capture_output=True, timeout=timeout)


def checked(argv, *, env=None, timeout=120):
    result = execute(argv, env=env, timeout=timeout)
    if result.returncode:
        detail = (result.stdout + result.stderr)[-4096:].decode("utf-8", errors="replace")
        password = os.environ.get("DB_PASSWORD")
        if password:
            detail = sanitize(detail, password)
        raise RuntimeError(f"Command failed with exit {result.returncode}: {argv[0]}\n{detail}")
    return result.stdout


def sanitize(data, password):
    if not password:
        raise ValueError("A nonempty redaction secret is required")
    text = data.decode("utf-8", errors="replace") if isinstance(data, bytes) else data
    for secret in {
        password,
        html.escape(password, quote=True),
        urllib.parse.quote(password, safe=""),
        base64.b64encode(password.encode()).decode(),
    }:
        text = text.replace(secret, "[REDACTED]")
    return text


def public_write(folder, name, data, password):
    if Path(name).name != name or Path(name).suffix not in {".json", ".xml", ".txt"}:
        raise RuntimeError("Unapproved diagnostic artifact path")
    if len(data) > 16 * 1024 * 1024:
        raise RuntimeError("Diagnostic artifact exceeds its size limit")
    text = sanitize(data, password)
    if name.endswith(".xml"):
        ET.fromstring(text)
    elif name.endswith(".json"):
        json.loads(text)
    if password in text:
        raise RuntimeError("Secret remained in the public artifact")
    (folder / name).write_text(text, encoding="utf-8")


def inspect_owned(name, owner, absent_ok=False):
    result = execute(["docker", "container", "inspect", name], timeout=20)
    if result.returncode:
        error = result.stderr.decode("utf-8", errors="replace").lower()
        if absent_ok and ("no such container" in error or "no such object" in error):
            return None
        raise RuntimeError(f"Could not inspect owned container {name}")
    rows = json.loads(result.stdout)
    labels = rows[0]["Config"]["Labels"] if len(rows) == 1 else None
    if not isinstance(labels, dict) or labels.get(LABEL) != owner:
        raise RuntimeError(f"Container ownership mismatch: {name}")
    return rows[0]


def paths():
    root = Path(os.environ["BUILD_SOURCESDIRECTORY"])
    scratch = Path(os.environ["AGENT_TEMPDIRECTORY"]) / "task47285"
    return root, scratch, scratch / "public"


def guard(args):
    admission()
    root, scratch, public = paths()
    scratch.mkdir(mode=0o700, exist_ok=False)
    public.mkdir(mode=0o700)
    head = checked(["git", "rev-parse", "HEAD"]).decode().strip()
    if head != os.environ["BUILD_SOURCEVERSION"]:
        raise RuntimeError("Checkout does not match the queued diagnostic commit")
    expected = json.loads((root / RELATIVE / "expected.json").read_text())
    for name, digest in expected["protected_sources"].items():
        if hashlib.sha256((root / name).read_bytes()).hexdigest() != digest:
            raise RuntimeError(f"Diagnostic checkout changed protected source {name}")
    identity = {
        "revision": head,
        "base_revision": expected["base_sha"],
        "owner": args.owner,
        "distro": args.distro,
        "protected_sources": expected["protected_sources"],
    }
    (scratch / "identity.json").write_text(json.dumps(identity, indent=2))
    password = secrets.token_urlsafe(24) + "Aa1!"
    print("##vso[task.setvariable variable=DB_PASSWORD;issecret=true]" + password)
    print(json.dumps({"admitted": True, "revision": head, "distro": args.distro}))


def test(args):
    admission()
    root, scratch, public = paths()
    password = os.environ["DB_PASSWORD"]
    identity = json.loads((scratch / "identity.json").read_text())
    if identity["owner"] != args.owner:
        raise RuntimeError("Diagnostic job identity changed")
    import emulation_setup

    emulator = emulation_setup.verify_client(args)
    public_write(public, "emulator.json", json.dumps(emulator, indent=2), password)
    client = inspect_owned(args.client, args.owner)
    server = inspect_owned(args.server, args.owner)
    networks = server["NetworkSettings"]["Networks"]
    if set(networks) != {"bridge"} or not networks["bridge"]["IPAddress"]:
        raise RuntimeError("Unexpected SQL container network")
    address = networks["bridge"]["IPAddress"]
    environment = dict(
        os.environ,
        SQLCMDPASSWORD=password,
        DB_CONNECTION_STRING=(
            f"Server={address};Database=TestDB;UID=sa;"
            f"PWD={password};Encrypt=yes;TrustServerCertificate=yes"
        ),
    )
    sql = checked(
        [
            "docker",
            "exec",
            "-e",
            "SQLCMDPASSWORD",
            args.server,
            "/opt/mssql-tools18/bin/sqlcmd",
            "-S",
            "localhost",
            "-U",
            "sa",
            "-C",
            "-b",
            "-h",
            "-1",
            "-W",
            "-Q",
            "SET NOCOUNT ON; SELECT CONVERT(int, SERVERPROPERTY('ProductMajorVersion'));",
        ],
        env=environment,
        timeout=30,
    )
    if sql.decode().strip() != "16":
        raise RuntimeError("The diagnostic requires a real SQL Server 2022 instance")
    runtime = checked(
        ["docker", "exec", args.client, args.python, f"{RELATIVE}/runtime_info.py"],
        timeout=120,
    )
    public_write(public, "runtime.json", runtime, password)
    images = {}
    for kind, container in (("client", client), ("server", server)):
        image = json.loads(checked(["docker", "image", "inspect", container["Image"]]))[0]
        images[kind] = {
            "image_id": container["Image"],
            "digests": image.get("RepoDigests", []),
            "architecture": image["Architecture"],
        }
    if images["client"]["architecture"] != "arm64" or images["server"]["architecture"] != "amd64":
        raise RuntimeError("Expected an emulated ARM64 client and native x64 SQL Server")
    public_write(public, "identity.json", json.dumps({**identity, "images": images}), password)
    command = [
        "docker",
        "exec",
        "-e",
        "DB_CONNECTION_STRING",
        "-e",
        "DB_PASSWORD",
        "-e",
        "PYTHONFAULTHANDLER=1",
        "-e",
        "TASK47285_REPORT_DIR=/tmp/task47285",
        "-e",
        f"PYTHONPATH=/workspace/{RELATIVE}:/workspace",
        args.client,
        args.python,
        "-m",
        "pytest",
        *TESTS,
        "-p",
        "qemu_unskip",
        "-m",
        "not stress",
        "-o",
        "addopts=",
        "-v",
        "--color=no",
        "--junitxml=/tmp/task47285/pytest.xml",
        "--cov=.",
        "--cov-report=xml:/tmp/task47285/coverage.xml",
        "--capture=tee-sys",
        "--cache-clear",
    ]
    started = time.monotonic()
    timed_out = False
    try:
        result = execute(command, env=environment, timeout=900)
        output = result.stdout + result.stderr
        exit_code = result.returncode
    except subprocess.TimeoutExpired as exc:
        output = (exc.stdout or b"") + (exc.stderr or b"")
        exit_code = None
        timed_out = True
    public_write(public, "pytest-output.txt", output[-2 * 1024 * 1024 :], password)
    outcome = {
        "pytest_root_exit_code": exit_code,
        "timeout": timed_out,
        "seconds": time.monotonic() - started,
        "accepted": False,
    }
    public_write(public, "outcome.json", json.dumps(outcome), password)
    copied = execute(
        ["docker", "cp", f"{args.client}:/tmp/task47285/.", str(scratch / "raw")],
        timeout=30,
    )
    outcome["artifact_errors"] = []
    if copied.returncode:
        outcome["artifact_errors"].append(f"Result collection exited {copied.returncode}")
    for name in ("pytest.xml", "coverage.xml", "processes.json"):
        path = scratch / "raw" / name
        if path.is_file():
            public_write(public, name, path.read_bytes(), password)
        else:
            outcome["artifact_errors"].append(f"Missing result: {name}")
    if outcome["artifact_errors"]:
        public_write(public, "outcome.json", json.dumps(outcome, indent=2), password)
        (scratch / "collection-complete").write_text("partial failure\n")
        print(json.dumps(outcome))
        return 1
    cases = list(ET.parse(public / "pytest.xml").iter("testcase"))
    processes = json.loads((public / "processes.json").read_text())
    expected = json.loads((root / RELATIVE / "expected.json").read_text())
    nodeids = {
        case.attrib["classname"].rsplit(".", 1)[0].replace(".", "/")
        + ".py::"
        + case.attrib["classname"].rsplit(".", 1)[1]
        + "::"
        + case.attrib["name"]
        for case in cases
    }
    outcome["cases"] = len(cases)
    outcome["accepted"] = (
        exit_code == 0
        and not timed_out
        and len(cases) == 44
        and nodeids == set(expected["nodeids"])
        and not any(
            case.find(tag) is not None for case in cases for tag in ("failure", "error", "skipped")
        )
        and not processes["errors"]
        and processes["final_exitstatus"] == 0
        and set(processes["collected"]) == set(expected["nodeids"])
        and len(processes["removed"]) == 2
        and all(type(row["condition"]) is bool for row in processes["removed"])
        and emulator["running_client_verified"]
        and emulator["guest_argv_verified"]
    )
    public_write(public, "outcome.json", json.dumps(outcome, indent=2), password)
    (scratch / "collection-complete").write_text("complete\n")
    print(json.dumps(outcome))
    return 0 if outcome["accepted"] else 1


def cleanup(args):
    admission()
    root, scratch, public = paths()
    if not (scratch / "identity.json").exists():
        raise RuntimeError("Cleanup requires successful hosted admission")
    password = os.environ["DB_PASSWORD"]
    errors = []
    try:
        result = execute(
            [
                sys.executable,
                str(root / "eng/scripts/setup_sql_container.py"),
                "--cleanup",
                "--name",
                args.server,
                "--owner",
                args.owner,
            ],
            timeout=65,
        )
        if result.returncode:
            errors.append(f"SQL cleanup exited {result.returncode}")
        public_write(public, "sql-cleanup.txt", result.stdout + result.stderr, password)
    except (subprocess.TimeoutExpired, OSError, RuntimeError, ValueError) as exc:
        errors.append(type(exc).__name__ + " during SQL cleanup")
    try:
        container = inspect_owned(args.client, args.owner, absent_ok=True)
        if container:
            checked(["docker", "container", "rm", "-f", container["Id"]], timeout=35)
            if inspect_owned(container["Id"], args.owner, absent_ok=True) is not None:
                raise RuntimeError("The owned client still exists after removal")
    except (subprocess.TimeoutExpired, RuntimeError, OSError) as exc:
        errors.append(str(exc))
    public_write(public, "cleanup.json", json.dumps({"errors": errors}), password)
    if errors:
        raise RuntimeError("Owned cleanup failed: " + "; ".join(errors))
    if (scratch / "collection-complete").exists():
        for path in public.iterdir():
            if path.suffix not in {".json", ".xml", ".txt"}:
                raise RuntimeError("Unexpected public artifact")
            data = path.read_text()
            if sanitize(data, password) != data:
                raise RuntimeError("Public artifact privacy validation failed")
        print("##vso[task.setvariable variable=TASK47285_ARTIFACT_READY]true")
        if (public / "pytest.xml").exists():
            print("##vso[task.setvariable variable=TASK47285_HAS_JUNIT]true")
    print("Owned SQL/client cleanup completed")
    return 0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("guard", "test", "cleanup"))
    parser.add_argument("--client", required=True)
    parser.add_argument("--server", required=True)
    parser.add_argument("--python", required=True)
    parser.add_argument("--owner", required=True)
    parser.add_argument("--distro", required=True)
    args = parser.parse_args()
    try:
        result = {"guard": guard, "test": test, "cleanup": cleanup}[args.action](args)
        return result or 0
    except (
        RuntimeError,
        OSError,
        ValueError,
        KeyError,
        subprocess.TimeoutExpired,
        ET.ParseError,
    ) as exc:
        password = os.environ.get("DB_PASSWORD")
        message = sanitize(str(exc), password) if password else str(exc)
        print("ARM64 diagnostic failed: " + message, file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
