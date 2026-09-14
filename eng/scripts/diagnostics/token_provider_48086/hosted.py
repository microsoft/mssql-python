"""Hosted-only disposable resources. Never publish raw logs, credentials or binaries."""

import argparse
import json
import os
from pathlib import Path
import platform
import re
import secrets
import sys
import time
import xml.etree.ElementTree as ET

from contracts import (
    AUTH,
    BASE,
    HERE,
    LABEL,
    REL,
    bounded,
    canonical,
    checked,
    clean_env,
    redact,
    require,
    save,
    sha,
    source_pair,
)

REGISTRATION = Path("/proc/sys/fs/binfmt_misc/qemu-aarch64")
QEMU_IMAGE = (
    "tonistiigi/binfmt@sha256:400a4873b838d1b89194d982c45e5fb3cda4593fbfd7e08a02e76b03b21166f0"
)
QEMU_SHA = "1ad17b7bd5e15ce60075d0994d5c5e3914d16899a1e3119040b5c9e76e067f24"


def admission():
    require(
        os.environ.get("AGENT_ISSELFHOSTED") == "0"
        and os.environ.get("TF_BUILD") == "True"
        and os.environ.get("AGENT_OS") == "Linux"
        and platform.system() == "Linux"
        and platform.machine().lower() in ("amd64", "x86_64"),
        "Requires a disposable Microsoft-hosted Linux x64 Azure agent",
    )
    require(
        os.environ.get("SYSTEM_JOBATTEMPT") == "1"
        and os.environ.get("SYSTEM_STAGEATTEMPT", "1") == "1",
        "Only first attempt is allowed",
    )
    branch = os.environ["BUILD_SOURCEBRANCH"]
    require(
        branch == "refs/heads/jahnvi/test-48086-token-provider-mocks-20260914",
        "Not the approved testing branch",
    )
    owner = os.environ["BUILD_BUILDID"] + "." + os.environ["SYSTEM_JOBID"]
    require(re.fullmatch(r"[0-9]+\.[a-fA-F0-9-]+", owner) is not None, "Invalid job ownership")
    return owner


def owned(name, owner, kind="container", absent=False):
    result, output = bounded(["docker", kind, "inspect", name], timeout=20, env=clean_env())
    if result["returncode"]:
        if (
            absent
            and not result["timeout"]
            and (
                b"No such container" in output
                or b"No such object" in output
                or b"not found" in output
            )
        ):
            return None
        raise RuntimeError("Unable to inspect owned " + kind)
    rows = json.loads(output)
    require(len(rows) == 1, "Ambiguous resource identity")
    labels = rows[0]["Config"].get("Labels") if kind == "container" else rows[0].get("Labels")
    require(isinstance(labels, dict) and labels.get(LABEL) == owner, "Ownership mismatch")
    return rows[0]


def cleanup(owner):
    failures = []
    for suffix in ("client", "sql", "emulator"):
        name = "ab48086-" + owner + "-" + suffix
        try:
            resource = owned(name, owner, absent=True)
            if resource:
                checked(["docker", "container", "rm", "-f", "-v", resource["Id"]], timeout=45)
                require(owned(name, owner, absent=True) is None, "Container survived removal")
        except (RuntimeError, OSError, ValueError, KeyError) as exc:
            failures.append({"resource": name, "error": str(exc)})
    network = "ab48086-" + owner
    try:
        resource = owned(network, owner, kind="network", absent=True)
        if resource:
            checked(["docker", "network", "rm", resource["Id"]], timeout=30)
            require(
                owned(network, owner, kind="network", absent=True) is None,
                "Network survived removal",
            )
        leftovers = checked(["docker", "ps", "-aq", "--filter", "label=" + LABEL + "=" + owner])
        require(not leftovers.strip(), "Owned containers remain")
    except (RuntimeError, OSError, ValueError, KeyError) as exc:
        failures.append({"resource": network, "error": str(exc)})
    return {"owner": owner, "accepted": not failures, "failures": failures}


def registration(text):
    lines = text.strip().splitlines()
    require(lines and lines[0] == "enabled", "ARM64 handler disabled")
    fields = dict(line.split(" ", 1) for line in lines[1:])
    require(
        {"F", "P"}.issubset(set(fields["flags:"]))
        and Path(fields["interpreter"]).name == "qemu-aarch64"
        and fields["offset"] == "0",
        "Unexpected binfmt configuration",
    )
    return fields


def emulator(owner, scratch):
    name = "ab48086-" + owner + "-emulator"
    require(owned(name, owner, absent=True) is None, "Emulator resource already exists")

    def tool(arguments, export=False):
        command = [
            "docker",
            "create",
            "--name",
            name,
            "--label",
            LABEL + "=" + owner,
            "--platform",
            "linux/amd64",
        ]
        command += ["--entrypoint", "/usr/bin/qemu-aarch64"] if export else ["--privileged"]
        identifier = checked([*command, QEMU_IMAGE, *arguments]).decode().strip()
        try:
            if export:
                checked(
                    [
                        "docker",
                        "cp",
                        identifier + ":/usr/bin/qemu-aarch64",
                        str(scratch / "qemu-aarch64"),
                    ]
                )
            else:
                checked(["docker", "start", "-a", identifier], timeout=120)
                state = owned(identifier, owner)["State"]
                require(not state["Running"] and state["ExitCode"] == 0, "Registration failed")
        finally:
            resource = owned(identifier, owner)
            checked(["docker", "container", "rm", "-f", "-v", resource["Id"]], timeout=30)
            require(owned(identifier, owner, absent=True) is None, "Emulator cleanup failed")

    tool(["--version"], export=True)
    binary = scratch / "qemu-aarch64"
    require(sha(binary.read_bytes()) == QEMU_SHA, "Unexpected QEMU binary")
    binary.chmod(0o500)
    version = checked([str(binary), "--version"]).decode().splitlines()[0]
    require("version 10.2.3 " in version, "Unexpected QEMU version")
    # This is hosted disposable admission only, and touches arm64 registration only.
    if REGISTRATION.exists():
        tool(["--uninstall", "qemu-aarch64"])
    tool(["--install", "arm64"])
    return {
        "version": version,
        "sha256": QEMU_SHA,
        "image": QEMU_IMAGE,
        "registration": registration(REGISTRATION.read_text()),
    }


def verify_client(name, owner, record):
    client = owned(name, owner)
    pid = client["State"]["Pid"]
    require(client["State"]["Running"] and type(pid) is int and pid > 0, "Client not running")
    running = checked(["sudo", "-n", "sha256sum", f"/proc/{pid}/exe"]).decode().split()[0]
    require(running == QEMU_SHA, "Actual client interpreter is not approved QEMU")
    require(registration(REGISTRATION.read_text()) == record["registration"], "Binfmt drift")
    argv = checked(
        [
            "docker",
            "exec",
            name,
            "/bin/sh",
            "-c",
            'printf "[%s]\\n" "$0" "$@"',
            "marker",
            "space value",
            "--flag",
        ]
    )
    require(argv == b"[marker]\n[space value]\n[--flag]\n", "Guest argv not preserved")
    require(
        checked(["docker", "exec", name, "uname", "-m"]).strip() == b"aarch64",
        "Client is not ARM64",
    )
    require(owned(name, owner)["State"]["Pid"] == pid, "Client restarted")
    return {
        **record,
        "running_sha256": running,
        "guest_argv_verified": True,
        "client_host_pid": pid,
        "hardware": "QEMU ARM64 user emulation on hosted x64",
    }


def verify_checkout(root, expected):
    require(
        checked(["git", "rev-parse", "HEAD"]).decode().strip() == os.environ["BUILD_SOURCEVERSION"],
        "Checkout revision mismatch",
    )
    baseline = checked(["git", "show", BASE + ":" + AUTH])
    canonical(baseline, expected["baseline_auth_sha256"])
    require(baseline == (HERE / "baseline.snapshot").read_bytes(), "Runtime baseline drift")
    source_pair(root)
    changed = checked(["git", "diff", "--name-only", BASE, "HEAD"]).decode().splitlines()
    require(set(changed) == set(expected["overlay_paths"]), "Unexpected diagnostic tree diff")
    for path, digest in expected["protected_sources"].items():
        require(sha((root / path).read_bytes()) == digest, "Protected source drift: " + path)
    for path, digest in expected["helper_sha256"].items():
        require(sha((root / path).read_bytes()) == digest, "Diagnostic helper drift: " + path)


def export_public(scratch, secret):
    public = scratch / "public"
    public.mkdir(exist_ok=True)
    hashes = {}
    malformed = []
    for folder in (scratch / "host-logs", scratch / "raw"):
        if not folder.exists():
            continue
        for path in sorted(folder.iterdir()):
            require(path.is_file() and not path.is_symlink(), "Unexpected evidence entry")
            require(path.suffix in (".json", ".jsonl", ".xml", ".log"), "Unexpected artifact type")
            require(path.stat().st_size <= 64 * 1024 * 1024, "Evidence file exceeds bound")
            text = redact(path.read_bytes(), secret)
            require(secret not in text, "Unredacted secret")
            name = folder.name + "-" + path.name
            try:
                if path.suffix == ".json":
                    json.loads(text)
                elif path.suffix == ".jsonl":
                    for line in text.splitlines():
                        json.loads(line)
                elif path.suffix == ".xml":
                    ET.fromstring(text)
            except (ValueError, ET.ParseError) as exc:
                malformed.append({"file": name, "error": str(exc)})
                name += ".incomplete.log"
            (public / name).write_text(text, encoding="utf-8")
            hashes[name] = sha((public / name).read_bytes())
    save(public / "artifacts.json", hashes)
    save(public / "artifact-validation.json", {"accepted": not malformed, "malformed": malformed})
    require(not malformed, "Malformed artifacts retained as sanitized incomplete logs")


def run(distro, owner, scratch):
    root = Path(os.environ["BUILD_SOURCESDIRECTORY"])
    expected = json.loads((HERE / "expected.json").read_text())
    verify_checkout(root, expected)
    leg = expected["images"][distro]
    scratch.mkdir(mode=0o700, exist_ok=False)
    logs = scratch / "host-logs"
    logs.mkdir()
    password = secrets.token_urlsafe(32) + "Aa1!"
    env = clean_env()
    env.update(MSSQL_SA_PASSWORD=password, SQLCMDPASSWORD=password)
    client = "ab48086-" + owner + "-client"
    server = "ab48086-" + owner + "-sql"
    network = "ab48086-" + owner
    started = time.monotonic()
    failures = []
    phases = []

    def command(name, argv, seconds, environment=None):
        remaining = 6600 - (time.monotonic() - started)
        require(remaining > 0, "Overall diagnostic deadline exhausted")
        result, output = bounded(
            argv,
            timeout=min(seconds, remaining),
            env=environment if environment is not None else clean_env(),
        )
        (logs / (name + ".log")).write_bytes(output)
        phases.append({"phase": name, **result})
        save(logs / "phases.json", phases)
        require(not result["timeout"] and result["returncode"] == 0, "Phase failed: " + name)
        return output

    try:
        for suffix in ("client", "sql", "emulator"):
            require(
                owned("ab48086-" + owner + "-" + suffix, owner, absent=True) is None,
                "Refusing existing job resources",
            )
        require(
            owned(network, owner, kind="network", absent=True) is None,
            "Refusing existing job network",
        )
        emulation = emulator(owner, scratch)
        command(
            "network", ["docker", "network", "create", "--label", LABEL + "=" + owner, network], 30
        )
        for name, image, architecture in (
            ("client", leg["image"], "arm64"),
            ("sql", expected["sql_image"], "amd64"),
        ):
            command(
                "pull-" + name,
                ["docker", "pull", "--platform", "linux/" + architecture, image],
                300,
            )
            metadata = json.loads(checked(["docker", "image", "inspect", image]))[0]
            require(metadata["Architecture"] == architecture, "Image architecture mismatch")
            save(
                logs / (name + "-image.json"),
                {
                    "image": image,
                    "id": metadata["Id"],
                    "architecture": metadata["Architecture"],
                    "digests": metadata["RepoDigests"],
                },
            )
        command(
            "create-sql",
            [
                "docker",
                "create",
                "--name",
                server,
                "--label",
                LABEL + "=" + owner,
                "--platform",
                "linux/amd64",
                "--network",
                network,
                "--network-alias",
                "sqlserver",
                "-e",
                "MSSQL_SA_PASSWORD",
                "-e",
                "ACCEPT_EULA=Y",
                "-e",
                "MSSQL_PID=Developer",
                expected["sql_image"],
            ],
            60,
            env,
        )
        command("start-sql", ["docker", "start", server], 30)
        require(owned(server, owner)["State"]["Running"], "SQL container not running")
        sqlcmd = [
            "docker",
            "exec",
            "-e",
            "SQLCMDPASSWORD",
            server,
            "/opt/mssql-tools18/bin/sqlcmd",
            "-S",
            "localhost",
            "-U",
            "sa",
            "-C",
            "-b",
            "-l",
            "5",
            "-t",
            "10",
            "-h",
            "-1",
            "-W",
            "-Q",
        ]
        probes = []
        ready_at = time.monotonic()
        while True:
            result, output = bounded(
                [*sqlcmd, "SET NOCOUNT ON; SELECT 1;"],
                timeout=20,
                env=env,
            )
            probes.append(result)
            save(
                logs / "sql-readiness.json",
                {
                    "probes": probes,
                    "setup_recovery_actions": 0,
                    "meaning": "bounded initial readiness polling, never recreate/restart/retry setup",
                },
            )
            if result["returncode"] == 0 and not result["timeout"]:
                break
            require(time.monotonic() - ready_at < 180, "SQL readiness deadline exhausted")
            time.sleep(2)
        output = command(
            "sql-initialize",
            [
                *sqlcmd,
                "SET NOCOUNT ON; CREATE DATABASE TestDB; "
                "SELECT CONVERT(int, SERVERPROPERTY('ProductMajorVersion'));",
            ],
            30,
            env,
        )
        require(output.decode().strip() == "16", "Owned service is not SQL Server 2022")
        command(
            "create-client",
            [
                "docker",
                "create",
                "--name",
                client,
                "--label",
                LABEL + "=" + owner,
                "--platform",
                "linux/arm64",
                "--network",
                network,
                "--mount",
                "type=bind,src=" + str(root) + ",dst=/workspace",
                "--mount",
                "type=bind,src=" + str(scratch) + ",dst=/evidence",
                "--workdir",
                "/workspace",
                leg["image"],
                "/bin/sleep",
                "infinity",
            ],
            60,
        )
        command("start-client", ["docker", "start", client], 30)
        save(logs / "emulator.json", verify_client(client, owner, emulation))
        command(
            "bootstrap", ["docker", "exec", client, "/bin/sh", REL + "/bootstrap.sh", distro], 2400
        )
        verify_checkout(root, expected)
        save(
            logs / "identity.json",
            {
                "revision": os.environ["BUILD_SOURCEVERSION"],
                "baseline": BASE,
                "owner": owner,
                "leg": distro,
                "source_manifest": expected,
                "job_attempt": os.environ["SYSTEM_JOBATTEMPT"],
            },
        )
        env.update(
            DB_PASSWORD=password,
            DB_CONNECTION_STRING="Server=sqlserver;Database=TestDB;UID=sa;"
            "PWD=" + password + ";Encrypt=yes;TrustServerCertificate=yes",
        )
        command(
            "qualification",
            [
                "docker",
                "exec",
                "-e",
                "DB_CONNECTION_STRING",
                "-e",
                "DB_PASSWORD",
                "-e",
                "PYTHONDONTWRITEBYTECODE=1",
                client,
                "/opt/venv/bin/python",
                REL + "/client.py",
                leg["python"],
            ],
            6000,
            env,
        )
        verify_checkout(root, expected)
    except (RuntimeError, OSError, ValueError, KeyError) as exc:
        failures.append(str(exc))
    finally:
        cleanup_result = cleanup(owner)
        save(logs / "cleanup.json", cleanup_result)
        if not cleanup_result["accepted"]:
            failures.append("Owned cleanup failed")
        save(
            logs / "outcome.json",
            {
                "accepted": not failures,
                "failures": failures,
                "elapsed_seconds": time.monotonic() - started,
            },
        )
        export_public(scratch, password)
    require(not failures, "Hosted diagnostic failed; sanitized evidence retained")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("run", "cleanup"))
    parser.add_argument("distro", choices=("ubuntu", "debian", "alpine"))
    args = parser.parse_args()
    owner = admission()
    scratch = Path(os.environ["AGENT_TEMPDIRECTORY"]) / ("ab48086-" + args.distro)
    if args.action == "cleanup":
        result = cleanup(owner)
        public = scratch / "public"
        public.mkdir(parents=True, exist_ok=True)
        save(public / "final-cleanup.json", result)
        require(result["accepted"], "Final cleanup failed")
    else:
        run(args.distro, owner, scratch)


if __name__ == "__main__":
    main()
