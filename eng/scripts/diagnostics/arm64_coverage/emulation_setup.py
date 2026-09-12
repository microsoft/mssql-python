"""Hosted-only registration and running-process proof for the pinned emulator."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import uuid

import hosted

REGISTRATION = Path("/proc/sys/fs/binfmt_misc/qemu-aarch64")


def context(owner):
    hosted.admission()
    if platform.system() != "Linux" or platform.machine().lower() not in ("x86_64", "amd64"):
        raise RuntimeError("Emulator registration requires the hosted native x64 Linux agent")
    root, scratch, public = hosted.paths()
    identity = json.loads((scratch / "identity.json").read_text())
    if identity["owner"] != owner or identity["revision"] != os.environ["BUILD_SOURCEVERSION"]:
        raise RuntimeError("Hosted admission identity changed")
    expected = json.loads((root / hosted.RELATIVE / "expected.json").read_text())["hosted_qemu"]
    return scratch, expected


def registration(text):
    lines = text.strip().splitlines()
    if not lines or lines[0] != "enabled":
        raise RuntimeError("ARM64 binfmt handler is not enabled")
    fields = {}
    for line in lines[1:]:
        name, value = line.split(" ", 1)
        fields[name.rstrip(":")] = value.strip()
    if (
        not {"F", "P"}.issubset(set(fields["flags"]))
        or Path(fields["interpreter"]).name != "qemu-aarch64"
        or fields["offset"] != "0"
    ):
        raise RuntimeError("Unexpected ARM64 binfmt interpreter or F/P flags")
    return fields


def owned_tool(owner, image, arguments, *, export=None):
    name = "task47285-emulator-" + uuid.uuid4().hex
    failure = None
    cleanup_error = None
    output = b""
    try:
        command = [
            "docker",
            "create",
            "--name",
            name,
            "--label",
            hosted.LABEL + "=" + owner,
            "--platform",
            "linux/amd64",
        ]
        if export is None:
            command.append("--privileged")
        else:
            command += ["--entrypoint", "/usr/bin/qemu-aarch64"]
        identifier = hosted.checked(command + [image, *arguments]).decode().strip()
        if export is not None:
            hosted.checked(["docker", "cp", identifier + ":/usr/bin/qemu-aarch64", str(export)])
        else:
            output = hosted.checked(["docker", "start", "-a", identifier], timeout=120)
            state = hosted.inspect_owned(identifier, owner)["State"]
            if state["Running"] or state["ExitCode"] != 0:
                raise RuntimeError("The binfmt installer did not exit successfully")
    except (RuntimeError, OSError, ValueError, KeyError, subprocess.TimeoutExpired) as exc:
        failure = str(exc)
    finally:
        try:
            container = hosted.inspect_owned(name, owner, absent_ok=True)
            if container:
                hosted.checked(
                    ["docker", "container", "rm", "-f", "-v", container["Id"]], timeout=35
                )
                if hosted.inspect_owned(container["Id"], owner, absent_ok=True) is not None:
                    raise RuntimeError("Owned emulator-tool container remains after cleanup")
        except (RuntimeError, OSError, ValueError, KeyError, subprocess.TimeoutExpired) as exc:
            cleanup_error = str(exc)
    if failure or cleanup_error:
        raise RuntimeError(f"Emulator tool failure={failure}; cleanup failure={cleanup_error}")
    return output


def setup(args):
    scratch, expected = context(args.owner)
    binary = scratch / "qemu-aarch64"
    owned_tool(args.owner, expected["image"], ["--version"], export=binary)
    actual = hashlib.sha256(binary.read_bytes()).hexdigest()
    if actual != expected["binary_sha256"]:
        raise RuntimeError("Downloaded emulator does not match the locally qualified binary")
    binary.chmod(0o500)
    version = hosted.checked([str(binary), "--version"]).decode().splitlines()[0]
    if version != expected["version"]:
        raise RuntimeError("Pinned emulator version mismatch")
    if REGISTRATION.exists():
        owned_tool(args.owner, expected["image"], ["--uninstall", "qemu-aarch64"])
    owned_tool(args.owner, expected["image"], ["--install", "arm64"])
    record = {
        "image": expected["image"],
        "binary_sha256": actual,
        "version": version,
        "registration": registration(REGISTRATION.read_text()),
        "host_machine": platform.machine(),
        "host_kernel": platform.release(),
        "owner": args.owner,
        "running_client_verified": False,
    }
    (scratch / "emulator.json").write_text(json.dumps(record, indent=2))
    print(json.dumps(record))
    return record


def verify_client(args):
    scratch, expected = context(args.owner)
    record = json.loads((scratch / "emulator.json").read_text())
    if (
        record["owner"] != args.owner
        or record["image"] != expected["image"]
        or record["binary_sha256"] != expected["binary_sha256"]
        or record["version"] != expected["version"]
    ):
        raise RuntimeError("Emulator setup identity changed")
    handler = registration(REGISTRATION.read_text())
    if handler != record["registration"]:
        raise RuntimeError("ARM64 binfmt registration changed")
    container = hosted.inspect_owned(args.client, args.owner)
    pid = container["State"]["Pid"]
    if not container["State"]["Running"] or type(pid) is not int or pid <= 0:
        raise RuntimeError("Owned ARM64 client is not running")
    actual = hosted.checked(["sudo", "-n", "sha256sum", f"/proc/{pid}/exe"]).decode().split()[0]
    if actual != expected["binary_sha256"]:
        raise RuntimeError("Running ARM64 client is using a different emulator")
    argv = [
        "docker",
        "exec",
        container["Id"],
        "/bin/sh",
        "-c",
        'printf "[%s]\\n" "$0" "$@"',
        "marker",
        "space value",
        "--flag",
    ]
    if hosted.checked(argv).decode() != "[marker]\n[space value]\n[--flag]\n":
        raise RuntimeError("Actual binfmt execution did not preserve guest argv")
    machine = hosted.checked(["docker", "exec", container["Id"], "uname", "-m"]).decode().strip()
    if machine != "aarch64":
        raise RuntimeError("Client execution is not ARM64")
    after = hosted.inspect_owned(container["Id"], args.owner)["State"]
    if not after["Running"] or after["Pid"] != pid:
        raise RuntimeError("Client identity changed during emulator verification")
    record.update(
        running_client_verified=True,
        running_emulator_sha256=actual,
        client_machine=machine,
        guest_argv_verified=True,
    )
    (scratch / "emulator.json").write_text(json.dumps(record, indent=2))
    print(json.dumps(record))
    return record


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("setup", "verify"))
    parser.add_argument("--owner", required=True)
    parser.add_argument("--client")
    args = parser.parse_args()
    if args.action == "verify" and not args.client:
        parser.error("verify requires --client")
    try:
        (setup if args.action == "setup" else verify_client)(args)
    except (RuntimeError, OSError, ValueError, KeyError, subprocess.TimeoutExpired) as exc:
        print(f"Emulator qualification failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
