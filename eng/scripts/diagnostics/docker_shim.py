#!/usr/bin/env python3
"""Hosted experiment only: delegate Docker and kill only the labelled test SQL container."""
import json
import os
from pathlib import Path
import subprocess
import sys


def record(event):
    with Path(os.environ["RECOVERY_EVENTS"]).open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(event) + "\n")


def main():
    args = sys.argv[1:]
    password = os.environ.get("SQLCMDPASSWORD", "")
    if not password or os.environ.get("MSSQL_SA_PASSWORD") != password:
        record({"event": "inconsistent-effective-secret"})
        return 92
    if password in " ".join(args):
        record({"event": "secret-on-argv"})
        return 90
    offset = 2 if args[:1] == ["--context"] else 0
    operation = args[offset]
    identifier = args[-1] if operation in ("start", "rm", "logs") else None
    if operation == "exec":
        rest = args[offset + 1:]
        if rest[:2] != ["--env", "SQLCMDPASSWORD"] or len(rest) < 4:
            record({"event": "unexpected-exec-shape"})
            return 93
        identifier = rest[2]
    record({"event": "before", "operation": operation, "id": identifier})
    real = os.environ["REAL_DOCKER"]
    result = subprocess.run([real] + args)
    record({"event": "after", "operation": operation, "id": identifier, "code": result.returncode})
    if operation != "start" or result.returncode:
        return result.returncode
    events = [
        json.loads(line)
        for line in Path(os.environ["RECOVERY_EVENTS"]).read_text(encoding="utf-8").splitlines()
    ]
    starts = sum(event.get("operation") == "start" and event["event"] == "before" for event in events)
    mode = os.environ["RECOVERY_MODE"]
    if mode == "clean" or (mode == "recover" and starts != 1):
        return 0
    context = args[:offset]
    owner = subprocess.run(
        [real] + context + [
            "inspect", "--format",
            '{{index .Config.Labels "com.microsoft.mssql-python.ci-owner"}}',
            identifier,
        ],
        capture_output=True, text=True, timeout=15,
    )
    if owner.returncode or owner.stdout.strip() != os.environ["RECOVERY_OWNER"]:
        record({"event": "injection-refused", "id": identifier})
        return 91
    killed = subprocess.run(
        [real] + context + ["kill", "--signal", "KILL", identifier],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=15,
    )
    record({"event": "injected-exit", "id": identifier, "code": killed.returncode})
    return killed.returncode


if __name__ == "__main__":
    sys.exit(main())
