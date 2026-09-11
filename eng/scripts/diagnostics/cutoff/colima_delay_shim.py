#!/usr/bin/env python3
"""Private controlled timing proof, not a natural Colima boot measurement."""

import json
import os
from pathlib import Path
import subprocess
import sys
import time

MINIMUM_SECONDS = 610


def record(event):
    with Path(os.environ["CUTOFF_COLIMA_EVENTS"]).open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(event) + "\n")


def main(argv=None):
    args = sys.argv[1:] if argv is None else argv
    real = os.environ["REAL_COLIMA"]
    if Path(real).resolve() == Path(__file__).resolve():
        raise RuntimeError("Real Colima must not resolve to the delay shim")
    if args[:1] != ["start"]:
        os.execv(real, [real] + args)
        raise RuntimeError("Colima delegation unexpectedly returned")
    mode = os.environ["RECOVERY_MODE"]
    started = time.monotonic()
    record({"event": "start", "case": mode})
    # Inherit the helper's regular-file output; never wait for daemon pipe EOF.
    result = subprocess.run([real] + args)
    real_seconds = time.monotonic() - started
    record(
        {
            "event": "real-complete",
            "case": mode,
            "returncode": result.returncode,
            "real_seconds": real_seconds,
        }
    )
    if result.returncode != 0:
        return result.returncode if result.returncode > 0 else 128 - result.returncode
    if mode != "clean":
        return 0
    try:
        descriptor = os.open(
            os.environ["CUTOFF_COLIMA_MARKER"], os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600
        )
    except FileExistsError:
        return 0
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        stream.write("First clean success delay claimed\n")
    injected = max(0, MINIMUM_SECONDS - real_seconds)
    if injected:
        time.sleep(injected)
    elapsed = time.monotonic() - started
    if elapsed < MINIMUM_SECONDS:
        raise RuntimeError("Controlled Colima delay ended before its required minimum")
    record(
        {
            "event": "delayed-success",
            "case": mode,
            "real_returncode": result.returncode,
            "real_seconds": real_seconds,
            "wrapper_seconds": elapsed,
            "injected_delay_seconds": injected,
            "minimum_seconds": MINIMUM_SECONDS,
        }
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
