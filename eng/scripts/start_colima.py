#!/usr/bin/env python3
"""Start a verified Colima Docker daemon, with one bounded clean retry on CI."""

import os
from pathlib import Path
import signal
import subprocess
import time


def run(command, timeout):
    print(f"+ {' '.join(command)} (timeout={timeout}s)", flush=True)
    with subprocess.Popen(command, start_new_session=True) as process:
        try:
            return process.wait(timeout=timeout) == 0
        except subprocess.TimeoutExpired:
            # Kill the command's process group, not unrelated agent processes.
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
            return False


def diagnostics():
    run(["colima", "status"], 15)
    root = Path.home() / ".colima" / "_lima" / "colima"
    for path in sorted(root.glob("*.log")):
        print(f"=== {path.name} (last 16 KiB) ===", flush=True)
        with path.open("rb") as stream:
            stream.seek(max(0, path.stat().st_size - 16384))
            print(stream.read().decode("utf-8", errors="replace"), flush=True)


def start():
    for command in (["sw_vers"], ["uname", "-m"], ["colima", "version"], ["docker", "--version"]):
        if not run(command, 15):
            raise RuntimeError(f"Tool/runner diagnostic failed: {command[0]}")
    for attempt in range(2):
        print(f"Colima startup attempt {attempt + 1}/2", flush=True)
        if run(["colima", "start", "--cpu", "4", "--memory", "8", "--disk", "50"], 600):
            if run(["docker", "context", "use", "colima"], 30):
                for _ in range(12):
                    if run(["docker", "--context", "colima", "info"], 10):
                        return
                    time.sleep(5)
        diagnostics()
        if attempt == 0:
            # Only the disposable default profile owned by this hosted CI job.
            if not run(["colima", "stop", "--force"], 60):
                raise RuntimeError("Colima cleanup failed; refusing an unsafe retry")
            if not run(["colima", "delete", "--force"], 60):
                raise RuntimeError("Colima profile deletion failed; refusing an unsafe retry")
    raise RuntimeError("Colima Docker daemon did not become ready after two attempts")


if __name__ == "__main__":
    start()
