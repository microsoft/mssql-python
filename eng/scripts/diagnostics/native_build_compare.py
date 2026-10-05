"""Diagnostics only: four planned clean builds, with the final build parallel."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import resource
import signal
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
ORDER = (2, 1, 1, 2)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    if (output / "comparison.json").exists():
        raise RuntimeError("Refusing to overwrite an earlier comparison")
    result = {
        "diagnostics_only": True,
        "order": ORDER,
        "per_build_timeout_seconds": 900,
        "platform": platform.platform(),
        "python": sys.version,
        "cpu_count": os.cpu_count(),
        "source": subprocess.check_output(
            ["git", "-c", f"safe.directory={ROOT}", "rev-parse", "HEAD"], cwd=ROOT, text=True
        ).strip(),
        "build_script_sha256": hashlib.sha256(
            (ROOT / "mssql_python/pybind/build.sh").read_bytes()
        ).hexdigest(),
        "samples": [],
        "status": "running",
    }

    def save():
        (output / "comparison.json").write_text(json.dumps(result, indent=2), encoding="utf-8")

    save()
    for index, workers in enumerate(ORDER, 1):
        print(f"Planned clean build {index}/4: CMAKE_BUILD_PARALLEL_LEVEL={workers}", flush=True)
        env = dict(os.environ, CMAKE_BUILD_PARALLEL_LEVEL=str(workers))
        log_path = output / f"build-{index}-j{workers}.log"
        usage_before = resource.getrusage(resource.RUSAGE_CHILDREN)
        started = time.perf_counter()
        timed_out = False
        with log_path.open("w", encoding="utf-8") as log:
            process = subprocess.Popen(
                ["bash", "build.sh"], cwd=ROOT / "mssql_python/pybind",
                env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True,
            )
            try:
                code = process.wait(timeout=900)
            except subprocess.TimeoutExpired:
                timed_out = True
                os.killpg(process.pid, signal.SIGKILL)
                code = process.wait(timeout=15)
        elapsed = time.perf_counter() - started
        usage_after = resource.getrusage(resource.RUSAGE_CHILDREN)
        sample = {
            "index": index, "workers": workers, "elapsed_seconds": elapsed,
            "returncode": code, "timed_out": timed_out,
            "user_cpu_seconds": usage_after.ru_utime - usage_before.ru_utime,
            "system_cpu_seconds": usage_after.ru_stime - usage_before.ru_stime,
            "log_sha256": hashlib.sha256(log_path.read_bytes()).hexdigest(),
        }
        result["samples"].append(sample)
        if code or timed_out:
            result["status"] = "failed"
            save()
            print(f"Build {index} failed: exit={code}, timeout={timed_out}; no retries.", flush=True)
            print(log_path.read_text(encoding="utf-8", errors="replace")[-12000:], flush=True)
            return 1
        extension, = (ROOT / "mssql_python").glob("ddbc_bindings*.so")
        sample["extension_sha256"] = hashlib.sha256(extension.read_bytes()).hexdigest()
        if sys.platform == "darwin":
            slices = subprocess.check_output(["lipo", "-archs", str(extension)], text=True).split()
            sample["macos_slices"] = slices
            if set(slices) != {"x86_64", "arm64"}:
                result["status"] = "failed"
                save()
                raise RuntimeError(f"Missing universal2 architecture in build {index}")
        save()
        print(f"Build {index} completed in {elapsed:.3f}s", flush=True)
    result["status"] = "completed"
    save()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
