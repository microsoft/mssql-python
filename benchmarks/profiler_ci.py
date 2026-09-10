"""Build and measure base/candidate in isolated directories on the same CI agent."""

import argparse
import contextlib
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import sys
import tarfile
import tempfile

ROOT = Path(__file__).resolve().parents[1]
SHA = re.compile(r"[0-9a-f]{40}")
LEGS = ("Windows-SQL2022", "Windows-SQL2025", "macOS-SQL2022", "macOS-SQL2025", "Linux-SQL2022")


def git(*args):
    return subprocess.check_output(["git", "-C", str(ROOT), *args], text=True).strip()


def resolve_revisions(base, candidate):
    candidate = git("rev-parse", "--verify", "--end-of-options", f"{candidate}^{{commit}}")
    # ADO validates refs/pull/N/merge. Its first parent is the exact target snapshot,
    # not whichever main build happened to finish most recently.
    base = git(
        "rev-parse", "--verify", "--end-of-options", f"{base or candidate + '^1'}^{{commit}}"
    )
    return base, candidate


def checkout(revision, path):
    with tempfile.TemporaryFile() as archive:
        subprocess.run(["git", "-C", str(ROOT), "archive", revision], stdout=archive, check=True)
        archive.seek(0)
        with tarfile.open(fileobj=archive) as tar:
            tar.extractall(path, filter="data")


def build(path, log):
    env = dict(os.environ, ENABLE_PROFILING="1")
    # build scripts find Python via PATH; keep the controller's interpreter.
    env["PATH"] = str(Path(sys.executable).parent) + os.pathsep + env["PATH"]
    command = ["cmd", "/c", "build.bat"] if os.name == "nt" else ["bash", "build.sh"]
    with log.open("w", encoding="utf-8") as output:
        subprocess.run(
            command,
            cwd=path / "mssql_python/pybind",
            env=env,
            stdout=output,
            stderr=subprocess.STDOUT,
            timeout=900,
            check=True,
        )


def check_build(source_root, profiling):
    sys.path.insert(0, str(source_root))
    import mssql_python
    import mssql_python_odbc
    from mssql_python import ddbc_bindings, perf_timer

    for module in (mssql_python, ddbc_bindings, mssql_python_odbc):
        if not Path(module.__file__).resolve().is_relative_to(source_root.resolve()):
            raise RuntimeError("Imported driver outside the selected checkout")
    if hasattr(ddbc_bindings, "profiling") != profiling:
        raise RuntimeError("Native profiling build configuration mismatch")
    if perf_timer.is_enabled() or (profiling and ddbc_bindings.profiling.is_enabled()):
        raise RuntimeError("Profiling must default to recording OFF")


def load_suite():
    # Load only the common profiler package by path. Keep the revision checkout
    # first on sys.path so lazy provider imports cannot select candidate binaries.
    spec = importlib.util.spec_from_file_location(
        "profiler",
        ROOT / "profiler/__init__.py",
        submodule_search_locations=[str(ROOT / "profiler")],
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules["profiler"] = module
    spec.loader.exec_module(module)
    import profiler.core as core
    import profiler_workloads as workloads

    return core, workloads


def worker(args):
    # Import the chosen driver FIRST, then the SAME workload/controller for both
    # revisions. Never mix two native extensions into one interpreter.
    check_build(args.source_root, profiling=True)
    core, workloads = load_suite()

    cases = workloads.registry()
    chosen = args.scenarios or list(cases)
    if set(chosen) - set(cases):
        raise ValueError("Unknown benchmark scenario")
    core.SCENARIOS = cases
    # The runner owns enable/disable/cleanup, just as in the documented CLI.
    with core.Profiler() as profiler:
        with contextlib.redirect_stdout(io.StringIO()):
            results = profiler.run(*chosen)
        profiler._ensure_connection()
        with profiler._conn.cursor() as cursor:
            cursor.execute("SELECT CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(80))")
            sql_version = cursor.fetchone()[0]
        environment = dict(
            os=platform.system(),
            architecture=platform.machine().lower(),
            python=platform.python_version(),
            sql_version=sql_version,
        )
        output = {}
        for name, result in zip(chosen, results):
            if result["cpp"] is None or result["py"] is None:
                raise RuntimeError(f"Scenario {name} was skipped")
            if not result["cpp"]:
                raise RuntimeError(f"Scenario {name} has no native samples")
            # Only generated workload counts and instrumentation, never query data.
            output[name] = {key: result[key] for key in ("wall_ms", "cpp", "py")}
            output[name]["work"] = result.get("detail", "Connection: 1").split(" (")[0]
        args.output.write_text(
            json.dumps(dict(environment=environment, scenarios=output), allow_nan=False),
            encoding="utf-8",
        )


def measure(path, output, scenarios):
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--worker",
        "--source-root",
        str(path),
        "--output",
        str(output),
    ]
    if scenarios:
        command += ["--scenarios", *scenarios]
    with output.with_suffix(".log").open("w", encoding="utf-8") as log:
        subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, timeout=240, check=True)
    return json.loads(output.read_text(encoding="utf-8"))


def run(args):
    base, candidate = resolve_revisions(args.base, args.candidate)
    args.output.mkdir(parents=True, exist_ok=True)
    report_path = args.output / "report.json"
    report_path.unlink(missing_ok=True)
    suite = hashlib.sha256()
    for file in [
        Path(__file__),
        ROOT / "benchmarks/profiler_workloads.py",
        *sorted((ROOT / "profiler").glob("*.py")),
    ]:
        suite.update(file.name.encode())
        suite.update(file.read_bytes().replace(b"\r\n", b"\n"))
    head = os.environ.get("SYSTEM_PULLREQUEST_SOURCECOMMITID", candidate)
    if not SHA.fullmatch(head):
        head = candidate
    report = dict(
        schema_version=1,
        status="incomplete",
        leg=args.leg,
        base_commit=base,
        source_commit=candidate,
        head_commit=head,
        build_id=int(os.environ.get("BUILD_BUILDID", "0")),
        suite_hash=suite.hexdigest(),
        samples=args.samples,
        warmups=args.warmups,
        pairs=[],
    )
    report_path.write_text(json.dumps(report), encoding="utf-8")
    # CI reuses the profiling build already exercised by pytest. The base always
    # has its own checkout and process. Local runs can build both sides instead.
    with tempfile.TemporaryDirectory(prefix="profiler-ci-") as directory:
        paths = {side: Path(directory) / side for side in ("base", "candidate")}
        for side, revision in (("base", base), ("candidate", candidate)):
            if side == "candidate" and args.reuse_candidate:
                if candidate != git("rev-parse", "HEAD"):
                    raise ValueError("--reuse-candidate requires candidate to be checkout HEAD")
                paths[side] = ROOT
                subprocess.run(
                    [sys.executable, str(Path(__file__).resolve()), "--check-build", "on"],
                    check=True,
                    timeout=60,
                )
                continue
            checkout(revision, paths[side])
            print(f"Building profiling {side}: {revision}", flush=True)
            build(paths[side], args.output / f"build-{side}.log")
        for sample in range(args.warmups + args.samples):
            pair = {}
            order = ("base", "candidate") if sample % 2 == 0 else ("candidate", "base")
            for side in order:
                print(f"Measuring pair {sample + 1}: {side}", flush=True)
                pair[side] = measure(
                    paths[side],
                    args.output / f"{side}-{sample}.json",
                    args.scenarios,
                )
            if pair["base"]["environment"] != pair["candidate"]["environment"]:
                raise RuntimeError("Base and candidate environments differ")
            if sample >= args.warmups:
                report["pairs"].append(pair)
                report_path.write_text(json.dumps(report, allow_nan=False), encoding="utf-8")
    report["status"] = "complete"
    report_path.write_text(json.dumps(report, allow_nan=False), encoding="utf-8")
    print(f"Paired profiler report: {report_path}", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", help="Exact base revision; defaults to candidate first parent")
    parser.add_argument("--candidate", default="HEAD")
    parser.add_argument("--leg", choices=LEGS)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--samples", type=int, default=5)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--scenarios", nargs="+", help="Local subset; CI runs the full registry")
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--source-root", type=Path, help=argparse.SUPPRESS)
    parser.add_argument(
        "--reuse-candidate",
        action="store_true",
        help="Reuse checkout HEAD's profiling build after pytest",
    )
    parser.add_argument(
        "--check-build",
        choices=("on", "off"),
        help="Verify native compile configuration and recording OFF, then exit",
    )
    args = parser.parse_args()
    if args.check_build:
        check_build(ROOT, profiling=args.check_build == "on")
    elif args.worker:
        if args.source_root is None or args.output is None:
            parser.error("--worker requires --source-root and --output")
        worker(args)
    else:
        if (
            not args.output
            or not args.leg
            or not 3 <= args.samples <= 15
            or not 1 <= args.warmups <= 3
        ):
            parser.error("Choose a leg, 3-15 measured pairs and 1-3 warmup pairs")
        run(args)


if __name__ == "__main__":
    main()
