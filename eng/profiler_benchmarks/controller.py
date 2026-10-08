"""Build and measure base/candidate in isolated directories on the same CI agent."""

import argparse
import ast
import contextlib
import faulthandler
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path, PurePosixPath
import platform
import re
import signal
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
import textwrap

from .report import (
    LEGS,
    MODES,
    validate,
    validate_ci_mode,
    validate_ci_header,
    ci_mode_reports,
    validate_row_route,
    expected_constructors,
    validate_python_sources,
    validate_samples,
)
from . import workloads

ROOT = Path(__file__).resolve().parents[2]
SHA = re.compile(r"[0-9a-f]{40}")
# Twelve six-minute passes plus a 15-minute base build and preflight need 88
# minutes. Local runs build both revisions and receive another 15 minutes.
BENCHMARK_TIMEOUT = 90 * 60
LOCAL_BENCHMARK_TIMEOUT = 105 * 60
WORKER_TIMEOUT = 6 * 60
WINDOWS = os.name == "nt"
FETCH_WORKER_TIMEOUT = 30
CI_FINISH_RESERVE = 180


class ProcessCleanupError(RuntimeError):
    """Further work is unsafe until the previous process tree is reaped."""


def git(*args, timeout=None):
    return subprocess.check_output(
        ["git", "-C", str(ROOT), *args], text=True, timeout=timeout
    ).strip()


def resolve_revisions(base, candidate, timeout=None):
    options = {"timeout": timeout} if timeout is not None else {}
    candidate = git(
        "rev-parse", "--verify", "--end-of-options", f"{candidate}^{{commit}}", **options
    )
    # ADO validates refs/pull/N/merge. Its first parent is the exact target snapshot,
    # not whichever main build happened to finish most recently.
    base = git(
        "rev-parse",
        "--verify",
        "--end-of-options",
        f"{base or candidate + '^1'}^{{commit}}",
        **options,
    )
    return base, candidate


def checkout(revision, path):
    with tempfile.TemporaryFile() as archive:
        subprocess.run(["git", "-C", str(ROOT), "archive", revision], stdout=archive, check=True)
        archive.seek(0)
        with tarfile.open(fileobj=archive) as tar:
            if sys.version_info >= (3, 12):
                tar.extractall(path, filter="data")
                return
            members = tar.getmembers()
            for member in members:
                member_path = PurePosixPath(member.name)
                if (
                    not member.name
                    or member_path.is_absolute()
                    or ".." in member_path.parts
                    or "\\" in member.name
                    or re.match(r"^[A-Za-z]:", member.name)
                    or not (member.isfile() or member.isdir())
                ):
                    raise ValueError("Unsafe git archive member")
            tar.extractall(path, members=members)


def terminate_process_tree(process):
    if WINDOWS:
        result = subprocess.run(
            ["taskkill", "/PID", str(process.pid), "/T", "/F"],
            capture_output=True,
            text=True,
        )
        if result.returncode:
            if process.poll() is not None:
                return
            process.kill()
            process.wait()
            raise RuntimeError(f"Failed to terminate build process tree: {result.stdout.strip()}")
        process.wait(timeout=5)
        return

    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    process.wait(timeout=5)


def build(path, log, timeout=900, profiling=True):
    env = dict(os.environ, ENABLE_PROFILING="1" if profiling else "0")
    # build scripts find Python via PATH; keep the controller's interpreter.
    env["PATH"] = str(Path(sys.executable).parent) + os.pathsep + env["PATH"]
    command = ["cmd", "/c", "build.bat"] if os.name == "nt" else ["bash", "build.sh"]
    run_process(command, log, timeout, cwd=path / "mssql_python/pybind", env=env)


def run_process(command, log, timeout, **options):
    with log.open("w", encoding="utf-8") as output:
        process = subprocess.Popen(
            command,
            **options,
            stdout=output,
            stderr=subprocess.STDOUT,
            start_new_session=not WINDOWS,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if WINDOWS else 0,
        )
        try:
            returncode = process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            try:
                terminate_process_tree(process)
            except (OSError, RuntimeError, subprocess.TimeoutExpired) as error:
                raise ProcessCleanupError("Could not reap the timed-out process tree") from error
            raise
        if returncode:
            raise subprocess.CalledProcessError(returncode, command)


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

    return core, workloads


class _FetchContext:
    """Controller-only recording policy; the interactive Profiler stays unchanged."""

    def __init__(self, mode, native, python):
        if mode not in ("latency", "route") or (native is not None) != (mode == "route"):
            raise ValueError("Invalid fetch recording configuration")
        self.native, self.python = native, python

    def disable(self):
        self.python.disable()
        self.python.disable_timeline()
        if self.native is not None:
            self.native.disable()
            self.native.disable_timeline()

    def enable(self):
        self.disable()
        self.python.reset()
        if self.native is not None:
            self.native.reset()
            self.native.enable()

    def collect(self):
        if self.python.is_enabled():
            raise RuntimeError("Python phases became enabled during a fetch measurement")
        self.disable()
        py = self.python.get_stats()
        if py:
            raise RuntimeError("Python phase samples contaminate the fetch route")
        return self.native.get_stats() if self.native is not None else {}, py


def verify_reused_source(source_root, revision):
    if source_root.resolve() == ROOT.resolve():
        if git("rev-parse", "HEAD", timeout=5) != revision:
            raise ValueError("Reused checkout revision changed")
        git(
            "diff",
            "--exit-code",
            "--quiet",
            revision,
            "--",
            "mssql_python",
            "mssql_python_odbc",
            timeout=5,
        )


# Closed, reviewed dispatch bodies: main666, f539, many-only, and plain completion.
# Method/comment/docstring edits require review and an explicit fingerprint update.
# These reviewed method-source SHA256 digests are not credentials.
_LEGACY_PYTHON_ROUTE = (
    "52681c15f92e85c01e0b43a9bd763872051fdc956701046534a5beb2467e6c3c"  # DevSkim: ignore DS173237
)
_LEGACY_FUSED_ROUTE = (
    "d4c6671b89ace027c22585e761c4f4c3243e297be2162213d9bbb38d5bed03c6"  # DevSkim: ignore DS173237
)
_MANY_ONLY_ROUTE = (
    "544d5b0ccbf9df1f0b6ec72c79ecbfa49a3517cab0437a5520940b8941966759"  # DevSkim: ignore DS173237
)
_PLAIN_COMPLETION_ROUTE = (
    "776ebc4532b69fe2492317ef6af9d4b2bb08e8a0f112613a7ce2600500bc37d8"  # DevSkim: ignore DS173237
)


def python_source_identity(source_root, revision):
    """Read a selected root's descriptive policy, never infer it from measurements."""
    if not SHA.fullmatch(revision):
        raise ValueError("Invalid Python source revision")
    source = (source_root / "mssql_python" / "cursor.py").read_text(encoding="utf-8")
    try:
        tree = ast.parse(source)
    except SyntaxError as error:
        raise ValueError("Invalid Python route source syntax") from error
    name = "_DEFAULT_NATIVE_ROW_ROUTE"
    mentions = [node for node in ast.walk(tree) if isinstance(node, ast.Name) and node.id == name]
    declarations = [
        node
        for node in tree.body
        if isinstance(node, ast.Assign)
        and len(node.targets) == 1
        and isinstance(node.targets[0], ast.Name)
        and node.targets[0].id == name
    ]
    if mentions and (len(mentions) != 1 or len(declarations) != 1):
        raise ValueError("Malformed or duplicate Python route declaration")
    classes = [
        node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "Cursor"
    ]
    if len(classes) != 1 or classes[0].decorator_list:
        raise ValueError("Missing, duplicate or decorated Cursor class")
    methods = []
    for method in ("fetchone", "fetchmany", "fetchval"):
        nodes = [
            node
            for node in classes[0].body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == method
        ]
        if len(nodes) != 1:
            raise ValueError("Missing or duplicate fetch method")
        if nodes[0].decorator_list or isinstance(nodes[0], ast.AsyncFunctionDef):
            raise ValueError("Decorated or asynchronous fetch method is not a reviewed route")
        methods.append(textwrap.dedent(ast.get_source_segment(source, nodes[0])))
    # Check class-namespace bindings, not locals in methods or nested scopes.
    route_names = {"fetchone", "fetchmany", "fetchval"}
    pending = list(classes[0].body)
    while pending:
        node = pending.pop()
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            if node.name in route_names and (
                isinstance(node, ast.ClassDef) or node not in classes[0].body
            ):
                raise ValueError("Rebound Python fetch method")
            pending.extend(node.decorator_list)
            if isinstance(node, ast.ClassDef):
                pending.extend(node.bases + node.keywords)
            else:
                pending.append(node.args)
                if node.returns is not None:
                    pending.append(node.returns)
            continue
        if isinstance(node, ast.Lambda):
            pending.append(node.args)
            continue
        if isinstance(node, (ast.ListComp, ast.SetComp, ast.DictComp, ast.GeneratorExp)):
            continue
        if (
            isinstance(node, ast.Name)
            and isinstance(node.ctx, (ast.Store, ast.Del))
            and node.id in route_names
            or isinstance(node, ast.alias)
            and (node.asname or node.name.split(".")[0]) in route_names
            or isinstance(node, (ast.ExceptHandler, ast.MatchAs, ast.MatchStar))
            and node.name in route_names
            or isinstance(node, ast.MatchMapping)
            and node.rest in route_names
        ):
            raise ValueError("Rebound Python fetch method")
        pending.extend(ast.iter_child_nodes(node))
    fingerprint = hashlib.sha256(
        json.dumps(methods, ensure_ascii=True, separators=(",", ":")).encode()
    ).hexdigest()
    if fingerprint in (_LEGACY_PYTHON_ROUTE, _PLAIN_COMPLETION_ROUTE):
        values = (False, False, False)
    elif fingerprint == _LEGACY_FUSED_ROUTE:
        values = (True, True, True)
    elif fingerprint == _MANY_ONLY_ROUTE:
        values = (False, True, False)
    else:
        raise ValueError("Unknown Python fetch dispatch fingerprint")
    expected = dict(version=1, methods=dict(zip(("fetchone", "fetchmany", "fetchval"), values)))
    if declarations:
        expression = declarations[0].value
        # literal_eval alone silently accepts duplicate dictionary keys.
        for node in ast.walk(expression):
            if isinstance(node, ast.Dict):
                keys = [ast.literal_eval(key) for key in node.keys]
                if any(type(key) is not str for key in keys) or len(set(keys)) != len(keys):
                    raise ValueError("Duplicate or invalid route declaration key")
        route = ast.literal_eval(expression)
        validate_row_route(route)
        if route != expected:
            raise ValueError("Python source-policy drift")
    elif fingerprint in (_MANY_ONLY_ROUTE, _PLAIN_COMPLETION_ROUTE):
        raise ValueError("Missing Python route declaration")
    return dict(
        source_commit=revision,
        python_cursor_sha256=hashlib.sha256(source.encode()).hexdigest(),
        row_route=expected,
    )


def native_identity(source_root, revision, profiling):
    from mssql_python import ddbc_bindings
    import mssql_python.cursor as cursor_module

    if (
        Path(cursor_module.__file__).resolve()
        != (source_root / "mssql_python" / "cursor.py").resolve()
    ):
        raise ValueError("Python cursor is outside the selected checkout")
    python_identity = python_source_identity(source_root, revision)
    guarded = hasattr(ddbc_bindings, "DDBCSQLFetchRow")
    validate_row_route(python_identity["row_route"], guarded)
    native_file = Path(ddbc_bindings.module.__file__).resolve()
    if not native_file.is_relative_to(source_root.resolve()):
        raise RuntimeError("Native binary is outside the selected checkout")
    return dict(
        **python_identity,
        native_file=str(native_file),
        native_sha256=hashlib.sha256(native_file.read_bytes()).hexdigest(),
        native_profiling=profiling,
        guarded_row=guarded,
    )


def fetch_worker(args, mode):
    if mode not in ("latency", "route") or not SHA.fullmatch(args.revision or ""):
        raise ValueError("Fetch measurement requires a mode and exact revision")
    verify_reused_source(args.source_root, args.revision)
    check_build(args.source_root, profiling=mode == "route")
    import mssql_python
    from mssql_python import ddbc_bindings, perf_timer

    provenance = native_identity(args.source_root, args.revision, mode == "route")
    cases = workloads.single_row_registry()
    chosen = args.scenarios if args.scenarios is not None else list(cases)
    if not chosen or set(chosen) - set(cases):
        raise ValueError("Unknown or empty single-row workload selection")
    ctx = _FetchContext(mode, ddbc_bindings.profiling if mode == "route" else None, perf_timer)
    output = {}

    def checkpoint(active=None, environment=None):
        args.output.write_text(
            json.dumps(
                dict(
                    status="running" if environment is None else "complete",
                    active_scenario=active,
                    mode=mode,
                    provenance=provenance,
                    scenarios=output,
                    environment=environment,
                ),
                allow_nan=False,
            ),
            encoding="utf-8",
        )

    checkpoint()
    try:
        with mssql_python.connect(os.environ["DB_CONNECTION_STRING"]) as conn:
            for name in chosen:
                checkpoint(name)
                result = cases[name](conn, ctx)
                if mode == "route":
                    timer = (
                        "ddbc::FetchMany_wrap"
                        if name.endswith("fetchmany")
                        else "ddbc::FetchOne_wrap"
                    )
                    if result["cpp"].get(timer, {}).get("calls") != workloads.SINGLE_ROW_COUNT + 1:
                        raise RuntimeError("Native fetch call count does not match the workload")
                    constructors = (
                        result["cpp"].get("ddbc::FetchRow::construct_row", {}).get("calls", 0)
                    )
                    if constructors != expected_constructors(provenance, name.split("_")[1]):
                        raise RuntimeError("Native Row construction route was not established")
                output[name] = {key: result[key] for key in ("wall_ms", "cpp", "py")}
                output[name]["work"] = result["detail"]
                checkpoint()
            with conn.cursor() as cursor:
                cursor.execute("SELECT CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(80))")
                sql_version = cursor.fetchone()[0]
            environment = dict(
                os=platform.system(),
                architecture=platform.machine().lower(),
                python=platform.python_version(),
                sql_version=sql_version,
            )
    finally:
        ctx.disable()
    checkpoint(environment=environment)


def worker(args):
    mode = getattr(args, "mode", "diagnostic")
    if mode != "diagnostic":
        return fetch_worker(args, mode)
    # Import the chosen driver FIRST, then the SAME workload/controller for both
    # revisions. Never mix two native extensions into one interpreter.
    revision = getattr(args, "revision", None)
    if revision:
        verify_reused_source(args.source_root, revision)
    check_build(args.source_root, profiling=True)
    provenance = native_identity(args.source_root, revision, True) if revision else None
    core, workloads = load_suite()

    cases = workloads.registry()
    chosen = args.scenarios or list(cases)
    if set(chosen) - set(cases):
        raise ValueError("Unknown benchmark scenario")
    core.SCENARIOS = cases
    # The runner owns enable/disable/cleanup, just as in the documented CLI.
    with core.Profiler() as profiler:
        output = {}
        for name in chosen:
            print(f"Starting scenario: {name}", flush=True)
            args.output.write_text(
                json.dumps(dict(status="running", active_scenario=name, scenarios=output)),
                encoding="utf-8",
            )
            # Keep phase tables out of logs, but never hide which workload stalled.
            with contextlib.redirect_stdout(io.StringIO()):
                result = profiler.run(name)[0]
            if result["cpp"] is None or result["py"] is None:
                raise RuntimeError(f"Scenario {name} was skipped")
            if not result["cpp"]:
                raise RuntimeError(f"Scenario {name} has no native samples")
            output[name] = {key: result[key] for key in ("wall_ms", "cpp", "py")}
            output[name]["work"] = result.get("detail", "Connection: 1").split(" (")[0]
            args.output.write_text(
                json.dumps(
                    dict(status="running", active_scenario=None, scenarios=output), allow_nan=False
                ),
                encoding="utf-8",
            )
            print(f"Completed scenario: {name} ({result['wall_ms']:.3f} ms)", flush=True)
        print("Collecting server metadata", flush=True)
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
    result = dict(environment=environment, scenarios=output)
    if provenance is not None:
        result.update(status="complete", mode="diagnostic", provenance=provenance)
    args.output.write_text(json.dumps(result, allow_nan=False), encoding="utf-8")


def measure(
    path,
    output,
    scenarios,
    timeout=WORKER_TIMEOUT,
    mode="diagnostic",
    revision=None,
    isolated=False,
):
    command = [
        sys.executable,
        "-u",
        "-m",
        "eng.profiler_benchmarks.controller",
        "--worker",
        "--source-root",
        str(path),
        "--output",
        str(output),
    ]
    if mode != "diagnostic" or revision is not None:
        command += ["--mode", mode, "--revision", revision]
    if scenarios:
        command += ["--scenarios", *scenarios]
    output.unlink(missing_ok=True)
    if isolated:
        run_process(command, output.with_suffix(".log"), timeout)
    else:
        with output.with_suffix(".log").open("w", encoding="utf-8") as log:
            subprocess.run(
                command, stdout=log, stderr=subprocess.STDOUT, timeout=timeout, check=True
            )
    return json.loads(output.read_text(encoding="utf-8"))


def remaining(deadline, limit):
    seconds = deadline - time.monotonic()
    if seconds <= 0:
        raise TimeoutError("Profiler benchmarks exhausted their overall build/measurement budget")
    return min(seconds, limit)


def write_report(path, report):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(report, allow_nan=False), encoding="utf-8")
    temporary.replace(path)


def run_ci_report(args):
    if not args.reuse_candidate or args.mode != "diagnostic" or args.scenarios is not None:
        raise ValueError(
            "--ci-report requires --reuse-candidate and the complete default workload sets"
        )
    if args.samples != 5 or args.warmups != 1:
        raise ValueError("--ci-report requires five pairs and one warmup pair")
    finish_deadline = time.monotonic() + BENCHMARK_TIMEOUT
    deadline = finish_deadline - CI_FINISH_RESERVE
    base, candidate = resolve_revisions(args.base, args.candidate, timeout=remaining(deadline, 30))
    if candidate != git("rev-parse", "HEAD", timeout=remaining(deadline, 30)):
        raise ValueError("--ci-report must reuse checkout HEAD")
    head = os.environ.get("SYSTEM_PULLREQUEST_SOURCECOMMITID", candidate)
    if not SHA.fullmatch(head):
        raise ValueError("Invalid PR head identity")
    args.output.mkdir(parents=True, exist_ok=True)
    report_path = args.output / "report.json"
    common = dict(
        status="incomplete",
        leg=args.leg,
        base_commit=base,
        source_commit=candidate,
        head_commit=head,
        build_id=int(os.environ.get("BUILD_BUILDID", "0")),
        samples=5,
        warmups=1,
    )
    modes = {
        mode: dict(
            common,
            schema_version=1 if mode == "diagnostic" else 2,
            pairs=[],
            unavailable_reason="Not started: waiting for earlier modes",
        )
        for mode in MODES
    }
    for mode in ("latency", "route"):
        modes[mode]["mode"] = mode
    bundle = modes["diagnostic"]
    bundle.update(
        measurement_bundle_version=1,
        fetch_measurements={mode: modes[mode] for mode in ("latency", "route")},
    )
    validate_ci_header(bundle)
    write_report(report_path, bundle)
    on_identities = {}
    failures = (
        subprocess.CalledProcessError,
        subprocess.TimeoutExpired,
        TimeoutError,
        ValueError,
        KeyError,
        TypeError,
        OSError,
    )
    directory = tempfile.mkdtemp(prefix="profiler-ci-bundle-")
    safe_to_clean = True
    try:
        roots = {name: Path(directory) / name for name in ("base-off", "candidate-off", "base-on")}
        on_ready = False
        for mode in ("latency", "route", "diagnostic"):
            report = modes[mode]
            stage = "admission"
            try:
                remaining(deadline, 1)
                if mode == "diagnostic" and not on_ready:
                    raise ValueError("Shared ON build unavailable")
                builds = (
                    (("base-off", base), ("candidate-off", candidate))
                    if mode == "latency"
                    else (("base-on", base),) if mode == "route" else ()
                )
                for name, revision in builds:
                    stage = "archive " + name
                    report["unavailable_reason"] = "Incomplete: " + stage
                    write_report(report_path, bundle)
                    run_process(
                        [
                            sys.executable,
                            "-m",
                            "eng.profiler_benchmarks.controller",
                            "--archive-source",
                            revision,
                            "--source-root",
                            str(roots[name]),
                        ],
                        args.output / ("archive-" + name + ".log"),
                        remaining(deadline, 60),
                    )
                    stage = "build " + name
                    report["unavailable_reason"] = "Incomplete: " + stage
                    write_report(report_path, bundle)
                    build(
                        roots[name],
                        args.output / ("build-" + name + ".log"),
                        remaining(deadline, 900),
                        profiling=mode != "latency",
                    )
                if mode == "route":
                    on_ready = True
                paths = {
                    "base": roots["base-off" if mode == "latency" else "base-on"],
                    "candidate": roots["candidate-off"] if mode == "latency" else ROOT,
                }
                sources = {
                    side: python_source_identity(paths[side], base if side == "base" else candidate)
                    for side in ("base", "candidate")
                }
                for previous in modes.values():
                    if "python_sources" in previous and previous["python_sources"] != sources:
                        raise ValueError("Python source identity changed across modes")
                report["python_sources"] = sources
                validate_python_sources(report)
                identity = None
                for sample in range(6):
                    pair = {}
                    for side in (
                        ("base", "candidate") if sample % 2 == 0 else ("candidate", "base")
                    ):
                        stage = f"{mode} pair {sample} {side}"
                        report["unavailable_reason"] = "Incomplete: " + stage
                        write_report(report_path, bundle)
                        pair[side] = measure(
                            paths[side],
                            args.output / f"{mode}-{side}-{sample}.json",
                            None,
                            remaining(
                                deadline,
                                WORKER_TIMEOUT if mode == "diagnostic" else FETCH_WORKER_TIMEOUT,
                            ),
                            mode=mode,
                            revision=base if side == "base" else candidate,
                            isolated=True,
                        )
                    probe = dict(report, pairs=[*report["pairs"], pair])
                    validate_ci_mode(probe, mode)
                    observed = {
                        side: (pair[side]["provenance"], pair[side]["environment"]) for side in pair
                    }
                    if identity is not None and observed != identity:
                        raise ValueError("Worker identity changed after warmup")
                    identity = observed
                    if mode != "latency":
                        for side in pair:
                            if side in on_identities and on_identities[side] != observed[side]:
                                raise ValueError("Shared ON worker identity mismatch")
                            on_identities[side] = observed[side]
                    if sample:
                        report["pairs"].append(pair)
                        write_report(report_path, bundle)
                remaining(deadline, 1)
                report["status"] = "complete"
                validate_ci_mode(report, mode)
                report.pop("unavailable_reason", None)
            except ProcessCleanupError:
                safe_to_clean = False
                bundle["cleanup_required"] = directory
                for pending in modes.values():
                    if pending["status"] != "complete":
                        pending["unavailable_reason"] = (
                            "Not completed: prior process cleanup failed"
                        )
                report["status"] = "incomplete"
                report["unavailable_reason"] = (
                    "Process cleanup failed; further modes were not started"
                )
                write_report(report_path, bundle)
                raise
            except failures as error:
                report["status"] = "incomplete"
                detail = (
                    str(error)[:140]
                    if isinstance(error, ValueError)
                    else "see raw worker/build evidence"
                )
                report["unavailable_reason"] = f"{stage}: {type(error).__name__}: {detail}"
                print(report["unavailable_reason"], file=sys.stderr, flush=True)
            finally:
                write_report(report_path, bundle)
    finally:
        if safe_to_clean:
            try:
                shutil.rmtree(directory)
            except OSError as error:
                bundle["status"] = "incomplete"
                bundle["cleanup_required"] = directory
                bundle["unavailable_reason"] = (
                    "Build-directory cleanup failed; retained evidence requires cleanup"
                )
                write_report(report_path, bundle)
                raise ProcessCleanupError("CI build-directory cleanup failed") from error
    _, errors = ci_mode_reports(bundle)
    for mode, reason in errors.items():
        modes[mode]["status"] = "incomplete"
        modes[mode]["unavailable_reason"] = reason
    write_report(report_path, bundle)
    if time.monotonic() > finish_deadline:
        bundle["status"] = "incomplete"
        bundle["unavailable_reason"] = "Aggregate finish deadline exceeded during finalization"
        write_report(report_path, bundle)
    if any(report["status"] != "complete" for report in modes.values()):
        raise RuntimeError("CI performance report is incomplete; see per-mode reasons and raw logs")


def run(args):
    mode = getattr(args, "mode", "diagnostic")
    if mode not in MODES:
        raise ValueError("Unknown measurement mode")
    if mode == "latency" and args.reuse_candidate:
        raise ValueError(
            "Latency requires fresh native-OFF builds; cannot reuse the CI profiling build"
        )
    base, candidate = resolve_revisions(args.base, args.candidate)
    args.output.mkdir(parents=True, exist_ok=True)
    report_path = args.output / "report.json"
    report_path.unlink(missing_ok=True)
    head = os.environ.get("SYSTEM_PULLREQUEST_SOURCECOMMITID", candidate)
    if not SHA.fullmatch(head):
        head = candidate
    report = dict(
        schema_version=1 if mode == "diagnostic" else 2,
        status="incomplete",
        leg=args.leg,
        base_commit=base,
        source_commit=candidate,
        head_commit=head,
        build_id=int(os.environ.get("BUILD_BUILDID", "0")),
        samples=args.samples,
        warmups=args.warmups,
        pairs=[],
    )
    if mode != "diagnostic":
        report["mode"] = mode
    report_path.write_text(json.dumps(report), encoding="utf-8")
    timeout = BENCHMARK_TIMEOUT if args.reuse_candidate else LOCAL_BENCHMARK_TIMEOUT
    deadline = time.monotonic() + timeout
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
                    [
                        sys.executable,
                        "-m",
                        "eng.profiler_benchmarks.controller",
                        "--check-build",
                        "on",
                    ],
                    check=True,
                    timeout=remaining(deadline, 60),
                )
                continue
            checkout(revision, paths[side])
            print(f"Building {mode} {side}: {revision}", flush=True)
            build_options = {"profiling": mode != "latency"} if mode != "diagnostic" else {}
            build(
                paths[side],
                args.output / f"build-{side}.log",
                remaining(deadline, 900),
                **build_options,
            )
        if mode != "diagnostic":
            report["python_sources"] = {
                side: python_source_identity(paths[side], base if side == "base" else candidate)
                for side in ("base", "candidate")
            }
            validate_python_sources(report)
            report["unavailable_reason"] = "Collecting selected workload pairs"
        identity = None
        for sample in range(args.warmups + args.samples):
            pair = {}
            order = ("base", "candidate") if sample % 2 == 0 else ("candidate", "base")
            for side in order:
                print(f"Measuring pair {sample + 1}: {side}", flush=True)
                pair[side] = measure(
                    paths[side],
                    args.output / f"{side}-{sample}.json",
                    args.scenarios,
                    remaining(deadline, WORKER_TIMEOUT),
                    **(
                        {"mode": mode, "revision": base if side == "base" else candidate}
                        if mode != "diagnostic"
                        else {}
                    ),
                )
            if pair["base"]["environment"] != pair["candidate"]["environment"]:
                raise RuntimeError("Base and candidate environments differ")
            if mode != "diagnostic":
                probe = dict(
                    report, pairs=[*report["pairs"], pair], unavailable_reason="Collecting pairs"
                )
                if args.scenarios is None:
                    validate_ci_mode(probe, mode)
                else:
                    validate_samples(probe, selected_cases=args.scenarios)
                observed = {
                    side: (pair[side]["provenance"], pair[side]["environment"]) for side in pair
                }
                if identity is not None and identity != observed:
                    raise ValueError("Worker identity changed after warmup")
                identity = observed
            if sample >= args.warmups:
                report["pairs"].append(pair)
                report_path.write_text(json.dumps(report, allow_nan=False), encoding="utf-8")
    if args.scenarios is None:
        report["status"] = "complete"
        if mode != "diagnostic":
            validate(report)
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
    parser.add_argument(
        "--mode",
        choices=MODES,
        default="diagnostic",
        help="diagnostic: both recorders; latency: native OFF/Python OFF; route: native ON/Python OFF",
    )
    parser.add_argument("--revision", help=argparse.SUPPRESS)
    parser.add_argument(
        "--ci-report",
        action="store_true",
        help="Bounded latency-first CI report with route and legacy diagnostics",
    )
    parser.add_argument("--archive-source", help=argparse.SUPPRESS)
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
    if args.ci_report and (args.worker or args.archive_source or args.check_build):
        parser.error("--ci-report cannot be combined with worker, archive or build-check modes")
    if args.archive_source:
        if not SHA.fullmatch(args.archive_source) or args.source_root is None:
            parser.error("--archive-source requires an exact revision and --source-root")
        checkout(args.archive_source, args.source_root)
    elif args.check_build:
        check_build(ROOT, profiling=args.check_build == "on")
    elif args.worker:
        if args.source_root is None or args.output is None:
            parser.error("--worker requires --source-root and --output")
        if args.mode != "diagnostic" and not SHA.fullmatch(args.revision or ""):
            parser.error("Fetch measurement workers require an exact --revision")
        # Dumps contain stack locations, not locals or connection strings. The
        # parent still kills/reaps the worker at its deadline if it cannot finish.
        faulthandler.enable()
        faulthandler.dump_traceback_later(60, repeat=True)
        try:
            worker(args)
        finally:
            faulthandler.cancel_dump_traceback_later()
    else:
        if (
            not args.output
            or not args.leg
            or not 3 <= args.samples <= 15
            or not 1 <= args.warmups <= 3
        ):
            parser.error("Choose a leg, 3-15 measured pairs and 1-3 warmup pairs")
        if args.ci_report:
            run_ci_report(args)
        else:
            run(args)


if __name__ == "__main__":
    main()
