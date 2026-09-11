#!/usr/bin/env python3
"""Run private process checks and five SQL recovery cases with controlled Colima delay.

Usage: python3 real_sql_recovery.py --helper <checkout>/eng/scripts/setup_sql_container.py
       --work-dir <Agent.TempDirectory>/sql-recovery --owner <BuildId.JobId> --colima
Use --sql-version 2022 or 2025; --process-only omits Docker (--owner is optional).
Place docker_shim.py and colima_delay_shim.py beside this script. Requires real Docker and
Colima already installed for --colima. No pytest, driver rebuild, or pipeline
queueing happens here. The helper is unmodified; the first successful clean
Colima invocation is deliberately held to >=610s, not a natural boot measurement.
"""

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import platform
import re
import secrets
import shutil
import signal
import stat
import subprocess
import sys
import time
from types import SimpleNamespace

EXPECTED_HELPER_SHA256 = "1015ab7d94d55d6a4e15cfb896a4fa21eda33c3f8d5d9d7a0819f2340eb3025b"


def invoke(args, env, timeout, password):
    if timeout <= 0:
        raise RuntimeError("Hosted recovery experiment deadline exhausted")
    process = subprocess.Popen(
        args, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
    )
    try:
        output, _ = process.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        process.send_signal(signal.SIGTERM)
        try:
            output, _ = process.communicate(timeout=115)
        except subprocess.TimeoutExpired:
            process.kill()
            process.communicate(timeout=5)
            raise RuntimeError("Experiment subprocess exceeded cancellation bound") from None
        raise RuntimeError("Experiment subprocess exceeded its deadline") from None
    finally:
        if process.poll() is None:
            process.send_signal(signal.SIGTERM)
            try:
                process.communicate(timeout=115)
            except subprocess.TimeoutExpired:
                process.kill()
                process.communicate(timeout=5)
    if password in output:
        raise RuntimeError("Secret canary detected; refusing to publish captured output")
    return process.returncode, output


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def verify_clean_startup(events_path, real_docker, query, env, password, expected_major):
    version_query = [real_docker] + query[1:]
    version_query[version_query.index("-Q") + 1] = (
        "SET NOCOUNT ON; SELECT CONVERT(varchar(10), SERVERPROPERTY('ProductMajorVersion'));"
    )
    version_query += ["-h", "-1", "-W"]
    code, output = invoke(version_query, env, 15, password)
    lines = [line.strip() for line in output.splitlines() if line.strip()]
    require(
        code == 0 and lines == [str(expected_major)],
        "Real SQL ProductMajorVersion does not match the selected image",
    )
    events = [json.loads(line) for line in events_path.read_text().splitlines()]
    starts = [event for event in events if event["event"] == "start" and event["case"] == "clean"]
    completed = [
        event for event in events if event["event"] == "real-complete" and event["case"] == "clean"
    ]
    delayed = [event for event in events if event["event"] == "delayed-success"]
    require(
        len(starts) == len(completed) == len(delayed) == 1, "Expected one controlled clean startup"
    )
    timing = delayed[0]
    require(
        completed[0]["returncode"] == timing["real_returncode"] == 0
        and timing["case"] == "clean"
        and timing["minimum_seconds"] == 610
        and 0 <= timing["real_seconds"] <= timing["wrapper_seconds"] < 2460
        and timing["real_seconds"] == completed[0]["real_seconds"]
        and 610 <= timing["wrapper_seconds"]
        and 0 <= timing["injected_delay_seconds"] <= timing["wrapper_seconds"],
        "Real Colima completion or controlled >600s timing proof is invalid",
    )
    return {"product_major_version": expected_major, "controlled_delay": True, **timing}


def process_state(pid):
    if sys.platform.startswith("linux"):
        try:
            text = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8", errors="replace")
        except (FileNotFoundError, ProcessLookupError):
            return ""
        comm, sep, fields = text.rpartition(")")
        require(sep and comm.startswith(f"{pid} ("), "Malformed process state")
        return fields.split()[0]
    result = subprocess.run(
        ["/bin/ps", "-o", "stat=", "-p", str(pid)], capture_output=True, text=True, timeout=2
    )
    require(result.returncode in (0, 1) and not result.stderr, "Cannot read process state")
    return result.stdout.strip()


def wait_stopped(pid, timeout=2):
    deadline = time.monotonic() + timeout
    while True:
        state = process_state(pid)
        if not state or state.startswith("Z"):
            return
        require(time.monotonic() < deadline, "Owned descendant is still running")
        time.sleep(0.01)


def stop_known(pid):
    state = process_state(pid)
    if not state or state.startswith("Z"):
        return
    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


def launcher_diagnostics(module, root):
    results = []
    captures = []
    temporary = module.tempfile
    secret = "ProcessCanary!" + secrets.token_hex(16)
    env = {**os.environ, "PROCESS_CANARY": secret}

    def create_capture(*args, **kwargs):
        output = temporary.TemporaryFile(*args, **kwargs)
        state = os.fstat(output.fileno())
        require(stat.S_IMODE(state.st_mode) == 0o600, "Capture permissions are not owner-only")
        require(state.st_nlink == 0, "Capture is not anonymous/unlinked")
        captures.append((output, state.st_ino))
        return output

    def closed():
        require(all(output.closed for output, _ in captures), "Parent capture descriptor leaked")

    module.tempfile = SimpleNamespace(TemporaryFile=create_capture)
    try:
        launcher = (
            "import subprocess,sys;"
            "p=subprocess.Popen([sys.executable,'-c','import time; time.sleep(5)']);"
            "print(p.pid,flush=True)"
        )
        started = time.monotonic()
        result = module.Commands("").run_launcher([sys.executable, "-c", launcher], 2)
        elapsed = time.monotonic() - started
        require(result.returncode == 0 and elapsed < 2, "Review launcher still requires EOF")
        pid = int(next(line for line in result.output.splitlines() if line.isdigit()))
        require(process_state(pid) and not process_state(pid).startswith("Z"), "Daemon was killed")
        closed()
        wait_stopped(pid, timeout=6)
        results.append(
            {"case": "exact-review-daemon-launcher", "success": True, "seconds": elapsed}
        )

        gate, written = root / "launcher-returned", root / "daemon-write.json"
        child = (
            "import json,os,pathlib,stat,time; "
            f"gate=pathlib.Path({str(gate)!r}); end=time.monotonic()+5\n"
            "while not gate.exists() and time.monotonic()<end: time.sleep(0.01)\n"
            "assert gate.exists()\n"
            "print('daemon output after parent descriptor closed',flush=True)\n"
            "s=os.fstat(1)\n"
            f"p=pathlib.Path({str(written)!r}); pending=p.with_suffix('.pending')\n"
            "pending.write_text(json.dumps("
            "{'inode':s.st_ino,'links':s.st_nlink,'permissions':stat.S_IMODE(s.st_mode)}))\n"
            "pending.replace(p)\n"
        )
        launcher = (
            "import os,subprocess,sys; "
            f"p=subprocess.Popen([sys.executable,'-c',{child!r}]); "
            "print('launcher '+os.environ['PROCESS_CANARY'],flush=True)"
        )
        started = time.monotonic()
        result = module.Commands(secret).run_launcher([sys.executable, "-c", launcher], 2, env=env)
        require(result.returncode == 0 and time.monotonic() - started < 2, "Daemon launch failed")
        require(
            secret not in result.output and "[REDACTED]" in result.output,
            "Launcher redaction failed",
        )
        closed()
        gate.write_text("parent descriptor closed", encoding="utf-8")
        end = time.monotonic() + 5
        while not written.exists() and time.monotonic() < end:
            time.sleep(0.01)
        require(written.exists(), "Daemon could not write after launcher completion")
        state = json.loads(written.read_text())
        require(
            state == {"inode": captures[-1][1], "links": 0, "permissions": 0o600},
            "Inherited capture inode contract changed",
        )
        results.append({"case": "daemon-writes-after-parent-fd-close", "success": True})

        ready = root / "failed-launcher-child.pid"
        child = (
            "import os,pathlib,signal,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); "
            f"pathlib.Path({str(ready)!r}).write_text(str(os.getpid())); time.sleep(60)"
        )
        launcher = (
            "import os,pathlib,subprocess,sys,time; "
            f"subprocess.Popen([sys.executable,'-c',{child!r}]); p=pathlib.Path({str(ready)!r})\n"
            "while not p.exists(): time.sleep(0.01)\n"
            "print('failed launcher '+os.environ['PROCESS_CANARY'],flush=True)\n"
            "sys.exit(7)\n"
        )
        try:
            result = module.Commands(secret).run_launcher(
                [sys.executable, "-c", launcher], 5, env=env
            )
            require(result.returncode == 7, "Nonzero launcher status was lost")
            require(
                secret not in result.output and "[REDACTED]" in result.output,
                "Failed launcher output was lost/unredacted",
            )
            wait_stopped(int(ready.read_text()))
            closed()
            results.append({"case": "nonzero-launcher-descendant-stopped", "success": True})
        finally:
            if ready.exists():
                stop_known(int(ready.read_text()))

        code = (
            "import os,time; print('launcher startup '+os.environ['PROCESS_CANARY'],flush=True); "
            "time.sleep(60)"
        )
        started = time.monotonic()
        try:
            module.Commands(secret).run_launcher([sys.executable, "-c", code], 2, env=env)
        except module.SetupTimeout as exc:
            require("Setup command timed out" in str(exc), "Primary launcher timeout was lost")
            require(
                "launcher startup" in str(exc) and secret not in str(exc),
                "Launcher timeout output was lost/unredacted",
            )
        else:
            raise RuntimeError("Timed-out launcher returned success")
        require(time.monotonic() - started < 3, "Launcher teardown exceeded its bound")
        closed()
        results.append({"case": "launcher-timeout-captured", "success": True})

        code = "import sys; sys.stdout.write(('x'*255+'\\n')*5000)"
        result = module.Commands("").run_launcher([sys.executable, "-c", code], 5)
        require(result.returncode == 0 and len(result.output) < 75000, "Launcher capture unbounded")
        require(
            "Colima startup snapshot" in result.output
            and "diagnostic output truncated" in result.output,
            "Launcher truncation not identified",
        )
        closed()
        results.append({"case": "large-launcher-fixed-snapshot", "success": True})
    finally:
        module.tempfile = temporary
    return results


def process_diagnostics(helper, root):
    spec = importlib.util.spec_from_file_location("candidate_sql_helper", helper)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    results = []
    reader = module.SafeCapture.read

    def delayed(self, stream):
        time.sleep(0.5)
        reader(self, stream)

    try:
        module.SafeCapture.read = delayed
        result = module.Commands("").run([sys.executable, "-c", "print('normal drain')"], 5)
        require(result.returncode == 0 and "normal drain" in result.output, "Delayed drain failed")
        results.append({"case": "normal-delayed-output", "success": True})
    finally:
        module.SafeCapture.read = reader
    result = module.Commands("").run(
        [sys.executable, "-c", "import sys; sys.stdout.write(('x'*255+'\\n')*5000)"], 5
    )
    require(result.returncode == 0 and len(result.output) < 75000, "Large output capture failed")
    results.append({"case": "large-output", "success": True, "retained_chars": len(result.output)})
    ready = root / "descendant.pid"
    child = (
        "import os,pathlib,signal,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); "
        f"pathlib.Path({str(ready)!r}).write_text(str(os.getpid())); time.sleep(60)"
    )
    launcher = (
        "import subprocess,sys,pathlib,time; "
        f"subprocess.Popen([sys.executable,'-c',{child!r}]); "
        f"p=pathlib.Path({str(ready)!r})\nwhile not p.exists(): time.sleep(0.01)\n"
    )
    control = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(60)"], start_new_session=True
    )
    try:
        started = time.monotonic()
        try:
            module.Commands("").run([sys.executable, "-c", launcher], 5)
        except module.SetupFailure as exc:
            require("output drain timed out" in str(exc), "Unexpected descendant result")
        else:
            raise RuntimeError("Hung descendant was reported as success")
        require(time.monotonic() - started < 6, "Descendant exceeded its command budget")
        require(ready.exists(), "Descendant never started")
        wait_stopped(int(ready.read_text()))
        require(control.poll() is None, "Unrelated process was terminated")
        results.append({"case": "hung-descendant-control-survives", "success": True})
    finally:
        if ready.exists():
            stop_known(int(ready.read_text()))
        control.terminate()
        control.wait(timeout=5)
    results.extend(launcher_diagnostics(module, root))
    for method, sig in (
        ("run", signal.SIGINT),
        ("run", signal.SIGTERM),
        ("run_launcher", signal.SIGINT),
        ("run_launcher", signal.SIGTERM),
    ):
        ready = root / f"{method}-signal-{sig}.pid"
        secret = "SignalCanary!" + secrets.token_hex(16)
        child = (
            "import os,pathlib,time; "
            "print('signal startup '+os.environ['PROCESS_CANARY'],flush=True); "
            f"pathlib.Path({str(ready)!r}).write_text(str(os.getpid())); time.sleep(60)"
        )
        worker = (
            "import importlib.util,os,signal,sys\n"
            f"s=importlib.util.spec_from_file_location('worker_helper',{str(helper)!r})\n"
            "m=importlib.util.module_from_spec(s); sys.modules[s.name]=m; s.loader.exec_module(m)\n"
            "def cancel(sig,frame): raise m.Cancelled(sig)\n"
            "signal.signal(signal.SIGINT,cancel); signal.signal(signal.SIGTERM,cancel)\n"
            f"try: m.Commands(os.environ['PROCESS_CANARY']).{method}("
            f"[sys.executable,'-c',{child!r}],600)\n"
            "except m.Cancelled as exc: sys.exit(128+exc.signum)\n"
        )
        process = subprocess.Popen(
            [sys.executable, "-c", worker],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            start_new_session=True,
            env={**os.environ, "PROCESS_CANARY": secret},
        )
        try:
            deadline = time.monotonic() + 10
            while not ready.exists() and process.poll() is None and time.monotonic() < deadline:
                time.sleep(0.01)
            require(ready.exists(), "Signal worker never started")
            started = time.monotonic()
            process.send_signal(sig)
            output, _ = process.communicate(timeout=6)
            elapsed = time.monotonic() - started
            require(process.returncode == 128 + sig, "Cancellation status was not preserved")
            require(elapsed < 6, "Cancellation waited for the 600-second original budget")
            require(
                secret not in output and "[REDACTED]" in output and "signal startup" in output,
                "Cancellation startup output was lost/unredacted",
            )
            wait_stopped(int(ready.read_text()))
            results.append(
                {
                    "case": ("" if method == "run" else "launcher-")
                    + f"signal-{sig}-short-teardown",
                    "success": True,
                    "exit": process.returncode,
                    "teardown_seconds": elapsed,
                }
            )
        finally:
            if process.poll() is None:
                process.kill()
                process.communicate(timeout=5)
            if ready.exists():
                stop_known(int(ready.read_text()))
    print(json.dumps(results, indent=2), flush=True)
    return results


def main():
    def cancel(_signum, _frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, cancel)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--helper", required=True, type=Path)
    parser.add_argument("--work-dir", required=True, type=Path)
    parser.add_argument("--owner")
    parser.add_argument("--colima", action="store_true")
    parser.add_argument("--process-only", action="store_true")
    parser.add_argument("--sql-version", choices=("2022", "2025"), default="2025")
    args = parser.parse_args()
    require(os.name == "posix", "This experiment requires an approved Unix Docker host")
    deadline = time.monotonic() + 3600
    helper = args.helper.resolve(strict=True)
    helper_hash = hashlib.sha256(helper.read_bytes()).hexdigest()
    require(helper_hash == EXPECTED_HELPER_SHA256, "Exact cutoff-fix helper hash mismatch")
    host = {
        "python": sys.version.split()[0],
        "platform": sys.platform,
        "machine": platform.machine(),
        "os_release": platform.release(),
    }
    print(json.dumps({"helper_sha256": helper_hash, "host": host}), flush=True)
    root = args.work_dir.resolve()
    root.mkdir(parents=True, exist_ok=False)
    processes = process_diagnostics(helper, root)
    require(len(processes) == 12, "Not all twelve unchanged process controls completed")
    (root / "process-summary.json").write_text(
        json.dumps({"helper_sha256": helper_hash, "results": processes}, indent=2), encoding="utf-8"
    )
    if args.process_only:
        return
    require(args.colima and sys.platform == "darwin", "Full cutoff proof requires macOS and Colima")
    require(
        sys.version_info[:2] == (3, 13 if args.sql_version == "2022" else 14),
        "Python version does not match the selected macOS matrix leg",
    )
    real_docker = shutil.which("docker")
    require(real_docker is not None and args.owner, "Real Docker and --owner are required")
    bindir = root / "bin"
    bindir.mkdir()
    shim = bindir / "docker"
    shutil.copyfile(Path(__file__).with_name("docker_shim.py"), shim)
    shim.chmod(0o700)
    real_colima = shutil.which("colima") if args.colima else None
    colima_events = root / "colima-timing.jsonl"
    if args.colima:
        require(real_colima is not None, "Real Colima is required")
        colima_shim = bindir / "colima"
        shutil.copyfile(Path(__file__).with_name("colima_delay_shim.py"), colima_shim)
        colima_shim.chmod(0o700)
    password = "Recovery!" + secrets.token_urlsafe(32) + "aZ42"
    nonce = secrets.token_hex(5)
    env = os.environ.copy()
    env.pop("DB_CONNECTION_STRING", None)
    env.update(
        DB_PASSWORD=password,
        SQLCMDPASSWORD=password,
        MSSQL_SA_PASSWORD=password,
        REAL_DOCKER=real_docker,
        PATH=str(bindir) + os.pathsep + env["PATH"],
    )
    if args.colima:
        env.update(
            REAL_COLIMA=real_colima,
            CUTOFF_COLIMA_EVENTS=str(colima_events),
            CUTOFF_COLIMA_MARKER=str(root / "first-clean-delay.claimed"),
        )
    context = ["--context", "colima"] if args.colima else []
    summaries = []
    cases = (
        ("clean", 1, 1),
        ("recover", 2, 2),
        ("permanent", 2, 2),
        ("busy-once", 1, 2),
        ("unavailable", 0, 2),
    )
    for mode, expected_creates, expected_attempts in cases:
        name = f"sql-recovery-{nonce}-{mode}"
        owner = args.owner + "." + nonce + "." + mode
        events_path = root / (mode + ".jsonl")
        sentinel = root / (mode + ".dependent-sql-query")
        env.update(RECOVERY_EVENTS=str(events_path), RECOVERY_MODE=mode, RECOVERY_OWNER=owner)
        common = [sys.executable, str(helper), "--name", name, "--owner", owner]
        if args.colima:
            common.append("--colima")
        image = f"mcr.microsoft.com/mssql/server:{args.sql_version}-latest"
        command = common + ["--image", image]
        if not args.colima:
            command += ["--database", "TestDB"]
        output = ""
        failure = None
        cleanup_failure = None
        clean_proof = None
        try:
            code, output = invoke(
                command,
                env,
                min(2460 if args.colima else 1260, deadline - time.monotonic() - 120),
                password,
            )
            print(f"=== {mode}: helper exit={code} ===", flush=True)
            print(output, flush=True)
            (root / (mode + ".log")).write_text(output, encoding="utf-8")
            events = [json.loads(line) for line in events_path.read_text().splitlines()]
            require(
                not any(event["event"] == "secret-on-argv" for event in events),
                "Secret was passed on Docker argv",
            )
            image_lines = re.findall(
                r"^\[sql\] SQL image .+; repository digests=(.+)$", output, re.M
            )
            require(
                len(image_lines) == (0 if mode == "unavailable" else 1),
                "Unexpected SQL image resolution count",
            )
            digests = json.loads(image_lines[0]) if image_lines else []
            if mode != "unavailable":
                require(bool(digests), "Real SQL image repository digest missing")
            if args.colima:
                require(
                    output.count("[sql] Starting Colima once") == 1,
                    "Colima was not started exactly once outside SQL retry",
                )
            before = [event for event in events if event["event"] == "before"]
            starts = [event["id"] for event in before if event["operation"] == "start"]
            require(len(starts) == expected_creates, "Unexpected real SQL start count")
            require(
                sum(event["operation"] == "create" for event in before) == expected_creates,
                "Unexpected real SQL creation count",
            )
            require(len(set(starts)) == expected_creates, "A container ID was reused")
            attempts = re.findall(r"^\[sql\] SQL setup attempt (\d+)/2$", output, re.M)
            require(
                attempts == [str(i) for i in range(1, expected_attempts + 1)],
                "Unexpected SQL attempt count or third attempt",
            )
            injections = [event for event in events if event["event"] == "injected-exit"]
            require(
                len(injections) == (1 if mode == "recover" else 2 if mode == "permanent" else 0),
                "Expected deterministic injection was not performed",
            )
            require(all(event["code"] == 0 for event in injections), "An injected kill failed")
            for event in injections:
                actions = [item["operation"] for item in before if item.get("id") == event["id"]]
                require(
                    "logs" in actions and "rm" in actions,
                    "Failed container evidence/cleanup absent",
                )
                require(
                    actions.index("logs") < actions.index("rm"), "Container removed before evidence"
                )
            if code == 0:
                database = "master" if args.colima else "TestDB"
                query = [
                    "docker",
                    *context,
                    "exec",
                    "--env",
                    "SQLCMDPASSWORD",
                    starts[-1],
                    "/opt/mssql-tools18/bin/sqlcmd",
                    "-S",
                    "localhost",
                    "-U",
                    "SA",
                    "-C",
                    "-b",
                    "-l",
                    "5",
                    "-t",
                    "5",
                    "-d",
                    database,
                    "-Q",
                    "SELECT 1",
                ]
                query_code, query_output = invoke(query, env, 15, password)
                require(query_code == 0, "Real post-recovery SQL query failed")
                if mode == "clean" and args.colima:
                    clean_proof = verify_clean_startup(
                        colima_events,
                        real_docker,
                        query,
                        env,
                        password,
                        16 if args.sql_version == "2022" else 17,
                    )
                sentinel.write_text("Dependent SQL query succeeded\n", encoding="utf-8")
            if mode in ("permanent", "unavailable"):
                require(code != 0, "Persistent failure incorrectly returned success")
                require("no further attempts" in output, "Persistent retry limit was not reported")
                require(not sentinel.exists(), "Dependent SQL query ran despite permanent failure")
            else:
                require(code == 0 and sentinel.exists(), "Dependent SQL query did not succeed")
                expected = (
                    "ready on first attempt" if mode == "clean" else "recovered on second attempt"
                )
                require(expected in output, "Clean success and recovery were not distinguished")
            if mode == "unavailable":
                require(
                    [event["operation"] for event in before] == ["container", "container"],
                    "Unavailable daemon case issued more than two read-only lookups",
                )
            if mode == "busy-once":
                require(
                    sum(event["event"] == "injected-read-only-timeout" for event in events) == 1
                    and "Setup command timed out" in output,
                    "First lookup did not exercise a real command timeout",
                )
            events = [json.loads(line) for line in events_path.read_text().splitlines()]
            for index, event in enumerate(events):
                if event["event"] == "injected-exit":
                    require(
                        not any(
                            later["event"] == "before"
                            and later.get("operation") == "exec"
                            and later.get("id") == event["id"]
                            for later in events[index + 1 :]
                        ),
                        "Injected failed container received SQL exec after kill",
                    )
            summaries.append(
                {
                    "case": mode,
                    "helper_exit": code,
                    "container_ids": starts,
                    "injected_exits": len(injections),
                    "sql_query_ran": sentinel.exists(),
                    "dependent_sentinel": sentinel.name,
                    "source_sha": os.environ.get("BUILD_SOURCEVERSION", "record-exact-source-sha"),
                    "helper_sha256": helper_hash,
                    "image_digests": digests,
                    "setup_attempts": len(attempts),
                    "sql_image": image,
                    "host": host,
                    "clean_cutoff_proof": clean_proof,
                }
            )
        except (RuntimeError, OSError, subprocess.SubprocessError) as exc:
            failure = str(exc)
        finally:
            try:
                cleanup_env = {**env, "RECOVERY_MODE": "clean"}
                cleanup_code, cleanup_output = invoke(
                    common + ["--cleanup"], cleanup_env, 115, password
                )
                print(cleanup_output, flush=True)
                require(cleanup_code == 0, f"Owned experiment cleanup failed (exit {cleanup_code})")
                query_code, present = invoke(
                    [
                        real_docker,
                        *context,
                        "container",
                        "ls",
                        "--all",
                        "--filter",
                        f"name=^/{name}$",
                        "--format",
                        "{{.ID}}",
                    ],
                    env,
                    15,
                    password,
                )
                require(query_code == 0 and not present.strip(), "Experiment container remains")
            except (RuntimeError, OSError, subprocess.SubprocessError) as exc:
                cleanup_failure = str(exc)
                print(f"[experiment cleanup] {cleanup_failure}", file=sys.stderr, flush=True)
        if failure is not None or cleanup_failure is not None:
            details = []
            if failure is not None:
                details.append("Scenario failed: " + failure)
            if cleanup_failure is not None:
                details.append("Cleanup failed: " + cleanup_failure)
            raise RuntimeError(f"{mode}: " + "\n".join(details))
    require(len(summaries) == 5, "Not all five SQL cases completed")
    require(
        len({tuple(item["image_digests"]) for item in summaries if item["image_digests"]}) == 1,
        "SQL image digest changed between experiment cases",
    )
    (root / "summary.json").write_text(json.dumps(summaries, indent=2), encoding="utf-8")
    print(json.dumps(summaries, indent=2))
    print("Injected exits validate recovery semantics, not the engine crash root cause.")
    print("The first-clean >600s delay is controlled proof, not a natural Colima boot measurement.")


if __name__ == "__main__":
    main()
