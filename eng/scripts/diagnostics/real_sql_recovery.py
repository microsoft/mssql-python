#!/usr/bin/env python3
"""Run approved clean/recover/permanent-failure SQL2025 experiments on a Unix host.

Usage: python3 real_sql_recovery.py --helper <checkout>/eng/scripts/setup_sql_container.py
       --work-dir <Agent.TempDirectory>/sql-recovery --owner <BuildId.JobId> [--colima]
Place docker_shim.py beside this script. Requires Python 3, real Docker, and
Colima already installed for --colima. No pytest, driver rebuild, or pipeline
queueing happens here. The production helper is unmodified.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import signal
import subprocess
import sys
import time


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


def main():
    def cancel(_signum, _frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, cancel)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--helper", required=True, type=Path)
    parser.add_argument("--work-dir", required=True, type=Path)
    parser.add_argument("--owner", required=True)
    parser.add_argument("--colima", action="store_true")
    args = parser.parse_args()
    require(os.name == "posix", "This experiment requires an approved Unix Docker host")
    real_docker = shutil.which("docker")
    require(real_docker is not None, "Real Docker executable is required")
    helper = args.helper.resolve(strict=True)
    helper_hash = hashlib.sha256(helper.read_bytes()).hexdigest()
    root = args.work_dir.resolve()
    root.mkdir(parents=True, exist_ok=False)
    bindir = root / "bin"
    bindir.mkdir()
    shim = bindir / "docker"
    shutil.copyfile(Path(__file__).with_name("docker_shim.py"), shim)
    shim.chmod(0o700)
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
    context = ["--context", "colima"] if args.colima else []
    deadline = time.monotonic() + 3600
    summaries = []
    for mode, expected_attempts in (("clean", 1), ("recover", 2), ("permanent", 2)):
        name = f"sql-recovery-{nonce}-{mode}"
        owner = args.owner + "." + nonce + "." + mode
        events_path = root / (mode + ".jsonl")
        sentinel = root / (mode + ".dependent-sql-query")
        env.update(
            RECOVERY_EVENTS=str(events_path), RECOVERY_MODE=mode, RECOVERY_OWNER=owner
        )
        common = [sys.executable, str(helper), "--name", name, "--owner", owner]
        if args.colima:
            common.append("--colima")
        command = common + ["--image", "mcr.microsoft.com/mssql/server:2025-latest"]
        if not args.colima:
            command += ["--database", "TestDB"]
        output = ""
        failure = None
        try:
            code, output = invoke(
                command, env, min(2460 if args.colima else 1260, deadline - time.monotonic() - 120),
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
            image_lines = re.findall(r"^\[sql\] SQL image .+; repository digests=(.+)$", output, re.M)
            require(len(image_lines) == 1, "Expected one resolved SQL image per helper invocation")
            digests = json.loads(image_lines[0])
            require(bool(digests), "Real SQL image repository digest missing")
            if args.colima:
                require(
                    output.count("[sql] Starting Colima once") == 1,
                    "Colima was not started exactly once outside SQL retry",
                )
            before = [event for event in events if event["event"] == "before"]
            starts = [event["id"] for event in before if event["operation"] == "start"]
            require(len(starts) == expected_attempts, "Unexpected real SQL start attempt count")
            require(
                sum(event["operation"] == "create" for event in before) == expected_attempts,
                "Unexpected real SQL creation count",
            )
            require(len(set(starts)) == expected_attempts, "A container ID was reused")
            injections = [event for event in events if event["event"] == "injected-exit"]
            require(
                len(injections) == (0 if mode == "clean" else 1 if mode == "recover" else 2),
                "Expected deterministic injection was not performed",
            )
            require(all(event["code"] == 0 for event in injections), "An injected kill failed")
            for event in injections:
                actions = [item["operation"] for item in before if item.get("id") == event["id"]]
                require("logs" in actions and "rm" in actions, "Failed container evidence/cleanup absent")
                require(actions.index("logs") < actions.index("rm"), "Container removed before evidence")
            if code == 0:
                database = "master" if args.colima else "TestDB"
                query = [
                    "docker", *context, "exec", "--env", "SQLCMDPASSWORD", starts[-1],
                    "/opt/mssql-tools18/bin/sqlcmd", "-S", "localhost", "-U", "SA",
                    "-C", "-b", "-l", "5", "-t", "5", "-d", database, "-Q", "SELECT 1",
                ]
                query_code, query_output = invoke(query, env, 15, password)
                require(query_code == 0, "Real post-recovery SQL query failed")
                sentinel.write_text("Dependent SQL query succeeded\n", encoding="utf-8")
            if mode == "permanent":
                require(code != 0, "Persistent failure incorrectly returned success")
                require("no further attempts" in output, "Persistent retry limit was not reported")
                require(not sentinel.exists(), "Dependent SQL query ran despite permanent failure")
            else:
                require(code == 0 and sentinel.exists(), "Dependent SQL query did not succeed")
                expected = "ready on first attempt" if mode == "clean" else "recovered on second attempt"
                require(expected in output, "Clean success and recovery were not distinguished")
            events = [json.loads(line) for line in events_path.read_text().splitlines()]
            for index, event in enumerate(events):
                if event["event"] == "injected-exit":
                    require(
                        not any(
                            later["event"] == "before"
                            and later.get("operation") == "exec"
                            and later.get("id") == event["id"]
                            for later in events[index + 1:]
                        ),
                        "Injected failed container received SQL exec after kill",
                    )
            summaries.append({
                "case": mode, "helper_exit": code, "container_ids": starts,
                "injected_exits": len(injections), "sql_query_ran": sentinel.exists(),
                "dependent_sentinel": sentinel.name,
                "source_sha": os.environ.get("BUILD_SOURCEVERSION", "record-exact-source-sha"),
                "helper_sha256": helper_hash,
                "image_digests": digests,
            })
        except (RuntimeError, OSError, subprocess.SubprocessError) as exc:
            failure = str(exc)
        finally:
            cleanup_code, cleanup_output = invoke(common + ["--cleanup"], env, 115, password)
            print(cleanup_output, flush=True)
            require(cleanup_code == 0, "Owned experiment cleanup failed")
            query_code, present = invoke(
                [real_docker, *context, "container", "ls", "--all", "--filter",
                 f"name=^/{name}$", "--format", "{{.ID}}"],
                env, 15, password,
            )
            require(query_code == 0 and not present.strip(), "Experiment container remains")
        if failure:
            raise RuntimeError(f"{mode}: {failure}")
    require(
        len({tuple(item["image_digests"]) for item in summaries}) == 1,
        "SQL image digest changed between experiment cases",
    )
    (root / "summary.json").write_text(json.dumps(summaries, indent=2), encoding="utf-8")
    print(json.dumps(summaries, indent=2))
    print("Injected exits validate recovery semantics, not the engine crash root cause.")


if __name__ == "__main__":
    main()
