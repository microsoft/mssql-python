"""Standalone Windows shutdown acceptance runner. Public output is redacted text only."""

import argparse
from collections import Counter
import contextlib
import hashlib
import importlib
import importlib.metadata
import io
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import sys
import sysconfig
import time
from unittest.mock import patch
import xml.etree.ElementTree as ET

NORMAL_FILES = [
    r"tests\test_005_connection_cursor_lifecycle.py",
    r"tests\test_009_pooling.py",
    r"tests\test_013_SqlHandle_free_shutdown.py",
    r"tests\test_024_context_manager_transaction.py",
    r"tests\test_025_logging_concurrency_deadlock.py",
]
CANARY = "HOSTED_SECRET_CANARY_591aD4eC"
ALLOWED = {".json", ".log", ".xml"}


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def redact(text, secrets):
    for value in sorted(set(secrets), key=len, reverse=True):
        if value:
            text = re.sub(re.escape(value), "[REDACTED]", text, flags=re.IGNORECASE)
    return text


def public_write(out, name, text, secrets):
    target = out / name
    assert target.parent == out and target.suffix in ALLOWED
    clean = redact(text, secrets)
    assert all(not secret or secret.casefold() not in clean.casefold() for secret in secrets)
    if target.suffix == ".xml":
        ET.fromstring(clean)
    target.write_text(clean, encoding="utf-8")


def publish_check(out, secrets):
    checked = 0
    for path in out.rglob("*"):
        if path.is_symlink() or not path.resolve().is_relative_to(out.resolve()):
            raise RuntimeError("Public artifact link rejected")
        if path.is_dir():
            continue
        if path.suffix not in ALLOWED:
            raise RuntimeError("Public artifact extension rejected")
        text = path.read_text(encoding="utf-8-sig")
        if re.search(r"^(?:TREE_DRAIN_INCOMPLETE|TREE_CLEANUP_INCOMPLETE)\b", text, re.M):
            raise RuntimeError("Publication refused: native process-tree cleanup was not confirmed")
        if any(secret and secret.casefold() in text.casefold() for secret in secrets):
            raise RuntimeError("Public artifact secret/canary scan failed")
        if path.suffix == ".json":
            json.loads(text)
        elif path.suffix == ".xml":
            ET.fromstring(text)
        checked += 1
    if not checked:
        raise RuntimeError("No public diagnostic artifacts")
    return checked


def verify_sources(repo, manifest_path):
    expected = json.loads(manifest_path.read_text(encoding="utf-8"))
    actual = {}
    materialized = []
    for name, spec in expected["files"].items():
        path = repo / name
        data = path.read_bytes()
        lf = data.replace(b"\r\n", b"\n") if spec["text"] else data
        if hashlib.sha256(lf).hexdigest() != spec["lf_sha256"]:
            raise RuntimeError(f"Approved source/content mismatch: {name}")
        if hashlib.sha256(data).hexdigest() != spec["tested_bytes_sha256"]:
            if not spec["text"]:
                raise RuntimeError(f"Approved binary mismatch: {name}")
            data = lf.replace(b"\n", b"\r\n") if spec["tested_crlf"] else lf
            if hashlib.sha256(data).hexdigest() != spec["tested_bytes_sha256"]:
                raise RuntimeError(f"Cannot materialize approved source bytes: {name}")
            path.write_bytes(data)
            materialized.append(name)
        actual[name] = {"lf_sha256": spec["lf_sha256"], "tested_bytes_sha256": digest(path)}
    return {
        "approved_base": expected["base"],
        "files": actual,
        "lf_tree_sha256": expected["lf_tree_sha256"],
        "line_endings_materialized": materialized,
        "checkout_commit": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=repo, text=True
        ).strip(),
    }


def child_env(repo, connection_string=None):
    env = os.environ.copy()
    env.pop("_NO_DEBUG_HEAP", None)
    env.pop("MSSQL_DIAG_SKIP_FINAL_ENV", None)
    env.pop("DB_CONNECTION_STRING", None)
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONPATH"] = os.pathsep.join([str(repo), sysconfig.get_path("purelib")])
    if connection_string is not None:
        env["DB_CONNECTION_STRING"] = connection_string
    return env


def collect(probe, command, env, repo, global_deadline, cap_seconds, plain=False):
    now = int(time.time() * 1000)
    deadline = min(now + int(cap_seconds * 1000), global_deadline - 10000)
    if deadline <= now:
        raise TimeoutError("Global work deadline reached; cleanup time remains reserved")
    launch = [str(probe), *(["--plain"] if plain else []), str(deadline), *command]
    timeout = min((deadline - now) / 1000 + 7, (global_deadline - now) / 1000)
    return subprocess.run(
        launch,
        cwd=repo,
        env=env,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
    )


def native_accounting(result, tag, require_extension=True):
    text = result.stderr
    processes = re.findall(r"^PROCESS pid=(\d+) root=([01]) image=(.*)$", text, re.M)
    roots = [pid for pid, root, _ in processes if root == "1"]
    creates = re.findall(r"^CREATE pid=(\d+)$", text, re.M)
    exits = re.findall(r"^EXIT pid=(\d+) code=([0-9a-f]+)$", text, re.M)
    assert len(roots) == 1 and len(creates) == len(set(creates)), "Invalid root/create accounting"
    assert sorted(creates) == sorted(pid for pid, _, _ in processes), "Missing process identity"
    assert sorted(creates) == sorted(pid for pid, _ in exits), "Missing/duplicate process exits"
    root = roots[0]
    root_exits = [code for pid, code in exits if pid == root]
    assert len(root_exits) == 1
    assert int(root_exits[0], 16) == result.returncode % (1 << 32), "Root returncode mismatch"
    python_loaded = bool(re.search(rf"MODULE pid={root} .*name=python{tag}\.dll ", text))
    extension_loaded = bool(
        re.search(rf"MODULE pid={root} .*name=ddbc_bindings\.cp{tag}-amd64\.pyd ", text)
    )
    if require_extension:
        assert (
            python_loaded and extension_loaded
        ), "Test root did not load expected Python/extension"
    auxiliary = [
        {
            "pid": pid,
            "exit_hex": code,
            "image": next(image for p, _, image in processes if p == pid),
        }
        for pid, code in exits
        if pid != root
    ]
    assert all(int(item["exit_hex"], 16) == 0 for item in auxiliary), "Auxiliary process failed"
    return {
        "root_pid": root,
        "root_exit_hex": root_exits[0],
        "root_python_loaded": python_loaded,
        "root_extension_loaded": extension_loaded,
        "auxiliary_processes": auxiliary,
        "second_chance": "SECOND_CHANCE" in text,
        "timeout": "BOUNDED_TIMEOUT" in text,
    }


def private_capture(private, stem, result):
    raw = private / "raw"
    raw.mkdir(exist_ok=True)
    (raw / f"{stem}.stdout.txt").write_text(result.stdout, encoding="utf-8")
    (raw / f"{stem}.stderr.txt").write_text(result.stderr, encoding="utf-8")


def self_test(args, secrets):
    deadline = int(time.time() * 1000) + 90000
    control = args.private / "control.json"
    if control.exists():
        deadline = min(
            deadline, json.loads(control.read_text(encoding="utf-8-sig"))["work_deadline_unix_ms"]
        )
    env = child_env(args.repo)
    smoke = collect(args.probe, [str(args.probe), "--symbol-smoke"], env, args.repo, deadline, 20)
    private_capture(args.private, "symbol-smoke", smoke)
    assert smoke.returncode % (1 << 32) == 0xC0000005
    assert "hosted_probe_symbol_smoke" in smoke.stderr and "SECOND_CHANCE" in smoke.stderr
    public_write(args.out, "symbol-smoke.log", smoke.stderr, secrets)
    code = (
        "import subprocess,sys,mssql_python; "
        "subprocess.run([sys.executable,'-c',\"print('AUXILIARY_OK')\"],check=True); "
        "print('ROOT_IMPORT_OK')"
    )
    result = collect(args.probe, [sys._base_executable, "-c", code], env, args.repo, deadline, 20)
    private_capture(args.private, "accounting-smoke", result)
    account = native_accounting(result, args.tag)
    assert result.returncode == 0 and len(account["auxiliary_processes"]) >= 1
    assert "ROOT_IMPORT_OK" in result.stdout
    public_write(args.out, "accounting-smoke.log", result.stderr, secrets)
    # OutputDebugString creates continuous debugger events, not idle waits.
    busy = "import ctypes\nwhile True: ctypes.windll.kernel32.OutputDebugStringW('busy')"
    started = time.monotonic()
    timeout = collect(args.probe, [sys._base_executable, "-c", busy], env, args.repo, deadline, 1)
    assert timeout.returncode == 124 and "BOUNDED_TIMEOUT" in timeout.stderr
    assert time.monotonic() - started < 10, "Continuous-event deadline did not stop the child"
    public_write(args.out, "deadline-smoke.log", timeout.stderr, secrets)
    tree_results = []
    for label, exit_code, expected in (
        ("timeout", None, 124),
        ("failure", 7, 7),
        ("orphan", 0, 125),
    ):
        parent_path = args.private / "raw" / f"tree-{label}-parent.heartbeat"
        child_path = args.private / "raw" / f"tree-{label}-grandchild.heartbeat"
        parent_path.unlink(missing_ok=True)
        child_path.unlink(missing_ok=True)
        child_code = (
            "import os,time\n"
            f"with open({str(child_path)!r},'ab',buffering=0) as f:\n"
            "    while True:\n"
            "        f.write(b'grandchild\\n');os.fsync(f.fileno());time.sleep(0.005)\n"
        )
        parent_code = (
            "import os,subprocess,sys,time\n"
            f"p=subprocess.Popen([sys.executable,'-c',{child_code!r}],"
            "stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)\n"
            f"while not os.path.exists({str(child_path)!r}):time.sleep(0.005)\n"
            "print(f'GRANDCHILD_PID={p.pid}',flush=True)\n"
            f"with open({str(parent_path)!r},'ab',buffering=0) as f:\n"
            "    f.write(b'parent\\n');os.fsync(f.fileno())\n"
        )
        if exit_code is None:
            parent_code += (
                "    while True:\n"
                "        f.write(b'parent\\n');os.fsync(f.fileno());time.sleep(0.005)\n"
            )
        else:
            parent_code += f"sys.exit({exit_code})\n"
        started = time.monotonic()
        tree = collect(
            args.probe,
            [sys._base_executable, "-c", parent_code],
            env,
            args.repo,
            deadline,
            3,
            plain=True,
        )
        elapsed = time.monotonic() - started
        private_capture(args.private, f"tree-{label}", tree)
        assert tree.returncode == expected, tree.stderr
        assert "TREE_DRAIN_COMPLETE active=0 root_signaled=1" in tree.stderr
        assert "cleanup_complete=1" in tree.stderr
        assert "TREE_DRAIN_INCOMPLETE" not in tree.stderr
        assert "GRANDCHILD_PID=" in tree.stdout and elapsed < 9
        sizes = [parent_path.stat().st_size, child_path.stat().st_size]
        assert all(size > 0 for size in sizes)
        time.sleep(0.15)
        assert sizes == [parent_path.stat().st_size, child_path.stat().st_size]
        public_write(args.out, f"tree-{label}.log", tree.stdout + tree.stderr, secrets)
        tree_results.append(
            {
                "case": label,
                "exit_code": tree.returncode,
                "seconds": elapsed,
                "stable_heartbeat_bytes": sizes,
            }
        )
    generated_failure = f"childscript connect('Server=localhost;Pwd={CANARY}')\n{CANARY.lower()}"
    public_write(args.out, "privacy-smoke.log", generated_failure, secrets)
    assert CANARY not in (args.out / "privacy-smoke.log").read_text()
    fixture = args.private / "test_private_privacy_failure.py"
    payload = "connect('Server=localhost;Pwd=" + secrets[-1] + "') " + CANARY
    fixture.write_text(
        "def test_expected_private_failure(record_property):\n"
        f"    record_property('secret_attribute', {secrets[-1]!r})\n"
        f"    child_script = {payload!r}\n"
        "    assert False, child_script\n",
        encoding="utf-8",
    )
    privacy_xml = args.private / "raw" / "pytest-privacy.xml"
    try:
        privacy = collect(
            args.probe,
            [
                sys._base_executable,
                "-m",
                "pytest",
                "-q",
                str(fixture),
                f"--junitxml={privacy_xml}",
                "--confcutdir",
                str(args.private),
                "-p",
                "no:cacheprovider",
            ],
            env,
            args.repo,
            deadline,
            20,
            plain=True,
        )
    finally:
        fixture.unlink()
    private_capture(args.private, "pytest-privacy", privacy)
    assert privacy.returncode == 1 and CANARY in privacy.stdout and secrets[-1] in privacy.stdout
    public_write(args.out, "pytest-privacy.log", privacy.stdout + privacy.stderr, secrets)
    raw_xml = privacy_xml.read_text(encoding="utf-8")
    xml_root = ET.fromstring(raw_xml)
    assert any(secrets[-1] in value for node in xml_root.iter() for value in node.attrib.values())
    assert any(CANARY in (node.text or "") for node in xml_root.iter())
    public_write(args.out, "pytest-privacy.xml", raw_xml, secrets)
    ET.parse(args.out / "pytest-privacy.xml")
    public_write(
        args.out,
        "self-test.json",
        json.dumps(
            {
                "symbolized_expected_fault": True,
                "continuous_event_deadline": True,
                "root_and_auxiliary_accounting": account,
                "privacy_canary_redacted": True,
                "actual_pytest_failure_redacted": True,
                "redacted_pytest_junit_valid": True,
                "plain_tree_drain_checks": tree_results,
                "sql_connections_created": 0,
            }
        ),
        secrets,
    )
    publish_check(args.out, secrets)


def run_acceptance(args, secrets):
    if os.name != "nt" or sys.maxsize <= 2**32 or sysconfig.get_config_var("Py_GIL_DISABLED"):
        raise RuntimeError("Acceptance requires standard Windows x64 Python")
    control = json.loads((args.private / "control.json").read_text(encoding="utf-8-sig"))
    deadline = control["work_deadline_unix_ms"]
    secret = json.loads((args.private / "sql-secret.json").read_text(encoding="utf-8-sig"))
    password = secret["password"]
    cs = f"Server=localhost;Database=TestDB;Uid=testuser;Pwd={password};Encrypt=yes;TrustServerCertificate=yes"
    env = child_env(args.repo, cs)
    os.environ["DB_CONNECTION_STRING"] = cs
    secrets.extend([password, cs])
    sys.path[:0] = [str(args.repo), str(args.repo / "tests")]
    source = verify_sources(args.repo, args.manifest)
    import mssql_python
    from mssql_python.constants import GetInfoConstants

    assert Path(mssql_python.__file__).resolve().parent == args.repo / "mssql_python"
    binary = args.repo / "mssql_python" / f"ddbc_bindings.cp{args.tag}-amd64.pyd"
    pdb = binary.with_suffix(".pdb")
    try:
        with mssql_python.connect(cs) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT @@VERSION, SERVERPROPERTY('ProductMajorVersion'), "
                    "CONNECTIONPROPERTY('net_transport'), CONNECTIONPROPERTY('auth_scheme')"
                )
                sql_version, major, transport, auth = cursor.fetchone()
            assert str(major) == {"SQL2022": "16", "SQL2025": "17"}[args.sql_version]
            driver_version = connection.getinfo(GetInfoConstants.SQL_DRIVER_VER.value)
    except mssql_python.Error as error:
        raise RuntimeError(redact(f"SQL metadata query failed: {error}", secrets)) from None
    public_write(
        args.out,
        "runtime-and-source.json",
        json.dumps(
            {
                "source": source,
                "python": sys.version,
                "os": platform.platform(),
                "pybind11": importlib.metadata.version("pybind11"),
                "pytest": importlib.metadata.version("pytest"),
                "sql_version": sql_version,
                "transport": transport,
                "authentication": auth,
                "driver_version": driver_version,
                "pyd_sha256": digest(binary),
                "pdb_sha256": digest(pdb),
                "native_probe_sha256": digest(args.probe),
                "work_deadline_unix_ms": deadline,
                "repetitions_per_workload": args.repetitions,
            },
            indent=2,
        ),
        secrets,
    )
    normal_xml = args.private / "raw" / "normal.xml"
    normal_xml.parent.mkdir(exist_ok=True)
    normal = collect(
        args.probe,
        [
            sys._base_executable,
            "-m",
            "pytest",
            "-v",
            *NORMAL_FILES,
            f"--junitxml={normal_xml}",
            f"--basetemp={args.private / 'pytest'}",
            "-p",
            "no:cacheprovider",
        ],
        env,
        args.repo,
        deadline,
        300,
        plain=True,
    )
    private_capture(args.private, "normal", normal)
    public_write(args.out, "normal.log", normal.stdout + "\n" + normal.stderr, secrets)
    if normal_xml.exists():
        xml_text = normal_xml.read_text(encoding="utf-8")
        try:
            ET.fromstring(xml_text)
        except ET.ParseError:
            public_write(args.out, "normal-junit-incomplete.log", xml_text, secrets)
            raise RuntimeError("Normal suite JUnit XML was incomplete") from None
        public_write(args.out, "normal.xml", xml_text, secrets)
    if normal.returncode != 0 or not normal_xml.exists():
        raise RuntimeError(f"Normal lifecycle suite failed or incomplete: exit {normal.returncode}")
    shutdown = importlib.import_module("test_013_SqlHandle_free_shutdown").TestHandleFreeShutdown()
    contexts = importlib.import_module(
        "test_024_context_manager_transaction"
    ).TestContextManagerCommit()
    import pytest

    cases = [
        (
            "all_handle_types_comprehensive",
            lambda: shutdown.test_all_handle_types_comprehensive(cs),
        ),
        (
            "rapid_connection_churn_with_shutdown",
            lambda: shutdown.test_rapid_connection_churn_with_shutdown(cs),
        ),
        ("nested_context_managers", contexts.test_nested_context_managers),
        ("rollback_on_exception", contexts.test_rollback_on_exception),
        ("autocommit_no_explicit_commit", contexts.test_autocommit_no_explicit_commit),
        (
            "explicit_close_nonpooled",
            lambda: shutdown.test_env_handle_cleanup_at_shutdown(cs, False),
        ),
        ("explicit_close_pooled", lambda: shutdown.test_env_handle_cleanup_at_shutdown(cs, True)),
    ]
    records = []
    original_run = subprocess.run
    current = {}

    def run_child(command, **kwargs):
        assert command[1] == "-c", "Unexpected test child command"
        started = time.monotonic()
        # Restore only while launching: collect() itself uses subprocess.run.
        with patch.object(subprocess, "run", original_run):
            result = collect(
                args.probe,
                [sys._base_executable, *command[1:]],
                child_env(args.repo, cs),
                args.repo,
                deadline,
                90,
            )
        stem = f"{current['iteration']:03d}-{current['workload']}"
        private_capture(args.private, stem, result)
        public_write(args.out, f"{stem}.stdout.log", result.stdout, secrets)
        public_write(args.out, f"{stem}.native.log", result.stderr, secrets)
        record = {
            **current,
            "returncode": result.returncode,
            "seconds": time.monotonic() - started,
            "original_assertions_passed": False,
        }
        records.append(record)
        record.update(native_accounting(result, args.tag))
        assert not record["second_chance"] and not record["timeout"]
        return result

    failure = None
    for iteration in range(args.repetitions):
        for name, execute in cases:
            current = {"iteration": iteration, "workload": name}
            before = len(records)
            captured = io.StringIO()
            try:
                with contextlib.redirect_stdout(captured), contextlib.redirect_stderr(captured):
                    with patch.object(subprocess, "run", run_child):
                        execute()
                assert len(records) == before + 1, "Expected exactly one actual test child"
                records[-1]["original_assertions_passed"] = True
            except (
                AssertionError,
                pytest.fail.Exception,
                subprocess.TimeoutExpired,
                TimeoutError,
                OSError,
            ) as error:
                failure = redact(str(error), secrets)
                if len(records) == before:
                    records.append(
                        {
                            **current,
                            "original_assertions_passed": False,
                            "incomplete_before_exit_record": True,
                        }
                    )
                records[-1]["failure"] = failure
            public_write(args.out, "children.json", json.dumps(records, indent=2), secrets)
            if failure is not None:
                break
        if failure is not None:
            break
        print(f"Completed iteration {iteration + 1}/{args.repetitions}", flush=True)
    summary = {
        name: {
            "actual_test_child_exit_records": sum(
                r["workload"] == name and "root_exit_hex" in r for r in records
            ),
            "passed": sum(
                r["workload"] == name and r["original_assertions_passed"] for r in records
            ),
            "root_exit_codes": dict(
                Counter(
                    r.get("root_exit_hex", "incomplete") for r in records if r["workload"] == name
                )
            ),
            "auxiliary_exit_records": sum(
                len(r.get("auxiliary_processes", [])) for r in records if r["workload"] == name
            ),
        }
        for name, _ in cases
    }
    passed = failure is None and len(records) == len(cases) * args.repetitions
    public_write(
        args.out,
        "acceptance.json",
        json.dumps(
            {
                "status": "passed" if passed else "failed_or_incomplete",
                "expected_test_children": len(cases) * args.repetitions,
                "normal_suite_exit": normal.returncode,
                "workloads": summary,
                "failure": failure,
                "replay_used": False,
            },
            indent=2,
        ),
        secrets,
    )
    if not passed:
        raise RuntimeError(
            "Candidate child acceptance failed or incomplete; inspect redacted artifacts"
        )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["verify", "self-test", "run", "publish-check"])
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--private", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument(
        "--manifest", type=Path, default=Path(__file__).with_name("approved-sources.json")
    )
    parser.add_argument("--probe", type=Path)
    parser.add_argument("--sql-version", choices=["SQL2022", "SQL2025"])
    parser.add_argument("--repetitions", type=int, default=100)
    args = parser.parse_args()
    args.repo = args.repo.resolve()
    args.private = args.private.resolve()
    args.out = args.out.resolve()
    if (
        args.private == args.out
        or args.private.is_relative_to(args.out)
        or args.out.is_relative_to(args.private)
    ):
        parser.error("Private and publish directories must be separate, non-nested paths")
    args.private.mkdir(parents=True, exist_ok=True)
    args.out.mkdir(parents=True, exist_ok=True)
    args.probe = args.probe or args.private / "bin" / "native_probe.exe"
    args.tag = f"{sys.version_info.major}{sys.version_info.minor}"
    secrets = [CANARY]
    secret_file = args.private / "sql-secret.json"
    if secret_file.exists():
        secrets.append(json.loads(secret_file.read_text(encoding="utf-8-sig"))["password"])
    try:
        if args.mode == "verify":
            public_write(
                args.out,
                "approved-source-check.json",
                json.dumps(verify_sources(args.repo, args.manifest), indent=2),
                secrets,
            )
        elif args.mode == "self-test":
            self_test(args, secrets)
        elif args.mode == "run":
            if args.repetitions != 100 or not args.sql_version:
                parser.error("Hosted acceptance requires --repetitions 100 and --sql-version")
            run_acceptance(args, secrets)
        else:
            for name in ("pdb-verify",):
                raw_path = args.private / f"{name}.raw.log"
                if raw_path.exists():
                    public_write(
                        args.out,
                        f"{name}.log",
                        raw_path.read_text(encoding="utf-8-sig", errors="replace"),
                        secrets,
                    )
            for stem in ("setup", "self-test", "runner"):
                raw_paths = [
                    args.private / f"{stem}.{stream}.raw.log" for stream in ("stdout", "stderr")
                ]
                text = "\n".join(
                    p.read_text(encoding="utf-8-sig", errors="replace")
                    for p in raw_paths
                    if p.exists()
                )
                if text:
                    public_write(args.out, f"{stem}-console.log", text, secrets)
            checked = publish_check(args.out, secrets)
            print(f"Artifact safety gate passed: {checked} allowlisted text files")
            print("##vso[task.setvariable variable=SHUTDOWN_ARTIFACTS_SAFE]true")
            return 0
        publish_check(args.out, secrets)
        print(f"{args.mode} completed")
        return 0
    except (
        AssertionError,
        OSError,
        RuntimeError,
        ValueError,
        ImportError,
        subprocess.SubprocessError,
        ET.ParseError,
    ) as error:
        message = redact(f"{type(error).__name__}: {error}", secrets)
        public_write(
            args.out,
            f"{args.mode}-failure.json",
            json.dumps({"status": "failed", "error": message}),
            secrets,
        )
        if args.mode == "run" and not (args.out / "acceptance.json").exists():
            public_write(
                args.out,
                "acceptance.json",
                json.dumps(
                    {
                        "status": "failed_or_incomplete",
                        "expected_test_children": 700,
                        "failure": message,
                        "replay_used": False,
                    }
                ),
                secrets,
            )
        print(message, file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
