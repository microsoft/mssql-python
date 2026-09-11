import importlib.util
import io
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
from types import SimpleNamespace

import pytest

HELPER = Path(__file__).resolve().parents[1] / "eng" / "scripts" / "setup_sql_container.py"
if not HELPER.is_file():
    pytest.skip("SQL setup contracts require a source checkout", allow_module_level=True)
spec = importlib.util.spec_from_file_location("sql_container_setup", HELPER)
sql_setup = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = sql_setup
spec.loader.exec_module(sql_setup)

PASSWORD = "Dummy-Secret-Canary!42"
OWNER = "123.owned-job"
IMAGE_ID = "sha256:" + "a" * 64


class Clock:
    def __init__(self):
        self.now = 0
        self.sleeps = []

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


class Docker:
    def __init__(self, clock, scenario="success"):
        self.clock = clock
        self.scenario = scenario
        self.calls = []
        self.created = 0
        self.identifier = None
        self.status = "created"
        self.owner = OWNER

    def count(self, command):
        return sum(args[0] == command for args, _, _ in self.calls)

    def run(self, args, timeout, *, env):
        assert timeout > 0
        args = list(args)
        if args[0] == "docker":
            args.pop(0)
            if args[:2] == ["--context", "colima"]:
                args = args[2:]
        self.calls.append((args, timeout, env))
        command = args[0]
        result = sql_setup.Result
        if command == "colima":
            return result(0, "")
        if command == "info":
            return result(1 if self.scenario == "daemon" else 0, "docker status")
        if command == "container":
            if self.scenario == "lookup-failure":
                return result(1, "cannot query daemon")
            return result(0, (self.identifier + "\n") if self.identifier else "")
        if command == "inspect":
            return result(0, f"{self.identifier}|{self.owner}|{self.status}|137|false|{IMAGE_ID}\n")
        if command == "pull":
            return result(1 if self.scenario == "pull-failure" else 0, "pull result")
        if command == "image":
            return result(0, f'{IMAGE_ID} ["mcr.microsoft.com/mssql/server@{IMAGE_ID}"]\n')
        if command == "create":
            self.created += 1
            if self.scenario == "create-once" and self.created == 1:
                return result(1, "creation failed")
            self.identifier = f"{self.created:064x}"
            self.status = "created"
            if self.scenario == "create-timeout" and self.created == 1:
                self.clock.sleep(timeout)
                raise sql_setup.SetupFailure("create timed out after creating the container")
            return result(0, self.identifier)
        if command == "start":
            self.status = "running"
            if self.scenario == "start-once" and self.created == 1:
                return result(1, "start failed")
            if self.scenario == "cancel":
                raise sql_setup.Cancelled(signal.SIGTERM)
            if self.scenario == "interrupt-exit":
                return result(130, "interrupted")
            if self.scenario == "dead" or (self.scenario == "dead-once" and self.created == 1):
                self.status = "exited"
            return result(0, "")
        if command == "exec":
            if self.scenario == "hung":
                self.clock.sleep(timeout)
                raise sql_setup.SetupFailure("exec timed out")
            if self.scenario == "timeout":
                self.clock.sleep(min(5, timeout))
                return result(1, "not ready")
            if self.scenario == "no-sqlcmd":
                return result(127, "missing sqlcmd")
            if self.scenario == "database" and args[-1] == "CREATE DATABASE TestDB":
                return result(1, "SQL initialization error")
            return result(0, "1")
        if command == "logs":
            return result(0, f"fatal header\nPASSWORD={PASSWORD}\nend of log\n")
        if command == "rm":
            assert args[-1] == self.identifier
            if self.scenario == "remove-failure":
                return result(1, "removal denied")
            self.identifier = None
            return result(0, "")
        raise AssertionError(args)


@pytest.fixture
def setup_factory(monkeypatch):
    def factory(scenario="success", *, colima=False, database=True):
        clock = Clock()
        monkeypatch.setattr(sql_setup, "time", clock)
        args = SimpleNamespace(
            name="sqlserver",
            owner=OWNER,
            image="mcr.microsoft.com/mssql/server:2025-latest",
            colima=colima,
            cleanup=False,
            database="TestDB" if database else None,
        )
        docker = Docker(clock, scenario)
        return sql_setup.SqlSetup(args, PASSWORD, docker), docker, clock

    return factory


@pytest.mark.parametrize("database", [True, False])
def test_first_success_and_secret_environment(setup_factory, monkeypatch, capsys, database):
    monkeypatch.setenv("MSSQL_SA_PASSWORD", "wrong-inherited-password")
    monkeypatch.setenv("SQLCMDPASSWORD", "another-wrong-password")
    setup, docker, _ = setup_factory(database=database)
    setup.setup()
    assert docker.count("create") == docker.count("start") == docker.count("pull") == 1
    assert docker.count("rm") == 0
    assert docker.count("exec") == (2 if database else 1)
    for args, timeout, env in docker.calls:
        assert PASSWORD not in " ".join(args)
        assert "-P" not in args
        assert env["MSSQL_SA_PASSWORD"] == env["SQLCMDPASSWORD"] == PASSWORD
        assert "DB_CONNECTION_STRING" not in env
        if args[0] == "exec":
            assert args[1:3] == ["--env", "SQLCMDPASSWORD"]
            assert "-b" in args and args[args.index("-l") + 1] == "5"
            assert timeout <= 30
    assert "ready on first attempt" in capsys.readouterr().out


@pytest.mark.parametrize("scenario", ["create-once", "create-timeout", "start-once", "dead-once"])
def test_first_failure_recovers_exactly_once(setup_factory, capsys, scenario):
    setup, docker, clock = setup_factory(scenario, colima=True)
    setup.setup()
    assert docker.created == 2
    assert docker.count("pull") == docker.count("colima") == 1
    assert clock.sleeps.count(5) == 1
    assert docker.count("exec") == 2
    assert docker.identifier == f"{2:064x}"
    if scenario != "create-once":
        actions = [args[0] for args, _, _ in docker.calls]
        assert actions.index("logs") < actions.index("rm") < len(actions) - 1
        assert docker.count("rm") == 1
    out = capsys.readouterr().out
    assert PASSWORD not in out
    assert "attempt 1/2" in out and "attempt 2/2" in out
    assert "recovered on second attempt" in out


@pytest.mark.parametrize("scenario", ["dead", "database", "pull-failure", "hung", "timeout"])
def test_permanent_failure_stops_after_two_attempts(setup_factory, scenario, capsys):
    setup, docker, clock = setup_factory(scenario)
    with pytest.raises(sql_setup.SetupFailure, match="no further attempts"):
        setup.setup()
    assert docker.count("pull") == (2 if scenario == "pull-failure" else 1)
    assert docker.created == (0 if scenario == "pull-failure" else 2)
    assert docker.identifier is None
    assert docker.count("rm") == docker.created
    assert clock.sleeps.count(5) >= 1
    assert clock.now <= 1260
    if scenario == "dead":
        assert docker.count("exec") == 0
        assert clock.now == 5
    if scenario == "timeout":
        assert 240 <= clock.now <= 246
    assert "recovered" not in capsys.readouterr().out


@pytest.mark.parametrize("status", ["running", "exited"])
def test_preexisting_owned_container_is_not_accepted(setup_factory, status):
    setup, docker, _ = setup_factory()
    old_id = "e" * 64
    docker.identifier, docker.status = old_id, status
    setup.setup()
    actions = [args[0] for args, _, _ in docker.calls]
    assert actions.index("logs") < actions.index("rm") < actions.index("create")
    assert docker.created == 1
    assert docker.identifier != old_id
    assert next(args[-1] for args, _, _ in docker.calls if args[0] == "rm") == old_id


@pytest.mark.parametrize("scenario", ["daemon", "lookup-failure", "remove-failure"])
def test_unsafe_cleanup_or_daemon_failure_is_terminal(setup_factory, scenario):
    setup, docker, _ = setup_factory(scenario)
    if scenario == "remove-failure":
        docker.identifier = "e" * 64
    with pytest.raises(sql_setup.SetupFailure):
        setup.setup()
    assert docker.created == 0
    assert docker.count("pull") == 0


@pytest.mark.parametrize("owner", ["", "another-job"])
def test_foreign_container_is_never_read_removed_or_reused(setup_factory, owner):
    setup, docker, _ = setup_factory()
    docker.identifier, docker.owner = "e" * 64, owner
    with pytest.raises(sql_setup.SetupFailure):
        setup.setup()
    assert docker.count("logs") == docker.count("rm") == docker.created == 0


def test_missing_sqlcmd_is_not_retried(setup_factory):
    setup, docker, _ = setup_factory("no-sqlcmd")
    with pytest.raises(sql_setup.SetupFailure):
        setup.setup()
    assert docker.created == docker.count("rm") == 1


@pytest.mark.parametrize("scenario, code", [("cancel", 143), ("interrupt-exit", 130)])
def test_cancellation_propagates_and_cleans_without_retry(
    setup_factory, monkeypatch, scenario, code
):
    setup, docker, _ = setup_factory(scenario)
    monkeypatch.setenv("DB_PASSWORD", PASSWORD)
    monkeypatch.setattr(sql_setup, "SqlSetup", lambda *args: setup)
    monkeypatch.setattr(sql_setup.signal, "signal", lambda *args: None)
    assert (
        sql_setup.main(["--name", "sqlserver", "--owner", OWNER, "--image", setup.args.image])
        == code
    )
    assert docker.created == docker.count("rm") == 1
    assert docker.identifier is None


@pytest.mark.parametrize("value", ["", "two\nlines"])
def test_bad_secret_is_terminal_before_docker(monkeypatch, value):
    monkeypatch.setenv("DB_PASSWORD", value)
    monkeypatch.setattr(sql_setup.signal, "signal", lambda *args: None)
    assert (
        sql_setup.main(
            [
                "--name",
                "sqlserver",
                "--owner",
                OWNER,
                "--image",
                "mcr.microsoft.com/mssql/server:2025-latest",
            ]
        )
        == 1
    )


def test_cleanup_absent_container_is_success(setup_factory):
    setup, docker, _ = setup_factory()
    setup.args.cleanup = True
    setup.setup()
    assert docker.count("info") == 1
    assert docker.created == docker.count("rm") == 0


def test_stream_capture_redacts_across_chunks_and_retains_crash_header():
    capture = sql_setup.SafeCapture(PASSWORD, limit=1024)
    content = "FATAL HEADER\n" + "a" * 4068 + PASSWORD + "\n" + ("line\n" * 1000)
    content += "x" * 9000 + PASSWORD + "\nEND\n"
    capture.read(io.BytesIO(content.encode()))
    out = capture.output()
    assert out.startswith("FATAL HEADER")
    assert out.endswith("END\n")
    assert PASSWORD not in out
    assert "overlong diagnostic line omitted" in out
    assert "truncated" in out
    assert len(out) < 2200


def test_real_command_timeout_is_bounded_and_redacted():
    start = time.monotonic()
    with pytest.raises(sql_setup.SetupFailure, match="timed out") as error:
        sql_setup.Commands(PASSWORD).run(
            [
                sys.executable,
                "-c",
                "import os,signal,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); "
                "print(os.environ['SQLCMDPASSWORD'], flush=True); time.sleep(60)",
            ],
            0.5,
            env={**os.environ, "SQLCMDPASSWORD": PASSWORD},
        )
    assert time.monotonic() - start < 1.5
    assert PASSWORD not in str(error.value)
    assert "[REDACTED]" in str(error.value)


def test_missing_command_is_an_explicit_terminal_failure(tmp_path):
    with pytest.raises(sql_setup.SetupFailure, match="Cannot launch") as error:
        sql_setup.Commands(PASSWORD).run([str(tmp_path / "missing-docker-command")], 1)
    assert error.value.retryable is False


def test_cancellation_during_backoff_does_not_start_second_attempt(setup_factory):
    setup, docker, clock = setup_factory("dead")

    def interrupt(_seconds):
        raise sql_setup.Cancelled(signal.SIGINT)

    clock.sleep = interrupt
    with pytest.raises(sql_setup.Cancelled):
        setup.setup()
    assert docker.created == docker.count("rm") == 1
    assert docker.identifier is None


def test_commands_clamp_to_phase_and_total_deadlines(setup_factory):
    setup, docker, clock = setup_factory()
    setup.phase_deadline = 3
    setup.docker_command("info", timeout=15)
    assert docker.calls[-1][1] == 3
    setup.deadline = 2
    setup.docker_command("info", timeout=15)
    assert docker.calls[-1][1] == 2
    clock.now = 2
    count = len(docker.calls)
    with pytest.raises(sql_setup.SetupFailure, match="deadline exhausted"):
        setup.docker_command("info")
    assert len(docker.calls) == count


def test_cleanup_deadline_exhaustion_does_not_claim_removal(setup_factory, capsys):
    setup, docker, clock = setup_factory()
    docker.identifier = "e" * 64
    setup.deadline = 10
    original = docker.run

    def slow_lookup(args, timeout, *, env):
        result = original(args, timeout, env=env)
        clock.sleep(min(6, timeout))
        return result

    docker.run = slow_lookup
    with pytest.raises(sql_setup.SetupFailure, match="deadline exhausted"):
        setup.cleanup()
    assert clock.now == 10
    assert docker.count("rm") == 0
    assert docker.identifier is not None
    assert "cleanup complete" not in capsys.readouterr().out


def test_cleanup_only_has_its_own_global_deadline(setup_factory):
    setup, docker, clock = setup_factory()
    setup.args.cleanup = True
    cleanup = sql_setup.SqlSetup(setup.args, PASSWORD, docker)
    assert cleanup.deadline - clock.now == 115


@pytest.mark.parametrize("colima, cap, total", [(False, 600, 1260), (True, 900, 2460)])
def test_total_attempt_budget_reserves_cleanup(setup_factory, colima, cap, total):
    setup, docker, clock = setup_factory(colima=colima)
    assert setup.deadline == total
    attempts = []
    cleanups = []

    def preflight():
        clock.sleep((600 if colima else 0) + 15)

    original = docker.run

    def bounded_info(args, timeout, *, env):
        result = original(args, timeout, env=env)
        clock.sleep(timeout)
        return result

    def exhaust_attempt():
        remaining = setup.phase_deadline - clock.now
        attempts.append(remaining)
        clock.sleep(remaining)
        raise sql_setup.SetupFailure("attempt deadline exhausted")

    def cleanup():
        cleanups.append(setup.phase_deadline - clock.now)
        clock.sleep(100)

    setup.preflight = preflight
    docker.run = bounded_info
    setup.attempt = exhaust_attempt
    setup.cleanup = cleanup
    with pytest.raises(sql_setup.SetupFailure, match="no further attempts"):
        setup.setup()
    assert attempts == [cap - 100, cap - 100]
    assert cleanups == [100, 100]
    assert clock.now == (600 if colima else 0) + 2 * cap + 35
    assert clock.now <= total
    assert docker.created == 0


def _linux_process_state(pid, proc_root=Path("/proc")):
    try:
        stat = (proc_root / str(pid) / "stat").read_text(encoding="utf-8", errors="replace")
    except (FileNotFoundError, ProcessLookupError):
        return None
    # comm may contain spaces, newlines and parentheses; state follows its last ')'.
    comm, separator, fields = stat.rpartition(")")
    fields = fields.split()
    assert separator and comm.startswith(f"{pid} (") and fields, "Malformed process stat"
    assert len(fields[0]) == 1, "Malformed process state"
    return fields[0]


@pytest.mark.parametrize(
    "comm, state", [("worker", "R"), ("odd) (worker", "Z"), ("worker\nwith ) space", "S")]
)
def test_linux_process_state_without_ps(tmp_path, monkeypatch, comm, state):
    proc = tmp_path / "123"
    proc.mkdir()
    (proc / "stat").write_text(f"123 ({comm}) {state} 1 2 3\n", encoding="utf-8")
    monkeypatch.setenv("PATH", "")
    assert _linux_process_state(123, tmp_path) == state


def test_linux_process_state_when_already_reaped(tmp_path):
    assert _linux_process_state(123, tmp_path) is None


def test_linux_process_state_handles_reaping_during_read(tmp_path, monkeypatch):
    def reaped(*args, **kwargs):
        raise ProcessLookupError("Process exited during stat read")

    monkeypatch.setattr(Path, "read_text", reaped)
    assert _linux_process_state(123, tmp_path) is None


def test_linux_process_state_does_not_mask_permission_errors(tmp_path, monkeypatch):
    def denied(*args, **kwargs):
        raise PermissionError("Process stat is not readable")

    monkeypatch.setattr(Path, "read_text", denied)
    with pytest.raises(PermissionError):
        _linux_process_state(123, tmp_path)


@pytest.mark.skipif(os.name != "posix", reason="Unix descendant process-group contract")
def test_exited_launcher_descendant_is_terminated_without_touching_other_groups(
    tmp_path, monkeypatch
):
    if sys.platform.startswith("linux"):
        monkeypatch.setenv("PATH", "")
    ready = tmp_path / "descendant"
    child = (
        "import os,pathlib,signal,time; "
        "signal.signal(signal.SIGTERM,signal.SIG_IGN); "
        f"pathlib.Path({str(ready)!r}).write_text(str(os.getpid())); time.sleep(60)"
    )
    launcher = (
        "import subprocess,sys,time,pathlib; "
        f"subprocess.Popen([sys.executable,'-c',{child!r}]); "
        f"ready=pathlib.Path({str(ready)!r}); "
        "\nwhile not ready.exists(): time.sleep(0.01)\n"
    )
    unrelated = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(60)"], start_new_session=True
    )
    try:
        start = time.monotonic()
        with pytest.raises(sql_setup.SetupFailure, match="descendants holding output"):
            sql_setup.Commands("").run([sys.executable, "-c", launcher], 5)
        assert time.monotonic() - start < 6
        child_pid = int(ready.read_text())
        reaped_deadline = time.monotonic() + 2
        while True:
            if sys.platform.startswith("linux"):
                state = _linux_process_state(child_pid)
            else:
                result = subprocess.run(
                    ["/bin/ps", "-o", "stat=", "-p", str(child_pid)],
                    capture_output=True,
                    text=True,
                    timeout=2,
                )
                assert result.returncode in (0, 1) and not result.stderr, result.stderr
                state = result.stdout.strip()
            if not state or state.startswith("Z") or time.monotonic() >= reaped_deadline:
                break
            time.sleep(0.01)
        assert not state or state.startswith("Z")
        assert unrelated.poll() is None
    finally:
        unrelated.terminate()
        unrelated.wait(timeout=5)


@pytest.mark.skipif(os.name != "posix", reason="Unix process-group cancellation contract")
@pytest.mark.parametrize("signum", [signal.SIGINT, signal.SIGTERM])
def test_real_signal_stops_owned_command(tmp_path, signum):
    ready = tmp_path / "ready"
    child = (
        "import os, pathlib, time; "
        f"pathlib.Path({str(ready)!r}).write_text(str(os.getpid())); time.sleep(60)"
    )
    script = (
        "import importlib.util, pathlib, signal, sys\n"
        f"spec=importlib.util.spec_from_file_location('helper', {str(HELPER)!r})\n"
        "m=importlib.util.module_from_spec(spec); sys.modules['helper']=m; spec.loader.exec_module(m)\n"
        "def cancel(sig, frame): raise m.Cancelled(sig)\n"
        "signal.signal(signal.SIGINT,cancel); signal.signal(signal.SIGTERM,cancel)\n"
        "try:\n"
        f" m.Commands('').run([sys.executable,'-c',{child!r}],60)\n"
        "except m.Cancelled as exc: sys.exit(128+exc.signum)\n"
    )
    process = subprocess.Popen([sys.executable, "-c", script])
    try:
        deadline = time.monotonic() + 10
        while not ready.exists() and process.poll() is None and time.monotonic() < deadline:
            time.sleep(0.01)
        assert ready.exists()
        child_pid = int(ready.read_text())
        process.send_signal(signum)
        assert process.wait(timeout=8) == 128 + signum
        with pytest.raises(ProcessLookupError):
            os.kill(child_pid, 0)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
