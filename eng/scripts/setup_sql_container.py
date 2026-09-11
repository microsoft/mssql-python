#!/usr/bin/env python3
"""Create a job-owned SQL Server container, retrying SQL setup once.

Invoke on the Docker host with --name, --image and --owner; DB_PASSWORD is
required for setup and is never an argument. Use --database TestDB on Linux
test legs, --colima on macOS, and --cleanup for the always-running final step.
Cleanup refuses containers without the matching owner label.

Only SQL pull/create/start/readiness/database setup is retried. Colima starts
once. Configuration, ownership, preflight and cleanup failures are terminal.
Linux/macOS attempts are bounded at 600/900 seconds including diagnostics and
cleanup; entire invocations at 1260/2460 seconds including macOS VM startup.
Command budgets include termination/output-drain grace. Cleanup-only uses a
115-second deadline. OS scheduling/host loss can defeat cooperative deadlines;
the pipeline also enforces outer task limits.

Diagnostics retain redacted beginning/end excerpts, not dumps or full inspect
output. Timeout/cancellation stops only subprocesses created by this helper.
Hard host loss can still prevent cleanup; the pipeline also runs --cleanup.
"""

import argparse
import codecs
from dataclasses import dataclass
import json
import os
import re
import signal
import subprocess
import sys
import threading
import time

OWNER_LABEL = "com.microsoft.mssql-python.ci-owner"
STATE_FORMAT = (
    '{{.Id}}|{{index .Config.Labels "' + OWNER_LABEL + '"}}|'
    "{{.State.Status}}|{{.State.ExitCode}}|{{.State.OOMKilled}}|{{.Image}}"
)
SQLCMD = "/opt/mssql-tools18/bin/sqlcmd"


class SetupFailure(Exception):
    def __init__(self, message, *, retryable=True):
        super().__init__(message)
        self.retryable = retryable


class Cancelled(BaseException):
    def __init__(self, signum):
        self.signum = signum


def redact(text, password):
    if password:
        text = text.replace(password, "[REDACTED]")
    return re.sub(
        r"(?i)\b(?:DB_PASSWORD|MSSQL_SA_PASSWORD|SQLCMDPASSWORD|PASSWORD|PWD)"
        r"\s*[:=]\s*(?:\"[^\"]*\"|'[^']*'|[^;\r\n]*)",
        "[credential redacted]",
        text,
    )


class SafeCapture:
    """Redact complete lines before retaining bounded head/tail excerpts."""

    def __init__(self, password, limit=32768):
        self.password = password
        self.limit = limit
        self.head = ""
        self.tail = ""
        self.total = 0

    def add(self, line):
        safe = redact(line, self.password)
        self.total += len(safe)
        remaining = self.limit - len(self.head)
        self.head += safe[:remaining]
        self.tail = (self.tail + safe[remaining:])[-self.limit :]

    def read(self, stream):
        decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
        pending = ""
        omitted = False
        while True:
            chunk = stream.read(4096)
            text = decoder.decode(chunk, final=not chunk)
            for part in text.splitlines(keepends=True):
                pending += part
                if len(pending) > 8192:
                    if not omitted:
                        self.add("[overlong diagnostic line omitted]\n")
                    pending = ""
                    omitted = True
                if part.endswith(("\n", "\r")):
                    if not omitted:
                        self.add(pending)
                    pending = ""
                    omitted = False
            if not chunk:
                if pending and not omitted:
                    self.add(pending)
                return

    def output(self):
        marker = "\n[diagnostic output truncated]\n" if self.total > 2 * self.limit else ""
        return self.head + marker + self.tail


@dataclass
class Result:
    returncode: int
    output: str


class Commands:
    def __init__(self, password):
        self.password = password

    @staticmethod
    def stop(process, deadline):
        # start_new_session gives each command its own group. The launcher may
        # already be reaped while a descendant still holds its output pipe.
        try:
            if os.name == "posix":
                os.killpg(process.pid, signal.SIGTERM)
            elif process.poll() is None:
                process.terminate()
        except ProcessLookupError:
            pass
        try:
            process.wait(timeout=max(0, min(1, (deadline - time.monotonic()) / 2)))
        except subprocess.TimeoutExpired:
            pass
        try:
            if os.name == "posix":
                os.killpg(process.pid, signal.SIGKILL)
            elif process.poll() is None:
                process.kill()
        except ProcessLookupError:
            pass
        try:
            process.wait(timeout=max(0, min(1, deadline - time.monotonic())))
        except subprocess.TimeoutExpired:
            return False
        return True

    def run(self, args, timeout, *, env=None):
        if timeout <= 0:
            raise SetupFailure("SQL setup deadline exhausted")
        deadline = time.monotonic() + timeout
        grace = min(4, timeout / 2)
        capture = SafeCapture(self.password)
        try:
            process = subprocess.Popen(
                args,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                env=env,
                start_new_session=os.name == "posix",
            )
        except OSError:
            raise SetupFailure("Cannot launch required setup command", retryable=False) from None
        reader = threading.Thread(target=capture.read, args=(process.stdout,), daemon=True)
        reader.start()
        timed_out = False
        descendant_output = False
        reaped = True
        try:
            try:
                process.wait(timeout=max(0, deadline - time.monotonic() - grace))
            except subprocess.TimeoutExpired:
                timed_out = True
        finally:
            if process.poll() is None:
                reaped = self.stop(process, deadline)
            else:
                reader.join(timeout=max(0, min(0.2, (deadline - time.monotonic()) / 4)))
                if reader.is_alive():
                    descendant_output = True
                    reaped = self.stop(process, deadline)
            reader.join(timeout=max(0, deadline - time.monotonic()))
            if not reader.is_alive():
                process.stdout.close()
            if not reaped or reader.is_alive():
                print("[sql] Command teardown incomplete within its deadline", file=sys.stderr)
        if not reaped:
            raise SetupFailure("Setup command could not be reaped", retryable=False)
        if reader.is_alive():
            raise SetupFailure("Setup command output did not close", retryable=False)
        if descendant_output:
            raise SetupFailure("Setup command left descendants holding output", retryable=False)
        if timed_out:
            raise SetupFailure("Setup command timed out\n" + capture.output())
        return Result(process.returncode, capture.output())


@dataclass
class Container:
    identifier: str
    status: str
    exit_code: str
    oom_killed: str
    image: str


class SqlSetup:
    def __init__(self, args, password, commands=None):
        self.args = args
        self.password = password
        self.commands = commands or Commands(password)
        self.deadline = time.monotonic() + (115 if args.cleanup else 2460 if args.colima else 1260)
        self.phase_deadline = self.deadline
        self.container = None
        self.image_id = None
        self.docker = ["docker"] + (["--context", "colima"] if args.colima else [])
        self.env = os.environ.copy()
        for key in ("DB_PASSWORD", "DB_CONNECTION_STRING", "MSSQL_SA_PASSWORD", "SQLCMDPASSWORD"):
            self.env.pop(key, None)
        self.env.update(MSSQL_SA_PASSWORD=password, SQLCMDPASSWORD=password)

    def log(self, message):
        print("[sql] " + redact(message, self.password), flush=True)

    def command(self, args, timeout=15, *, check=True, deadline=None):
        end = min(self.deadline, self.phase_deadline if deadline is None else deadline)
        remaining = min(timeout, end - time.monotonic())
        if remaining <= 0:
            raise SetupFailure("SQL setup deadline exhausted")
        result = self.commands.run(args, remaining, env=self.env)
        if result.returncode in (-signal.SIGINT, -signal.SIGTERM, 130, 143):
            raise Cancelled(
                signal.SIGINT if result.returncode in (-signal.SIGINT, 130) else signal.SIGTERM
            )
        if check and result.returncode != 0:
            raise SetupFailure(f"Setup command failed (exit {result.returncode})\n{result.output}")
        return result

    def docker_command(self, *args, **kwargs):
        return self.command(self.docker + list(args), **kwargs)

    def find_owned(self, *, deadline=None):
        result = self.docker_command(
            "container",
            "ls",
            "--all",
            "--no-trunc",
            "--filter",
            f"name=^/{self.args.name}$",
            "--format",
            "{{.ID}}",
            deadline=deadline,
        )
        identifiers = result.output.split()
        if not identifiers:
            return None
        if len(identifiers) != 1 or not re.fullmatch(r"[0-9a-f]{64}", identifiers[0]):
            raise SetupFailure("Unexpected exact-name container lookup result", retryable=False)
        result = self.docker_command(
            "inspect", "--format", STATE_FORMAT, identifiers[0], deadline=deadline
        )
        fields = result.output.strip().split("|")
        if len(fields) != 6 or fields[0] != identifiers[0]:
            raise SetupFailure("Invalid selected container state", retryable=False)
        if fields[1] != self.args.owner:
            raise SetupFailure(
                "Container name is owned by another job or unlabelled", retryable=False
            )
        return Container(fields[0], fields[2], fields[3], fields[4], fields[5])

    def diagnostics(self, container, *, deadline):
        self.log(
            f"Container {container.identifier}: status={container.status} "
            f"exit={container.exit_code} OOMKilled={container.oom_killed} image={container.image}"
        )
        try:
            result = self.docker_command(
                "logs", container.identifier, timeout=20, check=False, deadline=deadline
            )
            self.log("Container log excerpts:\n" + result.output)
            if result.returncode:
                self.log(f"Container logs unavailable (exit {result.returncode})")
        except SetupFailure as exc:
            self.log(f"Container logs unavailable: {exc}")

    def remove(self, container, *, deadline):
        self.docker_command("rm", "--force", container.identifier, timeout=30, deadline=deadline)
        remaining = self.find_owned(deadline=deadline)
        if remaining is not None:
            raise SetupFailure("Owned container still exists after removal", retryable=False)
        self.container = None

    def cleanup(self, *, evidence=True):
        # Include lookup/ownership checks and removal verification in addition
        # to the log and rm deadlines.
        self.phase_deadline = min(self.deadline, time.monotonic() + 100)
        container = self.find_owned()
        if container is None:
            self.container = None
            return
        if evidence:
            self.diagnostics(
                container, deadline=min(self.phase_deadline - 30, time.monotonic() + 20)
            )
        self.remove(container, deadline=self.phase_deadline)

    def preflight(self):
        if self.args.colima and not self.args.cleanup:
            self.log("Starting Colima once (outside SQL retry)")
            self.command(["colima", "start", "--cpu", "4", "--memory", "8", "--disk", "50"], 600)
        self.docker_command("info", "--format", "{{.ServerVersion}}")

    def acquire_image(self):
        if self.image_id is not None:
            return
        self.docker_command(
            "pull",
            "--quiet",
            "--platform",
            "linux/amd64",
            self.args.image,
            timeout=600 if self.args.colima else 300,
        )
        result = self.docker_command(
            "image", "inspect", "--format", "{{.Id}} {{json .RepoDigests}}", self.args.image
        )
        parts = result.output.strip().split(" ", 1)
        if len(parts) != 2 or not re.fullmatch(r"sha256:[0-9a-f]{64}", parts[0]):
            raise SetupFailure("Invalid resolved SQL image", retryable=False)
        try:
            digests = json.loads(parts[1])
        except json.JSONDecodeError:
            raise SetupFailure("Invalid SQL image digest metadata", retryable=False) from None
        if not isinstance(digests, list) or not all(isinstance(item, str) for item in digests):
            raise SetupFailure("Missing SQL image digest metadata", retryable=False)
        self.image_id = parts[0]
        self.log(f"SQL image {self.image_id}; repository digests={json.dumps(digests)}")

    def sql(self, query, *, timeout=15, query_timeout=5, deadline=None):
        return self.docker_command(
            "exec",
            "--env",
            "SQLCMDPASSWORD",
            self.container.identifier,
            SQLCMD,
            "-S",
            "localhost",
            "-U",
            "SA",
            "-C",
            "-b",
            "-l",
            "5",
            "-t",
            str(query_timeout),
            "-Q",
            query,
            timeout=timeout,
            check=False,
            deadline=deadline,
        )

    def attempt(self):
        stale = self.find_owned()
        if stale is not None:
            self.log("Removing pre-existing same-job container before fresh setup")
            self.diagnostics(stale, deadline=min(self.phase_deadline, time.monotonic() + 20))
            try:
                self.remove(stale, deadline=min(self.phase_deadline, time.monotonic() + 30))
            except SetupFailure as exc:
                raise SetupFailure(str(exc), retryable=False) from None
        self.acquire_image()
        self.docker_command(
            "create",
            "--name",
            self.args.name,
            "--label",
            f"{OWNER_LABEL}={self.args.owner}",
            "--platform",
            "linux/amd64",
            "--env",
            "ACCEPT_EULA=Y",
            "--env",
            "MSSQL_SA_PASSWORD",
            "-p",
            "1433:1433",
            self.image_id,
            timeout=30,
        )
        self.container = self.find_owned()
        if self.container is None:
            raise SetupFailure("Created SQL container was not found")
        self.docker_command("start", self.container.identifier, timeout=30)
        ready_deadline = min(
            self.phase_deadline, time.monotonic() + (180 if self.args.colima else 120)
        )
        last_output = ""
        while time.monotonic() < ready_deadline:
            current = self.find_owned(deadline=ready_deadline)
            if current is None or current.identifier != self.container.identifier:
                raise SetupFailure("SQL container disappeared or was replaced", retryable=False)
            if current.status != "running":
                raise SetupFailure(
                    f"SQL container exited before readiness (status={current.status}, "
                    f"exit={current.exit_code}, OOMKilled={current.oom_killed})"
                )
            probe = self.sql("SELECT 1", deadline=ready_deadline)
            if probe.returncode == 0:
                if self.args.database:
                    result = self.sql("CREATE DATABASE TestDB", timeout=30, query_timeout=15)
                    if result.returncode != 0:
                        raise SetupFailure("TestDB initialization failed\n" + result.output)
                return
            if probe.returncode in (126, 127):
                raise SetupFailure("Required sqlcmd executable is unavailable", retryable=False)
            last_output = probe.output
            time.sleep(max(0, min(2, ready_deadline - time.monotonic())))
        raise SetupFailure("SQL readiness deadline exhausted\n" + last_output)

    def setup(self):
        self.preflight()
        if self.args.cleanup:
            self.cleanup(evidence=False)
            self.log("Owned SQL container cleanup complete (or already absent)")
            return
        for number in (1, 2):
            self.log(f"SQL setup attempt {number}/2")
            attempt_end = min(
                self.deadline,
                time.monotonic() + (900 if self.args.colima else 600),
            )
            self.phase_deadline = attempt_end - 100
            try:
                self.attempt()
            except SetupFailure as exc:
                self.log(f"Attempt {number}/2 failed: {exc}")
                self.phase_deadline = attempt_end
                try:
                    self.cleanup()
                except SetupFailure as cleanup_error:
                    raise SetupFailure(
                        f"Cannot safely recover/clean up: {cleanup_error}", retryable=False
                    ) from None
                if not exc.retryable or number == 2:
                    raise SetupFailure("SQL setup failed; no further attempts", retryable=False)
                self.phase_deadline = self.deadline
                self.docker_command("info", "--format", "{{.ServerVersion}}")
                if self.deadline - time.monotonic() < 5:
                    raise SetupFailure("SQL setup deadline exhausted before retry", retryable=False)
                self.log("Retrying SQL setup only after 5 seconds")
                time.sleep(5)
            else:
                outcome = "ready on first attempt" if number == 1 else "recovered on second attempt"
                self.log(f"SQL Server {outcome}")
                return


def arguments(argv):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--name", required=True)
    parser.add_argument("--owner", required=True)
    parser.add_argument("--image")
    parser.add_argument("--database", choices=("TestDB",))
    parser.add_argument("--colima", action="store_true")
    parser.add_argument("--cleanup", action="store_true")
    args = parser.parse_args(argv)
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", args.name):
        raise SetupFailure("Invalid SQL container name", retryable=False)
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]*", args.owner):
        raise SetupFailure("Invalid SQL container owner", retryable=False)
    if not args.cleanup and (
        not args.image
        or not re.fullmatch(r"mcr\.microsoft\.com/mssql/server:[A-Za-z0-9_.-]+", args.image)
    ):
        raise SetupFailure(
            "An explicit supported SQL Server image tag is required", retryable=False
        )
    return args


def main(argv=None):
    password = os.environ.get("DB_PASSWORD", "")
    setup = None

    def cancel(signum, _frame):
        raise Cancelled(signum)

    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, cancel)
    try:
        args = arguments(argv)
        if not args.cleanup and (not password or "\n" in password or "\r" in password):
            raise SetupFailure("DB_PASSWORD must be set to a single-line value", retryable=False)
        setup = SqlSetup(args, password)
        setup.setup()
        return 0
    except Cancelled as exc:
        for signum in (signal.SIGINT, signal.SIGTERM):
            signal.signal(signum, signal.SIG_IGN)
        print("[sql] Setup cancelled; no retry", flush=True)
        if setup is not None:
            try:
                setup.cleanup()
            except SetupFailure as cleanup_error:
                setup.log(f"Cancellation cleanup failed: {cleanup_error}")
        return 128 + exc.signum
    except SetupFailure as exc:
        print("[sql] " + redact(str(exc), password), file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
