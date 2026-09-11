#!/usr/bin/env python3
"""Create a job-owned SQL Server container, retrying SQL setup once.

Invoke on the Docker host with --name, --image and --owner; DB_PASSWORD is
required for setup and is never an argument. Use --database TestDB on Linux
test legs, --colima on macOS, and --cleanup for the always-running final step.
Cleanup refuses containers without the matching owner label.

Only SQL lookup/pull/create/start/readiness/database setup is retried. Colima
starts once. The owned-container lookup also checks Docker availability, so
transient read-only failures can consume the same two attempts without creating
or removing anything. Missing tools, invalid configuration/lookup results,
ownership conflicts and unsafe cleanup are terminal.
Linux/macOS attempts are bounded at 600/900 seconds including diagnostics and
cleanup; entire invocations at 1260/2460 seconds including macOS VM startup.
Command budgets include termination/output-drain grace. Cleanup-only uses a
115-second deadline. OS scheduling/host loss can defeat cooperative deadlines;
the pipeline also enforces outer task limits.

Readiness polls for 120/180 seconds (Linux/macOS), then performs one final
state/query check bounded at 45 seconds, for at most 165/225 seconds total.
Both are clamped to the remaining attempt/global budget, preserving cleanup.

Docker diagnostics request only the last 30 minutes and at most 5000 lines,
within 20 seconds. Redacted output retains 32768-character head/tail excerpts
plus at most 8192 characters of fatal/Reason context, not complete history.
No dumps or full inspect output are collected. Timeout/cancellation targets only
each command's original group and still-owned unreaped child, with at most four
seconds of teardown within the original command budget. Hard host loss can
still prevent cleanup; the pipeline also runs --cleanup.
Permission or identity failures are reported as incomplete, nonretryable
teardown without replacing the earliest failure or cancellation. On POSIX this
helper is the sole waiter: waitid(WNOWAIT) observes exit without releasing the
leader's PID before the final possible group signal. The child is then reaped
once. Default SIGCHLD handling and non-reaping APIs are required; lost wait
ownership forbids further signalling. This does not protect against arbitrary
external code stealing wait statuses. Departed groups are never followed.

Colima is a daemon launcher, not a finite-output command. Its direct exit is
captured through a private anonymous temporary file, without requiring EOF or
stopping successful background processes. Only a fixed-size startup snapshot
is read/redacted; Docker/SQL readiness is checked separately. The parent closes
its descriptor on every outcome. Daemons can retain the unlinked backing inode
until their descriptors close or the hosted job ends: raw backing storage is
not hard-capped. This contract is intended for job-scoped hosted Colima startup,
not a general-purpose persistent daemon logging service.
"""

import argparse
import codecs
from dataclasses import dataclass
import io
import json
import os
import re
import signal
import subprocess
import sys
import tempfile
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


class SetupTimeout(SetupFailure):
    pass


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
        self.fatal_context = ""
        self.context_remaining = 0

    def add(self, line):
        safe = redact(line, self.password)
        if re.search(r"\bfatal\b|\bReason:", safe, re.IGNORECASE):
            self.context_remaining = 12
        if self.context_remaining:
            remaining = min(8192, self.limit // 4) - len(self.fatal_context)
            self.fatal_context += safe[:remaining]
            self.context_remaining -= 1
        self.total += len(safe)
        remaining = self.limit - len(self.head)
        self.head += safe[:remaining]
        self.tail = (self.tail + safe[remaining:])[-self.limit :]

    def read(self, stream):
        decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
        pending = ""
        omitted = False
        read = getattr(stream, "read1", stream.read)
        while True:
            chunk = read(4096)
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
        context = (
            "\n[fatal/Reason context from retrieved output]\n" + self.fatal_context
            if marker and self.fatal_context
            else ""
        )
        return self.head + marker + context + self.tail


@dataclass
class Result:
    returncode: int
    output: str


@dataclass
class StopResult:
    reaped: bool
    errors: list[str]
    interrupted: int | None = None


class ChildProcess:
    """Keep POSIX wait ownership until all possible group signals are finished."""

    @staticmethod
    def check_platform():
        if os.name != "posix":
            return
        required = (
            "waitid",
            "waitpid",
            "waitstatus_to_exitcode",
            "P_PID",
            "WEXITED",
            "WNOHANG",
            "WNOWAIT",
            "CLD_EXITED",
            "CLD_KILLED",
            "CLD_DUMPED",
        )
        if any(not hasattr(os, name) for name in required) or any(
            not callable(getattr(os, name))
            for name in ("waitid", "waitpid", "waitstatus_to_exitcode")
        ):
            raise SetupFailure(
                "Required non-reaping child observation is unavailable", retryable=False
            )
        if signal.getsignal(signal.SIGCHLD) != signal.SIG_DFL:
            raise SetupFailure(
                "Default SIGCHLD handling is required for wait ownership", retryable=False
            )

    def __init__(self, process):
        self.process = process
        self.posix = os.name == "posix"
        self.observed_code = None
        self.reaped = False
        self.signals_finished = False
        self.failure = None

    def lose_ownership(self, message):
        if self.failure is None:
            self.failure = message
        raise SetupFailure(self.failure, retryable=False) from None

    def require_owned(self):
        if self.failure is not None:
            self.lose_ownership(self.failure)
        if self.reaped or self.process.returncode is not None:
            self.lose_ownership("Child wait ownership lost: already reaped outside its holder")
        if signal.getsignal(signal.SIGCHLD) != signal.SIG_DFL:
            self.lose_ownership("Child wait ownership lost: SIGCHLD handling changed")

    def observe(self):
        if self.failure is not None:
            self.lose_ownership(self.failure)
        if self.reaped:
            return self.process.returncode
        if not self.posix:
            self.observed_code = self.process.poll()
            self.reaped = self.observed_code is not None
            return self.observed_code
        self.require_owned()
        try:
            status = os.waitid(os.P_PID, self.process.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT)
        except ChildProcessError:
            self.lose_ownership("Child wait ownership lost (ECHILD)")
        except OSError as exc:
            self.lose_ownership(f"Cannot observe owned child without reaping (errno={exc.errno})")
        if status is None:
            if self.observed_code is not None:
                self.lose_ownership("Previously observed child exit is no longer waitable")
            return None
        if status.si_pid != self.process.pid:
            self.lose_ownership("Unexpected child identity in non-reaping observation")
        if status.si_code == os.CLD_EXITED:
            code = status.si_status
        elif status.si_code in (os.CLD_KILLED, os.CLD_DUMPED):
            code = -status.si_status
        else:
            self.lose_ownership("Unexpected child state in non-reaping observation")
        if self.observed_code is not None and code != self.observed_code:
            self.lose_ownership("Child exit status changed while held unreaped")
        self.observed_code = code
        return self.observed_code

    def signal_authority(self):
        if self.signals_finished:
            self.lose_ownership("Child signalling attempted after its final reap phase began")
        if self.posix:
            self.require_owned()
        return self.observe()

    def wait(self, timeout):
        if not self.posix:
            self.observed_code = self.process.wait(timeout=timeout)
            self.reaped = True
            return self.observed_code
        deadline = time.monotonic() + timeout
        while True:
            code = self.observe()
            if code is not None:
                return code
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise subprocess.TimeoutExpired("owned setup child", timeout)
            time.sleep(min(0.05, remaining))

    def reap(self, timeout):
        self.signals_finished = True
        if self.failure is not None:
            self.lose_ownership(self.failure)
        if self.reaped:
            return self.process.returncode
        if not self.posix:
            return self.wait(timeout)
        deadline = time.monotonic() + timeout
        while True:
            self.require_owned()
            try:
                pid, status = os.waitpid(self.process.pid, os.WNOHANG)
            except ChildProcessError:
                self.lose_ownership("Child wait ownership lost during final reap (ECHILD)")
            except OSError as exc:
                self.lose_ownership(f"Cannot reap owned child (errno={exc.errno})")
            if pid == self.process.pid:
                try:
                    code = os.waitstatus_to_exitcode(status)
                except ValueError:
                    self.lose_ownership("Unexpected child status during final reap")
                self.process.returncode = code
                self.reaped = True
                if self.observed_code is not None and code != self.observed_code:
                    self.lose_ownership("Child exit status changed after non-reaping observation")
                return code
            if pid != 0:
                self.lose_ownership("Unexpected child identity during final reap")
            if self.observed_code is not None:
                self.lose_ownership("Observed child exit disappeared before final reap")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise subprocess.TimeoutExpired("owned setup child", timeout)
            time.sleep(min(0.05, remaining))


class Commands:
    def __init__(self, password):
        self.password = password

    @staticmethod
    def stop(child, deadline):
        process = child.process
        errors = []
        group_blocked = False
        interrupted = None

        def record(exc):
            nonlocal interrupted
            if isinstance(exc, Cancelled):
                if interrupted is None:
                    interrupted = exc.signum
                errors.append(f"Command teardown interrupted by signal {exc.signum}")
            else:
                errors.append(str(exc))

        def signal_child(name):
            try:
                if child.signal_authority() is not None:
                    return
                if child.posix:
                    os.kill(process.pid, getattr(signal, "SIG" + name))
                elif name == "TERM":
                    process.terminate()
                else:
                    process.kill()
            except ProcessLookupError:
                pass
            except PermissionError as exc:
                errors.append(
                    f"Permission denied sending SIG{name} to owned child "
                    f"{process.pid} (errno={exc.errno})"
                )
            except (SetupFailure, Cancelled) as exc:
                record(exc)

        def signal_owned(name):
            nonlocal group_blocked
            if not child.posix:
                signal_child(name)
                return
            if group_blocked:
                signal_child(name)
                return
            try:
                child.signal_authority()
            except (SetupFailure, Cancelled) as exc:
                record(exc)
                group_blocked = True
                return
            try:
                group = os.getpgid(process.pid)
            except ProcessLookupError:
                # The unreaped leader still pins this identity even if its
                # group lookup no longer sees it.
                pass
            except PermissionError as exc:
                errors.append(
                    f"Permission denied checking original group {process.pid} (errno={exc.errno})"
                )
                group_blocked = True
            except Cancelled as exc:
                record(exc)
                return
            else:
                if group != process.pid:
                    errors.append(
                        f"Owned child {process.pid} left its original group; "
                        f"group {group} not followed"
                    )
                    group_blocked = True
            if group_blocked:
                signal_child(name)
                return
            try:
                child.signal_authority()
                os.killpg(process.pid, getattr(signal, "SIG" + name))
            except ProcessLookupError:
                signal_child(name)
            except PermissionError as exc:
                errors.append(
                    f"Permission denied sending SIG{name} to original group "
                    f"{process.pid} (errno={exc.errno})"
                )
                group_blocked = True
                signal_child(name)
            except (SetupFailure, Cancelled) as exc:
                record(exc)
                group_blocked = True

        signal_owned("TERM")
        try:
            child.wait(timeout=max(0, min(1, (deadline - time.monotonic()) / 2)))
        except subprocess.TimeoutExpired:
            pass
        except (SetupFailure, Cancelled) as exc:
            record(exc)
        signal_owned("KILL")
        try:
            child.reap(timeout=max(0, min(1, deadline - time.monotonic())))
        except subprocess.TimeoutExpired:
            pass
        except (SetupFailure, Cancelled) as exc:
            record(exc)
        return StopResult(child.reaped, errors, interrupted)

    @staticmethod
    def snapshot(output, capture):
        output.flush()
        size = os.fstat(output.fileno()).st_size
        window = 65536
        ranges = [(0, size)] if size <= 2 * window else [(0, window), (size - window, window)]
        capture.add(
            "[Colima startup snapshot: at most 64KiB head and 64KiB tail; "
            "partial boundary lines and later background output omitted]\n"
        )
        for index, (offset, length) in enumerate(ranges):
            if index:
                capture.context_remaining = 0
                capture.add("[Colima startup snapshot truncated; middle output omitted]\n")
            if hasattr(os, "pread"):
                data = os.pread(output.fileno(), length, offset)
            else:
                output.seek(offset)
                data = output.read(length)
            if offset:
                data = data.partition(b"\n")[2]
            if data and not data.endswith(b"\n"):
                data = data[: data.rfind(b"\n") + 1]
            capture.read(io.BytesIO(data))

    def run_launcher(self, args, timeout, *, env=None):
        deadline = time.monotonic() + timeout
        try:
            output = tempfile.TemporaryFile(mode="a+b")
        except OSError as exc:
            raise SetupFailure(
                f"Cannot create private Colima output capture (errno={exc.errno})", retryable=False
            ) from None
        primary = None
        try:
            return self._run(args, deadline - time.monotonic(), env=env, output_file=output)
        except (SetupFailure, Cancelled) as exc:
            primary = exc
            raise
        finally:
            try:
                output.close()
            except OSError as exc:
                message = f"Cannot close private Colima output capture (errno={exc.errno})"
                if primary is None:
                    raise SetupFailure(message, retryable=False) from None
                if isinstance(primary, SetupFailure):
                    primary.retryable = False
                    primary.args = (str(primary) + "\n" + message,)
                else:
                    print("[sql] " + message, file=sys.stderr, flush=True)

    def run(self, args, timeout, *, env=None):
        return self._run(args, timeout, env=env)

    def _run(self, args, timeout, *, env=None, output_file=None):
        if timeout <= 0:
            raise SetupTimeout("SQL setup deadline exhausted")
        deadline = time.monotonic() + timeout
        grace = min(4, timeout / 2)
        work_deadline = deadline - grace
        capture = SafeCapture(self.password)
        ChildProcess.check_platform()
        try:
            process = subprocess.Popen(
                args,
                stdout=subprocess.PIPE if output_file is None else output_file,
                stderr=subprocess.STDOUT,
                env=env,
                start_new_session=os.name == "posix",
            )
        except OSError:
            raise SetupFailure("Cannot launch required setup command", retryable=False) from None
        child = ChildProcess(process)
        output_done = threading.Event()
        read_errors = ["Setup output reader did not complete"] if output_file is None else []

        def read_output():
            try:
                capture.read(process.stdout)
            except OSError:
                read_errors[:] = ["Cannot read setup command output"]
            else:
                read_errors.clear()
            finally:
                output_done.set()

        reader = threading.Thread(target=read_output, daemon=True) if output_file is None else None
        reader_started = False
        primary = None
        exit_failure = None
        problems = []

        def record_error(exc):
            nonlocal primary
            if primary is None:
                primary = exit_failure or exc
                if primary is exc:
                    return
            problems.append(
                f"Additional cancellation during teardown (signal {exc.signum})"
                if isinstance(exc, Cancelled)
                else str(exc)
            )

        def failure_message(reason):
            message = reason
            if problems:
                message += "\nCommand teardown incomplete:\n" + "\n".join(problems)
            message += "\nCaptured command output (may be incomplete):"
            message += "\n" + (capture.output() or "[no completed output lines captured]")
            return redact(message, self.password)

        try:
            if reader is not None:
                try:
                    reader.start()
                    reader_started = True
                except RuntimeError:
                    raise SetupFailure(
                        "Cannot start setup output reader", retryable=False
                    ) from None
            try:
                code = child.wait(timeout=max(0, work_deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                primary = SetupTimeout("Setup command timed out")
            else:
                if code in (-signal.SIGINT, -signal.SIGTERM, 130, 143):
                    exit_failure = Cancelled(
                        signal.SIGINT if code in (-signal.SIGINT, 130) else signal.SIGTERM
                    )
                elif code != 0:
                    exit_failure = SetupFailure(f"Setup command failed (exit {code})")
                if output_file is None:
                    if not output_done.wait(timeout=max(0, work_deadline - time.monotonic())):
                        record_error(
                            SetupFailure("Setup command output drain timed out", retryable=False)
                        )
                    elif read_errors:
                        record_error(SetupFailure(read_errors[0], retryable=False))
        except (Cancelled, SetupFailure) as exc:
            record_error(exc)
        finally:
            teardown_deadline = min(deadline, time.monotonic() + 4)
            try:
                code = child.observe()
            except (SetupFailure, Cancelled) as exc:
                record_error(exc)
                code = child.observed_code
            if (
                primary is not None
                or code is None
                or (output_file is None and (not output_done.is_set() or read_errors))
                or (output_file is not None and code != 0)
            ):
                stopped = self.stop(child, teardown_deadline)
                problems.extend(stopped.errors)
                if stopped.interrupted is not None:
                    record_error(Cancelled(stopped.interrupted))
            else:
                try:
                    child.reap(timeout=max(0, min(1, teardown_deadline - time.monotonic())))
                except subprocess.TimeoutExpired:
                    problems.append("Final child reap exceeded its deadline")
                except (SetupFailure, Cancelled) as exc:
                    record_error(exc)
            if reader_started:
                try:
                    output_done.wait(timeout=max(0, teardown_deadline - time.monotonic()))
                except Cancelled as exc:
                    record_error(exc)
            if output_file is not None:
                try:
                    self.snapshot(output_file, capture)
                except OSError as exc:
                    problems.append(f"Cannot read Colima startup snapshot (errno={exc.errno})")
                except Cancelled as exc:
                    record_error(exc)
                output_done.set()
            elif not reader_started or output_done.is_set():
                try:
                    process.stdout.close()
                except OSError as exc:
                    problems.append(f"Cannot close command output (errno={exc.errno})")
                except Cancelled as exc:
                    record_error(exc)
            if not child.reaped:
                problems.append("Owned child could not be reaped within the teardown deadline")
            if reader_started and not output_done.is_set():
                problems.append("Output reader did not finish within the teardown deadline")
            elif reader_started and read_errors:
                problems.append("Output capture error: " + read_errors[0])
        if not child.reaped or process.returncode is None:
            problems.append("Command has no verified final child exit status")
        if primary is None and problems:
            primary = exit_failure or SetupFailure("Setup command teardown failed", retryable=False)
        if primary is not None:
            if isinstance(primary, Cancelled):
                print(
                    "[sql] " + failure_message(f"Command cancelled (signal {primary.signum})"),
                    file=sys.stderr,
                    flush=True,
                )
                raise primary
            if problems or read_errors:
                primary.retryable = False
            primary.args = (failure_message(str(primary)),)
            raise primary from None
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
        self.creation_requested = False
        self.image_id = None
        self.docker = ["docker"] + (["--context", "colima"] if args.colima else [])
        self.env = os.environ.copy()
        for key in ("DB_PASSWORD", "DB_CONNECTION_STRING", "MSSQL_SA_PASSWORD", "SQLCMDPASSWORD"):
            self.env.pop(key, None)
        self.env.update(MSSQL_SA_PASSWORD=password, SQLCMDPASSWORD=password)

    def log(self, message):
        print("[sql] " + redact(message, self.password), flush=True)

    def command(self, args, timeout=15, *, check=True, deadline=None, launcher=False):
        end = min(self.deadline, self.phase_deadline if deadline is None else deadline)
        remaining = min(timeout, end - time.monotonic())
        if remaining <= 0:
            raise SetupTimeout("SQL setup deadline exhausted")
        run = self.commands.run_launcher if launcher else self.commands.run
        result = run(args, remaining, env=self.env)
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
            f"name=^/{re.escape(self.args.name)}$",
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
        self.log(
            "Container log excerpts (last 30m, max 5000 lines; may omit older history; "
            "retained output may be truncated):"
        )
        try:
            result = self.docker_command(
                "logs",
                "--since",
                "30m",
                "--tail",
                "5000",
                container.identifier,
                timeout=20,
                check=False,
                deadline=deadline,
            )
            self.log(result.output)
            if result.returncode:
                self.log(f"Container logs unavailable (exit {result.returncode})")
        except SetupFailure as exc:
            self.log(f"Container logs unavailable: {exc}")
            if not exc.retryable:
                raise

    def remove(self, container, *, deadline):
        self.docker_command("rm", "--force", container.identifier, timeout=30, deadline=deadline)
        remaining = self.find_owned(deadline=deadline)
        if remaining is not None:
            raise SetupFailure("Owned container still exists after removal", retryable=False)
        self.container = None

    def remove_with_evidence(self, container, *, deadline, diagnostic_deadline):
        diagnostic_error = None
        try:
            self.diagnostics(container, deadline=diagnostic_deadline)
        except (SetupFailure, Cancelled) as exc:
            diagnostic_error = exc
        try:
            self.remove(container, deadline=deadline)
        except (SetupFailure, Cancelled) as removal_error:
            if diagnostic_error is None:
                raise
            if isinstance(diagnostic_error, Cancelled):
                self.log(
                    "Owned removal also failed during diagnostic cancellation: "
                    + (
                        f"signal {removal_error.signum}"
                        if isinstance(removal_error, Cancelled)
                        else str(removal_error)
                    )
                )
                raise diagnostic_error from None
            if isinstance(removal_error, Cancelled):
                self.log(f"Diagnostics also failed before removal cancellation: {diagnostic_error}")
                raise
            raise SetupFailure(
                f"Diagnostics failed: {diagnostic_error}\nOwned removal failed: {removal_error}",
                retryable=False,
            ) from None
        if diagnostic_error is not None:
            raise diagnostic_error

    def cleanup(self, *, evidence=True):
        # Include lookup/ownership checks and removal verification in addition
        # to the log and rm deadlines.
        self.phase_deadline = min(self.deadline, time.monotonic() + 100)
        container = self.find_owned()
        if container is None:
            self.container = None
            return
        if evidence:
            self.remove_with_evidence(
                container,
                deadline=self.phase_deadline,
                diagnostic_deadline=min(self.phase_deadline - 30, time.monotonic() + 20),
            )
        else:
            self.remove(container, deadline=self.phase_deadline)

    def prepare_runtime(self):
        if self.args.colima and not self.args.cleanup:
            self.log("Starting Colima once (outside SQL retry)")
            result = self.command(
                ["colima", "start", "--cpu", "4", "--memory", "8", "--disk", "50"],
                600,
                launcher=True,
            )
            self.log("Colima launcher completed; Docker and SQL readiness still require checks")
            self.log(result.output)

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

    def readiness_probe(self, deadline):
        current = self.find_owned(deadline=deadline)
        if current is None or current.identifier != self.container.identifier:
            raise SetupFailure("SQL container disappeared or was replaced", retryable=False)
        if current.status != "running":
            raise SetupFailure(
                f"SQL container exited before readiness (status={current.status}, "
                f"exit={current.exit_code}, OOMKilled={current.oom_killed})"
            )
        probe = self.sql("SELECT 1", deadline=deadline)
        if probe.returncode in (126, 127):
            raise SetupFailure("Required sqlcmd executable is unavailable", retryable=False)
        return probe

    def wait_ready(self):
        polling_deadline = min(
            self.deadline,
            self.phase_deadline,
            time.monotonic() + (180 if self.args.colima else 120),
        )
        while time.monotonic() < polling_deadline:
            try:
                probe = self.readiness_probe(polling_deadline)
            except SetupTimeout as exc:
                if not exc.retryable:
                    raise
                self.log(f"Readiness probe timed out: {exc}")
            else:
                if probe.returncode == 0:
                    return
            time.sleep(max(0, min(2, polling_deadline - time.monotonic())))
        final_deadline = min(self.deadline, self.phase_deadline, polling_deadline + 45)
        if time.monotonic() >= final_deadline:
            raise SetupTimeout("SQL readiness budget exhausted before final probe")
        self.log("Polling window ended; performing one final bounded SQL readiness check")
        probe = self.readiness_probe(final_deadline)
        if probe.returncode != 0:
            raise SetupFailure("SQL final readiness check failed\n" + probe.output)

    def attempt(self):
        stale = self.find_owned()
        if stale is not None:
            self.log("Removing pre-existing same-job container before fresh setup")
            try:
                self.remove_with_evidence(
                    stale,
                    deadline=self.phase_deadline,
                    diagnostic_deadline=min(self.phase_deadline, time.monotonic() + 20),
                )
            except SetupFailure as exc:
                raise SetupFailure(str(exc), retryable=False) from None
        self.acquire_image()
        # The daemon may create the container even when the CLI times out.
        self.creation_requested = True
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
        self.wait_ready()
        if self.args.database:
            result = self.sql("CREATE DATABASE TestDB", timeout=30, query_timeout=15)
            if result.returncode != 0:
                raise SetupFailure("TestDB initialization failed\n" + result.output)

    def setup(self):
        self.prepare_runtime()
        if self.args.cleanup:
            self.cleanup(evidence=False)
            self.log("Owned SQL container cleanup complete (or already absent)")
            return
        for number in (1, 2):
            self.creation_requested = False
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
                if self.creation_requested:
                    try:
                        self.cleanup()
                    except SetupFailure as cleanup_error:
                        raise SetupFailure(
                            f"SQL setup failed: {exc}\nCannot safely recover/clean up: {cleanup_error}",
                            retryable=False,
                        ) from None
                if not exc.retryable or number == 2:
                    raise SetupFailure("SQL setup failed; no further attempts", retryable=False)
                self.phase_deadline = self.deadline
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
                setup.cleanup(evidence=not setup.args.cleanup)
            except SetupFailure as cleanup_error:
                setup.log(f"Cancellation cleanup failed: {cleanup_error}")
        return 128 + exc.signum
    except SetupFailure as exc:
        print("[sql] " + redact(str(exc), password), file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
