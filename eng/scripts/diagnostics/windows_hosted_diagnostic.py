"""Opt-in Windows shutdown diagnostic; execute with Python 3.14.7 on SQL2025.

Parent job prerequisites:
  - Install/configure the same local SQL2025 Express instance and test DB as CI.
  - UsePythonVersion selects 3.14.7 x64; pybind11==3.1.0 and pytest==9.1.1 installed.
  - Put native_probe.cpp beside this script. Existing VS2022 + Windows SDK suffice.
  - Set DB_CONNECTION_STRING to local SQL-auth test credentials, not remote secrets.

All source instrumentation/builds/logs are inside --output. The checked-out
diagnostic branch and normal PR pipeline are not modified by this program.
Hosted capture never creates process dumps. Publish only the evidence directory,
which contains redacted text/JSON, not binaries, symbols or process memory.
"""

import argparse
import hashlib
import json
import os
import pathlib
import re
import shutil
import subprocess
import sys
import tarfile
import time

TESTS = [
    r"tests\test_013_SqlHandle_free_shutdown.py::TestHandleFreeShutdown"
    "::test_all_handle_types_comprehensive",
    r"tests\test_013_SqlHandle_free_shutdown.py::TestHandleFreeShutdown"
    "::test_rapid_connection_churn_with_shutdown",
    r"tests\test_024_context_manager_transaction.py::TestContextManagerCommit"
    "::test_nested_context_managers",
]
ANCHOR = "        bool pythonShuttingDown = is_python_finalizing();"
INSTRUMENTATION = ANCHOR + r"""
        if (_type == SQL_HANDLE_ENV) {
            bool skip = GetEnvironmentVariableA("MSSQL_DIAG_SKIP_FINAL_ENV", nullptr, 0) != 0;
            fprintf(stderr, "DIAG_ENV_FREE finalizing=%d initialized=%d skip=%d\n",
                    pythonShuttingDown, Py_IsInitialized(), skip);
            fflush(stderr);
            if (pythonShuttingDown && skip) {
                _handle = nullptr;
                return;
            }
        }"""
CHILD_BOOTSTRAP = (
    "import subprocess,pytest,sys;"
    "original=subprocess.run;"
    "subprocess.run=lambda *a,**kw: original(*a,**{**kw,'timeout':90});"
    "sys.exit(pytest.main(sys.argv[1:]))"
)
METADATA_SCRIPT = r"""
import json,os,sys,platform,pybind11,pytest
import mssql_python as m
with m.connect(os.environ["DB_CONNECTION_STRING"]) as conn:
    with conn.cursor() as cursor:
        row=cursor.execute(
            "SELECT CAST(SERVERPROPERTY('ProductVersion') AS varchar(40)), "
            "CAST(SERVERPROPERTY('Edition') AS varchar(120)), "
            "CAST(SERVERPROPERTY('ProductMajorVersion') AS varchar(10)), "
            "@@VERSION").fetchone()
    info={"python":sys.version,"platform":platform.platform(),
          "pybind11":pybind11.__version__,"pytest":pytest.__version__,
          "sql_version":row[0],"sql_edition":row[1],"sql_major":row[2],
          "sql_full_version":row[3],"driver_version":conn.getinfo(7),
          "provider":m.get_native_provider_info()}
    info["provider"].pop("driver_path",None)
m.pooling(enabled=False)
print("DIAGNOSTIC_METADATA="+json.dumps(info),flush=True)
"""
FINAL_CLOSE_SCRIPT = r"""
import atexit,os
import mssql_python as m
from mssql_python.pooling import shutdown_pooling
atexit.unregister(m._cleanup_connections)
atexit.unregister(shutdown_pooling)
atexit.register(lambda: print("AFTER_CONNECTION_AND_POOL_ATEXIT",flush=True))
atexit.register(shutdown_pooling)
atexit.register(m._cleanup_connections)
m.pooling(enabled=False)
connection=m.connect(os.environ["DB_CONNECTION_STRING"])
cursor=connection.cursor()
assert cursor.execute("SELECT 1").fetchone()[0] == 1
cursor.close()
connection.close()
print("EXPLICIT_CLOSE_DONE_NO_POOL",flush=True)
"""


def connection_fields(text):
    fields = {}
    position = 0
    while position < len(text):
        while position < len(text) and text[position] in "; \t\r\n":
            position += 1
        if position == len(text):
            break
        equal = text.find("=", position)
        if equal < 0:
            raise ValueError("Invalid connection string syntax")
        key = text[position:equal].strip().lower()
        position = equal + 1
        while position < len(text) and text[position].isspace():
            position += 1
        if position < len(text) and text[position] == "{":
            position += 1
            value = ""
            while position < len(text):
                if text[position : position + 2] == "}}":
                    value += "}"
                    position += 2
                elif text[position] == "}":
                    position += 1
                    break
                else:
                    value += text[position]
                    position += 1
            else:
                raise ValueError("Unclosed braced connection value")
            while position < len(text) and text[position].isspace():
                position += 1
            if position < len(text) and text[position] != ";":
                raise ValueError("Unexpected trailing connection value")
        else:
            end = text.find(";", position)
            if end < 0:
                end = len(text)
            value, position = text[position:end].strip(), end
        fields[key] = value
    return fields


def native_log_text(stdout, stderr):
    """Keep native diagnostics, not pytest tracebacks with embedded child scripts."""
    lines = []
    for line in stdout.splitlines():
        if re.match(r"^(FAILED|PASSED|SKIPPED|ERROR) tests[\\/]", line):
            lines.append(line.split(" - ", 1)[0])
        elif re.fullmatch(r"[0-9].*(passed|failed|skipped|error).* in [0-9.]+s", line):
            lines.append(line)
        elif line in {"EXPLICIT_CLOSE_DONE_NO_POOL", "AFTER_CONNECTION_AND_POOL_ATEXIT"}:
            lines.append(line)
        match = re.search(r"DIAG_ENV_FREE finalizing=[01] initialized=[01] skip=[01]", line)
        if match:
            lines.append(match.group())
    lines += [
        line
        for line in stderr.splitlines()
        if ("path=" not in line)
        and re.match(
            r"^(CREATE |MODULE |UNLOAD |EXIT |SECOND_CHANCE |REGISTERS |FRAME |"
            r"GET_CONTEXT_FAILED |CREATE_PROCESS_FAILED |BOUNDED_TIMEOUT|DIAG_ENV_FREE )",
            line,
        )
    ]
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=pathlib.Path, required=True)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    parser.add_argument("--baseline-runs", type=int, default=2)
    parser.add_argument("--candidate-runs", type=int, default=5)
    parser.add_argument("--restore-runs", type=int, default=1)
    parser.add_argument("--expected-sql-major", default="17")
    args = parser.parse_args()
    counts = (args.baseline_runs, args.candidate_runs, args.restore_runs)
    if any(x < 1 for x in counts) or sum(counts) > 12:
        raise ValueError("Require 1+ runs per arm and <=12 total suite runs")
    if os.name != "nt" or sys.version_info[:3] != (3, 14, 7):
        raise RuntimeError("This diagnostic requires Windows Python 3.14.7")
    import pybind11
    import pytest

    if pybind11.__version__ != "3.1.0" or pytest.__version__ != "9.1.1":
        raise RuntimeError("Expected pybind11==3.1.0 and pytest==9.1.1")
    repo, output = args.repo.resolve(), args.output.resolve()
    if not output.is_relative_to(repo):
        raise ValueError("Output must be underneath the checked-out diagnostic repository")
    if output.exists():
        raise ValueError("Output must be a fresh, uniquely named directory")
    cs = os.environ.get("DB_CONNECTION_STRING", "")
    fields = connection_fields(cs)
    server = fields.get("server", fields.get("data source", "")).lower()
    host = server.removeprefix("tcp:").split(",")[0].split("\\")[0]
    if host not in {"localhost", "127.0.0.1", ".", "(local)", "(localdb)"}:
        raise ValueError("Only a local diagnostic SQL endpoint is permitted")
    if "driver" in fields or "dsn" in fields:
        raise ValueError("Use the bundled driver, not Driver= or DSN=")
    passwords = [v for k, v in fields.items() if k in {"pwd", "password"}]
    secrets = [cs, *passwords, *(value.replace("}", "}}") for value in passwords)]
    for value in tuple(secrets):
        secrets += [repr(value)[1:-1], json.dumps(value)[1:-1]]
    secrets = sorted(set(x for x in secrets if x), key=len, reverse=True)

    def redact(text):
        for value in secrets:
            text = text.replace(value, "<redacted>")
        for path, label in [
            (str(output), "<diagnostic-output>"),
            (str(repo), "<diagnostic-repo>"),
            (os.environ.get("USERPROFILE", ""), "<user-profile>"),
        ]:
            if path:
                for spelling in [path, path.replace("\\", "/"), path.replace("\\", "\\\\")]:
                    text = text.replace(spelling, label)
        return text

    output.mkdir(parents=True)
    scratch = output / "scratch"
    scratch.mkdir()
    source = output / "source"
    build = output / "b"
    evidence = output / "evidence"
    evidence.mkdir()
    # Keep CI tokens and unrelated secret variables out of dumpable child processes.
    allowed = {
        "PATH",
        "PATHEXT",
        "COMSPEC",
        "SYSTEMROOT",
        "WINDIR",
        "USERPROFILE",
        "LOCALAPPDATA",
        "APPDATA",
        "PROGRAMFILES",
        "PROGRAMFILES(X86)",
        "PROGRAMW6432",
        "PROGRAMDATA",
        "HOME",
        "COMPUTERNAME",
        "PROCESSOR_ARCHITECTURE",
        "PROCESSOR_IDENTIFIER",
        "NUMBER_OF_PROCESSORS",
        "SYSTEMDRIVE",
        "HOMEDRIVE",
        "HOMEPATH",
        "USERNAME",
        "USERDOMAIN",
    }
    env = {k: v for k, v in os.environ.items() if k.upper() in allowed}
    env.update(
        TEMP=str(scratch),
        TMP=str(scratch),
        PYTHONPATH=str(source),
        PYTHONFAULTHANDLER="1",
        PYTHONUNBUFFERED="1",
        PYTEST_DISABLE_PLUGIN_AUTOLOAD="1",
    )
    env["PATH"] = str(pathlib.Path(sys.executable).parent) + os.pathsep + env.get("PATH", "")

    def run(
        command, label, *, cwd=repo, timeout=300, child_env=env, check=True, native_text_only=False
    ):
        started = time.monotonic()
        with (evidence / f"{label}.log").open("w", encoding="utf-8") as stream:
            try:
                result = subprocess.run(
                    command,
                    cwd=cwd,
                    env=child_env,
                    capture_output=True,
                    text=True,
                    errors="replace",
                    timeout=timeout,
                )
            except subprocess.TimeoutExpired as exc:
                stream.write("ORCHESTRATOR_TIMEOUT\n")
                raise RuntimeError(f"Bounded operation timed out: {label}") from exc
            text = (
                native_log_text(result.stdout, result.stderr)
                if native_text_only
                else (result.stdout + "\nSTDERR:\n" + result.stderr)
            )
            stream.write(redact(text))
        row = {
            "label": label,
            "exit": result.returncode,
            "seconds": round(time.monotonic() - started, 3),
            "log": f"{label}.log",
        }
        print(json.dumps(row), flush=True)
        if check and result.returncode:
            raise RuntimeError(f"{label} failed; see its redacted artifact log")
        return result, row

    try:
        revision = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=repo, text=True
        ).strip()
        archive = output / "snapshot.tar"
        run(["git", "archive", "--format=tar", "--output", str(archive), "HEAD"], "archive")
        source.mkdir()
        with tarfile.open(archive) as handle:
            handle.extractall(source, filter="data")
        archive.unlink()
        native_source = source / "mssql_python" / "pybind" / "ddbc_bindings.cpp"
        original = native_source.read_text(encoding="utf-8")
        if original.count(ANCHOR) != 1:
            raise RuntimeError("Finalization anchor changed; require manual review, do not guess")
        native_source.write_text(original.replace(ANCHOR, INSTRUMENTATION), encoding="utf-8")
        probe_source = pathlib.Path(__file__).with_name("native_probe.cpp")
        shutil.copy2(probe_source, output / "native_probe.cpp")
        vswhere = pathlib.Path(os.environ["ProgramFiles(x86)"]) / (
            r"Microsoft Visual Studio\Installer\vswhere.exe"
        )
        vs = subprocess.check_output(
            [
                str(vswhere),
                "-latest",
                "-products",
                "*",
                "-requires",
                "Microsoft.VisualStudio.Component.VC.Tools.x86.x64",
                "-property",
                "installationPath",
            ],
            text=True,
        ).strip()
        vcvars = pathlib.Path(vs) / r"VC\Auxiliary\Build\vcvars64.bat"
        cmake = (
            pathlib.Path(vs) / r"Common7\IDE\CommonExtensions\Microsoft\CMake\CMake\bin\cmake.exe"
        )
        if not vcvars.is_file() or not cmake.is_file():
            raise RuntimeError("Existing Visual Studio C++/CMake tooling is required")
        command = (
            f'call "{vcvars}" >nul && cd /d "{output}" && '
            "cl /nologo /EHsc /W4 /WX /std:c++17 native_probe.cpp "
            "/Fe:native_probe.exe /link dbghelp.lib version.lib && "
            f'"{cmake}" -S "{source / "mssql_python" / "pybind"}" -B "{build}" '
            f'-A x64 -DARCHITECTURE=x64 -DPython3_EXECUTABLE="{sys.executable}" '
            '"-DCMAKE_MODULE_LINKER_FLAGS_RELEASE=/DEBUG /OPT:REF /OPT:ICF" && '
            f'"{cmake}" --build "{build}" --config Release'
        )
        run([os.environ["COMSPEC"], "/d", "/c", command], "native-build", timeout=600)
        for suffix in ("pyd", "pdb"):
            name = f"ddbc_bindings.cp314-amd64.{suffix}"
            shutil.copy2(build / "Release" / name, source / "mssql_python" / name)
        shutil.copy2(
            source / "mssql_python_odbc" / r"libs\windows\x64\vcredist\msvcp140.dll",
            source / "mssql_python" / "msvcp140.dll",
        )
        runtime_env = {**env, "DB_CONNECTION_STRING": cs}
        metadata_result, _ = run(
            [sys.executable, "-P", "-c", METADATA_SCRIPT],
            "runtime-metadata",
            cwd=source,
            child_env={**runtime_env, "MSSQL_DIAG_SKIP_FINAL_ENV": "1"},
            timeout=60,
        )
        metadata = json.loads(
            next(
                x.split("=", 1)[1]
                for x in metadata_result.stdout.splitlines()
                if x.startswith("DIAGNOSTIC_METADATA=")
            )
        )
        if metadata["sql_major"] != args.expected_sql_major:
            raise RuntimeError("SQL engine major version is not the approved diagnostic target")
        if metadata["driver_version"] != "18.06.0002":
            raise RuntimeError("Bundled driver differs from the observed CI failures")
        metadata.pop("source", None)
        metadata.get("provider", {}).pop("driver_path", None)
        report = {
            "revision": revision,
            "metadata": metadata,
            "runs": [],
            "protocol": "Same binary, debugger heap enabled; baseline -> late ENV skip -> restored baseline",
            "privacy": "Process dump creation disabled. Published evidence is redacted text/JSON only. Child environment excludes CI tokens; disposable localhost SQL credentials are never printed.",
        }
        report_path = evidence / "report.json"

        def save_report():
            report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

        for phase, count, skip in [
            ("baseline", args.baseline_runs, False),
            ("skip-final-env", args.candidate_runs, True),
            ("restored", args.restore_runs, False),
        ]:
            phase_env = dict(runtime_env)
            if skip:
                phase_env["MSSQL_DIAG_SKIP_FINAL_ENV"] = "1"
            for i in range(count):
                label = f"{phase}-{i:02d}"
                result, row = run(
                    [
                        str(output / "native_probe.exe"),
                        "260",
                        "-",
                        sys.executable,
                        "-c",
                        CHILD_BOOTSTRAP,
                        "-q",
                        "-o",
                        "addopts=",
                        "--basetemp",
                        str(scratch / label),
                        *TESTS,
                    ],
                    label,
                    cwd=source,
                    timeout=280,
                    child_env=phase_env,
                    check=False,
                    native_text_only=True,
                )
                row["phase"] = phase
                row["second_chance_av"] = bool(
                    re.search(r"SECOND_CHANCE [^\r\n]*code=c0000005", result.stderr)
                )
                row["matches_local_env_sspi_stack"] = all(
                    marker.lower() in result.stderr.lower()
                    for marker in [
                        "SSPICLI!FreeCredentialsHandle",
                        "!SqlHandle::free",
                        "dynamic atexit destructor for 'envHandle'",
                        "!dllmain_crt_process_detach",
                    ]
                )
                row["rax_has_freed_heap_poison"] = "rax=feeefeeefeeefeee" in result.stderr.lower()
                row["late_env_trace"] = "DIAG_ENV_FREE finalizing=1 initialized=0" in result.stdout
                row["pytest_passed"] = "3 passed" in result.stdout
                report["runs"].append(row)
                save_report()
        # Explicit-close control adds no database writes.
        result, row = run(
            [str(output / "native_probe.exe"), "90", "-", sys.executable, "-c", FINAL_CLOSE_SCRIPT],
            "explicit-close-skip",
            cwd=source,
            timeout=110,
            child_env={**runtime_env, "MSSQL_DIAG_SKIP_FINAL_ENV": "1"},
            check=False,
            native_text_only=True,
        )
        report["explicit_close_control"] = row
        report["explicit_close_control"]["markers"] = all(
            marker in result.stdout
            for marker in ["EXPLICIT_CLOSE_DONE_NO_POOL", "AFTER_CONNECTION_AND_POOL_ATEXIT"]
        )
        report["local_env_skip_supported_on_host"] = (
            any(x["second_chance_av"] for x in report["runs"] if x["phase"] == "baseline")
            and any(x["second_chance_av"] for x in report["runs"] if x["phase"] == "restored")
            and all(
                x["exit"] == 0 and x["pytest_passed"]
                for x in report["runs"]
                if x["phase"] == "skip-final-env"
            )
        )
        report["same_native_cause_confirmed_on_host"] = (
            report["local_env_skip_supported_on_host"]
            and any(
                x["matches_local_env_sspi_stack"]
                for x in report["runs"]
                if x["phase"] == "baseline"
            )
            and any(
                x["matches_local_env_sspi_stack"]
                for x in report["runs"]
                if x["phase"] == "restored"
            )
        )
        report["files"] = [
            {
                "name": p.name,
                "bytes": p.stat().st_size,
                "sha256": hashlib.sha256(p.read_bytes()).hexdigest(),
            }
            for p in [
                source / "mssql_python" / "ddbc_bindings.cp314-amd64.pyd",
                source / "mssql_python" / "ddbc_bindings.cp314-amd64.pdb",
                source / "mssql_python_odbc" / r"libs\windows\x64\msodbcsql18.dll",
            ]
        ]
        save_report()
        print(
            json.dumps(
                {
                    "complete": True,
                    "report": str(report_path),
                    "aba_supported": report["local_env_skip_supported_on_host"],
                    "same_native_cause": report["same_native_cause_confirmed_on_host"],
                    "dump_creation_disabled": True,
                }
            ),
            flush=True,
        )
        return 0
    except Exception as exc:
        (evidence / "orchestration-error.json").write_text(
            json.dumps({"type": type(exc).__name__, "message": redact(str(exc))}, indent=2),
            encoding="utf-8",
        )
        print(
            f"Diagnostic orchestration failed: {type(exc).__name__}; see artifact logs", flush=True
        )
        return 1
    finally:
        # Always-on publish steps must never scoop up a binary or process dump.
        for path in evidence.rglob("*"):
            if path.is_file() and path.suffix.lower() not in {".log", ".txt", ".json"}:
                private = output / "private-not-for-publication"
                private.mkdir(exist_ok=True)
                shutil.move(str(path), str(private / path.name))


if __name__ == "__main__":
    sys.exit(main())
