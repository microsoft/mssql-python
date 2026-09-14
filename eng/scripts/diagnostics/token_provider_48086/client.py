"""Bounded client-side qualification; retain every failure and incomplete phase."""

import importlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import sys
import unittest.mock
import xml.etree.ElementTree as ET

from contracts import (
    AUTH,
    EXACT,
    HERE,
    REL,
    bounded,
    clean_env,
    configure_source_imports,
    import_source_package,
    require,
    save,
    sha,
    source_pair,
    validate_run,
)

ROOT = configure_source_imports(__file__)
OUT = Path("/evidence/raw")
PHASES = []


def environment(live=False):
    env = clean_env()
    if live:
        env["DB_CONNECTION_STRING"] = os.environ["DB_CONNECTION_STRING"]
        env["DB_PASSWORD"] = os.environ["DB_PASSWORD"]
    return env


def command(name, args, timeout, live=False):
    result, output = bounded(args, timeout=timeout, env=environment(live))
    (OUT / (name + ".log")).write_bytes(output)
    PHASES.append({"phase": name, **result})
    save(OUT / "phases.json", PHASES)
    return result


def success(result):
    return not result["timeout"] and result["returncode"] == 0


def runtime(version):
    require(
        platform.system() == "Linux" and platform.machine() == "aarch64",
        "Not an ARM64 Linux interpreter",
    )
    require(".".join(map(str, sys.version_info[:2])) == version, "Wrong Python major/minor")
    require(sys.flags.optimize == 0, "Assertions must remain enabled")
    source_pair(ROOT)
    required = {}
    from packaging.requirements import Requirement

    for line in (ROOT / "requirements.txt").read_text().splitlines():
        if not line.strip() or line.startswith("#"):
            continue
        requirement = Requirement(line)
        if requirement.marker and not requirement.marker.evaluate():
            continue
        installed = importlib.metadata.version(requirement.name)
        require(
            not requirement.specifier or installed in requirement.specifier,
            "Missing normal requirement " + requirement.name,
        )
        required[requirement.name] = installed
    modules = {}
    for name in (
        "mssql_python",
        "mssql_py_core",
        "mssql_python_odbc",
        "azure.identity",
        "pyarrow",
        "polars",
        "zstandard",
        "psutil",
    ):
        module = (
            import_source_package(ROOT, name)
            if name in ("mssql_python", "mssql_py_core", "mssql_python_odbc")
            else importlib.import_module(name)
        )
        modules[name] = str(Path(module.__file__).resolve())
    require(
        Path(modules["mssql_python"]) == ROOT / "mssql_python/__init__.py",
        "Imported a foreign package",
    )
    native = {
        name: Path(module.__file__).resolve()
        for name, module in list(sys.modules.items())
        if getattr(module, "__file__", None)
        and module.__file__.endswith(".so")
        and ("ddbc_bindings" in name or "mssql_py_core" in name)
    }
    require(
        any("ddbc_bindings" in name for name in native)
        and any("mssql_py_core" in name for name in native),
        "Native imports missing",
    )
    for path in native.values():
        require(path.is_relative_to(ROOT), "Foreign native module")
        data = path.read_bytes()
        require(
            data[:4] == b"\x7fELF" and int.from_bytes(data[18:20], "little") == 183,
            "Imported binary is not ELF AArch64",
        )
    native_files = {str(p.relative_to(ROOT)): sha(p.read_bytes()) for p in native.values()}
    runtime_data = {
        "python": sys.version,
        "python_executable": sys.executable,
        "python_executable_sha256": sha(Path(sys.executable).read_bytes()),
        "machine": platform.machine(),
        "libc": platform.libc_ver(),
        "os_release": Path("/etc/os-release").read_text(),
        "mock_path": unittest.mock.__file__,
        "mock_sha256": sha(Path(unittest.mock.__file__).read_bytes()),
        "module_origins": modules,
        "imported_native_binaries": native_files,
        "requirements": required,
        "dependencies": sorted(
            (d.metadata["Name"], d.version) for d in importlib.metadata.distributions()
        ),
        "auth_is_mocked": True,
        "hardware": "ARM64 guest under QEMU on hosted x64",
    }
    for name, argv in (("compiler", ["c++", "--version"]), ("libc_command", ["ldd", "--version"])):
        result, output = bounded(argv, timeout=20, env=environment())
        # musl ldd intentionally returns 1 for --version; retain the exact result.
        require(not result["timeout"], "Runtime identity command timed out")
        runtime_data[name] = {"result": result, "output": output.decode(errors="replace")}
    save(OUT / "runtime.json", runtime_data)


def pytest_phase(name, selectors, expected=None, repeats=False, full=False, live=False):
    plugins = ["-p", "accounting"] + (["-p", "repeat_plugin"] if repeats else [])
    common = [sys.executable, "-m", "pytest", *plugins, *selectors, "-v", "-ra", "--color=no"]
    # Retain normal pytest.ini, conftest and markers. No unskip or retry plugin.
    collect_path, run_path = OUT / (name + "-collect.json"), OUT / (name + "-run.json")
    env = environment(live)
    env["QUAL_REPORT"] = str(collect_path)
    result, output = bounded([*common, "--collect-only"], timeout=180, env=env)
    (OUT / (name + "-collect.log")).write_bytes(output)
    PHASES.append({"phase": name + "-collect", **result})
    save(OUT / "phases.json", PHASES)
    require(success(result), "Collection failed: " + name)
    collected = json.loads(collect_path.read_text())
    require(not collected["collection_issues"], "Collection issues: " + name)
    if expected is not None:
        require(collected["nodes"] == expected, "Frozen node mismatch: " + name)
    save(OUT / (name + "-frozen-nodes.json"), collected)
    env["QUAL_REPORT"] = str(run_path)
    result, output = bounded(
        [*common, f"--junitxml={OUT / (name + '.xml')}"],
        timeout=2400 if full else 360,
        env=env,
    )
    (OUT / (name + ".log")).write_bytes(output)
    PHASES.append({"phase": name, **result})
    save(OUT / "phases.json", PHASES)
    require(success(result), "Pytest failed or timed out: " + name)
    executed = json.loads(run_path.read_text())
    counter_count = 100 if repeats else int(EXACT in collected["nodes"])
    validate_run(collected, executed, expected, counters=counter_count, allow_skips=full)
    cases = ET.parse(OUT / (name + ".xml")).getroot().findall(".//testcase")
    require(len(cases) == len(collected["nodes"]), "JUnit count mismatch: " + name)
    require(
        not any(c.find("failure") is not None or c.find("error") is not None for c in cases),
        "JUnit failure: " + name,
    )
    require(
        sum(c.find("skipped") is not None for c in cases)
        == sum(r["outcome"] == "skipped" for r in executed["reports"]),
        "JUnit skip accounting mismatch: " + name,
    )


def main():
    OUT.mkdir(exist_ok=False)
    runtime(sys.argv[1])
    frozen = json.loads((HERE / "expected.json").read_text())
    failures = []

    def phase(name, function):
        try:
            function()
        except (RuntimeError, OSError, ValueError, KeyError, ET.ParseError) as exc:
            failures.append({"phase": name, "error": str(exc)})
            save(OUT / "failures.json", failures)

    phase("exact", lambda: pytest_phase("exact", [EXACT], frozen["nodes"]["exact"]))
    adjacent = [
        AUTH + "::" + cls
        for cls in (
            "TestCustomTokenProviderConnect",
            "TestTokenProviderValidation",
            "TestAcquireTokenFromCredential",
            "TestAcquireRawTokenFromCredential",
        )
    ]
    phase("adjacent", lambda: pytest_phase("adjacent", adjacent, frozen["nodes"]["adjacent"]))
    phase("auth", lambda: pytest_phase("auth", [AUTH], frozen["nodes"]["auth"]))
    for index in range(10):
        name = f"repeat-{index:02}"
        phase(
            name,
            lambda name=name: pytest_phase(
                name,
                [EXACT],
                [EXACT + f"[{n}]" for n in range(100)],
                repeats=True,
            ),
        )

    def controls():
        result = command(
            "controls",
            [sys.executable, str(HERE / "project_controls.py"), str(OUT / "controls.jsonl")],
            480,
        )
        require(success(result), "Controlled trials failed")
        require(
            len(json.loads((OUT / "controls.json").read_text())["trials"]) == 100,
            "Incomplete controls",
        )

    phase("controls", controls)

    def sql():
        result = command(
            "sql-smoke",
            [sys.executable, str(HERE / "sql_smoke.py"), str(OUT / "sql-smoke.json")],
            240,
            live=True,
        )
        require(success(result), "SQL smoke failed")

    phase("sql-smoke", sql)
    sql_nodes = ["tests/test_003_connection.py::" + name for name in frozen["sql_tests"]]
    phase("sql-selected", lambda: pytest_phase("sql-selected", sql_nodes, sql_nodes, live=True))
    phase("full-suite", lambda: pytest_phase("full-suite", [], full=True, live=True))
    save(
        OUT / "outcome.json",
        {
            "accepted": not failures,
            "failures": failures,
            "phases": PHASES,
            "job_task_pytest_retries": 0,
            "fixture_recovery": "direct profile counts per normal db_connection fixture in run JSON",
            "full_suite_skips": "unchanged active static skip markers frozen at collection; "
            "three exact dynamic skip rules require pinned source, exact reason and runtime predicate; "
            "all other dynamic/module skips remain terminal",
        },
    )
    require(not failures, "Qualification contains failures; inspect retained artifacts")


if __name__ == "__main__":
    if sys.argv[1:] == ["--verify-source-imports"]:
        package = import_source_package(ROOT, "mssql_python")
        print(json.dumps({"root": str(ROOT), "package_origin": package.__file__}))
    else:
        main()
