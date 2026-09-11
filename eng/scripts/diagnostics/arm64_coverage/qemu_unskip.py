"""Diagnostic-only restoration and subprocess accounting for two QEMU exclusions."""

import json
import os
from pathlib import Path
import subprocess
import sys
import time

import pytest

TARGETS = {
    ("test_013_SqlHandle_free_shutdown.py", "TestHandleFreeShutdown"),
    ("test_024_context_manager_transaction.py", "TestContextManagerCommit"),
}
STATE = {"removed": [], "collected": [], "roots": [], "reports": [], "errors": []}


def save():
    folder = Path(os.environ["TASK47285_REPORT_DIR"])
    folder.mkdir(mode=0o700, parents=True, exist_ok=True)
    (folder / "processes.json").write_text(json.dumps(STATE, indent=2), encoding="utf-8")


@pytest.hookimpl(trylast=True)
def pytest_collection_modifyitems(items):
    expected = json.loads(Path(__file__).with_name("expected.json").read_text())["nodeids"]
    actual = [item.nodeid for item in items]
    STATE["collected"] = actual
    save()
    if len(actual) != 44 or set(actual) != set(expected):
        missing = sorted(set(expected) - set(actual))
        extra = sorted(set(actual) - set(expected))
        raise pytest.UsageError(
            f"Expected 44 non-stress cases; got {len(actual)}. "
            f"Missing={len(missing)}; extra={len(extra)}; see processes.json"
        )
    classes = {}
    for item in items:
        for parent in item.iter_parents():
            if isinstance(parent, pytest.Class):
                key = (Path(str(item.path)).name, parent.name)
                if key in TARGETS:
                    classes[key] = parent
    if set(classes) != TARGETS:
        raise pytest.UsageError("The two expected shutdown classes were not collected")
    selected = []
    for key, node in classes.items():
        markers = [
            mark
            for mark in node.own_markers
            if mark.name == "skipif" and "QEMU" in mark.kwargs.get("reason", "")
        ]
        if len(markers) != 1 or markers[0].args != (True,):
            raise pytest.UsageError(f"Expected one active QEMU exclusion on {key}")
        selected.append((key, node, markers[0]))
    for key, node, marker in selected:
        node.own_markers.remove(marker)
        STATE["removed"].append({"file": key[0], "class": key[1], "condition": True})
    STATE["collected"] = actual
    save()


@pytest.hookimpl(hookwrapper=True, tryfirst=True)
def pytest_runtest_call(item):
    original = subprocess.run

    def tracked(*args, **kwargs):
        argv = args[0] if args else kwargs.get("args")
        if not isinstance(argv, (list, tuple)) or not argv or argv[0] != sys.executable:
            return original(*args, **kwargs)
        record = {"nodeid": item.nodeid, "exit_code": None, "timeout": False}
        started = time.monotonic()
        try:
            result = original(*args, **kwargs)
            record["exit_code"] = result.returncode
            return result
        except subprocess.TimeoutExpired:
            record["timeout"] = True
            raise
        except subprocess.CalledProcessError as exc:
            record["exit_code"] = exc.returncode
            raise
        except OSError as exc:
            record["spawn_error"] = type(exc).__name__
            raise
        finally:
            record["seconds"] = time.monotonic() - started
            STATE["roots"].append(record)
            save()

    subprocess.run = tracked
    try:
        yield
    finally:
        subprocess.run = original


def pytest_runtest_logreport(report):
    if report.when == "call" or report.failed or report.skipped:
        STATE["reports"].append(
            {"nodeid": report.nodeid, "when": report.when, "outcome": report.outcome}
        )


def pytest_sessionfinish(session, exitstatus):
    covered = {row["nodeid"] for row in STATE["roots"]}
    if covered != set(STATE["collected"]) or not covered:
        STATE["errors"].append("Subprocess accounting does not cover every selected case")
    fatal = {-4, -6, -7, -11, 132, 134, 135, 139}
    if any(row["exit_code"] in fatal or row["timeout"] for row in STATE["roots"]):
        STATE["errors"].append("A Python subprocess crashed or exceeded its existing timeout")
    if any(row["outcome"] == "skipped" for row in STATE["reports"]):
        STATE["errors"].append("Restored cases must execute, not skip")
    STATE["pytest_exitstatus"] = int(exitstatus)
    if STATE["errors"]:
        session.exitstatus = pytest.ExitCode.TESTS_FAILED
        reporter = session.config.pluginmanager.get_plugin("terminalreporter")
        if reporter is not None:
            reporter.write_sep("=", "ARM64 diagnostic accounting failed; see processes.json")
    STATE["final_exitstatus"] = int(session.exitstatus)
    save()
