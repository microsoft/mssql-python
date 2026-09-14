"""Observe normal pytest fixtures/tests without replacing their scheduling or assertions."""

import ast
import inspect
import json
import os
from pathlib import Path
import sys

import pytest
from _pytest.skipping import evaluate_skip_marks

from contracts import AUTH, HERE, NAME, SCOPE, require, save, sha
from skip_policy import RULES, observe_skip, optional_pandas_state

DATA = {
    "nodes": [],
    "static_skips": {},
    "collection_issues": [],
    "reports": [],
    "fixture_connect_attempts": [],
    "fixture_reconnections": [],
    "fixture_observer_errors": [],
    "retry_reports": [],
    "counters": [],
    "dynamic_skip_evidence": {},
    "optional_pandas": {},
}


def pytest_collection_finish(session):
    DATA["optional_pandas"] = optional_pandas_state()
    for item in session.items:
        DATA["nodes"].append(item.nodeid)
        skip = evaluate_skip_marks(item)
        if skip:
            DATA["static_skips"][item.nodeid] = "Skipped: " + skip.reason
    save(os.environ["QUAL_REPORT"], DATA)


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()
    if (
        report.skipped
        and report.when == "call"
        and call.excinfo is not None
        and item.nodeid in RULES
    ):
        DATA["dynamic_skip_evidence"][item.nodeid] = observe_skip(
            item.nodeid, call.excinfo.value, item.config.rootpath, DATA["optional_pandas"]
        )


def pytest_collectreport(report):
    if report.failed or report.skipped:
        DATA["collection_issues"].append(
            {
                "nodeid": report.nodeid,
                "outcome": report.outcome,
                "reason": str(report.longrepr),
            }
        )


def pytest_runtest_logreport(report):
    if getattr(report, "rerun", 0) or report.outcome == "rerun":
        DATA["retry_reports"].append(report.nodeid)
    DATA["reports"].append(
        {
            "nodeid": report.nodeid,
            "when": report.when,
            "outcome": report.outcome,
            "reason": report.longrepr[2] if report.skipped else None,
        }
    )
    save(os.environ["QUAL_REPORT"], DATA)


@pytest.hookimpl(hookwrapper=True)
def pytest_fixture_setup(fixturedef, request):
    target_code = protected_fixture_code(fixturedef, request.config.rootpath)
    if target_code is None:
        yield
        return
    require(sys.getprofile() is None, "Unexpected profiling hook during fixture")
    attempts = []

    def profile(frame, event, arg):
        caller = frame.f_back
        if (
            event == "call"
            and frame.f_code.co_name == "connect"
            and caller
            and caller.f_code is target_code
        ):
            attempts.append(1)

    sys.setprofile(profile)
    try:
        yield
    finally:
        sys.setprofile(None)
        row = {"fixture_node": request.node.nodeid, "connect_attempts": len(attempts)}
        DATA["fixture_connect_attempts"].append(row)
        if len(attempts) == 0:
            DATA["fixture_observer_errors"].append(row)
        elif len(attempts) > 1:
            DATA["fixture_reconnections"].append(row)


def protected_fixture_code(fixturedef, root):
    if fixturedef.argname != "db_connection":
        return None
    function = inspect.unwrap(fixturedef.func)
    code = getattr(function, "__code__", None)
    target = Path(root).resolve() / "tests" / "conftest.py"
    if code is None or Path(code.co_filename).resolve() != target:
        return None
    if function.__qualname__ != "db_connection":
        return None
    expected = json.loads((HERE / "expected.json").read_text())["protected_sources"][
        "tests/conftest.py"
    ]
    source = target.read_bytes()
    require(sha(source.replace(b"\r\n", b"\n")) == expected, "Protected fixture source drift")
    definitions = [
        node
        for node in ast.parse(source).body
        if isinstance(node, ast.FunctionDef) and node.name == "db_connection"
    ]
    require(len(definitions) == 1, "Protected fixture definition missing or duplicated")
    definition = definitions[0]
    first_line = min([definition.lineno] + [d.lineno for d in definition.decorator_list])
    require(code.co_firstlineno == first_line, "Protected fixture code identity changed")
    return code


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_call(item):
    if not item.nodeid.startswith(AUTH + "::") or item.originalname != NAME:
        yield
        return
    require(sys.getprofile() is None, "Unexpected profiling hook during exact test")

    def profile(frame, event, arg):
        if event != "return" or frame.f_code.co_name != NAME:
            return
        values = frame.f_locals
        if "natives" not in values:
            return
        natives = values["natives"]
        constructor, provider = values["mock_ddbc_conn"], values["mock_cred"]
        row = {
            "nodeid": item.nodeid,
            "operations": len(natives),
            "workers": values["executor"]._max_workers,
            "distinct_doubles": len({id(n) for n in natives}),
            "constructor_calls": constructor.call_count,
            "constructor_args": len(constructor.call_args_list),
            "provider_calls": provider.get_token.call_count,
            "provider_args": len(provider.get_token.call_args_list),
            "scopes_valid": all(
                c.args == (SCOPE,) and c.kwargs == {} for c in provider.get_token.call_args_list
            ),
            "rollback_calls": sum(n.rollback.call_count for n in natives),
            "close_calls": sum(n.close.call_count for n in natives),
            "per_double_valid": all(
                n.__bool__.return_value is True
                and n.get_autocommit.return_value is False
                and n.set_autocommit.call_count == 1
                and n.set_autocommit.call_args.args == (False,)
                and n.set_autocommit.call_args.kwargs == {}
                and n.rollback.call_count == n.close.call_count == 1
                and n.rollback.call_args.args == n.close.call_args.args == ()
                and n.rollback.call_args.kwargs == n.close.call_args.kwargs == {}
                for n in natives
            ),
        }
        DATA["counters"].append(row)

    sys.setprofile(profile)
    try:
        yield
    finally:
        sys.setprofile(None)


def pytest_sessionfinish(session, exitstatus):
    DATA["exitstatus"] = int(exitstatus)
    save(os.environ["QUAL_REPORT"], DATA)
