"""Diagnostic-only source, process, credential and accounting gates; no project imports."""

import base64
import hashlib
import html
import importlib
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import urllib.parse

BASE = "c8e2cab9d9be547dfcbab182b3158f2b8bfe2248"
OLD = "c0f6931019f670e1501bc642de714f9b56c9596ffc4d04e6b807484464726201"
FIXED = "b2205d287ef1725186279f1747d6ba65a46178ed8709eb2feeb6c05cbf605222"
REL = "eng/scripts/diagnostics/token_provider_48086"
AUTH = "tests/test_008_auth.py"
NAME = "test_concurrent_connections_with_same_token_provider"
EXACT = AUTH + "::TestCustomTokenProviderConnect::" + NAME
SCOPE = "https://database.windows.net/.default"
LABEL = "com.microsoft.mssql-python.ab48086-owner"
HERE = Path(__file__).resolve().parent


def configure_source_imports(entry_file):
    entry = Path(entry_file).resolve()
    require(entry.parent == HERE, "Entrypoint is outside the diagnostic source directory")
    root = HERE.parents[3]
    require(root / REL == HERE and Path.cwd().resolve() == root, "Unexpected source-root layout")
    require((root / "mssql_python" / "__init__.py").is_file(), "Source package is missing")
    sys.path.insert(0, str(root))
    source_package_origin(root, "mssql_python")
    return root


def source_package_origin(root, name):
    expected = root / name / "__init__.py"
    loaded = sys.modules.get(name)
    if loaded is not None:
        require(
            getattr(loaded, "__file__", None) is not None
            and Path(loaded.__file__).resolve() == expected,
            "Foreign package already imported: " + name,
        )
    spec = importlib.util.find_spec(name)
    require(
        spec is not None and spec.origin is not None and Path(spec.origin).resolve() == expected,
        "Source package import resolves outside the verified root: " + name,
    )
    return str(expected)


def import_source_package(root, name):
    source_package_origin(root, name)
    module = importlib.import_module(name)
    require(
        Path(module.__file__).resolve() == root / name / "__init__.py",
        "Imported a foreign source package: " + name,
    )
    return module


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def canonical(data, expected):
    require(b"\r" not in data and not data.startswith(b"\xef\xbb\xbf"), "Non-LF source")
    require(sha(data) == expected, "Source digest mismatch")
    return data.decode("utf-8")


def source_pair(root):
    old = (HERE / "baseline.snapshot").read_bytes()
    fixed = (root / AUTH).read_bytes()
    require(old != fixed, "Baseline and candidate must differ")
    return canonical(old, OLD), canonical(fixed, FIXED)


def clean_env(source=None):
    source = os.environ if source is None else source
    env = {
        k: v
        for k, v in source.items()
        if not any(
            w in k.upper()
            for w in ("CONNECTION_STRING", "AZURE", "TOKEN", "PASSWORD", "SECRET", "CREDENTIAL")
        )
        and not k.upper().startswith(("DB_", "SQL_", "MSSQL_"))
        and k.upper()
        not in (
            "PYTHONPATH",
            "PYTHONHOME",
            "PYTEST_ADDOPTS",
            "PYTEST_PLUGINS",
            "FEED_URL",
            "PYTHONOPTIMIZE",
        )
    }
    env["PYTHONPATH"] = str(HERE) + os.pathsep + str(Path.cwd())
    env["PYTHONFAULTHANDLER"] = "1"
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    env["PIP_RETRIES"] = "0"
    return env


def redact(data, secret):
    require(bool(secret), "Empty redaction secret")
    text = data.decode("utf-8", errors="replace") if isinstance(data, bytes) else data
    for value in (
        secret,
        html.escape(secret, quote=True),
        urllib.parse.quote(secret, safe=""),
        base64.b64encode(secret.encode()).decode(),
        json.dumps(secret)[1:-1],
    ):
        text = text.replace(value, "[REDACTED]")
    return text


def save(path, value):
    Path(path).write_text(json.dumps(value, indent=2), encoding="utf-8")


def bounded(argv, *, timeout, env=None, cwd=None):
    """Kill only this command's process group on timeout, including its grandchildren."""
    started = time.monotonic()
    with subprocess.Popen(
        argv,
        env=env,
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        start_new_session=os.name != "nt",
    ) as proc:
        timed_out = False
        try:
            output = proc.communicate(timeout=timeout)[0]
        except subprocess.TimeoutExpired:
            timed_out = True
            if os.name == "nt":
                subprocess.run(
                    ["taskkill", "/PID", str(proc.pid), "/T", "/F"],
                    capture_output=True,
                    check=True,
                    timeout=15,
                )
            else:
                os.killpg(proc.pid, signal.SIGKILL)
            output = proc.communicate(timeout=15)[0]
        return {
            "returncode": proc.returncode,
            "timeout": timed_out,
            "seconds": time.monotonic() - started,
        }, output


def checked(argv, *, timeout=120, env=None, cwd=None):
    result, output = bounded(argv, timeout=timeout, env=env, cwd=cwd)
    require(
        not result["timeout"] and result["returncode"] == 0,
        f"{Path(argv[0]).name} failed: {result}",
    )
    return output


def validate_counters(record):
    for key in (
        "operations",
        "distinct_doubles",
        "constructor_calls",
        "constructor_args",
        "provider_calls",
        "provider_args",
        "rollback_calls",
        "close_calls",
    ):
        require(type(record.get(key)) is int and record[key] == 20, "Wrong counter: " + key)
    require(record.get("workers") == 8, "Concurrency changed")
    require(record.get("scopes_valid") is True, "Wrong provider scopes")
    require(record.get("per_double_valid") is True, "Wrong native double calls")


def validate_run(collected, executed, expected=None, *, counters=0, allow_skips=False):
    ids = collected["nodes"]
    require(len(ids) == len(set(ids)) and bool(ids), "Empty or duplicate collection")
    if expected is not None:
        require(ids == expected, "Frozen node ID mismatch")
    require(executed["nodes"] == ids, "Execution collection drift")
    require(
        not collected["collection_issues"] and not executed["collection_issues"],
        "Collection failed or skipped a module",
    )
    require(not executed["fixture_reconnections"], "Normal fixture reconnected")
    require(not executed["fixture_observer_errors"], "Normal fixture observer recorded no calls")
    require(not executed["retry_reports"], "Pytest retried a case")
    allowed = collected["static_skips"] if allow_skips else {}
    if allow_skips:
        from skip_policy import validate_static_skips

        validate_static_skips(allowed)
    by_id = {}
    for row in executed["reports"]:
        by_id.setdefault(row["nodeid"], []).append(row)
    require(set(by_id) == set(ids), "Missing or extra executions")
    for nodeid, rows in by_id.items():
        phases = [r["when"] for r in rows]
        require(len(phases) == len(set(phases)), "Repeated pytest phase")
        require(all(r["outcome"] != "failed" for r in rows), "Failed test: " + nodeid)
        skipped = [r for r in rows if r["outcome"] == "skipped"]
        if skipped:
            require(len(skipped) == 1, "Repeated skip: " + nodeid)
            if nodeid in allowed:
                require(skipped[0]["reason"] == allowed[nodeid], "Skip reason changed: " + nodeid)
                require(set(phases) == {"setup", "teardown"}, "Incomplete static skipped test")
            else:
                from skip_policy import RULES

                rule = RULES.get(nodeid) if allow_skips else None
                evidence = executed["dynamic_skip_evidence"].get(nodeid)
                require(
                    rule is not None and evidence is not None, "Unexpected dynamic skip: " + nodeid
                )
                require(skipped[0]["reason"] == rule["reason"], "Dynamic skip reason changed")
                require(
                    evidence.get("verified") is True
                    and evidence.get("predicate") == rule["predicate"],
                    "Dynamic skip predicate not proven",
                )
                require(
                    skipped[0]["when"] == "call" and set(phases) == {"setup", "call", "teardown"},
                    "Incomplete dynamic skipped test",
                )
        else:
            require(nodeid not in allowed, "Expected static skip changed")
            require(set(phases) == {"setup", "call", "teardown"}, "Incomplete test")
            require(all(r["outcome"] == "passed" for r in rows), "Nonpassing outcome")
    require(len(executed["counters"]) == counters, "Missing exact-case counter records")
    for record in executed["counters"]:
        validate_counters(record)
