"""Proposed narrow full-suite skip policy; source identities are pinned by expected.json."""

import importlib.metadata
import importlib.util
import json
from pathlib import Path
import sys

from contracts import HERE, require, sha

RULES = {
    "tests/test_004_cursor_arrow.py::test_arrow_reader_read_pandas_returns_dataframe": {
        "reason": "Skipped: could not import 'pandas': No module named 'pandas'",
        "function": "test_arrow_reader_read_pandas_returns_dataframe",
        "predicate": "pandas_distribution_and_module_absent",
    },
    "tests/test_003_connection.py::test_set_attr_current_catalog_effect": {
        "reason": "Skipped: No other user databases available for testing",
        "function": "test_set_attr_current_catalog_effect",
        "predicate": "normal_test_observed_no_other_databases_from_TestDB",
    },
    "tests/test_027_getinfo.py::test_getinfo_oversized_decimal_string_keeps_compatibility": {
        "reason": "Skipped: Interpreter integer-string conversion limit is disabled or unavailable",
        "function": "test_getinfo_oversized_decimal_string_keeps_compatibility",
        "predicate": "normal_test_observed_disabled_integer_limit",
    },
}


def validate_static_skips(skips):
    forbidden = (
        "db_connection_string",
        "database connection",
        "requires database",
        "live database",
        "pyarrow",
        "polars",
        "azure-core",
        "mssql_py_core",
        "mssql_python_odbc",
        "native ddbc_bindings extension not built",
        "ddbc_bindings not available",
        "zstandard",
        "source checkout",
        "source tree",
        "not present",
    )
    for nodeid, reason in skips.items():
        require(
            not any(term in reason.lower() for term in forbidden),
            "Required dependency/database/source skip: " + nodeid + ": " + reason,
        )


def optional_pandas_state():
    spec_absent = importlib.util.find_spec("pandas") is None
    try:
        version = importlib.metadata.version("pandas")
    except importlib.metadata.PackageNotFoundError:
        version = None
    return {
        "module_absent": spec_absent,
        "distribution_absent": version is None,
        "version": version,
    }


def observe_skip(nodeid, exception, root, pandas_state):
    rule = RULES.get(nodeid)
    if rule is None:
        return None
    target = Path(root).resolve() / nodeid.split("::", 1)[0]
    expected = json.loads((HERE / "expected.json").read_text())["protected_sources"][
        nodeid.split("::", 1)[0]
    ]
    require(sha(target.read_bytes().replace(b"\r\n", b"\n")) == expected, "Skip source drift")
    frame = None
    traceback = exception.__traceback__
    while traceback is not None:
        candidate = traceback.tb_frame
        if (
            Path(candidate.f_code.co_filename).resolve() == target
            and candidate.f_code.co_name == rule["function"]
        ):
            frame = candidate
        traceback = traceback.tb_next
    if frame is None:
        return {"predicate": rule["predicate"], "verified": False}
    if rule["predicate"] == "pandas_distribution_and_module_absent":
        verified = pandas_state["module_absent"] and pandas_state["distribution_absent"]
        facts = pandas_state
    elif rule["predicate"] == "normal_test_observed_no_other_databases_from_TestDB":
        facts = {
            "original_database_is_owned_TestDB": frame.f_locals.get("original_db") == "TestDB",
            "other_database_rows_are_empty": frame.f_locals.get("rows") == [],
        }
        verified = all(facts.values())
    else:
        facts = {
            "test_local_limit_is_zero": frame.f_locals.get("limit") == 0,
            "interpreter_limit_is_zero": getattr(sys, "get_int_max_str_digits", lambda: 0)() == 0,
        }
        verified = all(facts.values())
    return {"predicate": rule["predicate"], "verified": bool(verified), "facts": facts}
