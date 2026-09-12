"""
Pins the type stub to the runtime signatures of connect() and Connection.__init__.

Parses source with ast only, so it needs neither the native ddbc_bindings module nor a server.
Annotations are not compared because the stub spells them differently (a string forward
reference, an unqualified RetryPolicy); kind, name, order and default are.
"""

import ast
from pathlib import Path

import pytest

PKG = Path(__file__).resolve().parent.parent / "mssql_python"


def _signature(path, name, cls=None):
    """Return [(kind, name, default-source)] for a def in path, optionally inside class cls."""
    body = ast.parse(path.read_text(encoding="utf-8")).body
    if cls is not None:
        body = next(n for n in body if isinstance(n, ast.ClassDef) and n.name == cls).body
    func = next(n for n in body if isinstance(n, ast.FunctionDef) and n.name == name)
    a = func.args
    positional = a.posonlyargs + a.args
    defaults = [None] * (len(positional) - len(a.defaults)) + a.defaults
    params = [
        ("pos", arg.arg, ast.unparse(d) if d is not None else None)
        for arg, d in zip(positional, defaults)
    ]
    if a.vararg:
        params.append(("*", a.vararg.arg, None))
    params += [
        ("kwonly", arg.arg, ast.unparse(d) if d is not None else None)
        for arg, d in zip(a.kwonlyargs, a.kw_defaults)
    ]
    if a.kwarg:
        params.append(("**", a.kwarg.arg, None))
    return params


@pytest.mark.parametrize(
    "runtime_file, name, cls",
    [
        ("connection.py", "__init__", "Connection"),
        ("db_connection.py", "connect", None),
    ],
)
def test_stub_matches_runtime_signature(runtime_file, name, cls):
    runtime = _signature(PKG / runtime_file, name, cls)
    stub = _signature(PKG / "mssql_python.pyi", name, cls)
    assert stub == runtime


def test_connect_forwards_every_connection_parameter():
    init = _signature(PKG / "connection.py", "__init__", "Connection")
    assert _signature(PKG / "db_connection.py", "connect") == init[1:]  # drop self
